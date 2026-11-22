from __future__ import annotations

import json
import sqlite3
from contextlib import closing

import pytest
from native_units import unit_budget
from riverhog_provenance import validate_journal
from riverhog_provenance._journal_prefix import JournalPrefix
from riverhog_provenance._native_conductor import LiveControl
from riverhog_provenance.journal_session import JournalValidationSession
from riverhog_provenance.journal_store import JournalStore
from sqlite_admission import SqliteProgressConnection
from test_retained_journal_session import OwnedSource, directory, open_session

ATTEMPT = "a" * 32


class _Connection(SqliteProgressConnection):
    failure = None

    def commit(self):
        if self.failure == "before":
            raise OSError("original private commit failed")
        super().commit()
        if self.failure == "after":
            raise OSError("original private commit response was lost")


def _reach(session, phase):
    for _ in range(4096):
        if session.progress().phase == phase:
            return
        session.advance(unit_budget())
    raise AssertionError("small journal did not reach its Native phase")


@pytest.mark.parametrize("failure", ["before", "after"])
def test_actual_frame_admission_receives_live_atom_only_after_original_commit(
    journal, catalog, failure
):
    def connect(*args, **kwargs):
        return sqlite3.connect(*args, factory=_Connection, **kwargs)

    with JournalValidationSession(
        (journal,), attempt_id=ATTEMPT, catalog=catalog, connection_factory=connect
    ) as session:
        session.advance(unit_budget())
        assert session._input.pending and session._replay._current is None
        db = session._store.db
        received = session._ledger._control.state
        db.failure = failure
        with pytest.raises(OSError, match="original private commit") as caught:
            session.advance(unit_budget())
        assert session._replay._current is None
        assert session._ledger._control.state is received
        assert not session._ledger.epoch.quiescent
        with closing(db.execute("SELECT count(*) FROM journal_validation_frame")) as rows:
            assert rows.fetchone() == ((0,) if failure == "before" else (1,))
        db.failure = None
        with pytest.raises(RuntimeError, match="session failed") as reused:
            session.advance(unit_budget())
        assert reused.value.__cause__ is caught.value
        with pytest.raises(RuntimeError, match="requires reconstruction"):
            session.progress()
    assert session.storage_received


def test_actual_frame_publication_is_not_quiescent_until_live_receiving_finishes(
    journal, catalog, monkeypatch
):
    with JournalValidationSession((journal,), attempt_id=ATTEMPT, catalog=catalog) as session:
        _reach(session, "publish")
        original = JournalStore.__setattr__
        frame_count = session._store.frame_count
        current = session._replay._current
        events = []

        def receive(store, name, value):
            if store is session._store and name == "frame_count":
                events.append(value)
                assert not store.db.in_transaction
                assert not session._ledger.epoch.quiescent
                assert session._replay._current is current
            original(store, name, value)

        monkeypatch.setattr(JournalStore, "__setattr__", receive)
        assert session.advance(unit_budget()).accepted_frames == frame_count + 1
        assert events == [frame_count + 1]
        assert session._replay._current is None and session._ledger.epoch.quiescent


def test_actual_postcommit_cache_failure_discards_executor_and_keeps_storage_receivable(
    journal, catalog, monkeypatch
):
    session = JournalValidationSession((journal,), attempt_id=ATTEMPT, catalog=catalog)
    try:
        _reach(session, "publish")
        original = JournalStore.__setattr__
        failure = RuntimeError("original frame-cache receiving failed")
        before = session._ledger.load().checkpoint.frame_ordinal

        def receive(store, name, value):
            if store is session._store and name == "frame_count":
                raise failure
            original(store, name, value)

        monkeypatch.setattr(JournalStore, "__setattr__", receive)
        with pytest.raises(RuntimeError) as caught:
            session.advance(unit_budget())
        assert caught.value is failure
        assert not session._ledger.epoch.quiescent
        failed_prefix = session._prefix
        with pytest.raises(RuntimeError, match="requires reconstruction") as unusable:
            failed_prefix.load()
        assert unusable.value.__cause__ is failure
        inputs = json.loads(failed_prefix.binding)
        verified = JournalPrefix(
            session._store.db,
            attempt_id=failed_prefix.attempt_id,
            source_binding=inputs["source_binding"],
            catalog=catalog,
            require_profiles=inputs["require_profiles"],
            expected_anchor=failed_prefix.expected_anchor,
            require_exact_tail=failed_prefix.require_exact_tail,
        )
        assert verified.epoch is not failed_prefix.epoch
        assert verified.binding == failed_prefix.binding
        assert verified.load().frame_count == before + 1
        with pytest.raises(RuntimeError, match="requires reconstruction"):
            session.progress()
        with pytest.raises(RuntimeError, match="terminal result"):
            session.result()
    finally:
        session.close()
    assert session.storage_received


def test_actual_retained_input_receiving_failure_requires_a_new_verified_executor(
    tmp_path, journal, catalog, journal_constructions, monkeypatch
):
    path = directory(tmp_path)
    source = OwnedSource(journal, catalog)
    original = LiveControl.receive
    with open_session(
        path, source, catalog, create=True, journal_constructions=journal_constructions
    ) as session:
        cursor = session._input.cursor
        failure = RuntimeError("original source-cursor receiving failed")

        def receive(control, value):
            if control is session._input._control:
                assert not session._ledger.epoch.quiescent
                raise failure
            original(control, value)

        monkeypatch.setattr(LiveControl, "receive", receive)
        with pytest.raises(RuntimeError) as caught:
            session.advance(unit_budget())
        assert caught.value is failure
        assert session._input.cursor == cursor
        assert not session._ledger.epoch.quiescent
        with pytest.raises(RuntimeError, match="requires reconstruction"):
            session.progress()
    monkeypatch.setattr(LiveControl, "receive", original)
    with open_session(
        path, source, catalog, epoch=1, journal_constructions=journal_constructions
    ) as resumed:
        assert resumed._input.cursor.byte_offset > cursor.byte_offset
        assert resumed._input.ledger is resumed._ledger is resumed._replay.ledger
        _reach(resumed, "done")
        result = resumed.result()
    with result, validate_journal(journal, catalog=catalog) as expected:
        assert result.materialize() == expected.materialize()


@pytest.mark.parametrize("fail", [False, True])
def test_actual_stream_buffer_receiving_stays_in_the_original_admission_epoch(
    journal, catalog, monkeypatch, fail
):
    with JournalValidationSession((journal,), attempt_id=ATTEMPT, catalog=catalog) as session:
        session.advance(unit_budget())
        source = session._input
        assert source.pending
        admitted = type(source).admitted
        failure = RuntimeError("original stream-buffer receiving failed")
        events = []

        def receive(owner):
            assert owner is source
            assert not session._store.db.in_transaction
            assert not session._ledger.epoch.quiescent
            assert session._replay._current is not None
            events.append(1)
            if fail:
                raise failure
            admitted(owner)

        monkeypatch.setattr(type(source), "admitted", receive)
        if fail:
            with pytest.raises(RuntimeError) as caught:
                session.advance(unit_budget())
            assert caught.value is failure
            assert source.pending and not session._ledger.epoch.quiescent
            with pytest.raises(RuntimeError, match="requires reconstruction"):
                session.progress()
        else:
            session.advance(unit_budget())
            assert not source.pending and session._ledger.epoch.quiescent
        assert events == [1]
    assert session.storage_received
