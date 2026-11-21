from __future__ import annotations

import sqlite3
from contextlib import closing

import pytest
from riverhog_provenance import IncompleteSourceError, validate_journal
from riverhog_provenance._journal_prefix import PrefixState
from riverhog_provenance._native_epoch import EpochContext
from riverhog_provenance._session_checkpoint import (
    StaleValidationCheckpoint,
    ValidationCheckpoint,
    ValidationLedger,
)
from riverhog_provenance.journal_session import JournalValidationSession
from test_journal_session import finish
from test_native_epoch_integration import _reach
from test_retained_journal_session import OwnedSource, directory, open_session

ATTEMPT = "a" * 32


def _refuse_decode(*_args):
    raise AssertionError("live typed control was decoded again")


def test_actual_session_completes_without_decoding_received_parent_state(
    journal, catalog, monkeypatch
):
    with validate_journal(journal, catalog=catalog) as expected:
        with JournalValidationSession((journal,), attempt_id=ATTEMPT, catalog=catalog) as session:
            monkeypatch.setattr(ValidationCheckpoint, "from_json", _refuse_decode)
            assert finish(session, rows=1).complete
            result = session.result()
        with result:
            assert result.materialize() == expected.materialize()
            assert result.journal_sha256 == expected.journal_sha256
    assert session.storage_received


def test_repeated_live_progress_uses_typed_prefix_without_history_payload_reads(
    journal, catalog, monkeypatch
):
    with JournalValidationSession((journal,), attempt_id=ATTEMPT, catalog=catalog) as session:
        _reach(session, "publish")
        session.advance()
        received = session._ledger.load()
        assert received.accepted.frame_count == 1
        monkeypatch.setattr(ValidationCheckpoint, "from_json", _refuse_decode)
        monkeypatch.setattr(PrefixState, "from_json", _refuse_decode)

        def authorize(action, table, column, *_rest):
            return (
                sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_READ and table == "frames" and column == "value"
                else sqlite3.SQLITE_OK
            )

        session._store.db.set_authorizer(authorize)
        try:
            expected = session.progress()
            for _ in range(4):
                assert session.progress() == expected
                assert session._ledger.load() is received
        finally:
            session._store.db.set_authorizer(None)


def test_cold_parent_decodes_once_but_checks_unchanged_prefix_predecessor_again(
    journal, catalog, monkeypatch
):
    with JournalValidationSession((journal,), attempt_id=ATTEMPT, catalog=catalog) as session:
        _reach(session, "publish")
        session.advance()
        original = session._ledger
        cold = ValidationLedger(
            session._store.db,
            attempt_id=original.attempt_id,
            binding=original.binding,
            worker_epoch=original.worker_epoch,
        )
        calls = {"parent": 0, "prefix": 0}
        parent_decode, prefix_decode = ValidationCheckpoint.from_json, PrefixState.from_json

        def parent(raw):
            calls["parent"] += 1
            return parent_decode(raw)

        def prefix(raw):
            calls["prefix"] += 1
            return prefix_decode(raw)

        monkeypatch.setattr(ValidationCheckpoint, "from_json", parent)
        monkeypatch.setattr(PrefixState, "from_json", prefix)
        received = cold.load()
        assert calls == {"parent": 1, "prefix": 1}
        assert cold.load() is received and calls == {"parent": 1, "prefix": 1}
        session._store.db.execute("UPDATE frames SET reference=?", (b"changed",)).close()
        session._store.db.commit()
        with pytest.raises(StaleValidationCheckpoint, match="predecessor"):
            cold.load()
        assert calls == {"parent": 1, "prefix": 1}


@pytest.mark.parametrize(
    "column,value",
    [
        ("worker_epoch", "1"),
        ("attempt_id", "b" * 32),
        ("binding", b"foreign"),
        ("algorithm", "foreign"),
        ("checkpoint", "{}"),
    ],
)
def test_live_typed_parent_does_not_cache_positive_authority(journal, catalog, column, value):
    with JournalValidationSession((journal,), attempt_id=ATTEMPT, catalog=catalog) as session:
        before = session._ledger.load()
        session._store.db.execute(
            "UPDATE journal_validation SET " + column + "=?", (value,)
        ).close()
        session._store.db.commit()
        with pytest.raises(RuntimeError):
            session.progress()
        assert session._ledger._received is before


def test_retained_live_progress_keeps_fresh_source_authority_and_never_reads_payload(
    tmp_path, journal, catalog, journal_constructions, monkeypatch
):
    source = OwnedSource(journal, catalog)
    with open_session(
        directory(tmp_path),
        source,
        catalog,
        create=True,
        journal_constructions=journal_constructions,
    ) as session:
        _reach(session, "publish")
        session.advance()
        source.calls.clear()
        monkeypatch.setattr(ValidationCheckpoint, "from_json", _refuse_decode)
        monkeypatch.setattr(PrefixState, "from_json", _refuse_decode)
        assert session.progress().accepted_frames == 1 and not source.calls
        source.current = False
        with pytest.raises(IncompleteSourceError):
            session.progress()
        assert not source.calls


def test_failed_original_parent_receiving_discards_executor_without_exposing_pending_state(
    journal, catalog, monkeypatch
):
    session = JournalValidationSession((journal,), attempt_id=ATTEMPT, catalog=catalog)
    original = ValidationLedger._receive
    failure = RuntimeError("original typed parent receiving failed")
    try:
        session.advance()
        before = session._ledger.load()

        def receive(ledger, pending):
            if ledger is session._ledger:
                assert pending.checkpoint.version == before.checkpoint.version + 1
                assert ledger._received is before
                assert not ledger.db.in_transaction and not ledger.epoch.quiescent
                raise failure
            original(ledger, pending)

        monkeypatch.setattr(ValidationLedger, "_receive", receive)
        with pytest.raises(RuntimeError) as caught:
            session.advance()
        assert caught.value is failure and session._ledger._received is before
        assert session._replay._current is None
        with pytest.raises(RuntimeError, match="requires reconstruction"):
            session.progress()
    finally:
        session.close()
    assert session.storage_received


def test_live_control_receiving_requires_exact_original_postcommit_phase():
    with (
        closing(sqlite3.connect(":memory:")) as db,
        closing(sqlite3.connect(":memory:")) as foreign,
    ):
        epoch = EpochContext(db)
        with pytest.raises(RuntimeError, match="original post-commit"):
            epoch.require_receiving(db)

        def mutate(owner):
            with pytest.raises(RuntimeError, match="original post-commit"):
                owner.require_receiving(db)

        def receive(_pending):
            epoch.require_receiving(db)
            with pytest.raises(RuntimeError, match="original post-commit"):
                epoch.require_receiving(foreign)
            db.execute("BEGIN").close()
            try:
                with pytest.raises(RuntimeError, match="original post-commit"):
                    epoch.require_receiving(db)
            finally:
                db.rollback()

        epoch.apply(mutate, receive=receive)
        assert epoch.quiescent
