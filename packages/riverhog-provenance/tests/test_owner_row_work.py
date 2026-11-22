from __future__ import annotations

import sqlite3
from contextlib import closing

import pytest
from native_units import unit_budget
from riverhog_provenance import ProvenanceValidationError
from riverhog_provenance._entry_replay import EntryReplay
from riverhog_provenance._journal_prefix import JournalPrefix
from riverhog_provenance._native_epoch import EpochContext
from riverhog_provenance._native_work import NativeWorkCapacity, WorkAmount
from riverhog_provenance._session_checkpoint import ValidationLedger
from riverhog_provenance._session_input import RetainedInput
from riverhog_provenance.journal_session import (
    JournalValidationSession,
    JournalValidationWorkBudget,
)
from test_assertion_staging import identity
from test_native_epoch_integration import _reach
from test_parent_work_quantum import accepted_prefix
from test_preparation_work import quantum
from test_retained_journal_session import OwnedSource, directory, open_session


def context(tmp_path, journal, catalog, journal_constructions, retained):
    if retained:
        return open_session(
            directory(tmp_path),
            OwnedSource(journal, catalog),
            catalog,
            create=True,
            journal_constructions=journal_constructions,
        )
    return JournalValidationSession((journal,), attempt_id="a" * 32, catalog=catalog)


@pytest.mark.parametrize("accepted", [False, True])
def test_prefix_point_attempts_share_original_parent_quantum_and_cannot_replenish_it(
    journal, catalog, accepted
):
    with JournalValidationSession((journal,), attempt_id="a" * 32, catalog=catalog) as session:
        before = accepted_prefix(session, accepted)
        prefix, original, trace = session._prefix, quantum(1 + accepted), []
        assert prefix.epoch is session._ledger.epoch is session._replay.epoch
        db = session._store.db
        db.set_trace_callback(trace.append)
        try:
            with prefix.epoch.execution(None, work=original):
                assert prefix.load() == before.accepted
                with pytest.raises(NativeWorkCapacity):
                    prefix.load()
        finally:
            db.set_trace_callback(None)
        assert len(trace) == original.consumed.rows == 1 + accepted
        assert all(sql.startswith("SELECT") for sql in trace)
        assert original.held == WorkAmount() and prefix.epoch.quiescent
        assert session._ledger.load() is before
        original.require_received()
    assert session.storage_received


@pytest.mark.parametrize("retained", [False, True])
def test_actual_quarantine_admission_reserves_its_mutation_with_parent_before_begin(
    tmp_path, journal, catalog, journal_constructions, retained, monkeypatch
):
    with context(tmp_path, journal, catalog, journal_constructions, retained) as session:
        for _ in range(32):
            if session._input.pending:
                break
            session.advance(unit_budget())
        else:
            pytest.fail("small actual source did not expose its pending frame")
        parent, prefix, db = session._ledger, session._prefix, session._store.db
        assert prefix.epoch is parent.epoch is session._replay.epoch
        if retained:
            assert session._input.ledger.epoch is parent.epoch
        else:
            assert session._input.epoch is parent.epoch
        before, cursor, changes, seen, trace = (
            session.progress(),
            session._input.cursor,
            db.total_changes,
            [],
            [],
        )
        execute = prefix._execute

        def selected(*args, **kwargs):
            seen.append(prefix.epoch.work)
            return execute(*args, **kwargs)

        monkeypatch.setattr(prefix, "_execute", selected)
        db.set_trace_callback(trace.append)
        try:
            with pytest.raises(NativeWorkCapacity):
                session.advance(unit_budget(work=JournalValidationWorkBudget(rows=11)))
        finally:
            db.set_trace_callback(None)
        assert not any(sql.startswith("BEGIN") for sql in trace)
        assert session.progress() == before and db.total_changes == changes
        assert session._input.cursor == cursor and session._input.pending
        assert session._replay._current is None and session._failure is None
        refused = seen[0]
        assert refused.consumed.rows == 5 and refused.held == WorkAmount()
        seen.clear()
        after = session.advance(unit_budget(work=JournalValidationWorkBudget(rows=12)))
        assert after.phase == "entry-admission" and seen[0] is not refused
        assert seen[0].consumed.rows == 10 and seen[0].held == WorkAmount()
        assert session._replay._current is not None and parent.epoch.quiescent
        refused.require_received()
        seen[0].require_received()
    assert session.storage_received


@pytest.mark.parametrize("retained", [False, True])
def test_actual_frame_publication_adds_prefix_and_quarantine_writes_to_parent_admission(
    tmp_path, journal, catalog, journal_constructions, retained, monkeypatch
):
    with context(tmp_path, journal, catalog, journal_constructions, retained) as session:
        _reach(session, "publish")
        parent, prefix, db = session._ledger, session._prefix, session._store.db
        before, changes, trace, seen = session.progress(), db.total_changes, [], []
        load = parent.load

        def loaded():
            if parent.epoch.work is not None:
                seen.append(parent.epoch.work)
            return load()

        monkeypatch.setattr(parent, "load", loaded)
        db.set_trace_callback(trace.append)
        try:
            with pytest.raises(NativeWorkCapacity):
                session.advance(unit_budget(work=JournalValidationWorkBudget(rows=12)))
        finally:
            db.set_trace_callback(None)
        assert not any(sql.startswith("BEGIN") for sql in trace)
        assert session.progress() == before and db.total_changes == changes
        assert prefix.load().frame_count == 0
        refused = seen[0]
        assert refused.consumed.rows == 4 and refused.held == WorkAmount()
        seen.clear()
        assert (
            session.advance(unit_budget(work=JournalValidationWorkBudget(rows=13))).accepted_frames
            == 1
        )
        assert seen[0] is not refused and seen[0].consumed.rows == 12
        assert seen[0].held == WorkAmount() and parent.epoch.quiescent
        assert prefix.load().frame_count == 1 and session._replay._current is None
        refused.require_received()
        seen[0].require_received()
    assert session.storage_received


def test_actual_retained_cursor_reserves_its_update_and_keeps_source_cost_on_refusal(
    tmp_path, journal, catalog, journal_constructions, monkeypatch
):
    with context(tmp_path, journal, catalog, journal_constructions, True) as session:
        parent, db, scan = session._ledger, session._store.db, RetainedInput.scan
        before, seen, trace = session._input.state, [], []
        changes = db.total_changes

        def scanned(current, *args, **kwargs):
            assert current.ledger.epoch is session._prefix.epoch
            seen.append(current.ledger.epoch.work)
            return scan(current, *args, **kwargs)

        monkeypatch.setattr(RetainedInput, "scan", scanned)
        db.set_trace_callback(trace.append)
        try:
            with pytest.raises(NativeWorkCapacity):
                session.advance(
                    unit_budget(source_octets=7, work=JournalValidationWorkBudget(rows=8))
                )
        finally:
            db.set_trace_callback(None)
        assert not any(sql.startswith("BEGIN") for sql in trace)
        assert session._input.state is before and db.total_changes == changes
        assert session._failure is None and parent.epoch.quiescent
        assert seen[0].consumed == WorkAmount(rows=2, octets=7, source_octets=7, transitions=1)
        assert seen[0].held == WorkAmount()
        assert (
            session.advance(
                unit_budget(source_octets=7, work=JournalValidationWorkBudget(rows=9))
            ).source_octets
            == 7
        )
        assert session._input.state is not before and seen[1] is not seen[0]
        assert seen[1].consumed == WorkAmount(rows=7, octets=7, source_octets=7, transitions=1)
        assert seen[1].held == WorkAmount() and parent.epoch.quiescent
        for original in seen:
            original.require_received()
    assert session.storage_received


@pytest.mark.parametrize("failure", [False, True])
def test_store_lifecycle_original_lookup_and_mutation_attempts_retain_cost_after_failure(
    journal, catalog, failure
):
    with JournalValidationSession((journal,), attempt_id="a" * 32, catalog=catalog) as session:
        epoch, db, trace = session._ledger.epoch, session._store.db, []
        original, refused = quantum(2), quantum(1)
        seen = []

        def inspected():
            seen.append(epoch.work)
            epoch.account(WorkAmount(rows=1))

        def mutate(_active):
            session._store._record_lifecycle_time(identity(900), "usage", 7, inspect_row=inspected)

        db.set_trace_callback(trace.append)
        try:
            with epoch.execution(None, work=refused):
                with pytest.raises(NativeWorkCapacity):
                    epoch.apply(mutate, receive=lambda _value: None, admission=WorkAmount(rows=2))
        finally:
            db.set_trace_callback(None)
        assert trace == seen == [] and refused.consumed == refused.held == WorkAmount()

        def deny_insert(action, table, *_rest):
            return (
                sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_INSERT and table == "lifecycle_extrema"
                else sqlite3.SQLITE_OK
            )

        if failure:
            db.set_authorizer(deny_insert)
        try:
            if failure:
                with pytest.raises(sqlite3.DatabaseError):
                    with epoch.execution(None, work=original):
                        epoch.apply(
                            mutate, receive=lambda _value: None, admission=WorkAmount(rows=2)
                        )
            else:
                with epoch.execution(None, work=original):
                    epoch.apply(mutate, receive=lambda _value: None, admission=WorkAmount(rows=2))
        finally:
            db.set_authorizer(None)
        assert seen == [original, original]
        assert original.consumed == WorkAmount(rows=2) and original.held == WorkAmount()
        stored = db.execute("SELECT earliest,latest_usage FROM lifecycle_extrema").fetchall()
        assert stored == ([] if failure else [("7", "7")])
        if failure:
            with pytest.raises(RuntimeError, match="requires reconstruction"):
                epoch.require_usable()
        else:
            assert epoch.quiescent
        refused.require_received()
        original.require_received()
    assert session.storage_received


@pytest.mark.parametrize("missing_context", [False, True])
def test_recording_charges_actual_effective_identity_probes_including_missing_context(
    journal, catalog, missing_context
):
    with JournalValidationSession((journal,), attempt_id="a" * 32, catalog=catalog) as session:
        _reach(session, "recording")
        document = dict(session._replay._current.document)
        if missing_context:
            document["recording_context_id"] = identity(999999)
        else:
            document.pop("recording_context_id", None)
        epoch, db, trace = session._ledger.epoch, session._store.db, []
        original = quantum(2 if missing_context else 1)
        db.set_trace_callback(trace.append)
        try:
            with epoch.execution(None, work=original):
                if missing_context:
                    with pytest.raises(ProvenanceValidationError, match="context"):
                        session._store._check_entry_recording(
                            document, inspect_row=session._replay._account_row
                        )
                else:
                    session._store._check_entry_recording(
                        document, inspect_row=session._replay._account_row
                    )
                with pytest.raises(NativeWorkCapacity):
                    session._store._check_entry_recording(
                        document, inspect_row=session._replay._account_row
                    )
        finally:
            db.set_trace_callback(None)
        expected = 2 if missing_context else 1
        assert len(trace) == original.consumed.rows == expected
        assert all(sql.startswith("SELECT") for sql in trace)
        assert original.held == WorkAmount() and epoch.quiescent
        original.require_received()
    assert session.storage_received


def test_prefix_and_conductor_refuse_foreign_original_epoch(journal, catalog):
    with (
        JournalValidationSession((journal,), attempt_id="a" * 32, catalog=catalog) as session,
        closing(sqlite3.connect(":memory:")) as foreign,
    ):
        with pytest.raises(ValueError, match="original epoch connection"):
            JournalPrefix(
                session._store.db,
                attempt_id="a" * 32,
                source_binding="owned-source",
                catalog=catalog,
                require_profiles=True,
                epoch=EpochContext(foreign),
            )
        unrelated = ValidationLedger(
            session._store.db,
            attempt_id=session.attempt_id,
            binding=session._prefix.binding,
            worker_epoch=0,
        )
        with pytest.raises(ValueError, match="original prefix and parent epoch"):
            EntryReplay(session._store, prefix=session._prefix, ledger=unrelated, catalog=catalog)
        assert session._ledger.epoch.quiescent
    assert session.storage_received
