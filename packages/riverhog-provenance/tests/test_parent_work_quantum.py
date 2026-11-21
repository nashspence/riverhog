from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import replace

import pytest
from riverhog_provenance._native_work import NativeWorkCapacity, WorkAmount
from riverhog_provenance._session_checkpoint import (
    StaleValidationCheckpoint,
    ValidationLedger,
)
from riverhog_provenance.journal_session import JournalValidationSession
from test_native_epoch_integration import _reach
from test_preparation_work import quantum
from test_validation_checkpoint import ATTEMPT, INPUTS, connect, install, install_prefix, prefix


def accepted_prefix(session, accepted):
    if accepted:
        _reach(session, "publish")
        assert session.advance().accepted_frames == 1
    return session._ledger.load()


@pytest.mark.parametrize("accepted", [False, True])
def test_fresh_parent_probe_attempts_are_charged_before_sql_and_retry_same_owner(
    journal, catalog, accepted
):
    with JournalValidationSession((journal,), attempt_id=ATTEMPT, catalog=catalog) as session:
        before = accepted_prefix(session, accepted)
        parent, db, trace = session._ledger, session._store.db, []
        probes = 2 + accepted
        refused = quantum(probes - 1)
        db.set_trace_callback(trace.append)
        try:
            with parent.epoch.execution(None, work=refused):
                with pytest.raises(NativeWorkCapacity):
                    parent.load()
        finally:
            db.set_trace_callback(None)
        assert len(trace) == refused.consumed.rows == probes - 1
        assert all(sql.startswith("SELECT") for sql in trace)
        assert parent.epoch.quiescent and refused.held == WorkAmount()
        assert parent.load() is before
        original = quantum(2 * probes)
        with parent.epoch.execution(None, work=original):
            assert parent.load() is before and parent.load() is before
        assert original.consumed == WorkAmount(rows=2 * probes)
        refused.require_received()
        original.require_received()
    assert session.storage_received


@pytest.mark.parametrize("accepted", [False, True])
def test_original_parent_application_reserves_before_begin_and_receives_unused_work(
    journal, catalog, accepted, monkeypatch
):
    with JournalValidationSession((journal,), attempt_id=ATTEMPT, catalog=catalog) as session:
        before = accepted_prefix(session, accepted)
        parent, db, trace = session._ledger, session._store.db, []
        envelope = parent._application_admission()
        assert envelope == WorkAmount(rows=6)
        refused = quantum(5)
        changes = db.total_changes
        db.set_trace_callback(trace.append)
        try:
            with parent.epoch.execution(None, work=refused):
                with pytest.raises(NativeWorkCapacity):
                    session._move(before, "terminal")
        finally:
            db.set_trace_callback(None)
        assert trace == [] and db.total_changes == changes
        assert parent.load() is before and parent.epoch.quiescent
        assert refused.consumed == refused.held == WorkAmount()
        original, receiving = quantum(6), []
        receive = ValidationLedger._receive

        def received(owner, pending):
            assert owner is parent and owner.epoch.work is original
            assert not db.in_transaction and not owner.epoch.quiescent
            assert owner._control.state is before
            assert original.consumed == WorkAmount(rows=4 + 2 * accepted)
            assert original.held == WorkAmount(rows=2 - 2 * accepted)
            receiving.append(pending)
            receive(owner, pending)

        monkeypatch.setattr(ValidationLedger, "_receive", received)
        with parent.epoch.execution(None, work=original):
            after = session._move(before, "terminal")
        assert receiving == [after] and parent.load() is after
        assert original.held == WorkAmount() and parent.epoch.quiescent
        refused.require_received()
        original.require_received()
    assert session.storage_received


@pytest.mark.parametrize("accepted", [False, True])
def test_successful_parent_commits_do_not_replenish_the_original_quantum(
    journal, catalog, accepted
):
    with JournalValidationSession((journal,), attempt_id=ATTEMPT, catalog=catalog) as session:
        before = accepted_prefix(session, accepted)
        parent, db, trace = session._ledger, session._store.db, []
        original = quantum(12)
        with parent.epoch.execution(None, work=original):
            first = session._move(before, "terminal")
            second = session._move(first, "done")
            assert original.consumed == WorkAmount(rows=2 * (4 + 2 * accepted))
            db.set_trace_callback(trace.append)
            try:
                with pytest.raises(NativeWorkCapacity):
                    session._move(second, "done")
            finally:
                db.set_trace_callback(None)
            assert trace == [] and original.held == WorkAmount()
            assert parent.epoch.quiescent
        assert parent.load() is second
        original.require_received()
    assert session.storage_received


@pytest.mark.parametrize("accepted", [False, True])
def test_entry_conductor_adds_parent_and_mutation_admission_before_original_writer(
    journal, catalog, accepted
):
    with JournalValidationSession((journal,), attempt_id=ATTEMPT, catalog=catalog) as session:
        before = accepted_prefix(session, accepted)
        parent, db, trace = session._ledger, session._store.db, []
        db.execute("CREATE TABLE admitted_mutations(value INTEGER)").close()
        db.commit()
        after = replace(before.checkpoint, version=before.checkpoint.version + 1)
        calls = []

        def mutate():
            calls.append(parent.epoch.work)
            for index in range(3):
                parent.epoch.account(WorkAmount(rows=1))
                db.execute("INSERT INTO admitted_mutations VALUES(?)", (index,)).close()
            return after

        refused = quantum(8)
        db.set_trace_callback(trace.append)
        try:
            with parent.epoch.execution(None, work=refused):
                with pytest.raises(NativeWorkCapacity):
                    session._replay._commit(before, mutate, admission=WorkAmount(rows=3))
        finally:
            db.set_trace_callback(None)
        assert trace == calls == [] and parent.load() is before
        original = quantum(9)
        with parent.epoch.execution(None, work=original):
            pending = session._replay._commit(before, mutate, admission=WorkAmount(rows=3))
        assert calls == [original] and pending.checkpoint is after
        assert db.execute("SELECT value FROM admitted_mutations ORDER BY value").fetchall() == [
            (0,),
            (1,),
            (2,),
        ]
        assert original.consumed == WorkAmount(rows=7 + 2 * accepted)
        assert original.held == WorkAmount() and parent.load() is pending
        refused.require_received()
        original.require_received()
    assert session.storage_received


def test_failed_parent_probe_retains_actual_attempt_without_poisoning_detached_executor(
    journal, catalog
):
    with JournalValidationSession((journal,), attempt_id=ATTEMPT, catalog=catalog) as session:
        parent, original = session._ledger, quantum(1)
        before = parent.load()
        with parent.epoch.execution(None, work=original):
            with pytest.raises(sqlite3.OperationalError, match="no such table"):
                parent._execute("SELECT value FROM missing_parent_fixture")
        assert original.consumed == WorkAmount(rows=1) and original.held == WorkAmount()
        assert parent.epoch.quiescent and parent.load() is before
        original.require_received()
    assert session.storage_received


@pytest.mark.parametrize("accepted", [False, True])
def test_failed_parent_mutation_keeps_attempt_cost_and_invalidates_original_executor(
    journal, catalog, accepted
):
    with JournalValidationSession((journal,), attempt_id=ATTEMPT, catalog=catalog) as session:
        before = accepted_prefix(session, accepted)
        parent, db, original = session._ledger, session._store.db, quantum(6)

        def deny_parent_update(action, table, *_rest):
            return (
                sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_UPDATE and table == "journal_validation"
                else sqlite3.SQLITE_OK
            )

        db.set_authorizer(deny_parent_update)
        try:
            with pytest.raises(sqlite3.DatabaseError):
                with parent.epoch.execution(None, work=original):
                    session._move(before, "terminal")
        finally:
            db.set_authorizer(None)
        assert original.consumed == WorkAmount(rows=4 + 2 * accepted)
        assert original.held == WorkAmount() and not db.in_transaction
        assert parent._control.state is before
        assert db.execute("SELECT checkpoint FROM journal_validation").fetchone() == (
            before.checkpoint.to_json(),
        )
        with pytest.raises(RuntimeError, match="requires reconstruction"):
            parent.epoch.require_usable()
        original.require_received()
    assert session.storage_received


def test_parent_initialization_and_worker_replacement_reserve_their_original_work(tmp_path):
    with closing(connect(tmp_path / "parent-work.sqlite3")) as db:
        install_prefix(db)
        install(db)
        prefix(db).initialize()
        parent = ValidationLedger(db, attempt_id=ATTEMPT, binding=INPUTS, worker_epoch=2)
        trace = []
        refused = quantum(1)
        db.set_trace_callback(trace.append)
        try:
            with parent.epoch.execution(None, work=refused):
                with pytest.raises(NativeWorkCapacity):
                    parent.initialize()
        finally:
            db.set_trace_callback(None)
        assert trace == [] and refused.consumed == refused.held == WorkAmount()
        initial = quantum(2)
        with parent.epoch.execution(None, work=initial):
            before = parent.initialize()
        assert initial.consumed == WorkAmount(rows=2)
        trace.clear()
        refused_worker = quantum(5)
        db.set_trace_callback(trace.append)
        try:
            with parent.epoch.execution(None, work=refused_worker):
                with pytest.raises(NativeWorkCapacity):
                    parent.replace_worker(3)
        finally:
            db.set_trace_callback(None)
        assert len(trace) == refused_worker.consumed.rows == 2
        assert not any(sql.startswith("BEGIN") for sql in trace)
        assert parent.load() is before
        admitted = quantum(6)
        with parent.epoch.execution(None, work=admitted):
            replacement = parent.replace_worker(3)
        assert replacement.epoch is parent.epoch and replacement.worker_epoch == 3
        assert admitted.consumed == WorkAmount(rows=5) and admitted.held == WorkAmount()
        assert replacement.load().checkpoint.version == before.checkpoint.version + 1
        with pytest.raises(StaleValidationCheckpoint, match="worker changed"):
            parent.load()
        for work in (refused, initial, refused_worker, admitted):
            work.require_received()


def test_actual_frame_publication_reserves_new_prefix_predecessor_and_receives_it(
    journal, catalog, monkeypatch
):
    with JournalValidationSession((journal,), attempt_id=ATTEMPT, catalog=catalog) as session:
        _reach(session, "publish")
        parent = session._ledger
        assert parent.load().accepted.frame_count == 0
        receive, seen = ValidationLedger._receive, []

        def received(owner, pending):
            work = owner.epoch.work
            assert owner is parent and work is not None
            assert pending.accepted.frame_count == 1 and not owner.db.in_transaction
            assert not owner.epoch.quiescent
            assert work.consumed.rows == 12 and work.held.rows == 1
            seen.append(work)
            receive(owner, pending)

        monkeypatch.setattr(ValidationLedger, "_receive", received)
        assert session.advance().accepted_frames == 1
        assert len(seen) == 1 and seen[0].held == WorkAmount()
        assert parent.epoch.quiescent and parent.load().accepted.frame_count == 1
        seen[0].require_received()
    assert session.storage_received
