from __future__ import annotations

import copy
import sqlite3
from contextlib import closing

import pytest
from riverhog_provenance import append_correction, assertion_reference, new_id, validate_journal
from riverhog_provenance._native_epoch import EpochContext
from riverhog_provenance._native_work import NativeWorkCapacity, WorkAmount
from riverhog_provenance.journal_session import (
    JournalValidationBudget,
    JournalValidationSession,
    JournalValidationWorkBudget,
)
from test_correction_projection import TABLES, assertions, connect, create, fill, work
from test_native_epoch_integration import _reach
from test_retained_journal_session import OwnedSource, directory, open_session
from test_semantic_work import quantum


def setup(path, operation):
    create(path)
    db = connect(path)
    accepted, _retired = assertions(db, 5, 17)
    for table in TABLES:
        fill(db, table, 5)
    # An already queued active identity must still incur the attempted seed
    # write even when that write changes no row.
    db.execute("INSERT INTO changes VALUES (?)", (min(accepted),)).close()
    db.commit()
    current = work(db, operation=operation, epoch=EpochContext(db))
    return db, current


@pytest.mark.parametrize("operation", ["retire", "seed"])
def test_original_projection_application_reserves_before_begin_and_can_retry_same_owner(
    tmp_path, operation
):
    db, current = setup(tmp_path / "projection.sqlite3", operation)
    with closing(db):
        current.initialize()
        batch = current.prepare(row_budget=2)
        assert len(batch.keys) == 2
        envelope, trace = current._application_admission(batch), []
        original = quantum(envelope.rows - 1)
        with current.epoch.execution(None, work=original):
            db.set_trace_callback(trace.append)
            try:
                with pytest.raises(NativeWorkCapacity):
                    current.epoch.apply(
                        lambda _active: current._apply(batch),
                        receive=current._control.receive,
                        admission=envelope,
                    )
            finally:
                db.set_trace_callback(None)
        assert trace == [] and original.consumed == original.held == WorkAmount()
        retry = quantum(envelope.rows)
        with current.epoch.execution(None, work=retry):
            after = current.epoch.apply(
                lambda _active: current._apply(batch),
                receive=current._control.receive,
                admission=envelope,
            )
        assert retry.consumed == envelope and retry.held == WorkAmount()
        assert current._control.get(after.to_json()) is after
        assert current.epoch.quiescent
        original.require_received()
        retry.require_received()


@pytest.mark.parametrize("operation", ["retire", "seed"])
def test_every_projection_phase_charges_actual_attempts_and_ranges_on_one_quantum(
    tmp_path, operation
):
    db, current = setup(tmp_path / "projection.sqlite3", operation)
    with closing(db):
        original, phases, trace = quantum(), set(), []
        with current.epoch.execution(None, work=original):
            admission = current.prepare_admission()
            current.epoch.apply(
                lambda _active: current._admit(admission),
                receive=current._control.receive,
                admission=current._application_admission(admission),
            )
            admitted_cost = original.consumed.rows
            assert admitted_cost == 3
            db.set_trace_callback(trace.append)
            try:
                for _ in range(128):
                    before = current.load()
                    phases.add(before.phase)
                    batch = current.prepare(row_budget=2)
                    after = current.epoch.apply(
                        lambda _active, proposal=batch: current._apply(proposal),
                        receive=current._control.receive,
                        admission=current._application_admission(batch),
                    )
                    assert original.held == WorkAmount() and current.epoch.quiescent
                    assert current._control.get(after.to_json()) is after
                    if before.done:
                        break
                else:
                    pytest.fail("small projection did not finish")
            finally:
                db.set_trace_callback(None)
        attempts = sum(sql.startswith(("SELECT", "INSERT", "UPDATE", "DELETE")) for sql in trace)
        assert original.consumed.rows - admitted_cost == attempts + (
            5 if operation == "seed" else 0
        )
        if operation == "retire":
            assert phases == {*TABLES, "done"}
            assert all(db.execute("SELECT count(*) FROM " + t).fetchone() == (0,) for t in TABLES)
            assert db.execute("SELECT count(*) FROM changes").fetchone() == (1,)
        else:
            assert phases == {"seed", "done"}
            assert db.execute("SELECT count(*) FROM changes").fetchone() == (5,)
        original.require_received()


def test_seed_range_reserves_before_query_and_returns_unused_capacity_on_short_page(tmp_path):
    db, current = setup(tmp_path / "projection.sqlite3", "seed")
    with closing(db):
        current.initialize()
        original, trace = quantum(3), []
        with current.epoch.execution(None, work=original):
            db.set_trace_callback(trace.append)
            try:
                with pytest.raises(NativeWorkCapacity):
                    current.prepare(row_budget=3)
            finally:
                db.set_trace_callback(None)
        assert len(trace) == 1 and trace[0].startswith("SELECT algorithm")
        assert original.consumed == WorkAmount(rows=1) and original.held == WorkAmount()
        assert current.epoch.quiescent
        retry = quantum(8)
        with current.epoch.execution(None, work=retry):
            batch = current.prepare(row_budget=6)
        assert len(batch.keys) == 5
        assert retry.consumed == WorkAmount(rows=7) and retry.remaining.rows == 1
        assert retry.held == WorkAmount()
        original.require_received()
        retry.require_received()


def test_failed_original_range_query_retains_probe_cost_and_receives_held_page(tmp_path):
    db, current = setup(tmp_path / "projection.sqlite3", "seed")
    with closing(db):
        original = quantum(4)
        with current.epoch.execution(None, work=original):
            with pytest.raises(sqlite3.OperationalError):
                current._many("SELECT missing FROM nonexistent LIMIT ?", (3,), limit=3)
        assert original.consumed == WorkAmount(rows=1) and original.held == WorkAmount()
        assert current.epoch.quiescent
        original.require_received()


@pytest.mark.parametrize("operation", ["retire", "seed"])
def test_original_projection_write_failure_keeps_consumption_after_rollback(
    tmp_path, operation, monkeypatch
):
    db, current = setup(tmp_path / "projection.sqlite3", operation)
    with closing(db):
        current.initialize()
        batch = current.prepare(row_budget=2)
        before = tuple(db.iterdump())
        original, write = quantum(), current._write
        failure = OSError("original projection mutation response failed")

        def fail(sql, parameters=()):
            cursor = write(sql, parameters)
            cursor.close()
            raise failure

        monkeypatch.setattr(current, "_write", fail)
        with pytest.raises(OSError) as caught:
            with current.epoch.execution(None, work=original):
                current.epoch.apply(
                    lambda _active: current._apply(batch),
                    receive=current._control.receive,
                    admission=current._application_admission(batch),
                )
        assert caught.value is failure and tuple(db.iterdump()) == before
        assert original.consumed == WorkAmount(rows=1) and original.held == WorkAmount()
        with pytest.raises(RuntimeError, match="requires reconstruction"):
            current.epoch.require_usable()
        original.require_received()


@pytest.mark.parametrize("retained", [False, True])
@pytest.mark.parametrize("phase", ["retire", "seed"])
def test_actual_native_correction_keeps_original_child_on_refusal_then_retries(
    tmp_path,
    journal,
    who,
    catalog,
    journal_constructions,
    retained,
    phase,
    monkeypatch,
):
    with validate_journal(journal, catalog=catalog) as summary:
        original = dict(summary.graph["agents"][0])
        target = assertion_reference(summary, original["assertion_id"])
    replacement = copy.deepcopy(original)
    replacement["assertion_id"] = new_id()
    replacement["name"] = "Changed label"
    raw = append_correction(
        journal,
        [target],
        reason="label",
        recorded_by_agent_id=who,
        assertions={"agents": [replacement]},
        catalog=catalog,
    )
    context = (
        open_session(
            directory(tmp_path),
            OwnedSource(raw, catalog),
            catalog,
            create=True,
            journal_constructions=journal_constructions,
        )
        if retained
        else JournalValidationSession((raw,), attempt_id="a" * 32, catalog=catalog)
    )
    with context as session:
        _reach(session, phase)
        child = session._replay._current.child
        assert child.operation == phase and child.epoch is session._ledger.epoch
        before, trace = session.progress(), []
        db, one, active = session._store.db, child._one, []
        changes = db.total_changes

        def probe(sql, parameters=()):
            active.append(child.epoch.work)
            return one(sql, parameters)

        monkeypatch.setattr(child, "_one", probe)
        db.set_trace_callback(trace.append)
        try:
            with pytest.raises(NativeWorkCapacity):
                session.advance(
                    JournalValidationBudget(rows=1, work=JournalValidationWorkBudget(rows=1))
                )
        finally:
            db.set_trace_callback(None)
        assert not any(sql.startswith("BEGIN") for sql in trace)
        assert session.progress() == before and db.total_changes == changes
        refused = active[0]
        assert refused.consumed.rows == 1 and refused.held == WorkAmount()
        active.clear()
        after = session.advance(JournalValidationBudget(rows=1))
        assert after != before and session._replay._current.child is child
        assert active[0] is not refused and active[0].consumed.rows > 1
        assert active[0].held == WorkAmount() and child.epoch.quiescent
        refused.require_received()
        active[0].require_received()
    assert session.storage_received
