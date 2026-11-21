from __future__ import annotations

import sqlite3
from contextlib import closing

import pytest
from riverhog_provenance._correction_targets import CorrectionTargets
from riverhog_provenance._native_epoch import EpochContext
from riverhog_provenance._native_work import NativeWorkCapacity, WorkAmount, WorkQuantum
from riverhog_provenance.journal_session import (
    JournalValidationBudget,
    JournalValidationSession,
    JournalValidationWorkBudget,
)
from test_assertion_staging import connect, create, frame, identity, row, work
from test_correction_targets import create as create_targets
from test_native_epoch_integration import _reach
from test_retained_journal_session import OwnedSource, directory, open_session
from test_semantic_finish import assertion, external, lifecycle
from test_semantic_finish import create as create_finish
from test_semantic_finish import work as finish_work


def quantum(rows=8192):
    return WorkQuantum(WorkAmount(rows=rows, octets=2**24, source_octets=2**20, transitions=64))


def setup(path, kind):
    if kind == "targets":
        source = create_targets(path, count=5, history=17)
    elif kind == "finish":
        create_finish(path)
    else:
        create(path)
        source = frame(
            [
                {
                    **row(1),
                    "type": "activity",
                    "kind": "transformation",
                    "associations": [{"agent_id": identity(200 + n)} for n in range(5)],
                }
            ]
        )
    db = connect(path)
    epoch = EpochContext(db)
    if kind == "targets":
        current = CorrectionTargets(db, attempt_id="targets", frame=source, epoch=epoch)
    elif kind == "finish":
        state = lifecycle(db)
        assertion(db, 1, "generation", state=state)
        for n in range(5):
            assertion(
                db,
                100 + n,
                "usage",
                changed=False,
                state=state,
                at="2025-01-01T00:00:00Z",
            )
        db.execute(
            "INSERT INTO lifecycle_extrema VALUES (?,?,?)", (state["object_id"], "0", None)
        ).close()
        description = assertion(
            db,
            2,
            "reported_description",
            state=state,
            content={"size_bytes": "4", "digests": [{"algorithm": "sha-256", "value": "b" * 64}]},
        )
        for n in range(5):
            external(db, description, n)
        db.execute(
            "INSERT INTO fixities VALUES (?,?,?)", (state["object_id"], "4", "a" * 64)
        ).close()
        db.commit()
        current = finish_work(db, epoch=epoch)
    else:
        current = work(db, source, epoch=epoch)
    return db, current


@pytest.mark.parametrize("kind", ["targets", "staging", "finish"])
def test_original_child_admission_refuses_before_writer_then_retries_without_new_owner(
    tmp_path, kind
):
    db, current = setup(tmp_path / "child.sqlite3", kind)
    with closing(db):
        epoch = current.epoch
        batch = current.prepare_admission()
        envelope = current._application_admission(batch)
        refused, trace = quantum(envelope.rows - 1), []
        with epoch.execution(None, work=refused):
            db.set_trace_callback(trace.append)
            try:
                with pytest.raises(NativeWorkCapacity):
                    epoch.apply(
                        lambda _active: current._admit(batch),
                        receive=current._control.receive,
                        admission=envelope,
                    )
            finally:
                db.set_trace_callback(None)
            assert trace == [] and epoch.quiescent
            assert refused.consumed == refused.held == WorkAmount()
        original = quantum(envelope.rows)
        with epoch.execution(None, work=original):
            state = epoch.apply(
                lambda _active: current._admit(batch),
                receive=current._control.receive,
                admission=envelope,
            )
        assert original.consumed == envelope and original.held == WorkAmount()
        assert current._control.get(state.to_json()) is state
        assert epoch.quiescent
        original.require_received()
        refused.require_received()


@pytest.mark.parametrize("kind", ["targets", "staging", "finish"])
def test_all_real_child_phases_share_one_quantum_and_receive_each_original_application(
    tmp_path, kind
):
    db, current = setup(tmp_path / "child.sqlite3", kind)
    with closing(db):
        current.initialize()
        original, phases, trace = quantum(), set(), []
        with current.epoch.execution(None, work=original):
            db.set_trace_callback(trace.append)
            try:
                for _ in range(128):
                    before = current.load()
                    phases.add(before.phase)
                    batch = current.prepare(row_budget=2, byte_budget=2**20)
                    envelope = current._application_admission(batch)
                    consumed = original.consumed.rows
                    state = current.epoch.apply(
                        lambda _active, proposal=batch: current._apply(proposal),
                        receive=current._control.receive,
                        admission=envelope,
                    )
                    assert 0 < original.consumed.rows - consumed <= envelope.rows
                    assert original.held == WorkAmount() and current.epoch.quiescent
                    assert current._control.get(state.to_json()) is state
                    if before.done:
                        break
                else:
                    pytest.fail("small real child did not finish")
            finally:
                db.set_trace_callback(None)
        assert {"done"} <= phases
        assert (
            {"verify", "apply", "clean"}
            if kind == "targets"
            else {"stage", "observers"}
            if kind == "staging"
            else {"row", "lifecycle", "invalidation", "usage", "content", "externals"}
        ) <= phases
        attempts = sum(sql.startswith(("SELECT", "INSERT", "UPDATE", "DELETE")) for sql in trace)
        # Correction additionally inspects immutable target rows; finishing
        # inspects the bounded fixity rows after its indexed range query.
        assert original.consumed.rows >= attempts > 0
        if kind == "staging":
            assert original.consumed.rows == attempts
            assert db.execute("SELECT count(*) FROM observation_observers").fetchone() == (5,)
        elif kind == "targets":
            assert db.execute("SELECT count(*) FROM correction_targets").fetchone() == (0,)
            assert db.execute("SELECT count(*) FROM assertions WHERE active=1").fetchone() == (0,)
        else:
            assert db.execute("SELECT count(*) FROM findings").fetchone() == (1,)
        original.require_received()


@pytest.mark.parametrize("kind", ["targets", "staging", "finish"])
def test_probe_miss_and_execution_failure_are_charged_before_sql_and_capacity_stops_next_probe(
    tmp_path, kind
):
    db, current = setup(tmp_path / "child.sqlite3", kind)
    with closing(db):
        original, trace = quantum(1), []
        with current.epoch.execution(None, work=original):
            db.set_trace_callback(trace.append)
            try:
                assert current._one("SELECT 1 WHERE 0") is None
                with pytest.raises(NativeWorkCapacity):
                    current._one("SELECT 2")
            finally:
                db.set_trace_callback(None)
        assert trace == ["SELECT 1 WHERE 0"]
        assert original.consumed == WorkAmount(rows=1) and current.epoch.quiescent
        failing = quantum(1)
        with current.epoch.execution(None, work=failing):
            with pytest.raises(sqlite3.OperationalError):
                current._one("SELECT absent FROM nonexistent")
        assert failing.consumed == WorkAmount(rows=1) and current.epoch.quiescent
        original.require_received()
        failing.require_received()


@pytest.mark.parametrize("kind", ["targets", "staging", "finish"])
def test_actual_mutation_work_survives_rollback_and_original_executor_is_discarded(
    tmp_path, kind, monkeypatch
):
    db, current = setup(tmp_path / "child.sqlite3", kind)
    with closing(db):
        current.initialize()
        batch = current.prepare(row_budget=2, byte_budget=2**20)
        before = db.iterdump()
        before = tuple(before)
        original = quantum()
        write = current._write
        failure = OSError("original row mutation response failed")

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
        assert caught.value is failure
        assert tuple(db.iterdump()) == before
        assert original.consumed == WorkAmount(rows=1) and original.held == WorkAmount()
        with pytest.raises(RuntimeError, match="requires reconstruction"):
            current.epoch.require_usable()
        original.require_received()


@pytest.mark.parametrize("retained", [False, True])
@pytest.mark.parametrize("phase", ["staging", "finish"])
def test_actual_native_session_preserves_progress_on_capacity_and_retries_same_child(
    tmp_path,
    journal,
    catalog,
    journal_constructions,
    retained,
    phase,
    monkeypatch,
):
    context = (
        open_session(
            directory(tmp_path),
            OwnedSource(journal, catalog),
            catalog,
            create=True,
            journal_constructions=journal_constructions,
        )
        if retained
        else JournalValidationSession((journal,), attempt_id="a" * 32, catalog=catalog)
    )
    with context as session:
        _reach(session, phase)
        child = session._replay._current.child
        assert child.epoch is session._ledger.epoch
        before, trace = session.progress(), []
        db = session._store.db
        changes = db.total_changes
        original = child._one
        admissions = []

        def one(sql, parameters=()):
            admissions.append(child.epoch.work)
            return original(sql, parameters)

        parent_probes = 2 * (2 + bool(session._ledger.load().accepted.frame_count))
        monkeypatch.setattr(child, "_one", one)
        db.set_trace_callback(trace.append)
        try:
            with pytest.raises(NativeWorkCapacity):
                session.advance(
                    JournalValidationBudget(
                        rows=1, work=JournalValidationWorkBudget(rows=parent_probes + 1)
                    )
                )
        finally:
            db.set_trace_callback(None)
        assert not any(sql.startswith("BEGIN") for sql in trace)
        assert session.progress() == before and db.total_changes == changes
        assert session._failure is None and child.epoch.quiescent
        refused = admissions[0]
        assert refused.consumed.rows == parent_probes + 1 and refused.held == WorkAmount()
        admissions.clear()
        after = session.advance(JournalValidationBudget(rows=1))
        assert after != before and session._replay._current.child is child
        assert admissions[0] is not refused and admissions[0].consumed.rows > 1
        assert child.epoch.quiescent and admissions[0].held == WorkAmount()
        refused.require_received()
        admissions[0].require_received()
    assert session.storage_received
