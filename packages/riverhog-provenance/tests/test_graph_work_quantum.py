from __future__ import annotations

import sqlite3
from contextlib import closing

import pytest
import test_generic_semantics as generic
import test_row_semantics as rows
import test_semantic_edge as edge
from native_units import unit_budget
from riverhog_provenance._native_epoch import EpochContext
from riverhog_provenance._native_work import NativeWorkCapacity, WorkAmount
from riverhog_provenance._online_dag import OnlineDag, install
from riverhog_provenance.journal_session import (
    JournalValidationSession,
    JournalValidationWorkBudget,
)
from test_native_epoch_integration import _reach
from test_online_dag import invariants, rich_graph, vertex
from test_retained_journal_session import OwnedSource, directory, open_session
from test_semantic_work import quantum


def graph(kind="derivation"):
    db = sqlite3.connect(":memory:")
    install(db)
    epoch = EpochContext(db)
    dag = OnlineDag(db, kind, "original", epoch=epoch)
    dag.bind()
    return db, dag


def apply_graph(dag, plan):
    return dag.epoch.apply(
        lambda _active: dag._apply(plan),
        receive=lambda _value: None,
        admission=dag._application_admission(plan),
    )


@pytest.mark.parametrize("kind", ["derivation", "specialization"])
@pytest.mark.parametrize("cycle", [False, True])
def test_actual_graph_repair_and_cycle_undo_share_one_original_quantum(kind, cycle):
    db, dag = graph(kind)
    with closing(db):
        source, target, expected = rich_graph(dag, degree=3, cycle=cycle)
        original, phases = quantum(), set()
        with dag.epoch.execution(None, work=original):
            assert apply_graph(dag, dag._prepare_start("new-edge", source, target))
            for _ in range(256):
                before = dag._load()
                phases.add(before.work.phase)
                if before.work.phase == "complete":
                    break
                plan = dag._prepare(2)
                spent = original.consumed.rows
                assert apply_graph(dag, plan)
                assert original.consumed.rows - spent == 1 + len(plan.writes)
                assert dag.epoch.quiescent and original.held == WorkAmount()
            else:
                pytest.fail("small original graph did not finish")
            assert dag.result("new-edge") == ("cycle" if cycle else "accepted")
        assert original.consumed.rows > 0 and {"initialize", "complete"} <= phases
        if cycle:
            assert "undo" in phases
        else:
            expected.add((source, target))
        invariants(dag, expected)
        original.require_received()


@pytest.mark.parametrize("kind", ["derivation", "specialization"])
def test_graph_replacement_refuses_before_begin_and_keeps_original_owner_for_retirement(kind):
    db, dag = graph(kind)
    with closing(db):
        dag.add(vertex(1), vertex(2))
        batch = dag._prepare_replacement("replacement")
        envelope, trace = dag._application_admission(batch), []
        refused = quantum(envelope.rows - 1)
        with dag.epoch.execution(None, work=refused):
            db.set_trace_callback(trace.append)
            try:
                with pytest.raises(NativeWorkCapacity):
                    dag.epoch.apply(
                        lambda _active: dag._apply_replacement(batch),
                        receive=lambda _value: None,
                        admission=envelope,
                    )
            finally:
                db.set_trace_callback(None)
        assert trace == [] and refused.consumed == refused.held == WorkAmount()
        original = quantum()
        with dag.epoch.execution(None, work=original):
            replacement = dag.epoch.apply(
                lambda _active: dag._apply_replacement(batch),
                receive=lambda _value: None,
                admission=envelope,
            )
            assert replacement.epoch is dag.epoch and original.consumed == envelope
            for _ in range(128):
                retired = replacement._prepare_retirement(2, byte_budget=2**20)
                if retired is None:
                    break
                spent = original.consumed.rows
                replacement.epoch.apply(
                    lambda _active, proposal=retired: replacement._apply_retirement(proposal),
                    receive=lambda _value: None,
                    admission=replacement._application_admission(retired),
                )
                assert original.consumed.rows - spent == 1 + len(retired.plan.writes)
                assert original.held == WorkAmount() and replacement.epoch.quiescent
            else:
                pytest.fail("small original retired graph did not finish")
        assert db.execute("SELECT count(*) FROM dag_retired").fetchone() == (0,)
        assert db.execute("SELECT run_id FROM dag_runs WHERE kind=?", (kind,)).fetchone() == (
            "replacement",
        )
        assert db.execute("SELECT count(*) FROM dag_edges").fetchone() == (0,)
        original.require_received()
        refused.require_received()


def test_graph_range_refuses_before_query_and_consumes_actual_rows_on_short_and_empty_pages():
    db, dag = graph()
    with closing(db):
        for n in range(3):
            dag.add(vertex(n), vertex(n + 1))
        sql = "SELECT source,target FROM dag_edges WHERE kind=? AND run_id=? LIMIT ?"
        original, trace = quantum(3), []
        with dag.epoch.execution(None, work=original):
            db.set_trace_callback(trace.append)
            try:
                with pytest.raises(NativeWorkCapacity):
                    dag._rows(sql, (dag.kind, dag.run_id, 3), limit=3)
            finally:
                db.set_trace_callback(None)
        assert trace == [] and original.consumed == original.held == WorkAmount()
        short = quantum(6)
        with dag.epoch.execution(None, work=short):
            assert len(dag._rows(sql, (dag.kind, dag.run_id, 5), limit=5)) == 3
        assert short.consumed == WorkAmount(rows=4) and short.remaining.rows == 2
        empty = quantum(6)
        with dag.epoch.execution(None, work=empty):
            assert dag._rows(sql, (dag.kind, "missing", 5), limit=5) == []
        assert empty.consumed == WorkAmount(rows=1) and empty.remaining.rows == 5
        for received in (original, short, empty):
            received.require_received()


def test_original_graph_range_failure_keeps_probe_cost_and_receives_unused_page():
    db, dag = graph()
    with closing(db):
        original = quantum(4)
        with dag.epoch.execution(None, work=original):
            with pytest.raises(sqlite3.OperationalError):
                dag._rows("SELECT missing FROM nonexistent LIMIT ?", (3,), limit=3)
        assert original.consumed == WorkAmount(rows=1) and original.held == WorkAmount()
        assert dag.epoch.quiescent
        original.require_received()


@pytest.mark.parametrize("kind", ["derivation", "specialization"])
def test_semantic_edge_admission_reserves_all_original_fences_and_receives_child(tmp_path, kind):
    path = tmp_path / "edge.sqlite3"
    edge.create(path, kind)
    with closing(edge.connect(path)) as db:
        current = edge.work(db, kind=kind, epoch=EpochContext(db))
        assert current.checker.epoch is current.epoch
        batch = current.prepare_admission()
        original, trace = quantum(4), []
        with current.epoch.execution(None, work=original):
            db.set_trace_callback(trace.append)
            try:
                with pytest.raises(NativeWorkCapacity):
                    current.epoch.apply(
                        lambda _active: current._admit(batch),
                        receive=current._control.receive,
                        admission=current._application_admission(batch),
                    )
            finally:
                db.set_trace_callback(None)
        assert trace == [] and original.consumed == original.held == WorkAmount()
        retry = quantum(5)
        with current.epoch.execution(None, work=retry):
            pending = current.epoch.apply(
                lambda _active: current._admit(batch),
                receive=current._control.receive,
                admission=current._application_admission(batch),
            )
        assert retry.consumed == WorkAmount(rows=5) and retry.held == WorkAmount()
        assert current._control.state is pending and current.epoch.quiescent
        original.require_received()
        retry.require_received()


@pytest.mark.parametrize("kind", ["derivation", "specialization"])
def test_actual_generic_edge_and_graph_share_original_quantum_reservation_and_receiving(
    tmp_path, catalog, kind
):
    path, row = tmp_path / "nested.sqlite3", rows.subject(kind)
    rows.prepare(path, row)
    with closing(rows.connect(path)) as db:
        current = rows.work(db, row, catalog, operation="row", epoch=EpochContext(db))
        original, phases, nested = quantum(), set(), []
        with current.epoch.execution(None, work=original):
            admission = current.prepare_admission()
            state = current.epoch.apply(
                lambda _active: current._admit(admission),
                receive=current._control.receive,
                admission=current._application_admission(admission),
            )
            for _ in range(1024):
                phases.add(state.phase)
                if state.done:
                    break
                batch = current.prepare(step_budget=1)

                def receive(pending, proposal=batch):
                    current.epoch.require_receiving(db)
                    assert current.epoch.work is original
                    current._control.receive(pending)
                    current._receive_edge(proposal)

                state = current.epoch.apply(
                    lambda _active, proposal=batch: current._apply(proposal),
                    receive=receive,
                    admission=current._application_admission(batch),
                )
                if batch.edge is not None:
                    child = current._edge
                    nested.append(child)
                    assert child.epoch is child.checker.epoch is current.epoch
                    assert child._control.state is batch.edge.after
                assert current.epoch.quiescent and original.held == WorkAmount()
            else:
                pytest.fail("small nested semantic graph did not finish")
            current.result()
        assert {"walk", "edge", "keys", "outputs", "done"} <= phases
        assert nested and all(child is nested[0] for child in nested)
        assert original.consumed.rows > 8
        rows.require_reclaimed(db)
        original.require_received()


def test_generic_prepare_capacity_keeps_actual_probe_cost_without_sql_writer(tmp_path, catalog):
    row, path = generic.subject(3), tmp_path / "generic.sqlite3"
    generic.create(path, row)
    with closing(generic.connect(path)) as db:
        current = generic.work(db, row, catalog, epoch=EpochContext(db))
        before = current.initialize()
        original, trace = quantum(3), []
        with current.epoch.execution(None, work=original):
            db.set_trace_callback(trace.append)
            try:
                with pytest.raises(NativeWorkCapacity):
                    current.prepare(step_budget=1)
            finally:
                db.set_trace_callback(None)
        assert len(trace) == 3 and all(sql.startswith("SELECT") for sql in trace)
        assert original.consumed == WorkAmount(rows=3) and original.held == WorkAmount()
        assert current.load() == before and current.epoch.quiescent
        retry = quantum()
        with current.epoch.execution(None, work=retry):
            batch = current.prepare(step_budget=1)
        assert batch.before == before and batch.after != before
        assert retry.consumed.rows > 3
        original.require_received()
        retry.require_received()


def test_original_generic_mutation_failure_retains_fence_and_write_cost_after_rollback(
    tmp_path, catalog, monkeypatch
):
    row, path = generic.subject(3), tmp_path / "generic.sqlite3"
    generic.create(path, row)
    with closing(generic.connect(path)) as db:
        current = generic.work(db, row, catalog, epoch=EpochContext(db))
        current.initialize()
        batch = current.prepare(step_budget=1)
        before = tuple(db.iterdump())
        original, write = quantum(), current._write
        failure = OSError("original generic mutation response failed")

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
        assert original.consumed == WorkAmount(rows=3) and original.held == WorkAmount()
        with pytest.raises(RuntimeError, match="requires reconstruction"):
            current.epoch.require_usable()
        original.require_received()


def test_generic_cleanup_accounts_for_fetched_sentinel_before_any_prepared_write(tmp_path, catalog):
    row, path = generic.subject(17), tmp_path / "generic.sqlite3"
    generic.create(path, row)
    with closing(generic.connect(path)) as db:
        current = generic.work(db, row, catalog, epoch=EpochContext(db))
        current.initialize()
        generic.position(current, "keys")
        before, original, trace = current.load(), quantum(6), []
        with current.epoch.execution(None, work=original):
            db.set_trace_callback(trace.append)
            try:
                with pytest.raises(NativeWorkCapacity):
                    current.prepare(step_budget=2)
            finally:
                db.set_trace_callback(None)
        assert len(trace) == 3 and original.consumed == WorkAmount(rows=3)
        assert current.load() == before and original.held == WorkAmount()
        retry = quantum(7)
        with current.epoch.execution(None, work=retry):
            batch = current.prepare(step_budget=2)
        assert len(batch.writes) == 2 and retry.consumed == WorkAmount(rows=7)
        assert current.load() == before and retry.held == WorkAmount()
        original.require_received()
        retry.require_received()


@pytest.mark.parametrize("retained", [False, True])
def test_actual_native_generic_refusal_preserves_child_then_retries(
    tmp_path, journal, catalog, journal_constructions, retained, monkeypatch
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
        _reach(session, "generic")
        current = session._replay._current.child
        before, trace, active = session.progress(), [], []
        db, one = session._store.db, current._one
        changes = db.total_changes

        def probe(sql, parameters=()):
            active.append(current.epoch.work)
            return one(sql, parameters)

        parent_probes = 2 * (2 + bool(session._ledger.load().accepted.frame_count))
        monkeypatch.setattr(current, "_one", probe)
        db.set_trace_callback(trace.append)
        try:
            with pytest.raises(NativeWorkCapacity):
                session.advance(
                    unit_budget(rows=1, work=JournalValidationWorkBudget(rows=parent_probes + 1))
                )
        finally:
            db.set_trace_callback(None)
        assert not any(sql.startswith("BEGIN") for sql in trace)
        assert session.progress() == before and db.total_changes == changes
        refused = active[0]
        assert refused.consumed.rows == parent_probes + 1 and refused.held == WorkAmount()
        active.clear()
        assert session.advance(unit_budget(rows=1)) != before
        assert session._replay._current.child is current and active[0] is not refused
        assert current.epoch.quiescent and active[0].held == WorkAmount()
        refused.require_received()
        active[0].require_received()
    assert session.storage_received
