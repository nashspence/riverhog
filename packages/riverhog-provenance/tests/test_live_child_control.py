from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import replace

import pytest
import test_assertion_staging as staging
import test_correction_projection as projection
import test_correction_targets as targets
import test_entry_preparation as preparation
import test_generic_semantics as generic
import test_row_semantics as rows
import test_semantic_edge as edge
import test_semantic_finish as finish
from native_units import unit_budget
from riverhog_provenance._native_conductor import ControlSlot
from riverhog_provenance._native_control import ChildControl
from riverhog_provenance.journal_session import JournalValidationSession
from test_journal_session import finish as finish_session
from test_native_epoch_integration import _Connection, _reach

FAMILIES = {
    "preparation": (preparation, "journal_entry_preparation"),
    "staging": (staging, "journal_assertion_staging"),
    "targets": (targets, "journal_correction_targets"),
    "retirement": (projection, "journal_correction_work"),
    "generic": (generic, "journal_generic_semantics"),
    "finish": (finish, "journal_semantic_finish"),
    "edge": (edge, "journal_semantic_edges"),
}


def work(path, catalog, kind):
    module, _table = FAMILIES[kind]
    if kind == "generic":
        source = generic.subject(1)
        generic.create(path, source)
    else:
        source = module.create(path)
    db = module.connect(path)
    try:
        if kind == "preparation":
            child = preparation.work(db, preparation.frame())
        elif kind == "staging":
            child = staging.work(db, staging.frame((staging.row(0),)))
        elif kind == "targets":
            child = targets.work(db, source)
        elif kind == "generic":
            child = generic.work(db, source, catalog)
        else:
            child = module.work(db)
    except BaseException:
        db.close()
        raise
    return db, child


def admit(child):
    batch = child.prepare_admission()
    return child.epoch.apply(lambda _epoch: child._admit(batch), receive=child._control.receive)


def apply_generic(child, batch):
    def receive(pending):
        child._control.receive(pending)
        child._receive_edge(batch)

    return child.epoch.apply(lambda _epoch: child._apply(batch), receive=receive)


def refuse_decode(*_args):
    raise AssertionError("received typed child control was decoded again")


@pytest.mark.parametrize("kind", FAMILIES)
def test_all_original_children_prepare_apply_and_load_the_same_received_typed_control(
    tmp_path, catalog, monkeypatch, kind
):
    db, child = work(tmp_path / "child.sqlite3", catalog, kind)
    with closing(db):
        before = admit(child)
        assert child._control.state is before and child.epoch.quiescent
        monkeypatch.setattr(type(before), "from_json", refuse_decode)
        batch = child.prepare()
        assert batch.before is before
        after = child.epoch.apply(
            lambda _epoch: child._apply(batch), receive=child._control.receive
        )
        assert child.load() is after and child._control.state is after
        assert child.epoch.quiescent and not db.in_transaction
        with pytest.raises(RuntimeError, match="original post-commit"):
            child._control.receive(before)
        assert child._control.state is after


@pytest.mark.parametrize("kind", FAMILIES)
@pytest.mark.parametrize("column,value", [("algorithm", "foreign"), ("checkpoint", "{}")])
def test_typed_child_control_keeps_fresh_checkpoint_and_algorithm_checks(
    tmp_path, catalog, kind, column, value
):
    db, child = work(tmp_path / "child.sqlite3", catalog, kind)
    with closing(db):
        before = admit(child)
        table = FAMILIES[kind][1]
        db.execute("UPDATE " + table + " SET " + column + "=?", (value,)).close()
        db.commit()
        with pytest.raises(RuntimeError):
            child.load()
        assert child._control.state is before


@pytest.mark.parametrize("kind", ["entry", "staging", "generic", "finish"])
def test_actual_session_retains_its_child_control_across_warm_steps(
    journal, catalog, monkeypatch, kind
):
    budget = unit_budget(rows=1)
    with JournalValidationSession((journal,), attempt_id="a" * 32, catalog=catalog) as session:
        _reach(session, kind + "-admission")
        assert session.advance(budget).phase == kind
        child = session._replay._current.child
        received = child._control.state
        assert received is not None and child.epoch is session._ledger.epoch
        with monkeypatch.context() as patch:
            patch.setattr(type(received), "from_json", refuse_decode)
            for _ in range(4096):
                if session.advance(budget).phase != kind:
                    break
            else:
                pytest.fail("small Native child did not complete")
        assert finish_session(session, rows=1).complete
        with session.result() as summary:
            assert summary.journal_bytes == len(journal)
    assert session.storage_received


@pytest.mark.parametrize("failure", ["before", "after"])
def test_failed_original_commit_cannot_publish_pending_child_control(journal, catalog, failure):
    def connect(*args, **options):
        return sqlite3.connect(*args, factory=_Connection, **options)

    with JournalValidationSession(
        (journal,), attempt_id="a" * 32, catalog=catalog, connection_factory=connect
    ) as session:
        _reach(session, "entry")
        child = session._replay._current.child
        before = child._control.state
        assert before is not None
        db = session._store.db
        db.failure = failure
        with pytest.raises(OSError, match="original private commit"):
            session.advance(unit_budget())
        db.failure = None
        assert child._control.state is before and not session._ledger.epoch.quiescent
        with pytest.raises(RuntimeError, match="requires reconstruction"):
            child.load()
        with pytest.raises(RuntimeError, match="requires reconstruction"):
            session.progress()
    assert session.storage_received


def test_failed_child_receiving_discards_the_actual_session(journal, catalog, monkeypatch):
    with JournalValidationSession((journal,), attempt_id="a" * 32, catalog=catalog) as session:
        _reach(session, "entry")
        child = session._replay._current.child
        original, before = ChildControl.receive, child._control.state
        failure = RuntimeError("original child receiving failed")

        def receive(control, pending):
            if control is child._control:
                assert pending != before and control.state is before
                assert not session._store.db.in_transaction
                assert not session._ledger.epoch.quiescent
                raise failure
            return original(control, pending)

        monkeypatch.setattr(ChildControl, "receive", receive)
        with pytest.raises(RuntimeError) as caught:
            session.advance(unit_budget())
        assert caught.value is failure and child._control.state is before
        with pytest.raises(RuntimeError, match="requires reconstruction"):
            session.progress()
    assert session.storage_received


@pytest.mark.parametrize("kind", FAMILIES)
def test_child_control_encoding_belongs_to_each_immutable_value(tmp_path, catalog, kind):
    db, child = work(tmp_path / "child.sqlite3", catalog, kind)
    with closing(db):
        state = admit(child)
        encoded = state.to_json()
        assert state.to_json() is encoded
        after = replace(state, version=state.version + 1)
        assert after.to_json() != encoded and state.to_json() is encoded


@pytest.mark.parametrize("kind", ["derivation", "specialization"])
def test_generic_graph_work_receives_one_original_typed_edge_child(
    tmp_path, catalog, monkeypatch, kind
):
    path, row = tmp_path / "graph.sqlite3", rows.subject(kind)
    rows.prepare(path, row)
    with closing(rows.connect(path)) as db:
        child = rows.work(db, row, catalog, operation="row")
        state = admit(child)
        original_edge = None
        for _ in range(1024):
            if state.done:
                break
            batch = child.prepare(step_budget=1)

            state = apply_generic(child, batch)
            if batch.edge is not None:
                if original_edge is None:
                    original_edge = child._edge
                    monkeypatch.setattr(type(batch.edge.after), "from_json", refuse_decode)
                assert child._edge is original_edge
                assert original_edge.epoch is child.epoch
                assert original_edge._control.state is batch.edge.after
                if getattr(batch.edge, "graph_plan", None) is not None:
                    assert original_edge.checker._control.state == batch.edge.graph_plan.after
                    assert original_edge.checker.epoch is child.epoch
                assert original_edge._control.slot is (
                    ControlSlot.DERIVATION_EDGE
                    if kind == "derivation"
                    else ControlSlot.SPECIALIZATION_EDGE
                )
        else:
            pytest.fail("small actual semantic graph child did not complete")
        assert original_edge is not None
        child.result()
        rows.require_reclaimed(db)


@pytest.mark.parametrize("kind", ["derivation", "specialization"])
def test_received_semantic_edge_keeps_fresh_graph_algorithm_check(tmp_path, catalog, kind):
    path, row = tmp_path / "graph.sqlite3", rows.subject(kind)
    rows.prepare(path, row)
    with closing(rows.connect(path)) as db:
        child = rows.work(db, row, catalog, operation="row")
        state = admit(child)
        for _ in range(1024):
            batch = child.prepare(step_budget=1)

            state = apply_generic(child, batch)
            if batch.edge is not None:
                break
        else:
            pytest.fail("small actual row did not admit its graph child")
        original = child._edge
        assert original is not None and state.phase == "edge"
        db.execute("UPDATE dag_algorithm SET algorithm='foreign'").close()
        db.commit()
        with pytest.raises(RuntimeError, match="algorithm binding differs"):
            child.prepare(step_budget=1)
        assert child._edge is original and child._control.state is state
