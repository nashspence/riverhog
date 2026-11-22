from __future__ import annotations

import sqlite3
import weakref
from contextlib import closing
from dataclasses import replace

import pytest
from native_units import unit_budget
from riverhog_provenance._correction_projection import CorrectionState, CorrectionWork
from riverhog_provenance._entry_preparation import PreparationState
from riverhog_provenance._native_conductor import ControlSlot, LiveControl
from riverhog_provenance._native_control import ChildControl
from riverhog_provenance._native_epoch import EpochContext
from riverhog_provenance._online_dag import OnlineDag, _Work
from riverhog_provenance._online_dag import install as install_graph
from riverhog_provenance._semantic_edge import EdgeState
from riverhog_provenance._session_checkpoint import ValidationLedger
from riverhog_provenance._session_input import RetainedInput, _InputState
from riverhog_provenance.journal_session import JournalValidationSession
from test_live_child_control import FAMILIES, admit, refuse_decode, work
from test_native_epoch_integration import _Connection
from test_online_dag import vertex
from test_retained_journal_session import OwnedSource, directory, open_session


@pytest.mark.parametrize("kind", FAMILIES)
def test_recreated_child_control_uses_the_original_received_conductor(
    tmp_path, catalog, monkeypatch, kind
):
    db, child = work(tmp_path / "child.sqlite3", catalog, kind)
    with closing(db):
        before = admit(child)
        original = child._control
        child._control = ChildControl(
            child.epoch, type(before), original.identity, slot=original.slot
        )
        assert child._control is not original and child._control.state is before
        monkeypatch.setattr(type(before), "from_json", refuse_decode)
        batch = child.prepare()
        assert batch.before is before
        after = child.epoch.apply(
            lambda _epoch: child._apply(batch), receive=child._control.receive
        )
        assert child.load() is after and original.state is after
        foreign = ChildControl(
            child.epoch, type(before), (*original.identity, "foreign"), slot=original.slot
        )
        assert foreign.state is None and foreign.get(after.to_json()) is None
        with pytest.raises(RuntimeError, match="original post-commit"):
            foreign.receive(after)
        assert original.state is after and child.epoch.quiescent


def test_recreated_parent_shares_received_control_but_keeps_fresh_worker_fence(
    journal, catalog, monkeypatch
):
    with JournalValidationSession((journal,), attempt_id="a" * 32, catalog=catalog) as session:
        parent = session._ledger
        before = parent.load()
        rebuilt = ValidationLedger(
            parent.db,
            attempt_id=parent.attempt_id,
            binding=parent.binding,
            worker_epoch=parent.worker_epoch,
            epoch=parent.epoch,
        )
        monkeypatch.setattr(type(before.checkpoint), "from_json", refuse_decode)
        assert rebuilt._control is not parent._control and rebuilt.load() is before
        parent.db.execute("UPDATE journal_validation SET worker_epoch='1'").close()
        parent.db.commit()
        with pytest.raises(RuntimeError, match="worker changed"):
            rebuilt.load()
        assert parent._control.state is before
    assert session.storage_received


def test_recreated_retained_input_reuses_typed_cursor_after_fresh_storage_check(
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
        session.advance(unit_budget())
        before = session._input.state
        monkeypatch.setattr(_InputState, "decode", refuse_decode)
        rebuilt = RetainedInput(session._ledger, source)
        assert rebuilt is not session._input and rebuilt.state is before
        session._store.db.execute("UPDATE journal_validation_input SET checkpoint='{}'").close()
        session._store.db.commit()
        with pytest.raises(AssertionError, match="decoded again"):
            RetainedInput(session._ledger, source)
        assert session._input.state is before
    assert session.storage_received


def test_replacing_live_attempts_does_not_retain_a_history_of_control_values():
    with closing(sqlite3.connect(":memory:")) as db:
        epoch, controls, states = EpochContext(db), [], []
        for index in range(32):
            control = LiveControl(
                epoch, PreparationState, ("attempt", index), slot=ControlSlot.PREPARATION
            )
            pending = PreparationState(version=index)
            states.append(weakref.ref(pending))
            controls.append(control)
            epoch.apply(lambda _epoch, value=pending: value, receive=control.receive)
            assert control.state is pending
        assert all(state() is None for state in states[:-1])
        assert all(control.state is None for control in controls[:-1])
        assert states[-1]() is controls[-1].state
        replacement = LiveControl(
            epoch, PreparationState, ("attempt", 31), slot=ControlSlot.PREPARATION
        )
        assert replacement.state is pending
        independent = LiveControl(
            EpochContext(db), PreparationState, ("attempt", 31), slot=ControlSlot.PREPARATION
        )
        assert independent.state is None
        assert epoch.quiescent


@pytest.mark.parametrize("failure", ["rollback", "receiving"])
def test_failed_original_epoch_never_exposes_a_usable_new_control(failure):
    with closing(sqlite3.connect(":memory:")) as db:
        epoch = EpochContext(db)
        control = LiveControl(epoch, PreparationState, ("source", 1), slot=ControlSlot.PREPARATION)
        before = PreparationState()
        epoch.apply(lambda _epoch: before, receive=control.receive)
        pending = replace(before, version=1)
        cause = OSError("original epoch failed")

        def apply(_epoch):
            assert control.state is before
            if failure == "rollback":
                raise cause
            return pending

        def receive(value):
            assert value is pending and control.state is before
            assert not epoch.quiescent and not db.in_transaction
            raise cause

        with pytest.raises(OSError) as caught:
            epoch.apply(apply, receive=receive)
        assert caught.value is cause and control.state is before
        with pytest.raises(RuntimeError, match="requires reconstruction"):
            control.observe(pending)
        with pytest.raises(RuntimeError, match="original post-commit"):
            control.receive(pending)


def test_detached_observation_cannot_publish_control_inside_an_original_writer():
    with closing(sqlite3.connect(":memory:")) as db:
        epoch = EpochContext(db)
        control = LiveControl(epoch, PreparationState, ("source", 1), slot=ControlSlot.PREPARATION)
        before, pending = PreparationState(), PreparationState(version=1)
        control.observe(before)

        def apply(_epoch):
            with pytest.raises(RuntimeError, match="already active"):
                control.observe(pending)
            assert control.state is before
            return pending

        epoch.apply(apply, receive=control.receive)
        assert control.state is pending and epoch.quiescent
        with pytest.raises(TypeError, match="exact continuation type"):
            control.observe("wrong type")
        assert control.state is pending


@pytest.mark.parametrize("slot", ["preparation", None, 1, object()])
def test_unowned_control_slots_cannot_extend_the_native_universe(slot):
    with closing(sqlite3.connect(":memory:")) as db:
        epoch = EpochContext(db)
        with pytest.raises(TypeError, match="source-owned slot"):
            LiveControl(epoch, PreparationState, ("source",), slot=slot)
        assert epoch.conductor._received == {} and epoch.quiescent


def test_control_type_cannot_be_reassigned_to_another_source_role():
    with closing(sqlite3.connect(":memory:")) as db:
        epoch = EpochContext(db)
        with pytest.raises(TypeError, match="exact continuation type"):
            LiveControl(epoch, PreparationState, ("source",), slot=ControlSlot.INPUT)
        assert epoch.conductor._received == {} and epoch.quiescent


def test_distinct_correction_operations_receive_without_evicting_each_other():
    with closing(sqlite3.connect(":memory:")) as db:
        epoch = EpochContext(db)
        retire = CorrectionWork(
            db, operation="retire", attempt_id="same", source_binding="source", epoch=epoch
        )
        seed = CorrectionWork(
            db, operation="seed", attempt_id="same", source_binding="source", epoch=epoch
        )
        retired, seeded = CorrectionState("changes", version=3), CorrectionState("seed", version=7)
        epoch.apply(lambda _epoch: retired, receive=retire._control.receive)
        epoch.apply(lambda _epoch: seeded, receive=seed._control.receive)
        assert retire._control.slot is ControlSlot.RETIREMENT
        assert seed._control.slot is ControlSlot.SEED
        assert retire._control.state is retired and seed._control.state is seeded
        rebuilt = CorrectionWork(
            db, operation="retire", attempt_id="same", source_binding="source", epoch=epoch
        )
        assert rebuilt._control.state is retired
        replacement = CorrectionWork(
            db, operation="retire", attempt_id="next", source_binding="source", epoch=epoch
        )
        newer = replace(retired, version=4)
        epoch.apply(lambda _epoch: newer, receive=replacement._control.receive)
        assert rebuilt._control.state is None and retire._control.state is None
        assert replacement._control.state is newer and seed._control.state is seeded
        assert len(epoch.conductor._received) == 2


def test_graph_kind_slots_retain_both_original_controls_with_the_same_python_type():
    with closing(sqlite3.connect(":memory:")) as db:
        epoch = EpochContext(db)
        derivation = LiveControl(epoch, EdgeState, ("same",), slot=ControlSlot.DERIVATION_EDGE)
        specialization = LiveControl(
            epoch, EdgeState, ("same",), slot=ControlSlot.SPECIALIZATION_EDGE
        )
        first, second = EdgeState("graph", 3), EdgeState("done", 7)
        epoch.apply(lambda _epoch: first, receive=derivation.receive)
        epoch.apply(lambda _epoch: second, receive=specialization.receive)
        assert derivation.state is first and specialization.state is second
        assert len(epoch.conductor._received) == 2


def test_live_control_does_no_sql_or_recovery_codec_work_and_does_not_narrow_scalars(monkeypatch):
    with closing(sqlite3.connect(":memory:")) as db:
        epoch = EpochContext(db)
        value = 1 << 10000
        original = LiveControl(
            epoch, PreparationState, ("source", value), slot=ControlSlot.PREPARATION
        )
        state = PreparationState(version=value)
        queries = []
        db.set_trace_callback(queries.append)
        monkeypatch.setattr(PreparationState, "to_json", refuse_decode)
        monkeypatch.setattr(PreparationState, "from_json", refuse_decode)
        original.observe(state)
        rebuilt = LiveControl(
            epoch, PreparationState, ("source", value), slot=ControlSlot.PREPARATION
        )
        assert original.state is rebuilt.state is state
        assert rebuilt.state.version == value
        assert queries == [] and epoch.quiescent


@pytest.mark.parametrize("kind", ["derivation", "specialization"])
def test_original_graph_control_is_shared_after_fresh_run_and_work_fences(monkeypatch, kind):
    with closing(sqlite3.connect(":memory:")) as db:
        install_graph(db)
        original = OnlineDag(db, kind, "original")
        original.bind()
        before = original._control.state
        assert before is not None
        rebuilt = OnlineDag(db, kind, "original", epoch=original.epoch)
        monkeypatch.setattr(_Work, "decode", refuse_decode)
        assert rebuilt._load() is before
        rebuilt.start("edge", vertex(1), vertex(2))
        after = rebuilt._control.state
        assert after is original._control.state and after is not before
        assert original._load() is after and before.work.phase == "idle"
        db.execute("UPDATE dag_runs SET work=? WHERE kind=?", ('{"phase":"foreign"}', kind)).close()
        db.commit()
        with pytest.raises(AssertionError, match="decoded again"):
            rebuilt._load()
        assert original._control.state is after


@pytest.mark.parametrize("kind", ["derivation", "specialization"])
@pytest.mark.parametrize("operation", ["step", "replacement"])
@pytest.mark.parametrize("failure", ["before", "after"])
def test_graph_control_keeps_original_value_when_commit_is_unconfirmed(kind, operation, failure):
    with closing(sqlite3.connect(":memory:", factory=_Connection)) as db:
        install_graph(db)
        graph = OnlineDag(db, kind, "original")
        graph.bind()
        before = graph._control.state
        db.failure = failure
        with pytest.raises(OSError, match="original private commit") as caught:
            if operation == "step":
                graph.start("edge", vertex(1), vertex(2))
            else:
                graph.replace_run("replacement")
        assert graph._control.state is before and not graph.epoch.quiescent
        with pytest.raises(RuntimeError, match="requires reconstruction") as refused:
            graph.progress()
        assert refused.value.__cause__ is caught.value
        db.failure = None
        run = db.execute("SELECT run_id FROM dag_runs WHERE kind=?", (kind,)).fetchone()[0]
        rebuilt = OnlineDag(db, kind, run)
        observed = rebuilt._load()
        assert observed.version == before.version + (failure == "after")
        assert observed.work.phase == (
            "initialize" if operation == "step" and failure == "after" else "idle"
        )
        assert run == (
            "replacement" if operation == "replacement" and failure == "after" else "original"
        )


def test_graph_receiving_failure_discards_the_committed_original_executor(monkeypatch):
    with closing(sqlite3.connect(":memory:")) as db:
        install_graph(db)
        graph = OnlineDag(db, "derivation", "original")
        graph.bind()
        before = graph._control.state
        failure = OSError("graph receiving failed")
        receive = OnlineDag._receive

        def refused(current, pending):
            if current is graph:
                assert current._control.state is before and not db.in_transaction
                assert not current.epoch.quiescent and pending.work.phase == "initialize"
                raise failure
            receive(current, pending)

        monkeypatch.setattr(OnlineDag, "_receive", refused)
        with pytest.raises(OSError) as caught:
            graph.start("edge", vertex(1), vertex(2))
        assert caught.value is failure and graph._control.state is before
        with pytest.raises(RuntimeError, match="requires reconstruction"):
            graph.progress()
        rebuilt = OnlineDag(db, graph.kind, graph.run_id)
        assert rebuilt.progress().phase == "initialize"


def test_both_actual_graph_roles_keep_one_current_run_in_the_shared_conductor():
    with closing(sqlite3.connect(":memory:")) as db:
        install_graph(db)
        epoch = EpochContext(db)
        derivation = OnlineDag(db, "derivation", "same", epoch=epoch)
        specialization = OnlineDag(db, "specialization", "same", epoch=epoch)
        derivation.bind()
        specialization.bind()
        derivation.start("first", vertex(1), vertex(2))
        first = derivation._control.state
        specialization.start("second", vertex(2), vertex(1))
        second = specialization._control.state
        assert first is not second and first.work.operation_id == "first"
        assert second.work.operation_id == "second"
        replacement = derivation.replace_run("new")
        assert derivation._control.state is None
        assert replacement._control.state.work.phase == "idle"
        assert specialization._control.state is second
        assert len(epoch.conductor._received) == 2
        plan = replacement._prepare_start("third", vertex(3), vertex(4))
        pending = plan.after
        plan.work.phase = "complete"
        assert pending.work.phase == "initialize"
        assert replacement._control.state.work.phase == "idle"
