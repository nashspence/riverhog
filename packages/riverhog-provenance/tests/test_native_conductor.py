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
from riverhog_provenance._semantic_edge import EdgeState
from riverhog_provenance._session_checkpoint import ValidationLedger
from riverhog_provenance._session_input import RetainedInput, _InputState
from riverhog_provenance.journal_session import JournalValidationSession
from test_live_child_control import FAMILIES, admit, refuse_decode, work
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
