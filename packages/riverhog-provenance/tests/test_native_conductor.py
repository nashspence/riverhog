from __future__ import annotations

import sqlite3
import weakref
from contextlib import closing
from dataclasses import dataclass, replace

import pytest
from native_units import unit_budget
from riverhog_provenance._native_conductor import LiveControl
from riverhog_provenance._native_control import ChildControl
from riverhog_provenance._native_epoch import EpochContext
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
        child._control = ChildControl(child.epoch, type(before), original.identity)
        assert child._control is not original and child._control.state is before
        monkeypatch.setattr(type(before), "from_json", refuse_decode)
        batch = child.prepare()
        assert batch.before is before
        after = child.epoch.apply(
            lambda _epoch: child._apply(batch), receive=child._control.receive
        )
        assert child.load() is after and original.state is after
        foreign = ChildControl(child.epoch, type(before), (*original.identity, "foreign"))
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


@dataclass(frozen=True)
class _State:
    value: int


def test_replacing_live_attempts_does_not_retain_a_history_of_control_values():
    with closing(sqlite3.connect(":memory:")) as db:
        epoch, controls, states = EpochContext(db), [], []
        for index in range(32):
            control = LiveControl(epoch, _State, ("attempt", index))
            pending = _State(index)
            states.append(weakref.ref(pending))
            controls.append(control)
            epoch.apply(lambda _epoch, value=pending: value, receive=control.receive)
            assert control.state is pending
        assert all(state() is None for state in states[:-1])
        assert all(control.state is None for control in controls[:-1])
        assert states[-1]() is controls[-1].state
        replacement = LiveControl(epoch, _State, ("attempt", 31))
        assert replacement.state is pending
        independent = LiveControl(EpochContext(db), _State, ("attempt", 31))
        assert independent.state is None
        assert epoch.quiescent


@pytest.mark.parametrize("failure", ["rollback", "receiving"])
def test_failed_original_epoch_never_exposes_a_usable_new_control(failure):
    with closing(sqlite3.connect(":memory:")) as db:
        epoch = EpochContext(db)
        control = LiveControl(epoch, _State, ("source", 1))
        before = _State(0)
        epoch.apply(lambda _epoch: before, receive=control.receive)
        pending = replace(before, value=1)
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
        control = LiveControl(epoch, _State, ("source", 1))
        before, pending = _State(0), _State(1)
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
