from __future__ import annotations

import sqlite3
from contextlib import closing

import pytest
from riverhog_provenance._entry_preparation import EntryPreparation
from riverhog_provenance._native_epoch import EpochContext
from riverhog_provenance._native_work import NativeWorkCapacity, WorkAmount, WorkQuantum
from riverhog_provenance.journal_session import (
    JournalValidationBudget,
    JournalValidationSession,
    JournalValidationWorkBudget,
)
from sqlite_admission import SqliteProgressConnection
from test_assertion_staging import connect, create
from test_entry_preparation import frame, position, queued, work
from test_native_epoch_integration import _reach
from test_retained_journal_session import OwnedSource, directory, open_session


def quantum(rows):
    return WorkQuantum(WorkAmount(rows=rows, octets=2**24, source_octets=2**20, transitions=64))


def database():
    db = sqlite3.connect(":memory:", factory=SqliteProgressConnection)
    db.execute("CREATE TABLE facts(value TEXT)").close()
    return db


def insert(original, rows):
    original.account(WorkAmount(rows=rows))
    original.db.execute("INSERT INTO facts VALUES('pending')").close()
    return "received"


def test_application_capacity_is_refused_before_begin_without_poisoning_original_executor():
    with closing(database()) as db:
        epoch, admission, trace = EpochContext(db), quantum(2), []
        with epoch.execution(None, work=admission):
            db.set_trace_callback(trace.append)
            try:
                with pytest.raises(NativeWorkCapacity):
                    epoch.apply(
                        lambda _original: pytest.fail("unadmitted mutation ran"),
                        receive=lambda _value: pytest.fail("unadmitted receiving ran"),
                        admission=WorkAmount(rows=3),
                    )
            finally:
                db.set_trace_callback(None)
            assert trace == [] and epoch.quiescent
            assert epoch._application_work is None
            assert admission.consumed == admission.held == WorkAmount()
        admission.require_received()
        retry = quantum(3)
        received = []
        with epoch.execution(None, work=retry):
            assert (
                epoch.apply(
                    lambda original: insert(original, 3),
                    receive=received.append,
                    admission=WorkAmount(rows=3),
                )
                == "received"
            )
        assert received == ["received"] and retry.consumed.rows == 3
        retry.require_received()
        assert db.execute("SELECT count(*) FROM facts").fetchone() == (1,)


def test_unused_application_admission_is_received_after_commit_and_live_receiving():
    with closing(database()) as db:
        epoch, original = EpochContext(db), quantum(7)
        live = []

        def receive(value):
            epoch.require_receiving(db)
            assert epoch.work is original and original.held.rows == 2
            assert live == []
            epoch.account(WorkAmount(rows=1))
            live.append(value)

        with epoch.execution(None, work=original):
            epoch.apply(
                lambda current: insert(current, 2),
                receive=receive,
                admission=WorkAmount(rows=4),
            )
            assert live == ["received"] and epoch.quiescent
            assert original.consumed.rows == 3 and original.held == WorkAmount()
            epoch.apply(
                lambda current: insert(current, 3),
                receive=lambda _value: None,
                admission=WorkAmount(rows=4),
            )
            assert original.consumed.rows == 6 and original.remaining.rows == 1
            with pytest.raises(NativeWorkCapacity):
                epoch.apply(
                    lambda _current: pytest.fail("spent work was replenished"),
                    receive=lambda _value: None,
                    admission=WorkAmount(rows=2),
                )
            assert epoch.quiescent and original.consumed.rows == 6
        original.require_received()
        assert db.execute("SELECT count(*) FROM facts").fetchone() == (2,)


@pytest.mark.parametrize("receiving_failure", [False, True])
def test_actual_work_survives_original_failure_and_unused_reservation_is_received(
    receiving_failure,
):
    with closing(database()) as db:
        epoch, original = EpochContext(db), quantum(5)
        failure = OSError("original application or receiving failed")

        def mutate(current):
            value = insert(current, 2)
            if not receiving_failure:
                raise failure
            return value

        def receive(_value):
            epoch.require_receiving(db)
            assert original.held.rows == 3
            raise failure

        with pytest.raises(OSError) as caught:
            with epoch.execution(None, work=original):
                epoch.apply(mutate, receive=receive, admission=WorkAmount(rows=5))
        assert caught.value is failure and epoch.work is None
        assert original.consumed == WorkAmount(rows=2) and original.held == WorkAmount()
        original.require_received()
        assert epoch._application_work is None
        assert db.execute("SELECT count(*) FROM facts").fetchone() == (
            1 if receiving_failure else 0,
        )
        with pytest.raises(RuntimeError, match="requires reconstruction"):
            with epoch.execution(None, work=quantum(5)):
                pytest.fail("failed original execution was reused")


def test_underestimated_application_is_an_executor_failure_not_ordinary_capacity_yield():
    with closing(database()) as db:
        epoch, original = EpochContext(db), quantum(3)

        def mutate(current):
            insert(current, 1)
            current.account(WorkAmount(rows=1))

        with pytest.raises(RuntimeError, match="exceeds its original admission") as caught:
            with epoch.execution(None, work=original):
                epoch.apply(mutate, receive=lambda _value: None, admission=WorkAmount(rows=1))
        assert not isinstance(caught.value, NativeWorkCapacity)
        assert isinstance(caught.value.__cause__, NativeWorkCapacity)
        assert original.consumed == WorkAmount(rows=1) and original.held == WorkAmount()
        assert db.execute("SELECT count(*) FROM facts").fetchone() == (0,)
        original.require_received()
        with pytest.raises(RuntimeError, match="requires reconstruction"):
            epoch.require_usable()


def test_a_writer_cannot_account_unreserved_work_from_the_rest_of_the_quantum():
    with closing(database()) as db:
        epoch, original = EpochContext(db), quantum(3)
        with pytest.raises(RuntimeError, match="requires pre-writer admission"):
            with epoch.execution(None, work=original):
                epoch.apply(lambda current: insert(current, 1), receive=lambda _value: None)
        assert original.consumed == original.held == WorkAmount()
        original.require_received()
        assert db.execute("SELECT count(*) FROM facts").fetchone() == (0,)


@pytest.mark.parametrize(
    "phase,expected", [("journal", 3), ("entry", 4), ("changes", 7), ("done", 5)]
)
def test_real_preparation_application_envelope_covers_its_point_probes_and_retired_rows(
    tmp_path, phase, expected
):
    path = tmp_path / "preparation.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        queued(db, 7)
        epoch = EpochContext(db)
        current = work(db, frame(), epoch=epoch)
        current.initialize()
        position(current, phase)
        batch = current.prepare(row_budget=3)
        envelope = current._application_admission(batch)
        assert envelope == WorkAmount(rows=expected)
        original, trace = quantum(expected), []
        with epoch.execution(None, work=original):
            db.set_trace_callback(trace.append)
            try:
                state = epoch.apply(
                    lambda active: current._apply(batch),
                    receive=current._control.receive,
                    admission=envelope,
                )
            finally:
                db.set_trace_callback(None)
        probes = sum(sql.startswith("SELECT ") for sql in trace)
        assert probes == (4 if phase == "changes" else expected)
        assert original.consumed == WorkAmount(rows=expected)
        assert original.held == WorkAmount() and epoch.quiescent
        assert current._control.get(state.to_json()) is state
        if phase == "changes":
            assert state.retired - batch.before.retired == 3
        original.require_received()


def test_real_child_admission_uses_its_original_two_probe_reservation(tmp_path):
    path = tmp_path / "preparation.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        epoch, original = EpochContext(db), quantum(2)
        current = work(db, frame(), epoch=epoch)
        batch = current.prepare_admission()
        with epoch.execution(None, work=original):
            state = epoch.apply(
                lambda _active: current._admit(batch),
                receive=current._control.receive,
                admission=current._application_admission(batch),
            )
        assert original.consumed == WorkAmount(rows=2)
        assert current._control.get(state.to_json()) is state and epoch.quiescent
        original.require_received()


def test_partial_retirement_observation_remains_charged_after_original_rollback(
    tmp_path, monkeypatch
):
    path = tmp_path / "preparation.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        queued(db, 7)
        epoch, original = EpochContext(db), quantum(7)
        current = work(db, frame(), epoch=epoch)
        current.initialize()
        position(current, "changes")
        batch = current.prepare(row_budget=3)
        inspect, calls = current._inspect_row, []
        failure = OSError("original retirement observation failed")

        def inspected():
            inspect()
            calls.append(1)
            if len(calls) == 5:
                raise failure

        monkeypatch.setattr(current, "_inspect_row", inspected)
        with pytest.raises(OSError) as caught:
            with epoch.execution(None, work=original):
                epoch.apply(
                    lambda _active: current._apply(batch),
                    receive=current._control.receive,
                    admission=current._application_admission(batch),
                )
        assert caught.value is failure
        assert original.consumed == WorkAmount(rows=5) and original.held == WorkAmount()
        assert db.execute("SELECT count(*) FROM changes").fetchone() == (7,)
        assert len(calls) == 5 and epoch._application_work is None
        original.require_received()


@pytest.mark.parametrize("retained", [False, True])
def test_actual_session_reserves_preparation_before_any_writer_and_retries_same_owner(
    tmp_path, journal, catalog, journal_constructions, monkeypatch, retained
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
        _reach(session, "entry")
        child = session._replay._current.child
        assert isinstance(child, EntryPreparation)
        for _ in range(16):
            if child.load().phase == "changes":
                break
            session.advance()
        else:
            pytest.fail("small actual preparation did not reach changes")
        assert session._store.db.execute("SELECT count(*) FROM changes").fetchone() == (0,)
        plans, plan = [], child._application_admission

        def admission(batch):
            result = plan(batch)
            plans.append((child.epoch.work, result))
            return result

        monkeypatch.setattr(child, "_application_admission", admission)
        before, changes, trace = session.progress(), session._store.db.total_changes, []
        session._store.db.set_trace_callback(trace.append)
        try:
            with pytest.raises(NativeWorkCapacity):
                session.advance(
                    JournalValidationBudget(rows=1, work=JournalValidationWorkBudget(rows=12))
                )
        finally:
            session._store.db.set_trace_callback(None)
        assert not any(sql.startswith("BEGIN") for sql in trace)
        assert session.progress() == before and session._store.db.total_changes == changes
        refused, envelope = plans[0]
        assert envelope == WorkAmount(rows=5)
        assert refused.consumed.rows == 8 and refused.held == WorkAmount()
        assert session._failure is None and session._ledger.epoch.quiescent
        refused.require_received()
        after = session.advance(
            JournalValidationBudget(rows=1, work=JournalValidationWorkBudget(rows=13))
        )
        assert after != before and child.load().done
        received = plans[1][0]
        assert received is not refused and received.consumed.rows == 12
        assert received.held == WorkAmount() and session._ledger.epoch.quiescent
        received.require_received()
    assert session.storage_received


def test_failed_unused_admission_receiving_invalidates_the_committed_executor(monkeypatch):
    from riverhog_provenance._native_work import WorkReservation

    with closing(database()) as db:
        epoch, original, retained = EpochContext(db), quantum(3), []
        close = WorkReservation.close
        failure = MemoryError("original unused-admission receiving failed")
        failed = False

        def receive_reservation(reservation):
            nonlocal failed
            if retained and reservation is retained[0] and not failed:
                failed = True
                raise failure
            close(reservation)

        def mutate(current):
            retained.append(current._application_work)
            return insert(current, 1)

        monkeypatch.setattr(WorkReservation, "close", receive_reservation)
        try:
            with pytest.raises(MemoryError) as caught:
                with epoch.execution(None, work=original):
                    epoch.apply(
                        mutate,
                        receive=lambda _value: None,
                        admission=WorkAmount(rows=3),
                    )
            assert caught.value is failure
            assert original.consumed == WorkAmount(rows=1)
            assert original.held == WorkAmount()
            original.require_received()
            assert db.execute("SELECT count(*) FROM facts").fetchone() == (1,)
            assert not epoch.quiescent
            with pytest.raises(RuntimeError, match="requires reconstruction") as unusable:
                epoch.require_usable()
            assert unusable.value.__cause__ is failure
        finally:
            for reservation in retained:
                close(reservation)
