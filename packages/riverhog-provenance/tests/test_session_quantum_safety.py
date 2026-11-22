from __future__ import annotations

import json
import sqlite3

import pytest
from native_units import unit_budget
from riverhog_provenance._native_control import ChildControl
from riverhog_provenance._native_epoch import EpochContext
from riverhog_provenance._native_work import (
    NativeWorkCapacity,
    WorkAmount,
    WorkQuantum,
    WorkReservation,
)
from riverhog_provenance._session_checkpoint import ValidationLedger
from riverhog_provenance.journal_session import (
    JournalValidationBudget,
    JournalValidationSession,
    JournalValidationWorkBudget,
)
from sqlite_admission import SqliteProgressConnection
from test_retained_journal_session import OwnedSource, directory, open_session


class _Connection(SqliteProgressConnection):
    armed = False
    commits = 0
    fail_at = None
    fail_when = None
    failure = None
    expired = False

    def commit(self):
        if self.armed:
            self.commits += 1
            if self.commits == self.fail_at and self.fail_when == "before":
                raise self.failure
        super().commit()
        if self.armed:
            self.expired = True
            if self.commits == self.fail_at and self.fail_when == "after":
                raise self.failure


def _session(journal, catalog):
    def connect(*args, **kwargs):
        return sqlite3.connect(*args, factory=_Connection, **kwargs)

    return JournalValidationSession(
        (journal,), attempt_id="a" * 32, catalog=catalog, connection_factory=connect
    )


def _observe_units(monkeypatch, session):
    original = JournalValidationSession._advance_unit
    seen = []

    def advance(owner, budget):
        assert owner is session
        epoch = owner._ledger.epoch
        assert epoch.quiescent and epoch._execution_active
        work = epoch.work
        assert work is not None and work.held == WorkAmount()
        seen.append(work)
        assert all(item is work for item in seen)
        assert work.consumed.transitions == len(seen)
        return original(owner, budget)

    monkeypatch.setattr(JournalValidationSession, "_advance_unit", advance)
    return seen


def test_default_quantum_attempts_exactly_64_empty_pulls_and_renders_progress_once(
    journal, catalog, monkeypatch
):
    pulls = []

    def chunks():
        for index in range(64):
            pulls.append(index)
            yield b""
        pulls.append("journal")
        yield journal

    with JournalValidationSession(chunks(), attempt_id="a" * 32, catalog=catalog) as session:
        seen = _observe_units(monkeypatch, session)
        progress, rendered = JournalValidationSession._progress, []
        contexts, execution = [], EpochContext.execution

        def render(owner, snapshot):
            rendered.append(snapshot)
            return progress(owner, snapshot)

        def execute(epoch, check, *, work=None):
            contexts.append((epoch, check, work))
            return execution(epoch, check, work=work)

        monkeypatch.setattr(JournalValidationSession, "_progress", render)
        monkeypatch.setattr(EpochContext, "execution", execute)

        def original_check():
            pass

        after = session.advance(check=original_check)
        assert not after.complete and after.source_octets == 0
        assert pulls == list(range(64)) and len(seen) == 64
        assert contexts == [(session._ledger.epoch, original_check, seen[0])]
        assert len(rendered) == 1
        assert seen[0].consumed == WorkAmount(rows=128, transitions=64)
        assert seen[0].limits == WorkAmount(
            JournalValidationWorkBudget().rows,
            JournalValidationWorkBudget().octets,
            JournalValidationWorkBudget().source_octets,
            64,
        )
        seen[0].require_received()
    assert session.storage_received


@pytest.mark.parametrize("retained", [False, True])
def test_later_capacity_yields_only_received_progress_and_retains_failed_attempt_cost(
    tmp_path, journal, catalog, journal_constructions, monkeypatch, retained
):
    source = OwnedSource(journal, catalog) if retained else None
    context = (
        open_session(
            directory(tmp_path),
            source,
            catalog,
            create=True,
            journal_constructions=journal_constructions,
        )
        if retained
        else _session(journal, catalog)
    )
    with context as session:
        before = session._ledger.load()
        seen = _observe_units(monkeypatch, session)
        trace = []
        db = session._store.db
        db.set_trace_callback(trace.append)
        try:
            after = session.advance(
                JournalValidationBudget(
                    source_octets=7,
                    work=JournalValidationWorkBudget(rows=10 if retained else 3),
                )
            )
        finally:
            db.set_trace_callback(None)
        assert len(seen) == 2 and after.source_octets == 7
        assert after.observation_sequence == 1
        assert sum(sql == "COMMIT" for sql in trace) == int(retained)
        assert session._ledger.load().checkpoint.version == before.checkpoint.version + int(
            retained
        )
        assert session._ledger.epoch.quiescent and session._failure is None
        assert seen[0].consumed == WorkAmount(
            rows=9 if retained else 3,
            octets=14 if retained else 7,
            source_octets=14 if retained else 7,
            transitions=2,
        )
        seen[0].require_received()
        if source is not None:
            assert source.calls == [(0, 7), (7, 7)]
        # Observe the original next invocation separately, without replacing its input owner.
        monkeypatch.undo()
        assert session.advance(unit_budget(source_octets=7)).source_octets == 14
        if source is not None:
            assert source.calls == [(0, 7), (7, 7), (7, 7)]
    assert session.storage_received


class _Cancelled(BaseException):
    pass


@pytest.mark.parametrize("failure_kind", ["source", "foreign-admission", "reservation-overrun"])
def test_later_caller_capacity_keeps_original_cause_and_never_returns_false_progress(
    journal, catalog, monkeypatch, failure_kind
):
    failures = []
    foreign = WorkQuantum(WorkAmount(1, 1, 1, 1))

    def chunks():
        yield journal[:7]
        try:
            if failure_kind == "source":
                raise NativeWorkCapacity("original caller failed after its first partial chunk")
            if failure_kind == "foreign-admission":
                foreign.reserve(WorkAmount(rows=2))
                raise AssertionError("foreign admission unexpectedly succeeded")
            # This is this invocation's original reservation, but consuming more
            # than admitted is a failed operation, not a pre-work refusal.
            with session._ledger.epoch.work.reserve(WorkAmount(octets=1)) as reservation:
                reservation.consume(WorkAmount(octets=2))
                raise AssertionError("actual reservation overrun unexpectedly succeeded")
        except NativeWorkCapacity as error:
            failures.append(error)
            raise

    with JournalValidationSession(chunks(), attempt_id="a" * 32, catalog=catalog) as session:
        before = session._ledger.load()
        db = session._store.db
        stored = db.execute("SELECT checkpoint FROM journal_validation").fetchone()
        seen = _observe_units(monkeypatch, session)
        rendered = []
        progress = JournalValidationSession._progress

        def render(owner, state):
            rendered.append(state)
            return progress(owner, state)

        monkeypatch.setattr(JournalValidationSession, "_progress", render)
        with pytest.raises(NativeWorkCapacity) as caught:
            session.advance()
        assert len(failures) == 1 and caught.value is failures[0]
        assert len(seen) == 2 and not rendered
        assert not seen[0].owns_admission_refusal(caught.value)
        assert foreign.owns_admission_refusal(caught.value) == (failure_kind == "foreign-admission")
        assert seen[0].consumed == WorkAmount(rows=4, octets=7, source_octets=7, transitions=2)
        assert session._input.cursor.byte_offset == 7
        assert session._ledger.load() == before
        assert db.execute("SELECT checkpoint FROM journal_validation").fetchone() == stored
        assert not db.in_transaction and session._ledger.epoch.quiescent
        seen[0].require_received()
        foreign.require_received()
    assert session.storage_received


@pytest.mark.parametrize("port", ["verify_current", "read_window", "read_exact"])
def test_later_retained_source_capacity_preserves_original_received_commit(
    tmp_path, journal, catalog, journal_constructions, monkeypatch, port
):
    source = OwnedSource(journal, catalog)
    with open_session(
        directory(tmp_path),
        source,
        catalog,
        create=True,
        journal_constructions=journal_constructions,
    ) as session:
        before = session._ledger.load()
        original = NativeWorkCapacity("original retained source port failed: " + port)
        callback = getattr(source, port)
        failed_state = []

        def invoke(*args):
            ready = (
                session._input.pending
                if port == "read_exact"
                else session._input.cursor.byte_offset > 0
            )
            if ready:
                failed_state.append(session._ledger._control.state)
                raise original
            return callback(*args)

        monkeypatch.setattr(source, port, invoke)
        seen = _observe_units(monkeypatch, session)
        rendered = []
        progress = JournalValidationSession._progress

        def render(owner, state):
            rendered.append(state)
            return progress(owner, state)

        monkeypatch.setattr(JournalValidationSession, "_progress", render)
        trace = []
        db = session._store.db
        db.set_trace_callback(trace.append)
        try:
            with pytest.raises(NativeWorkCapacity) as caught:
                session.advance(
                    JournalValidationBudget(source_octets=65536 if port == "read_exact" else 7)
                )
        finally:
            db.set_trace_callback(None)
        assert caught.value is original and len(failed_state) == 1
        assert len(seen) == 2 and not rendered
        assert not seen[0].owns_admission_refusal(original)
        assert sum(sql == "COMMIT" for sql in trace) == 1
        received = session._ledger._control.state
        assert received is failed_state[0]
        assert received.checkpoint.version == before.checkpoint.version + 1
        assert session._ledger.load() == received
        stored = json.loads(db.execute("SELECT checkpoint FROM journal_validation").fetchone()[0])
        assert stored["version"] == str(received.checkpoint.version)
        assert stored["phase"] == received.checkpoint.phase == "frame"
        assert session._input.cursor.byte_offset > 0 and len(source.calls) == 1
        assert seen[0].consumed.transitions == 2
        assert not db.in_transaction and session._ledger.epoch.quiescent
        seen[0].require_received()
    assert session.storage_received


@pytest.mark.parametrize("cause", [TimeoutError, NativeWorkCapacity, _Cancelled])
def test_later_preparation_check_preserves_original_cause_after_received_commit(
    tmp_path, journal, catalog, journal_constructions, monkeypatch, cause
):
    source = OwnedSource(journal, catalog)
    with open_session(
        directory(tmp_path),
        source,
        catalog,
        create=True,
        journal_constructions=journal_constructions,
    ) as session:
        before = session._ledger.load()
        seen = _observe_units(monkeypatch, session)
        original = cause("original later-unit execution cause")

        def check():
            work = session._ledger.epoch.work
            if work is not None and work.consumed.transitions == 2:
                raise original

        with pytest.raises(cause) as caught:
            session.advance(JournalValidationBudget(source_octets=7), check=check)
        assert caught.value is original
        assert len(seen) == 2 and seen[0].consumed.transitions == 2
        assert source.calls == [(0, 7)] and session._input.cursor.byte_offset == 7
        assert session._ledger.load().checkpoint.version == before.checkpoint.version + 1
        assert session._ledger.epoch.quiescent
        seen[0].require_received()
    assert session.storage_received


def test_deadline_after_commit_receives_original_live_state_before_next_unit_check(
    journal, catalog, monkeypatch
):
    with _session(journal, catalog) as session:
        session.advance(unit_budget())
        assert session._input.pending
        db = session._store.db
        db.armed = True
        seen = _observe_units(monkeypatch, session)
        original = TimeoutError("original deadline expired after commit")

        def check():
            if db.expired:
                raise original

        with pytest.raises(TimeoutError) as caught:
            session.advance(check=check)
        assert caught.value is original and db.commits == 1
        assert len(seen) == 1 and seen[0].consumed.transitions == 1
        assert session._replay._current is not None and not session._input.pending
        assert session._ledger.epoch.quiescent and not db.in_transaction
        assert session._ledger.load().checkpoint.phase == "entry-admission"
        assert db.execute("SELECT count(*) FROM journal_validation_frame").fetchone() == (1,)
        seen[0].require_received()
    assert session.storage_received


@pytest.mark.parametrize("when", ["before", "after"])
@pytest.mark.parametrize("cause", [OSError, NativeWorkCapacity])
def test_second_original_commit_failure_keeps_exact_sql_and_never_attempts_third_unit(
    journal, catalog, monkeypatch, when, cause
):
    with _session(journal, catalog) as session:
        session.advance(unit_budget())
        db, parent = session._store.db, session._ledger
        db.armed, db.fail_at, db.fail_when = True, 2, when
        db.failure = cause("original second commit failed")
        seen = _observe_units(monkeypatch, session)
        with pytest.raises(cause) as caught:
            session.advance()
        assert caught.value is db.failure and db.commits == 2 and len(seen) == 2
        received = parent._control.state
        assert received.checkpoint.phase == "entry-admission"
        state = json.loads(db.execute("SELECT checkpoint FROM journal_validation").fetchone()[0])
        assert state["version"] == str(received.checkpoint.version + int(when == "after"))
        assert state["phase"] == ("entry" if when == "after" else "entry-admission")
        assert session._replay._current is not None and not session._input.pending
        assert not db.in_transaction and not parent.epoch.quiescent
        seen[0].require_received()
        with pytest.raises(RuntimeError, match="requires reconstruction"):
            session.progress()
        failed_message = (
            "requires reconstruction" if cause is NativeWorkCapacity else "session failed"
        )
        with pytest.raises(RuntimeError, match=failed_message) as reused:
            session.advance()
        assert reused.value.__cause__ is db.failure
        db.fail_at = None
    assert session.storage_received


@pytest.mark.parametrize("receiver", ["parent", "child", "unused-admission"])
@pytest.mark.parametrize("cause", [RuntimeError, NativeWorkCapacity])
def test_second_commit_receiving_failure_never_returns_progress_or_runs_another_unit(
    journal, catalog, monkeypatch, receiver, cause
):
    with _session(journal, catalog) as session:
        session.advance(unit_budget())
        db, parent = session._store.db, session._ledger
        db.armed = True
        seen = _observe_units(monkeypatch, session)
        original = cause("original receiving failed")
        failed = False

        def fail():
            nonlocal failed
            if db.commits == 2 and not failed:
                failed = True
                raise original

        if receiver == "parent":
            receive = ValidationLedger._receive

            def parent_receive(owner, state):
                fail()
                receive(owner, state)

            monkeypatch.setattr(ValidationLedger, "_receive", parent_receive)
        elif receiver == "child":
            receive = ChildControl.receive

            def child_receive(owner, state):
                fail()
                receive(owner, state)

            monkeypatch.setattr(ChildControl, "receive", child_receive)
        else:
            close = WorkReservation.close

            def receive_unused(reservation):
                if parent.epoch._application_work is reservation:
                    fail()
                close(reservation)

            monkeypatch.setattr(WorkReservation, "close", receive_unused)
        with pytest.raises(cause) as caught:
            session.advance()
        assert caught.value is original and failed
        assert db.commits == 2 and len(seen) == 2
        state = json.loads(db.execute("SELECT checkpoint FROM journal_validation").fetchone()[0])
        assert state["phase"] == "entry"
        assert parent._control.state.checkpoint.phase == (
            "entry-admission" if receiver == "parent" else "entry"
        )
        assert not db.in_transaction and not parent.epoch.quiescent
        seen[0].require_received()
        with pytest.raises(RuntimeError, match="requires reconstruction"):
            session.progress()
        failed_message = (
            "requires reconstruction" if cause is NativeWorkCapacity else "session failed"
        )
        with pytest.raises(RuntimeError, match=failed_message) as reused:
            session.advance()
        assert reused.value.__cause__ is original
    assert session.storage_received


def test_unreceived_work_after_commit_invalidates_original_epoch_before_next_unit(
    journal, catalog, monkeypatch
):
    retained = []
    with _session(journal, catalog) as session:
        session.advance(unit_budget())
        db, parent = session._store.db, session._ledger
        db.armed = True
        seen = _observe_units(monkeypatch, session)
        receive = ValidationLedger._receive

        def receiving(owner, state):
            receive(owner, state)
            retained.append(owner.epoch.work.reserve(WorkAmount(octets=1)))

        monkeypatch.setattr(ValidationLedger, "_receive", receiving)
        try:
            with pytest.raises(RuntimeError, match="unreceived original work"):
                session.advance()
            assert db.commits == 1 and len(seen) == 1
            assert not db.in_transaction and not parent.epoch.quiescent
            assert parent._control.state.checkpoint.phase == "entry-admission"
            assert seen[0].held == WorkAmount(octets=1)
            with pytest.raises(RuntimeError, match="requires reconstruction"):
                session.progress()
        finally:
            for reservation in retained:
                reservation.close()
        seen[0].require_received()
    assert session.storage_received
