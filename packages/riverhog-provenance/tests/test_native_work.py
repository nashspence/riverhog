from __future__ import annotations

import sqlite3
from contextlib import closing

import pytest
from riverhog_provenance._entry_preparation import EntryPreparation
from riverhog_provenance._native_control import ChildControl
from riverhog_provenance._native_epoch import EpochContext
from riverhog_provenance._native_work import NativeWorkCapacity, WorkAmount, WorkQuantum
from riverhog_provenance._session_input import RetainedInput
from riverhog_provenance.errors import ProvenanceValidationError
from riverhog_provenance.journal_session import (
    JournalValidationBudget,
    JournalValidationSession,
    JournalValidationWorkBudget,
)
from sqlite_admission import SqliteProgressConnection
from test_native_epoch_integration import _reach
from test_retained_journal_session import OwnedSource, directory, open_session


def quantum():
    return WorkQuantum(WorkAmount(rows=8, octets=128, source_octets=64, transitions=4))


def test_outstanding_admission_cannot_be_spent_by_an_independent_child():
    work = quantum()
    with work.reserve(WorkAmount(rows=7, octets=100)) as first:
        first.consume(WorkAmount(rows=2, octets=31))
        before = work.consumed, work.held, work.remaining
        with pytest.raises(NativeWorkCapacity):
            work.reserve(WorkAmount(rows=2))
        assert (work.consumed, work.held, work.remaining) == before
        with work.reserve(WorkAmount(rows=1, source_octets=64)) as second:
            second.consume(WorkAmount(rows=1, source_octets=17))
        assert work.consumed == WorkAmount(rows=3, octets=31, source_octets=17)
        assert work.held == WorkAmount(rows=5, octets=69)
    work.require_received()
    assert work.remaining == WorkAmount(rows=5, octets=97, source_octets=47, transitions=4)
    with pytest.raises(RuntimeError, match="already received"):
        first.consume(WorkAmount(rows=1))
    first.close()
    assert work.consumed == WorkAmount(rows=3, octets=31, source_octets=17)


def test_overrun_cannot_spend_another_units_held_admission():
    work = quantum()
    with work.reserve(WorkAmount(rows=3)) as first:
        with work.reserve(WorkAmount(rows=5)):
            before = work.consumed, work.held
            with pytest.raises(NativeWorkCapacity):
                first.consume(WorkAmount(rows=4))
            assert (work.consumed, work.held) == before
            first.consume(WorkAmount(rows=3))
    assert work.remaining.rows == 5 and work.consumed.rows == 3
    work.require_received()


@pytest.mark.parametrize("value", [True, False, -1, 1.0, "1", None])
def test_work_amounts_require_exact_nonnegative_integers(value):
    with pytest.raises(ValueError):
        WorkAmount(rows=value)


@pytest.mark.parametrize("coordinate", ["rows", "octets", "source_octets", "transitions"])
def test_quantum_requires_positive_limits_for_every_dimension(coordinate):
    values = dict(rows=1, octets=1, source_octets=1, transitions=1)
    values[coordinate] = 0
    with pytest.raises(ValueError):
        WorkQuantum(WorkAmount(**values))


def test_rollback_does_not_replenish_actual_work_or_replace_the_original_quantum():
    with closing(sqlite3.connect(":memory:", factory=SqliteProgressConnection)) as db:
        db.execute("CREATE TABLE semantic(value TEXT)").close()
        db.executemany("INSERT INTO semantic VALUES(?)", [("before",), ("retained",)]).close()
        db.commit()
        epoch, work = EpochContext(db), quantum()
        failure = OSError("original semantic application failed")

        def mutate(original):
            assert original is epoch and original.work is work
            with work.reserve(WorkAmount(rows=4, octets=64)) as admitted:
                with closing(db.execute("SELECT value FROM semantic")) as rows:
                    values = rows.fetchall()
                admitted.consume(
                    WorkAmount(rows=len(values), octets=sum(len(row[0]) for row in values))
                )
                db.execute("DELETE FROM semantic").close()
                raise failure

        with pytest.raises(OSError) as caught:
            with epoch.execution(None, work=work):
                with pytest.raises(RuntimeError, match="already active"):
                    with epoch.execution(None, work=quantum()):
                        pytest.fail("a nested owner replaced the original quantum")
                epoch.apply(mutate, receive=lambda _value: None)
        assert caught.value is failure and epoch.work is None
        work.require_received()
        assert work.consumed == WorkAmount(rows=2, octets=14)
        with closing(db.execute("SELECT value FROM semantic")) as rows:
            assert rows.fetchall() == [("before",), ("retained",)]
        with pytest.raises(RuntimeError, match="requires reconstruction"):
            with epoch.execution(None, work=quantum()):
                pytest.fail("a new quantum reused the failed original epoch")


def test_actual_session_children_share_one_original_quantum_through_receiving(
    journal, catalog, monkeypatch
):
    with JournalValidationSession((journal,), attempt_id="a" * 32, catalog=catalog) as session:
        _reach(session, "entry")
        child, seen = session._replay._current.child, []
        prepare, apply, receive = (
            EntryPreparation.prepare,
            EntryPreparation._apply,
            ChildControl.receive,
        )

        def prepared(current, *args, **kwargs):
            seen.append(current.epoch.work)
            assert current is child and seen[0].consumed.transitions == 1
            return prepare(current, *args, **kwargs)

        def applied(current, batch):
            seen.append(current.epoch.work)
            return apply(current, batch)

        def received(control, pending):
            if control is child._control:
                seen.append(control.epoch.work)
                assert not session._store.db.in_transaction
                assert not control.epoch.quiescent
            return receive(control, pending)

        monkeypatch.setattr(EntryPreparation, "prepare", prepared)
        monkeypatch.setattr(EntryPreparation, "_apply", applied)
        monkeypatch.setattr(ChildControl, "receive", received)
        session.advance(JournalValidationBudget(work=JournalValidationWorkBudget(transitions=1)))
        assert len(seen) == 3 and all(item is seen[0] for item in seen)
        seen[0].require_received()
        assert seen[0].consumed.transitions == 1 and session._ledger.epoch.work is None
    assert session.storage_received


def test_empty_source_pull_still_consumes_one_control_transition(journal, catalog, monkeypatch):
    from riverhog_provenance.journal_session import _ChunkInput

    scan, seen = _ChunkInput.scan, []
    with JournalValidationSession(
        (b"", b"", journal), attempt_id="a" * 32, catalog=catalog
    ) as session:

        def scanned(current, octets):
            seen.append(session._ledger.epoch.work)
            return scan(current, octets)

        monkeypatch.setattr(_ChunkInput, "scan", scanned)
        for _ in range(2):
            assert (
                session.advance(
                    JournalValidationBudget(work=JournalValidationWorkBudget(transitions=1))
                ).source_octets
                == 0
            )
        assert len(seen) == 2 and seen[0] is not seen[1]
        assert all(
            work.consumed.transitions == 1 and not work.remaining.transitions for work in seen
        )
        assert all(work.consumed.rows == 2 for work in seen)
    assert session.storage_received


def test_unreceived_unit_cannot_return_successful_session_progress(journal, catalog, monkeypatch):
    original, retained = EntryPreparation.prepare, []
    with JournalValidationSession((journal,), attempt_id="a" * 32, catalog=catalog) as session:
        _reach(session, "entry")

        def prepare(child, *args, **kwargs):
            retained.append(child.epoch.work.reserve(WorkAmount(octets=1)))
            return original(child, *args, **kwargs)

        monkeypatch.setattr(EntryPreparation, "prepare", prepare)
        try:
            with pytest.raises(RuntimeError, match="unreceived original work"):
                session.advance()
            assert session._ledger.epoch.work is None
            with pytest.raises(RuntimeError, match="requires reconstruction"):
                session.progress()
        finally:
            for reservation in retained:
                reservation.close()
    assert session.storage_received


@pytest.mark.parametrize("transitions", [True, 0, -1, 1.0, None])
def test_session_control_transition_limit_requires_a_positive_integer(transitions):
    with pytest.raises(ValueError):
        JournalValidationBudget(work=JournalValidationWorkBudget(transitions=transitions))


def test_successful_private_commits_do_not_replenish_the_shared_quantum():
    with closing(sqlite3.connect(":memory:", factory=SqliteProgressConnection)) as db:
        db.execute("CREATE TABLE semantic(value TEXT)").close()
        db.executemany("INSERT INTO semantic VALUES(?)", [("one",), ("two",)]).close()
        db.commit()
        epoch = EpochContext(db)
        work = WorkQuantum(WorkAmount(rows=3, octets=100, source_octets=1, transitions=3))

        def mutate(original, admitted, limit):
            assert original is epoch and original.work is work
            with closing(db.execute("SELECT value FROM semantic LIMIT ?", (limit,))) as rows:
                values = rows.fetchall()
            admitted.consume(WorkAmount(rows=len(values)))
            db.execute("UPDATE semantic SET value=value||'!'").close()

        with epoch.execution(None, work=work):
            for count in (1, 2):
                with work.reserve(WorkAmount(rows=count)) as admitted:
                    epoch.apply(
                        lambda original, admission=admitted, limit=count: mutate(
                            original, admission, limit
                        ),
                        receive=lambda _value: None,
                    )
                assert epoch.quiescent and epoch.work is work
            with pytest.raises(NativeWorkCapacity):
                work.reserve(WorkAmount(rows=1))
            assert epoch.quiescent and work.consumed.rows == 3
        work.require_received()
        assert epoch.work is None
        with closing(db.execute("SELECT value FROM semantic")) as rows:
            assert rows.fetchall() == [("one!!",), ("two!!",)]


@pytest.mark.parametrize("different_epoch", [False, True])
def test_completed_quantum_cannot_attach_to_another_execution(different_epoch):
    with closing(sqlite3.connect(":memory:", factory=SqliteProgressConnection)) as db:
        first, work = EpochContext(db), quantum()
        with first.execution(None, work=work):
            work.consume(WorkAmount(transitions=1))
        next_owner = EpochContext(db) if different_epoch else first
        with pytest.raises(RuntimeError, match="single-invocation"):
            with next_owner.execution(None, work=work):
                pytest.fail("original work admission was attached to another execution")
        assert next_owner.quiescent and next_owner.work is None
        assert work.consumed.transitions == 1


@pytest.mark.parametrize("retained", [False, True])
@pytest.mark.parametrize("malformed", [False, True])
def test_actual_scan_accounts_the_returned_window_before_framing_or_cursor_publication(
    tmp_path, journal, catalog, journal_constructions, monkeypatch, retained, malformed
):
    seen = []
    if retained:
        source = OwnedSource(journal, catalog)
        if malformed:
            source.raw = b"X" + source.raw[1:]
        context = open_session(
            directory(tmp_path),
            source,
            catalog,
            create=True,
            journal_constructions=journal_constructions,
        )
        original = RetainedInput.scan

        def scan(current, *args):
            seen.append(current.ledger.epoch.work)
            return original(current, *args)

        monkeypatch.setattr(RetainedInput, "scan", scan)
    else:
        from riverhog_provenance.journal_session import _ChunkInput

        raw = b"X" + journal[1:] if malformed else journal
        context = JournalValidationSession((raw,), attempt_id="a" * 32, catalog=catalog)
        original = _ChunkInput.scan

        def scan(current, *args):
            seen.append(current.epoch.work)
            return original(current, *args)

        monkeypatch.setattr(_ChunkInput, "scan", scan)
    with context as session:
        before = session._input.cursor.byte_offset
        budget = JournalValidationBudget(
            source_octets=7, work=JournalValidationWorkBudget(transitions=1)
        )
        if malformed:
            with pytest.raises(ProvenanceValidationError):
                session.advance(budget)
            assert session._input.cursor.byte_offset == before
        else:
            assert session.advance(budget).source_octets == before + 7
        assert len(seen) == 1
        assert seen[0].consumed == WorkAmount(
            rows=7 if retained and not malformed else 2,
            octets=7,
            source_octets=7,
            transitions=1,
        )
        seen[0].require_received()
        assert session._ledger.epoch.work is None
    assert session.storage_received


def test_page_admission_and_aggregate_actual_work_are_independent(journal, catalog, monkeypatch):
    from riverhog_provenance import JournalValidationWorkBudget as PublicWorkBudget
    from riverhog_provenance.journal_session import _ChunkInput

    assert PublicWorkBudget is JournalValidationWorkBudget
    original, seen = _ChunkInput.scan, []
    aggregate = PublicWorkBudget(rows=37, octets=1000, source_octets=131072, transitions=2)
    budget = JournalValidationBudget(rows=1, source_octets=7, work=aggregate)
    with JournalValidationSession((journal,), attempt_id="a" * 32, catalog=catalog) as session:

        def scan(current, *args):
            seen.append(current.epoch.work)
            return original(current, *args)

        monkeypatch.setattr(_ChunkInput, "scan", scan)
        assert session.advance(budget).source_octets == 7
        assert seen[0].limits == WorkAmount(
            rows=37, octets=1000, source_octets=131072, transitions=2
        )
        assert seen[0].consumed == WorkAmount(rows=2, octets=7, source_octets=7, transitions=1)
        seen[0].require_received()
    assert session.storage_received


@pytest.mark.parametrize("coordinate", ["rows", "octets", "source_octets", "transitions"])
@pytest.mark.parametrize("value", [True, 0, -1, 1.0, None])
def test_aggregate_session_limits_require_exact_positive_integers(coordinate, value):
    with pytest.raises(ValueError):
        JournalValidationWorkBudget(**{coordinate: value})


def test_an_unadmitted_source_window_is_refused_before_pulling_retained_bytes(
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
        before = session._input.state
        original, seen = RetainedInput.scan, []

        def scan(current, *args):
            seen.append(current.ledger.epoch.work)
            return original(current, *args)

        monkeypatch.setattr(RetainedInput, "scan", scan)
        tiny = JournalValidationBudget(
            source_octets=7, work=JournalValidationWorkBudget(source_octets=6)
        )
        with pytest.raises(NativeWorkCapacity):
            session.advance(tiny)
        assert source.calls == [] and session._input.state is before
        assert session._failure is None and seen[0].consumed == WorkAmount(rows=2, transitions=1)
        seen[0].require_received()
        assert session.advance(JournalValidationBudget(source_octets=7)).source_octets == 7
    assert session.storage_received


@pytest.mark.parametrize("retained", [False, True])
def test_complete_frame_transfer_uses_aggregate_budget_not_the_scan_window(
    tmp_path, journal, catalog, journal_constructions, monkeypatch, retained
):
    if retained:
        source = OwnedSource(journal, catalog)
        context = open_session(
            directory(tmp_path),
            source,
            catalog,
            create=True,
            journal_constructions=journal_constructions,
        )
        owner = RetainedInput
    else:
        from riverhog_provenance.journal_session import _ChunkInput

        context = JournalValidationSession((journal,), attempt_id="a" * 32, catalog=catalog)
        owner = _ChunkInput
    with context as session:
        for _ in range(64):
            if session._input.pending:
                break
            session.advance()
        else:
            pytest.fail("small exact source did not expose its complete frame")
        size = session._input.state.pending.json_bytes if retained else len(session._input.buffer)
        assert size > 7
        if retained:
            source.calls.clear()
        original, seen = owner.frame, []

        def frame(current, *args):
            seen.append(session._ledger.epoch.work)
            return original(current, *args)

        monkeypatch.setattr(owner, "frame", frame)
        assert session.advance(JournalValidationBudget(source_octets=7)).phase == "entry-admission"
        assert len(seen) == 1
        assert seen[0].consumed == WorkAmount(
            rows=10, octets=3 * size, source_octets=size if retained else 0, transitions=1
        )
        seen[0].require_received()
        if retained:
            assert source.calls == [(session._input.state.pending.byte_offset + 1, size)]
    assert session.storage_received


@pytest.mark.parametrize("retained", [False, True])
def test_unadmitted_frame_transfer_keeps_the_original_pending_atom_without_reading(
    tmp_path, journal, catalog, journal_constructions, monkeypatch, retained
):
    if retained:
        source = OwnedSource(journal, catalog)
        context = open_session(
            directory(tmp_path),
            source,
            catalog,
            create=True,
            journal_constructions=journal_constructions,
        )
    else:
        context = JournalValidationSession((journal,), attempt_id="a" * 32, catalog=catalog)
    with context as session:
        for _ in range(64):
            if session._input.pending:
                break
            session.advance()
        else:
            pytest.fail("small exact source did not expose its complete frame")
        before = session._input.state if retained else session._input.cursor
        size = before.pending.json_bytes if retained else len(session._input.buffer)
        if retained:
            source.calls.clear()
            limits = JournalValidationWorkBudget(source_octets=size - 1)
        else:
            limits = JournalValidationWorkBudget(octets=size - 1)
        with pytest.raises(NativeWorkCapacity):
            session.advance(JournalValidationBudget(work=limits))
        assert (session._input.state if retained else session._input.cursor) is before
        assert session._input.pending and session._failure is None
        if retained:
            assert source.calls == []
        assert session.advance().phase == "entry-admission"
    assert session.storage_received
