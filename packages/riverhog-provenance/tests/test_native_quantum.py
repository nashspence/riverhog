from __future__ import annotations

import itertools
import sqlite3
from contextlib import contextmanager
from dataclasses import replace

import pytest
from riverhog_provenance import validate_journal
from riverhog_provenance._generic_semantics import GenericSemanticsCapacity
from riverhog_provenance._native_work import NativeWorkCapacity, WorkAmount
from riverhog_provenance.errors import JournalValidationCapacity
from riverhog_provenance.journal_session import (
    JournalValidationBudget,
    JournalValidationSession,
    JournalValidationWorkBudget,
)
from test_journal_session import finish
from test_native_epoch_integration import _Connection
from test_retained_journal_session import OwnedSource, directory, open_session


@contextmanager
def session_case(tmp_path, journal, catalog, constructions, retained):
    if retained:
        with open_session(
            directory(tmp_path),
            OwnedSource(journal, catalog),
            catalog,
            create=True,
            journal_constructions=constructions,
        ) as session:
            yield session
    else:
        with JournalValidationSession((journal,), attempt_id="a" * 32, catalog=catalog) as session:
            yield session


@pytest.mark.parametrize("retained", [False, True], ids=["stream", "retained"])
def test_actual_multiple_epochs_share_original_work_and_render_progress_once(
    tmp_path, journal, catalog, journal_constructions, monkeypatch, retained
):
    with validate_journal(journal, catalog=catalog) as expected:
        with session_case(tmp_path, journal, catalog, journal_constructions, retained) as session:
            original, rendered, units, statements = session._advance_unit, [], [], []
            render = session._progress

            def unit(budget):
                work = session._ledger.epoch.work
                assert work is not None and session._ledger.epoch.quiescent
                assert work.held == WorkAmount()
                units.append(work)
                result = original(budget)
                assert session._ledger.epoch.quiescent and work.held == WorkAmount()
                return result

            def progress(snapshot):
                rendered.append(snapshot)
                return render(snapshot)

            with monkeypatch.context() as patch:
                patch.setattr(session, "_advance_unit", unit)
                patch.setattr(session, "_progress", progress)
                session._store.db.set_trace_callback(statements.append)
                try:
                    observed = session.advance(
                        JournalValidationBudget(work=JournalValidationWorkBudget(transitions=16))
                    )
                finally:
                    session._store.db.set_trace_callback(None)
            assert 1 < len(units) <= 16 and all(work is units[0] for work in units)
            assert len(rendered) == 1
            assert sum(sql == "COMMIT" for sql in statements) >= 2
            assert units[0].consumed.transitions == len(units)
            units[0].require_received()
            assert session._ledger.epoch.work is None
            assert observed.accepted_frames <= len(expected.frames)
            assert finish(session).complete
            result = session.result()
        with result:
            assert result.materialize() == expected.materialize()
            assert result.journal_sha256 == expected.journal_sha256
        assert session.storage_received


@pytest.mark.parametrize("coordinate", ["rows", "octets", "source_octets"])
def test_later_original_capacity_yields_received_progress_without_reset_or_writer(
    journal, catalog, monkeypatch, coordinate
):
    amount = {"rows": 4, "octets": min(len(journal), 65536), "source_octets": 1}[coordinate]
    limits = replace(JournalValidationWorkBudget(), **{coordinate: amount})
    budget = JournalValidationBudget(
        source_octets=1 if coordinate == "source_octets" else 65536, work=limits
    )
    with JournalValidationSession((journal,), attempt_id="a" * 32, catalog=catalog) as session:
        original, works, statements = session._advance_unit, [], []

        def unit(budget):
            works.append(session._ledger.epoch.work)
            return original(budget)

        with monkeypatch.context() as patch:
            patch.setattr(session, "_advance_unit", unit)
            session._store.db.set_trace_callback(statements.append)
            try:
                progress = session.advance(budget)
            finally:
                session._store.db.set_trace_callback(None)
        assert len(works) == 2 and works[0] is works[1]
        work = works[0]
        assert getattr(work.remaining, coordinate) == 0
        assert work.consumed.transitions == 2 and work.held == WorkAmount()
        work.require_received()
        assert progress.source_octets > 0 and progress.accepted_frames == 0
        assert not any(sql.startswith("BEGIN") for sql in statements)
        assert session._failure is None and session._ledger.epoch.quiescent
        assert finish(session).complete
        with session.result() as result:
            assert result.journal_bytes == len(journal)
    assert session.storage_received


def test_first_unit_capacity_refusal_preserves_retry_and_does_not_pull_source(journal, catalog):
    pulls = []

    def source():
        pulls.append(1)
        yield journal

    with JournalValidationSession(source(), attempt_id="a" * 32, catalog=catalog) as session:
        before = session.progress()
        with pytest.raises(NativeWorkCapacity):
            session.advance(JournalValidationBudget(work=JournalValidationWorkBudget(rows=1)))
        assert not pulls and session.progress() == before
        assert session._failure is None and session._ledger.epoch.quiescent
        assert finish(session).complete and pulls == [1]
        session.result().close()
    assert session.storage_received


@pytest.mark.parametrize("transitions", [1, 2, 16, 64])
def test_empty_sources_spend_every_attempted_transition_in_one_original_quantum(
    catalog, monkeypatch, transitions
):
    with JournalValidationSession(
        itertools.repeat(b""), attempt_id="a" * 32, catalog=catalog
    ) as session:
        original, works = session._advance_unit, []

        def unit(budget):
            works.append(session._ledger.epoch.work)
            return original(budget)

        monkeypatch.setattr(session, "_advance_unit", unit)
        progress = session.advance(
            JournalValidationBudget(work=JournalValidationWorkBudget(transitions=transitions))
        )
        assert len(works) == transitions and all(work is works[0] for work in works)
        assert works[0].consumed.transitions == transitions and not works[0].remaining.transitions
        assert works[0].held == WorkAmount() and session._ledger.epoch.work is None
        assert not progress.complete and progress.source_octets == progress.accepted_frames == 0
    assert session.storage_received


@pytest.mark.parametrize(
    "capacity", [NativeWorkCapacity, JournalValidationCapacity, GenericSemanticsCapacity]
)
def test_original_capacity_class_callback_is_not_mistaken_for_a_partial_yield(
    journal, catalog, monkeypatch, capacity
):
    with JournalValidationSession((journal,), attempt_id="a" * 32, catalog=catalog) as session:
        original, units = session._advance_unit, []
        cause = capacity("original owner cancellation")

        def check():
            if len(units) == 2:
                raise cause

        def unit(budget):
            units.append(session._ledger.epoch.work)
            session._ledger.epoch.check()
            return original(budget)

        monkeypatch.setattr(session, "_advance_unit", unit)
        with pytest.raises(capacity) as caught:
            session.advance(check=check)
        assert caught.value is cause and len(units) == 2
        assert units[0] is units[1] and units[0].held == WorkAmount()
        assert session._ledger.epoch.quiescent and session._ledger.epoch.work is None
    assert session.storage_received


@pytest.mark.parametrize("failure", ["before", "after"])
def test_failure_of_a_later_commit_does_not_publish_pending_control(
    journal, catalog, monkeypatch, failure
):
    def connect(*args, **kwargs):
        return sqlite3.connect(*args, factory=_Connection, **kwargs)

    with JournalValidationSession(
        (journal,), attempt_id="a" * 32, catalog=catalog, connection_factory=connect
    ) as session:
        original, received = session._advance_unit, []

        def unit(budget):
            if len(received) == 2:
                session._store.db.failure = failure
            result = original(budget)
            received.append(result)
            return result

        monkeypatch.setattr(session, "_advance_unit", unit)
        with pytest.raises(OSError, match="original private commit") as caught:
            session.advance()
        session._store.db.failure = None
        assert len(received) == 2
        assert session._ledger._control.state is received[-1]
        assert not session._ledger.epoch.quiescent and session._ledger.epoch.work is None
        with pytest.raises(RuntimeError, match="requires reconstruction"):
            session.progress()
        with pytest.raises(RuntimeError, match="session failed") as reused:
            session.advance()
        assert reused.value.__cause__ is caught.value
    assert session.storage_received


def test_original_cancellation_between_received_epochs_prevents_the_next_unit(
    journal, catalog, monkeypatch
):
    with JournalValidationSession((journal,), attempt_id="a" * 32, catalog=catalog) as session:
        original, received = session._advance_unit, []
        cause = OSError("owner cancelled between private epochs")

        def unit(budget):
            result = original(budget)
            received.append(result)
            return result

        def check():
            if len(received) == 2:
                assert session._ledger.epoch.quiescent
                raise cause

        monkeypatch.setattr(session, "_advance_unit", unit)
        with pytest.raises(OSError) as caught:
            session.advance(check=check)
        assert caught.value is cause and len(received) == 2
        assert session._ledger._control.state is received[-1]
        assert session._ledger.epoch.quiescent and session._ledger.epoch.work is None
        with pytest.raises(RuntimeError, match="session failed") as reused:
            session.advance()
        assert reused.value.__cause__ is cause
    assert session.storage_received


def test_private_unit_cannot_be_used_as_an_alternate_unbudgeted_executor(journal, catalog):
    with JournalValidationSession((journal,), attempt_id="a" * 32, catalog=catalog) as session:
        before = session.progress()
        with pytest.raises(RuntimeError, match="original invocation quantum"):
            session._advance_unit(JournalValidationBudget())
        assert session.progress() == before and not session._input.pending
        assert session._ledger.epoch.quiescent
    assert session.storage_received


def test_later_child_page_capacity_preserves_refusal_and_received_pending_progress(
    journal, catalog
):
    from riverhog_provenance import parse_journal
    from riverhog_provenance._entry_replay import EntryReplayCapacity

    with JournalValidationSession((journal,), attempt_id="a" * 32, catalog=catalog) as session:
        before = session._ledger.load()
        prefix_octets = sum(
            len(value.encode()) if isinstance(value, str) else len(value) for value in before.prefix
        )
        minimum = 32768 + 16 * (
            len(session._ledger.binding) + prefix_octets + len(before.checkpoint.to_json().encode())
        )
        atom = parse_journal(journal)[0]
        budget = JournalValidationBudget(octets=2 * len(atom.json_bytes) + 65535)
        assert budget.octets >= minimum
        statements = []
        session._store.db.set_trace_callback(statements.append)
        try:
            with pytest.raises(EntryReplayCapacity, match="existing frame atom"):
                session.advance(budget)
        finally:
            session._store.db.set_trace_callback(None)
        progress = session.progress()
        assert progress.source_octets > 0 and session._input.pending
        assert not any(sql.startswith("BEGIN") for sql in statements)
        assert session._failure is None and session._ledger.epoch.quiescent
        with pytest.raises(EntryReplayCapacity, match="existing frame atom"):
            session.advance(budget)
        assert session.progress() == progress and session._input.pending
        assert finish(session).complete
        session.result().close()
    assert session.storage_received
