from __future__ import annotations

import sqlite3

import pytest
from riverhog_provenance._native_work import NativeWorkCapacity, WorkAmount
from riverhog_provenance.journal_session import (
    JournalValidationBudget,
    JournalValidationWorkBudget,
)
from test_native_epoch_integration import _reach
from test_owner_row_work import context


@pytest.fixture(params=[False, True], ids=["ordinary", "retained"])
def session(tmp_path, journal, catalog, journal_constructions, request):
    with context(tmp_path, journal, catalog, journal_constructions, request.param) as current:
        yield current
    assert current.storage_received


def capture_work(session, monkeypatch):
    original, seen = session._ledger.load, []

    def load():
        work = session._ledger.epoch.work
        if work is not None and not seen:
            seen.append(work)
        return original()

    monkeypatch.setattr(session._ledger, "load", load)
    return seen


def test_quarantine_copy_reservation_refuses_before_writer_and_keeps_original_pending_source(
    session, monkeypatch
):
    for _ in range(64):
        if session._input.pending:
            break
        session.advance()
    else:
        pytest.fail("exact source did not expose a pending frame")
    retained = hasattr(session._input, "state")
    before = session._input.state if retained else session._input.cursor
    size = before.pending.json_bytes if retained else len(session._input.buffer)
    progress, db = session.progress(), session._store.db
    changes, statements = db.total_changes, []
    seen = capture_work(session, monkeypatch)
    db.set_trace_callback(statements.append)
    try:
        with pytest.raises(NativeWorkCapacity):
            session.advance(
                JournalValidationBudget(work=JournalValidationWorkBudget(octets=6 * size + 3))
            )
    finally:
        db.set_trace_callback(None)
    assert not any(sql.startswith("BEGIN") for sql in statements)
    assert session.progress() == progress and db.total_changes == changes
    assert (session._input.state if retained else session._input.cursor) is before
    assert session._input.pending and session._replay._current is None
    assert session._failure is None and session._ledger.epoch.quiescent
    refused = seen.pop()
    assert refused.consumed.octets == 5 * size + 4
    assert refused.consumed.source_octets == (size if retained else 0)
    assert refused.held == WorkAmount()
    after = session.advance(
        JournalValidationBudget(work=JournalValidationWorkBudget(octets=6 * size + 4))
    )
    assert after.phase == "entry-admission"
    assert session._replay._current is not None and seen[0] is not refused
    assert seen[0].consumed.octets == 6 * size + 4
    assert seen[0].held == WorkAmount() and session._ledger.epoch.quiescent
    refused.require_received()
    seen[0].require_received()


def test_accepted_frame_copy_is_reserved_before_parent_or_prefix_writes(session, monkeypatch):
    _reach(session, "publish")
    current, prefix, db = session._replay._current, session._prefix, session._store.db
    raw = current.frame.json_bytes
    before, changes, statements = session.progress(), db.total_changes, []
    seen = capture_work(session, monkeypatch)
    db.set_trace_callback(statements.append)
    try:
        with pytest.raises(NativeWorkCapacity):
            session.advance(
                JournalValidationBudget(work=JournalValidationWorkBudget(octets=len(raw) - 1))
            )
    finally:
        db.set_trace_callback(None)
    assert not any(sql.startswith("BEGIN") for sql in statements)
    assert session.progress() == before and db.total_changes == changes
    assert session._replay._current is current and prefix.load().frame_count == 0
    refused = seen.pop()
    assert refused.consumed.octets == refused.consumed.source_octets == 0
    assert refused.held == WorkAmount() and session._ledger.epoch.quiescent
    after = session.advance(
        JournalValidationBudget(work=JournalValidationWorkBudget(octets=len(raw)))
    )
    assert after.accepted_frames == 1 and session._replay._current is None
    assert seen[0] is not refused
    assert seen[0].consumed.octets == len(raw) and seen[0].consumed.source_octets == 0
    assert seen[0].held == WorkAmount() and session._ledger.epoch.quiescent
    assert db.execute("SELECT value FROM frames").fetchone() == (raw,)
    assert db.execute("SELECT value FROM journal_validation_frame").fetchone() is None
    refused.require_received()
    seen[0].require_received()


def test_failed_original_frame_insert_keeps_copy_cost_and_rolls_back_whole_publication(
    session, monkeypatch
):
    _reach(session, "publish")
    current, prefix, db = session._replay._current, session._prefix, session._store.db
    raw = current.frame.json_bytes
    before_prefix = db.execute("SELECT checkpoint FROM journal_prefix").fetchone()
    before_parent = session._ledger.load()
    seen = capture_work(session, monkeypatch)

    def authorize(action, table, _column, _database, _trigger):
        return (
            sqlite3.SQLITE_DENY
            if action == sqlite3.SQLITE_INSERT and table == "frames"
            else sqlite3.SQLITE_OK
        )

    db.set_authorizer(authorize)
    try:
        with pytest.raises(sqlite3.DatabaseError, match="not authorized") as caught:
            session.advance(
                JournalValidationBudget(work=JournalValidationWorkBudget(octets=len(raw)))
            )
    finally:
        db.set_authorizer(None)
    assert session._failure is caught.value and not db.in_transaction
    assert db.execute("SELECT checkpoint FROM journal_prefix").fetchone() == before_prefix
    assert db.execute("SELECT value FROM frames").fetchone() is None
    assert db.execute("SELECT value FROM journal_validation_frame").fetchone() == (raw,)
    assert session._ledger._received is before_parent and session._replay._current is current
    assert seen[0].consumed.octets == len(raw) and seen[0].consumed.source_octets == 0
    assert seen[0].held == WorkAmount()
    with pytest.raises(RuntimeError, match="requires reconstruction"):
        prefix.load()
    with pytest.raises(RuntimeError, match="requires reconstruction"):
        _ = current.frame.document
    seen[0].require_received()
