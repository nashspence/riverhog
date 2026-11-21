from __future__ import annotations

from contextlib import closing

import pytest
from riverhog_provenance._changed_assertions import read_changed_assertion
from riverhog_provenance._native_epoch import EpochContext
from riverhog_provenance._native_work import NativeWorkCapacity, WorkAmount, WorkQuantum
from riverhog_provenance.journal_session import (
    JournalValidationSession,
    JournalValidationWorkBudget,
)
from test_assertion_staging import connect
from test_changed_assertions import Connection, prepare
from test_native_epoch_integration import _reach
from test_retained_journal_session import OwnedSource, directory, open_session


def quantum(octets):
    # These cases isolate byte admission; use the owning default for row work.
    return WorkQuantum(
        WorkAmount(
            rows=JournalValidationWorkBudget().rows,
            octets=octets,
            source_octets=1,
            transitions=1,
        )
    )


@pytest.fixture(params=[False, True], ids=["ordinary", "retained"])
def session(tmp_path, journal, catalog, journal_constructions, request):
    context = (
        open_session(
            directory(tmp_path),
            OwnedSource(journal, catalog),
            catalog,
            create=True,
            journal_constructions=journal_constructions,
        )
        if request.param
        else JournalValidationSession((journal,), attempt_id="a" * 32, catalog=catalog)
    )
    with context as current:
        _reach(current, "entry")
        yield current
    assert current.storage_received


def test_selected_payload_byte_admission_refuses_before_query_and_preserves_retry(tmp_path):
    path = tmp_path / "assertions.sqlite3"
    prepare(path, 1, "active", payload=64)
    with closing(connect(path, factory=Connection)) as db:
        epoch, work = EpochContext(db), quantum(63)
        before = db.total_changes
        with db.observe() as metrics:
            with pytest.raises(NativeWorkCapacity):
                with epoch.execution(None, work=work):
                    read_changed_assertion(db, after=None, byte_budget=64, work=work)
        assert metrics["sql_statements"] == 2 and metrics["returned_atom_octets"] == 0
        assert work.consumed == WorkAmount(rows=2) and work.held == WorkAmount()
        assert db.total_changes == before and epoch.quiescent
        retry = quantum(64)
        with epoch.execution(None, work=retry), db.observe() as metrics:
            selected = read_changed_assertion(db, after=None, byte_budget=64, work=retry)
        assert selected.raw == b"x" * 64
        assert retry.consumed == WorkAmount(rows=3, octets=64)
        assert metrics["returned_atom_octets"] == 64 and db.total_changes == before
        work.require_received()
        retry.require_received()


def test_original_payload_fetch_failure_keeps_its_admitted_copy_attempt_charged(tmp_path):
    path = tmp_path / "assertions.sqlite3"
    prepare(path, 1, "active", payload=64)
    with closing(connect(path, factory=Connection)) as db:
        work = quantum(64)
        db.fault = "SELECT value"
        with db.observe() as metrics:
            with pytest.raises(RuntimeError, match="injected fetch failure"):
                read_changed_assertion(db, after=None, byte_budget=64, work=work)
        assert metrics["sql_statements"] == 3 and metrics["returned_atom_octets"] == 64
        assert work.consumed == WorkAmount(rows=3, octets=64)
        assert work.held == WorkAmount() and metrics["changed_rows"] == 0
        work.require_received()


def test_cold_current_frame_admits_sql_copy_before_fetch_and_retries_exact_owner(session):
    replay, epoch = session._replay, session._ledger.epoch
    before = session._ledger.load()
    original = replay._current
    size = len(original.frame.json_bytes)
    replay._current = None
    changes, statements = replay.db.total_changes, []
    small = quantum(size - 1)
    replay.db.set_trace_callback(statements.append)
    try:
        with pytest.raises(NativeWorkCapacity):
            with epoch.execution(None, work=small):
                replay._entry(before.checkpoint)
    finally:
        replay.db.set_trace_callback(None)
    assert not any(sql.startswith(("SELECT value", "BEGIN")) for sql in statements)
    assert small.consumed == WorkAmount(rows=1) and small.held == WorkAmount()
    assert replay._current is None and epoch.quiescent
    retry = quantum(5 * size + 4)
    with epoch.execution(None, work=retry):
        restored = replay._entry(before.checkpoint)
    assert restored is replay._current and restored is not original
    assert restored.frame.json_bytes == original.frame.json_bytes
    assert retry.consumed == WorkAmount(rows=3, octets=5 * size + 4)
    assert session._ledger.load() == before and replay.db.total_changes == changes
    small.require_received()
    retry.require_received()
    warm = quantum(1)
    with epoch.execution(None, work=warm):
        assert replay._entry(before.checkpoint) is restored
    assert warm.consumed == warm.held == WorkAmount()
    warm.require_received()


def test_changed_cold_frame_charges_copy_and_hash_without_parsing_or_publication(session):
    replay, epoch = session._replay, session._ledger.epoch
    before = session._ledger.load()
    raw = replay._current.frame.json_bytes
    replay._current = None
    replay.db.execute(
        "UPDATE journal_validation_frame SET value=? WHERE singleton=1", (b"X" + raw[1:],)
    ).close()
    replay.db.commit()
    work = quantum(2 * len(raw))
    with pytest.raises(RuntimeError, match="retained entry atom is missing or changed"):
        with epoch.execution(None, work=work):
            replay._entry(before.checkpoint)
    assert replay._current is None and epoch.quiescent
    assert work.consumed == WorkAmount(rows=2, octets=2 * len(raw))
    assert work.held == WorkAmount() and session._ledger.load() == before
    work.require_received()


def test_row_decode_capacity_refuses_before_parent_writer_and_reuses_original_owner(session):
    _reach(session, "row-admit")
    replay, epoch = session._replay, session._ledger.epoch
    before, current = session._ledger.load(), replay._current
    selected = read_changed_assertion(
        replay.db, after=before.checkpoint.row_after, byte_budget=1024 * 1024
    )
    assert selected is not None and selected.raw
    size, changes, statements = len(selected.raw), replay.db.total_changes, []
    assert current.row is None
    small = quantum(3 * size - 1)
    replay.db.set_trace_callback(statements.append)
    try:
        with pytest.raises(NativeWorkCapacity):
            with epoch.execution(None, work=small):
                replay._admit_row(before, current)
    finally:
        replay.db.set_trace_callback(None)
    assert not any(sql.startswith("BEGIN") for sql in statements)
    assert small.consumed == WorkAmount(rows=3, octets=2 * size)
    assert small.held == WorkAmount() and epoch.quiescent and current.row is None
    assert session._ledger.load() == before and replay.db.total_changes == changes
    retry = quantum(3 * size)
    with epoch.execution(None, work=retry):
        after = replay._admit_row(before, current)
    assert after.checkpoint.phase == "generic-admission" and current.row
    assert retry.consumed.octets == 3 * size and retry.consumed.source_octets == 0
    assert retry.held == WorkAmount() and epoch.quiescent
    warm = quantum(1)
    with epoch.execution(None, work=warm):
        assert replay._row(after.checkpoint, current) is current.row
    assert warm.consumed == WorkAmount() and warm.held == WorkAmount()
    for work in (small, retry, warm):
        work.require_received()


def test_changed_cold_assertion_charges_copy_and_hash_before_decode(session):
    _reach(session, "generic-admission")
    replay, epoch = session._replay, session._ledger.epoch
    before, current = session._ledger.load(), replay._current
    selected = read_changed_assertion(
        replay.db, after=before.checkpoint.row_after, byte_budget=1024 * 1024
    )
    raw = selected.raw
    current.row_key, current.row_sha256, current.row, current.atom = None, None, None, None
    replay.db.execute(
        "UPDATE assertions SET value=? WHERE assertion_id=?",
        (b"X" + raw[1:], selected.assertion_id),
    ).close()
    replay.db.commit()
    work = quantum(2 * len(raw))
    with pytest.raises(RuntimeError, match="retained assertion atom differs"):
        with epoch.execution(None, work=work):
            replay._row(before.checkpoint, current)
    assert work.consumed == WorkAmount(rows=3, octets=2 * len(raw))
    assert work.held == WorkAmount() and epoch.quiescent and current.row is None
    assert session._ledger.load() == before
    work.require_received()
