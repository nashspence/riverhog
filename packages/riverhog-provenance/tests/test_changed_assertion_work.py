from __future__ import annotations

from contextlib import closing

import pytest
from riverhog_provenance import _entry_replay
from riverhog_provenance._changed_assertions import (
    ChangedAssertionCapacity,
    read_changed_assertion,
)
from riverhog_provenance._native_epoch import EpochContext
from riverhog_provenance._native_work import NativeWorkCapacity, WorkAmount, WorkQuantum
from riverhog_provenance.journal_session import (
    JournalValidationBudget,
    JournalValidationSession,
    JournalValidationWorkBudget,
)
from test_assertion_staging import connect, identity
from test_changed_assertions import Connection, prepare
from test_native_epoch_integration import _reach
from test_retained_journal_session import OwnedSource, directory, open_session


def quantum(rows=3):
    return WorkQuantum(WorkAmount(rows=rows, octets=4096, source_octets=1, transitions=1))


@pytest.mark.parametrize(
    "mode,probes,returned",
    [("end", 1, 0), ("missing", 2, 1), ("retired", 2, 1), ("active", 3, 3)],
)
def test_actual_indexed_probe_attempts_are_distinct_from_returned_rows(
    tmp_path, mode, probes, returned
):
    path = tmp_path / "assertions.sqlite3"
    prepare(path, 1, mode)
    with closing(connect(path, factory=Connection)) as db:
        db.execute("PRAGMA query_only=ON").close()
        epoch, work = EpochContext(db), quantum()
        with epoch.execution(None, work=work), db.observe() as metrics:
            selected = read_changed_assertion(
                db, after=identity(9999) if mode == "end" else None, byte_budget=4096, work=work
            )
        assert (selected is None) == (mode == "end")
        if selected is not None:
            assert (selected.raw is not None) == (mode == "active")
        assert metrics["sql_statements"] == probes
        assert metrics["returned_rows"] == returned
        assert metrics["changed_rows"] == 0 and metrics["sqlite_vm_instructions"] > probes
        assert work.consumed == WorkAmount(rows=probes)
        assert epoch.quiescent and epoch.work is None
        work.require_received()


@pytest.mark.parametrize("admitted", [1, 2])
def test_refusal_precedes_the_next_probe_and_larger_retry_keeps_original_custody(
    tmp_path, admitted
):
    path = tmp_path / "assertions.sqlite3"
    prepare(path, 1, "active")
    with closing(connect(path, factory=Connection)) as db:
        epoch, work = EpochContext(db), quantum(admitted)
        before = db.total_changes
        with db.observe() as metrics:
            with pytest.raises(NativeWorkCapacity):
                with epoch.execution(None, work=work):
                    read_changed_assertion(db, after=None, byte_budget=4096, work=work)
        assert metrics["sql_statements"] == metrics["returned_rows"] == admitted
        assert db.total_changes == before and epoch.quiescent
        assert work.consumed == WorkAmount(rows=admitted)
        work.require_received()
        retry = quantum()
        with epoch.execution(None, work=retry), db.observe() as metrics:
            assert read_changed_assertion(db, after=None, byte_budget=4096, work=retry).raw
        assert metrics["sql_statements"] == retry.consumed.rows == 3
        assert db.total_changes == before and epoch.quiescent
        retry.require_received()


def test_byte_capacity_refusal_charges_only_the_two_original_metadata_probes(tmp_path):
    path = tmp_path / "assertions.sqlite3"
    prepare(path, 1, "active", payload=64)
    with closing(connect(path, factory=Connection)) as db:
        work = quantum()
        with db.observe() as metrics:
            with pytest.raises(ChangedAssertionCapacity):
                read_changed_assertion(db, after=None, byte_budget=63, work=work)
        assert metrics["sql_statements"] == work.consumed.rows == 2
        assert metrics["returned_atom_octets"] == metrics["changed_rows"] == 0
        work.require_received()


@pytest.mark.parametrize(
    "fault,attempts",
    [("SELECT assertion_id", 1), ("SELECT length(value)", 2), ("SELECT value", 3)],
)
def test_original_fetch_failure_keeps_attempted_inspections_charged_and_cursors_received(
    tmp_path, fault, attempts
):
    path = tmp_path / "assertions.sqlite3"
    prepare(path, 1, "active")
    with closing(connect(path, factory=Connection)) as db:
        work = quantum()
        db.fault = fault
        with db.observe() as metrics:
            with pytest.raises(RuntimeError, match="injected fetch failure"):
                read_changed_assertion(db, after=None, byte_budget=4096, work=work)
        assert metrics["sql_statements"] == work.consumed.rows == attempts
        assert metrics["changed_rows"] == 0
        work.require_received()


@pytest.mark.parametrize("retained", [False, True])
@pytest.mark.parametrize("phase", ["row-admit", "generic-admission"])
def test_actual_session_row_selection_and_cold_row_restore_use_the_original_quantum(
    tmp_path, journal, catalog, journal_constructions, monkeypatch, retained, phase
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
        _reach(session, phase)
        current = session._replay._current
        if phase == "generic-admission":
            current.row_key, current.row_sha256, current.row, current.atom = None, None, None, None
        original, seen = _entry_replay.read_changed_assertion, []

        def selected(*args, **kwargs):
            work = kwargs["work"]
            assert work is session._ledger.epoch.work and not session._store.db.in_transaction
            seen.append(work)
            return original(*args, **kwargs)

        monkeypatch.setattr(_entry_replay, "read_changed_assertion", selected)
        before, changes = session.progress(), session._store.db.total_changes
        with pytest.raises(NativeWorkCapacity):
            session.advance(JournalValidationBudget(work=JournalValidationWorkBudget(rows=2)))
        assert session.progress() == before
        assert session._store.db.total_changes == changes
        assert session._failure is None and session._ledger.epoch.quiescent
        assert seen[0].consumed.rows == 2 and seen[0].consumed.source_octets == 0
        seen[0].require_received()
        after = session.advance(JournalValidationBudget(work=JournalValidationWorkBudget(rows=3)))
        assert after != before and len(seen) == 2 and seen[1] is not seen[0]
        assert seen[1].consumed.rows == 3 and seen[1].consumed.source_octets == 0
        seen[1].require_received()
        assert session._ledger.epoch.quiescent
    assert session.storage_received
