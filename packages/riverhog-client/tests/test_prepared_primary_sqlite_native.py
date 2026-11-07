"""SQL isolation of native transaction roles; compiled NTFS proofs run separately."""

from __future__ import annotations

import os
import sqlite3
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_client import PrimaryStateCapacity, PrimaryStateError
from riverhog_client._prepared_primary_sqlite import SqlitePrimaryLimits, TransactionalPrimaryFiles
from riverhog_client._prepared_primary_sqlite_native import (
    NativeSqliteIncrease,
    NativeSqliteTransactions,
    start,
)
from riverhog_client._prepared_primary_sqlite_operations import (
    SqliteOperationBank,
    SqliteOperationGrants,
)

from tests.support.prepared_primary import SqlitePrimaryTestGuard

pytestmark = pytest.mark.skipif(os.name == "nt", reason="SQL role isolation uses the Unix VFS")
LIMITS = SqlitePrimaryLimits(2 * 1024 * 1024, 1024 * 1024, 128 * 1024)
BANK = SqliteOperationBank(
    512, 8 * 1024 * 1024, 65536, 8 * 1024 * 1024, 65536, 262144, 0, 1 << 40, 1 << 60, 1 << 40
)
GRANTS = SqliteOperationGrants(BANK, BANK)


class Guard(SqlitePrimaryTestGuard):
    def __init__(self, path):
        super().__init__(path)
        self.arms = []
        self.files = None
        self.events = []

    def arm_operation(self, grants, limits, *, database_pages=None, journal_bytes=None):
        caps = dict(database_pages=database_pages, journal_bytes=journal_bytes)
        if self.files is not None and self.files._connection is not None:
            assert not self.files._connection.in_transaction
        self.events.append("arm")
        self.arms.append((grants, limits, caps))
        return SimpleNamespace()


def opened(path, *, initialize=False, operations=GRANTS):
    if initialize:
        path.touch(exist_ok=False)
    guard = Guard(path)
    files = TransactionalPrimaryFiles(
        guard,
        allocation_descriptor=b"native roles",
        limits=LIMITS,
        initialize=initialize,
        operations=operations,
    )
    guard.files = files
    return files, guard


def managed(path, *, operations=GRANTS):
    files, guard = opened(path, initialize=True, operations=operations)
    start(files, guard)
    return files, guard


def dependencies(files):
    plan = dict(prefix_lengths=(8201, 16), workspace_bytes=65536)
    for _ in range(32):
        if files.reserve_journal_step(**plan).done:
            break
    else:
        pytest.fail("fixture journal did not finish")
    plan = dict(
        free_pages=8,
        target_pages=files.database_reserve().target_pages
        or files.space_usage().database_pages + 32,
        workspace_bytes=128 * 1024,
    )
    for _ in range(256):
        if files.reserve_database_step(**plan).done:
            return
    pytest.fail("fixture database did not finish")


def test_native_start_closes_reconciled_pager_before_fresh_forward_and_keeps_custodian(tmp_path):
    files, guard = opened(tmp_path / "state.sqlite3", initialize=True)
    old = files._connection
    with files:
        assert start(files, guard) is files
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            old.execute("SELECT 1")
        assert files._guard is guard
        assert isinstance(files._native_transactions, NativeSqliteTransactions)
        assert guard.arms == [(GRANTS, LIMITS, {"database_pages": None, "journal_bytes": None})]
        assert files.operation_grants().grants == GRANTS


def test_native_transaction_arms_once_before_begin_and_does_not_renew_inside_it(tmp_path):
    files, guard = managed(tmp_path / "state.sqlite3")
    with files:
        guard.events.clear()
        files._db().set_trace_callback(lambda query: guard.events.append(query))
        arms = len(guard.arms)
        with files._transaction() as db:
            files.resource_grants()
            files.operation_grants()
            with db.blobopen("primary_format", "barrier", 1) as blob:
                raw = blob.read(8)
                blob.seek(0)
                blob.write((int.from_bytes(raw, "big") + 1).to_bytes(8, "big"))
        assert len(guard.arms) == arms + 1
        assert guard.events.index("arm") < guard.events.index("BEGIN IMMEDIATE")
        assert guard.events.index("BEGIN IMMEDIATE") < guard.events.index("COMMIT")


def test_native_payload_and_fixed_progress_use_backing_but_preparation_keeps_original_caps(
    tmp_path,
):
    files, guard = managed(tmp_path / "state.sqlite3")
    with files:
        files.prepare_controls(("receipt",), byte_limit=8192)
        files.put_control("receipt", b"original receipt", byte_limit=8192)
        dependencies(files)
        ready = files.prepare_physical_ready()
        original = files.put_bytes("payload", (b"original payload",), byte_limit=16)
        assert guard.arms[-1][2] == {
            "database_pages": ready.backed_pages,
            "journal_bytes": ready.journal_bytes,
        }
        from riverhog_client._prepared_primary_sqlite_ready import deplete

        with files._transaction():
            deplete(files)
        assert files.physical_readiness().phase == "depleted"
        files.put_control("receipt", b"new receipt", byte_limit=8192, expected=b"original receipt")
        assert guard.arms[-1][2] == {
            "database_pages": files.space_usage().database_pages,
            "journal_bytes": files.journal_reserved_bytes(),
        }
        before = len(guard.arms)
        consumed = []

        def source():
            consumed.append(True)
            yield b"new"

        with pytest.raises(PrimaryStateCapacity, match="readiness"):
            files.put_bytes("refused", source(), byte_limit=3)
        assert not consumed and len(guard.arms) == before
        with files._transaction(preparing=True):
            pass
        assert guard.arms[-1][2] == {"database_pages": None, "journal_bytes": None}
        with files.verified(original) as stream:
            assert stream.read() == b"original payload"


@pytest.mark.parametrize(
    "fault", ["database-end", "database-allocation", "journal-end", "journal-allocation"]
)
def test_native_progress_refuses_lost_backing_before_bank_replacement(tmp_path, monkeypatch, fault):
    files, guard = managed(tmp_path / "state.sqlite3")
    with files:
        dependencies(files)
        files.prepare_physical_ready()
        database, journal = guard.file_space()
        if fault == "database-end":
            database = replace(database, end_bytes=database.end_bytes - 1)
        elif fault == "database-allocation":
            database = replace(database, allocated_bytes=database.end_bytes - 1)
        elif fault == "journal-end":
            journal = replace(journal, end_bytes=files.journal_reserved_bytes() - 1)
        else:
            journal = replace(journal, allocated_bytes=files.journal_reserved_bytes() - 1)
        monkeypatch.setattr(guard, "file_space", lambda: (database, journal))
        arms, changes = len(guard.arms), files._db().total_changes
        with pytest.raises(PrimaryStateCapacity, match="backing"):
            with files._transaction():
                pytest.fail("lost backing entered mutation")
        assert len(guard.arms) == arms and files._db().total_changes == changes
        assert not files._poisoned


def test_native_payload_rechecks_selected_profile_under_begin_before_input(tmp_path, monkeypatch):
    from riverhog_client import _prepared_primary_sqlite_ready as ready_owner

    path = tmp_path / "state.sqlite3"
    files, guard = managed(path)
    with files:
        dependencies(files)
        ready = files.prepare_physical_ready()
        profiles = iter((ready, replace(ready, free_pages=ready.free_pages - 1)))
        monkeypatch.setattr(ready_owner, "payload_profile", lambda _files: next(profiles))
        entered = []
        with pytest.raises(PrimaryStateError, match="changed before BEGIN"):
            with files._transaction(payload=True):
                entered.append(True)
        assert not entered and files._poisoned


def test_native_ready_requires_fixed_progress_pages_and_explicit_increase_can_pay_update(tmp_path):
    small = replace(GRANTS, forward=replace(BANK, database_pages=15))
    files, guard = managed(tmp_path / "state.sqlite3", operations=small)
    with files:
        dependencies(files)
        with pytest.raises(PrimaryStateCapacity, match="preserve progress"):
            files.prepare_physical_ready()
        assert files.physical_readiness().phase == "preparing"
        files.increase_resource_grants(
            database_bytes=LIMITS.database_bytes,
            journal_bytes=LIMITS.journal_bytes,
            operations=GRANTS,
        )
        assert guard.arms[-1][0] == GRANTS
        assert files.operation_grants().grants == GRANTS
        dependencies(files)
        assert files.prepare_physical_ready().phase == "ready"


@pytest.mark.parametrize("fault", ["database", "journal", "forward", "recovery"])
def test_native_increase_cannot_revoke_original_resources_before_arming(tmp_path, fault):
    files, guard = managed(tmp_path / "state.sqlite3")
    with files:
        limits, grants = LIMITS, GRANTS
        if fault == "database":
            limits = replace(limits, database_bytes=limits.database_bytes - 4096)
        elif fault == "journal":
            limits = replace(limits, journal_bytes=limits.journal_bytes - 4096)
        else:
            grants = replace(grants, **{fault: replace(BANK, read_calls=BANK.read_calls - 1)})
        before = len(guard.arms)
        with pytest.raises(PrimaryStateError, match="revoked"):
            with files._transaction(increase=NativeSqliteIncrease(limits, grants)):
                pytest.fail("revoked increase entered mutation")
        assert len(guard.arms) == before and not files._poisoned


def test_native_failed_selection_cannot_silently_renew_the_bank(tmp_path, monkeypatch):
    files, guard = managed(tmp_path / "state.sqlite3")
    with files:

        def failed():
            raise PrimaryStateCapacity("selection exhausted")

        monkeypatch.setattr(files, "operation_grants", failed)
        before = len(guard.arms)
        with pytest.raises(PrimaryStateCapacity, match="selection exhausted"):
            with files._transaction():
                pytest.fail("exhausted selection renewed admission")
        assert len(guard.arms) == before


@pytest.mark.parametrize("fault", ["child", "guard", "transaction", "profile"])
def test_native_start_refuses_unsettled_or_unowned_input_without_replacing_pager(tmp_path, fault):
    operations = None if fault == "profile" else GRANTS
    files, guard = opened(tmp_path / "state.sqlite3", initialize=True, operations=operations)
    with files:
        target = files.child("child", initialize=True) if fault == "child" else files
        owner = Guard(guard.path) if fault == "guard" else guard
        if fault == "transaction":
            files._db().execute("BEGIN").close()
        old = files._connection
        with pytest.raises(PrimaryStateError):
            start(target, owner)
        assert files._connection is old and not guard.arms
        if fault == "transaction":
            files._db().rollback()


@pytest.mark.parametrize(
    ("pages", "journal", "required"),
    [
        (512, 0, 1),
        (512, 4103, 1),
        (512, 4104, 2),
        (512, 82592, 21),
        (4, 82592, 4),
        ((1 << 63) // 4096 - 1, (1 << 63) - 1, 2247410340364225),
    ],
)
def test_native_restoration_page_admission_covers_journal_records_and_original_ceiling(
    pages, journal, required
):
    from riverhog_client._prepared_primary_sqlite_operations import require_recovery_pages

    exact = replace(GRANTS, recovery=replace(BANK, database_pages=required))
    assert require_recovery_pages(exact, database_pages=pages, journal_bytes=journal) == required
    short = replace(exact, recovery=replace(exact.recovery, database_pages=required - 1))
    with pytest.raises(PrimaryStateCapacity, match="restoration page grant"):
        require_recovery_pages(short, database_pages=pages, journal_bytes=journal)


@pytest.mark.parametrize("entry", ["ready", "payload", "progress"])
def test_native_restoration_page_admission_refuses_before_mutation_and_increase_is_independent(
    tmp_path, monkeypatch, entry
):
    from riverhog_client import _prepared_primary_sqlite_ready as ready_owner

    # Bootstrap and explicit reserve preparation can defer under their own caps.
    # The retained R page allowance cannot cover the backed progress journal.
    small = replace(GRANTS, recovery=replace(BANK, database_pages=20))
    files, guard = managed(tmp_path / "state.sqlite3", operations=small)
    with files:
        original = files.put_bytes("original", (b"exact original",), byte_limit=14)
        dependencies(files)
        database, journal = files.database_reserve(), files.progress_journal()
        required = min(database.backed_pages, 1 + journal.backed_bytes // 4104)
        assert required > small.recovery.database_pages
        if entry == "payload":
            selected = ready_owner.SqlitePhysicalReadiness(
                "ready",
                0,
                database.backed_pages,
                database.free_pages,
                journal.backed_bytes,
                journal.minimum_bytes,
                journal.minimum_pages,
            )
            monkeypatch.setattr(ready_owner, "payload_profile", lambda _files: selected)
        arms, changes = len(guard.arms), files._db().total_changes
        consumed = []

        def source():
            consumed.append(True)
            yield b"refused"

        with pytest.raises(PrimaryStateCapacity, match="restoration page grant"):
            if entry == "ready":
                files.prepare_physical_ready()
            elif entry == "payload":
                files.put_bytes("refused", source(), byte_limit=7)
            else:
                files.sync()
        assert not consumed and len(guard.arms) == arms
        assert files._db().total_changes == changes and not files._poisoned
        assert files.operation_grants().grants == small
        monkeypatch.undo()
        # Explicit increases fund their own fixed update even when the old
        # profile cannot pass progress admission; no cleanup is required first.
        exact = replace(small, recovery=replace(small.recovery, database_pages=required))
        files.increase_resource_grants(
            database_bytes=LIMITS.database_bytes,
            journal_bytes=LIMITS.journal_bytes,
            operations=exact,
        )
        assert guard.arms[-1][0] == exact
        dependencies(files)
        assert files.prepare_physical_ready().phase == "ready"
        files.sync()
        with files.verified(original) as stream:
            assert stream.read() == b"exact original"


def test_native_existing_physical_ready_rechecks_restoration_before_reporting_ready(tmp_path):
    small = replace(GRANTS, recovery=replace(BANK, database_pages=20))
    files, guard = opened(tmp_path / "state.sqlite3", initialize=True, operations=small)
    with files:
        # Physical readiness alone is independent of native operation admission.
        dependencies(files)
        assert files.prepare_physical_ready().phase == "ready"
        start(files, guard)
        arms, changes = len(guard.arms), files._db().total_changes
        with pytest.raises(PrimaryStateCapacity, match="restoration page grant"):
            files.require_physical_ready()
        with pytest.raises(PrimaryStateCapacity, match="restoration page grant"):
            files.prepare_physical_ready()
        assert len(guard.arms) == arms and files._db().total_changes == changes
        assert not files._poisoned


def test_retained_ordering_replay_keeps_original_grants_without_arming_another_operation(tmp_path):
    files, guard = managed(tmp_path / "state.sqlite3")
    with files:
        dependencies(files)
        assert files.prepare_physical_ready().phase == "ready"
        rows = [(f"{number:064x}", number, "a" * 64) for number in range(17)]
        with files.ordered_rows(rows, byte_limit=128 * 1024) as selected:
            retained = selected.retain()
        offered = files.resource_grants(), files.operation_grants()
        arms = len(guard.arms)
        guard.events.clear()
        files._connection.set_trace_callback(guard.events.append)
        for _ in range(2):
            with retained.open() as selected:
                assert list(selected.rows()) == rows
        assert len(guard.arms) == arms
        assert (files.resource_grants(), files.operation_grants()) == offered
        assert not any(
            statement.startswith(("INSERT", "UPDATE", "DELETE", "BEGIN", "COMMIT"))
            for statement in guard.events
        )
