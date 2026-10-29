"""Native factory and per-transaction admission under one retained custodian."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from riverhog_client._prepared_primary_sqlite import ContainerGuard, SqlitePrimaryLimits
from riverhog_client._prepared_primary_sqlite_operations import (
    SqliteOperationGrants,
    require_recovery_pages,
)
from riverhog_client._prepared_primary_types import PrimaryStateCapacity, PrimaryStateError

if TYPE_CHECKING:
    from pathlib import Path

    from riverhog_client._prepared_primary_native import SqliteProcessBudget
    from riverhog_client._prepared_primary_sqlite import TransactionalPrimaryFiles
    from riverhog_client._prepared_primary_sqlite_operations import NativeOperationBank
    from riverhog_client._prepared_primary_sqlite_ready import SqlitePhysicalReadiness


class NativeOperationGuard(ContainerGuard, Protocol):
    def arm_operation(
        self,
        grants: SqliteOperationGrants,
        limits: SqlitePrimaryLimits,
        *,
        database_pages: int | None = None,
        journal_bytes: int | None = None,
    ) -> NativeOperationBank: ...


@dataclass(frozen=True, slots=True)
class NativeSqliteIncrease:
    limits: SqlitePrimaryLimits
    operations: SqliteOperationGrants


@dataclass(frozen=True, slots=True)
class NativeSqliteTransactions:
    guard: NativeOperationGuard

    def arm(
        self,
        files: TransactionalPrimaryFiles,
        *,
        payload: bool,
        preparing: bool,
        increase: NativeSqliteIncrease | None,
    ) -> SqlitePhysicalReadiness | None:
        """Paid fixed-profile selection precedes a fresh mutation bank and BEGIN.

        Selection reads still spend the preceding bank. A failed or exhausted
        selection must close/reopen; it cannot silently renew its own authority.
        """
        retained = files.operation_grants()
        grants = retained.grants
        if grants is None:
            raise PrimaryStateError("prepared-primary native operation profile is missing")
        limits = files._limits
        if increase is not None:
            if (
                type(increase) is not NativeSqliteIncrease
                or not increase.operations.preserves(grants)
                or increase.limits.database_bytes < limits.database_bytes
                or increase.limits.journal_bytes < limits.journal_bytes
            ):
                raise PrimaryStateError("prepared-primary native increase revoked original grants")
            grants, limits = increase.operations, increase.limits
        ready = None
        pages: int | None = None
        journal: int | None = None
        if payload:
            from riverhog_client._prepared_primary_sqlite_ready import payload_profile

            ready = payload_profile(files)
            if ready is not None:
                if grants.forward.database_pages < ready.minimum_pages:
                    raise PrimaryStateCapacity(
                        "prepared-primary native page grant cannot preserve progress"
                    )
                pages, journal = ready.backed_pages, ready.journal_bytes
        elif not preparing:
            floor = files.journal_reserved_bytes()
            if floor:
                database, backing = self.guard.file_space()
                if (
                    database.end_bytes <= 0
                    or database.end_bytes % 4096
                    or database.allocated_bytes < database.end_bytes
                    or backing.end_bytes < floor
                    or backing.allocated_bytes < floor
                ):
                    raise PrimaryStateCapacity(
                        "prepared-primary native progress backing is unavailable"
                    )
                pages, journal = database.end_bytes // 4096, floor
        if pages is not None and journal is not None and increase is None:
            require_recovery_pages(grants, database_pages=pages, journal_bytes=journal)
        # Preparation is explicitly structural and may defer under its original
        # banks. An owner increase remains independent of that deferred work.
        self.guard.arm_operation(grants, limits, database_pages=pages, journal_bytes=journal)
        return ready


def start(
    files: TransactionalPrimaryFiles, guard: NativeOperationGuard
) -> TransactionalPrimaryFiles:
    """Close the reconciled pager before fresh F, retaining native metadata custody."""
    from riverhog_client._prepared_primary_sqlite import _connect

    with files._mutex:
        if files is not files._root or files._guard is not guard:
            raise PrimaryStateError("prepared-primary native startup requires its original root")
        retained = files.operation_grants()
        if retained.grants is None:
            raise PrimaryStateError(
                "prepared-primary native startup needs original operation grants"
            )
        db = files._db()
        if db.in_transaction or files._readers:
            raise PrimaryStateError("prepared-primary native startup has live execution")
        db.close()
        files._connection = None
        guard.arm_operation(retained.grants, files._limits)
        files._connection = _connect(guard, files._limits)
        files._native_transactions = NativeSqliteTransactions(guard)
        return files


def open_native(
    path: Path,
    *,
    allocation_descriptor: bytes,
    limits: SqlitePrimaryLimits,
    operations: SqliteOperationGrants,
    process: SqliteProcessBudget,
    retry_profile: tuple[int, int],
    initialize: bool,
) -> TransactionalPrimaryFiles:
    """Compose a finite native owner under an already admitted application process.

    This does not configure application consumers, choose priorities or activate a
    producer. The application must reconcile those before exposing execution.
    """
    from riverhog_client import _prepared_primary_native as native
    from riverhog_client._prepared_primary_runtime import (
        _QUALIFIED_INITIAL_RETRIES,
        _qualified_runtime,
    )
    from riverhog_client._prepared_primary_sqlite import (
        TransactionalPrimaryFiles,
        _admit_cold_extents,
    )
    from riverhog_client._prepared_primary_sqlite_operations import native_bank
    from riverhog_client._prepared_primary_windows_native import (
        NativeWindowsContainer,
        windows_container_path,
    )

    if (
        type(initialize) is not bool
        or type(limits) is not SqlitePrimaryLimits
        or type(operations) is not SqliteOperationGrants
        or not allocation_descriptor
        or path != windows_container_path(path.parent, allocation_descriptor)
    ):
        raise ValueError("prepared-primary native factory requires an exact allocation and grants")
    identity = _qualified_runtime()
    if (
        type(process) is not native.SqliteProcessBudget
        or native.current_process_budget() is not process
    ):
        raise PrimaryStateError(
            "prepared-primary native factory needs the current application budget"
        )
    process.report()
    # Validate native signed retry arithmetic before metadata or pager admission.
    native.retry_bootstrap_cost(retry_profile)
    counters = native_bank(operations, limits, recovering=not initialize)
    gate = native.WriteVfs(
        identity["sqlite_source_id"], counters.reads, counters.callbacks, process
    )
    # The constructor owns failed/uncertain metadata custody and its teardown.
    guard = NativeWindowsContainer(path, initialize=initialize, gate=gate)
    try:
        if not initialize:
            _admit_cold_extents(guard, limits)
        retries = process.retry_report()
        if retries["state"] == "unconfigured":
            gate.admit_retries(
                _QUALIFIED_INITIAL_RETRIES,
                retry_profile,
                native.retry_bootstrap_cost(_QUALIFIED_INITIAL_RETRIES),
            )
        elif (
            retries["state"] != "admitted"
            or (retries["selected_retry_count"], retries["selected_delay_milliseconds"])
            != retry_profile
            or (
                retries["qualified_initial_retry_count"],
                retries["qualified_initial_delay_milliseconds"],
            )
            != _QUALIFIED_INITIAL_RETRIES
        ):
            raise PrimaryStateError("prepared-primary application retry admission differs")
        files = TransactionalPrimaryFiles(
            guard,
            allocation_descriptor=allocation_descriptor,
            limits=limits,
            initialize=initialize,
            operations=operations,
        )
        try:
            return start(files, guard)
        except BaseException:
            try:
                files.close()
            except BaseException:
                pass
            raise
    except BaseException:
        # Native custody retains unknown cleanup. Preserve the operation's cause.
        try:
            guard.close()
        except BaseException:
            pass
        raise
