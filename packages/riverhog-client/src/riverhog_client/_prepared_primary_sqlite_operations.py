"""Original native operation and restoration grants retained with their allocation."""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import TYPE_CHECKING

from riverhog_client._prepared_primary_types import PrimaryStateCapacity, PrimaryStateError

if TYPE_CHECKING:
    from riverhog_client._prepared_primary_native import CallbackQuota, ReadQuota, WriteQuota
    from riverhog_client._prepared_primary_sqlite import (
        SqlitePrimaryLimits,
        TransactionalPrimaryFiles,
    )

_TAG = b"RHOPGR01"
_SIZE = 184
_MAXIMUM = (1 << 63) - 1


@dataclass(frozen=True, slots=True)
class SqliteOperationBank:
    database_pages: int
    write_bytes: int
    write_calls: int
    read_bytes: int
    read_calls: int
    callback_calls: int
    callback_sleep_microseconds: int
    filesystem_attempts: int
    internal_sleep_microseconds: int
    buffer_bytes: int

    def values(self) -> tuple[int, ...]:
        return tuple(getattr(self, field.name) for field in fields(self))

    def __post_init__(self) -> None:
        if any(type(value) is not int or not 0 <= value <= _MAXIMUM for value in self.values()):
            raise ValueError("native operation grants require nonnegative signed-64-bit integers")

    def preserves(self, original: SqliteOperationBank) -> bool:
        return all(
            offered >= retained
            for offered, retained in zip(self.values(), original.values(), strict=True)
        )


@dataclass(frozen=True, slots=True)
class SqliteOperationGrants:
    forward: SqliteOperationBank
    recovery: SqliteOperationBank

    def __post_init__(self) -> None:
        if (
            type(self.forward) is not SqliteOperationBank
            or type(self.recovery) is not SqliteOperationBank
        ):
            raise TypeError("native operation grants require exact forward and recovery banks")

    def preserves(self, original: SqliteOperationGrants) -> bool:
        return self.forward.preserves(original.forward) and self.recovery.preserves(
            original.recovery
        )


def require_recovery_pages(
    grants: SqliteOperationGrants, *, database_pages: int, journal_bytes: int
) -> int:
    """Reserve page tracking for every before-image allowed by a backed journal.

    Each complete saved page consumes at least 4096+8 journal bytes. Include
    page one and bound distinct original pages by the database ceiling. Padding
    and headers make this conservative; SQLite remains the journal interpreter.
    Independent byte/read/callback/file limits can still stop restoration, which
    must retain the original database and journal for a sufficient later grant.
    """
    if (
        type(grants) is not SqliteOperationGrants
        or type(database_pages) is not int
        or not 1 <= database_pages <= _MAXIMUM // 4096
        or type(journal_bytes) is not int
        or not 0 <= journal_bytes <= _MAXIMUM
    ):
        raise ValueError("native recovery page admission requires exact bounded extents")
    required = min(database_pages, 1 + journal_bytes // (4096 + 8))
    if grants.recovery.database_pages < required:
        raise PrimaryStateCapacity(
            "prepared-primary native restoration page grant cannot cover the journal envelope"
        )
    return required


@dataclass(frozen=True, slots=True)
class SqliteRetainedOperations:
    revision: int = 0
    grants: SqliteOperationGrants | None = None

    def encode(self) -> bytes:
        if type(self.revision) is not int or not 0 <= self.revision <= (1 << 64) - 1:
            raise PrimaryStateError("prepared-primary operation grant revision is invalid")
        if self.grants is not None and type(self.grants) is not SqliteOperationGrants:
            raise PrimaryStateError("prepared-primary operation grants are invalid")
        values = (
            (0,) * 20
            if self.grants is None
            else (*self.grants.forward.values(), *self.grants.recovery.values())
        )
        return _TAG + b"".join(
            value.to_bytes(8, "big")
            for value in (self.revision, int(self.grants is not None), *values)
        )


def read(files: TransactionalPrimaryFiles) -> SqliteRetainedOperations:
    with files._mutex:
        original = files.resource_grants()
        row = files._one("SELECT operation_grants FROM primary_format WHERE id=1")
        raw = None if row is None else row[0]
        if type(raw) is not bytes or len(raw) != _SIZE or raw[:8] != _TAG:
            raise PrimaryStateError("prepared-primary operation grant record is invalid")
        values = tuple(int.from_bytes(raw[i : i + 8], "big") for i in range(8, _SIZE, 8))
        revision, selected, *banks = values
        if revision != original.revision or selected not in (0, 1) or (not selected and any(banks)):
            raise PrimaryStateError("prepared-primary operation grant profile is invalid")
        try:
            grants = (
                None
                if not selected
                else SqliteOperationGrants(
                    SqliteOperationBank(*banks[:10]), SqliteOperationBank(*banks[10:])
                )
            )
        except (TypeError, ValueError) as exc:
            raise PrimaryStateError("prepared-primary operation grant values are invalid") from exc
        return SqliteRetainedOperations(revision, grants)


def save(
    files: TransactionalPrimaryFiles, old: SqliteRetainedOperations, new: SqliteRetainedOperations
) -> None:
    with files._db().blobopen("primary_format", "operation_grants", 1) as blob:
        if len(blob) != _SIZE or blob.read(_SIZE) != old.encode():
            raise PrimaryStateError("prepared-primary operation grants changed during execution")
        blob.seek(0)
        blob.write(new.encode())


def reconcile(files: TransactionalPrimaryFiles, offered: SqliteOperationGrants | None) -> None:
    retained = read(files)
    if retained.grants is not None and (offered is None or not offered.preserves(retained.grants)):
        raise PrimaryStateCapacity(
            "prepared-primary cold recovery requires original operation grants"
        )


@dataclass(frozen=True, slots=True)
class NativeOperationBank:
    writes: WriteQuota
    reads: ReadQuota
    callbacks: CallbackQuota


def native_bank(
    grants: SqliteOperationGrants,
    limits: SqlitePrimaryLimits,
    *,
    database_pages: int | None = None,
    journal_bytes: int | None = None,
    recovering: bool = False,
) -> NativeOperationBank:
    """Construct fresh C counters; physical caps narrow F while R retains original ceilings.

    The owner must select this bank before the operation, preserve it through
    uncertain outcomes and reconcile all application consumers before activation.
    This constructor neither opens a container nor arms or authorizes an operation.
    """
    if type(grants) is not SqliteOperationGrants or type(recovering) is not bool:
        raise TypeError("native operation bank requires exact grants and a recovery selection")
    pages = limits.database_bytes // 4096 if database_pages is None else database_pages
    journal = limits.journal_bytes if journal_bytes is None else journal_bytes
    if (
        type(pages) is not int
        or not 1 <= pages <= limits.database_bytes // 4096
        or type(journal) is not int
        or not 0 < journal <= limits.journal_bytes
    ):
        raise ValueError("native forward physical caps exceed original resource grants")
    from riverhog_client import _prepared_primary_native as native

    forward, recovery = grants.forward, grants.recovery
    writes = native.WriteQuota(
        (
            4096,
            forward.database_pages,
            pages * 4096,
            journal,
            forward.write_bytes,
            forward.write_calls,
        ),
        (
            4096,
            recovery.database_pages,
            limits.database_bytes,
            limits.journal_bytes,
            recovery.write_bytes,
            recovery.write_calls,
        ),
    )
    reads = native.ReadQuota(
        writes, (forward.read_bytes, forward.read_calls), (recovery.read_bytes, recovery.read_calls)
    )
    work = native.NativeWorkQuota(
        writes,
        (forward.filesystem_attempts, forward.internal_sleep_microseconds),
        (recovery.filesystem_attempts, recovery.internal_sleep_microseconds),
    )
    callbacks = native.CallbackQuota(
        writes,
        (forward.callback_calls, forward.callback_sleep_microseconds),
        (recovery.callback_calls, recovery.callback_sleep_microseconds),
        work=work,
        buffers=(forward.buffer_bytes, recovery.buffer_bytes),
    )
    if recovering:
        writes.begin_recovery()
    return NativeOperationBank(writes, reads, callbacks)
