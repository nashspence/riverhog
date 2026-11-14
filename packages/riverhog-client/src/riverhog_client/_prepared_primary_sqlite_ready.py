"""Durable physical dependency barrier and transactional payload headroom checks."""

from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Literal

from riverhog_client._prepared_primary_types import (
    PrimaryStateBusy,
    PrimaryStateCapacity,
    PrimaryStateError,
)

if TYPE_CHECKING:
    from riverhog_client._prepared_primary_sqlite import TransactionalPrimaryFiles

Phase = Literal["unprepared", "preparing", "ready", "depleted"]
_PHASES: tuple[Phase, ...] = ("unprepared", "preparing", "ready", "depleted")
_TAG = b"RHREADY1"
_SIZE = 64
_MAXIMUM = (1 << 63) - 1


@dataclass(frozen=True, slots=True)
class SqlitePhysicalReadiness:
    phase: Phase = "unprepared"
    grant_revision: int = 0
    backed_pages: int = 0
    free_pages: int = 0
    journal_bytes: int = 0
    minimum_journal_bytes: int = 0
    minimum_pages: int = 0

    def encode(self) -> bytes:
        values = (
            self.backed_pages,
            self.free_pages,
            self.journal_bytes,
            self.minimum_journal_bytes,
            self.minimum_pages,
        )
        if (
            self.phase not in _PHASES
            or type(self.grant_revision) is not int
            or not 0 <= self.grant_revision <= (1 << 64) - 1
            or any(type(v) is not int or not 0 <= v <= _MAXIMUM for v in values)
            or self.free_pages > self.backed_pages
        ):
            raise PrimaryStateError("prepared-primary physical readiness record is invalid")
        if self.phase == "unprepared" and any(values):
            raise PrimaryStateError("prepared-primary bootstrap readiness has live obligations")
        if self.phase == "ready" and (
            self.backed_pages == 0
            or self.minimum_journal_bytes == 0
            or self.minimum_pages == 0
            or self.journal_bytes < self.minimum_journal_bytes
        ):
            raise PrimaryStateError("prepared-primary physical readiness profile is invalid")
        return _TAG + b"".join(
            v.to_bytes(8, "big") for v in (_PHASES.index(self.phase), self.grant_revision, *values)
        )


def read(files: TransactionalPrimaryFiles) -> SqlitePhysicalReadiness:
    with files._mutex:
        grants = files.resource_grants()
        row = files._one("SELECT physical_readiness FROM primary_format WHERE id=1")
        raw = None if row is None else row[0]
        if type(raw) is not bytes or len(raw) != _SIZE or raw[:8] != _TAG:
            raise PrimaryStateError("prepared-primary physical readiness record is invalid")
        values = tuple(int.from_bytes(raw[i : i + 8], "big") for i in range(8, _SIZE, 8))
        if values[0] >= len(_PHASES):
            raise PrimaryStateError("prepared-primary physical readiness phase is invalid")
        result = SqlitePhysicalReadiness(_PHASES[values[0]], *values[1:])
        result.encode()
        if (
            result.grant_revision != grants.revision
            or result.backed_pages > grants.database_bytes // 4096
            or result.journal_bytes > grants.journal_bytes
        ):
            raise PrimaryStateError("prepared-primary physical readiness grant changed")
        return result


def _save(
    files: TransactionalPrimaryFiles, old: SqlitePhysicalReadiness, new: SqlitePhysicalReadiness
) -> None:
    with files._db().blobopen("primary_format", "physical_readiness", 1) as blob:
        if len(blob) != _SIZE or blob.read(_SIZE) != old.encode():
            raise PrimaryStateError("prepared-primary physical readiness changed during execution")
        blob.seek(0)
        blob.write(new.encode())


def revised(
    files: TransactionalPrimaryFiles, current: SqlitePhysicalReadiness, revision: int
) -> None:
    _save(
        files,
        current,
        replace(
            current,
            grant_revision=revision,
            phase="depleted" if current.phase == "ready" else current.phase,
        ),
    )


def deplete(files: TransactionalPrimaryFiles) -> None:
    """Dependency preparation and vacuum invalidate Ready in their own transaction."""
    current = read(files)
    if current.phase == "ready":
        _save(files, current, replace(current, phase="depleted"))


def _pages(files: TransactionalPrimaryFiles) -> tuple[int, int]:
    values = []
    for name in ("page_count", "freelist_count"):
        row = files._one("PRAGMA " + name)
        if row is None or len(row) != 1 or type(row[0]) is not int or row[0] < 0:
            raise PrimaryStateError("prepared-primary readiness page accounting is invalid")
        values.append(row[0])
    pages, free = values
    if free > pages:
        raise PrimaryStateError("prepared-primary readiness page accounting is invalid")
    return pages, free


def _dependencies(files: TransactionalPrimaryFiles, current: SqlitePhysicalReadiness) -> None:
    database = files.database_reserve()
    journal = files.progress_journal()
    if (
        current.phase != "ready"
        or database.phase != "prepared"
        or journal.phase != "prepared"
        or (current.backed_pages, current.free_pages)
        != (database.backed_pages, database.free_pages)
        or current.journal_bytes != journal.backed_bytes
        or current.minimum_journal_bytes < journal.minimum_bytes
        or current.minimum_pages < journal.minimum_pages
        or current.journal_bytes < files.journal_reserved_bytes()
    ):
        raise PrimaryStateCapacity("prepared-primary physical readiness needs preparation")


def require(files: TransactionalPrimaryFiles) -> SqlitePhysicalReadiness:
    """Observe the complete physical dependencies without repairing or warming them."""
    with files._mutex:
        current = read(files)
        _dependencies(files, current)
        pages, free = _pages(files)
        database, journal = files._guard.file_space()
        if (
            pages != current.backed_pages
            or free < current.free_pages
            or database.end_bytes != pages * 4096
            or database.allocated_bytes < pages * 4096
            or journal.end_bytes < current.journal_bytes
            or journal.allocated_bytes < current.journal_bytes
        ):
            raise PrimaryStateCapacity("prepared-primary physical readiness backing is unavailable")
        _native_progress_grants(files, current)
        return current


def _native_progress_grants(
    files: TransactionalPrimaryFiles, current: SqlitePhysicalReadiness
) -> None:
    if files._root._native_transactions is None:
        return
    grants = files.operation_grants().grants
    if grants is None or grants.forward.database_pages < current.minimum_pages:
        raise PrimaryStateCapacity("prepared-primary native page grant cannot preserve progress")
    from riverhog_client._prepared_primary_sqlite_operations import require_recovery_pages

    require_recovery_pages(
        grants, database_pages=current.backed_pages, journal_bytes=current.journal_bytes
    )


def prepare(files: TransactionalPrimaryFiles) -> SqlitePhysicalReadiness:
    """Commit Ready only after its fixed write's journal need is already backed.

    Bootstrap may create the container and fixed extents. Selecting either
    reserve or this barrier ends bootstrap payload admission. Production owners
    must prepare this barrier before exposing a construction to payload work.
    Native operation/recovery grants and process admission remain independent.
    """
    with files._mutex:
        if files is not files._root:
            raise PrimaryStateError(
                "prepared-primary physical readiness requires the container root"
            )
        if files._db().in_transaction:
            raise PrimaryStateBusy(
                "prepared-primary physical readiness requires a settled transaction"
            )
        current = read(files)
        if current.phase == "ready":
            try:
                return require(files)
            except PrimaryStateCapacity:
                pass
        pending = replace(current, phase="preparing")
        if current != pending:
            with files._transaction():
                _save(files, current, pending)
        database = files.require_database_reserve()
        journal = files.require_progress_journal()
        from riverhog_client._prepared_primary_sqlite_database_reserve import _SIZE as database_size
        from riverhog_client._prepared_primary_sqlite_journal import _reservation
        from riverhog_client._prepared_primary_sqlite_operations import _SIZE as operations_size
        from riverhog_client._prepared_primary_sqlite_progress_journal import _SIZE as journal_size
        from riverhog_client._prepared_primary_sqlite_resources import _SIZE as grants_size

        # Grant increase updates all five existing fixed records atomically.
        # Control warming updates one existing maximum extent and its 16-byte record.
        profiles: list[tuple[int, ...]] = [
            (grants_size, database_size, journal_size, _SIZE, operations_size)
        ]
        _floor, representative = _reservation(files)
        if representative:
            row = files._one(
                "SELECT length(body),typeof(body) FROM primary_controls WHERE rowid=?",
                (representative,),
            )
            if row is None or row[1] != "blob" or type(row[0]) is not int or row[0] < 9:
                raise PrimaryStateError("prepared-primary progress control extent is unavailable")
            profiles.append((row[0], 16))
        envelopes = [files._guard.control_envelope(lengths) for lengths in profiles]
        minimum = max(journal.minimum_bytes, *(value.journal_bytes for value in envelopes))
        pages = max(journal.minimum_pages, *(value.database_pages for value in envelopes))
        if minimum > journal.backed_bytes or files.journal_reserved_bytes() > journal.backed_bytes:
            raise PrimaryStateCapacity("prepared-primary readiness update exceeds journal backing")
        selected = SqlitePhysicalReadiness(
            "ready",
            pending.grant_revision,
            database.backed_pages,
            database.free_pages,
            journal.backed_bytes,
            minimum,
            pages,
        )
        _native_progress_grants(files, selected)
        with files._transaction():
            _save(files, pending, selected)
        try:
            return require(files)
        except PrimaryStateCapacity:
            with files._transaction():
                _save(files, selected, replace(selected, phase="depleted"))
            raise


def payload_profile(files: TransactionalPrimaryFiles) -> SqlitePhysicalReadiness | None:
    """Observe bootstrap or complete backing without selecting a new bank or mutating SQL."""
    current = read(files)
    if (
        current.phase == "unprepared"
        and files.database_reserve().phase == "unprepared"
        and files.progress_journal().phase == "unprepared"
    ):
        return None
    return require(files)


def begin_payload(files: TransactionalPrimaryFiles) -> SqlitePhysicalReadiness | None:
    """Revalidate under BEGIN IMMEDIATE before the first payload mutation or input read."""
    current = payload_profile(files)
    if current is None:
        return None
    with closing(files._db().execute(f"PRAGMA max_page_count={current.backed_pages}")) as cursor:
        observed = cursor.fetchone()
    files._guard.check_operation()
    if observed != (current.backed_pages,):
        raise PrimaryStateCapacity("prepared-primary payload ceiling exceeds backed pages")
    return current


def precommit(files: TransactionalPrimaryFiles, current: SqlitePhysicalReadiness) -> None:
    """Dirty cached pages may not consume the progress floor or extend past backing."""
    pages, free = _pages(files)
    if pages > current.backed_pages or free < current.free_pages:
        raise PrimaryStateCapacity("prepared-primary payload would consume progress headroom")


def restore_ceiling(files: TransactionalPrimaryFiles) -> None:
    with closing(
        files._db().execute(f"PRAGMA max_page_count={files._limits.database_bytes // 4096}")
    ) as cursor:
        observed = cursor.fetchone()
    files._guard.check_operation()
    if observed != (files._limits.database_bytes // 4096,):
        raise PrimaryStateError("prepared-primary retained database ceiling was rejected")
