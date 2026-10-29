"""Bounded SQL allocation and release of owned reusable database headroom."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Literal

from riverhog_client._prepared_primary_types import (
    PrimaryStateBusy,
    PrimaryStateCapacity,
    PrimaryStateError,
)

if TYPE_CHECKING:
    import sqlite3

    from riverhog_client._prepared_primary_sqlite import TransactionalPrimaryFiles
    from riverhog_client._prepared_primary_sqlite_space import SqlitePrimarySpace

Phase = Literal["unprepared", "building", "releasing", "prepared", "depleted"]
_PHASES: tuple[Phase, ...] = ("unprepared", "building", "releasing", "prepared", "depleted")
_TAG = b"RHDBRES1"
_SIZE = 72
_BLOCK = 4096
_MAXIMUM = (1 << 63) - 1


@dataclass(frozen=True, slots=True)
class SqliteDatabaseReserve:
    phase: Phase = "unprepared"
    grant_revision: int = 0
    free_pages: int = 0
    target_pages: int = 0
    workspace_bytes: int = 0
    inserted: int = 0
    released: int = 0
    backed_pages: int = 0

    def encode(self) -> bytes:
        values = (
            self.free_pages,
            self.target_pages,
            self.workspace_bytes,
            self.inserted,
            self.released,
            self.backed_pages,
        )
        if (
            self.phase not in _PHASES
            or type(self.grant_revision) is not int
            or not 0 <= self.grant_revision <= (1 << 64) - 1
            or any(type(v) is not int or not 0 <= v <= _MAXIMUM for v in values)
            or self.released > self.inserted
        ):
            raise PrimaryStateError("prepared-primary database reserve record is invalid")
        if self.phase == "unprepared":
            if any(values):
                raise PrimaryStateError("prepared-primary unprepared reserve has live obligations")
        elif (
            self.target_pages == 0
            or self.workspace_bytes < _BLOCK
            or self.workspace_bytes % _BLOCK
            or self.inserted - self.released > self.workspace_bytes // _BLOCK
            or (
                self.phase == "prepared"
                and (self.inserted != self.released or self.backed_pages < self.target_pages)
            )
        ):
            raise PrimaryStateError("prepared-primary database reserve profile is invalid")
        return _TAG + b"".join(
            v.to_bytes(8, "big") for v in (_PHASES.index(self.phase), self.grant_revision, *values)
        )


@dataclass(frozen=True, slots=True)
class SqliteDatabaseReserveStep:
    reserve: SqliteDatabaseReserve
    space: SqlitePrimarySpace
    done: bool


def read(files: TransactionalPrimaryFiles) -> SqliteDatabaseReserve:
    with files._mutex:
        grants = files.resource_grants()
        row = files._one("SELECT database_reserve FROM primary_format WHERE id=1")
        raw = None if row is None else row[0]
        if type(raw) is not bytes or len(raw) != _SIZE or raw[:8] != _TAG:
            raise PrimaryStateError("prepared-primary database reserve record is invalid")
        values = tuple(int.from_bytes(raw[i : i + 8], "big") for i in range(8, _SIZE, 8))
        if values[0] >= len(_PHASES):
            raise PrimaryStateError("prepared-primary database reserve phase is invalid")
        result = SqliteDatabaseReserve(_PHASES[values[0]], *values[1:])
        result.encode()
        if (
            result.grant_revision != grants.revision
            or result.target_pages > grants.database_bytes // _BLOCK
            or result.free_pages > grants.database_bytes // _BLOCK
            or result.backed_pages > grants.database_bytes // _BLOCK
        ):
            raise PrimaryStateError("prepared-primary database reserve grant changed")
        return result


def save(
    db: sqlite3.Connection, previous: SqliteDatabaseReserve, selected: SqliteDatabaseReserve
) -> None:
    with db.blobopen("primary_format", "database_reserve", 1) as blob:
        if len(blob) != _SIZE or blob.read(_SIZE) != previous.encode():
            raise PrimaryStateError("prepared-primary database reserve changed during execution")
        blob.seek(0)
        blob.write(selected.encode())


def revised(current: SqliteDatabaseReserve, revision: int) -> SqliteDatabaseReserve:
    return replace(
        current,
        grant_revision=revision,
        phase="depleted" if current.phase == "prepared" else current.phase,
    )


def _usable(space: SqlitePrimarySpace, reserve: SqliteDatabaseReserve) -> bool:
    return (
        space.page_bytes == _BLOCK
        and space.database_pages == reserve.backed_pages
        and space.reusable_pages >= reserve.free_pages
        and space.database.allocated_bytes >= reserve.backed_pages * _BLOCK
    )


def require(files: TransactionalPrimaryFiles) -> SqliteDatabaseReserve:
    from riverhog_client._prepared_primary_sqlite_space import space_usage

    with files._mutex:
        reserve = read(files)
        if reserve.phase != "prepared" or not _usable(space_usage(files), reserve):
            raise PrimaryStateCapacity("prepared-primary database reserve is unavailable")
        return reserve


def step(
    files: TransactionalPrimaryFiles,
    *,
    free_pages: int,
    target_pages: int,
    workspace_bytes: int,
) -> SqliteDatabaseReserveStep:
    """Insert or delete at most one indexed 4096-byte reserve row per call.

    The configured workspace bounds live reserve BLOB bytes. Native transaction
    grants independently admit structural work and database/journal allocation.
    Prepared database headroom is one dependency of the complete resource barrier.
    """
    from riverhog_client._prepared_primary_sqlite_ready import deplete
    from riverhog_client._prepared_primary_sqlite_space import space_usage

    if (
        type(free_pages) is not int
        or not 0 <= free_pages <= _MAXIMUM
        or type(target_pages) is not int
        or not 1 <= target_pages <= _MAXIMUM
        or type(workspace_bytes) is not int
        or not _BLOCK <= workspace_bytes <= _MAXIMUM
        or workspace_bytes % _BLOCK
    ):
        raise ValueError("prepared-primary database reserve grants are invalid")
    with files._mutex:
        if files is not files._root:
            raise PrimaryStateError("prepared-primary database reserve requires the container root")
        if files._db().in_transaction:
            raise PrimaryStateBusy(
                "prepared-primary database reserve requires a settled transaction"
            )
        current = read(files)
        grants = files.resource_grants()
        if (
            target_pages > grants.database_bytes // _BLOCK
            or free_pages > grants.database_bytes // _BLOCK
        ):
            raise PrimaryStateCapacity(
                "prepared-primary database reserve exceeds its database grant"
            )
        if (
            free_pages < current.free_pages
            or target_pages < current.target_pages
            or workspace_bytes < current.workspace_bytes
        ):
            raise ValueError("prepared-primary database reserve cannot lower retained grants")
        selected = replace(
            current,
            free_pages=free_pages,
            target_pages=target_pages,
            workspace_bytes=workspace_bytes,
        )
        before = space_usage(files)
        if current.phase == "prepared" and selected == current and _usable(before, current):
            return SqliteDatabaseReserveStep(current, before, True)
        if selected.phase in ("unprepared", "prepared", "depleted"):
            selected = replace(selected, phase="building")
        live = selected.inserted - selected.released
        if (
            selected.phase == "building"
            and live
            and before.database_pages >= target_pages
            and live >= free_pages
        ):
            selected = replace(selected, phase="releasing")
        if not live and (
            before.database_pages >= target_pages
            and before.reusable_pages >= free_pages
            and before.database.allocated_bytes >= before.database.end_bytes
        ):
            prepared = replace(selected, phase="prepared", backed_pages=before.database_pages)
            with files._transaction() as db:
                deplete(files)
                save(db, current, prepared)
            after = space_usage(files)
            if not _usable(after, prepared):
                with files._transaction() as db:
                    deplete(files)
                    save(db, prepared, replace(prepared, phase="depleted"))
                raise PrimaryStateCapacity("prepared-primary database reserve lost backing")
            return SqliteDatabaseReserveStep(prepared, after, True)
        if selected.phase == "releasing" and not live:
            selected = replace(selected, phase="building")
        if selected.phase == "building":
            if not live:
                # Every known row has a confirmed deletion. Reuse its ID range
                # instead of imposing a cumulative lifetime preparation limit.
                selected = replace(selected, inserted=0, released=0)
            if live >= workspace_bytes // _BLOCK:
                raise PrimaryStateCapacity(
                    "prepared-primary database reserve needs larger workspace"
                )
            if selected.inserted == _MAXIMUM:
                raise PrimaryStateCapacity("prepared-primary database reserve cursor is exhausted")
            selected = replace(selected, inserted=selected.inserted + 1)
            with files._transaction() as db:
                deplete(files)
                db.execute(
                    "INSERT INTO primary_reserve_rows(id,body) VALUES (?,zeroblob(4096))",
                    (selected.inserted,),
                ).close()
                save(db, current, selected)
        else:
            identifier = selected.released + 1
            selected = replace(selected, released=identifier)
            with files._transaction() as db:
                deplete(files)
                cursor = db.execute("DELETE FROM primary_reserve_rows WHERE id=?", (identifier,))
                removed = cursor.rowcount
                cursor.close()
                if removed != 1:
                    raise PrimaryStateError("prepared-primary database reserve cursor lost its row")
                save(db, current, selected)
        after = space_usage(files)
        return SqliteDatabaseReserveStep(selected, after, False)
