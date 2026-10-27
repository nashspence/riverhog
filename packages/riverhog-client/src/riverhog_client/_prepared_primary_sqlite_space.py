"""Point physical-space observations and explicitly scheduled incremental maintenance."""

from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from typing import TYPE_CHECKING

from riverhog_client._prepared_primary_types import (
    PrimaryFileSpace,
    PrimaryStateBusy,
    PrimaryStateError,
)

if TYPE_CHECKING:
    from riverhog_client._prepared_primary_sqlite import TransactionalPrimaryFiles


@dataclass(frozen=True, slots=True)
class SqlitePrimarySpace:
    page_bytes: int
    database_pages: int
    reusable_pages: int
    database: PrimaryFileSpace
    journal: PrimaryFileSpace
    ordering_grant_bytes: int
    journal_reserved_bytes: int


@dataclass(frozen=True, slots=True)
class SqliteVacuumStep:
    before: SqlitePrimarySpace
    after: SqlitePrimarySpace
    done: bool


def _header_value(files: TransactionalPrimaryFiles, name: str) -> int:
    row = files._one("PRAGMA " + name)
    if row is None or len(row) != 1 or type(row[0]) is not int or row[0] < 0:
        raise PrimaryStateError("prepared-primary space header is unavailable")
    return row[0]


def space_usage(files: TransactionalPrimaryFiles) -> SqlitePrimarySpace:
    """Read headers and owned file metadata; no content scan or physical-release inference."""
    with files._mutex:
        if files._db().in_transaction:
            raise PrimaryStateBusy(
                "prepared-primary space observation requires a settled transaction"
            )
        pages = _header_value(files, "page_count")
        free = _header_value(files, "freelist_count")
        page_bytes = _header_value(files, "page_size")
        database, journal = files._guard.file_space()
        if not 0 <= free <= pages or database.end_bytes != pages * page_bytes:
            raise PrimaryStateError("prepared-primary physical extent disagrees with SQLite")
        return SqlitePrimarySpace(
            page_bytes,
            pages,
            free,
            database,
            journal,
            files.ordering_reserved_bytes(),
            files.journal_reserved_bytes(),
        )


def vacuum_step(files: TransactionalPrimaryFiles, *, keep_free_pages: int) -> SqliteVacuumStep:
    """Remove at most one freelist page, preserving the caller's explicit free-page floor.

    SQLite can additionally release administrative pages. File allocation changes
    are reported independently from page counts; neither reports device-level I/O.
    This execution operation never runs in status, receipt or startup paths.
    """
    if type(keep_free_pages) is not int or keep_free_pages < 0:
        raise ValueError("prepared-primary maintenance requires a nonnegative free-page floor")
    with files._mutex:
        files.check_current()
        if files is not files._root:
            raise PrimaryStateError("prepared-primary maintenance requires the container root")
        files._require_idle()
        if files._root._readers:
            raise PrimaryStateBusy("prepared-primary maintenance has a live replay reader")
        guard = files._root._ordering_guard
        if not guard.acquire(blocking=False):
            raise PrimaryStateBusy("prepared-primary maintenance has a live ordering reader")
        try:
            before = space_usage(files)
            if before.reusable_pages <= keep_free_pages:
                return SqliteVacuumStep(before, before, True)
            with files._transaction() as db:
                # N=1 bounds both the freelist work and the result rows drained here.
                with closing(db.execute("PRAGMA incremental_vacuum(1)")) as cursor:
                    cursor.fetchall()
            after = space_usage(files)
            if (
                after.reusable_pages != before.reusable_pages - 1
                or after.database_pages >= before.database_pages
            ):
                files._root._poisoned = True
                raise PrimaryStateError("prepared-primary incremental maintenance did not advance")
            return SqliteVacuumStep(before, after, after.reusable_pages <= keep_free_pages)
        finally:
            guard.release()
