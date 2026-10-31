"""Allocated rollback-journal headroom prepared through bounded SQLite writes."""

from __future__ import annotations

import sqlite3
from contextlib import closing, contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

from riverhog_client._prepared_primary_types import PrimaryStateCapacity, PrimaryStateError

if TYPE_CHECKING:
    from collections.abc import Iterator

    from riverhog_client._prepared_primary_sqlite import TransactionalPrimaryFiles


@dataclass(frozen=True, slots=True)
class FixedControlEnvelope:
    database_pages: int
    journal_bytes: int


def _validate_control_prefixes(prefix_lengths: tuple[int, ...]) -> None:
    if type(prefix_lengths) is not tuple:
        raise TypeError("fixed control prefixes require an exact tuple")
    for length in prefix_lengths:
        if type(length) is not int or not 0 < length <= (1 << 63) - 1:
            raise ValueError("fixed control prefixes require positive signed-64-bit lengths")


def control_envelope(
    prefix_lengths: tuple[int, ...], *, page_bytes: int, sector_bytes: int, reserved_bytes: int
) -> FixedControlEnvelope:
    """Bound one fixed-extent, no-topology-change transaction, not its allocation."""
    maximum = (1 << 63) - 1
    _validate_control_prefixes(prefix_lengths)
    if (
        type(page_bytes) is not int
        or not 512 <= page_bytes <= 65536
        or page_bytes & (page_bytes - 1)
        or type(sector_bytes) is not int
        or not 0 < sector_bytes <= maximum
        or sector_bytes & (sector_bytes - 1)
        or type(reserved_bytes) is not int
        or not 0 <= reserved_bytes <= 255
    ):
        raise ValueError("fixed control geometry is not qualified")
    usable = page_bytes - reserved_bytes - 4
    frames = 1
    for length in prefix_lengths:
        # Quotient/remainder avoids an overflowing rounded-up numerator.
        added = 2 + length // usable + int(length % usable != 0)
        if added > maximum - frames:
            raise OverflowError("fixed control page bound exceeds signed-64-bit admission")
        frames += added
    peers = max(1, sector_bytes // page_bytes + int(sector_bytes % page_bytes != 0))
    if frames > maximum // peers:
        raise OverflowError("fixed control page bound exceeds signed-64-bit admission")
    pages = frames * peers
    if sector_bytes > (maximum - 16) // 2:
        raise OverflowError("fixed control journal bound exceeds signed-64-bit admission")
    headers = 2 * sector_bytes + 16
    if pages > (maximum - headers) // (page_bytes + 8):
        raise OverflowError("fixed control journal bound exceeds signed-64-bit admission")
    return FixedControlEnvelope(pages, headers + pages * (page_bytes + 8))


_RESERVE_BYTES = 16
_QUANTUM = 4096


def _reservation(owner: TransactionalPrimaryFiles) -> tuple[int, int]:
    row = owner._one("SELECT journal_reserve FROM primary_format WHERE id=1")
    if row is None or not isinstance(row[0], bytes) or len(row[0]) != _RESERVE_BYTES:
        raise PrimaryStateError("prepared-primary journal reservation is invalid")
    floor, representative = (
        int.from_bytes(row[0][:8], "big"),
        int.from_bytes(row[0][8:], "big"),
    )
    if (floor == 0) != (representative == 0) or representative > 9223372036854775807:
        raise PrimaryStateError("prepared-primary journal reservation is invalid")
    if floor > owner._limits.journal_bytes:
        raise PrimaryStateCapacity("prepared-primary journal retention cannot preserve its reserve")
    return floor, representative


def reserved_bytes(owner: TransactionalPrimaryFiles) -> int:
    return max(_reservation(owner)[0], owner.progress_journal().backed_bytes)


def _backed(owner: TransactionalPrimaryFiles, floor: int) -> bool:
    if floor == 0:
        return True
    _database, journal = owner._guard.file_space()
    return journal.end_bytes >= floor and journal.allocated_bytes >= floor


def require(owner: TransactionalPrimaryFiles) -> None:
    """Point admission only; never grow, rewrite or scan the journal."""
    floor = reserved_bytes(owner)
    if not _backed(owner, floor):
        raise PrimaryStateCapacity("prepared-primary reserved journal backing is unavailable")


def validate_control_limit(owner: TransactionalPrimaryFiles, byte_limit: int) -> None:
    """One construction retains its explicitly selected control capacity."""
    _floor, representative = _reservation(owner)
    if representative:
        row = owner._one(
            "SELECT length(body),typeof(body) FROM primary_controls WHERE rowid=?",
            (representative,),
        )
        if row is None or row[1] != "blob" or row[0] != byte_limit + 9:
            raise PrimaryStateError("prepared-primary construction control capacity changed")


def _limit(db: sqlite3.Connection, value: int) -> None:
    with closing(db.execute(f"PRAGMA journal_size_limit={value}")) as cursor:
        if cursor.fetchone() != (value,):
            raise PrimaryStateError("prepared-primary journal retention setting was rejected")


@contextmanager
def _retention(owner: TransactionalPrimaryFiles, value: int) -> Iterator[None]:
    """Observe a controlled transaction's full retained extent, then restore policy."""
    if value != -1 and value < reserved_bytes(owner):
        raise PrimaryStateCapacity("prepared-primary journal retention cannot lower its reserve")
    db = owner._db()
    failed = False
    try:
        _limit(db, value)
        yield
    except BaseException as exc:
        failed = True
        if isinstance(exc, sqlite3.Error):
            owner._root._poisoned = True
            raise PrimaryStateError("prepared-primary journal preparation failed") from exc
        raise
    finally:
        try:
            _limit(db, owner._limits.journal_bytes)
        except BaseException:
            owner._root._poisoned = True
            if not failed:
                raise


def _warm(owner: TransactionalPrimaryFiles, identifier: int) -> None:
    from riverhog_client._prepared_primary_sqlite_controls import _length

    row = owner._one(
        "SELECT length(body),typeof(body) FROM primary_controls WHERE rowid=?", (identifier,)
    )
    if row is None or row[1] != "blob" or type(row[0]) is not int or row[0] < 9:
        raise PrimaryStateError("prepared-primary journal representative is unavailable")
    with owner._transaction(preparing=True) as db:
        with db.blobopen("primary_controls", "body", identifier) as blob:
            _length(blob, row[0] - 9)
            for offset in range(0, len(blob), _QUANTUM):
                blob.seek(offset)
                original = blob.read(min(_QUANTUM, len(blob) - offset))
                blob.seek(offset)
                blob.write(original)
        # The fixed reservation record must fit subsequent readiness commits too.
        with db.blobopen("primary_format", "journal_reserve", 1) as blob:
            original = blob.read(_RESERVE_BYTES)
            if len(original) != _RESERVE_BYTES:
                raise PrimaryStateError("prepared-primary journal reservation extent changed")
            blob.seek(0)
            blob.write(original)


def ensure(owner: TransactionalPrimaryFiles) -> None:
    """Explicit preparation can restore lost backing without changing selected controls."""
    _floor, representative = _reservation(owner)
    floor = reserved_bytes(owner)
    if _backed(owner, floor):
        return
    if not representative:
        raise PrimaryStateCapacity(
            "prepared-primary progress journal requires explicit preparation"
        )
    with _retention(owner, -1):
        _warm(owner, representative)
    require(owner)


def prepare(owner: TransactionalPrimaryFiles, identifier: int) -> None:
    """Warm one fixed extent before marking it usable; interrupted work stays pending."""
    floor, representative = _reservation(owner)
    ensure(owner)
    if reserved_bytes(owner) == 0:
        with _retention(owner, 0):
            # No usable control depends on initialization's historical journal extent.
            # Existing reservations are never released to measure another control.
            owner.sync()
    with _retention(owner, -1):
        _warm(owner, identifier)
        _database, journal = owner._guard.file_space()
        observed = journal.end_bytes
        if observed <= 0 or journal.allocated_bytes < observed:
            raise PrimaryStateCapacity(
                "prepared-primary journal preparation lacks allocated backing"
            )
        if observed > owner._limits.journal_bytes:
            # Restore the configured retained size through a valid bounded transaction.
            _limit(owner._db(), owner._limits.journal_bytes)
            owner.sync()
            raise PrimaryStateCapacity(
                "prepared-primary journal retention cannot admit this control"
            )
        if observed < floor:
            _warm(owner, representative)
        if not _backed(owner, max(floor, observed)):
            raise PrimaryStateCapacity("prepared-primary journal preparation lost its reserve")
    if observed > floor:
        floor, representative = observed, identifier
    with owner._transaction(preparing=True) as db:
        with db.blobopen("primary_format", "journal_reserve", 1) as blob:
            blob.write(floor.to_bytes(8, "big") + representative.to_bytes(8, "big"))
        with db.blobopen("primary_controls", "body", identifier) as blob:
            if blob.read(1) != b"\x02":
                raise PrimaryStateError("prepared-primary control preparation changed")
            blob.seek(0)
            blob.write(b"\x00")
    require(owner)
