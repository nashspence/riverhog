"""Fixed original resource grants retained with the owned SQLite allocation."""

from __future__ import annotations

import hashlib
from contextlib import closing
from dataclasses import dataclass
from typing import TYPE_CHECKING

from riverhog_client._prepared_primary_types import PrimaryStateCapacity, PrimaryStateError

if TYPE_CHECKING:
    from riverhog_client._prepared_primary_sqlite import (
        SqlitePrimaryLimits,
        TransactionalPrimaryFiles,
    )

_TAG = b"RHGRANT1"
_SIZE = 64
_MAXIMUM = (1 << 63) - 1


@dataclass(frozen=True, slots=True)
class SqliteResourceGrants:
    identity: bytes
    database_bytes: int
    journal_bytes: int
    revision: int

    def encode(self) -> bytes:
        if type(self.identity) is not bytes or len(self.identity) != 32:
            raise PrimaryStateError("prepared-primary resource identity is invalid")
        for value in (self.database_bytes, self.journal_bytes):
            if type(value) is not int or not 4096 <= value <= _MAXIMUM or value % 4096:
                raise PrimaryStateError("prepared-primary retained resource grant is invalid")
        if type(self.revision) is not int or not 0 <= self.revision <= (1 << 64) - 1:
            raise PrimaryStateError("prepared-primary resource revision is invalid")
        return (
            _TAG
            + self.identity
            + b"".join(
                value.to_bytes(8, "big")
                for value in (self.database_bytes, self.journal_bytes, self.revision)
            )
        )


def identity(source: object, geometry: str) -> bytes:
    if type(source) is not str or not source or len(source) > 4096:
        raise PrimaryStateError("prepared-primary resource engine identity is unavailable")
    source_bytes = source.encode("utf-8")
    geometry_bytes = geometry.encode("utf-8")
    return hashlib.sha256(
        _TAG
        + len(source_bytes).to_bytes(8, "big")
        + source_bytes
        + len(geometry_bytes).to_bytes(8, "big")
        + geometry_bytes
    ).digest()


def _decode(raw: object, expected: bytes) -> SqliteResourceGrants:
    if type(raw) is not bytes or len(raw) != _SIZE or raw[:8] != _TAG:
        raise PrimaryStateError("prepared-primary resource record is invalid")
    result = SqliteResourceGrants(
        raw[8:40], *(int.from_bytes(raw[start : start + 8], "big") for start in (40, 48, 56))
    )
    result.encode()
    if result.identity != expected:
        raise PrimaryStateError("prepared-primary resource engine or geometry changed")
    return result


def read(files: TransactionalPrimaryFiles) -> SqliteResourceGrants:
    """One fixed record; no payload, operation-cursor or reserve enumeration."""
    with files._mutex:
        files._guard.check_current()
        row = files._one("SELECT resource_grants FROM primary_format WHERE id=1")
        if row is None or len(row) != 1:
            raise PrimaryStateError("prepared-primary resource record is missing")
        return _decode(row[0], files._root._resource_identity)


def _configure(files: TransactionalPrimaryFiles, limits: SqlitePrimaryLimits) -> None:
    db = files._db()
    for name, value in (
        ("max_page_count", limits.database_bytes // 4096),
        ("journal_size_limit", limits.journal_bytes),
    ):
        with closing(db.execute(f"PRAGMA {name}={value}")) as cursor:
            observed = cursor.fetchone()
        files._guard.check_operation()
        if observed != (value,):
            raise PrimaryStateError("prepared-primary resource setting was rejected")


def reconcile(
    files: TransactionalPrimaryFiles, offered: SqlitePrimaryLimits, *, geometry: str
) -> None:
    """Validate original grants after engine recovery and before the open barrier."""
    from riverhog_client._prepared_primary_sqlite import SqlitePrimaryLimits

    row = files._one("SELECT sqlite_source_id()")
    files._resource_identity = identity(None if row is None else row[0], geometry)
    retained = read(files)
    if (
        offered.database_bytes < retained.database_bytes
        or offered.journal_bytes < retained.journal_bytes
    ):
        raise PrimaryStateCapacity(
            "prepared-primary cold recovery requires its original resource grants"
        )
    # A larger offer permits recovery; only an explicit increase changes grants.
    limits = SqlitePrimaryLimits(
        retained.database_bytes, retained.journal_bytes, offered.cache_bytes
    )
    _configure(files, limits)
    files._resource_limits = limits


def increase(
    files: TransactionalPrimaryFiles, *, database_bytes: int, journal_bytes: int
) -> SqliteResourceGrants:
    """Increase granted ceilings independently of candidate or cleanup progress."""
    from riverhog_client._prepared_primary_sqlite import SqlitePrimaryLimits

    with files._mutex:
        if files is not files._root:
            raise PrimaryStateError(
                "prepared-primary resource increases require the container root"
            )
        db = files._db()
        if db.in_transaction:
            raise PrimaryStateError(
                "prepared-primary resource increase requires a settled transaction"
            )
        limits = SqlitePrimaryLimits(database_bytes, journal_bytes, files._limits.cache_bytes)
        current = read(files)
        if database_bytes < current.database_bytes or journal_bytes < current.journal_bytes:
            raise ValueError("prepared-primary resource increases cannot lower retained grants")
        if (database_bytes, journal_bytes) == (current.database_bytes, current.journal_bytes):
            return current
        if current.revision == (1 << 64) - 1:
            raise PrimaryStateCapacity("prepared-primary resource revision is exhausted")
        selected = SqliteResourceGrants(
            current.identity, database_bytes, journal_bytes, current.revision + 1
        )
        with files._transaction():
            _configure(files, limits)
            with db.blobopen("primary_format", "resource_grants", 1) as blob:
                if len(blob) != _SIZE or blob.read(_SIZE) != current.encode():
                    raise PrimaryStateError(
                        "prepared-primary resource grant changed during increase"
                    )
                blob.seek(0)
                blob.write(selected.encode())
        files._resource_limits = limits
        return selected
