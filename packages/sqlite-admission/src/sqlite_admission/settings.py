"""Explicit per-connection working-set settings for owned SQLite projections."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal


class SqliteConfigurationError(RuntimeError):
    """The loaded connection cannot confirm its selected settings."""


@dataclass(frozen=True, slots=True, kw_only=True)
class SqliteConnectionSettings:
    """Connection-local targets, applied before schema or input work.

    cache_kib is a suggested cache target, not a private heap ceiling. None
    preserves the owner's existing SQLite cache default. MEMORY temporary
    storage needs the application's separate aggregate heap policy; FILE
    storage needs its separate physical scratch admission. Mmap is disabled.
    These settings do not change durability, identity, journal mode or schema.
    """

    cache_kib: int | None = None
    temporary_storage: Literal["FILE", "MEMORY"] = "FILE"

    def __post_init__(self) -> None:
        if self.cache_kib is not None and (type(self.cache_kib) is not int or self.cache_kib < 0):
            raise ValueError("SQLite cache target must be nonnegative integer KiB or None")
        if self.temporary_storage not in ("FILE", "MEMORY"):
            raise ValueError("SQLite temporary storage must be FILE or MEMORY")


def _rows(connection: sqlite3.Connection, statement: str, count: int) -> list[Any]:
    cursor = connection.cursor()
    try:
        cursor.execute(statement)
        return cursor.fetchmany(count)
    finally:
        cursor.close()


def _configure(
    connection: sqlite3.Connection,
    settings: SqliteConnectionSettings,
    read_rows: Callable[[str, int], list[Any]] | None = None,
) -> None:
    read = (
        (lambda statement, count: _rows(connection, statement, count))
        if read_rows is None
        else read_rows
    )
    # Changing temp_store deletes existing TEMP objects. Refuse a connection
    # that already owns TEMP/attached databases or an active transaction.
    if connection.in_transaction:
        raise SqliteConfigurationError("SQLite settings require a fresh transaction-free owner")
    databases = read("PRAGMA database_list", 3)
    if len(databases) != 1 or databases[0][1] != "main":
        raise SqliteConfigurationError("SQLite settings require only the owned main database")
    compiled = read(
        "SELECT sqlite_compileoption_used('TEMP_STORE=0'), "
        "sqlite_compileoption_used('TEMP_STORE=3')",
        1,
    )
    if (
        len(compiled) != 1
        or tuple(compiled[0]) not in ((0, 0), (1, 0), (0, 1))
        or (compiled[0][0] == 1 and settings.temporary_storage == "MEMORY")
        or (compiled[0][1] == 1 and settings.temporary_storage == "FILE")
    ):
        raise SqliteConfigurationError("SQLite compile-time temporary storage differs")
    if settings.cache_kib is not None:
        read(f"PRAGMA main.cache_size=-{settings.cache_kib}", 1)
        observed = read("PRAGMA main.cache_size", 1)
        if len(observed) != 1 or tuple(observed[0]) != (-settings.cache_kib,):
            raise SqliteConfigurationError("SQLite cache target was not selected")
    read("PRAGMA mmap_size=0", 1)
    mapping = read("PRAGMA mmap_size", 1)
    if mapping and (len(mapping) != 1 or tuple(mapping[0]) != (0,)):
        raise SqliteConfigurationError("SQLite memory mapping is still enabled")
    read("PRAGMA temp_store=" + settings.temporary_storage, 1)
    selected = read("PRAGMA temp_store", 1)
    wanted = 1 if settings.temporary_storage == "FILE" else 2
    if len(selected) != 1 or tuple(selected[0]) != (wanted,):
        raise SqliteConfigurationError("SQLite temporary storage was not selected")
