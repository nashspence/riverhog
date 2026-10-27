"""POSIX-owned disposable ordering scratch under one construction lease."""

from __future__ import annotations

import os
import sqlite3
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING

from riverhog_client._prepared_primary_types import PrimaryStateCapacity, PrimaryStateError

if TYPE_CHECKING:
    from riverhog_client._prepared_primary_posix import PrimaryFiles


_SCRATCH_NAMES = (
    "selection.sqlite3-journal",
    "selection.sqlite3",
    "candidate-selection.sqlite3",
)


def reserved_bytes(owner: PrimaryFiles) -> int:
    """Observe only known retained scratch names; never read database contents."""
    try:
        opened = owner.child("inventory-ordering", initialize=False)
    except FileNotFoundError:
        return 0
    total = 0
    with opened as files:
        for name in _SCRATCH_NAMES:
            try:
                descriptor = files._open(name)
            except FileNotFoundError:
                continue
            try:
                total += os.fstat(descriptor).st_size
            finally:
                os.close(descriptor)
    return total


def cleanup_step(owner: PrimaryFiles) -> bool:
    """A fixed number of unlinks removes derived state without enumerating the owner."""
    try:
        opened = owner.child("inventory-ordering", initialize=False)
    except FileNotFoundError:
        return True
    with opened as files:
        for name in _SCRATCH_NAMES:
            files.remove(name)
    return True


def ordered_rows(
    owner: PrimaryFiles, rows: Iterable[tuple[str, int, str]], *, byte_limit: int
) -> Iterator[tuple[str, int, str]]:
    """Order opaque rows in admitted scratch; retain no membership authority."""
    if type(byte_limit) is not int or byte_limit < 0:
        raise ValueError("ordering scratch admission must be nonnegative")
    page_size = 4096
    page_limit = (byte_limit - 2 * page_size) // page_size
    if page_limit < 1:
        raise PrimaryStateCapacity("owner inventory ordering has no scratch admission")
    try:
        opened = owner.child("inventory-ordering", initialize=True)
    except FileExistsError:
        opened = owner.child("inventory-ordering", initialize=False)
    with opened as files:
        for name in _SCRATCH_NAMES:
            files.remove(name)
        files.put(
            "selection.sqlite3",
            (),
            byte_limit=1,
            temporary_name="candidate-selection.sqlite3",
        )
        files.check_current()
        connection = sqlite3.connect(
            files.root / "selection.sqlite3", isolation_level=None, timeout=0
        )
        try:
            for setting in (
                f"page_size = {page_size}",
                f"max_page_count = {page_limit}",
                "cache_size = -512",
                "journal_mode = OFF",
                "synchronous = OFF",
            ):
                connection.execute("PRAGMA " + setting).close()
            connection.execute(
                "CREATE TABLE members (artifact_id TEXT PRIMARY KEY, bytes TEXT NOT NULL, "
                "sha256 TEXT NOT NULL) WITHOUT ROWID"
            ).close()
            for key, byte_count, digest in rows:
                files.check_current()
                connection.execute(
                    "INSERT INTO members VALUES (?, ?, ?)", (key, str(byte_count), digest)
                ).close()
            files.sync()
            after: str | None = None
            while True:
                files.check_current()
                cursor = connection.execute(
                    "SELECT artifact_id, bytes, sha256 FROM members ORDER BY artifact_id LIMIT 1"
                    if after is None
                    else (
                        "SELECT artifact_id, bytes, sha256 FROM members "
                        "WHERE artifact_id>? ORDER BY artifact_id LIMIT 1"
                    ),
                    () if after is None else (after,),
                )
                try:
                    row = cursor.fetchone()
                finally:
                    cursor.close()
                if row is None:
                    return
                after, byte_count, digest = row
                yield after, int(byte_count), digest
        except sqlite3.IntegrityError as exc:
            raise PrimaryStateError("owner inventory contains duplicate artifact IDs") from exc
        except sqlite3.OperationalError as exc:
            if getattr(exc, "sqlite_errorcode", None) == sqlite3.SQLITE_FULL:
                raise PrimaryStateCapacity(
                    "owner inventory ordering exceeds scratch admission"
                ) from exc
            raise
        finally:
            connection.close()
            files.remove("selection.sqlite3")
