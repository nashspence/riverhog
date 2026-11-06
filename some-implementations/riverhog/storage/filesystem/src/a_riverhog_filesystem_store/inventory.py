"""Disposable prefix index owned by the active filesystem adapter instance.

Provider files remain authoritative. Rebuilding reads their metadata once at
startup; index construction costs O(N log N), with linear metadata reads and
disk space. A page uses indexed seeks plus O(page items), with a bounded SQLite
page cache. No traversal repeatedly rescans the provider namespace.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import sqlite3
import stat
from pathlib import Path

from riverhog_storage_adapter_protocol import CleanupCandidate, StorageAdapterRejection


class FilesystemInventory:
    """An instance-local cache, never a durable coordination or recovery authority."""

    def __init__(self, root: Path) -> None:
        self._directory = root / "inventory-cache"
        if self._directory.exists():
            mode = self._directory.stat(follow_symlinks=False).st_mode
            if not stat.S_ISDIR(mode):
                raise RuntimeError("filesystem inventory cache is not a directory")
            # Caller holds the exclusive storage-owner lock. Previous cache bytes
            # are discarded without interpreting them as provider state.
            shutil.rmtree(self._directory)
        self._directory.mkdir(mode=0o700)
        self._connection = sqlite3.connect(
            self._directory / "index.sqlite3", isolation_level=None, check_same_thread=False
        )
        os.chmod(self._directory / "index.sqlite3", 0o600)
        self._connection.execute("PRAGMA journal_mode=OFF")
        self._connection.execute("PRAGMA synchronous=OFF")
        self._connection.execute("PRAGMA cache_size=-2048")
        self._connection.execute(
            "CREATE TABLE candidates (path BLOB NOT NULL, kind TEXT NOT NULL, token TEXT NOT NULL, "
            "object_key TEXT NOT NULL, PRIMARY KEY(path, kind, token)) WITHOUT ROWID"
        )
        self._connection.execute(
            "CREATE INDEX candidates_object_key ON candidates(object_key, kind)"
        )
        self._valid = True

    def close(self) -> None:
        self._valid = False
        self._connection.close()
        try:
            shutil.rmtree(self._directory)
        except FileNotFoundError:
            pass

    def _execute(self, sql: str, parameters: tuple[object, ...] = ()) -> sqlite3.Cursor:
        if not self._valid:
            raise StorageAdapterRejection(
                "provider_unavailable", "filesystem inventory is unavailable"
            )
        try:
            return self._connection.execute(sql, parameters)
        except sqlite3.Error as exc:
            self._valid = False
            raise StorageAdapterRejection(
                "provider_unavailable", "filesystem inventory requires owner recovery"
            ) from exc

    def check(self) -> None:
        self._execute("SELECT 1").fetchone()

    def add(self, item: CleanupCandidate) -> None:
        path = item.object_path.encode("utf-8")
        self._execute(
            "INSERT OR IGNORE INTO candidates(path, kind, token, object_key) VALUES (?, ?, ?, ?)",
            (path, item.kind, item.token, hashlib.sha256(path).hexdigest()),
        )

    def remove(self, item: CleanupCandidate) -> None:
        self._execute(
            "DELETE FROM candidates WHERE path = ? AND kind = ? AND token = ?",
            (item.object_path.encode("utf-8"), item.kind, item.token),
        )

    def remove_key(self, object_key: str, *, kind: str | None = None) -> None:
        sql = "DELETE FROM candidates WHERE object_key = ?"
        parameters: tuple[object, ...] = (object_key,)
        if kind is not None:
            sql += " AND kind = ?"
            parameters += (kind,)
        self._execute(sql, parameters)

    def page(
        self, prefix: str, *, after: CleanupCandidate | None, limit: int
    ) -> tuple[CleanupCandidate, ...]:
        if type(limit) is not int or limit < 1:
            raise ValueError("filesystem inventory page size must be positive")
        if after is not None and not after.object_path.startswith(prefix):
            raise ValueError("filesystem inventory position differs from its prefix")
        lower = prefix.encode("utf-8")
        upper = lower + bytes((255,))
        parameters: tuple[object, ...]
        if after is None:
            sql = "SELECT path, kind, token FROM candidates WHERE path >= ? AND path < ?"
            parameters = (lower, upper)
        else:
            # The validated position is already in this prefix. Seek from the
            # complete compound position, rather than filtering from prefix start.
            sql = (
                "SELECT path, kind, token FROM candidates "
                "WHERE (path, kind, token) > (?, ?, ?) AND path < ?"
            )
            parameters = (after.object_path.encode("utf-8"), after.kind, after.token, upper)
        sql += " ORDER BY path, kind, token LIMIT ?"
        rows = self._execute(sql, parameters + (limit,)).fetchall()
        return tuple(
            CleanupCandidate(object_path=path.decode("utf-8"), kind=kind, token=token)
            for path, kind, token in rows
        )
