"""Close-ready SQLite ownership for persistent FTP custody state."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self


@dataclass(frozen=True, slots=True)
class StateWrite:
    rowcount: int
    lastrowid: int | None


class FtpOperationalState:
    """Admit the actual connection before SQL, retaining uncertain cleanup.

    The database and its sidecars are persistent custody state. This owner never
    removes them, including after construction failure or confirmed close.
    A supplied factory retains any connection it fails to return.
    """

    def __init__(
        self,
        path: Path,
        *,
        connection_factory: Callable[..., sqlite3.Connection] | None = None,
        on_storage_open: Callable[[Self], None] | None = None,
    ) -> None:
        self._closing = False
        self._cleanup_unconfirmed = False
        self._connection_closed = False
        self._cursor: sqlite3.Cursor | None = None
        connect = sqlite3.connect if connection_factory is None else connection_factory
        self._db = connect(path)
        try:
            if on_storage_open is not None:
                on_storage_open(self)
            self._require_open()
            self.execute("PRAGMA foreign_keys = ON")
            self.execute("PRAGMA synchronous = FULL")
        except BaseException:
            if not self._closing:
                self.close()
            raise

    def _require_open(self) -> None:
        if self._closing:
            raise RuntimeError("FTP operational state is closed")

    @property
    def closing(self) -> bool:
        return self._closing

    @property
    def cleanup_unconfirmed(self) -> bool:
        """An actual cleanup call failed; a normal in-flight close is independent."""
        return self._cleanup_unconfirmed

    @property
    def cleanup_complete(self) -> bool:
        return self._connection_closed and self._cursor is None

    def __enter__(self) -> Self:
        self._require_open()
        return self

    def __exit__(self, *_exc: object) -> None:
        if not self._closing:
            self.close()

    def _close_cursor(self) -> None:
        if self._cursor is not None:
            try:
                self._cursor.close()
            except BaseException:
                self._cleanup_unconfirmed = True
                self._closing = True
                raise
            self._cursor = None

    def close(self) -> None:
        self._closing = True
        try:
            self._close_cursor()
            if not self._connection_closed:
                self._db.close()
                self._connection_closed = True
        except BaseException:
            self._cleanup_unconfirmed = True
            raise
        else:
            self._cleanup_unconfirmed = False

    def _run[T](
        self,
        statement: str,
        parameters: tuple[object, ...],
        read: Callable[[sqlite3.Cursor], T],
        *,
        script: bool = False,
    ) -> T:
        self._require_open()
        self._cursor = (
            self._db.executescript(statement) if script else self._db.execute(statement, parameters)
        )
        try:
            self._require_open()
            result = read(self._cursor)
            self._require_open()
            return result
        finally:
            if not self._closing:
                self._close_cursor()

    def execute(self, statement: str, parameters: tuple[object, ...] = ()) -> StateWrite:
        return self._run(
            statement, parameters, lambda cursor: StateWrite(cursor.rowcount, cursor.lastrowid)
        )

    def executescript(self, statement: str) -> None:
        self._run(statement, (), lambda _cursor: None, script=True)

    def one(self, statement: str, parameters: tuple[object, ...] = ()) -> Any:
        """Read one point result and close its cursor before returning it."""
        return self._run(statement, parameters, lambda cursor: cursor.fetchone())

    def all(self, statement: str, parameters: tuple[object, ...] = ()) -> list[Any]:
        """Read the caller's bounded page and close its cursor before handoff."""
        return self._run(statement, parameters, lambda cursor: cursor.fetchall())

    def commit(self) -> None:
        self._require_open()
        self._db.commit()
        self._require_open()
