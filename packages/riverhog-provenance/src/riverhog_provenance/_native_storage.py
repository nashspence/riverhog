"""Original shared Native storage and independently received reader leases."""

from __future__ import annotations

import shutil
import sqlite3
from collections.abc import Callable
from pathlib import Path
from threading import Lock
from typing import Protocol


class StorageFinalizer(Protocol):
    @property
    def alive(self) -> bool: ...

    def detach(self) -> object | None: ...


class StorageReader(Protocol):
    @property
    def _closing(self) -> bool: ...

    @property
    def _cleanup(self) -> StorageFinalizer: ...


class NativeStorage:
    """One original connection retained until its independent readers receive."""

    def __init__(
        self, connection: sqlite3.Connection, directory: Path, *, remove_on_release: bool = True
    ) -> None:
        self.connection = connection
        self.directory = directory
        self.remove_on_release = remove_on_release
        self.on_close: Callable[[], None] | None = None
        self._lock = Lock()
        self._readers = 0
        self._closing = False
        self._closed = False
        self._database_closed = False
        self._directory_removed = not remove_on_release

    def retain(self, owner: StorageReader | None = None) -> None:
        with self._lock:
            if (
                self._closing
                or self._closed
                or (owner is not None and (owner._closing or not owner._cleanup.alive))
            ):
                raise RuntimeError("journal summary is closed")
            self._readers += 1

    def release(self) -> None:
        with self._lock:
            if self._readers > 1:
                self._readers -= 1
                return
            if self._readers != 1:
                raise RuntimeError("journal storage has no remaining reader lease")
            self._closing = True
        # The final lease stays retained until every phase confirms closure.
        # New readers cannot enter while a close or owner release is uncertain.
        if not self._database_closed:
            self.connection.close()
            self._database_closed = True
        if not self._directory_removed:
            try:
                shutil.rmtree(self.directory)
            except FileNotFoundError:
                if self.directory.exists():
                    raise
            self._directory_removed = True
        if self.on_close is not None:
            self.on_close()
            self.on_close = None
        with self._lock:
            self._readers = 0
            self._closed = True


class NativeStorageLease:
    """Serialize one reader's explicit and finalizer release, retaining failed closes."""

    def __init__(self, storage: NativeStorage) -> None:
        self.storage = storage
        self._lock = Lock()
        self.closed = False

    def close(self) -> None:
        with self._lock:
            if not self.closed:
                self.storage.release()
                self.closed = True
