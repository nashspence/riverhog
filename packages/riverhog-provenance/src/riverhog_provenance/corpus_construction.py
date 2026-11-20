"""Caller-retained POSIX guard and SQLite receiving for one Native corpus."""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from threading import Lock, get_ident
from typing import Protocol

from retained_resource import Group
from sqlite_admission import SqliteConnectionScope


class _Storage(Protocol):
    def _receive_storage(self) -> None: ...
    def _release_storage(self) -> None: ...


class CorpusConstruction:
    """Passive original scope, long-lived guard/borrow and transient sync record.

    The caller retains this exact frame before Native effects. It supplies no
    namespace, worker-join or aggregate process-memory authority. The guard's
    original borrower stays active through validation and actual database close.
    Receiving cannot overlap construction or a bounded Native transaction.
    """

    def __init__(self, scope: SqliteConnectionScope, *, resource_limit: int) -> None:
        if os.name != "posix":
            raise RuntimeError("retained corpus custody requires the POSIX profile")
        if not isinstance(scope, SqliteConnectionScope):
            raise TypeError("retained corpus requires its original SQLite scope")
        if type(resource_limit) is not int or resource_limit < 3:
            raise ValueError("corpus guard, borrow and directory grants must precede effects")
        self.scope = scope
        self.files = Group(resource_limit)
        self.directory = self.files.resource("directory")
        self.guard = self.files.resource("file")
        self.borrow = self.files.borrow(self.guard)
        self.storage: _Storage | None = None
        self.phase = "new"
        self._gate = Lock()
        self._active_thread: int | None = None
        self._begun = False
        self._closed = False

    def require_active(self) -> None:
        if self._active_thread != get_ident():
            raise RuntimeError("the original Native corpus must own this execution")

    def begin(self) -> None:
        if not self._gate.acquire(blocking=False):
            raise BlockingIOError("the original Native corpus is executing")
        try:
            if self._begun or self._closed:
                raise RuntimeError("Native corpus construction is single-attempt")
            self._begun = True
            self._active_thread = get_ident()
            self.phase = "opening"
            self.scope.reserve()
        except BaseException:
            self.phase = "failed-retained"
            self._active_thread = None
            self._gate.release()
            raise

    def end(self) -> None:
        self.require_active()
        if self.phase != "ready":
            self.phase = "failed-retained"
        self._active_thread = None
        self._gate.release()

    @contextmanager
    def execution(self) -> Iterator[None]:
        if not self._gate.acquire(blocking=False):
            raise BlockingIOError("the original Native corpus is executing")
        try:
            if self.phase != "ready" or self._closed:
                raise RuntimeError("retained corpus is closed or unavailable")
            self._active_thread = get_ident()
            yield
        finally:
            self._active_thread = None
            self._gate.release()

    def retain_storage(self, storage: _Storage) -> None:
        self.require_active()
        if self.storage is not None:
            raise RuntimeError("corpus construction already retains its original storage")
        self.storage = storage

    def acquire_guard(self, path: Path, *, create: bool) -> None:
        self.require_active()
        self.guard.acquire(
            os.fsencode(Path(path).absolute()),
            writable=True,
            create=create,
            exclusive=create,
            private_owned=True,
        )
        self.borrow.acquire()

    def descriptor(self) -> int:
        self.require_active()
        return self.borrow.fileno()

    def sync_directory(self, path: Path) -> None:
        self.require_active()
        self.directory.acquire(os.fsencode(Path(path).absolute()), private_owned=True)
        self.directory.sync()

    def ready(self) -> None:
        self.require_active()
        # Only this transient receives here; the original guard stays pinned.
        self.directory.close()
        if not self.directory.received:
            raise BlockingIOError("corpus directory sync receiving remains pending")
        self.phase = "ready"

    @property
    def dependents_received(self) -> bool:
        """Safe during the callback which precedes Native's final marker."""
        return self.files.received and self.scope.received is True

    @property
    def received(self) -> bool:
        return self._closed and self.dependents_received

    def close_step(self) -> bool:
        if not self._gate.acquire(blocking=False):
            raise BlockingIOError("the original Native corpus execution has not returned")
        try:
            if self._closed:
                return True
            self._active_thread = get_ident()
            self.phase = "receiving"
            if self.storage is not None:
                self.storage._receive_storage()
            self.scope.close()
            if not self.scope.received:
                return False
            self.borrow.close()
            if not self.borrow.received:
                return False
            if not self.files.close_step(self.files.capacity):
                return False
            if self.storage is not None:
                self.storage._release_storage()
            self._closed = True
            self.phase = "received"
            return True
        finally:
            self._active_thread = None
            self._gate.release()
