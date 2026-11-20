"""Caller-retained construction receiving for one retained Native journal."""

from __future__ import annotations

import os
from pathlib import Path
from threading import Lock, get_ident
from typing import Protocol

from retained_resource import Group
from sqlite_admission import SqliteConnectionScope


class _Storage(Protocol):
    @property
    def storage_received(self) -> bool: ...

    def close(self) -> None: ...


class JournalConstruction:
    """Passive original file and SQLite receivers, separate from caller custody.

    The caller retains this frame before source checks or filesystem effects.
    Explicit file records and the original connection scope are allocated here,
    without opening, validating or reserving anything. Actual worker join and
    application namespace/capacity receiving remain with the caller.

    Journal creator and directory-sync records are transient. They receive
    before final-reader callback transfer; the exact SQLite scope remains until
    the original shared storage receives its final reader. This profile does
    not own a corpus's long-lived guard or authorize workspace reclamation.
    """

    def __init__(self, scope: SqliteConnectionScope, *, resource_limit: int) -> None:
        if not isinstance(scope, SqliteConnectionScope):
            raise TypeError("retained construction requires its original SQLite scope")
        needed = 2 if os.name == "posix" else 1
        if type(resource_limit) is not int or resource_limit < needed:
            raise ValueError("constructor file grants must precede effects")
        self.scope = scope
        self.files = Group(resource_limit)
        self.directory = self.files.resource("directory") if os.name == "posix" else None
        self.creator = self.files.resource("file")
        self.storage: _Storage | None = None
        self.phase = "new"
        self._gate = Lock()
        self._active_thread: int | None = None
        self._begun = False
        self._closed = False

    def begin(self) -> None:
        if not self._gate.acquire(blocking=False):
            raise BlockingIOError("the original Native constructor is executing")
        try:
            if self._begun or self._closed:
                raise RuntimeError("Native construction is single-attempt")
            self._active_thread = get_ident()
            self._begun = True
            self.phase = "opening"
            # The original shared SQLite reservation precedes file acquisition.
            self.scope.reserve()
        except BaseException:
            self.phase = "failed-retained"
            self._active_thread = None
            self._gate.release()
            raise

    def require_active(self) -> None:
        if self._active_thread != get_ident():
            raise RuntimeError("the original Native constructor must own this execution")

    def end(self) -> None:
        self.require_active()
        if self.phase != "ready":
            self.phase = "failed-retained"
        self._active_thread = None
        self._gate.release()

    def create_exclusive(self, path: Path) -> None:
        self.require_active()
        absolute = Path(path).absolute()
        encoded = os.fsencode(absolute) if os.name == "posix" else str(absolute)
        self.creator.acquire(
            encoded, writable=True, create=True, exclusive=True, private_owned=os.name == "posix"
        )
        self.creator.close()

    def sync_directory(self, path: Path) -> None:
        self.require_active()
        # Native retains its POSIX directory-sync profile. UCRT owns regular files.
        if self.directory is not None:
            if self.directory.state == "new":
                self.directory.acquire(os.fsencode(Path(path).absolute()), private_owned=True)
            self.directory.sync()

    def retain_storage(self, storage: _Storage) -> None:
        self.require_active()
        if self.storage is not None:
            raise RuntimeError("construction already retains its original Native storage")
        self.storage = storage

    def ready(self) -> None:
        self.require_active()
        if not self.files.close_step(self.files.capacity):
            raise BlockingIOError("original construction files have not received")
        self.phase = "ready"

    @property
    def dependents_received(self) -> bool:
        """Safe inside the final callback, before Native marks storage received."""
        return self.files.received and self.scope.received is True

    @property
    def received(self) -> bool:
        return self._closed and self.dependents_received

    def close_step(self) -> bool:
        if not self._gate.acquire(blocking=False):
            raise BlockingIOError("the original Native constructor has not returned")
        try:
            if self._closed:
                return True
            self.phase = "receiving"
            if self.storage is not None:
                self.storage.close()
                if not self.storage.storage_received:
                    # Closing one facade grants no authority over another reader.
                    return False
            self.scope.close()
            if not self.scope.received:
                return False
            if not self.files.close_step(self.files.capacity):
                return False
            self._closed = True
            self.phase = "received"
            return True
        finally:
            self._gate.release()
