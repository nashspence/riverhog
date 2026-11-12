"""Passive constructor receiving frame, independent of Native and SQLite.

The caller retains this object before calling any effectful method. A connection
scope is an original, pre-retained receiver supplied by its own owner. It must
not infer receiving from a missing Python return or an aggregate shared counter.
"""
from __future__ import annotations

import os
from pathlib import Path
from threading import Lock
from typing import Protocol

from . import Group


class ConnectionScope(Protocol):
    def connect(self, database, **options): ...
    def close(self) -> None: ...
    @property
    def received(self) -> bool: ...


class Construction:
    """Two admitted POSIX owners or one admitted Windows CRT owner.

    Construction allocates bookkeeping only. The scope, storage, borrowed readers,
    application namespace and actual worker execution retain separate receipts.
    No destructor, context-manager exit, or auto-disposal is supplied.
    """
    def __init__(self, scope: ConnectionScope, *, resource_limit: int):
        needed = 2 if os.name == "posix" else 1
        if type(resource_limit) is not int or resource_limit < needed:
            raise ValueError("constructor file/directory grants must precede effects")
        self.scope = scope
        self.files = Group(resource_limit)
        self.directory = self.files.resource("directory") if os.name == "posix" else None
        self.creator = self.files.resource("file")
        self.storage = None
        self.phase = "new"
        self._gate = Lock()
        self._begun = False
        self._closed = False

    def enter(self):
        if not self._gate.acquire(blocking=False):
            raise BlockingIOError("original constructor is executing")
        if self._begun or self._closed:
            self._gate.release()
            raise RuntimeError("constructor acquisition is single-attempt")
        self._begun = True
        self.phase = "opening"

    def leave(self):
        # This records only that the synchronous call returned/raised. A caller
        # using an executor still owes actual join before releasing its capacity.
        if self.phase != "ready":
            self.phase = "failed-retained"
        self._gate.release()

    def create_exclusive(self, path: Path):
        path = Path(path).absolute()
        encoded = os.fsencode(path) if os.name == "posix" else str(path)
        self.creator.acquire(encoded, writable=True, exclusive=True,
                             private_owned=os.name == "posix")
        self.creator.close()

    def sync_directory(self, path: Path):
        # Preserves the maintained Native profile: POSIX sync, not a fictional
        # Windows directory fsync/CRT directory implementation.
        if self.directory is not None:
            if self.directory.state == "new":
                self.directory.acquire(os.fsencode(Path(path).absolute()), private_owned=True)
            self.directory.sync()

    def connect(self, database, **options):
        return self.scope.connect(database, **options)

    def retain_storage(self, receiver):
        if self.storage is not None and self.storage is not receiver:
            raise RuntimeError("constructor cannot replace the original storage receiver")
        self.storage = receiver

    def ready(self):
        # Transient creators/directories must receive before an external final-
        # reader callback can be given parent-custody release authority.
        if not self.files.close_step(self.files.capacity):
            raise RuntimeError("construction resources did not receive")
        self.phase = "ready"

    @property
    def dependents_received(self):
        return self.files.received and self.scope.received is True

    @property
    def received(self):
        return self._closed and self.dependents_received

    def close_step(self):
        if not self._gate.acquire(blocking=False):
            raise BlockingIOError("original constructor has not returned")
        try:
            if self._closed:
                return True
            self.phase = "receiving"
            if self.storage is not None:
                self.storage.close()
                if self.storage.received is not True:
                    return False  # a borrowed reader still owns the connection
            self.scope.close()
            if self.scope.received is not True:
                return False  # unreturned/indeterminate connection remains charged
            if not self.files.close_step(self.files.capacity):
                return False
            self._closed = True
            self.phase = "received"
            return True
        finally:
            self._gate.release()
