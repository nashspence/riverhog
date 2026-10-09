"""Cooperative exclusive admission on one deployment-provided local lock file.

Participants must retain this same file/inode rather than unlinking or replacing
it. Opening and validating the file happens at deployment startup; probes only
try a nonblocking kernel lock. Resource naming never enters an invocation.
"""

from __future__ import annotations

import os
import stat
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Self

from stove0_extension_support import ExecutionOwner


@dataclass(frozen=True, slots=True)
class _FilePermit:
    owner: ExecutionOwner
    admission: FileExecutionAdmission

    @property
    def consumer_descriptors(self) -> tuple[int, ...]:
        with self.admission._lock:
            if self.admission._permit is not self:
                raise RuntimeError("execution reservation was released")
            return (self.admission._descriptor,)

    def activate(self, cancellation: threading.Event, *, deadline: float) -> None:
        with self.admission._lock:
            if self.admission._permit is not self:
                raise RuntimeError("execution reservation was released")
            if cancellation.is_set():
                raise InterruptedError("execution canceled before lease activation")
            if time.monotonic() >= deadline:
                raise TimeoutError("execution lease activation deadline elapsed")

    def release(self, *, deadline: float) -> None:
        import fcntl

        with self.admission._lock:
            if self.admission._permit is not self:
                return
            if time.monotonic() >= deadline:
                raise TimeoutError("execution lease release deadline elapsed")
            fcntl.flock(self.admission._descriptor, fcntl.LOCK_UN)
            self.admission._permit = None


class FileExecutionAdmission:
    """One exclusive local resource, shared with ordinary flock participants.

    A busy lock immediately defers without acquiring a workspace or payload
    worker. The native supervisor inherits the locked descriptor, keeping the
    resource reserved through consumer containment after component death.
    """

    def __init__(self, path: Path) -> None:
        if os.name != "posix":
            raise RuntimeError("cooperative file admission requires POSIX locking")
        self._lock = threading.Lock()
        self._permit: _FilePermit | None = None
        self._descriptor = os.open(
            path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600
        )
        if not stat.S_ISREG(os.fstat(self._descriptor).st_mode):
            os.close(self._descriptor)
            self._descriptor = -1
            raise ValueError("execution lease must be a regular local file")

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def probe(self, owner: ExecutionOwner, *, deadline: float) -> _FilePermit | None:
        import fcntl

        with self._lock:
            if self._descriptor < 0:
                raise RuntimeError("execution admission is closed")
            if time.monotonic() >= deadline:
                raise TimeoutError("execution lease probe deadline elapsed")
            if self._permit is not None:
                return self._permit if self._permit.owner == owner else None
            try:
                fcntl.flock(self._descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return None
            if time.monotonic() >= deadline:
                fcntl.flock(self._descriptor, fcntl.LOCK_UN)
                return None
            self._permit = _FilePermit(owner, self)
            return self._permit

    def withdraw(self, owner: ExecutionOwner, *, deadline: float) -> None:
        # No external registration is created by a deferred kernel-lock probe.
        # A granted permit is released only after its supervised consumers stop.
        pass

    def close(self) -> None:
        with self._lock:
            if self._permit is not None:
                raise RuntimeError("execution consumers still own the cooperative lease")
            if self._descriptor >= 0:
                os.close(self._descriptor)
                self._descriptor = -1
