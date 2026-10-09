"""Component-local admission and exclusive ownership of extension runtime state."""

from __future__ import annotations

import os
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class ExecutionOwner:
    """One component process's exact invocation, attempt and admission probe."""

    invocation_id: str
    attempt: int
    process_id: str
    probe_id: str


class ExecutionPermit(Protocol):
    """An owned resource reservation supervised by the selected local adapter.

    Activation must validate the reservation and arrange renewal and containment
    for its consumers. Losing permission must stop/fence consumption. Release is
    idempotent and occurs only after payload execution and its children stop.
    The adapter owns crash containment; expiry alone never proves that a consumer
    stopped. No lease or broker credentials enter an accepted invocation.
    """

    @property
    def owner(self) -> ExecutionOwner: ...

    def activate(self, cancellation: threading.Event, *, deadline: float) -> None: ...

    def release(self, *, deadline: float) -> None: ...


@runtime_checkable
class ExecutionConsumerOwnership(Protocol):
    """Optional local descriptors held by the native containment supervisor.

    These are private operating-system reservations, never wire contract fields.
    The permit remains responsible for activation and release after containment.
    """

    @property
    def consumer_descriptors(self) -> tuple[int, ...]: ...


class ExecutionAdmission(Protocol):
    """Bounded local port; resource identity and scheduling remain adapter-owned.

    A deferred probe returns None without parking a payload worker. An adapter
    that registers requests must deduplicate probes and withdraw registrations.
    Calls must complete by the supplied monotonic deadline, including unknown
    outcomes after transport failure; otherwise the component retains its actual
    in-flight probe and cannot pretend the caller stopped.
    """

    def probe(self, owner: ExecutionOwner, *, deadline: float) -> ExecutionPermit | None: ...

    def withdraw(self, owner: ExecutionOwner, *, deadline: float) -> None: ...


@dataclass(frozen=True, slots=True)
class _ImmediatePermit:
    owner: ExecutionOwner

    def activate(self, cancellation: threading.Event, *, deadline: float) -> None:
        if cancellation.is_set():
            raise InterruptedError("extension execution was canceled before activation")

    def release(self, *, deadline: float) -> None:
        pass


class ImmediateExecutionAdmission:
    """Default admission uses already-reserved local payload capacity."""

    def probe(self, owner: ExecutionOwner, *, deadline: float) -> ExecutionPermit:
        return _ImmediatePermit(owner)

    def withdraw(self, owner: ExecutionOwner, *, deadline: float) -> None:
        pass


class ExclusiveStateOwner:
    """Hold an OS lock for the complete lifetime of a component state owner."""

    def __init__(self, state_root: Path) -> None:
        path = state_root / ".owner.lock"
        descriptor = os.open(path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
        self._stream = os.fdopen(descriptor, "r+b", buffering=0)
        try:
            if sys.platform == "win32":
                import msvcrt

                if path.stat().st_size == 0:
                    self._stream.write(b"\0")
                self._stream.seek(0)
                msvcrt.locking(self._stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self._stream.close()
            raise RuntimeError("extension state already has an active owner") from exc

    @property
    def descriptor(self) -> int:
        return self._stream.fileno()

    def close(self) -> None:
        # Closing the locked open file description releases ownership on both
        # platforms; never unlink a live lock and create a second lock domain.
        self._stream.close()


__all__ = [
    "ExclusiveStateOwner",
    "ExecutionAdmission",
    "ExecutionConsumerOwnership",
    "ExecutionOwner",
    "ExecutionPermit",
    "ImmediateExecutionAdmission",
]
