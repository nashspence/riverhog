"""Retain a logical upload's exact next action without replaying its prefix."""

from __future__ import annotations

from collections.abc import Callable, Generator
from dataclasses import dataclass, field
from threading import Lock
from typing import Any, cast

from riverhog_protocol.errors import Conflict

from riverhog_core.provider_write_dispatch import ProviderWriteAdmissionRefused


@dataclass(frozen=True, slots=True)
class _UploadAction:
    execute: Callable[[], Any] = field(repr=False)
    checkpoint: bool = False


type UploadSteps[T] = Generator[_UploadAction, Any, T]


def upload_step[T](execute: Callable[[], T], *, checkpoint: bool = False) -> UploadSteps[T]:
    """Yield before a side effect; return only the original callback's result."""
    result = yield _UploadAction(execute, checkpoint)
    return cast(T, result)


class UploadContinuation[T]:
    """One original stack, prepared buffer and current action.

    A known admission refusal may retry the same not-dispatched action. A
    checkpoint retries only its retained exact report. Every other callback
    failure freezes forward progress: another call cannot invent permission to
    dispatch it again. Closing drops local inputs, never provider obligations
    or global execution ownership. The service must keep those independently.
    """

    def __init__(self, steps: UploadSteps[T]) -> None:
        self._steps = steps
        self._lock = Lock()
        self._action: _UploadAction | None = None
        self._send: Any = None
        self._started = False
        self._failed = False
        self._closed = False
        self._done = False
        self._result: T | None = None

    @property
    def has_pending(self) -> bool:
        return not self._done and not self._closed

    def advance(self) -> bool:
        """Execute at most one retained action in the owning physical worker."""
        if not self._lock.acquire(blocking=False):
            raise Conflict("original upload continuation is running")
        try:
            if self._closed:
                raise Conflict("original upload continuation is closed")
            if self._failed:
                raise Conflict("original upload action outcome is unresolved")
            if self._done:
                return True
            if self._action is None:
                try:
                    self._action = (
                        self._steps.send(self._send) if self._started else next(self._steps)
                    )
                    self._started = True
                    self._send = None
                except StopIteration as completed:
                    self._result = completed.value
                    self._done = True
                    return True
                except BaseException:
                    self._failed = True
                    raise
            self._failed = not self._action.checkpoint
            try:
                self._send = self._action.execute()
            except ProviderWriteAdmissionRefused:
                self._failed = False
                raise
            self._action = None
            self._failed = False
            return False
        finally:
            self._lock.release()

    def run(self) -> T:
        """Run retained actions; an exception leaves this exact continuation."""
        while not self.advance():
            pass
        return cast(T, self._result)

    def close(self) -> bool:
        """Nonblocking local release, only after any current action has exited."""
        if not self._lock.acquire(blocking=False):
            return False
        try:
            if not self._closed:
                self._steps.close()
                self._action = None
                self._send = None
                self._closed = True
            return True
        finally:
            self._lock.release()


def run_upload[T](steps: UploadSteps[T]) -> T:
    """Synchronous use of the same continuation implementation."""
    original = UploadContinuation(steps)
    try:
        return original.run()
    finally:
        original.close()
