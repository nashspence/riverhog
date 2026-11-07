"""Retain a completed finalizer step through its workflow acknowledgement."""

from __future__ import annotations

from collections.abc import Callable
from threading import Lock

from riverhog_core.catalog_db import SessionFactory
from riverhog_core.services.archive_finalizer_registry import RetainedArchiveFinalizer
from riverhog_core.services.archive_finalizer_terminal import (
    ArchiveFinalizerTerminal,
    TerminalDrainStatus,
)


class ArchiveFinalizerProgress:
    """Keep original work and feedback separate across lost acknowledgements.

    A successful child step remains pending until its original workflow receiver
    returns. Retrying feedback cannot advance the child or select fresh inputs.
    Feedback completion grants no provider closure or disposal rights. Terminal
    drainage retains both the original child and any unacknowledged result.
    """

    def __init__(
        self,
        factory: SessionFactory,
        *,
        collection_id: int,
        original: RetainedArchiveFinalizer,
        advance: Callable[[], bool],
        receive: Callable[[bool], None],
    ) -> None:
        self._original = original
        self._advance_original = advance
        self._receive = receive
        self._result: bool | None = None
        self._lock = Lock()
        self._terminal = ArchiveFinalizerTerminal(factory, collection_id)

    @property
    def has_pending(self) -> bool:
        return self._result is not None or self._original.has_pending

    def advance(self) -> bool:
        with self._lock:
            self._terminal.require_forward()
            if self._result is None:
                self._result = self._advance_original()
            result = self._result
            self._receive(result)
            self._result = None
            return result

    def drain_terminal(self, operation_id: str) -> TerminalDrainStatus:
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            return self._terminal.drain_original(operation_id, self._original)
        finally:
            self._lock.release()
