"""Keep component ownership until supervised native payload consumers stop."""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from stove0_extension_support import ExclusiveStateOwner
    from stove0_extension_support.subprocess import SupervisedProcess


class ConsumerScope:
    def __init__(self, owner: ExclusiveStateOwner, cancellation: threading.Event | None) -> None:
        self.owner = owner
        self.processes: set[SupervisedProcess[Any]] = set()
        self.lock = threading.Lock()
        self.cancellation = cancellation
        self._closed = threading.Event()
        self._monitor = threading.Thread(target=self._watch, daemon=True)
        self._monitor.start()

    def _watch(self) -> None:
        while not self._closed.wait(0.05):
            if self.cancellation is not None and self.cancellation.is_set():
                with self.lock:
                    for process in self.processes:
                        process.terminate()

    def retain(self, process: SupervisedProcess[Any]) -> None:
        with self.lock:
            self.processes.add(process)
            if self.cancellation is not None and self.cancellation.is_set():
                process.terminate()

    def stopped(self, process: SupervisedProcess[Any]) -> None:
        with self.lock:
            self.processes.discard(process)

    def close(self) -> None:
        self._closed.set()
        self._monitor.join()
        with self.lock:
            processes = tuple(self.processes)
        for process in processes:
            process.terminate()
        for process in processes:
            process.wait()


CURRENT_CONSUMERS: ContextVar[ConsumerScope | None] = ContextVar(
    "extension_consumers", default=None
)


@contextmanager
def consumer_scope(
    owner: ExclusiveStateOwner | None, cancellation: threading.Event | None = None
) -> Iterator[None]:
    if owner is None:
        yield
        return
    scope = ConsumerScope(owner, cancellation)
    token = CURRENT_CONSUMERS.set(scope)
    try:
        yield
    finally:
        try:
            scope.close()
        finally:
            CURRENT_CONSUMERS.reset(token)
