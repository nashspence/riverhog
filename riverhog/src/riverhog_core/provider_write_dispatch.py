"""Live provider dispatch and closure reports shared by durable effect owners.

Each effect owner commits its original target, request and reservation before
creating a live dispatch right. Retained rows do not recreate that right, and
these types do not grant catalog, claim, cache or cleanup authority.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, TypeVar

from riverhog_protocol.errors import Conflict

_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class ClosedProviderWrite:
    """Private adapter-owner evidence, never a public caller's assertion.

    The adapter must account for every delegated attempt before supplying this
    evidence. A response observation or a transport exception cannot construct it.
    """

    call_id: str
    request_sha256: str
    executor_id: str
    target_incarnation: str
    result: Mapping[str, Any] = field(repr=False)


class ArmedProviderWrite:
    """A live one-use dispatch right; retained rows cannot recreate it."""

    def __init__(
        self,
        *,
        call_id: str,
        collection_id: int,
        object_id: str,
        nonce: str,
        request_sha256: str,
        executor_id: str,
        target_incarnation: str,
    ) -> None:
        self.call_id = call_id
        self.collection_id = collection_id
        self.object_id = object_id
        self.request_sha256 = request_sha256
        self.executor_id = executor_id
        self.target_incarnation = target_incarnation
        self._nonce = nonce
        self._lock = threading.Lock()
        self._used = False
        self._running = False

    def dispatch(self, invoke: Callable[[], _T]) -> _T:
        with self._lock:
            if self._used:
                raise Conflict("provider write dispatch capability was already used")
            self._used = True
            self._running = True
        try:
            return invoke()
        finally:
            with self._lock:
                self._running = False

    def _close_dispatch(self) -> None:
        with self._lock:
            if self._running:
                raise Conflict("provider write executor is still running")
            self._used = True


@dataclass(frozen=True, slots=True)
class ProviderWriteReport:
    """One returned result; only adapter-owner evidence can close its invocation."""

    ticket: ArmedProviderWrite
    result: Mapping[str, Any] = field(repr=False)
    closure: ClosedProviderWrite | None = None
