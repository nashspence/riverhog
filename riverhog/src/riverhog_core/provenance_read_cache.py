"""Bounded process-local reuse of previously verified immutable provenance bytes."""

from collections import OrderedDict
from collections.abc import Callable, Hashable
from concurrent.futures import Future
from threading import Lock
from typing import overload


class ProvenanceReadCache:
    def __init__(self, *, byte_budget: int, entry_budget: int) -> None:
        self.byte_budget = byte_budget
        self._entry_budget = entry_budget
        self._entries: OrderedDict[Hashable, bytes] = OrderedDict()
        self._bytes = 0
        self._loads: dict[Hashable, Future[bytes | None]] = {}
        self._lock = Lock()

    def get(self, key: Hashable) -> bytes | None:
        with self._lock:
            value = self._entries.get(key)
            if value is not None:
                self._entries.move_to_end(key)
            return value

    @overload
    def get_or_load(self, key: Hashable, load: Callable[[], bytes]) -> bytes: ...

    @overload
    def get_or_load(self, key: Hashable, load: Callable[[], bytes | None]) -> bytes | None: ...

    def get_or_load(self, key: Hashable, load: Callable[[], bytes | None]) -> bytes | None:
        """Share a bounded in-flight verified read; failures never become cache entries."""
        with self._lock:
            cached = self._entries.get(key)
            if cached is not None:
                self._entries.move_to_end(key)
                return cached
            future = self._loads.get(key)
            owner = future is None and len(self._loads) < self._entry_budget
            if owner:
                future = Future()
                self._loads[key] = future
        if future is None:
            value = load()
            if value is not None:
                self.put(key, value)
            return value
        if not owner:
            return future.result()
        try:
            value = load()
            if value is not None:
                self.put(key, value)
            future.set_result(value)
            return value
        except BaseException as exc:
            future.set_exception(exc)
            raise
        finally:
            with self._lock:
                del self._loads[key]

    def put(self, key: Hashable, value: bytes) -> None:
        if len(value) > self.byte_budget:
            return
        with self._lock:
            if key in self._entries:
                self._entries.move_to_end(key)
                return
            while self._entries and (
                len(self._entries) >= self._entry_budget
                or self._bytes + len(value) > self.byte_budget
            ):
                _, removed = self._entries.popitem(last=False)
                self._bytes -= len(removed)
            self._entries[key] = value
            self._bytes += len(value)
