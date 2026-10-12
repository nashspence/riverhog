"""Bounded process-local reuse of previously verified immutable provenance bytes."""

from collections import OrderedDict
from collections.abc import Hashable
from threading import Lock


class ProvenanceReadCache:
    def __init__(self, *, byte_budget: int, entry_budget: int) -> None:
        self.byte_budget = byte_budget
        self._entry_budget = entry_budget
        self._entries: OrderedDict[Hashable, bytes] = OrderedDict()
        self._bytes = 0
        self._lock = Lock()

    def get(self, key: Hashable) -> bytes | None:
        with self._lock:
            value = self._entries.get(key)
            if value is not None:
                self._entries.move_to_end(key)
            return value

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
