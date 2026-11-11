"""Retained resource receiving, independent of archive and application authority.

A caller reserves capacity and retains a Group before acquisition. Borrowed
integers are non-owning: never close, dup, detach, transfer, or retain them past
the original borrow. Worker completion is established outside this module.
"""
from ._native import Borrow, Resource


class Group:
    """A bounded reverse-dependency receiving list, never a destructor stack.

    Each record is inserted before acquisition/pinning. Failed/lost close replies
    keep that exact record; only its positive receiving permits removal.
    """
    def __init__(self, capacity: int):
        if type(capacity) is not int or capacity < 1:
            raise ValueError("explicit positive handle-record admission is required")
        self.capacity = capacity
        self._entries: list[Resource | Borrow] = []
        self._closing = False

    def _retain(self, resource):
        if self._closing:
            raise RuntimeError("receiving has begun; no new resource admission")
        if len(self._entries) >= self.capacity:
            raise RuntimeError("handle-record grant exhausted before acquisition")
        self._entries.append(resource)
        return resource

    def _admit(self):
        if self._closing or len(self._entries) >= self.capacity:
            raise RuntimeError("handle-record admission unavailable before allocation")

    def resource(self, kind: str) -> Resource:
        self._admit()
        return self._retain(Resource(kind))

    def borrow(self, resource: Resource) -> Borrow:
        self._admit()
        return self._retain(Borrow(resource))

    @property
    def retained(self) -> int:
        return len(self._entries)

    @property
    def received(self) -> bool:
        return self._closing and not self._entries

    def close_step(self, record_budget: int) -> bool:
        if type(record_budget) is not int or record_budget < 1:
            raise ValueError("receiving requires a positive record budget")
        self._closing = True
        for _ in range(min(record_budget, len(self._entries))):
            original = self._entries[-1]
            if isinstance(original, Borrow) and original.active:
                raise BlockingIOError("the original borrower must receive after its actual I/O exits")
            original.close()
            if not original.received:
                raise RuntimeError("original owner did not positively receive")
            self._entries.pop()
        return self.received


__all__ = ["Resource", "Borrow", "Group"]
