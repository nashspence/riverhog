"""Retained resource receiving, independent of archive and application authority.

A caller reserves capacity and retains a Group before acquisition. Borrowed
integers are non-owning: never close, dup, detach, transfer, or retain them past
the original borrow. Worker completion is established outside this module.

File acquisition may explicitly create with private 0600 mode, optionally
exclusive. Existing files are never truncated, repaired or replaced. Directory
acquisition opens existing directories only. Creation uses the same single
native outcome boundary and original parent dependency as existing acquisition.
"""

import sys
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING or sys.platform in {"linux", "darwin"}:
    from ._native import Borrow, Resource
else:

    class Resource:
        def __new__(cls, *_args: Any, **_kwargs: Any) -> Any:
            raise RuntimeError("retained POSIX owners require native Linux or Darwin")

    class Borrow:
        def __new__(cls, *_args: Any, **_kwargs: Any) -> Any:
            raise RuntimeError("retained POSIX borrowing requires native Linux or Darwin")


class Group:
    """A bounded reverse-dependency receiving list, never a destructor stack.

    Each record is inserted before acquisition/pinning. Failed/lost close replies
    keep that exact record; only its positive receiving permits removal.
    """

    def __init__(self, capacity: int) -> None:
        if type(capacity) is not int or capacity < 1:
            raise ValueError("explicit positive handle-record admission is required")
        self.capacity = capacity
        self._entries: list[Resource | Borrow] = []
        self._closing = False

    def _retain(self, resource: Resource | Borrow) -> Resource | Borrow:
        if self._closing:
            raise RuntimeError("receiving has begun; no new resource admission")
        if len(self._entries) >= self.capacity:
            raise RuntimeError("handle-record grant exhausted before acquisition")
        self._entries.append(resource)
        return resource

    def _admit(self) -> None:
        if self._closing or len(self._entries) >= self.capacity:
            raise RuntimeError("handle-record admission unavailable before allocation")

    def resource(self, kind: Literal["file", "directory"]) -> Resource:
        self._admit()
        resource = Resource(kind)
        self._retain(resource)
        return resource

    def borrow(self, resource: Resource) -> Borrow:
        self._admit()
        borrow = Borrow(resource)
        self._retain(borrow)
        return borrow

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
                raise BlockingIOError(
                    "the original borrower must receive after its actual I/O exits"
                )
            original.close()
            if not original.received:
                raise RuntimeError("original owner did not positively receive")
            self._entries.pop()
        return self.received


__all__ = ["Resource", "Borrow", "Group"]
