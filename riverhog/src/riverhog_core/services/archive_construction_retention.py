"""Shared process-local retention reservations for original archive actors."""

from __future__ import annotations

from dataclasses import dataclass, field
from threading import Lock
from typing import Literal

from riverhog_protocol.errors import Conflict

from riverhog_core.archive_execution_tuning import ArchiveExecutionTuning
from riverhog_core.services.physical_execution import PhysicalExecutionKey

RetentionRefusal = Literal["busy", "retention-capacity", "target-retention-capacity"]


@dataclass(frozen=True, slots=True, eq=False)
class ArchiveConstructionReservation:
    """An original local resource reservation, never semantic execution authority."""

    key: PhysicalExecutionKey
    target_incarnation: str
    owner: object = field(repr=False)


class ArchiveConstructionRetention:
    """Count all retained originals against one service-owned configured budget.

    Allocation precedes construction. Independent registries share this owner;
    separate physical workers and durable effect reservations retain their own
    accounting. Only the original reservation and local recipient can retire it.
    Its semantic owner must establish that original receiving and joins permit
    retirement; a capacity counter cannot supply that evidence.
    """

    def __init__(self, tuning: ArchiveExecutionTuning) -> None:
        self._capacity = tuning.retained_constructions
        self._per_target = tuning.retained_constructions_per_target
        self._lock = Lock()
        self._reservations: dict[int, ArchiveConstructionReservation] = {}
        self._targets: dict[str, int] = {}

    def try_retain(
        self,
        key: PhysicalExecutionKey,
        *,
        target_incarnation: str,
        owner: object,
    ) -> ArchiveConstructionReservation | RetentionRefusal:
        if not target_incarnation:
            raise ValueError("archive construction retention requires an exact target")
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            if len(self._reservations) >= self._capacity:
                return "retention-capacity"
            if self._targets.get(target_incarnation, 0) >= self._per_target:
                return "target-retention-capacity"
            original = ArchiveConstructionReservation(key, target_incarnation, owner)
            self._reservations[id(original)] = original
            self._targets[target_incarnation] = self._targets.get(target_incarnation, 0) + 1
            return original
        finally:
            self._lock.release()

    def retire(self, original: ArchiveConstructionReservation, *, owner: object) -> bool:
        """Retire one exact original, or defer without losing its charge."""
        if not self._lock.acquire(blocking=False):
            return False
        try:
            if self._reservations.get(id(original)) is not original or original.owner is not owner:
                raise Conflict("archive construction reservation is not this original owner")
            del self._reservations[id(original)]
            count = self._targets[original.target_incarnation] - 1
            if count:
                self._targets[original.target_incarnation] = count
            else:
                del self._targets[original.target_incarnation]
            return True
        finally:
            self._lock.release()
