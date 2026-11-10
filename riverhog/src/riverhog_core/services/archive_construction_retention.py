"""Shared process-local retention reservations for original archive actors."""

from __future__ import annotations

from dataclasses import dataclass, field
from threading import Lock
from typing import Literal

from riverhog_protocol.errors import Conflict

from riverhog_core.archive_execution_tuning import ArchiveExecutionTuning
from riverhog_core.services.physical_execution import (
    PhysicalExecutionKey,
    PhysicalWorkspaceExecutionKey,
)
from riverhog_core.services.workspace_retention_growth import declare_workspace_retention_growth

RetentionRefusal = Literal["busy", "retention-capacity", "target-retention-capacity"]


@dataclass(frozen=True, slots=True, eq=False)
class ArchiveConstructionReservation:
    """An original local resource reservation, never semantic execution authority."""

    key: PhysicalExecutionKey
    target_incarnation: str
    owner: object = field(repr=False)
    terminal: bool = False


@dataclass(frozen=True, slots=True, eq=False)
class WorkspaceConstructionReservation:
    """Original local workspace charge in the ordinary shared retention lane."""

    key: PhysicalWorkspaceExecutionKey
    target_incarnation: str
    owner: object = field(repr=False)
    terminal: bool = field(default=False, init=False)


type _Reservation = ArchiveConstructionReservation | WorkspaceConstructionReservation


class ArchiveConstructionRetention:
    """Count retained originals against the service-owned configured allowances.

    Allocation precedes construction. Independent registries share this owner.
    The reserved terminal allowance belongs only to global terminal adjudication;
    construction drains and independent cache cleanup use the ordinary budget.
    Neither allowance lends unused reservations to the other. The
    separate physical workers and durable effect reservations retain their own
    accounting. Only the original reservation and local recipient can retire it.
    Its semantic owner must establish that original receiving and joins permit
    retirement; a capacity counter cannot supply that evidence.
    """

    def __init__(self, tuning: ArchiveExecutionTuning) -> None:
        self._capacity = {False: tuning.retained_constructions, True: tuning.retained_terminals}
        self._per_target = {
            False: tuning.retained_constructions_per_target,
            True: tuning.retained_terminals_per_target,
        }
        self._lock = Lock()
        self._reservations: dict[int, _Reservation] = {}
        self._targets: dict[tuple[bool, str], int] = {}
        self._counts = {False: 0, True: 0}

    def try_retain(
        self,
        key: PhysicalExecutionKey,
        *,
        target_incarnation: str,
        owner: object,
        terminal: bool = False,
    ) -> ArchiveConstructionReservation | RetentionRefusal:
        if not isinstance(key, PhysicalExecutionKey):
            raise ValueError("archive retention requires its exact collection key")
        result = self._try_retain(
            key,
            target_incarnation=target_incarnation,
            owner=owner,
            terminal=terminal,
        )
        if isinstance(result, WorkspaceConstructionReservation):
            raise Conflict("archive retention returned another reservation kind")
        return result

    def try_retain_workspace(
        self,
        key: PhysicalWorkspaceExecutionKey,
        *,
        target_incarnation: str,
        owner: object,
    ) -> WorkspaceConstructionReservation | RetentionRefusal:
        """Charge an independent original before construction, using ordinary capacity."""
        if not isinstance(key, PhysicalWorkspaceExecutionKey) or owner is None:
            raise ValueError("workspace retention requires its exact key and receiver")
        result = self._try_retain(
            key,
            target_incarnation=target_incarnation,
            owner=owner,
            terminal=False,
        )
        if isinstance(result, ArchiveConstructionReservation):
            raise Conflict("workspace retention returned another reservation kind")
        return result

    def _try_retain(
        self,
        key: PhysicalExecutionKey | PhysicalWorkspaceExecutionKey,
        *,
        target_incarnation: str,
        owner: object,
        terminal: bool,
    ) -> _Reservation | RetentionRefusal:
        if type(terminal) is not bool:
            raise ValueError("archive retention lane must be explicit")
        if not target_incarnation:
            raise ValueError("archive construction retention requires an exact target")
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            if self._counts[terminal] >= self._capacity[terminal]:
                return "retention-capacity"
            target = (terminal, target_incarnation)
            if self._targets.get(target, 0) >= self._per_target[terminal]:
                return "target-retention-capacity"
            original: _Reservation
            if isinstance(key, PhysicalWorkspaceExecutionKey):
                if terminal:
                    raise ValueError("independent workspace retention uses ordinary capacity")
                original = WorkspaceConstructionReservation(key, target_incarnation, owner)
            else:
                original = ArchiveConstructionReservation(key, target_incarnation, owner, terminal)
            self._reservations[id(original)] = original
            self._targets[target] = self._targets.get(target, 0) + 1
            self._counts[terminal] += 1
            return original
        finally:
            self._lock.release()

    def retire(self, original: _Reservation, *, owner: object) -> bool:
        """Retire one exact original, or defer without losing its charge."""
        if not self._lock.acquire(blocking=False):
            return False
        try:
            if self._reservations.get(id(original)) is not original or original.owner is not owner:
                raise Conflict("archive construction reservation is not this original owner")
            del self._reservations[id(original)]
            target = (original.terminal, original.target_incarnation)
            self._counts[original.terminal] -= 1
            count = self._targets[target] - 1
            if count:
                self._targets[target] = count
            else:
                del self._targets[target]
            return True
        finally:
            self._lock.release()


declare_workspace_retention_growth(ArchiveConstructionRetention.try_retain_workspace, retire=False)
declare_workspace_retention_growth(ArchiveConstructionRetention.retire, retire=True)
