"""Run fixed semantic finalization stages inside one retained worker."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from threading import Lock
from typing import Protocol

from riverhog_protocol.errors import Conflict

from riverhog_core.catalog_db import SessionFactory
from riverhog_core.services.archive_finalizer_registry import RetainedArchiveFinalizer
from riverhog_core.services.archive_finalizer_terminal import (
    ArchiveFinalizerTerminal,
    TerminalDrainStatus,
)


class ArchiveFinalizationStage(RetainedArchiveFinalizer, Protocol):
    def advance(self) -> bool: ...


@dataclass(frozen=True, slots=True)
class ArchiveFinalizationStagePlan:
    """Select read-only source facts before declaring a child constructor started."""

    select: Callable[[], Callable[[], ArchiveFinalizationStage] | None] = field(repr=False)

    @classmethod
    def fixed(cls, create: Callable[[], ArchiveFinalizationStage]) -> ArchiveFinalizationStagePlan:
        return cls(lambda: create)


class ArchiveFinalizationPipeline:
    """Select at most one progressing step from an application-owned fixed plan.

    Selection, constructors and continuations run in the containing registry's worker.
    The original child survives every failure, including a constructor without
    a confirmed receiver. A returned boolean is one workflow turn's result,
    never a global completion, resource-release or terminal disposition proof.
    """

    def __init__(
        self,
        factory: SessionFactory,
        *,
        collection_id: int,
        stages: tuple[ArchiveFinalizationStagePlan, ...],
    ) -> None:
        if not stages:
            raise ValueError("archive finalization requires its fixed stage plan")
        self._stages = stages
        self._index = 0
        self._original: ArchiveFinalizationStage | None = None
        self._selected_create: Callable[[], ArchiveFinalizationStage] | None = None
        self._construction_failure = False
        self._result: bool | None = None
        self._lock = Lock()
        self._terminal = ArchiveFinalizerTerminal(factory, collection_id)

    @property
    def has_pending(self) -> bool:
        return self._construction_failure or (
            (self._original is not None or self._selected_create is not None)
            and self._result is None
        )

    def advance(self) -> bool:
        with self._lock:
            self._terminal.require_forward()
            if self._result is not None:
                return self._result
            if self._construction_failure:
                raise Conflict("original finalization constructor remains unresolved")
            while self._index < len(self._stages):
                if self._original is None:
                    if self._selected_create is None:
                        create = self._stages[self._index].select()
                        if create is None:
                            self._index += 1
                            continue
                        self._selected_create = create
                    self._construction_failure = True
                    self._original = self._selected_create()
                    if self._original is None:
                        raise ValueError("original finalization constructor returned no receiver")
                    self._construction_failure = False
                progressed = self._original.advance()
                if self._original.has_pending:
                    raise Conflict("original finalization step has not finished its bounded turn")
                if progressed:
                    self._result = True
                    return True
                self._original = None
                self._selected_create = None
                self._index += 1
            self._result = False
            return False

    def drain_terminal(self, operation_id: str) -> TerminalDrainStatus:
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            status = self._terminal.drain_original(
                operation_id, None if self._construction_failure else self._original
            )
            return "pending" if self._construction_failure else status
        finally:
            self._lock.release()
