"""One bounded execution quantum for an independently abandoned cache owner."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from threading import Lock
from typing import Literal

from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy.orm import Session

from riverhog_core.cleanup_execution_tuning import CleanupExecutionTuning
from riverhog_core.domain.retrieval_cache import DisposedCachePopulation
from riverhog_core.ports.object_cleanup import ObjectCleanupPort
from riverhog_core.services.cache_population_cleanup import CachePopulationCleanup
from riverhog_core.services.object_cleanup_turn import ObjectCleanupTurn
from riverhog_core.services.physical_execution import PhysicalExecutionKey


class CacheCleanupNotAdmitted(Conflict):
    """The pure local binding refused before entering any cleanup actor."""


class CachePopulationCleanupTurn:
    """Keep population disposition separate from shared provider receiving."""

    def __init__(
        self,
        owner: CachePopulationCleanup,
        target: ObjectCleanupPort,
        *,
        guard: Callable[[Session], None],
        tuning: CleanupExecutionTuning,
    ) -> None:
        receipt = owner.receipt
        value = json.loads(receipt.receipt_json)
        self.key = PhysicalExecutionKey(
            receipt.collection_id, hashlib.sha256(receipt.receipt_json.encode()).hexdigest()
        )
        self.target_incarnation = value["cache_incarnation_id"]
        self._owner = owner
        self._guard = guard
        self._execution = ObjectCleanupTurn(
            owner,
            target,
            collection_id=receipt.collection_id,
            operation_id=receipt.operation_id,
            target_incarnation=self.target_incarnation,
            object_prefix=value["object_path"],
            guard=guard,
            tuning=tuning,
        )
        self._outcome: DisposedCachePopulation | None = None
        self._lock = Lock()

    @property
    def outcome(self) -> DisposedCachePopulation | None:
        """Only an originally confirmed disposition permits local retirement."""
        return self._outcome

    def run(self) -> Literal["pending", "disposed", "busy"]:
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            if self._outcome is not None:
                return "disposed"
            if not self._execution.has_pending:
                try:
                    self._outcome = self._owner.outcome()
                except NotFound:
                    pass
                else:
                    return "disposed"
            result = self._execution.advance()
            if result == "inventory-exhausted":
                self._outcome = self._owner.finish(guard=self._guard)
                return "disposed"
            return result
        finally:
            self._lock.release()
