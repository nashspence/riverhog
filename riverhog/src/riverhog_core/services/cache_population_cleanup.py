"""Receive native cleanup only inside an independently abandoned cache scope.

The cache owner validates its own current disposition prerequisites. Archive
cleanup permission and global upload quiescence are not borrowed. Provider
progress alone does not release the population's data reservation.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy import select
from sqlalchemy.orm import Session
from time_formats import utc_timestamp_now

from riverhog_core.archive_cleanup_grants import ArchiveCleanupGrants
from riverhog_core.catalog_cache_write_models import (
    CachePopulationBeginRecord,
    CachePopulationDispositionRecord,
    CachePopulationWriteEffectRecord,
)
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.catalog_models import (
    RetrievalCacheObjectRecord,
    RetrievalCachePopulationClaimRecord,
    RetrievalCachePopulationRecord,
)
from riverhog_core.catalog_object_cleanup_models import (
    ObjectCleanupAttemptRecord,
    ObjectCleanupPageRecord,
)
from riverhog_core.domain.retrieval_cache import AbandonedCachePopulation, DisposedCachePopulation
from riverhog_core.services.archive_upload_drain import lock_terminal_selection
from riverhog_core.services.cache_population_abandonment import CachePopulationAbandonment
from riverhog_core.services.object_cleanup_receiving import (
    CleanupScope,
    ObjectCleanupReceiving,
    _json,
    _sha,
)
from riverhog_core.services.retrieval_cache_accounting import locked_cache_accounting
from riverhog_core.services.terminal_cleanup_capacity import CleanupCharge, change_cleanup_capacity
from riverhog_core.storage_incarnations import require_storage_incarnation


class CachePopulationCleanup(ObjectCleanupReceiving):
    def __init__(
        self,
        factory: SessionFactory,
        receipt: AbandonedCachePopulation,
        *,
        grants: ArchiveCleanupGrants,
    ) -> None:
        super().__init__(factory, grants=grants)
        CachePopulationAbandonment._value(receipt)
        self._receipt = receipt

    @property
    def receipt(self) -> AbandonedCachePopulation:
        return self._receipt

    def _scope_id(self, operation_id: str) -> str:
        if operation_id != self._receipt.operation_id:
            raise Conflict("cache cleanup has another original terminal selection")
        return _sha(self._receipt.receipt_json)

    def _context(
        self,
        session: Session,
        collection_id: int,
        operation_id: str,
        *,
        admit: bool = False,
        guard: Callable[[Session], None] | None = None,
    ) -> CleanupScope:
        receipt = self._receipt
        if collection_id != receipt.collection_id or operation_id != receipt.operation_id:
            raise Conflict("cache cleanup differs from its original population scope")
        if admit and not callable(guard):
            raise ValueError("cache cleanup admission requires a current disposal guard")
        lock_terminal_selection(
            session, collection_id, operation_id, require_upload=False, guard=guard
        )
        value, _, population = CachePopulationAbandonment._retained(session, receipt)
        if admit:
            self._require_population(session, value, population)
        return CleanupScope(
            self._scope_id(operation_id),
            operation_id,
            value["cache_incarnation_id"],
            value["object_path"],
            value["object_path"],
        )

    def _require_population(
        self,
        session: Session,
        value: dict[str, Any],
        population: RetrievalCachePopulationRecord | None,
    ) -> None:
        if (
            population is None
            or population.source_incarnation_id != value["source_incarnation_id"]
            or population.cache_store != value["cache_store"]
            or population.cache_incarnation_id != value["cache_incarnation_id"]
            or population.object_path != value["object_path"]
            or population.write_token != value["write_token"]
            or population.expected_bytes != value["expected_bytes"]
            or population.closed_receipt_json is not None
            or population.publication_json is not None
            or require_storage_incarnation(session, "cache", value["cache_store"])
            != value["cache_incarnation_id"]
        ):
            raise Conflict("cache cleanup has no current exact abandoned owner")
        key = (self._receipt.source_store, self._receipt.collection_id, self._receipt.object_id)
        if (
            session.get(RetrievalCacheObjectRecord, key) is not None
            or session.scalar(
                select(RetrievalCachePopulationClaimRecord.owner)
                .where(
                    RetrievalCachePopulationClaimRecord.source_store == key[0],
                    RetrievalCachePopulationClaimRecord.collection_id == key[1],
                    RetrievalCachePopulationClaimRecord.object_id == key[2],
                )
                .limit(1)
            )
            is not None
        ):
            raise Conflict("cache cleanup still has current placement or claimant ownership")
        for model in (CachePopulationBeginRecord, CachePopulationWriteEffectRecord):
            if (
                session.scalar(
                    select(model.id)
                    .where(
                        model.source_store == key[0],
                        model.collection_id == key[1],
                        model.object_id == key[2],
                    )
                    .limit(1)
                )
                is not None
            ):
                raise Conflict("cache cleanup still has original forward receiving")

    def _page_selected(self, session: Session, scope: CleanupScope) -> None:
        _, _, population = CachePopulationAbandonment._retained(session, self._receipt)
        if population is None:
            raise Conflict("cache cleanup original receiving owner is unavailable")
        population.updated_at = utc_timestamp_now()

    @staticmethod
    def _disposition_bytes(record: CachePopulationDispositionRecord) -> bytes:
        return _json(
            {
                "id": record.id,
                "terminal_id": record.terminal_id,
                "source_store": record.source_store,
                "collection_id": record.collection_id,
                "object_id": record.object_id,
                "population_generation": record.population_generation,
                "receipt_json": record.receipt_json,
                "outcome_json": record.outcome_json,
            }
        ).encode()

    def _outcome(self, record: CachePopulationDispositionRecord) -> DisposedCachePopulation:
        value = CachePopulationAbandonment._value(self._receipt)
        try:
            outcome = json.loads(record.outcome_json)
            if (
                not isinstance(outcome, dict)
                or _json(outcome) != record.outcome_json
                or set(outcome)
                != {"abandonment_sha256", "last_page_sha256", "disposed_at", "data_bytes", "state"}
                or outcome["abandonment_sha256"] != self._scope_id(self._receipt.operation_id)
                or outcome["state"] != "disposed"
                or type(outcome["data_bytes"]) is not int
                or outcome["data_bytes"] != value["expected_bytes"]
                or not isinstance(outcome["disposed_at"], str)
                or not outcome["disposed_at"]
                or not isinstance(outcome["last_page_sha256"], str)
                or len(outcome["last_page_sha256"]) != 64
                or any(c not in "0123456789abcdef" for c in outcome["last_page_sha256"])
                or len(self._disposition_bytes(record)) > value["capacity_bytes"]
            ):
                raise ValueError
        except (KeyError, TypeError, ValueError) as exc:
            raise Conflict("cache disposition original outcome changed") from exc
        return DisposedCachePopulation(self._receipt, record.outcome_json)

    def outcome(self) -> DisposedCachePopulation:
        """Read only the exact confirmed outcome, independently of live admission."""
        with session_scope(self._factory) as session:
            record = CachePopulationAbandonment._disposition(session, self._receipt)
            if record is None:
                raise NotFound("cache disposition original outcome is unavailable")
            return self._outcome(record)

    def finish(self, *, guard: Callable[[Session], None]) -> DisposedCachePopulation:
        """Atomically retain positive cleanup proof and release only its population."""
        if not callable(guard):
            raise ValueError("cache disposition requires a current disposal guard")
        receipt = self._receipt
        with session_scope(self._factory) as session:
            scope = self._context(session, receipt.collection_id, receipt.operation_id, guard=guard)
            prior = CachePopulationAbandonment._disposition(session, receipt)
            if prior is not None:
                return self._outcome(prior)
            value, _, population = CachePopulationAbandonment._retained(session, receipt)
            self._require_population(session, value, population)
            assert population is not None
            record = session.get(ObjectCleanupPageRecord, scope.scope_id)
            if record is None:
                raise Conflict("cache disposition has no retained cleanup proof")
            page = self._page(record, receipt.operation_id, scope)
            if not page.complete or not page.request.inventory.exhausted:
                raise Conflict("cache disposition cleanup proof is incomplete")
            if (
                session.scalar(
                    select(ObjectCleanupAttemptRecord.id)
                    .where(ObjectCleanupAttemptRecord.scope_id == scope.scope_id)
                    .limit(1)
                )
                is not None
            ):
                raise Conflict("cache disposition still has original cleanup receiving")
            encoded = _json(
                {
                    "abandonment_sha256": scope.scope_id,
                    "last_page_sha256": page.request_sha256,
                    "disposed_at": utc_timestamp_now(),
                    "data_bytes": population.expected_bytes,
                    "state": "disposed",
                }
            )
            outcome = CachePopulationDispositionRecord(
                id=scope.scope_id,
                terminal_id=receipt.operation_id,
                source_store=receipt.source_store,
                collection_id=receipt.collection_id,
                object_id=receipt.object_id,
                population_generation=receipt.generation,
                receipt_json=receipt.receipt_json,
                outcome_json=encoded,
            )
            # The original receiver's reserved byte grant covers this bounded transfer.
            if len(self._disposition_bytes(outcome)) > value["capacity_bytes"]:
                raise Conflict("cache disposition exceeds its reserved receiving byte grant")
            accounting = locked_cache_accounting(session, value["cache_store"])
            if (
                accounting.cache_incarnation_id != value["cache_incarnation_id"]
                or accounting.reserved_bytes < population.expected_bytes
            ):
                raise Conflict("cache disposition original data accounting changed")
            accounting.reserved_bytes -= population.expected_bytes
            accounting.updated_at = utc_timestamp_now()
            change_cleanup_capacity(
                session,
                receipt.operation_id,
                grants=self._grants,
                removed=CleanupCharge(
                    control_bytes=self._page_bytes(
                        receipt.operation_id, page.request, record.inventory_request_json
                    )
                ),
            )
            session.add(outcome)
            session.delete(record)
            session.delete(population)
            return self._outcome(outcome)
