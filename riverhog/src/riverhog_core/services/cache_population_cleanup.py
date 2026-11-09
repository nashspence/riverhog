"""Receive native cleanup only inside an independently abandoned cache scope.

The cache owner validates its own current disposition prerequisites. Archive
cleanup permission and global upload quiescence are not borrowed. Provider
progress alone does not release the population's data reservation.
"""

from __future__ import annotations

from collections.abc import Callable

from riverhog_protocol.errors import Conflict
from sqlalchemy import select
from sqlalchemy.orm import Session
from time_formats import utc_timestamp_now

from riverhog_core.archive_cleanup_grants import ArchiveCleanupGrants
from riverhog_core.catalog_cache_write_models import (
    CachePopulationBeginRecord,
    CachePopulationWriteEffectRecord,
)
from riverhog_core.catalog_db import SessionFactory
from riverhog_core.catalog_models import (
    RetrievalCacheObjectRecord,
    RetrievalCachePopulationClaimRecord,
)
from riverhog_core.domain.retrieval_cache import AbandonedCachePopulation
from riverhog_core.services.archive_upload_drain import lock_terminal_selection
from riverhog_core.services.cache_population_abandonment import CachePopulationAbandonment
from riverhog_core.services.object_cleanup_receiving import (
    CleanupScope,
    ObjectCleanupReceiving,
    _sha,
)
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
            key = (receipt.source_store, collection_id, receipt.object_id)
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
        return CleanupScope(
            self._scope_id(operation_id),
            operation_id,
            value["cache_incarnation_id"],
            value["object_path"],
            value["object_path"],
        )

    def _page_selected(self, session: Session, scope: CleanupScope) -> None:
        _, _, population = CachePopulationAbandonment._retained(session, self._receipt)
        if population is None:
            raise Conflict("cache cleanup original receiving owner is unavailable")
        population.updated_at = utc_timestamp_now()
