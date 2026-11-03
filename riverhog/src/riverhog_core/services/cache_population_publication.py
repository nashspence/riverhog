"""Transfer a closed original cache population into accepted placement ownership."""

from __future__ import annotations

import json
from dataclasses import dataclass

from riverhog_protocol.errors import Conflict, NotFound
from riverhog_storage_adapter_protocol import (
    CompletedObjectReceipt,
)
from sqlalchemy import select
from time_formats import format_utc_timestamp, utc_now

from riverhog_core.catalog_cache_write_models import (
    CachePopulationBeginRecord,
    CachePopulationWriteEffectRecord,
)
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.catalog_models import (
    ArchiveCopyRetirementRecord,
    CollectionArchiveCopyRecord,
    CollectionArchiveObjectRecord,
    CollectionDeletionRecord,
    CollectionRecord,
    RetrievalCacheObjectRecord,
    StorageIncarnationRecord,
)
from riverhog_core.domain.retrieval_cache import RetrievalCacheReceipt
from riverhog_core.retrieval_cache_receipts import (
    parse_retrieval_cache_receipt,
    retrieval_cache_receipt_payload,
)
from riverhog_core.services.cache_population_begin import (
    _json,
    _sha,
    _writer,
)
from riverhog_core.services.cache_population_closure import (
    CachePopulationClosure,
    ClosedCachePopulation,
)
from riverhog_core.services.retrieval_cache_accounting import (
    adjust_cache_committed_bytes,
    locked_cache_accounting,
)


@dataclass(frozen=True, slots=True)
class PublishedCachePopulation:
    closed: ClosedCachePopulation
    publication_json: str
    receipt: RetrievalCacheReceipt


class CachePopulationPublication:
    """Retain a durable transfer acknowledgement before retiring original controls.

    The receiving placement owns its independent lifetime, including eviction.
    Publication replay returns the original transfer; it cannot recreate a
    placement, add committed bytes again or infer execution closure.
    """

    def __init__(self, session_factory: SessionFactory) -> None:
        self._factory = session_factory
        self.closure = CachePopulationClosure(session_factory)

    @staticmethod
    def _receipt(closed: ClosedCachePopulation, encoded: str) -> PublishedCachePopulation:
        original = CachePopulationClosure._value(closed)
        if len(encoded.encode()) > original["capacity_bytes"]:
            raise Conflict("cache publication acknowledgement exceeds its reserved byte grant")
        value = json.loads(encoded)
        completed = CompletedObjectReceipt.model_validate(original["completed"])
        receipt = parse_retrieval_cache_receipt(value["receipt"])
        if (
            value["closed_receipt_sha256"] != _sha(closed.receipt_json)
            or value["accepted_archive"] != original["archive"]
            or receipt is None
            or receipt.cache_store != original["cache_store"]
            or receipt.object_path != completed.object_path
            or receipt.revision != completed.revision
            or receipt.stored_bytes != completed.stored_bytes
            or receipt.stored_sha256 is not None
            or receipt.cached_at != completed.completed_at
        ):
            raise Conflict("cache publication original acknowledgement changed")
        return PublishedCachePopulation(closed, encoded, receipt)

    def publish(self, closed: ClosedCachePopulation) -> PublishedCachePopulation:
        with session_scope(self._factory) as session:
            _writer(session)
            key = (closed.source_store, closed.collection_id, closed.object_id)
            collection = session.get(CollectionRecord, closed.collection_id, with_for_update=True)
            copy = session.get(
                CollectionArchiveCopyRecord,
                (closed.collection_id, closed.source_store),
                with_for_update=True,
            )
            value, source, population = self.closure._retained(session, closed)
            historical = (
                source.cache_publication_json
                if source is not None and source.cache_publication_json
                else population.publication_json
                if population is not None
                else None
            )
            if historical is not None:
                return self._receipt(closed, historical)
            construction = value["construction"]
            accepted = session.get(
                CollectionArchiveObjectRecord,
                (closed.collection_id, closed.source_store, closed.object_id),
                with_for_update=True,
            )
            if (
                collection is None
                or not collection.is_published
                or collection.archive_root_sha256 is None
                or copy is None
                or copy.state != "uploaded"
                or copy.last_verified_at is None
                or accepted is None
                or accepted.verified_at is None
                or accepted.kind not in {"pack", "segment"}
                or copy.incarnation_id != value["source_incarnation_id"]
                or construction["archive_store"] != copy.store
                or construction["archive_incarnation_id"] != copy.incarnation_id
                or construction["archive_storage_prefix"] != copy.archive_storage_prefix
                or construction["creation_identity"] != collection.creation_identity_sha256
                or construction["archive_generation"] != collection.archive_generation
                or construction["delivery_context_id"] != collection.delivery_context_id
                or construction["encryption_format"] != collection.encryption_format
                or construction["passphrase_id"] != collection.passphrase_id
                or copy.archive_storage_prefix is None
                or not accepted.object_path.startswith(copy.archive_storage_prefix + "/")
                or session.get(ArchiveCopyRetirementRecord, (closed.collection_id, copy.store))
                is not None
                or session.get(CollectionDeletionRecord, closed.collection_id) is not None
            ):
                raise Conflict("cache publication has no exact accepted original archive copy")
            completed = CompletedObjectReceipt.model_validate(value["completed"])
            if (
                population is None
                or population.state != "sealed"
                or population.begin_call_id != value["begin_call_id"]
                or population.source_incarnation_id != copy.incarnation_id
                or population.cache_store != value["cache_store"]
                or population.cache_incarnation_id != value["cache_incarnation_id"]
                or population.object_path != completed.object_path
                or population.expected_bytes != accepted.stored_bytes
                or population.expected_bytes != completed.stored_bytes
                or accepted.object_path != value["archive"]["object_path"]
                or accepted.revision != value["archive"]["revision"]
                or accepted.plan_sha256 != value["archive"]["plan_sha256"]
                or accepted.kind != value["archive"]["kind"]
                or session.get(CachePopulationBeginRecord, value["begin_call_id"]) is not None
                or session.scalar(
                    select(CachePopulationWriteEffectRecord.id)
                    .where(CachePopulationWriteEffectRecord.begin_call_id == value["begin_call_id"])
                    .limit(1)
                )
                is not None
            ):
                raise Conflict("cache publication retains a different or unacknowledged population")
            # Keep the exact current bindings through the accounting wait and
            # commit, in the same order as configuration reconciliation.
            bindings = {
                (record.kind, record.name): record
                for record in session.scalars(
                    select(StorageIncarnationRecord)
                    .where(
                        (
                            (StorageIncarnationRecord.kind == "archive")
                            & (StorageIncarnationRecord.name == copy.store)
                        )
                        | (
                            (StorageIncarnationRecord.kind == "cache")
                            & (StorageIncarnationRecord.name == value["cache_store"])
                        )
                    )
                    .order_by(StorageIncarnationRecord.kind, StorageIncarnationRecord.name)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            }
            for kind, name, incarnation in (
                ("archive", copy.store, copy.incarnation_id),
                ("cache", value["cache_store"], value["cache_incarnation_id"]),
            ):
                binding = bindings.get((kind, name))
                if binding is None or binding.state != "bound" or binding.id != incarnation:
                    raise Conflict("cache publication original target incarnation changed")
            if session.get(RetrievalCacheObjectRecord, key) is not None:
                raise Conflict("cache publication placement already has another owner")
            now = format_utc_timestamp(utc_now())
            receipt = RetrievalCacheReceipt(
                cache_store=value["cache_store"],
                object_path=completed.object_path,
                revision=completed.revision,
                stored_bytes=completed.stored_bytes,
                stored_sha256=None,
                cached_at=completed.completed_at,
                verified_at=now,
            )
            encoded = _json(
                {
                    "closed_receipt_sha256": _sha(closed.receipt_json),
                    "accepted_archive": value["archive"],
                    "receipt": retrieval_cache_receipt_payload(receipt),
                }
            )
            if len(encoded.encode()) > value["capacity_bytes"]:
                raise Conflict("cache publication acknowledgement exceeds its reserved byte grant")
            accounting = locked_cache_accounting(session, value["cache_store"])
            if (
                accounting.cache_incarnation_id != value["cache_incarnation_id"]
                or accounting.reserved_bytes < population.expected_bytes
            ):
                raise Conflict("cache publication original data accounting changed")
            accounting.reserved_bytes -= population.expected_bytes
            adjust_cache_committed_bytes(accounting, delta=population.expected_bytes)
            session.add(
                RetrievalCacheObjectRecord(
                    source_store=key[0],
                    source_incarnation_id=copy.incarnation_id,
                    collection_id=key[1],
                    object_id=key[2],
                    cache_store=receipt.cache_store,
                    cache_incarnation_id=value["cache_incarnation_id"],
                    object_path=receipt.object_path,
                    revision=receipt.revision,
                    stored_bytes=receipt.stored_bytes,
                    stored_sha256=receipt.stored_sha256,
                    cached_at=receipt.cached_at,
                    verified_at=receipt.verified_at,
                    state="ready",
                )
            )
            population.state, population.updated_at = "published", now
            population.publication_json = encoded
            if source is not None:
                source.cache_publication_json = encoded
            return PublishedCachePopulation(closed, encoded, receipt)

    def acknowledge(self, publication: PublishedCachePopulation) -> None:
        with session_scope(self._factory) as session:
            _writer(session)
            value, source, population = self.closure._retained(session, publication.closed)
            encoded = (
                source.cache_publication_json
                if source is not None and source.cache_publication_json
                else population.publication_json
                if population is not None
                else None
            )
            if encoded is None:
                raise NotFound("cache publication original acknowledgement is unavailable")
            if encoded != publication.publication_json:
                raise Conflict(
                    "cache publication acknowledgement differs from its original transfer"
                )
            original = self._receipt(publication.closed, encoded)
            if publication.receipt != original.receipt:
                raise Conflict("cache publication receipt differs from its original transfer")
            if population is not None:
                if (
                    population.begin_call_id != value["begin_call_id"]
                    or population.state != "published"
                ):
                    raise Conflict("cache publication original population ownership changed")
                session.delete(population)
