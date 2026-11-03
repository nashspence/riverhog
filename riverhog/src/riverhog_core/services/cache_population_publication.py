"""Transfer a closed original cache population into accepted placement ownership."""

from __future__ import annotations

import json
from dataclasses import dataclass

from riverhog_protocol.errors import Conflict, NotFound
from riverhog_storage_adapter_protocol import (
    CompletedObjectReceipt,
    CompleteWriteOperation,
    WriteExecutionResult,
    WriteSession,
    validate_write_execution,
)
from sqlalchemy import select
from sqlalchemy.orm import Session
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
    RetrievalCachePopulationRecord,
    StorageIncarnationRecord,
)
from riverhog_core.domain.retrieval_cache import RetrievalCacheReceipt
from riverhog_core.provider_write_dispatch import ArmedProviderWrite
from riverhog_core.retrieval_cache_receipts import (
    parse_retrieval_cache_receipt,
    retrieval_cache_receipt_payload,
)
from riverhog_core.services.cache_population_begin import (
    CachePopulationBeginAdmission,
    _json,
    _sha,
    _writer,
)
from riverhog_core.services.cache_population_writes import CachePopulationWriteAdmission
from riverhog_core.services.retrieval_cache_accounting import (
    adjust_cache_committed_bytes,
    locked_cache_accounting,
)


@dataclass(frozen=True, slots=True)
class PublishedCachePopulation:
    ticket: ArmedProviderWrite
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

    @staticmethod
    def _original(
        session: Session, ticket: ArmedProviderWrite
    ) -> tuple[CachePopulationBeginRecord, CachePopulationWriteEffectRecord]:
        original = session.get(CachePopulationWriteEffectRecord, ticket.call_id)
        if original is None:
            raise NotFound("cache publication original completion is unavailable")
        begin = session.get(
            CachePopulationBeginRecord,
            original.begin_call_id,
            with_for_update=True,
            populate_existing=True,
        )
        effect = CachePopulationWriteAdmission._effect(session, ticket)
        if (
            begin is None
            or _sha(begin.request_json) != begin.request_sha256
            or effect.kind != "complete"
            or effect.state != "settled"
            or effect.unresolved
            or effect.result_json is None
            or begin.state != "settled"
            or begin.unresolved
            or begin.result_json is None
            or begin.completion_call_id != effect.id
            or begin.population_generation != effect.population_generation
            or (begin.source_store, begin.collection_id, begin.object_id)
            != (effect.source_store, effect.collection_id, effect.object_id)
            or (begin.cache_store, begin.cache_incarnation_id)
            != (effect.cache_store, effect.cache_incarnation_id)
        ):
            raise Conflict("cache publication requires its original closed completion")
        beginning = WriteExecutionResult.model_validate(
            {**json.loads(begin.result_json), "execution": "closed"}
        )
        response = WriteExecutionResult.model_validate(
            {**json.loads(effect.result_json), "execution": "closed"}
        )
        validate_write_execution(CachePopulationBeginAdmission._invocation(begin), beginning)
        validate_write_execution(CachePopulationWriteAdmission._invocation(effect), response)
        start, request = json.loads(begin.request_json), json.loads(effect.request_json)
        if (
            beginning.outcome != "session"
            or response.outcome != "completed"
            or not isinstance(beginning.result, WriteSession)
            or request["construction"] != start["construction"]
            or request["epoch"] != start["epoch"]
            or request["owner"] != start["owner"]
            or request["population_generation"] != begin.population_generation
            or request["source_incarnation_id"] != start["source_incarnation_id"]
            or request["operation"]["request"]["session"]
            != beginning.result.model_dump(mode="json")
        ):
            raise Conflict("cache publication original continuation changed")
        return begin, effect

    @staticmethod
    def _receipt(
        ticket: ArmedProviderWrite,
        begin: CachePopulationBeginRecord,
        effect: CachePopulationWriteEffectRecord,
    ) -> PublishedCachePopulation:
        assert begin.publication_json is not None and effect.result_json is not None
        if len(begin.publication_json.encode()) > begin.result_capacity_bytes:
            raise Conflict("cache publication acknowledgement exceeds its reserved byte grant")
        value = json.loads(begin.publication_json)
        start = json.loads(begin.request_json)
        completed = CompletedObjectReceipt.model_validate(json.loads(effect.result_json)["result"])
        receipt = parse_retrieval_cache_receipt(value["receipt"])
        if (
            value["begin_call_id"] != begin.id
            or value["begin_request_sha256"] != begin.request_sha256
            or value["completion_call_id"] != effect.id
            or value["completion_request_sha256"] != effect.request_sha256
            or value["completion_result_sha256"] != _sha(effect.result_json)
            or value["population_generation"] != begin.population_generation
            or value["source_incarnation_id"] != start["source_incarnation_id"]
            or value["archive_generation"] != start["construction"]["archive_generation"]
            or value["accepted_archive"]["stored_bytes"] != completed.stored_bytes
            or not value["accepted_archive"]["object_path"].startswith(
                start["construction"]["archive_storage_prefix"] + "/"
            )
            or receipt is None
            or receipt.cache_store != effect.cache_store
            or receipt.object_path != completed.object_path
            or receipt.revision != completed.revision
            or receipt.stored_bytes != completed.stored_bytes
            or receipt.stored_sha256 is not None
            or receipt.cached_at != completed.completed_at
        ):
            raise Conflict("cache publication original acknowledgement changed")
        return PublishedCachePopulation(ticket, begin.publication_json, receipt)

    def publish(self, ticket: ArmedProviderWrite) -> PublishedCachePopulation:
        with session_scope(self._factory) as session:
            _writer(session)
            original = session.get(CachePopulationWriteEffectRecord, ticket.call_id)
            if original is None:
                raise NotFound("cache publication original completion is unavailable")
            key = (original.source_store, original.collection_id, original.object_id)
            collection = session.get(CollectionRecord, original.collection_id, with_for_update=True)
            copy = session.get(
                CollectionArchiveCopyRecord,
                (original.collection_id, original.source_store),
                with_for_update=True,
            )
            population = session.get(
                RetrievalCachePopulationRecord, key, with_for_update=True, populate_existing=True
            )
            begin, effect = self._original(session, ticket)
            if begin.publication_json is not None:
                return self._receipt(ticket, begin, effect)
            start = json.loads(begin.request_json)
            construction = start["construction"]
            accepted = session.get(
                CollectionArchiveObjectRecord,
                (effect.collection_id, effect.source_store, effect.object_id),
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
                or copy.incarnation_id != start["source_incarnation_id"]
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
                or session.get(ArchiveCopyRetirementRecord, (effect.collection_id, copy.store))
                is not None
                or session.get(CollectionDeletionRecord, effect.collection_id) is not None
            ):
                raise Conflict("cache publication has no exact accepted original archive copy")
            operation = CachePopulationWriteAdmission._invocation(effect).operation
            assert isinstance(operation, CompleteWriteOperation)
            completed = CompletedObjectReceipt.model_validate(
                json.loads(effect.result_json or "{}")["result"]
            )
            if (
                population is None
                or population.state != "admitted"
                or population.generation != begin.population_generation
                or population.begin_call_id != begin.id
                or population.source_incarnation_id != copy.incarnation_id
                or population.cache_store != effect.cache_store
                or population.cache_incarnation_id != effect.cache_incarnation_id
                or population.object_path != completed.object_path
                or population.write_token != operation.request.session.write_token
                or population.expected_bytes != accepted.stored_bytes
                or population.expected_bytes != completed.stored_bytes
                or session.scalar(
                    select(CachePopulationWriteEffectRecord.id)
                    .where(
                        CachePopulationWriteEffectRecord.begin_call_id == begin.id,
                        CachePopulationWriteEffectRecord.id != effect.id,
                    )
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
                            & (StorageIncarnationRecord.name == effect.cache_store)
                        )
                    )
                    .order_by(StorageIncarnationRecord.kind, StorageIncarnationRecord.name)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            }
            for kind, name, incarnation in (
                ("archive", copy.store, copy.incarnation_id),
                ("cache", effect.cache_store, effect.cache_incarnation_id),
            ):
                binding = bindings.get((kind, name))
                if binding is None or binding.state != "bound" or binding.id != incarnation:
                    raise Conflict("cache publication original target incarnation changed")
            if session.get(RetrievalCacheObjectRecord, key) is not None:
                raise Conflict("cache publication placement already has another owner")
            now = format_utc_timestamp(utc_now())
            receipt = RetrievalCacheReceipt(
                cache_store=effect.cache_store,
                object_path=completed.object_path,
                revision=completed.revision,
                stored_bytes=completed.stored_bytes,
                stored_sha256=None,
                cached_at=completed.completed_at,
                verified_at=now,
            )
            encoded = _json(
                {
                    "begin_call_id": begin.id,
                    "begin_request_sha256": begin.request_sha256,
                    "completion_call_id": effect.id,
                    "completion_request_sha256": effect.request_sha256,
                    "completion_result_sha256": _sha(effect.result_json or ""),
                    "population_generation": begin.population_generation,
                    "source_incarnation_id": copy.incarnation_id,
                    "archive_generation": collection.archive_generation,
                    "accepted_archive": {
                        "object_path": accepted.object_path,
                        "revision": accepted.revision,
                        "stored_bytes": accepted.stored_bytes,
                        "stored_sha256": accepted.stored_sha256,
                        "plan_sha256": accepted.plan_sha256,
                    },
                    "receipt": retrieval_cache_receipt_payload(receipt),
                }
            )
            if len(encoded.encode()) > begin.result_capacity_bytes:
                raise Conflict("cache publication acknowledgement exceeds its reserved byte grant")
            accounting = locked_cache_accounting(session, effect.cache_store)
            if (
                accounting.cache_incarnation_id != effect.cache_incarnation_id
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
                    cache_incarnation_id=effect.cache_incarnation_id,
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
            begin.publication_json = encoded
            return PublishedCachePopulation(ticket, encoded, receipt)

    def acknowledge(self, publication: PublishedCachePopulation) -> None:
        with session_scope(self._factory) as session:
            _writer(session)
            original = session.get(CachePopulationWriteEffectRecord, publication.ticket.call_id)
            if original is None:
                raise NotFound("cache publication original acknowledgement is unavailable")
            key = (original.source_store, original.collection_id, original.object_id)
            population = session.get(
                RetrievalCachePopulationRecord, key, with_for_update=True, populate_existing=True
            )
            begin, effect = self._original(session, publication.ticket)
            if begin.publication_json != publication.publication_json:
                raise Conflict(
                    "cache publication acknowledgement differs from its original transfer"
                )
            original_publication = self._receipt(publication.ticket, begin, effect)
            if publication.receipt != original_publication.receipt:
                raise Conflict("cache publication receipt differs from its original transfer")
            if population is not None and population.generation == begin.population_generation:
                if population.begin_call_id != begin.id or population.state != "published":
                    raise Conflict("cache publication original population ownership changed")
                session.delete(population)
            session.delete(effect)
            session.delete(begin)
