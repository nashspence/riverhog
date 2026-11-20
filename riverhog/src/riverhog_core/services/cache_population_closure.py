"""Durably receive exact closed cache reports without recreating dispatch rights."""

from __future__ import annotations

import json
from typing import Any

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
    CollectionArchiveObjectUploadRecord,
    CollectionUploadRecord,
    RetrievalCachePopulationRecord,
)
from riverhog_core.domain.retrieval_cache import ClosedCachePopulation
from riverhog_core.pack_upload import PackUploadCheckpoint
from riverhog_core.provider_write_dispatch import ArmedProviderWrite
from riverhog_core.raw_upload import RawUploadCheckpoint
from riverhog_core.services.cache_population_begin import (
    CachePopulationBeginAdmission,
    _json,
    _sha,
    _writer,
)
from riverhog_core.services.cache_population_writes import CachePopulationWriteAdmission


class CachePopulationClosure:
    """The existing population owns sealed bytes before archive visibility.

    Source acknowledgement and receiver evidence commit together. Original
    provider controls retire only after that positive durable acceptance.
    A closed receipt cannot authorize another provider call or ready placement.
    """

    def __init__(self, factory: SessionFactory) -> None:
        self._factory = factory

    @staticmethod
    def _original(
        session: Session, call_id: str, ticket: ArmedProviderWrite | None = None
    ) -> tuple[CachePopulationBeginRecord, CachePopulationWriteEffectRecord]:
        original = session.get(CachePopulationWriteEffectRecord, call_id)
        if original is None:
            raise NotFound("cache publication original completion is unavailable")
        begin = session.get(
            CachePopulationBeginRecord,
            original.begin_call_id,
            with_for_update=True,
            populate_existing=True,
        )
        effect = (
            CachePopulationWriteAdmission._effect(session, ticket)
            if ticket is not None
            else session.get(
                CachePopulationWriteEffectRecord,
                call_id,
                with_for_update=True,
                populate_existing=True,
            )
        )
        assert effect is not None
        if _sha(effect.request_json) != effect.request_sha256:
            raise Conflict("cache closed receipt original request commitment changed")
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
    def _value(receipt: ClosedCachePopulation) -> dict[str, Any]:
        value: dict[str, Any] = json.loads(receipt.receipt_json)
        if (
            value["source_store"] != receipt.source_store
            or value["collection_id"] != receipt.collection_id
            or value["object_id"] != receipt.object_id
            or value["population_generation"] != receipt.generation
            or type(value["capacity_bytes"]) is not int
            or len(receipt.receipt_json.encode()) > value["capacity_bytes"]
        ):
            raise Conflict("cache closed receipt differs from its original acceptance")
        return value

    @staticmethod
    def _retained(
        session: Session, receipt: ClosedCachePopulation
    ) -> tuple[
        dict[str, Any],
        CollectionArchiveObjectUploadRecord | None,
        RetrievalCachePopulationRecord | None,
    ]:
        value = CachePopulationClosure._value(receipt)
        source = session.get(
            CollectionArchiveObjectUploadRecord,
            (receipt.collection_id, receipt.object_id),
            with_for_update=True,
            populate_existing=True,
        )
        population = session.get(
            RetrievalCachePopulationRecord,
            (receipt.source_store, receipt.collection_id, receipt.object_id),
            with_for_update=True,
            populate_existing=True,
        )
        source_matches = source is not None and source.cache_receipt_json == receipt.receipt_json
        receiver_matches = (
            population is not None
            and population.generation == receipt.generation
            and population.closed_receipt_json == receipt.receipt_json
        )
        if not source_matches and not receiver_matches:
            raise NotFound("cache closed receipt original acceptance is unavailable")
        if (
            population is not None
            and population.generation == receipt.generation
            and not receiver_matches
        ):
            raise Conflict("cache closed receipt receiver evidence changed")
        return value, source if source_matches else None, population if receiver_matches else None

    def load(self, source_store: str, collection_id: int, object_id: str) -> ClosedCachePopulation:
        """Read durable receiving evidence, never rebuild a live invocation."""
        with session_scope(self._factory) as session:
            population = session.get(
                RetrievalCachePopulationRecord, (source_store, collection_id, object_id)
            )
            source = session.get(CollectionArchiveObjectUploadRecord, (collection_id, object_id))
            encoded = (
                source.cache_receipt_json
                if source is not None and source.cache_receipt_json
                else population.closed_receipt_json
                if population is not None
                else None
            )
            if encoded is None:
                raise NotFound("cache closed receipt is unavailable")
            value = json.loads(encoded)
            receipt = ClosedCachePopulation(
                source_store, collection_id, object_id, value["population_generation"], encoded
            )
            self._retained(session, receipt)
            return receipt

    def accept(self, ticket: ArmedProviderWrite) -> ClosedCachePopulation:
        return self.accept_report(
            ticket.collection_id, ticket.object_id, ticket.call_id, ticket=ticket
        )

    def accept_report(
        self,
        collection_id: int,
        object_id: str,
        completion_call_id: str,
        *,
        ticket: ArmedProviderWrite | None = None,
    ) -> ClosedCachePopulation:
        """Consume only original settled closure and its confirmed archive checkpoint."""
        with session_scope(self._factory) as session:
            _writer(session)
            session.get(CollectionUploadRecord, collection_id, with_for_update=True)
            source = session.get(
                CollectionArchiveObjectUploadRecord,
                (collection_id, object_id),
                with_for_update=True,
                populate_existing=True,
            )
            if source is None:
                raise Conflict("cache closed receipt has no source checkpoint owner")
            if source.cache_receipt_json is not None:
                value = json.loads(source.cache_receipt_json)
                receipt = ClosedCachePopulation(
                    value["source_store"],
                    collection_id,
                    object_id,
                    value["population_generation"],
                    source.cache_receipt_json,
                )
                self._retained(session, receipt)
                if value["completion_call_id"] != completion_call_id or (
                    ticket is not None
                    and (
                        value["completion_dispatch_sha256"] != _sha(ticket._nonce)
                        or value["completion_request_sha256"] != ticket.request_sha256
                        or value["completion_executor_id"] != ticket.executor_id
                        or value["cache_incarnation_id"] != ticket.target_incarnation
                    )
                ):
                    raise Conflict("cache closed receipt original completion changed")
                return receipt
            original = session.get(CachePopulationWriteEffectRecord, completion_call_id)
            if original is None:
                raise NotFound("cache closed receipt original completion is unavailable")
            key = (original.source_store, collection_id, object_id)
            population = session.get(
                RetrievalCachePopulationRecord, key, with_for_update=True, populate_existing=True
            )
            begin, effect = self._original(session, completion_call_id, ticket)
            assert begin.result_json is not None and effect.result_json is not None
            start = json.loads(begin.request_json)
            completed = CompletedObjectReceipt.model_validate(
                json.loads(effect.result_json)["result"]
            )
            checkpoint_type = PackUploadCheckpoint if source.kind == "pack" else RawUploadCheckpoint
            if source.checkpoint_json is None:
                raise Conflict("cache closed receipt has no confirmed source checkpoint")
            checkpoint = checkpoint_type.from_json(source.checkpoint_json)
            operation = CachePopulationWriteAdmission._invocation(effect).operation
            assert isinstance(operation, CompleteWriteOperation)
            if (
                effect.collection_id != collection_id
                or effect.object_id != object_id
                or checkpoint.completed is None
                or checkpoint.collection_id != collection_id
                or checkpoint.volume_id != object_id
                or checkpoint.object_path != source.object_path
                or checkpoint.completed.bytes != completed.stored_bytes
                or not source.object_path.startswith(
                    start["construction"]["archive_storage_prefix"] + "/"
                )
                or population is None
                or population.state != "admitted"
                or population.generation != begin.population_generation
                or population.begin_call_id != begin.id
                or population.source_incarnation_id != start["source_incarnation_id"]
                or population.cache_store != effect.cache_store
                or population.cache_incarnation_id != effect.cache_incarnation_id
                or population.object_path != completed.object_path
                or population.expected_bytes != completed.stored_bytes
                or population.write_token != operation.request.session.write_token
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
                raise Conflict(
                    "cache closed receipt retains a different or unacknowledged population"
                )
            encoded = _json(
                {
                    "source_store": key[0],
                    "collection_id": collection_id,
                    "object_id": object_id,
                    "population_generation": begin.population_generation,
                    "source_incarnation_id": population.source_incarnation_id,
                    "cache_store": effect.cache_store,
                    "cache_incarnation_id": effect.cache_incarnation_id,
                    "construction": start["construction"],
                    "begin_call_id": begin.id,
                    "begin_request_sha256": begin.request_sha256,
                    "begin_result_sha256": _sha(begin.result_json),
                    "completion_call_id": effect.id,
                    "completion_request_sha256": effect.request_sha256,
                    "completion_result_sha256": _sha(effect.result_json),
                    "completion_dispatch_sha256": effect.dispatch_sha256,
                    "completion_executor_id": effect.executor_id,
                    "capacity_bytes": begin.result_capacity_bytes,
                    "completed": completed.model_dump(mode="json"),
                    "archive": {
                        "object_path": checkpoint.object_path,
                        "revision": checkpoint.completed.revision,
                        "stored_bytes": checkpoint.completed.bytes,
                        "completed_at": checkpoint.completed.completed_at,
                        "plan_sha256": source.plan_sha256,
                        "kind": source.kind,
                    },
                }
            )
            if len(encoded.encode()) > begin.result_capacity_bytes:
                raise Conflict("cache closed receipt exceeds its reserved byte grant")
            source.cache_receipt_json = population.closed_receipt_json = encoded
            population.state = "sealed"
            population.updated_at = format_utc_timestamp(utc_now())
            return ClosedCachePopulation(
                key[0], collection_id, object_id, begin.population_generation, encoded
            )

    def acknowledge(self, receipt: ClosedCachePopulation) -> None:
        """Retire only controls positively accepted by the durable receiving owner."""
        with session_scope(self._factory) as session:
            _writer(session)
            value, source, population = self._retained(session, receipt)
            begin = session.get(
                CachePopulationBeginRecord,
                value["begin_call_id"],
                with_for_update=True,
                populate_existing=True,
            )
            effect = session.get(
                CachePopulationWriteEffectRecord,
                value["completion_call_id"],
                with_for_update=True,
                populate_existing=True,
            )
            if begin is None and effect is None:
                return
            if population is None:
                raise Conflict("cache closed receipt retirement requires its durable receiver")
            if (
                begin is None
                or effect is None
                or begin.state != "settled"
                or begin.unresolved
                or effect.state != "settled"
                or effect.unresolved
                or begin.population_generation != receipt.generation
                or effect.population_generation != receipt.generation
                or begin.completion_call_id != effect.id
                or effect.begin_call_id != begin.id
                or begin.request_sha256 != value["begin_request_sha256"]
                or _sha(begin.request_json) != begin.request_sha256
                or _sha(begin.result_json or "") != value["begin_result_sha256"]
                or effect.request_sha256 != value["completion_request_sha256"]
                or _sha(effect.request_json) != effect.request_sha256
                or _sha(effect.result_json or "") != value["completion_result_sha256"]
                or effect.dispatch_sha256 != value["completion_dispatch_sha256"]
                or effect.executor_id != value["completion_executor_id"]
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
                raise Conflict(
                    "cache closed receipt retirement differs from original accepted reports"
                )
            session.delete(effect)
            session.delete(begin)
