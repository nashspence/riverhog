"""Durable handoff of incomplete native populations to the existing cache owner.

Acceptance transfers cleanup responsibility without disposing data, releasing
data reservations, publishing placement or claiming global writer quiescence.
Only original positively closed begin reports and fully received children can
enter this owner. The retained receiver fences unmanaged namespace reuse.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from riverhog_protocol.errors import Conflict, NotFound
from riverhog_storage_adapter_protocol import (
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
    RetrievalCachePopulationClaimRecord,
    RetrievalCachePopulationRecord,
)
from riverhog_core.domain.retrieval_cache import AbandonedCachePopulation
from riverhog_core.services.archive_upload_drain import lock_terminal_selection
from riverhog_core.services.cache_population_begin import (
    CachePopulationBeginAdmission,
    _json,
    _sha,
    _writer,
)


class CachePopulationAbandonment:
    def __init__(self, factory: SessionFactory) -> None:
        self._factory = factory

    @staticmethod
    def _value(receipt: AbandonedCachePopulation) -> dict[str, Any]:
        try:
            value: dict[str, Any] = json.loads(receipt.receipt_json)
            if (
                _json(value) != receipt.receipt_json
                or value["source_store"] != receipt.source_store
                or value["collection_id"] != receipt.collection_id
                or value["object_id"] != receipt.object_id
                or value["population_generation"] != receipt.generation
                or value["selected_terminal_sha256"] != receipt.operation_id
                or type(value["capacity_bytes"]) is not int
                or len(receipt.receipt_json.encode()) > value["capacity_bytes"]
            ):
                raise ValueError
        except (KeyError, TypeError, ValueError) as exc:
            raise Conflict("cache abandonment receipt differs from original acceptance") from exc
        return value

    @staticmethod
    def _retained(
        session: Session, receipt: AbandonedCachePopulation
    ) -> tuple[
        dict[str, Any],
        CollectionArchiveObjectUploadRecord | None,
        RetrievalCachePopulationRecord | None,
    ]:
        value = CachePopulationAbandonment._value(receipt)
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
        source_matches = (
            source is not None and source.cache_abandonment_json == receipt.receipt_json
        )
        receiver_matches = (
            population is not None
            and population.generation == receipt.generation
            and population.abandonment_receipt_json == receipt.receipt_json
            and population.state == "abandoning"
            and population.begin_call_id == value["begin_call_id"]
        )
        if not source_matches and not receiver_matches:
            raise NotFound("cache abandonment original acceptance is unavailable")
        if (
            population is not None
            and population.generation == receipt.generation
            and not receiver_matches
        ):
            raise Conflict("cache abandonment receiver evidence changed")
        return value, source if source_matches else None, population if receiver_matches else None

    @staticmethod
    def _original_value(
        session: Session,
        begin: CachePopulationBeginRecord,
        population: RetrievalCachePopulationRecord,
        operation_id: str,
    ) -> dict[str, Any]:
        if (
            _sha(begin.request_json) != begin.request_sha256
            or begin.state != "settled"
            or begin.unresolved
            or begin.result_json is None
            or begin.completion_call_id is not None
        ):
            raise Conflict("cache abandonment requires its original closed begin")
        start = json.loads(begin.request_json)
        original = json.loads(begin.result_json)
        if original == {"dispatch": "never-started"}:
            session_value = None
        else:
            response = WriteExecutionResult.model_validate({**original, "execution": "closed"})
            validate_write_execution(CachePopulationBeginAdmission._invocation(begin), response)
            if not isinstance(response.result, WriteSession) or response.outcome != "session":
                raise Conflict("cache abandonment has no original population session")
            session_value = response.result
        key = (begin.source_store, begin.collection_id, begin.object_id)
        if (
            (population.source_store, population.collection_id, population.object_id) != key
            or start["population_generation"] != begin.population_generation
            or start["cache_store"] != begin.cache_store
            or start["cache_incarnation_id"] != begin.cache_incarnation_id
            or population.generation != begin.population_generation
            or population.begin_call_id != begin.id
            or population.source_incarnation_id != start["source_incarnation_id"]
            or population.cache_store != begin.cache_store
            or population.cache_incarnation_id != begin.cache_incarnation_id
            or population.expected_bytes != int(start["operation"]["request"]["expected_bytes"])
            or population.object_path != start["operation"]["request"]["object_path"]
            or population.closed_receipt_json is not None
            or population.publication_json is not None
            or population.state not in {"admitting", "admitted", "writing", "abandoning"}
            or (session_value is None and population.write_token is not None)
            or (
                session_value is not None
                and (
                    population.write_token != session_value.write_token
                    or population.object_path != session_value.object_path
                    or population.expected_bytes != session_value.expected_bytes
                )
            )
            or session.scalar(
                select(CachePopulationWriteEffectRecord.id)
                .where(CachePopulationWriteEffectRecord.begin_call_id == begin.id)
                .limit(1)
            )
            is not None
        ):
            raise Conflict("cache abandonment retains a different or unreceived population")
        if (
            session.get(RetrievalCachePopulationClaimRecord, (start["owner"], *key)) is None
            or session.scalar(
                select(RetrievalCachePopulationClaimRecord.owner)
                .where(
                    RetrievalCachePopulationClaimRecord.source_store == key[0],
                    RetrievalCachePopulationClaimRecord.collection_id == key[1],
                    RetrievalCachePopulationClaimRecord.object_id == key[2],
                    RetrievalCachePopulationClaimRecord.owner != start["owner"],
                )
                .limit(1)
            )
            is not None
        ):
            raise Conflict("cache abandonment cannot take another population owner's claim")
        return {
            "source_store": key[0],
            "collection_id": key[1],
            "object_id": key[2],
            "population_generation": begin.population_generation,
            "selected_terminal_sha256": operation_id,
            "construction": start["construction"],
            "original_epoch": start["epoch"],
            "owner": start["owner"],
            "source_incarnation_id": population.source_incarnation_id,
            "cache_store": begin.cache_store,
            "cache_incarnation_id": begin.cache_incarnation_id,
            "begin_call_id": begin.id,
            "begin_request_sha256": begin.request_sha256,
            "begin_result_sha256": _sha(begin.result_json),
            "begin_dispatch_sha256": begin.dispatch_sha256,
            "begin_executor_id": begin.executor_id,
            "object_path": population.object_path,
            "expected_bytes": population.expected_bytes,
            "write_token": population.write_token,
            "capacity_bytes": begin.cleanup_capacity_bytes,
        }

    def load(
        self, source_store: str, collection_id: int, object_id: str, *, operation_id: str
    ) -> AbandonedCachePopulation:
        with session_scope(self._factory) as session:
            source = session.get(CollectionArchiveObjectUploadRecord, (collection_id, object_id))
            population = session.get(
                RetrievalCachePopulationRecord, (source_store, collection_id, object_id)
            )
            encoded = (
                source.cache_abandonment_json
                if source is not None and source.cache_abandonment_json
                else population.abandonment_receipt_json
                if population is not None
                else None
            )
            if encoded is None:
                raise NotFound("cache abandonment original acceptance is unavailable")
            value = json.loads(encoded)
            receipt = AbandonedCachePopulation(
                source_store,
                collection_id,
                object_id,
                value["population_generation"],
                operation_id,
                encoded,
            )
            self._retained(session, receipt)
            return receipt

    def accept_report(
        self,
        collection_id: int,
        object_id: str,
        begin_call_id: str,
        *,
        operation_id: str,
        guard: Callable[[Session], None],
    ) -> AbandonedCachePopulation:
        if not callable(guard):
            raise ValueError("cache abandonment requires a current source guard")
        with session_scope(self._factory) as session:
            control, terminal = lock_terminal_selection(
                session, collection_id, operation_id, guard=guard, require_upload=False
            )
            source = session.get(
                CollectionArchiveObjectUploadRecord,
                (collection_id, object_id),
                with_for_update=True,
                populate_existing=True,
            )
            if source is not None and source.cache_abandonment_json is not None:
                value = json.loads(source.cache_abandonment_json)
                receipt = AbandonedCachePopulation(
                    value["source_store"],
                    collection_id,
                    object_id,
                    value["population_generation"],
                    operation_id,
                    source.cache_abandonment_json,
                )
                value, _, _ = self._retained(session, receipt)
                if value["begin_call_id"] != begin_call_id:
                    raise Conflict("cache abandonment original begin changed")
                return receipt
            if source is None:
                raise Conflict("cache abandonment has no original source owner")
            begin = session.get(
                CachePopulationBeginRecord,
                begin_call_id,
                with_for_update=True,
                populate_existing=True,
            )
            if begin is None or (begin.collection_id, begin.object_id) != (
                collection_id,
                object_id,
            ):
                raise NotFound("cache abandonment original begin is unavailable")
            population = session.get(
                RetrievalCachePopulationRecord,
                (begin.source_store, collection_id, object_id),
                with_for_update=True,
                populate_existing=True,
            )
            if (
                source is None
                or population is None
                or session.get(CollectionUploadRecord, collection_id) is None
            ):
                raise Conflict("cache abandonment has no original source owner")
            value = self._original_value(session, begin, population, operation_id)
            if (
                value["construction"] != json.loads(control.construction_json)
                or value["original_epoch"] != terminal.original_epoch
                or source.cache_receipt_json is not None
            ):
                raise Conflict("cache abandonment differs from selected terminal construction")
            encoded = _json(value)
            if len(encoded.encode()) > begin.cleanup_capacity_bytes:
                raise Conflict("cache abandonment exceeds its reserved byte grant")
            if population.abandonment_receipt_json not in {None, encoded}:
                raise Conflict("cache abandonment receiver has another original selection")
            source.cache_abandonment_json = population.abandonment_receipt_json = encoded
            population.state = "abandoning"
            population.updated_at = format_utc_timestamp(utc_now())
            return AbandonedCachePopulation(
                begin.source_store,
                collection_id,
                object_id,
                begin.population_generation,
                operation_id,
                encoded,
            )

    def acknowledge(self, receipt: AbandonedCachePopulation) -> None:
        """Release original forward controls only after exact durable acceptance."""
        with session_scope(self._factory) as session:
            _writer(session)
            value, _, population = self._retained(session, receipt)
            begin = session.get(
                CachePopulationBeginRecord,
                value["begin_call_id"],
                with_for_update=True,
                populate_existing=True,
            )
            if begin is None:
                return
            if (
                population is None
                or self._original_value(session, begin, population, receipt.operation_id) != value
            ):
                raise Conflict("cache abandonment retirement differs from accepted original")
            claim = session.get(
                RetrievalCachePopulationClaimRecord,
                (value["owner"], receipt.source_store, receipt.collection_id, receipt.object_id),
            )
            assert claim is not None
            session.delete(claim)
            session.delete(begin)
