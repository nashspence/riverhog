"""Admit one original cache begin before creating its live dispatch capability.

The cache data reservation and finite effect charges commit together. Reports
retain original outcomes after claim loss or parent terminal selection. These
records retain cache ownership; they do not transfer it or authorize disposal.
"""

from __future__ import annotations

import hashlib
import json
import secrets
from collections.abc import Callable
from dataclasses import dataclass

from riverhog_protocol.errors import Conflict, NotFound
from riverhog_protocol.pack_ingress import canonical_json_bytes
from riverhog_storage_adapter_protocol import (
    BeginWriteOperation,
    WriteCallIdentity,
    WriteExecutionResult,
    WriteInvocation,
    WriteSession,
    validate_write_execution,
    write_operation_sha256,
)
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from state_schema import read_snapshot
from time_formats import format_utc_timestamp, utc_now, utc_timestamp_now

from riverhog_core.app_permissions import Principal
from riverhog_core.catalog_cache_write_models import CachePopulationBeginRecord
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.catalog_models import (
    ArchiveCopyRetirementRecord,
    CollectionDeletionRecord,
    CollectionUploadRecord,
    RetrievalCacheObjectRecord,
    RetrievalCachePopulationClaimRecord,
    RetrievalCachePopulationRecord,
)
from riverhog_core.provider_write_dispatch import ArmedProviderWrite, WithdrawnProviderWrite
from riverhog_core.runtime_config import RetrievalCacheStoreRegistration
from riverhog_core.services.app_keys import require_key_authorization
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission, Guard
from riverhog_core.services.collection_upload_authority import (
    retain_upload_claim,
    retain_upload_key,
)
from riverhog_core.services.retrieval_cache_accounting import locked_cache_accounting
from riverhog_core.storage_incarnations import require_storage_incarnation
from riverhog_core.stores.storage_adapter_retrieval_cache import StorageAdapterRetrievalCache


def _json(value: object) -> str:
    return canonical_json_bytes(value).decode()


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _writer(session: Session) -> None:
    if session.get_bind().dialect.name == "sqlite":
        session.connection(execution_options={"sqlite_immediate_writer": True})


@dataclass(frozen=True, slots=True)
class CacheBeginGrants:
    calls: int
    control_bytes: int
    result_bytes: int
    cleanup_bytes: int

    def __post_init__(self) -> None:
        for value in (self.calls, self.control_bytes, self.result_bytes, self.cleanup_bytes):
            if type(value) is not int or value < 1:
                raise ValueError("cache begin grants must be positive integers")


class CachePopulationBeginAdmission:
    def __init__(self, session_factory: SessionFactory) -> None:
        self._factory = session_factory

    def arm(
        self,
        collection_id: int,
        object_id: str,
        *,
        owner: str,
        principal: Principal,
        expected_bytes: int,
        epoch: str,
        executor_id: str,
        candidate: StorageAdapterRetrievalCache,
        registration: RetrievalCacheStoreRegistration,
        grants: CacheBeginGrants,
        guard: Guard,
        validate_population: Callable[[Session, CollectionUploadRecord, str, int], None],
    ) -> ArmedProviderWrite:
        if not owner.strip() or type(expected_bytes) is not int or expected_bytes < 1:
            raise ValueError("cache begin requires owner and positive bytes")
        if len(executor_id) != 64 or any(c not in "0123456789abcdef" for c in executor_id):
            raise ValueError("cache begin executor identity is invalid")
        if candidate.name != registration.name or not registration.admission_enabled:
            raise Conflict("cache begin candidate is not enabled")
        nonce, call_id = secrets.token_hex(32), secrets.token_hex(32)
        with session_scope(self._factory) as session:
            _writer(session)
            key_fence = retain_upload_key(session, principal)
            claim_id = retain_upload_claim(session, principal.id, principal.key_id)
            guard(session)
            upload, control = ArchiveWriteAdmission.require_forward_construction(
                session, collection_id, epoch=epoch
            )
            if upload.initiated_by_principal_id != principal.id:
                raise NotFound("cache begin construction owner is unavailable")
            if not upload.use_cache:
                raise Conflict("cache begin construction cannot accept payload")
            validate_population(session, upload, object_id, expected_bytes)
            source_store = upload.archive_store
            key = (source_store, collection_id, object_id)
            if (
                session.get(ArchiveCopyRetirementRecord, (collection_id, source_store)) is not None
                or session.get(CollectionDeletionRecord, collection_id) is not None
            ):
                raise Conflict("cache begin source is retiring")
            if session.get(RetrievalCacheObjectRecord, key) is not None:
                raise Conflict("cache begin placement is already published")
            population = session.get(
                RetrievalCachePopulationRecord, key, with_for_update=True, populate_existing=True
            )
            if population is None:
                now = format_utc_timestamp(utc_now())
                try:
                    with session.begin_nested():
                        session.add(
                            RetrievalCachePopulationRecord(
                                source_store=source_store,
                                source_incarnation_id=upload.archive_incarnation_id,
                                collection_id=collection_id,
                                object_id=object_id,
                                expected_bytes=expected_bytes,
                                state="waiting",
                                initiated_at=now,
                                updated_at=now,
                            )
                        )
                        session.flush()
                except IntegrityError:
                    pass
                population = session.get(
                    RetrievalCachePopulationRecord,
                    key,
                    with_for_update=True,
                    populate_existing=True,
                )
            if population is None:
                raise Conflict("cache begin population is unavailable")
            if (
                population.expected_bytes != expected_bytes
                or population.source_incarnation_id != upload.archive_incarnation_id
                or population.state != "waiting"
                or population.cache_store is not None
                or population.begin_call_id is not None
            ):
                raise Conflict("cache begin original population changed")
            if (
                session.scalar(
                    select(CachePopulationBeginRecord.id)
                    .where(
                        CachePopulationBeginRecord.source_store == source_store,
                        CachePopulationBeginRecord.collection_id == collection_id,
                        CachePopulationBeginRecord.object_id == object_id,
                    )
                    .limit(1)
                )
                is not None
            ):
                raise Conflict("cache begin original effect is retained")
            incarnation = require_storage_incarnation(session, "cache", candidate.name)
            if (
                incarnation != candidate.incarnation_id
                or require_storage_incarnation(session, "archive", source_store)
                != population.source_incarnation_id
            ):
                raise Conflict("cache begin original target incarnation changed")
            operation = BeginWriteOperation(
                request=candidate.population_write_request(
                    source_store=source_store,
                    collection_id=collection_id,
                    object_id=object_id,
                    expected_bytes=expected_bytes,
                )
            )
            encoded = _json(
                {
                    "construction": json.loads(control.construction_json),
                    "epoch": control.epoch,
                    "owner": owner,
                    "population_generation": population.generation,
                    "source_incarnation_id": population.source_incarnation_id,
                    "cache_store": candidate.name,
                    "cache_incarnation_id": incarnation,
                    "operation": operation.model_dump(mode="json"),
                }
            )
            # Reserve both report copies and the population's original continuation.
            charged = len(encoded.encode()) + 3 * grants.result_bytes + grants.cleanup_bytes
            retained = list(
                session.execute(
                    select(CachePopulationBeginRecord.id, CachePopulationBeginRecord.control_bytes)
                    .where(CachePopulationBeginRecord.collection_id == collection_id)
                    .order_by(CachePopulationBeginRecord.id)
                    .limit(grants.calls)
                )
            )
            if len(retained) >= grants.calls or (
                sum(row.control_bytes for row in retained) + charged > grants.control_bytes
            ):
                raise Conflict("cache begin retained-effect grant is exhausted")
            accounting = locked_cache_accounting(session, candidate.name)
            if accounting.cache_incarnation_id != incarnation:
                raise Conflict("cache begin accounting incarnation changed")
            budget = registration.admission_budget_bytes
            if budget is not None and (
                accounting.reserved_bytes + accounting.committed_bytes + expected_bytes > budget
            ):
                raise Conflict("cache begin data reservation is exhausted")
            # Cache/accounting locks can wait behind unrelated collections. The
            # earlier key/claim rows are retained; only recheck their current facts.
            if key_fence is not None:
                require_key_authorization(session, key_fence, for_update=False)
            if (
                retain_upload_claim(session, principal.id, principal.key_id, for_update=False)
                != claim_id
            ):
                raise Conflict("cache begin execution authority changed")
            if upload.custody_mode == "custody-transfer" and (
                upload.lease_expires_at is None or upload.lease_expires_at <= utc_timestamp_now()
            ):
                raise Conflict("cache begin custody lease is not active")
            now = format_utc_timestamp(utc_now())
            accounting.reserved_bytes += expected_bytes
            accounting.updated_at = now
            population.begin_call_id = call_id
            population.cache_store = candidate.name
            population.cache_incarnation_id = incarnation
            population.object_path = operation.request.object_path
            population.state = "admitting"
            population.updated_at = now
            if session.get(RetrievalCachePopulationClaimRecord, (owner, *key)) is None:
                session.add(
                    RetrievalCachePopulationClaimRecord(
                        owner=owner,
                        source_store=source_store,
                        collection_id=collection_id,
                        object_id=object_id,
                        created_at=now,
                    )
                )
            session.add(
                CachePopulationBeginRecord(
                    id=call_id,
                    collection_id=collection_id,
                    source_store=source_store,
                    object_id=object_id,
                    population_generation=population.generation,
                    cache_store=candidate.name,
                    cache_incarnation_id=incarnation,
                    executor_id=executor_id,
                    dispatch_sha256=_sha(nonce),
                    request_sha256=_sha(encoded),
                    request_json=encoded,
                    state="armed",
                    unresolved=True,
                    control_bytes=charged,
                    result_capacity_bytes=grants.result_bytes,
                    cleanup_capacity_bytes=grants.cleanup_bytes,
                )
            )
        return ArmedProviderWrite(
            call_id=call_id,
            collection_id=collection_id,
            object_id=object_id,
            nonce=nonce,
            request_sha256=_sha(encoded),
            executor_id=executor_id,
            target_incarnation=incarnation,
        )

    @staticmethod
    def _effect(
        session: Session, ticket: ArmedProviderWrite | WithdrawnProviderWrite
    ) -> CachePopulationBeginRecord:
        effect = session.get(
            CachePopulationBeginRecord,
            ticket.call_id,
            with_for_update=True,
            populate_existing=True,
        )
        if effect is None:
            raise NotFound("cache begin original effect is unavailable")
        if (
            effect.collection_id != ticket.collection_id
            or effect.object_id != ticket.object_id
            or effect.dispatch_sha256 != _sha(ticket._nonce)
            or effect.request_sha256 != ticket.request_sha256
            or effect.executor_id != ticket.executor_id
            or effect.cache_incarnation_id != ticket.target_incarnation
        ):
            raise Conflict("cache begin original invocation changed")
        if _sha(effect.request_json) != effect.request_sha256:
            raise Conflict("cache begin retained request commitment changed")
        return effect

    @staticmethod
    def _invocation(effect: CachePopulationBeginRecord) -> WriteInvocation:
        operation = BeginWriteOperation.model_validate(json.loads(effect.request_json)["operation"])
        return WriteInvocation(
            call=WriteCallIdentity(
                call_id=effect.id,
                exact_request_sha256=write_operation_sha256(operation),
                target_incarnation=effect.cache_incarnation_id,
                dispatch_owner=effect.executor_id,
            ),
            operation=operation,
        )

    def execute(
        self, ticket: ArmedProviderWrite, *, candidate: StorageAdapterRetrievalCache
    ) -> WriteExecutionResult:
        with read_snapshot(self._factory) as session:
            # The original record validates the existing live capability; it does
            # not reconstruct a dispatch right after process loss.
            effect = session.get(CachePopulationBeginRecord, ticket.call_id)
            if effect is None:
                raise NotFound("cache begin original effect is unavailable")
            if (
                effect.collection_id != ticket.collection_id
                or effect.object_id != ticket.object_id
                or effect.dispatch_sha256 != _sha(ticket._nonce)
                or effect.request_sha256 != ticket.request_sha256
                or effect.executor_id != ticket.executor_id
                or effect.cache_incarnation_id != ticket.target_incarnation
                or _sha(effect.request_json) != effect.request_sha256
            ):
                raise Conflict("cache begin original invocation changed")
            if (
                candidate.name != effect.cache_store
                or candidate.incarnation_id != effect.cache_incarnation_id
            ):
                raise Conflict("cache begin selected executor target changed")
            invocation = self._invocation(effect)
        try:
            response = ticket.dispatch(lambda: candidate.execute_write(invocation))
            validate_write_execution(invocation, response)
        except BaseException:
            self.record_unknown(ticket)
            raise
        # The composing live actor retains this response across report retries.
        # Persisting it is a separate step and can never redispatch this capability.
        return response

    def settle_withdrawn(self, evidence: WithdrawnProviderWrite) -> None:
        """Record only the original capability's proof that no executor started.

        Keep the original population, reservation and retained charges. This
        report creates neither a provider outcome nor cleanup authority.
        """
        ticket = evidence.ticket
        ticket._require_withdrawal(evidence)
        encoded = _json({"dispatch": "never-started"})
        with session_scope(self._factory) as session:
            _writer(session)
            effect = self._effect(session, evidence)
            if len(encoded.encode()) > effect.result_capacity_bytes:
                raise Conflict("cache begin withdrawal exceeds its reserved byte grant")
            if effect.observation_json is not None or (
                effect.state == "settled" and effect.result_json != encoded
            ):
                raise Conflict("cache begin withdrawal conflicts with its original outcome")
            effect.state, effect.unresolved, effect.result_json = "settled", False, encoded

    def record_unknown(self, ticket: ArmedProviderWrite) -> None:
        ticket._close_dispatch()
        with session_scope(self._factory) as session:
            _writer(session)
            effect = self._effect(session, ticket)
            if effect.state != "settled":
                effect.state = "uncertain"

    def record_response(self, ticket: ArmedProviderWrite, response: WriteExecutionResult) -> None:
        ticket._close_dispatch()
        with session_scope(self._factory) as session:
            _writer(session)
            original = session.get(CachePopulationBeginRecord, ticket.call_id)
            if original is None:
                raise NotFound("cache begin original effect is unavailable")
            population = session.get(
                RetrievalCachePopulationRecord,
                (original.source_store, original.collection_id, original.object_id),
                with_for_update=True,
                populate_existing=True,
            )
            effect = self._effect(session, ticket)
            validate_write_execution(self._invocation(effect), response)
            if response.outcome == "unknown":
                if effect.state != "settled":
                    effect.state = "uncertain"
                return
            encoded = _json(
                {
                    "call": response.call.model_dump(mode="json"),
                    "outcome": response.outcome,
                    "result": response.result.model_dump(mode="json") if response.result else None,
                }
            )
            if len(encoded.encode()) > effect.result_capacity_bytes:
                raise Conflict("cache begin result exceeds its reserved byte grant")
            if effect.observation_json not in {None, encoded} or (
                effect.state == "settled" and effect.result_json != encoded
            ):
                raise Conflict("cache begin original outcome changed")
            # Report ownership comes from the original invocation, not a renewed
            # claim or live parent row. Report retry cannot recreate either.
            if response.execution == "closed":
                effect.state, effect.unresolved, effect.result_json = "settled", False, encoded
            elif effect.state != "settled":
                effect.state, effect.observation_json = "uncertain", encoded
            if (
                isinstance(response.result, WriteSession)
                and population is not None
                and population.generation == effect.population_generation
                and population.begin_call_id == effect.id
                and population.source_incarnation_id
                == json.loads(effect.request_json)["source_incarnation_id"]
                and population.expected_bytes == response.result.expected_bytes
                and population.object_path == response.result.object_path
                and population.cache_store == effect.cache_store
                and population.cache_incarnation_id == effect.cache_incarnation_id
            ):
                if population.write_token not in {None, response.result.write_token}:
                    raise Conflict("cache begin original population session changed")
                population.write_token = response.result.write_token
                if population.state == "admitting":
                    population.state = "admitted"

    def unresolved(self, collection_id: int, *, limit: int) -> tuple[str, ...]:
        if type(limit) is not int or limit < 1:
            raise ValueError("cache begin status limit must be positive")
        with read_snapshot(self._factory) as session:
            return tuple(
                session.scalars(
                    select(CachePopulationBeginRecord.id)
                    .where(
                        CachePopulationBeginRecord.collection_id == collection_id,
                        CachePopulationBeginRecord.unresolved.is_(True),
                    )
                    .order_by(CachePopulationBeginRecord.id)
                    .limit(limit)
                )
            )
