"""Original-session cache part and completion effects.

Forward work retains current construction authority. Original outcome reports
survive that authority and the population row; they never publish cache placement
or transfer ownership. Provider dispatch stays outside catalog transactions.
"""

from __future__ import annotations

import hashlib
import json
import secrets
from collections.abc import Callable

from riverhog_protocol.errors import Conflict, NotFound
from riverhog_storage_adapter_protocol import (
    BeginWriteOperation,
    CompleteWriteOperation,
    WriteCallIdentity,
    WriteExecutionResult,
    WriteInvocation,
    WritePartOperation,
    WriteSession,
    validate_write_execution,
    write_operation_sha256,
)
from sqlalchemy import select
from sqlalchemy.orm import Session
from state_schema import read_snapshot
from time_formats import utc_timestamp_now

from riverhog_core.app_permissions import Principal
from riverhog_core.cache_write_grants import CacheWriteGrants
from riverhog_core.catalog_cache_write_models import (
    CachePopulationBeginRecord,
    CachePopulationWriteEffectRecord,
)
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.catalog_models import (
    ArchiveCopyRetirementRecord,
    CollectionDeletionRecord,
    CollectionUploadRecord,
    RetrievalCachePopulationClaimRecord,
    RetrievalCachePopulationRecord,
)
from riverhog_core.provider_write_dispatch import ArmedProviderWrite, WithdrawnProviderWrite
from riverhog_core.services.app_keys import require_key_authorization
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission, Guard
from riverhog_core.services.cache_population_begin import _json, _sha, _writer
from riverhog_core.services.cache_write_capacity import require_cache_write_capacity
from riverhog_core.services.collection_upload_authority import (
    retain_upload_claim,
    retain_upload_key,
)
from riverhog_core.storage_incarnations import require_storage_incarnation
from riverhog_core.stores.storage_adapter_retrieval_cache import StorageAdapterRetrievalCache


class CachePopulationWriteAdmission:
    def __init__(self, session_factory: SessionFactory) -> None:
        self._factory = session_factory

    def arm(
        self,
        collection_id: int,
        object_id: str,
        *,
        begin_call_id: str,
        operation: WritePartOperation | CompleteWriteOperation,
        owner: str,
        principal: Principal,
        epoch: str,
        executor_id: str,
        candidate: StorageAdapterRetrievalCache,
        grants: CacheWriteGrants,
        guard: Guard,
        validate_population: Callable[[Session, CollectionUploadRecord, str, int], None],
    ) -> ArmedProviderWrite:
        if not isinstance(operation, (WritePartOperation, CompleteWriteOperation)):
            raise ValueError("cache session write must be a part or completion")
        if (
            not owner.strip()
            or len(executor_id) != 64
            or any(c not in "0123456789abcdef" for c in executor_id)
        ):
            raise ValueError("cache session write owner or executor is invalid")
        call_id, nonce = secrets.token_hex(32), secrets.token_hex(32)
        with session_scope(self._factory) as session:
            _writer(session)
            key_fence = retain_upload_key(session, principal)
            claim_id = retain_upload_claim(session, principal.id, principal.key_id)
            guard(session)
            upload, control = ArchiveWriteAdmission.require_forward_construction(
                session, collection_id, epoch=epoch
            )
            if upload.initiated_by_principal_id != principal.id or not upload.use_cache:
                raise NotFound("cache session write construction owner is unavailable")
            validate_population(
                session, upload, object_id, operation.request.session.expected_bytes
            )
            source_store = upload.archive_store
            key = (source_store, collection_id, object_id)
            if (
                session.get(ArchiveCopyRetirementRecord, (collection_id, source_store)) is not None
                or session.get(CollectionDeletionRecord, collection_id) is not None
            ):
                raise Conflict("cache session write source is retiring")
            population = session.get(
                RetrievalCachePopulationRecord, key, with_for_update=True, populate_existing=True
            )
            begin = session.get(
                CachePopulationBeginRecord,
                begin_call_id,
                with_for_update=True,
                populate_existing=True,
            )
            if population is None or begin is None:
                raise NotFound("cache session write original population is unavailable")
            if _sha(begin.request_json) != begin.request_sha256:
                raise Conflict("cache session write original begin commitment changed")
            original = json.loads(begin.request_json)
            if (
                begin.collection_id != collection_id
                or begin.source_store != source_store
                or begin.object_id != object_id
                or population.generation != begin.population_generation
                or population.begin_call_id != begin.id
                or population.source_incarnation_id != original["source_incarnation_id"]
                or population.cache_store != begin.cache_store
                or population.cache_incarnation_id != begin.cache_incarnation_id
                or population.state != "admitted"
                or begin.cache_store != candidate.name
                or begin.cache_incarnation_id != candidate.incarnation_id
                or original["owner"] != owner
                or original["epoch"] != epoch
                or original["construction"] != json.loads(control.construction_json)
                or session.get(RetrievalCachePopulationClaimRecord, (owner, *key)) is None
            ):
                raise Conflict("cache session write original ownership changed")
            if (
                begin.state != "settled"
                or begin.unresolved
                or begin.result_json is None
                or begin.completion_call_id is not None
            ):
                raise Conflict("cache session write original begin is not writable")
            report = json.loads(begin.result_json)
            if report.get("outcome") != "session":
                raise Conflict("cache session write has no original continuation")
            continuation = WriteSession.model_validate(report["result"])
            start = BeginWriteOperation.model_validate(original["operation"]).request
            if (
                operation.request.session != continuation
                or population.write_token != continuation.write_token
                or population.object_path != continuation.object_path
                or population.expected_bytes != continuation.expected_bytes
                or start.object_path != continuation.object_path
                or start.expected_bytes != continuation.expected_bytes
            ):
                raise Conflict("cache session write original continuation changed")
            if isinstance(operation, CompleteWriteOperation):
                request = operation.request
                if (
                    request.expected_bytes != start.expected_bytes
                    or request.expected_content_type != start.content_type
                    or request.required_identity_assertions != start.required_identity_assertions
                    or request.expected_placement_policy != start.placement_policy
                ):
                    raise Conflict("cache completion original identity changed")
                resource = "complete"
            else:
                if operation.request.stored_bytes > start.expected_bytes:
                    raise Conflict("cache part exceeds its original population bytes")
                resource = f"part:{operation.request.number}"
            overlap = select(CachePopulationWriteEffectRecord.id).where(
                CachePopulationWriteEffectRecord.begin_call_id == begin.id,
                CachePopulationWriteEffectRecord.unresolved.is_(True),
            )
            if isinstance(operation, WritePartOperation):
                overlap = overlap.where(
                    (CachePopulationWriteEffectRecord.resource == resource)
                    | (CachePopulationWriteEffectRecord.kind == "complete")
                )
            if session.scalar(overlap.limit(1)) is not None:
                raise Conflict("cache session write original lane is unresolved")
            if (
                require_storage_incarnation(session, "cache", candidate.name)
                != begin.cache_incarnation_id
                or require_storage_incarnation(session, "archive", source_store)
                != population.source_incarnation_id
            ):
                raise Conflict("cache session write original target incarnation changed")
            encoded = _json(
                {
                    "construction": original["construction"],
                    "epoch": original["epoch"],
                    "owner": owner,
                    "begin_call_id": begin.id,
                    "population_generation": begin.population_generation,
                    "source_incarnation_id": original["source_incarnation_id"],
                    "cache_store": begin.cache_store,
                    "cache_incarnation_id": begin.cache_incarnation_id,
                    "operation": operation.model_dump(mode="json"),
                }
            )
            charged = len(encoded.encode()) + 2 * grants.result_bytes + grants.cleanup_bytes
            require_cache_write_capacity(session, collection_id, charged=charged, grants=grants)
            # Population/begin locks may have waited. Recheck retained authority
            # without acquiring earlier locks again or renewing its lease.
            if key_fence is not None:
                require_key_authorization(session, key_fence, for_update=False)
            if (
                retain_upload_claim(session, principal.id, principal.key_id, for_update=False)
                != claim_id
            ):
                raise Conflict("cache session write execution authority changed")
            if upload.custody_mode == "custody-transfer" and (
                upload.lease_expires_at is None or upload.lease_expires_at <= utc_timestamp_now()
            ):
                raise Conflict("cache session write custody lease is not active")
            if isinstance(operation, CompleteWriteOperation):
                begin.completion_call_id = call_id
            incarnation = begin.cache_incarnation_id
            session.add(
                CachePopulationWriteEffectRecord(
                    id=call_id,
                    collection_id=collection_id,
                    source_store=source_store,
                    object_id=object_id,
                    population_generation=begin.population_generation,
                    cache_store=begin.cache_store,
                    cache_incarnation_id=begin.cache_incarnation_id,
                    begin_call_id=begin.id,
                    kind=operation.kind,
                    resource=resource,
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
        session: Session,
        ticket: ArmedProviderWrite | WithdrawnProviderWrite,
        *,
        lock: bool = True,
    ) -> CachePopulationWriteEffectRecord:
        effect = session.get(
            CachePopulationWriteEffectRecord,
            ticket.call_id,
            with_for_update=lock,
            populate_existing=True,
        )
        if effect is None:
            raise NotFound("cache session write original effect is unavailable")
        if (
            effect.collection_id != ticket.collection_id
            or effect.object_id != ticket.object_id
            or effect.dispatch_sha256 != _sha(ticket._nonce)
            or effect.request_sha256 != ticket.request_sha256
            or _sha(effect.request_json) != effect.request_sha256
            or effect.executor_id != ticket.executor_id
            or effect.cache_incarnation_id != ticket.target_incarnation
        ):
            raise Conflict("cache session write original invocation changed")
        return effect

    @staticmethod
    def _invocation(effect: CachePopulationWriteEffectRecord) -> WriteInvocation:
        value = json.loads(effect.request_json)["operation"]
        if effect.kind == "part":
            operation: WritePartOperation | CompleteWriteOperation = (
                WritePartOperation.model_validate(value)
            )
        elif effect.kind == "complete":
            operation = CompleteWriteOperation.model_validate(value)
        else:
            raise Conflict("cache session write original operation changed")
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
        self,
        ticket: ArmedProviderWrite,
        *,
        candidate: StorageAdapterRetrievalCache,
        content: bytes = b"",
    ) -> WriteExecutionResult:
        with read_snapshot(self._factory) as session:
            effect = self._effect(session, ticket, lock=False)
            if (
                candidate.name != effect.cache_store
                or candidate.incarnation_id != effect.cache_incarnation_id
            ):
                raise Conflict("cache session write selected executor target changed")
            invocation = self._invocation(effect)
        operation = invocation.operation
        if isinstance(operation, WritePartOperation):
            if (
                len(content) != operation.request.stored_bytes
                or hashlib.sha256(content).hexdigest() != operation.stored_sha256
            ):
                raise Conflict("cache part ciphertext differs from its admitted commitment")
        elif content:
            raise Conflict("cache completion cannot accept payload bytes")
        try:
            response = ticket.dispatch(lambda: candidate.execute_write(invocation, content=content))
            validate_write_execution(invocation, response)
        except BaseException:
            self.record_unknown(ticket)
            raise
        return response

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
                raise Conflict("cache session write result exceeds its reserved byte grant")
            if effect.observation_json not in {None, encoded} or (
                effect.state == "settled" and effect.result_json != encoded
            ):
                raise Conflict("cache session write original outcome changed")
            if response.execution == "closed":
                effect.state, effect.unresolved, effect.result_json = "settled", False, encoded
            elif effect.state != "settled":
                effect.state, effect.observation_json = "uncertain", encoded

    def settle_withdrawn(self, evidence: WithdrawnProviderWrite) -> None:
        evidence.ticket._require_withdrawal(evidence)
        encoded = _json({"dispatch": "never-started"})
        with session_scope(self._factory) as session:
            _writer(session)
            original = session.get(CachePopulationWriteEffectRecord, evidence.call_id)
            if original is None:
                raise NotFound("cache session write original effect is unavailable")
            # Retain begin before child, matching forward admission's lock order.
            begin = session.get(
                CachePopulationBeginRecord,
                original.begin_call_id,
                with_for_update=True,
                populate_existing=True,
            )
            effect = self._effect(session, evidence)
            if len(encoded.encode()) > effect.result_capacity_bytes:
                raise Conflict("cache session write withdrawal exceeds its reserved byte grant")
            if effect.observation_json is not None or (
                effect.state == "settled" and effect.result_json != encoded
            ):
                raise Conflict("cache session write withdrawal conflicts with its original outcome")
            effect.state, effect.unresolved, effect.result_json = "settled", False, encoded
            if begin is not None and begin.completion_call_id == effect.id:
                begin.completion_call_id = None

    def acknowledge_report(self, ticket: ArmedProviderWrite) -> None:
        """Retire acknowledged parts; keep original completion receipts owned."""
        with session_scope(self._factory) as session:
            _writer(session)
            effect = self._effect(session, ticket)
            if effect.state != "settled":
                raise Conflict("unresolved cache session write cannot release its charge")
            if effect.kind == "complete" and json.loads(effect.result_json or "{}") != {
                "dispatch": "never-started"
            }:
                raise Conflict("cache completion receipt remains independently owned")
            session.delete(effect)

    def unresolved(self, collection_id: int, *, limit: int) -> tuple[str, ...]:
        if type(limit) is not int or limit < 1:
            raise ValueError("cache session write status limit must be positive")
        with read_snapshot(self._factory) as session:
            return tuple(
                session.scalars(
                    select(CachePopulationWriteEffectRecord.id)
                    .where(
                        CachePopulationWriteEffectRecord.collection_id == collection_id,
                        CachePopulationWriteEffectRecord.unresolved.is_(True),
                    )
                    .order_by(CachePopulationWriteEffectRecord.id)
                    .limit(limit)
                )
            )
