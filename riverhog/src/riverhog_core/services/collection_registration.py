"""Bounded private registration transitions under the collection-upload owner."""

from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
import time
from collections.abc import Mapping, Sequence
from typing import Any

from http_api_contracts.browse import BrowseTokenCodec, BrowseTokenError
from riverhog_canonical_json import format_scalar, parse_scalar
from riverhog_protocol import (
    CollectionUploadArtifactBatchDocument,
    CollectionUploadRegistrationConstraintsDocument,
    validate_collection_upload_batch_against_registration_constraints,
)
from riverhog_protocol.errors import BadRequest, Conflict, NotFound
from riverhog_protocol.pack_ingress import canonical_json_bytes
from riverhog_protocol.transport import COLLECTION_UPLOAD_ARTIFACT_BATCH_MAX
from sqlalchemy import select, tuple_
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from state_schema import read_snapshot
from time_formats import format_utc_timestamp, utc_now, utc_timestamp_now

from riverhog_core.app_permissions import COLLECTIONS_CREATE, COLLECTIONS_DELETE, Principal
from riverhog_core.browse import validate_page_size
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import (
    CollectionArchiveObjectUploadRecord,
    CollectionRecord,
    CollectionUploadArtifactRecord,
    CollectionUploadArtifactVolumeRecord,
    CollectionUploadRecord,
)
from riverhog_core.catalog_registration_models import (
    CollectionRegistrationRecord,
    CollectionRegistrationVolumeRecord,
)
from riverhog_core.catalog_workflow_models import CollectionProcessingClaimRecord
from riverhog_core.collection_access import (
    require_collection_access,
    require_collection_create_access,
)
from riverhog_core.collection_construction import upload_construction
from riverhog_core.domain.archive import ArchiveArtifact, PackVolumePlan
from riverhog_core.incremental_emission import (
    VolumeEmissionCapacity,
    VolumeEmissionFrontier,
    VolumeEmissionLimits,
    advance_volume_emission,
    parse_volume_emission_frontier,
    volume_emission_frontier_bytes,
    volume_emission_metadata,
)
from riverhog_core.incremental_plan import (
    IncrementalVolumePlanBatch,
    OrderedArchiveArtifact,
    incremental_volume_planner_checkpoint_bytes,
    parse_incremental_volume_planner_checkpoint,
)
from riverhog_core.services import collection_uploads as uploads
from riverhog_core.services.app_keys import (
    KeyAuthorizationFence,
    require_key_authorization,
    retain_key_authorization,
    retain_key_validity,
)

_CHAIN_INITIAL = hashlib.sha256(b"riverhog-registration-emission-chain/v1\0").hexdigest()
_TOTALS_INITIAL = '{"coverage_rows":"0","metadata_bytes":"0"}'
_REGISTRATION_PHASES = ("staging", "verifying", "auditing", "ready")
_RUNNABLE_PHASES = (*_REGISTRATION_PHASES, "cleaning")


def _busy(error: OperationalError) -> bool:
    original = error.orig
    code = getattr(original, "sqlite_errorcode", None)
    return (
        isinstance(code, int) and code & 0xFF in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)
    ) or getattr(original, "sqlstate", None) in {"40001", "40P01", "55P03"}


def _json(value: object) -> str:
    return canonical_json_bytes(value).decode("utf-8")


def _begin_writer(session: Session, *, defer_if_busy: bool = False) -> None:
    if session.get_bind().dialect.name == "sqlite":
        session.connection(
            execution_options={
                "sqlite_immediate_writer": True,
                "sqlite_defer_if_busy": defer_if_busy,
            }
        )


def _open_lease(upload: CollectionUploadRecord) -> None:
    if upload.state != "open":
        raise Conflict("collection upload registration requires an open construction")
    if upload.custody_mode == "custody-transfer" and (
        upload.lease_expires_at is None or upload.lease_expires_at <= utc_timestamp_now()
    ):
        raise Conflict("collection upload custody lease is not active")


def _principal_key(
    session: Session, principal: Principal, *, for_update: bool = True
) -> KeyAuthorizationFence | None:
    if principal.id.startswith("processing:"):
        if principal.key_id is None:
            raise NotFound("collection upload execution key is unavailable")
        return retain_key_validity(session, principal.key_id, for_update=for_update)
    return retain_key_authorization(session, principal, for_update=for_update)


def _claim(
    session: Session,
    upload: CollectionUploadRecord,
    key_id: str | None,
    *,
    for_update: bool = True,
) -> str | None:
    return _execution_claim(
        session, upload.initiated_by_principal_id, key_id, for_update=for_update
    )


def _execution_claim(
    session: Session,
    principal_id: str,
    key_id: str | None,
    *,
    for_update: bool = True,
) -> str | None:
    if not principal_id.startswith("processing:"):
        return None
    execution = principal_id.removeprefix("processing:")
    statement = select(
        CollectionProcessingClaimRecord.id,
        CollectionProcessingClaimRecord.state,
        CollectionProcessingClaimRecord.plan_sealed_at,
        CollectionProcessingClaimRecord.expires_at,
        CollectionProcessingClaimRecord.consumer_key_id,
    ).where(CollectionProcessingClaimRecord.execution_id == execution)
    if for_update:
        statement = statement.with_for_update()
    claim = session.execute(statement).one_or_none()
    if (
        claim is None
        or claim.state != "active"
        or claim.plan_sealed_at is None
        or claim.expires_at <= utc_timestamp_now()
        or claim.consumer_key_id != key_id
    ):
        raise NotFound("collection upload execution authority is no longer active")
    return str(claim.id)


def _request(
    document: CollectionUploadArtifactBatchDocument, checkpoint_json: str
) -> tuple[uploads._RegisteredArtifact, ...]:
    checkpoint = parse_incremental_volume_planner_checkpoint(checkpoint_json)
    constraints = CollectionUploadRegistrationConstraintsDocument.model_validate(
        uploads._registration_constraints_payload(checkpoint.policy)
    )
    validate_collection_upload_batch_against_registration_constraints(document, constraints)
    return tuple(
        uploads._normalize_artifact(value, constraints=constraints) for value in document.artifacts
    )


def _inputs(operation: CollectionRegistrationRecord) -> tuple[OrderedArchiveArtifact, ...]:
    document = CollectionUploadArtifactBatchDocument.model_validate_json(operation.request_json)
    members = _request(document, operation.base_checkpoint_json)
    base = parse_incremental_volume_planner_checkpoint(operation.base_checkpoint_json)
    indices = json.loads(operation.new_members_json)
    if (
        not isinstance(indices, list)
        or any(type(i) is not int or not 0 <= i < len(members) for i in indices)
        or indices != sorted(set(indices))
    ):
        raise RuntimeError("registration member partition is inconsistent")
    return tuple(
        OrderedArchiveArtifact(
            base.next_artifact_order + ordinal,
            ArchiveArtifact(
                members[index]["artifact_id"], members[index]["bytes"], members[index]["sha256"]
            ),
        )
        for ordinal, index in enumerate(indices)
    )


def _chain(chain: str, volumes: Sequence[Any]) -> str:
    for plan in volumes:
        body, _, coverage = volume_emission_metadata(plan)
        framed = bytes.fromhex(chain)
        for value in (body, coverage):
            framed += len(value).to_bytes(8, "big") + value
        chain = hashlib.sha256(framed).hexdigest()
    return chain


def _totals(content: str, *, edges: int, metadata: int) -> str:
    value = json.loads(content)
    if set(value) != {"coverage_rows", "metadata_bytes"}:
        raise RuntimeError("registration accounting is inconsistent")
    return _json(
        {
            "coverage_rows": format_scalar("nonnegative", int(value["coverage_rows"]) + edges),
            "metadata_bytes": format_scalar("nonnegative", int(value["metadata_bytes"]) + metadata),
        }
    )


def _outcome(operation: CollectionRegistrationRecord) -> dict[str, object]:
    if operation.phase == "published":
        if operation.acknowledgement_json is None:
            raise RuntimeError("published registration has no acknowledgement")
        return dict(json.loads(operation.acknowledgement_json))
    result: dict[str, object] = {
        "phase": operation.phase,
        "operation_id": operation.id,
        "collection_id": format_scalar("sequence63", operation.collection_id),
        "request_sha256": operation.request_sha256,
    }
    if operation.failure_json is not None:
        result["failure"] = json.loads(operation.failure_json)
    return result


def _live(
    session: Session, upload: CollectionUploadRecord, operation: CollectionRegistrationRecord
) -> None:
    _open_lease(upload)
    if (
        hashlib.sha256(operation.request_json.encode()).hexdigest() != operation.request_sha256
        or hashlib.sha256(
            canonical_json_bytes(
                {
                    "format": "riverhog-registration-operation/v1",
                    "construction": json.loads(operation.construction_json),
                    "request": json.loads(operation.request_json),
                }
            )
        ).hexdigest()
        != operation.id
    ):
        raise RuntimeError("registration retained request identity differs")
    if (
        upload.active_registration_id != operation.id
        or str(upload.registration_revision) != operation.base_revision
        or upload.planner_checkpoint_json != operation.base_checkpoint_json
        or upload_construction(upload) != operation.construction_json
        or upload.tag_staging_set_identity != operation.authorized_tag_set_identity
    ):
        raise Conflict("collection upload registration fence changed")
    if operation.key_id is not None:
        if operation.key_app is None or operation.key_generation is None:
            raise RuntimeError("registration key fence is incomplete")
        require_key_authorization(
            session,
            KeyAuthorizationFence(operation.key_id, operation.key_app, operation.key_generation),
        )
    if _claim(session, upload, operation.key_id) != operation.claim_id:
        raise Conflict("collection upload registration execution changed")


def _owned_members(
    session: Session,
    operation: CollectionRegistrationRecord,
    inputs: Sequence[OrderedArchiveArtifact],
) -> None:
    # Request size is bounded. This is admission/publication control work, not a
    # pass over the emitted volume or coverage interval.
    rows = list(
        session.scalars(
            select(CollectionUploadArtifactRecord)
            .where(
                CollectionUploadArtifactRecord.collection_id == operation.collection_id,
                CollectionUploadArtifactRecord.registration_id == operation.id,
            )
            .order_by(CollectionUploadArtifactRecord.artifact_order)
            .limit(len(inputs) + 1)
        )
    )
    document = CollectionUploadArtifactBatchDocument.model_validate_json(operation.request_json)
    members = _request(document, operation.base_checkpoint_json)
    selected = [members[i] for i in json.loads(operation.new_members_json)]
    if len(rows) != len(inputs):
        raise RuntimeError("registration staged member set differs")
    for row, ordered, expected in zip(rows, inputs, selected, strict=True):
        if (
            row.artifact_order != ordered.order
            or uploads._registered_artifact_identity(row) != expected
        ):
            raise RuntimeError("registration staged member identity differs")


class CollectionRegistrationOwner:
    """One private transition per call; the upload service owns activation."""

    def __init__(self, service: uploads.SqlAlchemyCollectionUploadService) -> None:
        self._service = service
        self._factory = service._session_factory
        self._tokens = BrowseTokenCodec(
            service._config.browse_token_signing_key,
            lifetime_seconds=int(service._config.browse_token_lifetime.total_seconds()),
        )

    def _readable(
        self,
        session: Session,
        collection_id: int,
        operation_id: str,
        principal: Principal,
    ) -> CollectionRegistrationRecord:
        operation = session.scalar(
            select(CollectionRegistrationRecord).where(
                CollectionRegistrationRecord.collection_id == collection_id,
                CollectionRegistrationRecord.id == operation_id,
            )
        )
        if operation is None:
            raise NotFound("collection upload registration not found")
        fence = _principal_key(session, principal, for_update=False)
        upload = session.get(CollectionUploadRecord, collection_id)
        if upload is not None:
            if upload_construction(upload) != operation.construction_json:
                raise NotFound("collection upload registration construction differs")
            if upload.initiated_by_principal_id == principal.id:
                # A current key generation and exact tag-set identity retain the
                # admission proof. Changed access is checked against today's set.
                if not (
                    fence is not None
                    and principal.authorization_view_identity is not None
                    and not principal.has_artifact_scope
                    and operation.principal_id == principal.id
                    and operation.key_id == fence.key_id
                    and operation.key_app == fence.app
                    and operation.key_generation == fence.generation
                    and operation.authorized_tag_set_identity == upload.tag_staging_set_identity
                ):
                    uploads._require_upload_create_access(session, collection_id, principal)
                _claim(session, upload, principal.key_id, for_update=False)
                return operation
            if uploads._upload_visible_to_deleter(session, upload, principal):
                return operation
            raise NotFound("collection upload registration not found")
        collection = session.get(CollectionRecord, collection_id)
        if collection is None or not collection.is_published:
            raise NotFound("collection upload registration not found")
        if collection.created_by_principal_id == principal.id:
            require_collection_create_access(principal, COLLECTIONS_CREATE)
            _execution_claim(session, principal.id, principal.key_id, for_update=False)
        else:
            require_collection_access(session, principal, COLLECTIONS_DELETE, collection_id)
        return operation

    def status(
        self, collection_id: int, operation_id: str, *, principal: Principal
    ) -> dict[str, object]:
        normalized_id = uploads._collection_id(collection_id)
        with read_snapshot(self._factory) as session:
            return _outcome(self._readable(session, normalized_id, operation_id, principal))

    def list_volumes(
        self,
        collection_id: int,
        operation_id: str,
        *,
        principal: Principal,
        page_size: int = 100,
        page_token: str | None = None,
    ) -> dict[str, object]:
        validate_page_size(page_size)
        normalized_id = uploads._collection_id(collection_id)
        with read_snapshot(self._factory) as session:
            operation = self._readable(session, normalized_id, operation_id, principal)
            if operation.phase != "published":
                raise Conflict("collection upload registration is not published")
            acknowledgement = _outcome(operation)
            interval = acknowledgement["emitted"]
            if not isinstance(interval, dict):
                raise RuntimeError("registration acknowledgement interval is invalid")
            first = parse_scalar("sequence256", interval["first"])
            end = parse_scalar("sequence256", interval["next"])
            selectors = {
                "collection_id": format_scalar("sequence63", normalized_id),
                "operation_id": operation.id,
                "construction_sha256": hashlib.sha256(
                    operation.construction_json.encode()
                ).hexdigest(),
                "request_sha256": operation.request_sha256,
                "first": interval["first"],
                "next": interval["next"],
                "order": "sequence-asc",
                "page_size": page_size,
            }
            identity = {"principal_id": principal.id, "key_id": principal.key_id}
            try:
                position = self._tokens.verify(
                    page_token,
                    operation="collection-registration-volumes",
                    principal=identity,
                    selectors=selectors,
                )
                after = None
                if position is not None:
                    if len(position) != 1:
                        raise ValueError("registration page position has the wrong arity")
                    after = parse_scalar("sequence256", position[0])
                    if not first <= after < end:
                        raise ValueError("registration page position is outside its interval")
            except (BrowseTokenError, ValueError) as exc:
                raise BadRequest(str(exc)) from exc
            statement = select(CollectionRegistrationVolumeRecord).where(
                CollectionRegistrationVolumeRecord.registration_id == operation.id,
                CollectionRegistrationVolumeRecord.sequence >= first,
                CollectionRegistrationVolumeRecord.sequence < end,
            )
            if after is not None:
                statement = statement.where(CollectionRegistrationVolumeRecord.sequence > after)
            rows = list(
                session.scalars(
                    statement.order_by(CollectionRegistrationVolumeRecord.sequence).limit(
                        page_size + 1
                    )
                )
            )
            expected = first if after is None else after + 1
            for row in rows:
                if row.sequence != expected:
                    raise RuntimeError("published registration summary interval has a gap")
                expected += 1
            if len(rows) < page_size + 1 and expected != end:
                raise RuntimeError("published registration summary interval is incomplete")
            summaries: list[dict[str, str]] = []
            used = 0
            budget = self._service._config.registration.summary_page_bytes
            for row in rows[:page_size]:
                summary = {
                    "sequence": format_scalar("sequence256", row.sequence),
                    "object_id": row.object_id,
                    "kind": row.kind,
                }
                charged = len(canonical_json_bytes(summary))
                if charged > budget and not summaries:
                    raise VolumeEmissionCapacity(
                        dimension="summary_page_bytes", required=charged, admitted=budget
                    )
                if used + charged > budget:
                    break
                summaries.append(summary)
                used += charged
            has_more = len(rows) > len(summaries)
            next_token = (
                self._tokens.issue(
                    operation="collection-registration-volumes",
                    principal=identity,
                    selectors=selectors,
                    position=(summaries[-1]["sequence"],),
                )
                if has_more
                else None
            )
            return {
                "collection_id": selectors["collection_id"],
                "operation_id": operation.id,
                "request_sha256": operation.request_sha256,
                "emitted": interval,
                "volumes": summaries,
                "next_page_token": next_token,
                "exhausted": not has_more,
            }

    def admit(
        self,
        collection_id: int,
        artifacts: Sequence[Mapping[str, object]],
        *,
        principal: Principal,
    ) -> dict[str, object]:
        normalized_id = uploads._collection_id(collection_id)
        if not artifacts or len(artifacts) > COLLECTION_UPLOAD_ARTIFACT_BATCH_MAX:
            raise BadRequest(
                "collection upload artifact batch must contain "
                f"1 to {COLLECTION_UPLOAD_ARTIFACT_BATCH_MAX} artifacts"
            )
        try:
            document = CollectionUploadArtifactBatchDocument.model_validate(
                {"artifacts": list(artifacts)}
            )
        except ValueError as exc:
            raise BadRequest(str(exc)) from exc
        request_json = _json(document.model_dump(mode="json"))
        request_sha256 = hashlib.sha256(request_json.encode()).hexdigest()
        with session_scope(self._factory) as session:
            _begin_writer(session)
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == normalized_id)
                .with_for_update()
            )
            if upload is None or upload.initiated_by_principal_id != principal.id:
                raise NotFound("collection upload session not found")
            # Lock order is upload -> operation -> application key -> execution claim.
            construction = upload_construction(upload)
            operation_id = hashlib.sha256(
                canonical_json_bytes(
                    {
                        "format": "riverhog-registration-operation/v1",
                        "construction": json.loads(construction),
                        "request": json.loads(request_json),
                    }
                )
            ).hexdigest()
            operation = session.scalar(
                select(CollectionRegistrationRecord)
                .where(CollectionRegistrationRecord.id == operation_id)
                .with_for_update()
            )
            fence = _principal_key(session, principal)
            uploads._require_upload_create_access(session, normalized_id, principal)
            if upload.copy_to_json != "[]" and upload.initiated_by_key_id != principal.key_id:
                raise Conflict("collection upload copy intent initiator changed")
            claim_id = _claim(session, upload, principal.key_id)
            if operation is not None:
                if (
                    operation.request_json != request_json
                    or operation.construction_json != construction
                    or operation.request_sha256 != request_sha256
                ):
                    raise Conflict("collection upload registration identity collision")
                if operation.phase == "published":
                    return _outcome(operation)
                resume_phase = operation.phase
                if operation.phase == "paused":
                    failure = json.loads(operation.failure_json or "{}")
                    if failure.get("kind") == "authority":
                        resume_phase = failure.get("resume_phase")
                if resume_phase not in _REGISTRATION_PHASES:
                    raise Conflict(
                        "collection upload registration cannot resume in its current phase"
                    )
                _open_lease(upload)
                if upload.active_registration_id != operation.id or operation.claim_id != claim_id:
                    raise Conflict("collection upload registration fence changed")
                if (
                    operation.key_id != (fence.key_id if fence is not None else None)
                    or operation.key_generation != (fence.generation if fence is not None else None)
                    or operation.authorized_tag_set_identity != upload.tag_staging_set_identity
                ):
                    operation.execution_epoch = secrets.token_hex(32)
                operation.key_id = fence.key_id if fence is not None else None
                operation.key_app = fence.app if fence is not None else None
                operation.key_generation = fence.generation if fence is not None else None
                operation.authorized_tag_set_identity = upload.tag_staging_set_identity
                if operation.phase == "paused":
                    operation.phase = resume_phase
                    operation.execution_epoch = secrets.token_hex(32)
                    operation.failure_json = None
                    operation.due_at = utc_timestamp_now()
                uploads._touch_upload(upload, config=self._service._config)
                return _outcome(operation)
            _open_lease(upload)
            if upload.active_registration_id is not None:
                raise Conflict("collection upload registration is pending")
            try:
                members = _request(document, upload.planner_checkpoint_json)
            except ValueError as exc:
                raise BadRequest(str(exc)) from exc
            checkpoint = uploads._planner_checkpoint(upload)
            if (
                checkpoint.closed
                or checkpoint.next_artifact_order != upload.artifact_count
                or checkpoint.bytes_seen != upload.artifact_bytes
                or checkpoint.next_sequence != upload.planner_next_sequence
            ):
                raise RuntimeError("published planner frontiers differ from upload counters")
            existing = {
                row.artifact_id: row
                for row in session.scalars(
                    select(CollectionUploadArtifactRecord).where(
                        CollectionUploadArtifactRecord.collection_id == normalized_id,
                        CollectionUploadArtifactRecord.artifact_id.in_(
                            tuple(m["artifact_id"] for m in members)
                        ),
                    )
                )
            }
            indices = []
            for index, member in enumerate(members):
                row = existing.get(member["artifact_id"])
                if row is not None:
                    if uploads._registered_artifact_identity(row) != member:
                        raise Conflict("collection upload artifact already has different metadata")
                else:
                    indices.append(index)
            now = utc_timestamp_now()
            frontier = volume_emission_frontier_bytes(VolumeEmissionFrontier(checkpoint)).decode()
            upload.registration_revision += 1
            operation = CollectionRegistrationRecord(
                id=operation_id,
                collection_id=normalized_id,
                principal_id=principal.id,
                construction_json=construction,
                request_sha256=request_sha256,
                request_json=request_json,
                new_members_json=_json(indices),
                base_checkpoint_json=upload.planner_checkpoint_json,
                base_revision=str(upload.registration_revision),
                stage_frontier_json=frontier,
                verify_frontier_json=frontier,
                stage_chain=_CHAIN_INITIAL,
                verify_chain=_CHAIN_INITIAL,
                stage_totals_json=_TOTALS_INITIAL,
                verify_totals_json=_TOTALS_INITIAL,
                coverage_audit_after_artifact_id=None,
                coverage_audit_after_object_id=None,
                coverage_audited_rows="0",
                phase="staging",
                execution_epoch=secrets.token_hex(32),
                authorized_tag_set_identity=upload.tag_staging_set_identity,
                key_id=fence.key_id if fence is not None else None,
                key_app=fence.app if fence is not None else None,
                key_generation=fence.generation if fence is not None else None,
                claim_id=claim_id,
                due_at=now,
                created_at=now,
                updated_at=now,
            )
            session.add(operation)
            for ordinal, index in enumerate(indices):
                session.add(
                    CollectionUploadArtifactRecord(
                        collection_id=normalized_id,
                        artifact_order=checkpoint.next_artifact_order + ordinal,
                        registration_id=operation_id,
                        **members[index],
                    )
                )
            upload.active_registration_id = operation_id
            uploads._touch_upload(upload, config=self._service._config, now=now)
            session.flush()
            return _outcome(operation)

    def advance(
        self,
        collection_id: int,
        operation_id: str,
        *,
        limits: VolumeEmissionLimits,
        expected_epoch: str | None = None,
    ) -> dict[str, object]:
        normalized_id = uploads._collection_id(collection_id)
        with session_scope(self._factory) as session:
            _begin_writer(session)
            session.info["include_unpublished_registration"] = True
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == normalized_id)
                .with_for_update()
            )
            operation = session.scalar(
                select(CollectionRegistrationRecord)
                .where(
                    CollectionRegistrationRecord.collection_id == normalized_id,
                    CollectionRegistrationRecord.id == operation_id,
                )
                .with_for_update()
            )
            if operation is None:
                raise NotFound("collection upload registration not found")
            if expected_epoch is not None and operation.execution_epoch != expected_epoch:
                raise Conflict("collection upload registration execution changed")
            if operation.phase in {"published", "disposed"}:
                return _outcome(operation)
            if upload is None:
                raise Conflict("collection upload registration construction is unavailable")
            self._advance(session, upload, operation, limits)
            session.flush()
            return _outcome(operation)

    def _advance(
        self,
        session: Session,
        upload: CollectionUploadRecord,
        operation: CollectionRegistrationRecord,
        limits: VolumeEmissionLimits,
    ) -> None:
        if operation.phase == "cleaning":
            from riverhog_core.services.collection_registration_cleanup import (
                advance_registration_cleanup,
            )

            advance_registration_cleanup(session, upload, operation, limits=limits)
        else:
            _live(session, upload, operation)
        if operation.phase in {"cleaning", "disposed"}:
            pass
        elif operation.phase == "staging":
            self._stage(session, upload, operation, limits)
        elif operation.phase == "verifying":
            self._verify(session, upload, operation, limits)
        elif operation.phase == "auditing":
            self._audit_coverage(session, operation, limits)
        elif operation.phase == "ready":
            self._publish(session, upload, operation)
        else:
            raise Conflict("collection upload registration cannot advance in its current phase")
        operation.updated_at = utc_timestamp_now()
        operation.failure_json = None
        if operation.phase not in {"published", "disposed"}:
            operation.due_at = operation.updated_at

    def process_due(self, *, limit: int = 1) -> int:
        """Advance one quantum per selected construction, yielding between turns."""
        if limit < 1:
            return 0
        tuning = self._service._config.registration
        deadline = time.monotonic() + tuning.execution_quantum.total_seconds()
        visited: list[str] = []
        processed = 0
        for _ in range(limit):
            if processed and time.monotonic() >= deadline:
                break
            try:
                operation_id = self._process_one_due(excluding=visited)
            except OperationalError as exc:
                if not _busy(exc):
                    raise
                break
            if operation_id is None:
                break
            visited.append(operation_id)
            processed += 1
        return processed

    def _process_one_due(self, *, excluding: Sequence[str]) -> str | None:
        tuning = self._service._config.registration
        with session_scope(self._factory) as session:
            _begin_writer(session, defer_if_busy=True)
            session.info["include_unpublished_registration"] = True
            now = utc_timestamp_now()
            # One seek per fixed phase uses (phase, due_at, collection_id, id).
            # PostgreSQL locks the upload first and skips other workers' owners.
            heads = []
            for phase in _RUNNABLE_PHASES:
                statement = (
                    select(
                        CollectionRegistrationRecord.due_at,
                        CollectionRegistrationRecord.collection_id,
                        CollectionRegistrationRecord.id,
                        CollectionRegistrationRecord.execution_epoch,
                    )
                    .join(
                        CollectionUploadRecord,
                        (
                            CollectionUploadRecord.collection_id
                            == CollectionRegistrationRecord.collection_id
                        )
                        & (
                            CollectionUploadRecord.active_registration_id
                            == CollectionRegistrationRecord.id
                        ),
                    )
                    .where(
                        CollectionRegistrationRecord.phase == phase,
                        CollectionRegistrationRecord.due_at <= now,
                    )
                    .order_by(
                        CollectionRegistrationRecord.due_at,
                        CollectionRegistrationRecord.collection_id,
                        CollectionRegistrationRecord.id,
                    )
                    .limit(1)
                    .with_for_update(of=CollectionUploadRecord, skip_locked=True)
                )
                if excluding:
                    statement = statement.where(CollectionRegistrationRecord.id.not_in(excluding))
                head = session.execute(statement).one_or_none()
                if head is not None:
                    heads.append(head)
            if not heads:
                return None
            due_at, collection_id, operation_id, epoch = min(heads)
            upload = session.get(CollectionUploadRecord, collection_id)
            operation = session.scalar(
                select(CollectionRegistrationRecord)
                .where(
                    CollectionRegistrationRecord.id == operation_id,
                    CollectionRegistrationRecord.execution_epoch == epoch,
                    CollectionRegistrationRecord.phase.in_(_RUNNABLE_PHASES),
                    CollectionRegistrationRecord.due_at == due_at,
                )
                .with_for_update()
                .execution_options(populate_existing=True)
            )
            if upload is None or operation is None:
                return None
            if session.get_bind().dialect.name == "postgresql":
                # Lock contention is deferred work, not an unbounded coordinator wait.
                from sqlalchemy import text

                session.execute(
                    text("SELECT set_config('lock_timeout', :timeout, true)"),
                    {"timeout": str(max(1, int(tuning.execution_quantum.total_seconds() * 1000)))},
                )
            phase = operation.phase
            try:
                # Failed quanta leave no rows, cursor movement or partial marker.
                with session.begin_nested():
                    self._advance(session, upload, operation, tuning.emission_limits())
                    session.flush()
            except VolumeEmissionCapacity as exc:
                operation.failure_json = _json(
                    {
                        "kind": "capacity",
                        "dimension": exc.dimension,
                        "required": format_scalar("nonnegative", exc.required),
                        "admitted": format_scalar("nonnegative", exc.admitted),
                    }
                )
                operation.due_at = format_utc_timestamp(utc_now() + tuning.retry_delay)
                operation.updated_at = now
            except (Conflict, NotFound):
                operation.phase = "failed" if phase == "cleaning" else "paused"
                operation.execution_epoch = secrets.token_hex(32)
                operation.failure_json = _json(
                    {"kind": "disposal-fence"}
                    if phase == "cleaning"
                    else {"kind": "authority", "resume_phase": phase}
                )
                operation.due_at = None
                operation.updated_at = now
            except (RuntimeError, ValueError):
                operation.phase = "failed"
                operation.execution_epoch = secrets.token_hex(32)
                operation.failure_json = _json({"kind": "integrity"})
                operation.due_at = None
                operation.updated_at = now
            session.flush()
            return str(operation.id)

    def _stage(
        self,
        session: Session,
        upload: CollectionUploadRecord,
        operation: CollectionRegistrationRecord,
        limits: VolumeEmissionLimits,
    ) -> None:
        inputs = _inputs(operation)
        frontier = parse_volume_emission_frontier(operation.stage_frontier_json)
        quantum = advance_volume_emission(frontier, inputs, limits=limits)
        uploads._persist_plan_batch(
            session,
            upload=upload,
            batch=IncrementalVolumePlanBatch(
                quantum.frontier.checkpoint,
                tuple(plan for plan in quantum.volumes if isinstance(plan, PackVolumePlan)),
                tuple(plan for plan in quantum.volumes if not isinstance(plan, PackVolumePlan)),
            ),
            registration_id=operation.id,
        )
        for plan in quantum.volumes:
            session.add(
                CollectionRegistrationVolumeRecord(
                    registration_id=operation.id,
                    sequence=plan.sequence,
                    object_id=plan.volume_id,
                    kind="pack" if isinstance(plan, PackVolumePlan) else "segment",
                )
            )
        operation.stage_frontier_json = volume_emission_frontier_bytes(quantum.frontier).decode()
        operation.stage_chain = _chain(operation.stage_chain, quantum.volumes)
        operation.stage_totals_json = _totals(
            operation.stage_totals_json,
            edges=quantum.coverage_rows,
            metadata=quantum.metadata_bytes,
        )
        if quantum.frontier.done:
            operation.phase = "verifying"

    def _verify(
        self,
        session: Session,
        upload: CollectionUploadRecord,
        operation: CollectionRegistrationRecord,
        limits: VolumeEmissionLimits,
    ) -> None:
        inputs = _inputs(operation)
        frontier = parse_volume_emission_frontier(operation.verify_frontier_json)
        quantum = advance_volume_emission(frontier, inputs, limits=limits)
        for plan in quantum.volumes:
            body, members, _ = volume_emission_metadata(plan)
            row = session.scalar(
                select(CollectionArchiveObjectUploadRecord).where(
                    CollectionArchiveObjectUploadRecord.collection_id == upload.collection_id,
                    CollectionArchiveObjectUploadRecord.registration_id == operation.id,
                    CollectionArchiveObjectUploadRecord.object_id == plan.volume_id,
                )
            )
            kind = "pack" if isinstance(plan, PackVolumePlan) else "segment"
            if (
                row is None
                or row.sequence != plan.sequence
                or row.kind != kind
                or row.plan_json.encode() != body
                or row.plan_sha256
                != (
                    plan.plan_sha256
                    if isinstance(plan, PackVolumePlan)
                    else hashlib.sha256(body).hexdigest()
                )
            ):
                raise RuntimeError("registration staged plan differs from independent replay")
            part_bytes = quantum.frontier.checkpoint.policy.raw_part_plaintext_bytes
            projection: dict[str, object]
            if isinstance(plan, PackVolumePlan):
                extension = "tar"
                projection = {
                    "source_bytes": sum(m.bytes for m in plan.members),
                    "source_artifact_id": None,
                    "source_first_part": None,
                    "source_part_count": None,
                    "unit_plaintext_bytes": plan.part_plaintext_bytes,
                    "total_units": len(plan.units),
                    "work_ready": True,
                }
            else:
                extension = "bin"
                parts = max(1, (plan.plaintext_bytes + part_bytes - 1) // part_bytes)
                projection = {
                    "source_bytes": plan.plaintext_bytes,
                    "source_artifact_id": plan.artifact_id,
                    "source_first_part": plan.artifact_offset // part_bytes,
                    "source_part_count": parts,
                    "unit_plaintext_bytes": part_bytes,
                    "total_units": parts,
                    "work_ready": False,
                }
            relative = f"volumes/{plan.volume_id}.{extension}.age"
            projection.update(
                {
                    "relative_path": relative,
                    "object_path": f"{upload.archive_storage_prefix}/{relative}",
                    "plaintext_bytes": plan.plaintext_bytes,
                    "state": "planned",
                    "uploaded_bytes": 0,
                    "uploaded_units": 0,
                    "checkpoint_json": None,
                    "sealed_receipt_json": None,
                    "metadata_receipt_json": None,
                    "sealed_at": None,
                }
            )
            if any(getattr(row, field) != value for field, value in projection.items()):
                raise RuntimeError("registration staged work projection differs from replay")
            edges = list(
                session.scalars(
                    select(CollectionUploadArtifactVolumeRecord)
                    .where(
                        CollectionUploadArtifactVolumeRecord.collection_id == upload.collection_id,
                        CollectionUploadArtifactVolumeRecord.object_id == plan.volume_id,
                    )
                    .limit(len(members) + 1)
                )
            )
            if {edge.artifact_id for edge in edges} != set(members) or any(
                edge.registration_id != operation.id for edge in edges
            ):
                raise RuntimeError("registration staged coverage differs from independent replay")
            summary = session.get(CollectionRegistrationVolumeRecord, (operation.id, plan.sequence))
            if summary is None or summary.object_id != plan.volume_id or summary.kind != kind:
                raise RuntimeError("registration retained summary differs from independent replay")
        operation.verify_frontier_json = volume_emission_frontier_bytes(quantum.frontier).decode()
        operation.verify_chain = _chain(operation.verify_chain, quantum.volumes)
        operation.verify_totals_json = _totals(
            operation.verify_totals_json,
            edges=quantum.coverage_rows,
            metadata=quantum.metadata_bytes,
        )
        if quantum.frontier.done:
            _owned_members(session, operation, inputs)
            if (
                operation.verify_frontier_json != operation.stage_frontier_json
                or operation.verify_chain != operation.stage_chain
                or operation.verify_totals_json != operation.stage_totals_json
            ):
                raise RuntimeError("registration verification differs from staging")
            base = parse_incremental_volume_planner_checkpoint(operation.base_checkpoint_json)
            pending = base.pending_pack_artifacts
            if pending:
                expected = [(m.artifact_id, m.bytes, m.sha256) for m in pending]
                rows = session.execute(
                    select(
                        CollectionUploadArtifactRecord.artifact_id,
                        CollectionUploadArtifactRecord.bytes,
                        CollectionUploadArtifactRecord.sha256,
                    )
                    .where(
                        CollectionUploadArtifactRecord.collection_id == operation.collection_id,
                        CollectionUploadArtifactRecord.artifact_order
                        >= base.artifacts_seen - len(pending),
                        CollectionUploadArtifactRecord.artifact_order < base.artifacts_seen,
                    )
                    .order_by(CollectionUploadArtifactRecord.artifact_order)
                    .limit(len(pending) + 1)
                ).all()
                if [tuple(row) for row in rows] != expected:
                    raise RuntimeError("registration prior pending member identity differs")
            self._check_extra_sequences(session, operation)
            operation.phase = "auditing"

    @staticmethod
    def _audit_coverage(
        session: Session, operation: CollectionRegistrationRecord, limits: VolumeEmissionLimits
    ) -> None:
        # A separate keyset pass accounts for unrelated extra edges as well as
        # those checked beside replayed plans. The final marker never scans E.
        statement = select(CollectionUploadArtifactVolumeRecord).where(
            CollectionUploadArtifactVolumeRecord.collection_id == operation.collection_id,
            CollectionUploadArtifactVolumeRecord.registration_id == operation.id,
        )
        after = operation.coverage_audit_after_artifact_id
        if after is not None:
            statement = statement.where(
                tuple_(
                    CollectionUploadArtifactVolumeRecord.artifact_id,
                    CollectionUploadArtifactVolumeRecord.object_id,
                )
                > (after, operation.coverage_audit_after_object_id)
            )
        rows = list(
            session.scalars(
                statement.order_by(
                    CollectionUploadArtifactVolumeRecord.artifact_id,
                    CollectionUploadArtifactVolumeRecord.object_id,
                ).limit(min(limits.coverage_rows, max(1, limits.metadata_bytes // 256)))
            )
        )
        used = 0
        consumed = 0
        for row in rows:
            size = len(
                canonical_json_bytes({"artifact_id": row.artifact_id, "object_id": row.object_id})
            )
            if size > limits.metadata_bytes:
                raise VolumeEmissionCapacity(
                    dimension="metadata_bytes", required=size, admitted=limits.metadata_bytes
                )
            if used + size > limits.metadata_bytes:
                break
            used += size
            consumed += 1
            operation.coverage_audit_after_artifact_id = row.artifact_id
            operation.coverage_audit_after_object_id = row.object_id
        audited = int(operation.coverage_audited_rows) + consumed
        expected = int(json.loads(operation.verify_totals_json)["coverage_rows"])
        if audited > expected:
            raise RuntimeError("registration contains extra staged coverage")
        operation.coverage_audited_rows = format_scalar("nonnegative", audited)
        if not rows:
            if audited != expected:
                raise RuntimeError("registration staged coverage accounting differs")
            operation.phase = "ready"

    @staticmethod
    def _check_extra_sequences(session: Session, operation: CollectionRegistrationRecord) -> None:
        first = parse_incremental_volume_planner_checkpoint(
            operation.base_checkpoint_json
        ).next_sequence
        end = parse_volume_emission_frontier(operation.stage_frontier_json).checkpoint.next_sequence
        for _model, scope, sequence, identity in (
            (
                CollectionArchiveObjectUploadRecord,
                (
                    CollectionArchiveObjectUploadRecord.collection_id == operation.collection_id,
                    CollectionArchiveObjectUploadRecord.registration_id == operation.id,
                ),
                CollectionArchiveObjectUploadRecord.sequence,
                CollectionArchiveObjectUploadRecord.object_id,
            ),
            (
                CollectionRegistrationVolumeRecord,
                (CollectionRegistrationVolumeRecord.registration_id == operation.id,),
                CollectionRegistrationVolumeRecord.sequence,
                CollectionRegistrationVolumeRecord.object_id,
            ),
        ):
            for outside in (sequence < first, sequence >= end):
                if session.scalar(select(identity).where(*scope, outside).limit(1)) is not None:
                    raise RuntimeError("registration contains an extra staged sequence")

    @staticmethod
    def _publish(
        session: Session, upload: CollectionUploadRecord, operation: CollectionRegistrationRecord
    ) -> None:
        if (
            operation.verify_frontier_json != operation.stage_frontier_json
            or operation.verify_chain != operation.stage_chain
            or operation.verify_totals_json != operation.stage_totals_json
            or int(operation.coverage_audited_rows)
            != int(json.loads(operation.verify_totals_json)["coverage_rows"])
        ):
            raise RuntimeError("registration publication has no matching verification")
        frontier = parse_volume_emission_frontier(operation.verify_frontier_json)
        if not frontier.done:
            raise RuntimeError("registration publication frontier is incomplete")
        inputs = _inputs(operation)
        _owned_members(session, operation, inputs)
        checkpoint = frontier.checkpoint
        document = CollectionUploadArtifactBatchDocument.model_validate_json(operation.request_json)
        indices = set(json.loads(operation.new_members_json))
        base = parse_incremental_volume_planner_checkpoint(operation.base_checkpoint_json)
        acknowledgement = {
            "phase": "published",
            "collection_id": format_scalar("sequence63", upload.collection_id),
            "operation_id": operation.id,
            "request_sha256": operation.request_sha256,
            "construction": json.loads(operation.construction_json),
            "artifacts": [
                {"identity": item.model_dump(mode="json"), "new": index in indices}
                for index, item in enumerate(document.artifacts)
            ],
            "emitted": {
                "first": format_scalar("sequence256", base.next_sequence),
                "next": format_scalar("sequence256", checkpoint.next_sequence),
            },
        }
        upload.planner_checkpoint_json = incremental_volume_planner_checkpoint_bytes(
            checkpoint
        ).decode()
        upload.planner_next_sequence = checkpoint.next_sequence
        upload.artifact_count = checkpoint.artifacts_seen
        upload.artifact_bytes = checkpoint.bytes_seen
        upload.registration_revision += 1
        upload.active_registration_id = None
        upload.archive_phase = "uploading" if checkpoint.next_sequence else "planning"
        upload.archive_phase_updated_at = utc_timestamp_now()
        operation.acknowledgement_json = _json(acknowledgement)
        operation.phase = "published"
        operation.due_at = None
