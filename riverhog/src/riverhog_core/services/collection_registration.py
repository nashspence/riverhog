"""Bounded private registration transitions under the collection-upload owner."""

from __future__ import annotations

import hashlib
import json
import secrets
from collections.abc import Mapping, Sequence
from typing import Any

from riverhog_canonical_json import format_scalar
from riverhog_protocol import (
    CollectionUploadArtifactBatchDocument,
    CollectionUploadRegistrationConstraintsDocument,
    validate_collection_upload_batch_against_registration_constraints,
)
from riverhog_protocol.errors import BadRequest, Conflict, NotFound
from riverhog_protocol.pack_ingress import canonical_json_bytes
from sqlalchemy import select, tuple_
from sqlalchemy.orm import Session
from time_formats import utc_timestamp_now

from riverhog_core.app_permissions import Principal
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import (
    CollectionArchiveObjectUploadRecord,
    CollectionUploadArtifactRecord,
    CollectionUploadArtifactVolumeRecord,
    CollectionUploadRecord,
)
from riverhog_core.catalog_registration_models import (
    CollectionRegistrationRecord,
    CollectionRegistrationVolumeRecord,
)
from riverhog_core.catalog_workflow_models import CollectionProcessingClaimRecord
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
    incremental_volume_planner_checkpoint_payload,
    parse_incremental_volume_planner_checkpoint,
)
from riverhog_core.services import collection_uploads as uploads
from riverhog_core.services.app_keys import (
    KeyAuthorizationFence,
    require_key_authorization,
    retain_key_authorization,
)

_CHAIN_INITIAL = hashlib.sha256(b"riverhog-registration-emission-chain/v1\0").hexdigest()
_TOTALS_INITIAL = '{"coverage_rows":"0","metadata_bytes":"0"}'


def _json(value: object) -> str:
    return canonical_json_bytes(value).decode("utf-8")


def _begin_writer(session: Session) -> None:
    if session.get_bind().dialect.name == "sqlite":
        session.connection(execution_options={"sqlite_immediate_writer": True})


def _construction(upload: CollectionUploadRecord) -> str:
    checkpoint = uploads._planner_checkpoint(upload)
    return _json(
        {
            "collection_id": format_scalar("sequence63", upload.collection_id),
            "creation_identity": upload.creation_identity_sha256,
            "idempotency_key": upload.idempotency_key,
            "delivery_context_id": upload.delivery_context_id,
            "archive_generation": upload.archive_generation,
            "archive_store": upload.archive_store,
            "archive_incarnation_id": upload.archive_incarnation_id,
            "archive_storage_prefix": upload.archive_storage_prefix,
            "encryption_format": upload.encryption_format,
            "passphrase_id": upload.passphrase_id,
            "principal_id": upload.initiated_by_principal_id,
            "initial_key_id": upload.initiated_by_key_id,
            "initial_tag_set_identity": upload.initial_tag_set_identity,
            "ingest_source": upload.ingest_source,
            "event_context": json.loads(upload.event_context_json or "null"),
            "custody_mode": upload.custody_mode,
            "use_cache": upload.use_cache,
            "copy_to": json.loads(upload.copy_to_json),
            "policy": incremental_volume_planner_checkpoint_payload(checkpoint)["policy"],
        }
    )


def _open_lease(upload: CollectionUploadRecord) -> None:
    if upload.state != "open":
        raise Conflict("collection upload registration requires an open construction")
    if upload.custody_mode == "custody-transfer" and (
        upload.lease_expires_at is None or upload.lease_expires_at <= utc_timestamp_now()
    ):
        raise Conflict("collection upload custody lease is not active")


def _claim(session: Session, upload: CollectionUploadRecord, key_id: str | None) -> str | None:
    if not upload.initiated_by_principal_id.startswith("processing:"):
        return None
    execution = upload.initiated_by_principal_id.removeprefix("processing:")
    claim = session.execute(
        select(
            CollectionProcessingClaimRecord.id,
            CollectionProcessingClaimRecord.state,
            CollectionProcessingClaimRecord.plan_sealed_at,
            CollectionProcessingClaimRecord.expires_at,
            CollectionProcessingClaimRecord.consumer_key_id,
        )
        .where(CollectionProcessingClaimRecord.execution_id == execution)
        .with_for_update()
    ).one_or_none()
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
    return {
        "phase": operation.phase,
        "operation_id": operation.id,
        "collection_id": format_scalar("sequence63", operation.collection_id),
        "request_sha256": operation.request_sha256,
    }


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
        or _construction(upload) != operation.construction_json
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

    def admit(
        self,
        collection_id: int,
        artifacts: Sequence[Mapping[str, object]],
        *,
        principal: Principal,
    ) -> dict[str, object]:
        normalized_id = uploads._collection_id(collection_id)
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
            construction = _construction(upload)
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
            fence = retain_key_authorization(session, principal)
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
                if operation.phase not in {"staging", "verifying", "auditing", "ready"}:
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
            if operation.phase == "published":
                return _outcome(operation)
            if upload is None:
                raise Conflict("collection upload registration construction is unavailable")
            _live(session, upload, operation)
            if operation.phase == "staging":
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
            if operation.phase != "published":
                operation.due_at = operation.updated_at
            session.flush()
            return _outcome(operation)

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
