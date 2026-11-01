"""Bounded hidden-row disposal under the existing upload terminal owner."""

from __future__ import annotations

import json
import re
import secrets
from collections.abc import Callable, Mapping, Sequence
from functools import lru_cache
from typing import Any, Literal, cast

from riverhog_canonical_json import format_scalar
from riverhog_protocol.errors import Conflict
from riverhog_protocol.pack_ingress import canonical_json_bytes
from sqlalchemy import ForeignKeyConstraint, Table, and_, bindparam, delete, select
from sqlalchemy.engine import CursorResult, Row
from sqlalchemy.orm import Session
from sqlalchemy.sql import ColumnElement, Select
from time_formats import utc_timestamp_now

from riverhog_core.catalog_base import Base
from riverhog_core.catalog_models import (
    CollectionArchiveObjectUploadRecord,
    CollectionDeletionRecord,
    CollectionRecord,
    CollectionUploadArtifactRecord,
    CollectionUploadArtifactVolumeRecord,
    CollectionUploadRecord,
)
from riverhog_core.catalog_registration_models import (
    CollectionRegistrationRecord,
    CollectionRegistrationVolumeRecord,
)
from riverhog_core.collection_registration_identity import registration_construction
from riverhog_core.incremental_emission import VolumeEmissionCapacity, VolumeEmissionLimits


def _json(value: object) -> str:
    return canonical_json_bytes(value).decode()


def _reservation(upload: CollectionUploadRecord, operation: CollectionRegistrationRecord) -> None:
    if (
        operation.collection_id != upload.collection_id
        or upload.active_registration_id != operation.id
        or str(upload.registration_revision) != operation.base_revision
        or upload.planner_checkpoint_json != operation.base_checkpoint_json
        or registration_construction(upload) != operation.construction_json
    ):
        raise Conflict("registration disposal reservation changed")
    if upload.state != "discarding":
        raise Conflict("registration disposal requires its selected terminal owner")
    if operation.disposition_json is not None:
        selected = json.loads(operation.disposition_json)
        if (
            not isinstance(selected, dict)
            or set(selected) != {"kind", "sha256"}
            or selected.get("kind") not in ("cancel", "orphan-discard")
            or not isinstance(selected.get("sha256"), str)
            or not re.fullmatch("[0-9a-f]{64}", selected["sha256"])
            or _json(selected) != operation.disposition_json
        ):
            raise RuntimeError("registration disposal selection is invalid")


def fence_registration_disposal(
    session: Session,
    upload: CollectionUploadRecord,
    *,
    kind: Literal["cancel", "orphan-discard"],
    identity: str,
) -> CollectionRegistrationRecord | None:
    """Fence a terminal request already authorized and selected by its owner.

    The caller holds the upload's early writer/row lock, checks existing sealed
    custody and discard guards, and closes construction in the same transaction.
    The existing discarding phase closes construction for either selected kind.
    This helper grants no cancellation/discard right and performs no provider work.
    """
    if kind not in {"cancel", "orphan-discard"} or not re.fullmatch("[0-9a-f]{64}", identity):
        raise ValueError("registration disposal identity is invalid")
    if upload.state != "discarding":
        raise Conflict("registration disposal requires its selected terminal owner")
    if upload.active_registration_id is None:
        return None
    operation = session.scalar(
        select(CollectionRegistrationRecord)
        .where(
            CollectionRegistrationRecord.collection_id == upload.collection_id,
            CollectionRegistrationRecord.id == upload.active_registration_id,
        )
        .with_for_update()
    )
    if operation is None:
        raise RuntimeError("registration disposal has no retained active operation")
    _reservation(upload, operation)
    disposition = _json({"kind": kind, "sha256": identity})
    if operation.disposition_json is not None:
        if operation.disposition_json != disposition:
            raise Conflict("registration disposal selection differs")
        if operation.phase != "cleaning":
            raise Conflict("registration disposal is not advancing")
        return operation
    if operation.phase not in {"staging", "verifying", "auditing", "ready", "paused", "failed"}:
        raise Conflict("accepted registration cannot be disposed as hidden work")
    operation.execution_epoch = secrets.token_hex(32)
    operation.disposition_json = disposition
    operation.cleanup_phase = "coverage"
    operation.phase = "cleaning"
    operation.failure_json = None
    operation.due_at = operation.updated_at = utc_timestamp_now()
    return operation


def _rows(
    session: Session,
    statement: Select[Any],
    *,
    count: int,
    budget: int,
    payload: Callable[[Row[Any]], object],
) -> list[Row[Any]]:
    # Every selected control serializes to at least eight bytes. A tiny byte
    # grant therefore probes one row before refusal, not an entire count grant.
    rows = session.execute(statement.limit(min(count, max(1, budget // 8)))).all()
    selected: list[Row[Any]] = []
    used = 0
    for row in rows:
        charge = len(canonical_json_bytes(payload(row)))
        if charge > budget and not selected:
            raise VolumeEmissionCapacity(
                dimension="metadata_bytes", required=charge, admitted=budget
            )
        if used + charge > budget:
            break
        selected.append(row)
        used += charge
    return selected


def _delete(
    session: Session,
    table: Table,
    scope: Sequence[Any],
    columns: Sequence[Any],
    keys: Sequence[Sequence[Any]],
) -> None:
    statement = delete(table).where(
        *scope, *(column == bindparam("target_" + column.name) for column in columns)
    )
    # Constant bind count per statement; the admitted row window bounds the
    # executemany, without relying on a backend's maximum IN-list length.
    result = session.execute(
        statement,
        [
            {"target_" + column.name: value for column, value in zip(columns, key, strict=True)}
            for key in keys
        ],
    )
    if cast(CursorResult[Any], result).rowcount != len(keys):
        raise RuntimeError("registration cleanup selection changed")


@lru_cache
def _children(name: str) -> tuple[ForeignKeyConstraint, ...]:
    return tuple(
        constraint
        for table in Base.metadata.tables.values()
        for constraint in table.foreign_key_constraints
        if constraint.referred_table.name == name
    )


def _reject_children(session: Session, table: Table, keys: Sequence[Mapping[str, object]]) -> None:
    for constraint in _children(table.name):
        for key in keys:
            if (
                session.execute(
                    select(*constraint.table.primary_key.columns)
                    .where(*(edge.parent == key[edge.column.name] for edge in constraint.elements))
                    .limit(1)
                ).first()
                is not None
            ):
                raise RuntimeError("hidden registration row has retained dependent state")


def _empty_plan(
    plan: Table, upload: CollectionUploadRecord, operation: CollectionRegistrationRecord
) -> ColumnElement[bool]:
    return and_(
        plan.c.registration_id == operation.id,
        plan.c.sequence >= upload.planner_next_sequence,
        plan.c.state == "planned",
        plan.c.checkpoint_json.is_(None),
        plan.c.sealed_receipt_json.is_(None),
        plan.c.metadata_receipt_json.is_(None),
        plan.c.failure.is_(None),
        plan.c.uploaded_bytes == 0,
        plan.c.uploaded_units == 0,
        plan.c.sealed_at.is_(None),
    )


def advance_registration_cleanup(
    session: Session,
    upload: CollectionUploadRecord,
    operation: CollectionRegistrationRecord,
    *,
    limits: VolumeEmissionLimits,
) -> None:
    """Delete one admitted window; retain its reservation until exact emptiness."""
    _reservation(upload, operation)
    if operation.phase != "cleaning" or operation.disposition_json is None:
        raise Conflict("registration has no selected disposal")
    edge = Base.metadata.tables[CollectionUploadArtifactVolumeRecord.__tablename__]
    plan = Base.metadata.tables[CollectionArchiveObjectUploadRecord.__tablename__]
    member = Base.metadata.tables[CollectionUploadArtifactRecord.__tablename__]
    summary = Base.metadata.tables[CollectionRegistrationVolumeRecord.__tablename__]
    scope: tuple[ColumnElement[bool], ...]
    if operation.cleanup_phase == "coverage":
        scope = (
            edge.c.collection_id == upload.collection_id,
            edge.c.registration_id == operation.id,
        )
        rows = _rows(
            session,
            select(
                edge.c.artifact_id,
                edge.c.object_id,
                _empty_plan(plan, upload, operation).label("empty"),
            )
            .select_from(
                edge.outerjoin(
                    plan,
                    (plan.c.collection_id == edge.c.collection_id)
                    & (plan.c.object_id == edge.c.object_id),
                )
            )
            .where(*scope)
            .order_by(edge.c.artifact_id, edge.c.object_id),
            count=limits.coverage_rows,
            budget=limits.metadata_bytes,
            payload=lambda row: {"artifact_id": row.artifact_id, "object_id": row.object_id},
        )
        if not rows:
            operation.cleanup_phase = "plans"
            return
        if any(row.empty is not True for row in rows):
            raise RuntimeError("hidden coverage references accepted or active payload work")
        _delete(
            session,
            edge,
            scope,
            (edge.c.artifact_id, edge.c.object_id),
            [(row.artifact_id, row.object_id) for row in rows],
        )
    elif operation.cleanup_phase == "plans":
        scope = (
            plan.c.collection_id == upload.collection_id,
            plan.c.registration_id == operation.id,
        )
        rows = _rows(
            session,
            select(
                plan.c.sequence,
                plan.c.object_id,
                _empty_plan(plan, upload, operation).label("empty"),
            )
            .where(*scope)
            .order_by(plan.c.sequence),
            count=limits.plans,
            budget=limits.metadata_bytes,
            payload=lambda row: {
                "sequence": format_scalar("sequence256", row.sequence),
                "object_id": row.object_id,
            },
        )
        if not rows:
            operation.cleanup_phase = "members"
            return
        if any(row.empty is not True for row in rows):
            raise RuntimeError("hidden plan contains retained payload state")
        _reject_children(
            session,
            plan,
            [{"collection_id": upload.collection_id, "object_id": row.object_id} for row in rows],
        )
        _delete(session, plan, scope, (plan.c.object_id,), [(row.object_id,) for row in rows])
    elif operation.cleanup_phase == "members":
        scope = (
            member.c.collection_id == upload.collection_id,
            member.c.registration_id == operation.id,
        )
        rows = _rows(
            session,
            select(
                member.c.artifact_id,
                and_(
                    member.c.artifact_order >= upload.artifact_count,
                    member.c.raw_parts_accepted == 0,
                    member.c.raw_part_commitment_sha256.is_(None),
                    member.c.payload_sealed_at.is_(None),
                    member.c.custody_receipt_json.is_(None),
                ).label("empty"),
            )
            .where(*scope)
            .order_by(member.c.artifact_order),
            count=limits.input_transitions,
            budget=limits.metadata_bytes,
            payload=lambda row: {"artifact_id": row.artifact_id},
        )
        if not rows:
            operation.cleanup_phase = "summaries"
            return
        if any(row.empty is not True for row in rows):
            raise RuntimeError("hidden member contains accepted or retained custody state")
        _reject_children(
            session,
            member,
            [
                {"collection_id": upload.collection_id, "artifact_id": row.artifact_id}
                for row in rows
            ],
        )
        _delete(
            session, member, scope, (member.c.artifact_id,), [(row.artifact_id,) for row in rows]
        )
    elif operation.cleanup_phase == "summaries":
        scope = (summary.c.registration_id == operation.id,)
        rows = _rows(
            session,
            select(summary.c.sequence, summary.c.object_id)
            .where(*scope)
            .order_by(summary.c.sequence),
            count=limits.plans,
            budget=limits.metadata_bytes,
            payload=lambda row: {
                "sequence": format_scalar("sequence256", row.sequence),
                "object_id": row.object_id,
            },
        )
        if not rows:
            operation.cleanup_phase = "finish"
            return
        _delete(session, summary, scope, (summary.c.sequence,), [(row.sequence,) for row in rows])
    elif operation.cleanup_phase == "finish":
        for table in (edge, plan, member):
            if (
                session.execute(
                    select(*table.primary_key.columns)
                    .where(
                        table.c.collection_id == upload.collection_id,
                        table.c.registration_id == operation.id,
                    )
                    .limit(1)
                ).first()
                is not None
            ):
                raise RuntimeError("registration cleanup has a remaining hidden row")
        if (
            session.execute(
                select(summary.c.sequence).where(summary.c.registration_id == operation.id).limit(1)
            ).first()
            is not None
        ):
            raise RuntimeError("registration cleanup has a remaining summary")
        operation.cleanup_phase = "done"
        operation.phase = "disposed"
        operation.due_at = None
        upload.active_registration_id = None
        upload.registration_revision += 1
    else:
        raise RuntimeError("registration cleanup phase is invalid")


def reclaim_collection_registrations(
    session: Session, collection_id: int, *, limits: VolumeEmissionLimits
) -> bool:
    """Reclaim one metadata window under an already selected collection deletion."""
    if session.get(CollectionDeletionRecord, collection_id) is None:
        raise Conflict("registration reclamation requires selected collection deletion")
    collection = session.get(CollectionRecord, collection_id)
    if collection is None or collection.is_published:
        raise Conflict("registration reclamation requires a closed collection")
    upload = session.get(CollectionUploadRecord, collection_id)
    if upload is not None and upload.active_registration_id is not None:
        raise Conflict("registration reclamation cannot bypass active construction disposal")
    operation_id = session.scalar(
        select(CollectionRegistrationRecord.id)
        .where(CollectionRegistrationRecord.collection_id == collection_id)
        .order_by(CollectionRegistrationRecord.id)
        .limit(1)
        .with_for_update()
    )
    if operation_id is None:
        return False
    control_bytes = len(canonical_json_bytes({"operation_id": operation_id}))
    if control_bytes > limits.metadata_bytes:
        raise VolumeEmissionCapacity(
            dimension="metadata_bytes", required=control_bytes, admitted=limits.metadata_bytes
        )
    summary = Base.metadata.tables[CollectionRegistrationVolumeRecord.__tablename__]
    scope = (summary.c.registration_id == operation_id,)
    rows = _rows(
        session,
        select(summary.c.sequence, summary.c.object_id).where(*scope).order_by(summary.c.sequence),
        count=limits.plans,
        budget=limits.metadata_bytes - control_bytes,
        payload=lambda row: {
            "sequence": format_scalar("sequence256", row.sequence),
            "object_id": row.object_id,
        },
    )
    if rows:
        _delete(session, summary, scope, (summary.c.sequence,), [(row.sequence,) for row in rows])
    else:
        operation = Base.metadata.tables[CollectionRegistrationRecord.__tablename__]
        _delete(
            session,
            operation,
            (operation.c.collection_id == collection_id,),
            (operation.c.id,),
            [(operation_id,)],
        )
    return True
