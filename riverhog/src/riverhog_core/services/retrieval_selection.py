from __future__ import annotations

import json
from collections.abc import Callable, Sequence

from http_api_contracts import canonical_json_bytes
from riverhog_age.resumable_age import UploadState
from riverhog_canonical_json import format_scalar
from riverhog_protocol.errors import InvalidState
from sqlalchemy import and_, exists, func, or_, select
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import ColumnElement

from riverhog_core.catalog_models import (
    ArchiveCopyRetirementRecord,
    CollectionArchiveArtifactObjectRecord,
    CollectionArchiveCopyRecord,
    CollectionArchiveObjectRecord,
    CollectionArtifactRecord,
    RetrievalCacheObjectRecord,
    RetrievalJobRecord,
    RetrievalPlanObjectRecord,
    RetrievalPlanRecord,
    StorageIncarnationRecord,
)


def cache_object_key(planned: RetrievalPlanObjectRecord) -> tuple[str, int, str]:
    source = planned.cache_source_store if planned.read_mode == "cache" else planned.source_store
    if source is None:
        raise InvalidState("retrieval cache source identity is missing")
    return source, planned.collection_id, planned.object_id


def cache_source_incarnation(planned: RetrievalPlanObjectRecord) -> str:
    incarnation = (
        planned.cache_source_incarnation_id
        if planned.read_mode == "cache"
        else planned.source_incarnation_id
    )
    if incarnation is None:
        raise InvalidState("retrieval cache source incarnation is missing")
    return incarnation


def cache_matches_plan() -> ColumnElement[bool]:
    """The selected direct cache, or the selected archive's eventual restored cache."""
    return and_(
        RetrievalPlanObjectRecord.read_mode.in_({"cache", "restore_required"}),
        RetrievalCacheObjectRecord.source_store
        == func.coalesce(
            RetrievalPlanObjectRecord.cache_source_store, RetrievalPlanObjectRecord.source_store
        ),
        RetrievalCacheObjectRecord.source_incarnation_id
        == func.coalesce(
            RetrievalPlanObjectRecord.cache_source_incarnation_id,
            RetrievalPlanObjectRecord.source_incarnation_id,
        ),
        RetrievalCacheObjectRecord.collection_id == RetrievalPlanObjectRecord.collection_id,
        RetrievalCacheObjectRecord.object_id == RetrievalPlanObjectRecord.object_id,
        or_(
            RetrievalPlanObjectRecord.read_mode == "restore_required",
            and_(
                RetrievalCacheObjectRecord.cache_store == RetrievalPlanObjectRecord.cache_store,
                RetrievalCacheObjectRecord.cache_incarnation_id
                == RetrievalPlanObjectRecord.cache_incarnation_id,
            ),
        ),
    )


def plan_object_uses_store(store: str) -> ColumnElement[bool]:
    return or_(
        RetrievalPlanObjectRecord.source_store == store,
        RetrievalPlanObjectRecord.cache_source_store == store,
    )


def active_cache_reference(now: str) -> ColumnElement[bool]:
    return exists(
        select(1)
        .select_from(RetrievalPlanObjectRecord)
        .join(RetrievalPlanRecord, RetrievalPlanRecord.id == RetrievalPlanObjectRecord.plan_id)
        .outerjoin(RetrievalJobRecord, RetrievalJobRecord.plan_id == RetrievalPlanRecord.id)
        .where(
            cache_matches_plan(),
            or_(
                (
                    RetrievalPlanRecord.state.in_({"planning", "ready"})
                    & (RetrievalPlanRecord.expires_at > now)
                ),
                (
                    (RetrievalJobRecord.state == "requested")
                    & (RetrievalPlanRecord.expires_at > now)
                ),
                ((RetrievalJobRecord.state == "ready") & (RetrievalJobRecord.expires_at > now)),
            ),
        )
    )


def select_equivalent_cache(
    session: Session,
    selected: CollectionArchiveObjectRecord,
    *,
    store_order: Sequence[str],
    usable: Callable[[RetrievalCacheObjectRecord], bool],
) -> RetrievalCacheObjectRecord | None:
    """Compare sealed catalog facts without probing the historical archive adapter."""
    statement = (
        select(RetrievalCacheObjectRecord, CollectionArchiveObjectRecord)
        .join(
            CollectionArchiveObjectRecord,
            and_(
                CollectionArchiveObjectRecord.collection_id
                == RetrievalCacheObjectRecord.collection_id,
                CollectionArchiveObjectRecord.store == RetrievalCacheObjectRecord.source_store,
                CollectionArchiveObjectRecord.object_id == RetrievalCacheObjectRecord.object_id,
            ),
        )
        .join(CollectionArchiveCopyRecord)
        .join(
            StorageIncarnationRecord,
            and_(
                StorageIncarnationRecord.kind == "archive",
                StorageIncarnationRecord.name == RetrievalCacheObjectRecord.source_store,
                StorageIncarnationRecord.id == RetrievalCacheObjectRecord.source_incarnation_id,
            ),
        )
        .where(
            RetrievalCacheObjectRecord.collection_id == selected.collection_id,
            RetrievalCacheObjectRecord.object_id == selected.object_id,
            RetrievalCacheObjectRecord.state == "ready",
            CollectionArchiveCopyRecord.state == "uploaded",
            CollectionArchiveCopyRecord.last_uploaded_at.is_not(None),
            CollectionArchiveCopyRecord.last_verified_at.is_not(None),
            CollectionArchiveCopyRecord.incarnation_id
            == RetrievalCacheObjectRecord.source_incarnation_id,
            StorageIncarnationRecord.state != "retired",
            ~exists(
                select(1).where(
                    ArchiveCopyRetirementRecord.collection_id == selected.collection_id,
                    ArchiveCopyRetirementRecord.store == RetrievalCacheObjectRecord.source_store,
                )
            ),
        )
        .order_by(RetrievalCacheObjectRecord.source_store)
        .with_for_update(of=RetrievalCacheObjectRecord)
    )
    selected_identity: bytes | None = None
    for store in store_order:
        rows = session.execute(
            statement.where(RetrievalCacheObjectRecord.cache_store == store).execution_options(
                yield_per=100
            )
        )
        try:
            for cached, source in rows.tuples():
                if not usable(cached):
                    continue
                if selected_identity is None:
                    selected_identity = _payload_identity(session, selected)
                if (
                    _payload_identity(session, source) != selected_identity
                    or cached.stored_bytes != selected.stored_bytes
                    or (
                        source.stored_sha256 is not None
                        and selected.stored_sha256 is not None
                        and source.stored_sha256 != selected.stored_sha256
                    )
                    or (
                        (expected_sha256 := source.stored_sha256 or selected.stored_sha256)
                        is not None
                        and cached.stored_sha256 != expected_sha256
                    )
                ):
                    raise InvalidState("archive copies disagree on sealed payload identity")
                return cached
        finally:
            rows.close()
    return None


def _payload_identity(session: Session, obj: CollectionArchiveObjectRecord) -> bytes:
    if not obj.age_state_json:
        raise InvalidState("sealed payload age state is missing")
    try:
        UploadState.from_json_bytes(obj.age_state_json)
        age_state = json.loads(obj.age_state_json)
    except ValueError as exc:
        raise InvalidState("sealed payload age state is invalid") from exc
    if not isinstance(age_state, dict):
        raise InvalidState("sealed payload age state is invalid")
    identity: dict[str, object] = {
        "kind": obj.kind,
        "object_id": obj.object_id,
        "plaintext_bytes": format_scalar("nonnegative", obj.plaintext_bytes),
        "stored_bytes": format_scalar("nonnegative", obj.stored_bytes),
        "age_state": age_state,
        "sha256": obj.sha256,
    }
    if obj.kind == "pack":
        if obj.plan_sha256 is None or obj.index_sha256 is None:
            raise InvalidState("sealed pack identity is missing")
        identity.update(plan_sha256=obj.plan_sha256, index_sha256=obj.index_sha256)
    elif obj.kind == "segment":
        rows = session.execute(
            select(CollectionArchiveArtifactObjectRecord, CollectionArtifactRecord)
            .join(CollectionArtifactRecord)
            .where(
                CollectionArchiveArtifactObjectRecord.collection_id == obj.collection_id,
                CollectionArchiveArtifactObjectRecord.store == obj.store,
                CollectionArchiveArtifactObjectRecord.object_id == obj.object_id,
            )
            .limit(2)
        ).all()
        if len(rows) != 1:
            raise InvalidState("sealed raw segment placement is not unique")
        placement, artifact = rows[0]
        identity["artifact"] = {
            "artifact_id": artifact.artifact_id,
            "bytes": format_scalar("nonnegative", artifact.bytes),
            "sha256": artifact.sha256,
            "artifact_offset": format_scalar("nonnegative", placement.artifact_offset),
            "object_offset": format_scalar("nonnegative", placement.object_offset),
            "segment_bytes": format_scalar("nonnegative", placement.bytes),
        }
    else:
        raise InvalidState("unsupported cached payload kind")
    return canonical_json_bytes(identity)
