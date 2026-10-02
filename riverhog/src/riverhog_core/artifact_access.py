from __future__ import annotations

from riverhog_protocol.errors import NotFound
from sqlalchemy import exists, select, true
from sqlalchemy.orm import InstrumentedAttribute, Session
from sqlalchemy.sql.elements import ColumnElement
from time_formats import utc_timestamp_now

from riverhog_core.app_permissions import Principal
from riverhog_core.catalog_workflow_models import (
    CollectionProcessingCapabilityArtifactRecord,
    CollectionProcessingCapabilityRecord,
    CollectionProcessingClaimRecord,
)


def require_current_artifact_capability(session: Session, principal: Principal) -> None:
    """A persisted member selection grants nothing after its live claim fence ends."""
    if not principal.has_artifact_scope:
        return
    capability = session.get(
        CollectionProcessingCapabilityRecord, principal.artifact_scope_capability_id
    )
    claim = (
        session.get(CollectionProcessingClaimRecord, capability.claim_id) if capability else None
    )
    now = utc_timestamp_now()
    if (
        capability is None
        or capability.state != "active"
        or capability.expires_at <= now
        or claim is None
        or claim.state != "active"
        or claim.expires_at <= now
        or capability.fence != claim.fence
        or claim.consumer_key_id != principal.key_id
    ):
        raise NotFound("processing capability is no longer active")


def artifact_scope_filter(
    collection_column: ColumnElement[int] | InstrumentedAttribute[int],
    artifact_column: ColumnElement[str] | InstrumentedAttribute[str],
    principal: Principal | None,
) -> ColumnElement[bool]:
    """Bind artifact scope without expanding persisted capabilities into predicates."""

    if principal is None or not principal.has_artifact_scope:
        return true()
    assert principal.artifact_scope_capability_id is not None
    return exists(
        select(1).where(
            CollectionProcessingCapabilityArtifactRecord.capability_id
            == principal.artifact_scope_capability_id,
            CollectionProcessingCapabilityArtifactRecord.collection_id == collection_column,
            CollectionProcessingCapabilityArtifactRecord.artifact_id == artifact_column,
        )
    )


def require_artifact_scope(
    session: Session,
    principal: Principal | None,
    collection_id: int,
    artifact_id: str,
) -> None:
    """Fail closed unless one exact artifact is inside the principal's scope."""

    if principal is None or not principal.has_artifact_scope:
        return
    assert principal.artifact_scope_capability_id is not None
    allowed = session.scalar(
        select(CollectionProcessingCapabilityArtifactRecord.capability_id)
        .where(
            CollectionProcessingCapabilityArtifactRecord.capability_id
            == principal.artifact_scope_capability_id,
            CollectionProcessingCapabilityArtifactRecord.collection_id == collection_id,
            CollectionProcessingCapabilityArtifactRecord.artifact_id == artifact_id,
        )
        .limit(1)
    )
    if allowed is not None:
        return
    raise NotFound(f"collection artifact not found: {collection_id}/{artifact_id}")


__all__ = ["artifact_scope_filter", "require_artifact_scope", "require_current_artifact_capability"]
