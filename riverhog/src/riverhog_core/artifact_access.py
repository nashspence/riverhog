from __future__ import annotations

from riverhog_protocol.errors import NotFound
from sqlalchemy import exists, select, true
from sqlalchemy.orm import InstrumentedAttribute, Session
from sqlalchemy.sql.elements import ColumnElement

from riverhog_core.app_permissions import Principal
from riverhog_core.catalog_workflow_models import CollectionProcessingCapabilityArtifactRecord


def artifact_scope_filter(
    collection_column: ColumnElement[int] | InstrumentedAttribute[int],
    path_column: ColumnElement[str] | InstrumentedAttribute[str],
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
            CollectionProcessingCapabilityArtifactRecord.path == path_column,
        )
    )


def require_artifact_scope(
    session: Session,
    principal: Principal | None,
    collection_id: int,
    path: str,
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
            CollectionProcessingCapabilityArtifactRecord.path == path,
        )
        .limit(1)
    )
    if allowed is not None:
        return
    raise NotFound(f"collection file not found: {collection_id}/{path}")


__all__ = ["artifact_scope_filter", "require_artifact_scope"]
