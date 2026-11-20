from __future__ import annotations

from typing import Any

from sqlalchemy import event, exists, select
from sqlalchemy.orm import ORMExecuteState, Session, with_loader_criteria

from riverhog_core.catalog_models import (
    CollectionArchiveObjectUploadRecord,
    CollectionUploadArtifactRecord,
    CollectionUploadArtifactVolumeRecord,
    CollectionUploadRecord,
)


def _published_member(member: Any) -> Any:
    upload = CollectionUploadRecord.__table__.alias()
    return exists(
        select(1)
        .select_from(upload)
        .where(
            upload.c.collection_id == member.collection_id,
            member.artifact_order < upload.c.artifact_count,
        )
    )


def _published_volume(volume: Any) -> Any:
    upload = CollectionUploadRecord.__table__.alias()
    return exists(
        select(1)
        .select_from(upload)
        .where(
            upload.c.collection_id == volume.collection_id,
            volume.sequence < upload.c.planner_next_sequence,
        )
    )


def _published_coverage(coverage: Any) -> Any:
    upload = CollectionUploadRecord.__table__.alias()
    member = CollectionUploadArtifactRecord.__table__.alias()
    volume = CollectionArchiveObjectUploadRecord.__table__.alias()
    return exists(
        select(1)
        .select_from(
            upload.join(member, member.c.collection_id == upload.c.collection_id).join(
                volume, volume.c.collection_id == upload.c.collection_id
            )
        )
        .where(
            upload.c.collection_id == coverage.collection_id,
            member.c.artifact_id == coverage.artifact_id,
            volume.c.object_id == coverage.object_id,
            member.c.artifact_order < upload.c.artifact_count,
            volume.c.sequence < upload.c.planner_next_sequence,
        )
    )


@event.listens_for(Session, "do_orm_execute")
def _select_published_registration(execution: ORMExecuteState) -> None:
    """Keep hidden registration suffixes outside ordinary ORM reads/mutations.

    Staging, verification and cleanup use an explicitly operation-scoped session.
    Raw SQL and the storage engine remain inside the trusted application owner.
    """

    if (
        execution.session.info.get("include_unpublished_registration", False)
        or execution.execution_options.get("include_unpublished_registration", False)
        or execution.is_column_load
        or execution.is_relationship_load
    ):
        return
    if execution.is_select or execution.is_update or execution.is_delete:
        execution.statement = execution.statement.options(
            with_loader_criteria(
                CollectionUploadArtifactRecord, _published_member, include_aliases=True
            ),
            with_loader_criteria(
                CollectionArchiveObjectUploadRecord, _published_volume, include_aliases=True
            ),
            with_loader_criteria(
                CollectionUploadArtifactVolumeRecord, _published_coverage, include_aliases=True
            ),
        )
