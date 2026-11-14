"""Bounded upload-child teardown under an original archive terminal proof."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from riverhog_protocol.errors import Conflict
from sqlalchemy import select
from sqlalchemy.orm import Session

from riverhog_core.catalog_base import Base
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.catalog_journal_validation_models import JournalValidationJobRecord as Job
from riverhog_core.catalog_models import CollectionUploadRecord
from riverhog_core.incremental_emission import VolumeEmissionLimits
from riverhog_core.services.archive_upload_cleanup import ArchiveUploadCleanup
from riverhog_core.services.catalog_retirement import delete_catalog_window, read_catalog_window
from riverhog_core.services.upload_journal_validation_jobs import retire_source

# Every ordinary FK child is deliberately routed in child-before-parent order.
# Independently retained validation work is detached through its own owner.
UPLOAD_CHILD_TABLES = (
    "collection_archive_object_upload_units",
    "collection_upload_artifact_materialization_decisions",
    "collection_upload_artifact_provenance_bindings",
    "collection_upload_artifact_volumes",
    "collection_upload_member_histories",
    "collection_upload_raw_part_digests",
    "collection_upload_initial_descriptions",
    "collection_upload_initial_tag_publications",
    "collection_upload_provenance_active_edges",
    "collection_upload_provenance_archive_volumes",
    "collection_upload_provenance_authority_publications",
    "collection_upload_provenance_capture_segments",
    "collection_upload_provenance_custody_objects",
    "collection_upload_provenance_epoch_arcs",
    "collection_upload_provenance_epoch_graphs",
    "collection_upload_provenance_epoch_rank_graphs",
    "collection_upload_provenance_epoch_rank_vertices",
    "collection_upload_provenance_epoch_scans",
    "collection_upload_provenance_epoch_vertices",
    "collection_upload_provenance_frames",
    "collection_upload_provenance_journal_chunks",
    "collection_upload_provenance_journal_publications",
    "collection_upload_provenance_retired_preparations",
    "collection_upload_provenance_structure",
    "collection_upload_provenance_topology_facts",
    "collection_upload_provenance_volume_publications",
    "collection_upload_provenance_journals",
    "collection_upload_tag_node_references",
    "collection_upload_tag_publication_frontier",
    "collection_upload_tags",
    "collection_upload_artifacts",
    "collection_archive_object_uploads",
)


def validate_upload_cleanup_routes() -> None:
    """Reject schema drift rather than allowing an unreviewed cascade."""
    descendants = {"collection_uploads"}
    while True:
        found = descendants | {
            table.name
            for table in Base.metadata.tables.values()
            for constraint in table.foreign_key_constraints
            if constraint.referred_table.name in descendants
        }
        if found == descendants:
            break
        descendants = found
    expected = {"collection_uploads", "journal_validation_jobs", *UPLOAD_CHILD_TABLES}
    if descendants != expected or len(UPLOAD_CHILD_TABLES) != len(set(UPLOAD_CHILD_TABLES)):
        raise RuntimeError("upload catalog cleanup routes differ from current schema")
    positions = {name: index for index, name in enumerate(UPLOAD_CHILD_TABLES)}
    for name, index in positions.items():
        table = Base.metadata.tables[name]
        if tuple(table.primary_key.columns)[0].name != "collection_id":
            raise RuntimeError("upload catalog cleanup requires an indexed collection scope")
        for constraint in table.foreign_key_constraints:
            parent = constraint.referred_table.name
            if parent in positions and positions[parent] <= index:
                raise RuntimeError("upload catalog cleanup requires child-before-parent routes")


@dataclass(frozen=True, slots=True)
class UploadCatalogProgress:
    """Internal child progress; no parent retirement or confirmed outcome."""

    table: str | None
    removed: int = 0
    metadata_bytes: int = 0
    upload_children_exhausted: bool = False


class ArchiveUploadCatalogCleanup:
    """Delete one first-live row window, keeping all independent receiving.

    Complete row metadata, opaque bytes and any detached validation fence count
    toward this quantum. A bounded first row may refuse an insufficient grant.
    Fixed-schema first-live seeks avoid revisiting a growing completed prefix.
    Upload, terminal, archive proof, construction preparations, materialized
    collection projection and independent registration/cache/workspace receipts
    keep their own retirement and confirmation steps.
    """

    def __init__(self, factory: SessionFactory, cleanup: ArchiveUploadCleanup) -> None:
        validate_upload_cleanup_routes()
        self._factory = factory
        self._cleanup = cleanup

    def advance(
        self,
        collection_id: int,
        operation_id: str,
        *,
        guard: Callable[[Session], None],
        limits: VolumeEmissionLimits,
    ) -> UploadCatalogProgress:
        with session_scope(self._factory) as session:
            self._cleanup.require_exhausted(session, collection_id, operation_id, guard=guard)
            upload = session.get(CollectionUploadRecord, collection_id)
            assert upload is not None
            if upload.active_registration_id is not None:
                raise Conflict("upload catalog cleanup requires completed hidden registration")

            def related(row: Mapping[str, Any]) -> Mapping[str, Any] | None:
                job = session.execute(
                    select(Job.workspace_id, Job.attempt_id)
                    .where(
                        Job.collection_id == collection_id,
                        Job.journal_id == row["journal_id"],
                    )
                    .with_for_update(nowait=True)
                ).one_or_none()
                return dict(job._mapping) if job is not None else None

            for name in UPLOAD_CHILD_TABLES:
                table = Base.metadata.tables[name]

                selected, used = read_catalog_window(
                    session,
                    table,
                    (table.c.collection_id == collection_id,),
                    count=limits.input_transitions,
                    metadata_bytes=limits.metadata_bytes,
                    related=related if name == "collection_upload_provenance_journals" else None,
                )
                if not selected:
                    continue
                for row in selected:
                    if row.related is not None:
                        retire_source(
                            session,
                            collection_id,
                            row.values["journal_id"],
                            row.related["attempt_id"],
                        )
                delete_catalog_window(session, table, selected)
                return UploadCatalogProgress(name, len(selected), used)
            return UploadCatalogProgress(None, upload_children_exhausted=True)
