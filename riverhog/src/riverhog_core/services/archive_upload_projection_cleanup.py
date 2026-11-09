"""Retire only the exact unpublished catalog projection of a closed upload."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from riverhog_protocol.errors import Conflict
from sqlalchemy import Table, select
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import ColumnElement

from riverhog_core.catalog_base import Base
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.catalog_discovery_validation_models import DiscoveryValidationJobRecord as Job
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexGenerationRecord as Generation,
)
from riverhog_core.incremental_emission import VolumeEmissionCapacity, VolumeEmissionLimits
from riverhog_core.services.archive_upload_cleanup import ArchiveUploadCleanup
from riverhog_core.services.catalog_retirement import (
    CatalogRetirementRow,
    delete_catalog_window,
    metadata_row_bytes,
    read_catalog_window,
)

PROJECTION_TABLES = (
    "collection_tag_publication_frontier",
    "collection_tag_published_nodes",
    "collection_tag_publications",
    "collection_description_publications",
    "collection_archive_artifact_objects",
    "collection_artifact_provenance",
    "collection_provenance_journal_segments",
    "collection_provenance_journal_agents",
    "collection_provenance_external_state_references",
    "collection_provenance_entities",
    "collection_provenance_journals",
    "collection_artifacts",
    "collection_archive_objects",
    "collection_archive_copies",
    "collection_tag_revisions",
    "collection_tag_memberships",
    "collection_provenance_index_state",
)
BUILD_TABLES = (
    "collection_provenance_index_text_chunks",
    "collection_provenance_index_values",
    "collection_provenance_index_profiles",
    "collection_provenance_index_memberships",
    "collection_provenance_index_edges",
    "collection_provenance_index_assertions",
    "collection_provenance_index_entries",
    "collection_provenance_index_snapshots",
    "collection_provenance_index_members",
)
INDEPENDENT_TABLES = (
    "archive_copy_jobs",
    "archive_copy_object_uploads",
    "archive_copy_retirements",
    "collection_derivations",
    "collection_mutable_document_publication_attempts",
    "collection_processing_capabilities",
    "collection_processing_capability_artifacts",
    "collection_processing_claim_artifacts",
    "collection_processing_claim_inputs",
    "collection_processing_claims",
    "collection_processing_consideration_evidence",
    "collection_processing_consideration_subjects",
    "collection_processing_disposition_outputs",
    "collection_processing_disposition_sets",
    "collection_processing_dispositions",
    "collection_processing_effect_settlements",
    "collection_processing_no_output_settlements",
    "collection_processing_outcomes",
    "collection_provenance_verification_agents",
    "collection_provenance_verification_entities",
    "collection_provenance_verification_entries",
    "collection_provenance_verification_external_states",
    "collection_provenance_verification_reachability",
    "collection_provenance_verifications",
    "collection_tag_mutation_node_references",
    "collection_tag_mutations",
    "collection_tag_node_gc",
    "discovery_validation_jobs",
    "retrieval_cache_leases",
    "retrieval_cache_objects",
    "retrieval_job_object_progress",
    "retrieval_plan_artifacts",
    "retrieval_plan_objects",
    "retrieval_plan_placements",
)

# These roots have their own mutable work or receiving. Their descendants never
# enter this owner's deletion routes; published collection disposal stays separate.
INDEPENDENT_ROOTS = {
    "collection_tag_node_gc": "collection_id",
    "collection_tag_mutations": "collection_id",
    "collection_provenance_verifications": "collection_id",
    "collection_mutable_document_publication_attempts": "collection_id",
    "archive_copy_retirements": "collection_id",
    "archive_copy_jobs": "collection_id",
    "retrieval_plan_objects": "collection_id",
    "retrieval_plan_artifacts": "collection_id",
    "retrieval_cache_objects": "collection_id",
    "collection_derivations": "collection_id",
    "collection_processing_claims": "output_collection_id",
}


def validate_projection_cleanup_routes() -> None:
    descendants = {"collections"}
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
    routes = (*PROJECTION_TABLES, *BUILD_TABLES, *INDEPENDENT_TABLES)
    if len(routes) != len(set(routes)) or descendants != {
        "collections",
        "collection_provenance_index_generations",
        *routes,
    }:
        raise RuntimeError("unpublished projection retirement routes differ from current schema")
    for names, scope in ((PROJECTION_TABLES, "collection_id"), (BUILD_TABLES, "build_id")):
        positions = {name: index for index, name in enumerate(names)}
        for name, index in positions.items():
            table = Base.metadata.tables[name]
            if tuple(table.primary_key.columns)[0].name != scope:
                raise RuntimeError("projection retirement requires indexed first-live scopes")
            for constraint in table.foreign_key_constraints:
                parent = constraint.referred_table.name
                if parent in positions and positions[parent] <= index:
                    raise RuntimeError("projection retirement requires child-before-parent routes")


@dataclass(frozen=True, slots=True)
class ProjectionCleanupProgress:
    table: str | None
    removed: int = 0
    metadata_bytes: int = 0
    exhausted: bool = False
    pending_reason: str | None = None


def _original_projection(session: Session, upload: Upload) -> dict[str, Any] | None:
    table = Base.metadata.tables["collections"]
    expected = {
        "id": upload.collection_id,
        "is_published": False,
        "creation_idempotency_key": upload.idempotency_key,
        "creation_identity_sha256": upload.creation_identity_sha256,
        "creation_custody_mode": upload.custody_mode,
        "creation_archive_store": upload.archive_store,
        "creation_use_cache": upload.use_cache,
        "archive_generation": upload.archive_generation,
        "delivery_context_id": upload.delivery_context_id,
        "encryption_format": upload.encryption_format,
        "passphrase_id": upload.passphrase_id,
        "created_by_principal_id": upload.initiated_by_principal_id,
        "created_by_key_id": upload.initiated_by_key_id,
        "artifact_set_identity": upload.catalog_artifact_set_identity,
        "inventory_identity": upload.catalog_inventory_identity,
        "provenance_identity": upload.provenance_identity,
    }
    row = (
        session.execute(
            select(*(table.c[name] for name in (*expected, "archive_root_sha256")))
            .where(table.c.id == upload.collection_id)
            .with_for_update()
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    if any(row[name] != value for name, value in expected.items()):
        raise Conflict("catalog projection is not the original unpublished upload")
    if upload.final_authority_json is None:
        raise Conflict("unpublished projection has no original archive authority")
    try:
        root = json.loads(upload.final_authority_json)["root"]["plaintext_sha256"]
    except (ValueError, KeyError, TypeError):
        raise Conflict("unpublished projection original archive authority is invalid") from None
    if not isinstance(root, str) or row["archive_root_sha256"] != root:
        raise Conflict("unpublished projection archive root differs from its original")
    original = dict(row)
    copies = Base.metadata.tables["collection_archive_copies"]
    source = (
        session.execute(
            select(copies.c.store, copies.c.incarnation_id, copies.c.archive_storage_prefix).where(
                copies.c.collection_id == upload.collection_id,
                copies.c.store == upload.archive_store,
            )
        )
        .mappings()
        .one_or_none()
    )
    if source is not None:
        if (
            source["incarnation_id"] != upload.archive_incarnation_id
            or source["archive_storage_prefix"] != upload.archive_storage_prefix
        ):
            raise Conflict("unpublished projection original archive source differs")
        original.update({"source_" + name: value for name, value in source.items()})
    return original


class ArchiveUploadProjectionCleanup:
    """Retire first-live materialized rows without published-root disposal rights.

    Every quantum retains current terminal rights, the exact complete archive
    proof and the original unpublished root fence. Independent receiving stays
    untouched. A linked discovery workspace remains pending until its owner
    supplies exclusive original custody; no absence or requested close is a
    replacement for that proof.
    """

    def __init__(self, factory: SessionFactory, cleanup: ArchiveUploadCleanup) -> None:
        validate_projection_cleanup_routes()
        self._factory = factory
        self._cleanup = cleanup

    def advance(
        self,
        collection_id: int,
        operation_id: str,
        *,
        guard: Callable[[Session], None],
        limits: VolumeEmissionLimits,
    ) -> ProjectionCleanupProgress:
        with session_scope(self._factory) as session:
            self._cleanup.require_exhausted(session, collection_id, operation_id, guard=guard)
            upload = session.get(Upload, collection_id)
            assert upload is not None
            if upload.active_registration_id is not None:
                raise Conflict("projection retirement requires completed hidden registration")
            original = _original_projection(session, upload)
            if original is None:
                return ProjectionCleanupProgress(None, exhausted=True)
            used = metadata_row_bytes(original)
            if used > limits.metadata_bytes:
                raise VolumeEmissionCapacity(
                    dimension="metadata_bytes", required=used, admitted=limits.metadata_bytes
                )

            def window(
                table: Table, scope: tuple[ColumnElement[bool], ...]
            ) -> tuple[list[CatalogRetirementRow], int]:
                available = limits.metadata_bytes - used
                if available < 1:
                    raise VolumeEmissionCapacity(
                        dimension="metadata_bytes",
                        required=used + 1,
                        admitted=limits.metadata_bytes,
                    )
                try:
                    rows, charge = read_catalog_window(
                        session,
                        table,
                        scope,
                        count=limits.input_transitions,
                        metadata_bytes=available,
                    )
                except VolumeEmissionCapacity as error:
                    raise VolumeEmissionCapacity(
                        dimension="metadata_bytes",
                        required=used + error.required,
                        admitted=limits.metadata_bytes,
                    ) from None
                return rows, used + charge

            for name, column in INDEPENDENT_ROOTS.items():
                table = Base.metadata.tables[name]
                if (
                    session.scalar(
                        select(1)
                        .select_from(table)
                        .where(table.c[column] == collection_id)
                        .limit(1)
                    )
                    is not None
                ):
                    raise Conflict("unpublished projection retains independent work: " + name)
            generation = (
                session.execute(
                    select(
                        Generation.build_id,
                        Generation.archive_generation,
                        Generation.archive_root_sha256,
                        Generation.provenance_identity,
                    )
                    .where(Generation.collection_id == collection_id)
                    .order_by(Generation.build_id)
                    .limit(1)
                    .with_for_update()
                )
                .mappings()
                .one_or_none()
            )
            if generation is not None:
                if any(
                    generation[name] != original[name]
                    for name in ("archive_generation", "archive_root_sha256", "provenance_identity")
                ):
                    raise Conflict("unpublished discovery generation differs from its original")
                used += metadata_row_bytes(dict(generation))
                if used > limits.metadata_bytes:
                    raise VolumeEmissionCapacity(
                        dimension="metadata_bytes", required=used, admitted=limits.metadata_bytes
                    )
                build_id = generation["build_id"]
                workspace = session.scalar(
                    select(Job.workspace_id).where(Job.build_id == build_id).limit(1)
                )
                if workspace is not None:
                    used += metadata_row_bytes({"workspace_id": workspace})
                    if used > limits.metadata_bytes:
                        raise VolumeEmissionCapacity(
                            dimension="metadata_bytes",
                            required=used,
                            admitted=limits.metadata_bytes,
                        )
                    return ProjectionCleanupProgress(
                        "discovery_validation_jobs",
                        metadata_bytes=used,
                        pending_reason="discovery-custody",
                    )
                for name in (*BUILD_TABLES, "collection_provenance_index_generations"):
                    table = Base.metadata.tables[name]
                    rows, charge = window(table, (table.c.build_id == build_id,))
                    if not rows:
                        continue
                    delete_catalog_window(session, table, rows)
                    return ProjectionCleanupProgress(name, len(rows), charge)
                raise Conflict("original discovery generation disappeared during retirement")
            for name in (*PROJECTION_TABLES, "collections"):
                table = Base.metadata.tables[name]
                scope = table.c.id if name == "collections" else table.c.collection_id
                rows, charge = window(table, (scope == collection_id,))
                if not rows:
                    continue
                for row in rows:
                    values = row.values
                    if "store" in values and values["store"] != upload.archive_store:
                        raise Conflict("unpublished projection retains another archive store")
                    if (
                        "incarnation_id" in values
                        and values["incarnation_id"] != upload.archive_incarnation_id
                    ):
                        raise Conflict("unpublished projection retains another archive incarnation")
                    if (
                        name == "collection_archive_copies"
                        and values["archive_storage_prefix"] != upload.archive_storage_prefix
                    ):
                        raise Conflict("unpublished projection retains another archive prefix")
                delete_catalog_window(session, table, rows)
                return ProjectionCleanupProgress(name, len(rows), charge)
            raise Conflict("original unpublished projection disappeared during retirement")
