"""Canonical provenance reads over published collection members and archive roots."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from riverhog_canonical_json import format_scalar
from riverhog_protocol import ArtifactId, CollectionArtifactProvenanceBindingDocument
from riverhog_protocol.errors import NotFound
from riverhog_protocol.paths import validate_collection_id
from riverhog_provenance_contracts import ProvenanceJournalId
from sqlalchemy import select
from sqlalchemy.orm import Session
from state_schema import read_snapshot

from riverhog_core.app_permissions import (
    CATALOG_READ,
    PROVENANCE_EXPORT,
    PROVENANCE_READ,
    Principal,
)
from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.artifact_access import artifact_scope_filter, require_artifact_scope
from riverhog_core.canonical_provenance_archive import PublishedCanonicalProvenance
from riverhog_core.catalog_db import SessionFactory, make_session_factory
from riverhog_core.catalog_models import (
    CollectionArtifactProvenanceRecord,
    CollectionArtifactRecord,
    CollectionRecord,
)
from riverhog_core.runtime_config import RuntimeConfig


class SqlAlchemyCanonicalProvenanceService:
    """Expose exact members and journals; the query index remains a separate projection."""

    def __init__(
        self,
        config: RuntimeConfig,
        archive_stores: ArchiveStoreRegistry,
        *,
        session_factory: SessionFactory | None = None,
    ) -> None:
        self._session_factory = session_factory or make_session_factory(config.database_url)
        self._archives = PublishedCanonicalProvenance(self._session_factory, archive_stores)

    def list_artifacts(
        self,
        collection_id: int,
        *,
        page_size: int,
        after_artifact_id: ArtifactId | None,
        principal: Principal,
    ) -> dict[str, Any]:
        normalized_id = validate_collection_id(collection_id)
        if type(page_size) is not int or not 1 <= page_size <= 200:
            raise ValueError("artifact page size must be 1 to 200")
        with read_snapshot(self._session_factory) as session:
            collection = _authorized_collection(session, normalized_id, principal)
            statement = select(CollectionArtifactRecord).where(
                CollectionArtifactRecord.collection_id == normalized_id,
                artifact_scope_filter(
                    CollectionArtifactRecord.collection_id,
                    CollectionArtifactRecord.artifact_id,
                    principal,
                ),
            )
            if after_artifact_id is not None:
                statement = statement.where(
                    CollectionArtifactRecord.artifact_id > str(after_artifact_id)
                )
            rows = list(
                session.scalars(
                    statement.order_by(CollectionArtifactRecord.artifact_id).limit(page_size + 1)
                )
            )
            more = len(rows) > page_size
            selected = rows[:page_size]
            return {
                "collection_id": format_scalar("sequence63", normalized_id),
                "archive_root_sha256": collection.archive_root_sha256,
                "artifact_set_identity": collection.artifact_set_identity,
                "provenance_identity": collection.provenance_identity,
                "artifacts": [_member_row(row) for row in selected],
                "next_artifact_id": selected[-1].artifact_id if more else None,
            }

    def get_artifact(
        self,
        collection_id: int,
        artifact_id: ArtifactId,
        *,
        principal: Principal,
    ) -> dict[str, Any]:
        normalized_id = validate_collection_id(collection_id)
        canonical_id = ArtifactId(artifact_id)
        with read_snapshot(self._session_factory) as session:
            collection = _authorized_collection(session, normalized_id, principal)
            require_artifact_scope(session, principal, normalized_id, canonical_id)
            member = session.get(CollectionArtifactRecord, (normalized_id, canonical_id))
            if member is None:
                raise NotFound(f"collection artifact not found: {normalized_id}/{canonical_id}")
            projection = session.get(
                CollectionArtifactProvenanceRecord, (normalized_id, canonical_id)
            )
            if projection is None:
                raise NotFound("collection artifact has no primary canonical binding")
            member_payload = _member_row(member)
            root_identity = collection.archive_root_sha256
            projected_binding = (
                projection.journal_id,
                projection.prefix_sha256,
                projection.delivery_association_id,
            )
        reader = self._archives.reader(normalized_id)
        binding: CollectionArtifactProvenanceBindingDocument | None = None
        for value in reader.iter_bindings():
            candidate = CollectionArtifactProvenanceBindingDocument.model_validate(value)
            if candidate.artifact_id == canonical_id:
                binding = candidate
                break
            if candidate.artifact_id > canonical_id:
                break
        if binding is None or (
            binding.journal.journal_id,
            binding.journal.prefix_sha256,
            binding.delivery_association_id,
        ) != projected_binding:
            raise NotFound("archive does not confirm the artifact's canonical binding")
        with read_snapshot(self._session_factory) as session:
            current = _authorized_collection(session, normalized_id, principal)
            require_artifact_scope(session, principal, normalized_id, canonical_id)
            if current.archive_root_sha256 != root_identity:
                raise NotFound("collection archive root changed during provenance read")
        return {
            "collection_id": format_scalar("sequence63", normalized_id),
            "archive_root_sha256": root_identity,
            "artifact": member_payload,
            "binding": binding.model_dump(mode="json"),
        }

    def journal_metadata(
        self, collection_id: int, journal_id: str, *, principal: Principal
    ) -> tuple[int, str]:
        normalized_id = validate_collection_id(collection_id)
        with read_snapshot(self._session_factory) as session:
            _authorized_collection(
                session, normalized_id, principal, permission=PROVENANCE_EXPORT
            )
        return self._archives.reader(normalized_id).journal_metadata(journal_id)

    def list_journals(
        self,
        collection_id: int,
        *,
        page_size: int,
        after_journal_id: ProvenanceJournalId | None,
        principal: Principal,
    ) -> dict[str, Any]:
        normalized_id = validate_collection_id(collection_id)
        if type(page_size) is not int or not 1 <= page_size <= 200:
            raise ValueError("journal page size must be 1 to 200")
        with read_snapshot(self._session_factory) as session:
            collection = _authorized_collection(
                session, normalized_id, principal, permission=PROVENANCE_EXPORT
            )
            root_identity = collection.archive_root_sha256
        headers = self._archives.reader(normalized_id).iter_journal_headers()
        rows: list[dict[str, str]] = []
        for journal_id, byte_count, sha256 in headers:
            if after_journal_id is not None and journal_id <= after_journal_id:
                continue
            rows.append(
                {
                    "journal_id": journal_id,
                    "bytes": format_scalar("nonnegative", byte_count),
                    "sha256": sha256,
                }
            )
            if len(rows) > page_size:
                break
        more = len(rows) > page_size
        selected = rows[:page_size]
        with read_snapshot(self._session_factory) as session:
            current = _authorized_collection(
                session, normalized_id, principal, permission=PROVENANCE_EXPORT
            )
            if current.archive_root_sha256 != root_identity:
                raise NotFound("collection archive root changed during provenance read")
        return {
            "collection_id": format_scalar("sequence63", normalized_id),
            "archive_root_sha256": root_identity,
            "journals": selected,
            "next_journal_id": selected[-1]["journal_id"] if more else None,
        }

    def iter_journal_range(
        self,
        collection_id: int,
        journal_id: str,
        *,
        offset: int = 0,
        size: int | None = None,
        principal: Principal,
    ) -> Iterator[bytes]:
        normalized_id = validate_collection_id(collection_id)
        with read_snapshot(self._session_factory) as session:
            _authorized_collection(
                session, normalized_id, principal, permission=PROVENANCE_EXPORT
            )
        yield from self._archives.reader(normalized_id).iter_journal_range(
            journal_id, offset=offset, size=size
        )


def _authorized_collection(
    session: Session,
    collection_id: int,
    principal: Principal,
    *,
    permission: str = PROVENANCE_READ,
) -> CollectionRecord:
    record = session.get(CollectionRecord, collection_id)
    if (
        record is None
        or not record.is_published
        or not principal.allows_collection(CATALOG_READ, collection_id)
        or not principal.allows_collection(permission, collection_id)
    ):
        raise NotFound(f"collection not found: {collection_id}")
    return record


def _member_row(row: CollectionArtifactRecord) -> dict[str, str]:
    return {
        "artifact_id": row.artifact_id,
        "bytes": format_scalar("nonnegative", row.bytes),
        "sha256": row.sha256,
    }


__all__ = ["SqlAlchemyCanonicalProvenanceService"]
