"""Canonical provenance reads over published collection members and archive roots."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from uuid import uuid4

from riverhog_archive_contracts import (
    RETAINED_HISTORY_EXTENT,
    MemberHistoryBinding,
    SourceMemberHistoryBindingProof,
    provenance_structure_object_path,
)
from riverhog_canonical_json import format_scalar
from riverhog_protocol import ArtifactId, CollectionArtifactProvenanceBindingDocument
from riverhog_protocol.collection_production_provenance import COLLECTION_MEMBER_ROLE
from riverhog_protocol.errors import InvalidState, NotFound, PreconditionFailed
from riverhog_protocol.paths import validate_collection_id
from riverhog_provenance import MemberHistoryClosure
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
from riverhog_core.artifact_access import (
    artifact_scope_filter,
    require_artifact_scope,
    require_current_artifact_capability,
)
from riverhog_core.canonical_discovery_rebuild import rebuild_canonical_index
from riverhog_core.canonical_provenance_archive import PublishedCanonicalProvenance
from riverhog_core.catalog_db import SessionFactory, make_session_factory
from riverhog_core.catalog_models import (
    CatalogSyncStateRecord,
    CollectionArtifactProvenanceRecord,
    CollectionArtifactRecord,
    CollectionRecord,
)
from riverhog_core.collection_access import require_collection_access
from riverhog_core.ports.download_allowance import DownloadAttribution
from riverhog_core.provenance_archive_read import CanonicalProvenanceArchiveReader
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services.app_keys import require_current_principal


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
        self._archives = PublishedCanonicalProvenance(
            self._session_factory,
            archive_stores,
            read_order=config.archive_read_order,
        )

    def rebuild_index(self, collection_id: int) -> str:
        return rebuild_canonical_index(
            self._session_factory, self._archives, validate_collection_id(collection_id)
        )

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
            statement = _artifact_list_statement(
                normalized_id, after_artifact_id=after_artifact_id, principal=principal
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
            root_identity = _archive_root(collection)
            projected_binding = (
                projection.journal_id,
                projection.prefix_sha256,
                projection.delivery_association_id,
            )
            projected_history = (projection.history_sha256, projection.history_bytes)
        with self._cached_reader(normalized_id, principal, permission=PROVENANCE_READ) as reader:
            final_binding: MemberHistoryBinding | None = None
            for value in reader.iter_bindings():
                candidate = MemberHistoryBinding.from_mapping(value)
                if candidate.artifact_id == canonical_id:
                    final_binding = candidate
                    break
                if candidate.artifact_id > canonical_id:
                    break
            if (
                final_binding is None
                or (final_binding.history_sha256, final_binding.history_bytes) != projected_history
                or (final_binding.artifact_id, str(final_binding.bytes), final_binding.sha256)
                != (
                    member_payload["artifact_id"],
                    member_payload["bytes"],
                    member_payload["sha256"],
                )
            ):
                raise NotFound("archive does not confirm the artifact's member history binding")
            history = reader.member_history(final_binding)
            binding = CollectionArtifactProvenanceBindingDocument.model_validate(
                {
                    "artifact_id": history.artifact_id,
                    "journal": history.primary.journal.to_mapping(),
                    "delivery_association_id": history.primary.delivery_association_id,
                }
            )
            if (
                binding.journal.journal_id,
                binding.journal.prefix_sha256,
                binding.delivery_association_id,
            ) != projected_binding:
                raise NotFound("archive does not confirm the artifact's primary binding")
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
            "history_binding": final_binding.to_mapping(),
            "member_history": history.to_mapping(),
        }

    def get_structure_object(
        self, collection_id: int, object_id: str, *, expected_root: str, principal: Principal
    ) -> bytes:
        normalized_id = validate_collection_id(collection_id)
        self._require_export_root(normalized_id, expected_root, principal)
        with self._cached_reader(normalized_id, principal) as reader:
            if principal.has_artifact_scope:
                with self._scope_closure(normalized_id, principal, reader) as closure:
                    if not closure.contains_structure_object(
                        provenance_structure_object_path(object_id)
                    ):
                        raise NotFound("history structure is outside the member selection")
            content = reader.structure_object(object_id)
        self._require_export_root(normalized_id, expected_root, principal)
        return content

    def get_history_binding_proof(
        self,
        collection_id: int,
        artifact_id: ArtifactId,
        *,
        expected_root: str,
        principal: Principal,
    ) -> bytes:
        normalized_id = validate_collection_id(collection_id)
        self._require_export_root(normalized_id, expected_root, principal, artifact_id=artifact_id)
        detail = self.get_artifact(normalized_id, artifact_id, principal=principal)
        binding = MemberHistoryBinding.from_mapping(detail["history_binding"])
        with self._cached_reader(normalized_id, principal) as reader:
            tree = reader.binding_inclusion(artifact_id)
            if tree.target_index is None:
                raise InvalidState("root-authenticated member binding is absent")
            with read_snapshot(self._session_factory) as session:
                state = session.get(CatalogSyncStateRecord, 1)
                if state is None:
                    raise InvalidState("catalog source identity is absent")
                source_identity = state.source_identity
            proof = SourceMemberHistoryBindingProof(
                source_identity=source_identity,
                collection_id=normalized_id,
                archive_root=self._archives.archive_root_preimage(
                    normalized_id,
                    attribution=_download_attribution(principal),
                ),
                provenance_root=reader.scan().root.to_json_bytes(),
                binding=binding,
                index=tree.target_index,
                siblings=tree.target_siblings,
            )
        self._require_export_root(normalized_id, expected_root, principal, artifact_id=artifact_id)
        return proof.to_json_bytes()

    def _require_export_root(
        self,
        collection_id: int,
        expected_root: str,
        principal: Principal,
        *,
        artifact_id: ArtifactId | None = None,
        permission: str = PROVENANCE_EXPORT,
    ) -> None:
        with read_snapshot(self._session_factory) as session:
            collection = _authorized_collection(
                session, collection_id, principal, permission=permission
            )
            if artifact_id is not None:
                require_artifact_scope(session, principal, collection_id, artifact_id)
            if collection.archive_root_sha256 != expected_root:
                raise PreconditionFailed("collection archive root changed")

    def journal_metadata(
        self, collection_id: int, journal_id: str, *, principal: Principal
    ) -> tuple[int, str]:
        normalized_id = validate_collection_id(collection_id)
        with read_snapshot(self._session_factory) as session:
            root = _archive_root(
                _authorized_collection(
                    session, normalized_id, principal, permission=PROVENANCE_EXPORT
                )
            )
        with self._cached_reader(normalized_id, principal) as reader:
            if principal.has_artifact_scope:
                with self._scope_closure(normalized_id, principal, reader) as closure:
                    result = next(
                        (
                            (anchor.prefix_bytes, anchor.prefix_sha256)
                            for anchor in closure.journal_anchors()
                            if anchor.journal_id == journal_id
                        ),
                        None,
                    )
                    if result is None:
                        raise NotFound("journal is outside the member selection")
            else:
                result = reader.journal_metadata(journal_id)
        self._require_export_root(normalized_id, root, principal)
        return result

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
            root_identity = _archive_root(collection)
        with (
            self._cached_reader(normalized_id, principal) as reader,
            self._scope_closure(normalized_id, principal, reader) as closure,
        ):
            headers = (
                (
                    (anchor.journal_id, anchor.prefix_bytes, anchor.prefix_sha256)
                    for anchor in closure.journal_anchors()
                )
                if principal.has_artifact_scope
                else reader.iter_journal_headers()
            )
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
            root = _archive_root(
                _authorized_collection(
                    session, normalized_id, principal, permission=PROVENANCE_EXPORT
                )
            )
        with self._cached_reader(normalized_id, principal) as reader:
            if principal.has_artifact_scope:
                with self._scope_closure(normalized_id, principal, reader) as closure:
                    anchor = next(
                        (
                            item
                            for item in closure.journal_anchors()
                            if item.journal_id == journal_id
                        ),
                        None,
                    )
                    if anchor is None:
                        raise NotFound("journal is outside the member selection")
                    if type(offset) is not int or offset < 0 or offset > anchor.prefix_bytes:
                        raise NotFound("journal range is outside the member selection")
                    if size is None:
                        size = anchor.prefix_bytes - offset
                    if type(size) is not int or size < 0 or offset + size > anchor.prefix_bytes:
                        raise NotFound("journal range is outside the member selection")
            for chunk in reader.iter_journal_range(journal_id, offset=offset, size=size):
                self._require_export_root(normalized_id, root, principal)
                yield chunk
            self._require_export_root(normalized_id, root, principal)

    @contextmanager
    def _cached_reader(
        self,
        collection_id: int,
        principal: Principal,
        *,
        permission: str = PROVENANCE_EXPORT,
    ) -> Iterator[CanonicalProvenanceArchiveReader]:
        """Reuse exact verified object bytes only for this read operation."""
        with read_snapshot(self._session_factory) as session:
            root = _archive_root(
                _authorized_collection(session, collection_id, principal, permission=permission)
            )

        def fence() -> None:
            self._require_export_root(collection_id, root, principal, permission=permission)

        with self._archives.reader(
            collection_id,
            attribution=_download_attribution(principal),
            fence=fence,
        ).cached() as reader:
            with reader.prepared():
                fence()
                yield reader
                fence()

    @contextmanager
    def _scope_closure(
        self,
        collection_id: int,
        principal: Principal,
        reader: CanonicalProvenanceArchiveReader,
    ) -> Iterator[MemberHistoryClosure]:
        """Exact retained member closure; each imported extent remains independently sealed."""
        with MemberHistoryClosure(
            reader.history_store(),
            lambda journal_id, end: reader.iter_journal_range(journal_id, size=end),
            member_role=COLLECTION_MEMBER_ROLE,
        ) as closure:
            if principal.has_artifact_scope:
                allowed = iter(self._scoped_artifacts(collection_id, principal))
                wanted = next(allowed, None)
                for raw in reader.iter_bindings():
                    if wanted is None:
                        break
                    selected = MemberHistoryBinding.from_mapping(raw)
                    if selected.artifact_id < wanted:
                        continue
                    if selected.artifact_id != wanted:
                        raise NotFound("archive does not confirm the selected member")
                    closure.resolve(selected, extent=RETAINED_HISTORY_EXTENT)
                    wanted = next(allowed, None)
                if wanted is not None:
                    raise NotFound("archive does not confirm the selected member")
            yield closure

    def _scoped_artifacts(self, collection_id: int, principal: Principal) -> Iterator[str]:
        after = None
        while True:
            with read_snapshot(self._session_factory) as session:
                _authorized_collection(
                    session, collection_id, principal, permission=PROVENANCE_EXPORT
                )
                rows = list(
                    session.scalars(
                        _artifact_list_statement(
                            collection_id,
                            after_artifact_id=after,
                            principal=principal,
                        ).limit(100)
                    )
                )
                ids = [row.artifact_id for row in rows]
            yield from ids
            if len(ids) < 100:
                return
            after = ids[-1]


def _authorized_collection(
    session: Session,
    collection_id: int,
    principal: Principal,
    *,
    permission: str = PROVENANCE_READ,
) -> CollectionRecord:
    require_current_principal(session, principal)
    require_current_artifact_capability(session, principal)
    record = session.get(CollectionRecord, collection_id)
    if (
        record is None
        or not record.is_published
        or not principal.allows(CATALOG_READ)
        or not principal.allows(permission)
    ):
        raise NotFound(f"collection not found: {collection_id}")
    require_collection_access(session, principal, CATALOG_READ, collection_id)
    require_collection_access(session, principal, permission, collection_id)
    return record


def _download_attribution(principal: Principal) -> DownloadAttribution | None:
    return (
        DownloadAttribution(key_id=principal.key_id, job_id="provenance-" + uuid4().hex)
        if principal.key_id is not None
        else None
    )


def _archive_root(collection: CollectionRecord) -> str:
    if collection.archive_root_sha256 is None:
        raise InvalidState("published collection has no archive root")
    return collection.archive_root_sha256


def _member_row(row: CollectionArtifactRecord) -> dict[str, str]:
    return {
        "artifact_id": row.artifact_id,
        "bytes": format_scalar("nonnegative", row.bytes),
        "sha256": row.sha256,
    }


__all__ = ["SqlAlchemyCanonicalProvenanceService"]


def _artifact_list_statement(
    collection_id: int, *, after_artifact_id: ArtifactId | None, principal: Principal | None
) -> Any:
    statement = select(CollectionArtifactRecord).where(
        CollectionArtifactRecord.collection_id == collection_id,
        artifact_scope_filter(
            CollectionArtifactRecord.collection_id, CollectionArtifactRecord.artifact_id, principal
        ),
    )
    if after_artifact_id is not None:
        statement = statement.where(CollectionArtifactRecord.artifact_id > str(after_artifact_id))
    return statement.order_by(CollectionArtifactRecord.artifact_id)
