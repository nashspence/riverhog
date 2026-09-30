"""Resolve one published collection's canonical history from encrypted custody."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from dataclasses import dataclass

from riverhog_archive_contracts import ARCHIVE_ROOT_DOCUMENT_BYTES_MAX, read_bounded_history_object
from riverhog_protocol.errors import InvalidState, NotFound
from sqlalchemy import case, select
from state_schema import read_snapshot

from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.catalog_db import SessionFactory
from riverhog_core.catalog_models import (
    CollectionArchiveCopyRecord,
    CollectionArchiveObjectRecord,
    CollectionRecord,
)
from riverhog_core.ports.archive_store import ArchiveObjectIdentity
from riverhog_core.provenance_archive_read import CanonicalProvenanceArchiveReader


@dataclass(frozen=True, slots=True)
class _SelectedCopy:
    collection_id: int
    archive_generation: str
    artifact_set_identity: str
    provenance_identity: str
    archive_root_sha256: str
    passphrase_id: str
    store_name: str
    incarnation_id: str
    storage_prefix: str


class PublishedCanonicalProvenance:
    """Bind the pure archive reader to a current, root-matched storage copy.

    The catalog selects an available copy; it never supplies journal semantics.
    The reader authenticates the selected root and its complete structural chain.
    """

    def __init__(
        self,
        session_factory: SessionFactory,
        archive_stores: ArchiveStoreRegistry,
    ) -> None:
        self._session_factory = session_factory
        self._archive_stores = archive_stores

    def reader(self, collection_id: int) -> CanonicalProvenanceArchiveReader:
        selected = self._select_copy(collection_id)

        def read_object(relative_path: str) -> Iterator[bytes]:
            yield from self._read_object(selected, relative_path)

        return CanonicalProvenanceArchiveReader(
            read_object,
            expected_root_sha256=selected.provenance_identity,
            archive_generation=selected.archive_generation,
            artifact_set_sha256=selected.artifact_set_identity,
        )

    def archive_root_preimage(self, collection_id: int) -> bytes:
        selected = self._select_copy(collection_id)
        content = read_bounded_history_object(
            self._read_object(selected, "manifest.json.age"), ARCHIVE_ROOT_DOCUMENT_BYTES_MAX
        )
        if hashlib.sha256(content).hexdigest() != selected.archive_root_sha256:
            raise InvalidState("archive root preimage differs from its publication")
        return content

    def _read_object(self, selected: _SelectedCopy, relative_path: str) -> Iterator[bytes]:
        binding = self._archive_stores.require_incarnation(
            selected.store_name, selected.incarnation_id
        )
        expected_path = f"{selected.storage_prefix}/{relative_path}"
        with read_snapshot(self._session_factory) as session:
            row = session.scalar(
                select(CollectionArchiveObjectRecord).where(
                    CollectionArchiveObjectRecord.collection_id == selected.collection_id,
                    CollectionArchiveObjectRecord.store == selected.store_name,
                    CollectionArchiveObjectRecord.object_path == expected_path,
                )
            )
            if row is None:
                raise InvalidState(f"published provenance object is missing: {relative_path}")
            object_identity = ArchiveObjectIdentity(
                object_id=row.object_id,
                kind=row.kind,
                object_path=row.object_path,
                plaintext_bytes=row.plaintext_bytes,
                stored_bytes=row.stored_bytes,
                sha256=row.sha256,
                stored_sha256=row.stored_sha256,
                revision=row.revision,
            )
        yield from binding.store.iter_archive_object(
            collection_id=selected.collection_id,
            object=object_identity,
            passphrase_id=selected.passphrase_id,
        )

    def _select_copy(self, collection_id: int) -> _SelectedCopy:
        with read_snapshot(self._session_factory) as session:
            collection = session.get(CollectionRecord, collection_id)
            if collection is None or not collection.is_published:
                raise NotFound(f"collection not found: {collection_id}")
            if collection.archive_root_sha256 is None:
                raise InvalidState("published collection has no exact archive root")
            copy = session.scalar(
                select(CollectionArchiveCopyRecord)
                .where(
                    CollectionArchiveCopyRecord.collection_id == collection_id,
                    CollectionArchiveCopyRecord.state == "uploaded",
                    CollectionArchiveCopyRecord.archive_storage_prefix.is_not(None),
                )
                .order_by(
                    case(
                        (
                            CollectionArchiveCopyRecord.store == collection.creation_archive_store,
                            0,
                        ),
                        else_=1,
                    ),
                    CollectionArchiveCopyRecord.store,
                )
                .limit(1)
            )
            if copy is None or copy.archive_storage_prefix is None:
                raise InvalidState("collection has no available exact archive copy")
            return _SelectedCopy(
                collection_id=collection_id,
                archive_generation=collection.archive_generation,
                artifact_set_identity=collection.artifact_set_identity,
                provenance_identity=collection.provenance_identity,
                archive_root_sha256=collection.archive_root_sha256,
                passphrase_id=collection.passphrase_id,
                store_name=copy.store,
                incarnation_id=copy.incarnation_id,
                storage_prefix=copy.archive_storage_prefix.strip("/"),
            )


__all__ = ["PublishedCanonicalProvenance"]
