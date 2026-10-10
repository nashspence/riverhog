"""Resolve one published collection's canonical history from encrypted custody."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass

from riverhog_archive_contracts import ARCHIVE_ROOT_DOCUMENT_BYTES_MAX, read_bounded_history_object
from riverhog_protocol.errors import InvalidState, NotFound
from sqlalchemy import select
from state_schema import read_snapshot

from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.catalog_db import SessionFactory
from riverhog_core.catalog_models import (
    CollectionArchiveObjectRecord,
    CollectionRecord,
)
from riverhog_core.ports.archive_store import ArchiveObjectIdentity
from riverhog_core.ports.download_allowance import DownloadAttribution
from riverhog_core.provenance_archive_read import CanonicalProvenanceArchiveReader
from riverhog_core.provenance_read_cache import ProvenanceReadCache
from riverhog_core.services.archive_records import select_readable_archive_copy

_OBJECT_CACHE_BYTES = 8 * 1024 * 1024
_OBJECT_CACHE_ENTRIES = 4096


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
        *,
        read_order: Sequence[str],
    ) -> None:
        self._session_factory = session_factory
        self._archive_stores = archive_stores
        self._read_order = tuple(read_order)
        # Immutable verified plaintext is reusable across requests. Capacity bounds
        # working memory, not history extent; misses always use normal custody I/O.
        self._objects = ProvenanceReadCache(
            byte_budget=_OBJECT_CACHE_BYTES, entry_budget=_OBJECT_CACHE_ENTRIES
        )
        self._metadata_indexes = ProvenanceReadCache(byte_budget=8 * 1024 * 1024, entry_budget=128)

    def reader(
        self,
        collection_id: int,
        *,
        attribution: DownloadAttribution | None,
        fence: Callable[[], None] | None = None,
    ) -> CanonicalProvenanceArchiveReader:
        selected = self._select_copy(collection_id)
        self._archive_stores.require_incarnation(selected.store_name, selected.incarnation_id)

        def read_object(relative_path: str) -> Iterator[bytes]:
            if fence is not None:
                fence()
            for chunk in self._read_object(selected, relative_path, attribution=attribution):
                if fence is not None:
                    fence()
                yield chunk

        return CanonicalProvenanceArchiveReader(
            read_object,
            expected_root_sha256=selected.provenance_identity,
            archive_generation=selected.archive_generation,
            artifact_set_sha256=selected.artifact_set_identity,
            metadata_cache=self._metadata_indexes,
            metadata_cache_key=selected,
        )

    def archive_root_preimage(
        self, collection_id: int, *, attribution: DownloadAttribution | None
    ) -> bytes:
        selected = self._select_copy(collection_id)
        content = read_bounded_history_object(
            self._read_object(selected, "manifest.json.age", attribution=attribution),
            ARCHIVE_ROOT_DOCUMENT_BYTES_MAX,
        )
        if hashlib.sha256(content).hexdigest() != selected.archive_root_sha256:
            raise InvalidState("archive root preimage differs from its publication")
        return content

    def _read_object(
        self,
        selected: _SelectedCopy,
        relative_path: str,
        *,
        attribution: DownloadAttribution | None,
    ) -> Iterator[bytes]:
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
        key = (
            selected.archive_root_sha256,
            selected.store_name,
            selected.incarnation_id,
            object_identity.sha256,
        )

        def verified_chunks() -> Iterator[bytes]:
            digest = hashlib.sha256()
            received = 0
            for chunk in binding.store.iter_archive_object(
                collection_id=selected.collection_id,
                object=object_identity,
                passphrase_id=selected.passphrase_id,
                attribution=attribution,
            ):
                received += len(chunk)
                if received > object_identity.plaintext_bytes:
                    raise InvalidState("published provenance object exceeds its exact identity")
                digest.update(chunk)
                yield chunk
            if (
                received != object_identity.plaintext_bytes
                or digest.hexdigest() != object_identity.sha256
            ):
                raise InvalidState("published provenance object differs from its exact identity")

        if object_identity.plaintext_bytes > _OBJECT_CACHE_BYTES:
            yield from verified_chunks()
            return
        cached = self._objects.get_or_load(key, lambda: b"".join(verified_chunks()))
        for offset in range(0, len(cached), 128 * 1024):
            yield cached[offset : offset + 128 * 1024]

    def _select_copy(self, collection_id: int) -> _SelectedCopy:
        with read_snapshot(self._session_factory) as session:
            collection = session.get(CollectionRecord, collection_id)
            if collection is None or not collection.is_published:
                raise NotFound(f"collection not found: {collection_id}")
            if collection.archive_root_sha256 is None:
                raise InvalidState("published collection has no exact archive root")
            copy = select_readable_archive_copy(
                session,
                collection_id,
                archive_stores=self._archive_stores,
                read_order=self._read_order,
            )
            if copy.archive_storage_prefix is None:
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
