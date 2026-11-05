"""Retain original staged structural publications across bounded scheduling steps."""

from __future__ import annotations

import threading

from riverhog_core.archive_provenance import ArchiveProvenancePublisher
from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.catalog_models import CollectionUploadRecord
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services.archive_provenance_structure_checkpoint import (
    ArchiveProvenanceStructureSource,
)
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission, Guard
from riverhog_core.services.immutable_archive_receiving import drain_original_immutable_report
from riverhog_core.services.immutable_archive_writes import ImmutableArchiveWrites


class ArchiveProvenanceStructureWriter:
    """One construction's retained structure actor, not global quiescence."""

    def __init__(
        self,
        factory: SessionFactory,
        *,
        collection_id: int,
        config: RuntimeConfig,
        archive_stores: ArchiveStoreRegistry,
        forward_guard: Guard,
    ) -> None:
        self._factory = factory
        self._collection_id = collection_id
        self._config = config
        self._archive_stores = archive_stores
        self._forward_guard = forward_guard
        self._lock = threading.Lock()
        self._source: ArchiveProvenanceStructureSource | None = None
        self._actor: ImmutableArchiveWrites | None = None
        self._passphrase: str | None = None
        self._scrypt_log_n: int | None = None

    @property
    def has_pending(self) -> bool:
        return self._actor is not None

    def advance(self, *, selected_object_id: str | None = None) -> bool:
        """Finish an original actor or publish one currently selected structure."""
        with self._lock:
            if self._actor is not None:
                return self._publish_current()
            if drain_original_immutable_report(self._factory, self._collection_id):
                return True
            with session_scope(self._factory) as session:
                source = ArchiveProvenanceStructureSource.select_current(
                    session,
                    self._collection_id,
                    forward_guard=self._forward_guard,
                    grants=self._config.archive_write,
                    selected_object_id=selected_object_id,
                )
                if source is None:
                    return False
                upload = session.get(CollectionUploadRecord, self._collection_id)
                assert upload is not None
                name, incarnation = upload.archive_store, upload.archive_incarnation_id
                passphrase_id = upload.passphrase_id
            binding = self._archive_stores.require_incarnation(name, incarnation)
            ports = binding.require_immutable_writes()
            epoch = ArchiveWriteAdmission(self._factory).bind_epoch(
                self._collection_id, guard=source.guard
            )
            passphrase = self._config.archive_passphrase_for(passphrase_id)
            actor = ImmutableArchiveWrites(
                self._factory,
                source=source,
                epoch=epoch,
                grants=source.write_grants(self._config.archive_write),
                executor=ports,
                observer=ports,
            )
            # Retain every original input before encryption, observation or IO.
            self._source, self._actor = source, actor
            self._passphrase = passphrase
            self._scrypt_log_n = self._config.archive_scrypt_work_factor
            return self._publish_current()

    def _publish_current(self) -> bool:
        assert self._source is not None and self._actor is not None
        received = self._actor.resume_original()
        if received is not None:
            self._source.publication.sealed_receipt(received)
        else:
            assert self._passphrase is not None and self._scrypt_log_n is not None
            ArchiveProvenancePublisher(
                object_store=self._actor,
                passphrase=self._passphrase,
                scrypt_log_n=self._scrypt_log_n,
            ).publish_object(self._source.publication)
        self._source = None
        self._actor = None
        self._passphrase = None
        self._scrypt_log_n = None
        return True
