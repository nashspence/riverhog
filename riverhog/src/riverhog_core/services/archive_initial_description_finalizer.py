"""Retain original initial description actors and progress acknowledgements."""

from __future__ import annotations

import threading
from typing import Literal

from riverhog_core.archive_collection_metadata import ArchiveCollectionMetadataPublisher
from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.catalog_models import CollectionUploadRecord
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services.archive_initial_description_checkpoint import (
    ArchiveInitialDescriptionSource,
)
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_core.services.immutable_archive_receiving import drain_original_immutable_report
from riverhog_core.services.immutable_archive_writes import ImmutableArchiveWrites


class ArchiveInitialDescriptionFinalizer:
    """One construction's retained authority step, not global quiescence."""

    def __init__(
        self,
        factory: SessionFactory,
        *,
        collection_id: int,
        config: RuntimeConfig,
        archive_stores: ArchiveStoreRegistry,
    ) -> None:
        self._factory = factory
        self._collection_id = collection_id
        self._config = config
        self._archive_stores = archive_stores
        self._lock = threading.Lock()
        self._source: ArchiveInitialDescriptionSource | None = None
        self._phase: Literal["publish", "progress"] | None = None
        self._actor: ImmutableArchiveWrites | None = None
        self._passphrase: str | None = None
        self._scrypt_log_n: int | None = None

    @property
    def has_pending(self) -> bool:
        return self._source is not None

    def advance(self) -> bool:
        """Finish an original step before selecting any fresh authority work."""
        with self._lock:
            if self._source is not None:
                return self._advance_original()
            if drain_original_immutable_report(self._factory, self._collection_id):
                return True
            with session_scope(self._factory) as session:
                source = ArchiveInitialDescriptionSource.stage_next(
                    session, self._collection_id, grants=self._config.archive_write
                )
                if source is None:
                    return False
                received = source.received(session)
                upload = session.get(CollectionUploadRecord, self._collection_id)
                assert upload is not None
                name, incarnation = upload.archive_store, upload.archive_incarnation_id
                passphrase_id = upload.passphrase_id
            if not source.has_publication or received is not None:
                self._source, self._phase = source, "progress"
                return self._advance_original()
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
            # Install every original input before encryption, observation or IO.
            self._source, self._phase, self._actor = source, "publish", actor
            self._passphrase = passphrase
            self._scrypt_log_n = self._config.archive_scrypt_work_factor
            return self._advance_original()

    def _advance_original(self) -> bool:
        assert self._source is not None and self._phase is not None
        if self._phase == "progress":
            with session_scope(self._factory) as session:
                self._source.commit_progress(session)
        else:
            assert self._actor is not None
            received = self._actor.resume_original()
            if received is not None:
                self._source.publication.receipt(received)
            else:
                assert self._passphrase is not None and self._scrypt_log_n is not None
                ArchiveCollectionMetadataPublisher(
                    object_store=self._actor,
                    passphrase=self._passphrase,
                    scrypt_log_n=self._scrypt_log_n,
                ).publish_object(self._source.publication)
        # Keep ownership until physical or logical transaction acknowledgement.
        self._source = None
        self._phase = None
        self._actor = None
        self._passphrase = None
        self._scrypt_log_n = None
        return True
