"""Bounded collection metadata steps with retained original live actor ownership."""

from __future__ import annotations

import threading

from riverhog_protocol.errors import Conflict
from sqlalchemy import select

from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.catalog_models import CollectionUploadRecord
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services.archive_volume_metadata_checkpoint import ArchiveVolumeMetadataSource
from riverhog_core.services.archive_volume_metadata_writes import ArchiveVolumeMetadataWrites
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_core.services.immutable_archive_receiving import drain_original_immutable_report


class ArchiveVolumeMetadataFinalizer:
    """One collection's source selection and live actor across resumable steps.

    The scheduler retains this owner while an original live actor has pending
    work. A fresh owner can drain closed facts, but cannot redispatch unknown work.
    """

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
        self._actor: ArchiveVolumeMetadataWrites | None = None
        self._source: ArchiveVolumeMetadataSource | None = None
        self._passphrase: str | None = None

    @property
    def has_pending(self) -> bool:
        return self._actor is not None

    def advance(self) -> bool:
        """Complete one original result or publish one current metadata document."""
        with self._lock:
            # Source progress may already have advanced after a lost reply.
            # Retain and retry this exact actor before selecting another source.
            if self._actor is not None:
                return self._publish_current()
            if drain_original_immutable_report(self._factory, self._collection_id):
                return True
            # A positive observation is not closure. Actor loss cannot turn an
            # advanced source pointer into permission for the next writer.
            admission = ArchiveWriteAdmission(self._factory)
            with session_scope(self._factory) as session:
                upload = session.scalar(
                    select(CollectionUploadRecord)
                    .where(CollectionUploadRecord.collection_id == self._collection_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
                if upload is None:
                    return False
                admission.require_no_unresolved_calls(session, self._collection_id)
                if upload.archive_tree_sha256 is None:
                    return False
                if upload.archive_terminal_receipt_json is not None:
                    if upload.archive_ordered_volume_sha256 is None:
                        raise Conflict("archive terminal metadata has no ordered commitment")
                    return False
                source = ArchiveVolumeMetadataSource.load(
                    session, self._collection_id, upload.archive_volume_next_sequence
                )
            binding = self._archive_stores.require_incarnation(
                source.archive_store, source.archive_incarnation_id
            )
            ports = binding.require_immutable_writes()
            epoch = admission.bind_epoch(self._collection_id, guard=source.guard)
            passphrase = self._config.archive_passphrase_for(source.passphrase_id)
            actor = ArchiveVolumeMetadataWrites(
                self._factory,
                source=source,
                epoch=epoch,
                grants=self._config.archive_write,
                executor=ports,
                observer=ports,
            )
            # Install ownership before encryption, preparation or provider work.
            self._source, self._actor, self._passphrase = source, actor, passphrase
            return self._publish_current()

    def _publish_current(self) -> bool:
        assert self._actor is not None and self._source is not None
        assert self._passphrase is not None
        self._actor.publish(
            passphrase=self._passphrase,
            scrypt_log_n=self._config.archive_scrypt_work_factor,
        )
        # publish returned only after receiving and retirement acknowledgement.
        self._actor = None
        self._source = None
        self._passphrase = None
        return True
