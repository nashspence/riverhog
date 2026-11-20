"""Retain original root and recovery actors across bounded authority steps."""

from __future__ import annotations

import threading

from riverhog_protocol.errors import Conflict
from sqlalchemy import select

from riverhog_core.archive_recovery_descriptor import ArchiveRecoveryDescriptorPublisher
from riverhog_core.archive_root import ArchiveRootPublisher
from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.catalog_models import CollectionUploadRecord
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services.archive_authority_checkpoint import (
    ArchiveRecoverySource,
    ArchiveRootSource,
)
from riverhog_core.services.archive_finalizer_reports import ArchiveFinalizerReports
from riverhog_core.services.archive_finalizer_terminal import (
    ArchiveFinalizerTerminal,
    TerminalDrainStatus,
)
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission, Guard
from riverhog_core.services.immutable_archive_writes import ImmutableArchiveWrites


class ArchiveAuthorityFinalizer:
    """One construction's original live authority call, not a quiescence certificate."""

    def __init__(
        self,
        factory: SessionFactory,
        *,
        collection_id: int,
        config: RuntimeConfig,
        archive_stores: ArchiveStoreRegistry,
        forward_guard: Guard | None = None,
    ) -> None:
        self._factory = factory
        self._collection_id = collection_id
        self._config = config
        self._archive_stores = archive_stores
        self._forward_guard = forward_guard
        self._lock = threading.Lock()
        self._terminal = ArchiveFinalizerTerminal(factory, collection_id)
        self._reports = ArchiveFinalizerReports(factory, collection_id)
        self._actor: ImmutableArchiveWrites | None = None
        self._source: ArchiveRootSource | ArchiveRecoverySource | None = None
        self._passphrase: str | None = None
        self._scrypt_log_n: int | None = None

    @property
    def has_pending(self) -> bool:
        return self._actor is not None or self._reports.has_pending

    def drain_terminal(self, operation_id: str) -> TerminalDrainStatus:
        """Fence this owner and drain its original child without releasing inputs."""
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            original = self._reports if self._reports.has_pending else self._actor
            return self._terminal.drain_original(operation_id, original)
        finally:
            self._lock.release()

    def advance(self) -> bool:
        """Receive one original closed fact or complete one current authority object."""
        with self._lock:
            self._terminal.require_forward()
            if self._actor is not None:
                return self._publish_current()
            if self._reports.advance():
                return True
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
                if self._forward_guard is not None:
                    self._forward_guard(session)
                # Positive data may already exist while its original execution
                # is unknown. Check closure before interpreting any progress.
                admission.require_no_unresolved_calls(session, self._collection_id)
                source: ArchiveRootSource | ArchiveRecoverySource
                if upload.archive_root_receipt_json is None:
                    if upload.final_authority_json is not None:
                        raise Conflict("archive authority has no original root receiving proof")
                    source = ArchiveRootSource.load(
                        session, self._collection_id, forward_guard=self._forward_guard
                    )
                else:
                    source = ArchiveRecoverySource.load(
                        session, self._collection_id, forward_guard=self._forward_guard
                    )
                    if upload.final_authority_json is not None:
                        if source.received(session) is None:
                            raise Conflict("archive authority receiving proof is unavailable")
                        return False
            root_source = source if isinstance(source, ArchiveRootSource) else source.root_source
            binding = self._archive_stores.require_incarnation(
                root_source.archive_store, root_source.archive_incarnation_id
            )
            ports = binding.require_immutable_writes()
            epoch = admission.bind_epoch(self._collection_id, guard=source.guard)
            passphrase = (
                self._config.archive_passphrase_for(root_source.passphrase_id)
                if isinstance(source, ArchiveRootSource)
                else None
            )
            actor = ImmutableArchiveWrites(
                self._factory,
                source=source,
                epoch=epoch,
                grants=self._config.archive_write,
                executor=ports,
                observer=ports,
            )
            # Install ownership before preparation or provider work. A failure
            # cannot make an original call or its returned data disappear.
            self._source, self._actor, self._passphrase = source, actor, passphrase
            self._scrypt_log_n = self._config.archive_scrypt_work_factor
            return self._publish_current()

    def _publish_current(self) -> bool:
        assert self._actor is not None and self._source is not None
        source = self._source
        received = self._actor.resume_original()
        if received is not None:
            source.publication.sealed_receipt(received)
        elif isinstance(source, ArchiveRootSource):
            assert self._passphrase is not None and self._scrypt_log_n is not None
            ArchiveRootPublisher(
                object_store=self._actor,
                passphrase=self._passphrase,
                scrypt_log_n=self._scrypt_log_n,
            ).publish_root_manifest(
                archive_storage_prefix=source.publication.archive_storage_prefix,
                manifest=source.publication.plaintext,
            )
        else:
            with session_scope(self._factory) as session:
                root = source.root_source.received(session)
                if root is None:
                    raise Conflict("archive recovery original root is unavailable")
            ArchiveRecoveryDescriptorPublisher(object_store=self._actor).publish(
                archive_storage_prefix=source.publication.archive_storage_prefix,
                root=root,
                encryption=source.publication.descriptor.encryption,
            )
        # The shared actor returned only after original receiving and retirement.
        self._actor = None
        self._source = None
        self._passphrase = None
        self._scrypt_log_n = None
        return True
