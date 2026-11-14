"""Retained original journal publication and logical receiving across scheduling steps."""

from __future__ import annotations

import threading

from riverhog_core.archive_provenance import ArchiveProvenancePublisher
from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.catalog_models import CollectionUploadRecord
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services.archive_finalizer_reports import ArchiveFinalizerReports
from riverhog_core.services.archive_finalizer_terminal import (
    ArchiveFinalizerTerminal,
    TerminalDrainStatus,
)
from riverhog_core.services.archive_provenance_journal_checkpoint import (
    ArchiveProvenanceJournalSource,
    ProvenanceJournalRange,
)
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission, Guard
from riverhog_core.services.immutable_archive_writes import ImmutableArchiveWrites


class ArchiveProvenanceJournalWriter:
    """One construction's original actor and selected association, not global quiescence.

    The scheduler retains this component across failures. Each advance first
    handles an original result; the caller then reads source progress before
    selecting another range. A cold owner can receive closed facts, never
    recreate a lost live invocation.
    """

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
        self._terminal = ArchiveFinalizerTerminal(factory, collection_id)
        self._reports = ArchiveFinalizerReports(factory, collection_id)
        self._source: ArchiveProvenanceJournalSource | None = None
        self._selected: ProvenanceJournalRange | None = None
        self._actor: ImmutableArchiveWrites | None = None
        self._passphrase: str | None = None
        self._scrypt_log_n: int | None = None

    @property
    def has_pending(self) -> bool:
        # Logical receiving may still lack its acknowledgement after physical
        # execution has retired or when its receipt came from another position.
        return self._source is not None or self._reports.has_pending

    def drain_terminal(self, operation_id: str) -> TerminalDrainStatus:
        """Fence this owner and drain its original child without releasing inputs."""
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            original = self._reports if self._reports.has_pending else self._actor
            return self._terminal.drain_original(operation_id, original)
        finally:
            self._lock.release()

    def advance(
        self,
        *,
        journal_id: str,
        offset: int,
        size: int,
        expected_range: ProvenanceJournalRange | None = None,
    ) -> bool:
        """Receive one original fact or selected range; false means already received."""
        with self._lock:
            self._terminal.require_forward()
            if self._source is not None:
                return self._publish_current()
            if self._reports.advance():
                return True
            with session_scope(self._factory) as session:
                source = ArchiveProvenanceJournalSource.stage(
                    session,
                    self._collection_id,
                    journal_id=journal_id,
                    offset=offset,
                    size=size,
                    grants=self._config.archive_write,
                    forward_guard=self._forward_guard,
                    expected_range=expected_range,
                )
                selected, _ = ProvenanceJournalRange.capture(
                    session, self._collection_id, journal_id, offset=offset, size=size
                )
                if source.received_association(session, selected) is not None:
                    return False
                received = source.received(session)
                upload = session.get(CollectionUploadRecord, self._collection_id)
                assert upload is not None
                name = upload.archive_store
                incarnation = upload.archive_incarnation_id
                passphrase_id = upload.passphrase_id
            if received is not None:
                self._source, self._selected = source, selected
                return self._publish_current()
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
            # Install all original ownership before encryption, preparation,
            # observation or invocation. New selection cannot displace it.
            self._source, self._selected, self._actor = source, selected, actor
            self._passphrase = passphrase
            self._scrypt_log_n = self._config.archive_scrypt_work_factor
            return self._publish_current()

    def _publish_current(self) -> bool:
        assert self._source is not None and self._selected is not None
        if self._actor is not None:
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
        # Physical retirement is independent of acknowledgement for the exact
        # selected logical association. Keep both through a lost commit reply.
        with session_scope(self._factory) as session:
            self._source.receive_association(session, self._selected)
        self._source = None
        self._selected = None
        self._actor = None
        self._passphrase = None
        self._scrypt_log_n = None
        return True
