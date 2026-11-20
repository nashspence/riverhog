"""Retained corpus actors and original progress acknowledgements across bounded steps."""

from __future__ import annotations

import threading
from typing import Literal

from riverhog_protocol.errors import Conflict

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
)
from riverhog_core.services.archive_provenance_volume_checkpoint import (
    ArchiveProvenanceVolumeSource,
)
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission, Guard, _writer
from riverhog_core.services.immutable_archive_writes import ImmutableArchiveWrites

_Phase = Literal["bindings", "journal", "metadata", "commit"]
_Source = ArchiveProvenanceVolumeSource | ArchiveProvenanceJournalSource


class ArchiveProvenanceVolumeFinalizer:
    """One construction's original physical/data step, not a quiescence certificate."""

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
        self._volume: ArchiveProvenanceVolumeSource | None = None
        self._source: _Source | None = None
        self._actor: ImmutableArchiveWrites | None = None
        self._phase: _Phase | None = None
        self._passphrase: str | None = None
        self._scrypt_log_n: int | None = None

    @property
    def has_pending(self) -> bool:
        # Original association or progress acknowledgement can remain after
        # physical retirement. This flag accounts only for this retained owner.
        return self._volume is not None or self._reports.has_pending

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
        """Complete one original step, or select one current physical/data step."""
        with self._lock:
            self._terminal.require_forward()
            if self._volume is not None:
                return self._advance_original()
            if self._reports.advance():
                return True
            with session_scope(self._factory) as session:
                volume = ArchiveProvenanceVolumeSource.stage_next(
                    session,
                    self._collection_id,
                    grants=self._config.archive_write,
                    forward_guard=self._forward_guard,
                )
                if volume is None:
                    return False
                phase: _Phase
                source: _Source
                received = None
                if volume.received(session) is not None:
                    phase, source = "commit", volume
                elif volume.received_payload(session) is not None:
                    phase, source = "metadata", volume
                elif volume.document.payload.kind == "bindings":
                    phase = "bindings"
                    source = ArchiveProvenanceVolumeSource.load(
                        session,
                        self._collection_id,
                        volume.sequence,
                        role="bindings",
                        forward_guard=self._forward_guard,
                    )
                else:
                    phase, source = "journal", volume
                upload = session.get(CollectionUploadRecord, self._collection_id)
                assert upload is not None
                name = upload.archive_store
                incarnation = upload.archive_incarnation_id
                passphrase_id = upload.passphrase_id
            if phase == "journal":
                # Each staging owner acquires the SQLite writer before reads;
                # the original volume guard is rechecked in this transaction.
                with session_scope(self._factory) as session:
                    source = volume.stage_journal_payload(
                        session, grants=self._config.archive_write
                    )
                    received = source.received(session)
            if phase == "commit" or received is not None:
                self._volume, self._source, self._phase = volume, source, phase
                return self._advance_original()
            binding = self._archive_stores.require_incarnation(name, incarnation)
            ports = binding.require_immutable_writes()
            epoch = ArchiveWriteAdmission(self._factory).bind_epoch(
                self._collection_id, guard=source.guard
            )
            passphrase = self._config.archive_passphrase_for(passphrase_id)
            available = self._config.archive_write
            if isinstance(source, ArchiveProvenanceJournalSource):
                available = volume.write_grants(available)
            grants = source.write_grants(available)
            actor = ImmutableArchiveWrites(
                self._factory,
                source=source,
                epoch=epoch,
                grants=grants,
                executor=ports,
                observer=ports,
            )
            # Install every original input before encryption, preparation,
            # observation or invocation. Retry cannot consult a new selection.
            self._volume, self._source, self._phase = volume, source, phase
            self._actor = actor
            self._passphrase = passphrase
            self._scrypt_log_n = self._config.archive_scrypt_work_factor
            return self._advance_original()

    def _advance_original(self) -> bool:
        assert self._volume is not None and self._source is not None
        assert self._phase is not None
        if self._phase == "commit":
            with session_scope(self._factory) as session:
                self._volume.commit_volume(session)
        else:
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
            if self._phase == "journal":
                assert isinstance(self._source, ArchiveProvenanceJournalSource)
                with session_scope(self._factory) as session:
                    _writer(session)
                    original = self._source.received(session)
                    if original is None:
                        raise Conflict("original corpus journal physical receipt is unavailable")
                    if self._volume.receive_journal_payload(session) != original:
                        raise Conflict("original corpus journal association proof changed")
        # Clear only after every physical, logical and transaction acknowledgement
        # for this original step. Later steps keep their own current guards.
        self._volume = None
        self._source = None
        self._actor = None
        self._phase = None
        self._passphrase = None
        self._scrypt_log_n = None
        return True
