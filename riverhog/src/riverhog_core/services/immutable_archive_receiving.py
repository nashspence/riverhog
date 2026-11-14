"""Route original closed immutable reports to their explicit source receivers."""

from __future__ import annotations

from threading import Lock

from riverhog_protocol import (
    COLLECTION_DESCRIPTION_DOCUMENT_FORMAT,
    COLLECTION_TAG_HEAD_FORMAT,
    COLLECTION_TAG_NODE_FORMAT,
)
from riverhog_protocol.errors import Conflict, NotFound
from riverhog_storage_adapter_protocol import ImmutableObjectReceipt
from sqlalchemy.orm import Session

from riverhog_core.archive_formats import (
    PROVENANCE_BINDING_SEGMENT_STORAGE_FORMAT,
    PROVENANCE_HISTORY_STORAGE_FORMAT,
    PROVENANCE_JOURNAL_SEGMENT_STORAGE_FORMAT,
    PROVENANCE_RECORD_PAGE_STORAGE_FORMAT,
    PROVENANCE_ROOT_STORAGE_FORMAT,
    PROVENANCE_SOURCE_PROOF_STORAGE_FORMAT,
    PROVENANCE_TERMINAL_STORAGE_FORMAT,
    PROVENANCE_VOLUME_METADATA_STORAGE_FORMAT,
    RECOVERY_DESCRIPTOR_STORAGE_FORMAT,
    ROOT_MANIFEST_STORAGE_FORMAT,
    VOLUME_METADATA_STORAGE_FORMAT,
)
from riverhog_core.catalog_archive_write_models import ArchiveWriteEffectRecord
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.services.archive_authority_checkpoint import (
    ArchiveRecoverySource,
    ArchiveRootSource,
)
from riverhog_core.services.archive_finalizer_terminal import (
    ArchiveFinalizerTerminal,
    TerminalDrainStatus,
)
from riverhog_core.services.archive_initial_description_checkpoint import (
    ArchiveInitialDescriptionSource,
)
from riverhog_core.services.archive_initial_tag_checkpoint import ArchiveInitialTagSource
from riverhog_core.services.archive_provenance_authority_checkpoint import (
    ArchiveProvenanceAuthoritySource,
)
from riverhog_core.services.archive_provenance_journal_checkpoint import (
    ArchiveProvenanceJournalSource,
)
from riverhog_core.services.archive_provenance_structure_checkpoint import (
    ArchiveProvenanceStructureSource,
)
from riverhog_core.services.archive_provenance_volume_checkpoint import (
    ArchiveProvenanceVolumeSource,
)
from riverhog_core.services.archive_volume_metadata_checkpoint import ArchiveVolumeMetadataSource
from riverhog_core.services.immutable_archive_admission import (
    ImmutableArchiveAdmission,
    SettledImmutableArchiveWrite,
)
from riverhog_core.services.immutable_archive_preparation import PreparedImmutableArchiveObject
from riverhog_core.services.immutable_archive_writes import ImmutableArchiveSource


def _source(session: Session, prepared: PreparedImmutableArchiveObject) -> ImmutableArchiveSource:
    format_name = prepared.operation.request.required_identity_assertions.get("riverhog-format")
    if format_name == COLLECTION_TAG_NODE_FORMAT:
        return ArchiveInitialTagSource.from_prepared(session, prepared, role="node")
    if format_name == COLLECTION_TAG_HEAD_FORMAT:
        return ArchiveInitialTagSource.from_prepared(session, prepared, role="head")
    if format_name == COLLECTION_DESCRIPTION_DOCUMENT_FORMAT:
        return ArchiveInitialDescriptionSource.from_prepared(session, prepared)
    if format_name == VOLUME_METADATA_STORAGE_FORMAT:
        return ArchiveVolumeMetadataSource.from_prepared(session, prepared)
    if format_name == ROOT_MANIFEST_STORAGE_FORMAT:
        return ArchiveRootSource.from_prepared(session, prepared)
    if format_name == RECOVERY_DESCRIPTOR_STORAGE_FORMAT:
        return ArchiveRecoverySource.from_prepared(session, prepared)
    if format_name == PROVENANCE_TERMINAL_STORAGE_FORMAT:
        return ArchiveProvenanceAuthoritySource.from_prepared(session, prepared, role="terminal")
    if format_name == PROVENANCE_ROOT_STORAGE_FORMAT:
        return ArchiveProvenanceAuthoritySource.from_prepared(session, prepared, role="root")
    if format_name == PROVENANCE_JOURNAL_SEGMENT_STORAGE_FORMAT:
        return ArchiveProvenanceJournalSource.from_prepared(session, prepared)
    if format_name == PROVENANCE_BINDING_SEGMENT_STORAGE_FORMAT:
        return ArchiveProvenanceVolumeSource.from_prepared(session, prepared, role="bindings")
    if format_name == PROVENANCE_VOLUME_METADATA_STORAGE_FORMAT:
        return ArchiveProvenanceVolumeSource.from_prepared(session, prepared, role="metadata")
    if format_name in {
        PROVENANCE_BINDING_SEGMENT_STORAGE_FORMAT,
        PROVENANCE_VOLUME_METADATA_STORAGE_FORMAT,
        PROVENANCE_HISTORY_STORAGE_FORMAT,
        PROVENANCE_RECORD_PAGE_STORAGE_FORMAT,
        PROVENANCE_SOURCE_PROOF_STORAGE_FORMAT,
    }:
        return ArchiveProvenanceStructureSource.from_prepared(session, prepared)
    raise Conflict("immutable archive report has no source receiver")


def receive_original_receipt(
    session: Session,
    prepared: PreparedImmutableArchiveObject,
    receipt: ImmutableObjectReceipt,
) -> None:
    _source(session, prepared).apply_prepared_receipt(session, prepared, receipt)


def verify_original_receipt(
    session: Session,
    prepared: PreparedImmutableArchiveObject,
    receipt: ImmutableObjectReceipt,
) -> None:
    _source(session, prepared).verify_prepared_receipt(session, prepared, receipt)


def drain_original_immutable_report(factory: SessionFactory, collection_id: int) -> bool:
    """Receive one original closed report, without minting forward execution rights."""
    owner = ImmutableArchiveAdmission(factory)
    settled = owner.load_next_collection_report(collection_id)
    if settled is None:
        return False
    if settled.receipt is None:
        owner.acknowledge_settled(settled)
    else:
        owner.receive_settled(
            settled,
            apply_receipt=receive_original_receipt,
            verify_receipt=verify_original_receipt,
        )
        owner.acknowledge_settled(settled, verify_receipt=verify_original_receipt)
    return True


class OriginalImmutableArchiveReport:
    """Retain one selected closed report through original receiving and retirement.

    Selection is bounded by the existing finite call pool. A missing next report
    says nothing about unknown execution. This owner never invokes a provider,
    grants forward admission, releases a worker, or proves global quiescence.
    """

    def __init__(self, factory: SessionFactory, collection_id: int) -> None:
        self._factory = factory
        self._collection_id = collection_id
        self._admission = ImmutableArchiveAdmission(factory)
        self._selected: SettledImmutableArchiveWrite | None = None
        self._result: bool | None = None
        self._lock = Lock()
        self._terminal = ArchiveFinalizerTerminal(factory, collection_id)

    @property
    def has_pending(self) -> bool:
        return self._selected is not None and self._result is None

    def advance(self) -> bool:
        with self._lock:
            self._terminal.require_forward()
            if self._result is not None:
                return self._result
            if self._selected is None:
                self._selected = self._admission.load_next_collection_report(self._collection_id)
                if self._selected is None:
                    self._result = False
                    return False
            self._receive_original()
            return True

    def _receive_original(self) -> None:
        selected = self._selected
        assert selected is not None
        receipt = selected.receipt
        try:
            if receipt is not None:
                self._admission.receive_settled(
                    selected,
                    apply_receipt=receive_original_receipt,
                    verify_receipt=verify_original_receipt,
                )
            self._admission.acknowledge_settled(
                selected,
                verify_receipt=verify_original_receipt if receipt is not None else None,
            )
        except NotFound:
            # This owner already retained the exact validated closed outcome.
            # Another receiver, or a lost retirement reply, can remove its call.
            # Positive data still needs the original source's durable proof.
            with session_scope(self._factory) as session:
                if session.get(ArchiveWriteEffectRecord, selected.call_id) is not None:
                    raise
                if receipt is not None:
                    verify_original_receipt(session, selected.prepared, receipt)
        self._result = True

    def drain_terminal(self, operation_id: str) -> TerminalDrainStatus:
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            self._terminal.drain_original(operation_id, None)
            if self.has_pending:
                self._receive_original()
            return "closed"
        finally:
            self._lock.release()
