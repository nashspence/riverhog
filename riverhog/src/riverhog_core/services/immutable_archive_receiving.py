"""Route original closed immutable reports to their explicit source receivers."""

from __future__ import annotations

from riverhog_protocol.errors import Conflict
from riverhog_storage_adapter_protocol import ImmutableObjectReceipt
from sqlalchemy.orm import Session

from riverhog_core.archive_formats import (
    RECOVERY_DESCRIPTOR_STORAGE_FORMAT,
    ROOT_MANIFEST_STORAGE_FORMAT,
    VOLUME_METADATA_STORAGE_FORMAT,
)
from riverhog_core.catalog_db import SessionFactory
from riverhog_core.services.archive_authority_checkpoint import (
    ArchiveRecoverySource,
    ArchiveRootSource,
)
from riverhog_core.services.archive_volume_metadata_checkpoint import ArchiveVolumeMetadataSource
from riverhog_core.services.immutable_archive_admission import ImmutableArchiveAdmission
from riverhog_core.services.immutable_archive_preparation import PreparedImmutableArchiveObject
from riverhog_core.services.immutable_archive_writes import ImmutableArchiveSource


def _source(session: Session, prepared: PreparedImmutableArchiveObject) -> ImmutableArchiveSource:
    format_name = prepared.operation.request.required_identity_assertions.get("riverhog-format")
    if format_name == VOLUME_METADATA_STORAGE_FORMAT:
        return ArchiveVolumeMetadataSource.from_prepared(session, prepared)
    if format_name == ROOT_MANIFEST_STORAGE_FORMAT:
        return ArchiveRootSource.from_prepared(session, prepared)
    if format_name == RECOVERY_DESCRIPTOR_STORAGE_FORMAT:
        return ArchiveRecoverySource.from_prepared(session, prepared)
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
