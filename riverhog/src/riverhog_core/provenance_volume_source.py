"""Shared corpus source selection, document construction and ordered commitments."""

from __future__ import annotations

import hashlib
import json
from typing import Literal

from riverhog_archive_contracts import (
    PROVENANCE_SEQUENCE_DOMAIN,
    MemberHistoryBinding,
    ProvenancePayload,
    ProvenanceVolumeDocument,
)
from riverhog_protocol.errors import Conflict
from sqlalchemy import select
from sqlalchemy.orm import Session

from riverhog_core.catalog_models import (
    CollectionUploadMemberHistoryRecord,
    CollectionUploadProvenanceJournalRecord,
    CollectionUploadRecord,
)
from riverhog_core.checkpoint_sha256 import CheckpointSHA256


def member_history_binding_row(row: CollectionUploadMemberHistoryRecord) -> MemberHistoryBinding:
    return MemberHistoryBinding.from_mapping(json.loads(row.binding_json))


def provenance_volume_document(
    *,
    archive_generation: str,
    artifact_set_sha256: str,
    sequence: int,
    payload: bytes,
    first_artifact_id: str | None = None,
    last_artifact_id: str | None = None,
    binding_count: int | None = None,
    journal: CollectionUploadProvenanceJournalRecord | None = None,
    journal_offset: int | None = None,
) -> ProvenanceVolumeDocument:
    kind: Literal["bindings", "journal"] = "journal" if journal is not None else "bindings"
    identity = ProvenancePayload(
        kind=kind,
        sequence=sequence,
        bytes=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
    )
    if journal is None:
        return ProvenanceVolumeDocument(
            archive_generation=archive_generation,
            artifact_set_sha256=artifact_set_sha256,
            sequence=sequence,
            payload=identity,
            first_artifact_id=first_artifact_id,
            last_artifact_id=last_artifact_id,
            binding_count=binding_count,
        )
    return ProvenanceVolumeDocument(
        archive_generation=archive_generation,
        artifact_set_sha256=artifact_set_sha256,
        sequence=sequence,
        payload=identity,
        journal_id=journal.journal_id,
        journal_offset=journal_offset,
        journal_bytes=journal.bytes,
        journal_sha256=journal.sha256,
    )


def provenance_commitment(upload: CollectionUploadRecord) -> CheckpointSHA256:
    if upload.provenance_archive_hash_state is not None:
        return CheckpointSHA256.from_state(upload.provenance_archive_hash_state)
    if upload.provenance_archive_next_sequence != 0:
        raise RuntimeError("provenance volume commitment state is missing")
    digest = CheckpointSHA256()
    digest.update(PROVENANCE_SEQUENCE_DOMAIN)
    return digest


def next_provenance_publication_journal(
    session: Session,
    upload: CollectionUploadRecord,
) -> CollectionUploadProvenanceJournalRecord | None:
    if upload.provenance_archive_current_journal_id is not None:
        journal = session.get(
            CollectionUploadProvenanceJournalRecord,
            (upload.collection_id, upload.provenance_archive_current_journal_id),
        )
    else:
        statement = select(CollectionUploadProvenanceJournalRecord).where(
            CollectionUploadProvenanceJournalRecord.collection_id == upload.collection_id
        )
        if upload.provenance_archive_last_journal_id is not None:
            statement = statement.where(
                CollectionUploadProvenanceJournalRecord.journal_id
                > upload.provenance_archive_last_journal_id
            )
        journal = session.scalar(
            statement.order_by(CollectionUploadProvenanceJournalRecord.journal_id).limit(1)
        )
    if journal is not None and journal.state != "sealed":
        raise Conflict(f"provenance journal is not sealed: {journal.journal_id}")
    return journal
