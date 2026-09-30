"""Exact canonical provenance read documents."""

from __future__ import annotations

from typing import Annotated

from pydantic import Field
from riverhog_protocol import (
    ArtifactId,
    ArtifactMemberIdentityDocument,
    CollectionArtifactProvenanceBindingDocument,
    CollectionId,
    MemberHistoryBindingDocument,
    MemberHistoryDescriptorDocument,
)
from riverhog_provenance_contracts import ProvenanceJournalId

from riverhog_api.schemas.common import RiverhogModel

Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class ListCollectionArtifactProvenanceOut(RiverhogModel):
    collection_id: CollectionId
    archive_root_sha256: Sha256
    artifact_set_identity: Sha256
    provenance_identity: Sha256
    artifacts: list[ArtifactMemberIdentityDocument] = Field(
        max_length=200,
        json_schema_extra={
            "x-riverhog-extent": {
                "policy": "segmented_no_total_max",
                "reason": "bounded-collection-artifact-provenance-page",
                "progression": "archive-root-bound-artifact-id",
            }
        },
    )
    next_artifact_id: ArtifactId | None = None


class CollectionArtifactProvenanceDetailOut(RiverhogModel):
    collection_id: CollectionId
    archive_root_sha256: Sha256
    artifact: ArtifactMemberIdentityDocument
    binding: CollectionArtifactProvenanceBindingDocument
    history_binding: MemberHistoryBindingDocument
    member_history: MemberHistoryDescriptorDocument


class ProvenanceJournalSummaryOut(RiverhogModel):
    journal_id: ProvenanceJournalId
    bytes: Annotated[str, Field(pattern=r"^(0|[1-9][0-9]*)$")]
    sha256: Sha256


class ListCollectionProvenanceJournalsOut(RiverhogModel):
    collection_id: CollectionId
    archive_root_sha256: Sha256
    journals: list[ProvenanceJournalSummaryOut] = Field(
        max_length=200,
        json_schema_extra={
            "x-riverhog-extent": {
                "policy": "segmented_no_total_max",
                "reason": "bounded-collection-journal-page",
                "progression": "archive-root-bound-journal-id",
            }
        },
    )
    next_journal_id: ProvenanceJournalId | None = None


__all__ = [
    "CollectionArtifactProvenanceDetailOut",
    "ListCollectionArtifactProvenanceOut",
    "ListCollectionProvenanceJournalsOut",
]
