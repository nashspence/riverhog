"""Structural member-to-canonical-history references for collection archives."""

from __future__ import annotations

from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator
from riverhog_provenance_contracts import EntryReference, ProvenanceId, ProvenanceJournalId

from riverhog_protocol.artifact_identity import ArtifactId
from riverhog_protocol.exact_scalar import NonnegativeDecimal
from riverhog_protocol.transport import COLLECTION_UPLOAD_ARTIFACT_BATCH_MAX


class JournalAnchorDocument(BaseModel):
    """One exact validated journal prefix, never an implied latest tail."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    journal_id: ProvenanceJournalId
    through: EntryReference
    prefix_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    prefix_bytes: NonnegativeDecimal = Field(ge=1)


class CollectionArtifactProvenanceBindingDocument(BaseModel):
    """A member's exact primary journal and delivery association."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    artifact_id: ArtifactId
    journal: JournalAnchorDocument
    delivery_association_id: ProvenanceId


class CollectionArtifactProvenanceBindingBatchDocument(BaseModel):
    """One bounded, member-ID ordered segment of structural bindings."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    bindings: list[CollectionArtifactProvenanceBindingDocument] = Field(
        min_length=1,
        max_length=COLLECTION_UPLOAD_ARTIFACT_BATCH_MAX,
        json_schema_extra={
            "x-riverhog-extent": {
                "policy": "segmented_no_total_max",
                "reason": "bounded-artifact-provenance-bindings",
                "progression": "artifact-id",
            }
        },
    )

    @model_validator(mode="after")
    def validate_order(self) -> Self:
        identities = tuple(item.artifact_id for item in self.bindings)
        if identities != tuple(sorted(set(identities))):
            raise ValueError("artifact provenance bindings must be strictly ID ordered")
        return self


class ArtifactMaterializationDecisionDocument(BaseModel):
    """Publication policy for one preallocated collection member."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    artifact_id: ArtifactId
    allow_missing_materialization_hint: bool = False


class ArtifactMaterializationDecisionBatchDocument(BaseModel):
    """One bounded, ID-ordered slice of explicit member decisions."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    decisions: list[ArtifactMaterializationDecisionDocument] = Field(
        min_length=1,
        max_length=COLLECTION_UPLOAD_ARTIFACT_BATCH_MAX,
        json_schema_extra={
            "x-riverhog-extent": {
                "policy": "segmented_no_total_max",
                "reason": "bounded-artifact-publication-decisions",
                "progression": "artifact-id",
            }
        },
    )

    @model_validator(mode="after")
    def validate_order(self) -> Self:
        identities = tuple(item.artifact_id for item in self.decisions)
        if identities != tuple(sorted(set(identities))):
            raise ValueError("artifact materialization decisions must be strictly ID ordered")
        return self


__all__ = [
    "ArtifactMaterializationDecisionBatchDocument",
    "ArtifactMaterializationDecisionDocument",
    "CollectionArtifactProvenanceBindingBatchDocument",
    "CollectionArtifactProvenanceBindingDocument",
    "JournalAnchorDocument",
]
