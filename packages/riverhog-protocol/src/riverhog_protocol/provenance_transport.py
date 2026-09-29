"""Structural member-to-canonical-history references for collection archives."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator
from riverhog_provenance_contracts import (
    PROFILE,
    ContractCatalog,
    EntryReference,
    ProvenanceId,
    ProvenanceJournalId,
)

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


def validate_archive_binding_page(
    bindings: Sequence[CollectionArtifactProvenanceBindingDocument | Mapping[str, Any]],
    *,
    max_members: int,
) -> tuple[CollectionArtifactProvenanceBindingDocument, ...]:
    """Validate an archive page independently of the smaller HTTP upload batch."""

    if type(max_members) is not int or max_members < 1:
        raise ValueError("archive binding page limit is invalid")
    if not 1 <= len(bindings) <= max_members:
        raise ValueError("archive binding page member count is invalid")
    rows = tuple(
        CollectionArtifactProvenanceBindingDocument.model_validate(row) for row in bindings
    )
    ids = tuple(row.artifact_id for row in rows)
    if ids != tuple(sorted(set(ids))):
        raise ValueError("archive binding page must be strictly artifact-ID ordered")
    return rows


class MaterializationHintDocument(BaseModel):
    """Exact core Occurrence advice supplied by the producing authority."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    components: list[str]

    @model_validator(mode="after")
    def validate_core_hint(self) -> Self:
        ContractCatalog().validate(
            PROFILE + "/materialization-hint.schema.json", self.model_dump(mode="json")
        )
        return self


class ArtifactMaterializationDecisionDocument(BaseModel):
    """Publication policy for one preallocated collection member."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    artifact_id: ArtifactId
    materialization_hint: MaterializationHintDocument | None = None
    allow_missing_materialization_hint: bool = False

    @model_validator(mode="after")
    def validate_choice(self) -> Self:
        if (self.materialization_hint is None) != self.allow_missing_materialization_hint:
            raise ValueError("exactly one materialization hint or explicit omission is required")
        return self


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
    "MaterializationHintDocument",
    "CollectionArtifactProvenanceBindingBatchDocument",
    "CollectionArtifactProvenanceBindingDocument",
    "JournalAnchorDocument",
    "validate_archive_binding_page",
]
