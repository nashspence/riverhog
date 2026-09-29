"""Canonical Riverhog retrieval request documents."""

from __future__ import annotations

from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from riverhog_protocol.artifact_identity import ArtifactId
from riverhog_protocol.paths import CollectionId
from riverhog_protocol.transport import RETRIEVAL_ARTIFACT_BATCH_MAX


class RetrievalTransportDocument(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class RetrievalArtifactReferenceDocument(RetrievalTransportDocument):
    collection_id: CollectionId
    artifact_id: ArtifactId


class RetrievalArtifactReferenceSetDocument(RetrievalTransportDocument):
    artifacts: list[RetrievalArtifactReferenceDocument] = Field(
        min_length=1,
        max_length=RETRIEVAL_ARTIFACT_BATCH_MAX,
        json_schema_extra={
            "x-riverhog-extent": {
                "policy": "segmented_no_total_max",
                "reason": "bounded-retrieval-work-request",
                "progression": "multiple-retrieval-jobs",
            }
        },
    )

    @model_validator(mode="after")
    def validate_exact_reference_set(self) -> Self:
        identities = [(item.collection_id, item.artifact_id) for item in self.artifacts]
        if len(identities) != len(set(identities)):
            raise ValueError("retrieval artifact references must be unique")
        canonical = sorted(identities)
        if identities != canonical:
            raise ValueError("retrieval artifact references must be in canonical order")
        return self


__all__ = [
    "RetrievalArtifactReferenceDocument",
    "RetrievalArtifactReferenceSetDocument",
]
