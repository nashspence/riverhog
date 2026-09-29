"""Exact canonical provenance read documents."""

from __future__ import annotations

from typing import Annotated

from pydantic import Field
from riverhog_protocol import (
    ArtifactId,
    ArtifactMemberIdentityDocument,
    CollectionArtifactProvenanceBindingDocument,
    CollectionId,
)

from riverhog_api.schemas.common import RiverhogModel

Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class ListCollectionArtifactProvenanceOut(RiverhogModel):
    collection_id: CollectionId
    archive_root_sha256: Sha256
    artifact_set_identity: Sha256
    provenance_identity: Sha256
    artifacts: list[ArtifactMemberIdentityDocument] = Field(max_length=200)
    next_artifact_id: ArtifactId | None = None


class CollectionArtifactProvenanceDetailOut(RiverhogModel):
    collection_id: CollectionId
    archive_root_sha256: Sha256
    artifact: ArtifactMemberIdentityDocument
    binding: CollectionArtifactProvenanceBindingDocument


__all__ = [
    "CollectionArtifactProvenanceDetailOut",
    "ListCollectionArtifactProvenanceOut",
]
