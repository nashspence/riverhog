from __future__ import annotations

from typing import Literal

from http_api_contracts import BrowsePageToken
from pydantic import Field, model_validator
from riverhog_protocol import (
    ArtifactMemberIdentityDocument,
    CollectionId,
    SearchSort,
    SortOrder,
)
from riverhog_protocol.provenance_transport import JournalAnchorDocument
from riverhog_provenance_contracts import EntryReference, ProvenanceId

from riverhog_api.schemas.common import RiverhogModel


class SearchArtifactOut(ArtifactMemberIdentityDocument):
    artifact_ref: str
    collection_id: CollectionId

    @model_validator(mode="after")
    def validate_artifact_ref(self) -> SearchArtifactOut:
        if self.artifact_ref != f"{self.collection_id}/{self.artifact_id}":
            raise ValueError("artifact_ref must match the exact collection artifact identity")
        return self


class SearchOut(RiverhogModel):
    query: str | None
    collection: CollectionId | None
    page_size: int = Field(ge=1, le=100)
    next_page_token: BrowsePageToken | None
    sort: SearchSort
    order: SortOrder
    artifacts: list[SearchArtifactOut]


class DiscoverySupportOut(RiverhogModel):
    journal_anchor: JournalAnchorDocument
    entry: EntryReference
    assertion_id: ProvenanceId
    referent_id: ProvenanceId
    assertion_kind: str
    assertion_state: str
    relationship: str
    pointer: str
    representation: str
    value_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class DiscoveryArtifactOut(RiverhogModel):
    source_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    collection_id: CollectionId
    archive_root_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    provenance_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    artifact: ArtifactMemberIdentityDocument
    index_generation: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    tag_revision: str
    description_revision: str
    matches: list[DiscoverySupportOut]


class DiscoveryPageOut(RiverhogModel):
    format: Literal["riverhog-artifact-discovery-page/v1"]
    query_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    read_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    artifacts: list[DiscoveryArtifactOut] = Field(
        max_length=200,
        json_schema_extra={
            "x-riverhog-extent": {
                "policy": "segmented_no_total_max",
                "reason": "bounded-stable-artifact-discovery-page",
                "progression": "read-identity-bound-page-token",
            }
        },
    )
    complete: bool
    next_page_token: BrowsePageToken | None
