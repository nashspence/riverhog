from __future__ import annotations

from http_api_contracts import BrowsePageToken
from pydantic import Field, model_validator
from riverhog_protocol import (
    ArtifactMemberIdentityDocument,
    CollectionId,
    SearchSort,
    SortOrder,
)

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
