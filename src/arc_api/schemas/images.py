from __future__ import annotations

from typing import Literal

from pydantic import Field

from arc_api.schemas.common import ArcModel


class FinalizedImageSummaryResponse(ArcModel):
    id: str
    filename: str
    finalized_at: str
    bytes: int
    fill: float
    files: int
    collections: int
    collection_ids: list[str]
    iso_ready: Literal[True] = True
    copy_count: int


class ListImagesResponse(ArcModel):
    page: int
    per_page: int
    total: int
    pages: int
    sort: Literal["finalized_at", "bytes", "copy_count"]
    order: Literal["asc", "desc"]
    images: list[FinalizedImageSummaryResponse]


class RegisterCopyRequest(ArcModel):
    id: str
    location: str


class CopyOut(ArcModel):
    id: str
    volume_id: str
    location: str
    created_at: str


class RegisterCopyResponse(ArcModel):
    copy_: CopyOut = Field(alias="copy")
