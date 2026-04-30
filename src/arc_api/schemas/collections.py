from __future__ import annotations

from typing import Literal

from pydantic import ConfigDict

from arc_api.schemas.archive import CollectionArchiveManifestOut, GlacierArchiveOut
from arc_api.schemas.common import ArcModel
from arc_api.schemas.images import CopyOut


class CollectionUploadFileIn(ArcModel):
    path: str
    bytes: int
    sha256: str


class CreateOrResumeCollectionUploadRequest(ArcModel):
    collection_id: str
    files: list[CollectionUploadFileIn]
    ingest_source: str | None = None


class CollectionSummaryOut(ArcModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    files: int
    bytes: int
    hot_bytes: int
    archived_bytes: int
    pending_bytes: int
    glacier: GlacierArchiveOut | None = None
    archive_manifest: CollectionArchiveManifestOut | None = None
    archive_format: str | None = None
    compression: str | None = None
    disc_coverage: CollectionDiscCoverageOut | None = None
    protection_state: str
    protected_bytes: int
    image_coverage: list[CollectionCoverageImageOut]


class CollectionCoverageImageOut(ArcModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    filename: str
    physical_protection_state: (
        Literal["unprotected", "partially_protected", "protected"] | None
    ) = None
    physical_copies_required: int
    physical_copies_registered: int
    physical_copies_verified: int
    physical_copies_missing: int
    covered_paths: list[str]
    copies: list[CopyOut]


class CollectionDiscCoverageOut(ArcModel):
    state: Literal["none", "partial", "full"]
    covered_bytes: int = 0
    verified_physical_bytes: int = 0


class ListCollectionsResponse(ArcModel):
    page: int
    per_page: int
    total: int
    pages: int
    collections: list[CollectionSummaryOut]


CollectionSummaryOut.model_rebuild()


class CollectionUploadFileOut(ArcModel):
    path: str
    bytes: int
    sha256: str
    upload_state: str
    uploaded_bytes: int
    upload_state_expires_at: str | None


class CollectionUploadSessionOut(ArcModel):
    collection_id: str
    ingest_source: str | None
    state: Literal["uploading", "archiving", "finalized", "failed"]
    files_total: int
    files_pending: int
    files_partial: int
    files_uploaded: int
    bytes_total: int
    uploaded_bytes: int
    missing_bytes: int
    upload_state_expires_at: str | None
    latest_failure: str | None = None
    files: list[CollectionUploadFileOut]
    collection: CollectionSummaryOut | None


class CollectionFileUploadSessionOut(ArcModel):
    path: str
    protocol: str
    upload_url: str
    offset: int
    length: int
    checksum_algorithm: str
    expires_at: str | None
