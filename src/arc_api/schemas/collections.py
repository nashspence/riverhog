from __future__ import annotations

from arc_api.schemas.common import ArcModel


class CollectionUploadFileIn(ArcModel):
    path: str
    bytes: int
    sha256: str


class CreateOrResumeCollectionUploadRequest(ArcModel):
    collection_id: str
    files: list[CollectionUploadFileIn]
    ingest_source: str | None = None


class CollectionSummaryOut(ArcModel):
    id: str
    files: int
    bytes: int
    hot_bytes: int
    archived_bytes: int
    pending_bytes: int


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
    state: str
    files_total: int
    files_pending: int
    files_partial: int
    files_uploaded: int
    bytes_total: int
    uploaded_bytes: int
    missing_bytes: int
    upload_state_expires_at: str | None
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
