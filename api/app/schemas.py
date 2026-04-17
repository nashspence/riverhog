from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class JobCreateRequest(BaseModel):
    description: str | None = None


class JobCreateResponse(BaseModel):
    job_id: str
    status: str


class JobDirectoryCreateRequest(BaseModel):
    relative_path: str


class UploadSlotCreateRequest(BaseModel):
    relative_path: str
    size_bytes: int = Field(gt=0)
    sha256: str | None = Field(default=None, min_length=64, max_length=64)
    mode: str = Field(default="0644")
    mtime: str
    uid: int | None = Field(default=None, ge=0)
    gid: int | None = Field(default=None, ge=0)


class UploadSlotCreateResponse(BaseModel):
    upload_id: str
    upload_token: str
    tus_create_url: str
    tus_metadata: dict[str, str]
    upload_stream_url: str
    aggregate_stream_url: str


class TreeNode(BaseModel):
    path: str
    kind: str
    size_bytes: int | None = None
    online: bool
    source: str | None = None
    disc_ids: list[str] = Field(default_factory=list)
    status: str | None = None
    extra: dict[str, Any] | None = None


class TreeResponse(BaseModel):
    root_id: str
    root_kind: str
    nodes: list[TreeNode]


class OfflineError(BaseModel):
    error: str
    message: str
    disc_ids: list[str] = Field(default_factory=list)


class SealJobResponse(BaseModel):
    job_id: str
    status: str
    closed_discs: list[str]
    buffer_bytes: int


class CacheSessionCreateResponse(BaseModel):
    session_id: str
    disc_id: str
    expected_total_bytes: int
    expected_files: int
    progress_stream_url: str


class CacheUploadSlotRequest(BaseModel):
    relative_path: str


class CacheSessionCompleteResponse(BaseModel):
    disc_id: str
    session_id: str
    status: str
    contents_hash: str


class IsoRegisterRequest(BaseModel):
    server_path: str


class DownloadSessionCreateResponse(BaseModel):
    session_id: str
    disc_id: str
    total_bytes: int
    progress_stream_url: str
    content_url: str
