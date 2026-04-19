from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class CollectionSealRequest(BaseModel):
    upload_path: str
    description: str | None = None
    keep_buffer_after_archive: bool = False


class CollectionSummary(BaseModel):
    collection_id: str
    status: str
    upload_relative_path: str
    upload_path: str
    buffer_path: str | None = None
    description: str | None = None
    keep_buffer_after_archive: bool
    file_count: int
    directory_count: int
    created_at: str
    sealed_at: str | None = None
    export_path: str
    hash_manifest_path: str | None = None
    hash_proof_path: str | None = None


class CollectionListResponse(BaseModel):
    collections: list[CollectionSummary]


class TreeNode(BaseModel):
    path: str
    kind: str
    size_bytes: int | None = None
    active: bool
    source: str | None = None
    container_ids: list[str] = Field(default_factory=list)
    status: str | None = None
    extra: dict[str, Any] | None = None


class TreeResponse(BaseModel):
    root_id: str
    root_kind: str
    nodes: list[TreeNode]


class InactiveError(BaseModel):
    error: str
    message: str
    container_ids: list[str] = Field(default_factory=list)


class IngestPlanResponse(BaseModel):
    target_bytes: int
    fill_bytes: int
    spill_fill_bytes: int
    buffer_planned_bytes: int
    buffer_payload_bytes: int
    closed_disc_count: int
    planned_disc_count: int
    discs: list[dict[str, Any]] = Field(default_factory=list)


class SealCollectionResponse(BaseModel):
    collection_id: str
    status: str
    closed_containers: list[str]
    buffer_bytes: int
    plan: IngestPlanResponse


class ActivationSessionCreateResponse(BaseModel):
    session_id: str
    container_id: str
    expected_total_bytes: int
    expected_files: int
    staging_path: str


class ActivationSessionCompleteResponse(BaseModel):
    container_id: str
    session_id: str
    status: str
    contents_hash: str


class IsoRegisterRequest(BaseModel):
    server_path: str


class IsoCreateRequest(BaseModel):
    volume_label: str | None = None
    overwrite: bool = False


class IsoCreateResponse(BaseModel):
    container_id: str
    iso_path: str
    size_bytes: int


class ContainerSummary(BaseModel):
    container_id: str
    status: str
    description: str | None = None
    total_root_bytes: int
    contents_hash: str
    entry_count: int
    active_root_present: bool
    iso_present: bool
    iso_size_bytes: int | None = None
    root_path: str
    active_root_path: str | None = None
    iso_path: str | None = None
    burn_confirmed_at: str | None = None
    created_at: str


class ContainerListResponse(BaseModel):
    containers: list[ContainerSummary]


class BurnConfirmResponse(BaseModel):
    container_id: str
    burn_confirmed_at: str
    released_collection_ids: list[str] = Field(default_factory=list)


class PartitioningPoolStatusResponse(BaseModel):
    state: str
    status_message: str
    pending_collection_count: int
    pending_piece_group_count: int
    pending_bytes: int
    target_bytes: int
    fill_bytes: int
    spill_fill_bytes: int
    buffer_max_bytes: int
    closeable_now: bool
    next_container_id: str | None = None
    next_container_bytes: int | None = None
    next_container_free_bytes: int | None = None
    next_container_collection_count: int | None = None
    next_container_piece_group_count: int | None = None
