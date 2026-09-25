from __future__ import annotations

from typing import Literal

from http_api_contracts import BrowsePageToken
from pydantic import Field
from riverhog_protocol import ArchiveStoreName, ArchiveStoreSort, SortOrder
from riverhog_storage_adapter_protocol import StorageIncarnationId
from time_formats import CanonicalUtcTimestamp

from riverhog_api.schemas.common import RiverhogModel


class ArchiveDownloadAllowanceOut(RiverhogModel):
    store: ArchiveStoreName
    state: Literal["open", "closed"]
    month_started_at: CanonicalUtcTimestamp
    resets_at: CanonicalUtcTimestamp
    allowance_bytes: int
    safety_buffer_bytes: int
    effective_limit_bytes: int
    accounted_bytes: int
    reserved_bytes: int
    remaining_bytes: int


class ArchiveStoreOut(RiverhogModel):
    store: ArchiveStoreName
    incarnation_id: StorageIncarnationId | None
    administrative_state: Literal["bound", "disabled", "retired"] | None
    configured: bool
    reachable: bool
    readable: bool
    writable: bool
    read_mode: Literal["immediate", "restore_required"] | None
    read_priority: int
    write_target: bool
    collections: int
    objects: int
    stored_bytes: int
    download_allowance: ArchiveDownloadAllowanceOut | None


class ArchiveStoreListOut(RiverhogModel):
    page_size: int = Field(ge=1, le=100)
    next_page_token: BrowsePageToken | None
    sort: ArchiveStoreSort
    order: SortOrder
    query: str | None
    stores: list[ArchiveStoreOut]
