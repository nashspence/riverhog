"""Typed FTP spool operator status and bounded browse page."""

from __future__ import annotations

from typing import Annotated, Literal

from http_api_contracts import BrowsePageToken
from pydantic import BaseModel, ConfigDict, Field

SourceId = Annotated[
    str,
    Field(pattern=r"^[a-z0-9](?:[a-z0-9._-]{0,118}[a-z0-9])?$"),
]


class CompletionFailureStatus(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    reason: str
    retryable: bool
    attempts: int = Field(ge=0)


class SourceStatus(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    id: SourceId
    ingest_source: str
    claims: int = Field(ge=0)
    claim_bytes: int = Field(ge=0)
    close_mode: Literal["stable", "explicit-flush"]
    max_files: int = Field(ge=1)
    max_bytes: int = Field(ge=1)
    provenance: Literal["capture", "omit"]
    pending_claim_capacity: int = Field(ge=1)
    completion_failures: int = Field(ge=0)
    completion_failure_capacity: int = Field(ge=1)
    oldest_completion_failure: CompletionFailureStatus | None


class FtpSpoolStatus(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    format: Literal["a-riverhog-ftp-spool-status/v1"]
    provenance_observer: str | None
    sources: list[SourceStatus] = Field(max_length=100)
    page_size: int = Field(ge=1, le=100)
    next_page_token: BrowsePageToken | None
    snapshot: Literal[False]


__all__ = ["FtpSpoolStatus"]
