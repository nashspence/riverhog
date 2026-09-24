"""Canonical durable-state structures owned by the FTP adapter."""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field
from riverhog_protocol import CollectionId

from a_riverhog_ftp_spool.completion import (
    COMPLETION_LOG_HEADER,
    CompletionRecordFormat,
)

_SHA256_PATTERN = r"^[0-9a-f]{64}$"
Sha256 = Annotated[str, Field(pattern=_SHA256_PATTERN)]
NonNegativeInt = Annotated[int, Field(ge=0)]

FTP_OPERATIONAL_STATE_DDL = """
CREATE TABLE IF NOT EXISTS claims (
    ordinal INTEGER PRIMARY KEY,
    claim_id TEXT NOT NULL UNIQUE,
    manifest_json TEXT NOT NULL,
    claim_bytes INTEGER NOT NULL CHECK (claim_bytes >= 0)
);
CREATE TABLE IF NOT EXISTS adapter_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS completion_events (
    event_id TEXT PRIMARY KEY,
    claim_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS completion_failures (
    ordinal INTEGER PRIMARY KEY,
    failure_id TEXT NOT NULL UNIQUE,
    generation TEXT NOT NULL,
    record_offset INTEGER NOT NULL CHECK (record_offset >= 0),
    raw BLOB NOT NULL,
    reason TEXT NOT NULL,
    retryable INTEGER NOT NULL CHECK (retryable IN (0, 1)),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0)
);
PRAGMA user_version = 2;
"""


class _StateModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class CapturedProvenanceState(_StateModel):
    path: str = Field(min_length=1)
    bytes: NonNegativeInt
    sha256: Sha256
    status: Literal["captured"]
    journal_id: str = Field(min_length=1)
    current_state_id: str = Field(min_length=1)


class OmittedProvenanceState(_StateModel):
    path: str = Field(min_length=1)
    bytes: NonNegativeInt
    sha256: Sha256
    status: Literal["omitted"]
    omission_reason: str = Field(min_length=1)


class CompletionRecordState(_StateModel):
    format: CompletionRecordFormat
    event_id: str = Field(min_length=1)
    source_id: str = Field(min_length=1)
    path: str = Field(min_length=1)
    custody: str = Field(min_length=1)
    bytes: NonNegativeInt
    device: NonNegativeInt
    inode: NonNegativeInt


class FtpClaimFileState(_StateModel):
    path: str = Field(min_length=1)
    bytes: NonNegativeInt
    sha256: Sha256
    device: NonNegativeInt
    inode: NonNegativeInt
    original: str = Field(min_length=1)
    completion_record: CompletionRecordState | None = None
    provenance: CapturedProvenanceState | OmittedProvenanceState


class FtpClaimState(_StateModel):
    format: Literal["a-riverhog-ftp-spool-claim/v1"]
    claim_id: str = Field(min_length=1)
    source_event_id: str = Field(min_length=1)
    source: str = Field(min_length=1)
    completion_event_ids: list[str] | None = None
    files: list[FtpClaimFileState] = Field(min_length=1)
    journals: dict[str, str]


class FtpReceiptState(_StateModel):
    format: Literal["a-riverhog-ftp-spool-receipt/v1"]
    claim_id: str = Field(min_length=1)
    source_event_id: str = Field(min_length=1)
    collection_id: CollectionId
    archive_root_sha256: Sha256
    content_identity: Sha256
    riverhog_receipt: dict[str, Any]


def ftp_custody_state_contract() -> dict[str, object]:
    """Return every durable unit needed to resume or reconcile FTP custody."""

    return {
        "kind": "composite",
        "units": [
            {
                "id": "operational-database",
                "kind": "sql-ddl",
                "dialect": "sqlite",
                "user_version": 2,
                "ddl": FTP_OPERATIONAL_STATE_DDL,
            },
            {
                "id": "completion-log",
                "kind": "append-only-json-sequence",
                "header": f"{COMPLETION_LOG_HEADER} <canonical-uuid>",
                "record_schema": CompletionRecordState.model_json_schema(mode="validation"),
            },
            {
                "id": "claim",
                "kind": "json-document",
                "schema": FtpClaimState.model_json_schema(mode="validation"),
            },
            {
                "id": "receipt",
                "kind": "json-document",
                "schema": FtpReceiptState.model_json_schema(mode="validation"),
            },
            {
                "id": "payload",
                "kind": "opaque-bytes",
                "identity": "claim file path, byte length, and SHA-256",
            },
        ],
    }
