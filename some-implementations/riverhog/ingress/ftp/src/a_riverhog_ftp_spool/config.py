"""Strict configuration for the maintained FTP collection producer."""

from __future__ import annotations

import json
import os
from importlib.resources import files
from pathlib import Path
from typing import Literal, Self

from config_validation import load_validated_yaml_config, read_secret_file
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from riverhog_protocol import CollectionDescription, CollectionTag
from riverhog_provenance.common import require_urn_uuid

CloseMode = Literal["stable", "explicit-flush"]
ProvenanceMode = Literal["capture", "omit"]


class ConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SourceConfig(ConfigModel):
    """One deployment-owned, content-opaque intake source."""

    id: str = Field(pattern=r"^[a-z0-9](?:[a-z0-9._-]{0,118}[a-z0-9])?$")
    root: Path
    ingest_source: str = Field(min_length=1, max_length=512)
    archive_store: str | None = Field(default=None, min_length=1, max_length=160)
    description: CollectionDescription | None = None
    tags: tuple[CollectionTag, ...] = ()
    close_mode: CloseMode = "stable"
    max_files: int = Field(default=1000, ge=1)
    max_bytes: int = Field(default=100 * 1024**3, ge=1)
    provenance: ProvenanceMode = "capture"
    provenance_omission_reason: str | None = Field(default=None, max_length=1000)

    @field_validator("root")
    @classmethod
    def absolute_root(cls, value: Path) -> Path:
        expanded = value.expanduser()
        if not expanded.is_absolute():
            raise ValueError("FTP spool source root must be absolute")
        return expanded.resolve()

    @model_validator(mode="after")
    def complete_policy(self) -> Self:
        if len(self.tags) != len(set(self.tags)):
            raise ValueError("FTP spool source tags must be unique")
        if self.provenance == "omit":
            reason = (self.provenance_omission_reason or "").strip()
            if not reason or reason != self.provenance_omission_reason:
                raise ValueError("omitted provenance requires a visible canonical reason")
        elif self.provenance_omission_reason is not None:
            raise ValueError("capture mode cannot declare a provenance omission reason")
        return self


class _FtpSpoolPolicy(ConfigModel):
    host_id: str = Field(min_length=1, max_length=255)
    riverhog_base_url: str = Field(min_length=1, max_length=2048)
    allow_insecure_http: bool = False
    provenance_observer: str | None = Field(default=None, min_length=1, max_length=255)
    sources: tuple[SourceConfig, ...] = Field(min_length=1)
    poll_seconds: float = Field(default=5.0, ge=0.1, le=3600)
    pending_claim_capacity: int = Field(default=128, ge=1)
    claim_attempt_budget: int = Field(default=8, ge=2)
    discovery_entry_budget: int = Field(default=4096, ge=1)
    completion_failure_capacity: int = Field(default=128, ge=1)
    completion_failure_attempt_budget: int = Field(default=8, ge=1)

    @field_validator("sources")
    @classmethod
    def unique_sources(cls, value: tuple[SourceConfig, ...]) -> tuple[SourceConfig, ...]:
        ids = [item.id for item in value]
        roots = [item.root for item in value]
        if ids != sorted(ids) or len(ids) != len(set(ids)):
            raise ValueError("FTP spool sources must be unique and ordered by ID")
        if len(roots) != len(set(roots)):
            raise ValueError("FTP spool source roots must be unique")
        return value

    @model_validator(mode="after")
    def provenance_authority(self) -> Self:
        captures = any(source.provenance == "capture" for source in self.sources)
        if captures:
            require_urn_uuid(self.host_id, "host_id")
            if self.provenance_observer is None:
                raise ValueError("captured provenance requires an explicit observer provider")
        elif self.provenance_observer is not None:
            raise ValueError("provenance observer is unused when every source omits provenance")
        return self

    def source(self, source_id: str) -> SourceConfig:
        for source in self.sources:
            if source.id == source_id:
                return source
        raise KeyError(source_id)


class FtpSpoolConfig(_FtpSpoolPolicy):
    """Resolved runtime state; credentials are never operator document fields."""

    riverhog_token: str = Field(min_length=1, max_length=4096, repr=False)
    api_token: str = Field(min_length=1, max_length=4096, repr=False)


class FtpSpoolDocument(_FtpSpoolPolicy):
    """Operator document; credentials remain file paths until policy is valid."""

    riverhog_token_file: Path
    api_token_file: Path


def load_config(path: Path | None = None) -> FtpSpoolConfig:
    raw_path = str(path) if path is not None else os.environ.get("A_RIVERHOG_FTP_SPOOL_CONFIG", "")
    if not raw_path.strip():
        raise ValueError("A_RIVERHOG_FTP_SPOOL_CONFIG is required")
    document = FtpSpoolDocument.model_validate(
        load_validated_yaml_config(Path(raw_path).expanduser(), FTP_SPOOL_CONFIG_SCHEMA)
    )
    riverhog_token = read_secret_file(document.riverhog_token_file, label="riverhog_token_file")
    api_token = read_secret_file(document.api_token_file, label="api_token_file")
    if len(riverhog_token) > 4096 or len(api_token) > 4096:
        raise ValueError("FTP spool token file exceeds the maximum token length")
    return FtpSpoolConfig.model_construct(
        **{
            name: value
            for name, value in document.__dict__.items()
            if name not in {"riverhog_token_file", "api_token_file"}
        },
        riverhog_token=riverhog_token,
        api_token=api_token,
    )


def load_source_config(path: Path | None, source_id: str) -> SourceConfig:
    """Load one listener source without granting it Riverhog credentials."""

    raw_path = str(path) if path is not None else os.environ.get("A_RIVERHOG_FTP_SPOOL_CONFIG", "")
    if not raw_path.strip():
        raise ValueError("A_RIVERHOG_FTP_SPOOL_CONFIG is required")
    payload = _load_document(Path(raw_path).expanduser())
    raw_sources = payload.get("sources")
    if not isinstance(raw_sources, list):
        raise ValueError("FTP spool sources must be a list")
    matches = [
        SourceConfig.model_validate(item)
        for item in raw_sources
        if isinstance(item, dict) and item.get("id") == source_id
    ]
    if len(matches) != 1:
        raise KeyError(source_id)
    return matches[0]


def _load_document(path: Path) -> dict[str, object]:
    return load_validated_yaml_config(path, FTP_SPOOL_CONFIG_SCHEMA)


def generated_config_schema() -> dict[str, object]:
    """Derive the published document schema from the executable typed model."""

    schema = FtpSpoolDocument.model_json_schema()
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["$id"] = (
        "https://nashspence.github.io/riverhog/v1/config/a-riverhog-ftp-spool.schema.json"
    )
    return schema


FTP_SPOOL_CONFIG_SCHEMA: dict[str, object] = json.loads(
    files("a_riverhog_ftp_spool").joinpath("config.schema.json").read_text(encoding="utf-8")
)


__all__ = ["FtpSpoolConfig", "SourceConfig", "load_config", "load_source_config"]
