"""One validated operator document for S3-backed storage adapters."""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Self

from config_validation import read_secret_file
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from a_riverhog_s3_store_lib.client import S3ClientConfig, S3TransportTuning


class S3StoreDocument(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    bucket: str = Field(min_length=1)
    region: str = Field(min_length=1)
    endpoint_url: str | None = None
    root_prefix: str = ""
    token_file: Path
    access_key_id_file: Path
    secret_access_key_file: Path
    session_token_file: Path | None = None
    force_path_style: bool = False
    max_pool_connections: int = Field(default=32, ge=1, le=4096)
    connect_timeout_seconds: float = Field(default=10.0, gt=0)
    read_timeout_seconds: float = Field(default=300.0, gt=0)
    max_attempts: int = Field(default=8, ge=1, le=100)
    retry_mode: Literal["standard", "adaptive"] = "standard"
    tcp_keepalive: bool = True
    read_chunk_bytes: int = Field(default=8 * 1024 * 1024, ge=64 * 1024)

    @field_validator(
        "token_file",
        "access_key_id_file",
        "secret_access_key_file",
        "session_token_file",
    )
    @classmethod
    def absolute_secret_file(cls, value: Path | None) -> Path | None:
        if value is not None and not value.is_absolute():
            raise ValueError("secret-file paths must be absolute")
        return value

    @model_validator(mode="after")
    def normalized_prefix(self) -> Self:
        if self.root_prefix.strip("/") != self.root_prefix:
            raise ValueError("root_prefix must not begin or end with a slash")
        return self

    def token(self) -> str:
        return read_secret_file(self.token_file, label="token_file")

    def client_config(self) -> S3ClientConfig:
        return S3ClientConfig(
            endpoint_url=self.endpoint_url,
            region=self.region,
            access_key_id=read_secret_file(self.access_key_id_file, label="access_key_id_file"),
            secret_access_key=read_secret_file(
                self.secret_access_key_file, label="secret_access_key_file"
            ),
            session_token=(
                read_secret_file(self.session_token_file, label="session_token_file")
                if self.session_token_file is not None
                else None
            ),
            force_path_style=self.force_path_style,
        )

    def transport_tuning(self) -> S3TransportTuning:
        return S3TransportTuning(
            max_pool_connections=self.max_pool_connections,
            connect_timeout_seconds=self.connect_timeout_seconds,
            read_timeout_seconds=self.read_timeout_seconds,
            max_attempts=self.max_attempts,
            retry_mode=self.retry_mode,
            tcp_keepalive=self.tcp_keepalive,
        )


__all__ = ["S3StoreDocument"]
