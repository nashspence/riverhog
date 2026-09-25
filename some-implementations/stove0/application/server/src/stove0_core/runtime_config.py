"""Schema-validated operator configuration for the Stove0 server."""

from __future__ import annotations

import json
import math
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from importlib import resources
from pathlib import Path
from typing import Literal

from config_validation import load_validated_yaml_config, read_secret_file
from pydantic import BaseModel, ConfigDict, Field, field_validator
from riverhog_protocol.workspace_protection import DeclaredWorkspaceProtection
from stove0_operator_contracts import AdmissionCatalog, DepartureCatalog
from stove0_recipe_config import RecipeCatalog

DEFAULT_OPERATIONAL_STATE_RETENTION_SECONDS = 30 * 24 * 60 * 60


def _postgresql_database_url(value: str) -> str:
    if value.strip().split(":", 1)[0].split("+", 1)[0] != "postgresql":
        raise ValueError("Stove0 database URL must use postgresql")
    return value


@dataclass(frozen=True, slots=True)
class EndpointRegistration:
    base_url: str
    token: str | None = field(repr=False)
    allow_insecure_http: bool
    semantic_validator_providers: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class Stove0RuntimeConfig:
    database_url: str
    api_token: str | None = field(repr=False)
    riverhog_base_url: str
    riverhog_token: str = field(repr=False)
    riverhog_allow_insecure_http: bool
    recipes: RecipeCatalog
    observers: dict[str, EndpointRegistration]
    targets: dict[str, EndpointRegistration]
    target_callback_base_url: str
    target_callback_allow_insecure_http: bool
    target_callback_signing_key: str = field(repr=False)
    target_authority_batch_size: int
    declared_workspace_protection: DeclaredWorkspaceProtection
    claim_lease_seconds: int
    capability_ttl_seconds: int
    scheduler_interval_seconds: float
    operational_state_retention_seconds: int
    browse_token_signing_key: str = field(repr=False)
    admissions: AdmissionCatalog = AdmissionCatalog()
    departures: DepartureCatalog = DepartureCatalog()
    departure_targets: dict[str, EndpointRegistration] = field(default_factory=dict)
    browse_token_lifetime_seconds: int = 24 * 60 * 60


class _Document(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class EndpointDocument(_Document):
    base_url: str = Field(min_length=1)
    token_file: Path | None = None
    allow_insecure_http: bool = False


class ObserverEndpointDocument(EndpointDocument):
    semantic_validator_providers: tuple[str, ...] = ()

    @field_validator("semantic_validator_providers")
    @classmethod
    def unique_providers(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not provider.strip() for provider in value) or len(value) != len(set(value)):
            raise ValueError("observer semantic validator providers must be nonempty and unique")
        return tuple(sorted(value))


class Stove0Document(_Document):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        json_schema_extra={
            "$id": "https://nashspence.github.io/riverhog/v1/config/stove0-server.schema.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
        },
    )

    database_url_file: Path
    api_token_file: Path | None = None
    riverhog_base_url: str = Field(min_length=1)
    riverhog_token_file: Path
    riverhog_allow_insecure_http: bool = False
    recipes: RecipeCatalog
    admissions: AdmissionCatalog = Field(default_factory=AdmissionCatalog)
    departures: DepartureCatalog = Field(default_factory=DepartureCatalog)
    observers: dict[str, ObserverEndpointDocument] = Field(default_factory=dict)
    targets: dict[str, EndpointDocument] = Field(default_factory=dict)
    departure_targets: dict[str, EndpointDocument] = Field(default_factory=dict)
    target_callback_base_url: str | None = None
    target_callback_allow_insecure_http: bool = False
    target_callback_signing_key_file: Path | None = None
    target_authority_batch_size: int = Field(default=100, ge=1, le=128)
    declared_workspace_protection: Literal["encrypted-at-rest", "memory-backed"]
    claim_lease_seconds: int = Field(default=1800, ge=30)
    capability_ttl_seconds: int = Field(default=900, ge=30)
    scheduler_interval_seconds: float = Field(default=5.0, ge=0.1)
    operational_state_retention_seconds: int = Field(
        default=DEFAULT_OPERATIONAL_STATE_RETENTION_SECONDS, ge=1
    )
    browse_token_signing_key_file: Path
    browse_token_lifetime_seconds: int = Field(default=86400, ge=1)

    @field_validator("scheduler_interval_seconds")
    @classmethod
    def finite_interval(cls, value: float) -> float:
        if not math.isfinite(value):
            raise ValueError("scheduler interval must be finite")
        return value


def generated_config_schema() -> dict[str, object]:
    return Stove0Document.model_json_schema()


def _document(path: Path) -> Stove0Document:
    schema = json.loads(resources.files("stove0_core").joinpath("config.schema.json").read_text())
    return Stove0Document.model_validate(load_validated_yaml_config(path, schema))


def _registrations(
    documents: Mapping[str, EndpointDocument],
) -> dict[str, EndpointRegistration]:
    return {
        name: EndpointRegistration(
            base_url=endpoint.base_url,
            token=(
                read_secret_file(endpoint.token_file, label=f"registration {name} token_file")
                if endpoint.token_file is not None
                else None
            ),
            allow_insecure_http=endpoint.allow_insecure_http,
            semantic_validator_providers=(
                endpoint.semantic_validator_providers
                if isinstance(endpoint, ObserverEndpointDocument)
                else ()
            ),
        )
        for name, endpoint in sorted(documents.items())
    }


def load_stove0_config(
    path: Path | None = None,
    *,
    require_api_token: bool = True,
) -> Stove0RuntimeConfig:
    if path is None:
        raw = os.environ.get("STOVE0_CONFIG", "").strip()
        if not raw:
            raise ValueError("STOVE0_CONFIG must name a YAML configuration document")
        path = Path(raw)
    document = _document(path)
    if require_api_token and document.api_token_file is None:
        raise ValueError("api_token_file is required for the API role")
    if document.targets and not document.target_callback_base_url:
        raise ValueError("target_callback_base_url is required when targets are configured")
    if document.targets and document.target_callback_signing_key_file is None:
        raise ValueError("target_callback_signing_key_file is required when targets are configured")
    return Stove0RuntimeConfig(
        database_url=_postgresql_database_url(
            read_secret_file(document.database_url_file, label="database_url_file")
        ),
        api_token=(
            read_secret_file(document.api_token_file, label="api_token_file")
            if require_api_token and document.api_token_file is not None
            else None
        ),
        riverhog_base_url=document.riverhog_base_url,
        riverhog_token=read_secret_file(document.riverhog_token_file, label="riverhog_token_file"),
        riverhog_allow_insecure_http=document.riverhog_allow_insecure_http,
        recipes=document.recipes,
        admissions=document.admissions,
        departures=document.departures,
        observers=_registrations(document.observers),
        targets=_registrations(document.targets),
        departure_targets=_registrations(document.departure_targets),
        target_callback_base_url=document.target_callback_base_url or "https://stove0.invalid",
        target_callback_allow_insecure_http=document.target_callback_allow_insecure_http,
        target_callback_signing_key=(
            read_secret_file(
                document.target_callback_signing_key_file,
                label="target_callback_signing_key_file",
            )
            if document.target_callback_signing_key_file is not None
            else "unused-target-callback-signing-key"
        ),
        target_authority_batch_size=document.target_authority_batch_size,
        declared_workspace_protection=document.declared_workspace_protection,
        claim_lease_seconds=document.claim_lease_seconds,
        capability_ttl_seconds=document.capability_ttl_seconds,
        scheduler_interval_seconds=document.scheduler_interval_seconds,
        operational_state_retention_seconds=document.operational_state_retention_seconds,
        browse_token_signing_key=read_secret_file(
            document.browse_token_signing_key_file, label="browse_token_signing_key_file"
        ),
        browse_token_lifetime_seconds=document.browse_token_lifetime_seconds,
    )


def database_url_from_config(path: Path | None = None) -> str:
    if path is None:
        raw = os.environ.get("STOVE0_CONFIG", "").strip()
        if not raw:
            raise ValueError("STOVE0_CONFIG must name a YAML configuration document")
        path = Path(raw)
    return _postgresql_database_url(
        read_secret_file(_document(path).database_url_file, label="database_url_file")
    )


__all__ = [
    "DEFAULT_OPERATIONAL_STATE_RETENTION_SECONDS",
    "EndpointRegistration",
    "Stove0RuntimeConfig",
    "database_url_from_config",
    "generated_config_schema",
    "load_stove0_config",
]
