"""Authenticated generic rclone effect target process."""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
import secrets
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from importlib.resources import files
from pathlib import Path
from typing import Literal

import uvicorn
from config_validation import load_validated_yaml_config, read_secret_file
from fastapi import Depends, FastAPI, Request, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.security import HTTPBearer
from http_api_contracts import HealthOut, error_payload, operation_openapi
from http_api_contracts.metadata_binding import read_control_body
from http_api_contracts.metadata_staging import MetadataStagingError
from pydantic import BaseModel, ConfigDict, Field, field_validator
from riverhog_canonical_json import canonical_json_bytes
from riverhog_materialization import DestinationRules
from stove0_target_support import (
    TARGET_HTTP_OPERATIONS,
    TargetHttpBinding,
    terminal_state_retention_seconds,
)

from a_stove0_rclone_target.target import RcloneDestination, RcloneEffectTargetService

SERVICE = "a-stove0-rclone-target"
PREFIX = "A_STOVE0_RCLONE_TARGET"
_PUBLIC_METHODS = frozenset({"GET", "POST", "PUT", "PATCH", "DELETE"})
_bearer = HTTPBearer(auto_error=False)


class RcloneNamingRules(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    windows_names: bool
    case_sensitive: bool
    unicode_equivalence: Literal["exact", "NFC"]
    component_bytes: int = Field(ge=1)
    relative_path_bytes: int = Field(ge=1)

    def qualified(self) -> DestinationRules:
        return DestinationRules(**self.model_dump())


class RcloneTargetConfig(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        json_schema_extra={
            "$id": "https://nashspence.github.io/riverhog/v1/config/a-stove0-rclone-target.schema.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
        },
    )

    token_file: Path
    destination_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    destination_naming_rules: RcloneNamingRules
    rclone_remote: str = Field(min_length=1)
    rclone_config_file: Path | None = None
    rclone_timeout_seconds: int = Field(default=86400, ge=1)

    @field_validator("token_file", "rclone_config_file")
    @classmethod
    def absolute_file(cls, value: Path | None) -> Path | None:
        if value is not None and not value.is_absolute():
            raise ValueError("rclone file paths must be absolute")
        return value


RCLONE_CONFIG_SCHEMA: dict[str, object] = json.loads(
    files("a_stove0_rclone_target").joinpath("config.schema.json").read_text(encoding="utf-8")
)


def load_config(path: Path) -> RcloneTargetConfig:
    return RcloneTargetConfig.model_validate(load_validated_yaml_config(path, RCLONE_CONFIG_SCHEMA))


def _image_id() -> str:
    value = os.getenv(f"{PREFIX}_IMAGE_ID", "").strip()
    if not (
        len(value) == 71
        and value.startswith("sha256:")
        and all(character in "0123456789abcdef" for character in value[7:])
    ):
        raise ValueError(f"{PREFIX}_IMAGE_ID must be an OCI ImageID (sha256:<64 lowercase hex>)")
    return value


def _effect_destination(config: RcloneTargetConfig) -> RcloneDestination:
    return RcloneDestination(
        identity=config.destination_identity,
        remote=config.rclone_remote,
        naming_rules=config.destination_naming_rules.qualified(),
        config_path=config.rclone_config_file,
        executable=os.getenv(f"{PREFIX}_RCLONE_BIN", "rclone").strip(),
        timeout_seconds=config.rclone_timeout_seconds,
    )


def create_app(*, token: str, target: RcloneEffectTargetService) -> FastAPI:
    credential = token.strip()
    if not credential:
        raise ValueError("rclone target token must be nonempty")
    binding = TargetHttpBinding(target)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            target.close()

    app = FastAPI(
        title="Stove0 rclone target",
        version="1",
        lifespan=lifespan,
        openapi_url="/v1/openapi.json",
    )

    @app.get("/health/live", response_model=HealthOut, tags=["health"])
    def live() -> dict[str, str]:
        return {"service": SERVICE, "status": "ok"}

    @app.get("/health/ready", response_model=HealthOut, tags=["health"])
    def ready() -> dict[str, str]:
        return {"service": SERVICE, "status": "ok"}

    async def dispatch(request: Request) -> Response:
        scheme, _, supplied = request.headers.get("authorization", "").partition(" ")
        if scheme.casefold() != "bearer" or not secrets.compare_digest(supplied, credential):
            return Response(
                content=json.dumps(
                    error_payload(
                        code="unauthorized", message="Bearer credential is not authorized"
                    ),
                    separators=(",", ":"),
                ),
                status_code=401,
                media_type="application/json",
            )
        try:
            body = await read_control_body(
                request, maximum_request_bytes=binding.maximum_request_bytes
            )
        except MetadataStagingError as exc:
            return Response(
                content=canonical_json_bytes({"error": {"code": exc.code, "message": exc.message}}),
                status_code=exc.status,
                media_type="application/json",
            )
        result = await run_in_threadpool(binding.handle, request.method, request.url.path, body)
        return Response(
            content=result.body, status_code=result.status, headers=dict(result.headers)
        )

    methods_by_path: dict[str, set[str]] = {}
    for operation in TARGET_HTTP_OPERATIONS:
        methods_by_path.setdefault(operation.path, set()).add(operation.method)
        app.add_api_route(
            operation.path,
            dispatch,
            methods=[operation.method],
            dependencies=[Depends(_bearer)],
            tags=["target"],
            **operation_openapi(operation),
        )
    for path, supported in methods_by_path.items():
        app.add_api_route(
            path,
            dispatch,
            methods=sorted(_PUBLIC_METHODS - supported),
            include_in_schema=False,
        )
    return app


_CLI_RESULT_CONTRACT = {
    "format": "riverhog-cli-result-contract/v1",
    "identity_prefix": "a-stove0-rclone-target-cli-result",
    "default_profile": "runtime",
    "profiles": {
        "runtime": {
            "id": "a-stove0-rclone-target-cli-runtime/v1",
            "structured_output": "none",
            "human_json_relationship": "not-applicable",
            "success": [
                {
                    "id": "stopped",
                    "exit_status": 0,
                    "stdout": {"all": "no-command-result"},
                    "stderr": {"all": "noncontractual-runtime-log"},
                }
            ],
            "failures": [
                {
                    "id": "usage",
                    "exit_status": 2,
                    "stdout": {"all": "empty"},
                    "stderr": {"all": "noncontractual-usage-diagnostic"},
                }
            ],
        }
    },
    "command_profiles": {},
    "command_overrides": {},
    "executable_groups": [],
    "outcome_selectors": {
        "stopped": {"kind": "service-runtime-returned"},
        "usage": {"kind": "parser-rejected-invocation"},
    },
    "output_authorities": {},
    "version_distribution": SERVICE,
}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=SERVICE)
    parser.add_argument("--version", action="version", version=importlib.metadata.version(SERVICE))
    parser.add_argument("--host", default=os.getenv(f"{PREFIX}_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv(f"{PREFIX}_PORT", "8080")))
    parser.add_argument("--config", type=Path, default=os.getenv(f"{PREFIX}_CONFIG"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.config is None:
        raise ValueError(f"{PREFIX}_CONFIG or --config is required")
    config = load_config(args.config)
    token = read_secret_file(config.token_file, label="token_file")
    target = RcloneEffectTargetService(
        state_root=Path(os.getenv(f"{PREFIX}_STATE_ROOT", "/var/lib/a-stove0-rclone-target")),
        workspace_root=Path(os.getenv(f"{PREFIX}_WORKSPACE", "/run/stove0-rclone")),
        destination=_effect_destination(config),
        source_revision=os.getenv(f"{PREFIX}_SOURCE_REVISION", "unknown"),
        image_id=_image_id(),
        implementation_version=importlib.metadata.version(SERVICE),
        terminal_state_retention_seconds=terminal_state_retention_seconds(),
    )
    uvicorn.run(create_app(token=token, target=target), host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["RCLONE_CONFIG_SCHEMA", "RcloneTargetConfig", "create_app", "load_config", "main"]
