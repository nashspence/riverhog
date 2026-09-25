"""Authenticated Review0 rclone target process."""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
from collections.abc import Sequence
from importlib.resources import files
from pathlib import Path

import uvicorn
from config_validation import load_validated_yaml_config, read_secret_file
from fastapi import FastAPI
from pydantic import ConfigDict, Field, field_validator
from review0_target_lib import (
    ReviewTargetConfig,
    create_target_app,
    sampler_registrations,
)
from stove0_target_support import terminal_state_retention_seconds

from a_review0_rclone_target.target import (
    RcloneReviewDestination,
    ReviewRcloneEffectTargetService,
)

SERVICE = "a-review0-rclone-target"
PREFIX = "A_REVIEW0_RCLONE_TARGET"


class RcloneTargetConfig(ReviewTargetConfig):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        json_schema_extra={
            "$id": "https://nashspence.github.io/riverhog/v1/config/a-review0-rclone-target.schema.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
        },
    )

    destination_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    rclone_remote: str = Field(min_length=1)
    rclone_config_file: Path | None = None
    rclone_timeout_seconds: int = Field(default=86400, ge=1)

    @field_validator("rclone_config_file")
    @classmethod
    def absolute_rclone_config_file(cls, value: Path | None) -> Path | None:
        if value is not None and not value.is_absolute():
            raise ValueError("rclone_config_file must be absolute")
        return value


RCLONE_CONFIG_SCHEMA: dict[str, object] = json.loads(
    files("a_review0_rclone_target").joinpath("config.schema.json").read_text(encoding="utf-8")
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


def _effect_destination(config: RcloneTargetConfig) -> RcloneReviewDestination:
    return RcloneReviewDestination(
        identity=config.destination_identity,
        remote=config.rclone_remote,
        config_path=config.rclone_config_file,
        executable=os.getenv(f"{PREFIX}_RCLONE_BIN", "rclone").strip(),
        timeout_seconds=config.rclone_timeout_seconds,
    )


def create_app(*, token: str, target: ReviewRcloneEffectTargetService) -> FastAPI:
    return create_target_app(
        service=SERVICE,
        title="Review0 rclone target",
        token=token,
        target=target,
    )


_CLI_RESULT_CONTRACT = {
    "format": "riverhog-cli-result-contract/v1",
    "identity_prefix": "a-review0-rclone-target-cli-result",
    "default_profile": "runtime",
    "profiles": {
        "runtime": {
            "id": "a-review0-rclone-target-cli-runtime/v1",
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
    "version_distribution": "a-review0-rclone-target",
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
    samplers = sampler_registrations(config)
    destination = _effect_destination(config)
    version = importlib.metadata.version(SERVICE)
    target = ReviewRcloneEffectTargetService(
        state_root=Path(os.getenv(f"{PREFIX}_STATE_ROOT", "/var/lib/a-review0-rclone-target")),
        workspace_root=Path(os.getenv(f"{PREFIX}_WORKSPACE", "/run/review0")),
        samplers=samplers,
        destination=destination,
        source_revision=os.getenv(f"{PREFIX}_SOURCE_REVISION", "unknown"),
        image_id=_image_id(),
        implementation_version=version,
        terminal_state_retention_seconds=terminal_state_retention_seconds(),
    )
    uvicorn.run(create_app(token=token, target=target), host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["RCLONE_CONFIG_SCHEMA", "RcloneTargetConfig", "create_app", "load_config", "main"]
