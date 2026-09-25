"""Authenticated review materialization target process."""

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
from review0_target_lib import (
    ReviewTargetConfig,
    create_target_app,
    sampler_registrations,
)
from stove0_target_support import terminal_state_retention_seconds

from a_review0_materializer.target import ReviewMaterializeTargetService

SERVICE = "a-review0-materializer"
PREFIX = "A_REVIEW0_MATERIALIZER"


MATERIALIZER_CONFIG_SCHEMA: dict[str, object] = json.loads(
    files("a_review0_materializer").joinpath("config.schema.json").read_text(encoding="utf-8")
)


def load_config(path: Path) -> ReviewTargetConfig:
    return ReviewTargetConfig.model_validate(
        load_validated_yaml_config(path, MATERIALIZER_CONFIG_SCHEMA)
    )


def _image_id() -> str:
    value = os.getenv(f"{PREFIX}_IMAGE_ID", "").strip()
    if not (
        len(value) == 71
        and value.startswith("sha256:")
        and all(character in "0123456789abcdef" for character in value[7:])
    ):
        raise ValueError(f"{PREFIX}_IMAGE_ID must be an OCI ImageID (sha256:<64 lowercase hex>)")
    return value


def create_app(*, token: str, target: ReviewMaterializeTargetService) -> FastAPI:
    return create_target_app(
        service=SERVICE,
        title="Review0 materializer",
        token=token,
        target=target,
    )


_CLI_RESULT_CONTRACT = {
    "format": "riverhog-cli-result-contract/v1",
    "identity_prefix": "a-review0-materializer-cli-result",
    "default_profile": "runtime",
    "profiles": {
        "runtime": {
            "id": "a-review0-materializer-cli-runtime/v1",
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
    "version_distribution": "a-review0-materializer",
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
    version = importlib.metadata.version(SERVICE)
    target = ReviewMaterializeTargetService(
        state_root=Path(os.getenv(f"{PREFIX}_STATE_ROOT", "/var/lib/a-review0-materializer")),
        workspace_root=Path(os.getenv(f"{PREFIX}_WORKSPACE", "/run/review0")),
        samplers=samplers,
        source_revision=os.getenv(f"{PREFIX}_SOURCE_REVISION", "unknown"),
        image_id=_image_id(),
        implementation_version=version,
        terminal_state_retention_seconds=terminal_state_retention_seconds(),
    )
    uvicorn.run(create_app(token=token, target=target), host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["MATERIALIZER_CONFIG_SCHEMA", "create_app", "load_config", "main"]
