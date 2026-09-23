"""Authenticated Review0 rclone target process."""

from __future__ import annotations

import argparse
import contextlib
import importlib.metadata
import os
from collections.abc import Sequence
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from review0_target_lib import (
    SamplerRegistration,
    create_target_app,
    load_sampler_registrations,
    parse_sampler_registrations,
)
from stove0_target_support import terminal_state_retention_seconds

from a_review0_rclone_target.target import (
    RcloneReviewDestination,
    ReviewRcloneEffectTargetService,
)

SERVICE = "a-review0-rclone-target"
PREFIX = "A_REVIEW0_RCLONE_TARGET"


def _sampler_registrations() -> tuple[SamplerRegistration, ...]:
    direct = os.getenv(f"{PREFIX}_SAMPLERS_JSON")
    path = os.getenv(f"{PREFIX}_SAMPLERS_JSON_FILE")
    if bool(direct) == bool(path):
        raise ValueError("set exactly one Review0 rclone target sampler configuration source")
    if direct is not None:
        return parse_sampler_registrations(direct)
    return load_sampler_registrations(Path(str(path)))


def _secret() -> str:
    direct = os.getenv(f"{PREFIX}_TOKEN")
    path = os.getenv(f"{PREFIX}_TOKEN_FILE")
    if bool(direct) == bool(path):
        raise ValueError("set exactly one Review0 rclone target token source")
    value = direct if direct is not None else Path(str(path)).read_text(encoding="utf-8")
    if not value.strip():
        raise ValueError("Review0 rclone target token must be nonempty")
    return value.strip()


def _image_id() -> str:
    value = os.getenv(f"{PREFIX}_IMAGE_ID", "").strip()
    if not (
        len(value) == 71
        and value.startswith("sha256:")
        and all(character in "0123456789abcdef" for character in value[7:])
    ):
        raise ValueError(f"{PREFIX}_IMAGE_ID must be an OCI ImageID (sha256:<64 lowercase hex>)")
    return value


def _effect_destination() -> RcloneReviewDestination:
    identity = os.getenv(f"{PREFIX}_DESTINATION_IDENTITY", "").strip()
    remote = os.getenv(f"{PREFIX}_RCLONE_REMOTE", "").strip()
    config = os.getenv(f"{PREFIX}_RCLONE_CONFIG_FILE", "").strip()
    if not identity or not remote:
        raise ValueError("Review0 rclone target requires destination identity and remote")
    timeout = int(os.getenv(f"{PREFIX}_RCLONE_TIMEOUT_SECONDS", "86400"))
    return RcloneReviewDestination(
        identity=identity,
        remote=remote,
        config_path=Path(config) if config else None,
        executable=os.getenv(f"{PREFIX}_RCLONE_BIN", "rclone").strip(),
        timeout_seconds=timeout,
    )


def create_app(*, token: str, target: ReviewRcloneEffectTargetService) -> FastAPI:
    return create_target_app(
        service=SERVICE,
        title="Review0 rclone target",
        token=token,
        target=target,
    )


_CLI_RESULT_CONTRACT = {
    "schema": "riverhog-cli-result-contract/v1",
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
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    version = importlib.metadata.version(SERVICE)
    target = ReviewRcloneEffectTargetService(
        state_root=Path(os.getenv(f"{PREFIX}_STATE_ROOT", "/var/lib/a-review0-rclone-target")),
        workspace_root=Path(os.getenv(f"{PREFIX}_WORKSPACE", "/run/review0")),
        samplers=_sampler_registrations(),
        destination=_effect_destination(),
        source_revision=os.getenv(f"{PREFIX}_SOURCE_REVISION", "unknown"),
        image_id=_image_id(),
        implementation_version=version,
        terminal_state_retention_seconds=terminal_state_retention_seconds(),
    )
    token = _secret()
    with contextlib.suppress(KeyError):
        os.environ.pop(f"{PREFIX}_TOKEN")
    uvicorn.run(create_app(token=token, target=target), host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["create_app", "main"]
