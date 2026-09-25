"""Runtime configuration for the narrowly scoped first-party Backblaze adapter."""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
from collections.abc import Sequence
from importlib.resources import files
from pathlib import Path

import uvicorn
from a_riverhog_s3_store_lib import (
    S3StorageAdapter,
    S3StorageAdapterConfig,
    create_s3_client,
)
from a_riverhog_s3_store_lib.incarnation import provision_storage_incarnation
from a_riverhog_s3_store_lib.runtime_config import S3StoreDocument
from config_validation import load_validated_yaml_config
from pydantic import ConfigDict, Field
from riverhog_storage_adapter_asgi_support import create_storage_adapter_app

SERVICE = "a-riverhog-b2-store"
_PREFIX = "A_RIVERHOG_B2_STORE_"


class B2StoreDocument(S3StoreDocument):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        json_schema_extra={
            "$id": "https://nashspence.github.io/riverhog/v1/config/a-riverhog-b2-store.schema.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
        },
    )

    endpoint_url: str = Field(min_length=1)


B2_CONFIG_SCHEMA: dict[str, object] = json.loads(
    files("a_riverhog_b2_store").joinpath("config.schema.json").read_text(encoding="utf-8")
)


def load_config(path: Path) -> B2StoreDocument:
    return B2StoreDocument.model_validate(load_validated_yaml_config(path, B2_CONFIG_SCHEMA))


_CLI_RESULT_CONTRACT = {
    "format": "riverhog-cli-result-contract/v1",
    "identity_prefix": "a-riverhog-b2-store-cli-result",
    "default_profile": "runtime",
    "profiles": {
        "runtime": {
            "id": "a-riverhog-b2-store-cli-runtime/v1",
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
    "version_distribution": "a-riverhog-b2-store",
}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=SERVICE)
    parser.add_argument("--version", action="version", version=importlib.metadata.version(SERVICE))
    parser.add_argument("--host", default=os.getenv(f"{_PREFIX}HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv(f"{_PREFIX}PORT", "8080")))
    parser.add_argument("--config", type=Path, default=os.getenv(f"{_PREFIX}CONFIG"))
    parser.add_argument("--provision-root", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.config is None:
        raise ValueError(f"{_PREFIX}CONFIG or --config is required")
    config = load_config(args.config)
    token = config.token()
    client_config = config.client_config()
    tuning = config.transport_tuning()
    client = create_s3_client(client_config, tuning=tuning)
    if args.provision_root:
        provision_storage_incarnation(client, bucket=config.bucket, root_prefix=config.root_prefix)
        return 0
    adapter = S3StorageAdapter(
        client,
        S3StorageAdapterConfig(
            implementation_id="a-riverhog-b2-store/v1",
            implementation_version=importlib.metadata.version(SERVICE),
            bucket=config.bucket,
            root_prefix=config.root_prefix,
            read_mode="immediate",
            read_chunk_bytes=config.read_chunk_bytes,
        ),
    )

    def readiness() -> None:
        client.head_bucket(Bucket=config.bucket)

    uvicorn.run(
        create_storage_adapter_app(
            service=SERVICE,
            token=token,
            adapter=adapter,
            readiness=readiness,
        ),
        host=args.host,
        port=args.port,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["B2_CONFIG_SCHEMA", "B2StoreDocument", "SERVICE", "load_config", "main"]
