"""Runtime configuration for the narrowly scoped first-party AWS adapter."""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
from collections.abc import Sequence
from importlib.resources import files
from pathlib import Path
from typing import Literal

import uvicorn
from a_riverhog_s3_store_lib import (
    S3StorageAdapter,
    S3StorageAdapterConfig,
    create_s3_client,
)
from a_riverhog_s3_store_lib.incarnation import provision_storage_incarnation
from a_riverhog_s3_store_lib.runtime_config import S3StoreDocument
from config_validation import load_validated_yaml_config
from pydantic import BaseModel, ConfigDict, Field, field_validator
from riverhog_storage_adapter_asgi_support import create_storage_adapter_app

from a_riverhog_aws_store.provider import (
    AwsCloudFrontConfig,
    AwsCloudFrontObjectReader,
    AwsDeepArchiveReadPreparation,
)

SERVICE = "a-riverhog-aws-store"
_PREFIX = "A_RIVERHOG_AWS_STORE_"


class AwsCloudFrontDocument(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    base_url: str = Field(min_length=1)
    public_key_id: str = Field(min_length=1)
    private_key_path: Path

    @field_validator("private_key_path")
    @classmethod
    def absolute_private_key_path(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("CloudFront private key path must be absolute")
        return value


class AwsStoreDocument(S3StoreDocument):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        json_schema_extra={
            "$id": "https://nashspence.github.io/riverhog/v1/config/a-riverhog-aws-store.schema.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
        },
    )

    read_mode: Literal["immediate", "restore_required"] = "restore_required"
    archive_storage_class: str = Field(default="DEEP_ARCHIVE", min_length=1)
    immediate_storage_class: str | None = None
    restore_tier: Literal["Bulk", "Standard", "Expedited"] = "Bulk"
    restore_days: int = Field(default=3, ge=1)
    cloudfront: AwsCloudFrontDocument | None = None


AWS_CONFIG_SCHEMA: dict[str, object] = json.loads(
    files("a_riverhog_aws_store").joinpath("config.schema.json").read_text(encoding="utf-8")
)


def load_config(path: Path) -> AwsStoreDocument:
    return AwsStoreDocument.model_validate(load_validated_yaml_config(path, AWS_CONFIG_SCHEMA))


_CLI_RESULT_CONTRACT = {
    "format": "riverhog-cli-result-contract/v1",
    "identity_prefix": "a-riverhog-aws-store-cli-result",
    "default_profile": "runtime",
    "profiles": {
        "runtime": {
            "id": "a-riverhog-aws-store-cli-runtime/v1",
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
    "version_distribution": "a-riverhog-aws-store",
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
    cloudfront = _cloudfront_reader(config.cloudfront)
    client = create_s3_client(client_config, tuning=tuning)
    if args.provision_root:
        if cloudfront is not None:
            cloudfront.close()
        provision_storage_incarnation(client, bucket=config.bucket, root_prefix=config.root_prefix)
        return 0
    preparation = (
        AwsDeepArchiveReadPreparation(
            tier=config.restore_tier,
            days=config.restore_days,
        )
        if config.read_mode == "restore_required"
        else None
    )
    adapter = S3StorageAdapter(
        client,
        S3StorageAdapterConfig(
            implementation_id="a-riverhog-aws-store/v1",
            implementation_version=importlib.metadata.version(SERVICE),
            bucket=config.bucket,
            root_prefix=config.root_prefix,
            read_mode=config.read_mode,
            archive_storage_class=config.archive_storage_class,
            immediate_storage_class=config.immediate_storage_class,
            read_chunk_bytes=config.read_chunk_bytes,
        ),
        read_preparation=preparation,
        object_reader=cloudfront,
    )

    def readiness() -> None:
        client.head_bucket(Bucket=config.bucket)

    app = create_storage_adapter_app(
        service=SERVICE,
        token=token,
        adapter=adapter,
        readiness=readiness,
    )
    try:
        uvicorn.run(app, host=args.host, port=args.port)
    finally:
        if cloudfront is not None:
            cloudfront.close()
    return 0


def _cloudfront_reader(config: AwsCloudFrontDocument | None) -> AwsCloudFrontObjectReader | None:
    if config is None:
        return None
    return AwsCloudFrontObjectReader(
        AwsCloudFrontConfig(
            base_url=config.base_url,
            public_key_id=config.public_key_id,
            private_key_path=config.private_key_path,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["AWS_CONFIG_SCHEMA", "AwsStoreDocument", "SERVICE", "load_config", "main"]
