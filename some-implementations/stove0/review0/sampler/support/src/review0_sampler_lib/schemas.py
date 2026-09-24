"""Machine-readable sampler protocol schema inventory."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
from collections.abc import Sequence
from typing import Any, Final

from http_api_contracts import (
    canonical_json_bytes,
    http_operation_inventory,
    structural_model_catalog,
)
from review0_sampler_protocol import (
    SAMPLER_HTTP_OPERATIONS,
    SamplerDescriptor,
    SamplerRequest,
    SamplerResult,
)

from review0_sampler_lib.conformance import SamplerConformanceResult

SAMPLER_SCHEMA_BUNDLE_FORMAT: Final = "review0-sampler-schema-bundle/v1"

_CLI_RESULT_CONTRACT = {
    "format": "riverhog-cli-result-contract/v1",
    "identity_prefix": "review0-sampler-schemas-cli-result",
    "default_profile": "machine-report",
    "profiles": {
        "machine-report": {
            "id": "review0-sampler-schemas-cli/v1",
            "structured_output": "always-json",
            "human_json_relationship": "not-applicable",
            "success": [
                {
                    "id": "emitted",
                    "exit_status": 0,
                    "stdout": {"json": SAMPLER_SCHEMA_BUNDLE_FORMAT},
                    "stderr": {"all": "empty"},
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
        "emitted": {"kind": "schema-bundle-emitted"},
        "usage": {"kind": "parser-rejected-invocation"},
    },
    "output_authorities": {},
    "version_distribution": "review0-sampler-lib",
}


def sampler_schema_bundle() -> dict[str, Any]:
    payload: dict[str, Any] = {
        "format": SAMPLER_SCHEMA_BUNDLE_FORMAT,
        "protocol": "review0-sampler/v1",
        "authorities": {
            "structural_models": "schemas",
            "http_operations": "http_binding.operations",
            "semantic_acceptance": "semantic_acceptance",
        },
        "http_binding": {
            "operations": http_operation_inventory(SAMPLER_HTTP_OPERATIONS),
        },
        "semantic_acceptance": {
            "kind": "request-bound-result",
            "validator": "validate_result",
        },
        "schemas": structural_model_catalog(
            SAMPLER_HTTP_OPERATIONS,
            additional_models=(
                SamplerDescriptor,
                SamplerRequest,
                SamplerResult,
                SamplerConformanceResult,
            ),
        ),
    }
    return {
        **payload,
        "bundle_sha256": hashlib.sha256(canonical_json_bytes(payload)).hexdigest(),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="review0-sampler-schemas")
    parser.add_argument(
        "--version",
        action="version",
        version=importlib.metadata.version("review0-sampler-lib"),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    parser.parse_args(argv)
    print(json.dumps(sampler_schema_bundle(), indent=2, sort_keys=True))
    return 0


__all__ = ["SAMPLER_SCHEMA_BUNDLE_FORMAT", "main", "sampler_schema_bundle"]
