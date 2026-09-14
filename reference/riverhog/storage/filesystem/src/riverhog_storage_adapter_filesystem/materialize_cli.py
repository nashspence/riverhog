"""Operator CLI for adapter-owned independent object materialization."""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from riverhog_storage_adapter_filesystem.materialize import (
    MaterializationError,
    MaterializationSelection,
    materialize_committed_objects,
)

COMMAND = "riverhog-storage-adapter-filesystem-materialize"
_MATERIALIZATION_OUTPUT = {
    "kind": "cli-local-json-schema",
    "identity": "riverhog-filesystem-materialization-result/v1",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "format",
            "destination",
            "selected_objects",
            "selected_bytes",
            "source_metadata_bytes",
            "destination_verified_objects",
            "destination_verified_bytes",
            "staging_verified_bytes",
            "copied_objects",
            "copied_bytes",
        ],
        "properties": {
            "format": {"const": "riverhog-filesystem-materialization-result/v1"},
            "destination": {"type": "string"},
            "selected_objects": {"type": "integer", "minimum": 0},
            "selected_bytes": {"type": "integer", "minimum": 0},
            "source_metadata_bytes": {"type": "integer", "minimum": 0},
            "destination_verified_objects": {"type": "integer", "minimum": 0},
            "destination_verified_bytes": {"type": "integer", "minimum": 0},
            "staging_verified_bytes": {"type": "integer", "minimum": 0},
            "copied_objects": {"type": "integer", "minimum": 0},
            "copied_bytes": {"type": "integer", "minimum": 0},
        },
    },
}

_CLI_RESULT_CONTRACT = {
    "schema": "riverhog-cli-result-contract/v1",
    "identity_prefix": "riverhog-storage-adapter-filesystem-materialize-cli-result",
    "default_profile": "human-json",
    "profiles": {
        "human-json": {
            "id": "riverhog-storage-adapter-filesystem-materialize-cli/v1",
            "structured_output": "optional-json",
            "human_json_relationship": "same-semantic-result",
            "success": [
                {
                    "id": "completed",
                    "exit_status": 0,
                    "stdout": {
                        "human": "noncontractual-presentation-of-command-result",
                        "json": _MATERIALIZATION_OUTPUT,
                    },
                    "stderr": {"all": "empty"},
                }
            ],
            "failures": [
                {
                    "id": "usage",
                    "exit_status": 2,
                    "stdout": {"all": "empty"},
                    "stderr": {"all": "noncontractual-usage-diagnostic"},
                },
                {
                    "id": "materialization",
                    "exit_status": 1,
                    "stdout": {"all": "empty"},
                    "stderr": {"all": "noncontractual-diagnostic"},
                },
            ],
        }
    },
    "command_profiles": {},
    "command_overrides": {},
    "executable_groups": [],
    "outcome_selectors": {
        "completed": {"kind": "materialization-completed"},
        "usage": {"kind": "parser-rejected-invocation"},
        "materialization": {"kind": "materialization-error"},
    },
    "output_authorities": {},
    "version_distribution": "riverhog-storage-adapter-filesystem",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=COMMAND,
        description=(
            "Materialize an exact committed logical-object projection from a quiesced "
            "Riverhog filesystem storage-adapter root."
        ),
    )
    parser.add_argument("--version", action="version", version=_version())
    parser.add_argument("source", type=Path, help="quiesced filesystem adapter root")
    parser.add_argument("destination", type=Path, help="new logical object-tree directory")
    selectors = parser.add_argument_group("selection")
    selectors.add_argument(
        "--path",
        action="append",
        default=[],
        help="include one exact logical object path; repeat as needed",
    )
    selectors.add_argument(
        "--prefix",
        action="append",
        default=[],
        help="include one logical object prefix; repeat as needed",
    )
    selectors.add_argument(
        "--all",
        action="store_true",
        dest="all_objects",
        help="include every current committed object instead of path selectors",
    )
    parser.add_argument("--json", action="store_true", help="emit one JSON result object")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.all_objects == bool(args.path or args.prefix):
        parser.error("select --all or at least one --path/--prefix, but not both")
    try:
        selection = MaterializationSelection(
            paths=tuple(sorted(set(args.path))),
            prefixes=tuple(sorted(set(args.prefix))),
            all_objects=args.all_objects,
        )
        summary = materialize_committed_objects(
            source=args.source,
            destination=args.destination,
            selection=selection,
        )
    except (MaterializationError, ValueError) as exc:
        print(f"{COMMAND}: {exc}", file=sys.stderr)
        return 1
    if args.json:
        print(
            json.dumps(
                summary.as_json(),
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ),
            flush=True,
        )
    else:
        print(
            f"Materialized {summary.selected_objects} objects "
            f"({summary.selected_bytes} bytes) to {summary.destination}; "
            f"copied={summary.copied_bytes} destination-verified="
            f"{summary.destination_verified_bytes} staging-verified="
            f"{summary.staging_verified_bytes} source-metadata="
            f"{summary.source_metadata_bytes}",
            flush=True,
        )
    return 0


def _version() -> str:
    try:
        return importlib.metadata.version("riverhog-storage-adapter-filesystem")
    except importlib.metadata.PackageNotFoundError:
        return "0.1.0"


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["COMMAND", "build_parser", "main"]
