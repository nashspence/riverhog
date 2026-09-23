from __future__ import annotations

import argparse
import getpass
import importlib.metadata
import json
import os
import sys
from pathlib import Path

from a_riverhog_recovery_tool.recovery import (
    RecoveryError,
    read_recovery_descriptor,
    recover_archive,
    recover_collection_description,
    recover_collection_tags,
)

_RECOVERED_TAGS_OUTPUT = {
    "kind": "cli-local-json-sequence",
    "identity": "riverhog-recovered-collection-tags/v1-json-sequence",
    "framing": "newline-delimited-json",
    "records": {
        "authority": {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "format",
                "record",
                "revision",
                "tag_set_identity",
                "head_identity",
            ],
            "properties": {
                "format": {"const": "riverhog-recovered-collection-tags/v1"},
                "record": {"const": "authority"},
                "revision": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 9_007_199_254_740_991,
                },
                "tag_set_identity": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
                "head_identity": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
            },
        },
        "tag": {
            "type": "object",
            "additionalProperties": False,
            "required": ["record", "tag"],
            "properties": {
                "record": {"const": "tag"},
                "tag": {"type": "string"},
            },
        },
        "complete": {
            "type": "object",
            "additionalProperties": False,
            "required": ["record", "tag_count"],
            "properties": {
                "record": {"const": "complete"},
                "tag_count": {"type": "integer", "minimum": 0},
            },
        },
    },
    "sequence": {"start": "authority", "repeated": "tag", "end": "complete"},
}

_CLI_RESULT_CONTRACT = {
    "schema": "riverhog-cli-result-contract/v1",
    "identity_prefix": "a-riverhog-recovery-tool-cli-result",
    "default_profile": "recovery",
    "profiles": {
        "recovery": {
            "id": "a-riverhog-recovery-tool-cli/v1",
            "structured_output": "mode-specific",
            "human_json_relationship": "mode-specific-results",
            "success": [
                {
                    "id": "archive-recovered",
                    "exit_status": 0,
                    "stdout": {"human": "noncontractual-recovery-summary"},
                    "stderr": {"all": "empty"},
                },
                {
                    "id": "description-recovered",
                    "exit_status": 0,
                    "stdout": {
                        "json": {
                            "kind": "schema-format-or-null",
                            "identity": "riverhog-collection-description/v1",
                        }
                    },
                    "stderr": {"all": "empty"},
                },
                {
                    "id": "tags-recovered",
                    "exit_status": 0,
                    "stdout": {"json": _RECOVERED_TAGS_OUTPUT},
                    "stderr": {"all": "empty"},
                },
            ],
            "failures": [
                {
                    "id": "usage",
                    "exit_status": 2,
                    "stdout": {"all": "empty"},
                    "stderr": {"all": "noncontractual-usage-diagnostic"},
                },
                {
                    "id": "recovery",
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
        "archive-recovered": {
            "kind": "options-absent",
            "parameters": ["description_only", "tags_only"],
        },
        "description-recovered": {
            "kind": "option-equals",
            "parameter": "description_only",
            "value": True,
        },
        "tags-recovered": {
            "kind": "option-equals",
            "parameter": "tags_only",
            "value": True,
        },
        "usage": {"kind": "parser-rejected-invocation"},
        "recovery": {"kind": "recovery-error"},
    },
    "output_authorities": {},
    "version_distribution": "a-riverhog-recovery-tool",
}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="a-riverhog-recovery-tool",
        description="Recover one complete Riverhog archive copy without Riverhog.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=importlib.metadata.version("a-riverhog-recovery-tool"),
    )
    parser.add_argument(
        "archive", type=Path, help="materialized canonical logical archive directory"
    )
    parser.add_argument("output", type=Path, nargs="?", help="new directory for recovered files")
    metadata = parser.add_mutually_exclusive_group()
    metadata.add_argument(
        "--description-only",
        action="store_true",
        help="validate and emit only description.json.age without reading collection payloads",
    )
    metadata.add_argument(
        "--tags-only",
        action="store_true",
        help="validate and stream the exact tag authority without reading collection payloads",
    )
    parser.add_argument(
        "--passphrases-file",
        type=Path,
        help="read an opaque key-ID to passphrase JSON map from a permission-restricted file",
    )
    parser.add_argument("--age-command", default="age", help=argparse.SUPPRESS)
    return parser


def _passphrases(path: Path | None, *, archive: Path) -> dict[str, str]:
    descriptor = read_recovery_descriptor(archive)
    passphrase_id = descriptor.encryption.passphrase_id
    if path is None:
        value = getpass.getpass(f"Archive passphrase ({passphrase_id}): ")
        if not value:
            raise RecoveryError(f"archive passphrase is empty for key ID {passphrase_id}")
        return {passphrase_id: value}
    try:
        if os.name != "nt" and path.stat().st_mode & 0o077:
            raise RecoveryError("passphrases file must not be accessible by group or others")
        payload = json.loads(path.read_text(encoding="utf-8"))
    except RecoveryError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RecoveryError(f"cannot read passphrases file: {exc}") from exc
    if not isinstance(payload, dict) or not all(
        isinstance(key, str) and isinstance(value, str) and value for key, value in payload.items()
    ):
        raise RecoveryError("passphrases file must contain a string-to-string JSON object")
    return payload


def main() -> None:
    args = _parser().parse_args()
    try:
        passphrases = _passphrases(args.passphrases_file, archive=args.archive)
        if args.description_only:
            if args.output is not None:
                raise RecoveryError("output must be omitted with --description-only")
            description = recover_collection_description(
                args.archive,
                passphrases=passphrases,
                age_command=args.age_command,
            )
            print(
                "null" if description is None else description.to_json_bytes().decode("utf-8"),
                flush=True,
            )
            return
        if args.tags_only:
            if args.output is not None:
                raise RecoveryError("output must be omitted with --tags-only")
            recovered = recover_collection_tags(
                args.archive,
                passphrases=passphrases,
                age_command=args.age_command,
            )
            print(
                json.dumps(
                    {
                        "format": "riverhog-recovered-collection-tags/v1",
                        "record": "authority",
                        "revision": recovered.head.revision,
                        "tag_set_identity": recovered.head.tag_set_identity,
                        "head_identity": recovered.head.head_identity,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                flush=True,
            )
            count = 0
            for tag in recovered.iter_tags():
                print(
                    json.dumps(
                        {"record": "tag", "tag": tag},
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    flush=True,
                )
                count += 1
            print(
                json.dumps(
                    {"record": "complete", "tag_count": count},
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                flush=True,
            )
            return
        if args.output is None:
            raise RecoveryError(
                "output is required unless --description-only or --tags-only is used"
            )
        summary = recover_archive(
            args.archive,
            args.output,
            passphrases=passphrases,
            age_command=args.age_command,
        )
    except RecoveryError as exc:
        print(f"a-riverhog-recovery-tool: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    print(
        f"Recovered {summary.files} files ({summary.bytes} bytes) to {summary.output}; "
        f"provenance={summary.provenance_mode} journals={summary.provenance_journals}",
        flush=True,
    )


if __name__ == "__main__":
    main()
