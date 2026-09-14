from __future__ import annotations

import argparse
import importlib.metadata
import json
import logging
import sys
from pathlib import Path

from state_schema import StateSchemaError

from mango_fish.relay import MangoFish, load_config, summarize_config
from mango_fish.schema import state_schema

_STATE_STATUS_OUTPUT = {
    "kind": "cli-local-json-schema",
    "identity": "state-schema-status/v1",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "required": ["name", "condition", "current_revision", "head_revision"],
        "properties": {
            "name": {"type": "string"},
            "condition": {
                "enum": ["empty", "current", "upgrade_required", "unversioned", "incompatible"]
            },
            "current_revision": {"type": ["string", "null"]},
            "head_revision": {"type": "string"},
        },
    },
}

_CLI_RESULT_CONTRACT = {
    "schema": "riverhog-cli-result-contract/v1",
    "identity_prefix": "mango-fish-cli-result",
    "default_profile": "state-human-json",
    "profiles": {
        "state-human-json": {
            "id": "mango-fish-cli-state-human-json/v1",
            "structured_output": "optional-json",
            "human_json_relationship": "same-semantic-result",
            "success": [
                {
                    "id": "completed",
                    "exit_status": 0,
                    "stdout": {
                        "human": "noncontractual-presentation-of-command-result",
                        "json": "$command-json-output",
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
                    "id": "state-schema",
                    "exit_status": 1,
                    "stdout": {"all": "empty"},
                    "stderr": {"all": "mango-fish-state-schema-diagnostic/v1"},
                },
            ],
        },
        "relay-runtime": {
            "id": "mango-fish-cli-relay-runtime/v1",
            "structured_output": "mode-specific",
            "human_json_relationship": "mode-specific-results",
            "success": [
                {
                    "id": "configuration-check",
                    "exit_status": 0,
                    "stdout": {"all": "mango-fish-configuration-summary/v1"},
                    "stderr": {"all": "noncontractual-runtime-log-or-empty"},
                },
                {
                    "id": "relay-completed",
                    "exit_status": 0,
                    "stdout": {"all": "no-command-result"},
                    "stderr": {"all": "noncontractual-runtime-log-or-empty"},
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
                    "id": "relay-pass-failed",
                    "exit_status": 1,
                    "stdout": {"all": "no-command-result"},
                    "stderr": {"all": "noncontractual-runtime-log"},
                },
            ],
        },
    },
    "command_profiles": {"$root": "relay-runtime"},
    "command_overrides": {},
    "executable_groups": ["$root"],
    "outcome_selectors": {
        "completed": {"kind": "state-schema-operation-completed"},
        "configuration-check": {
            "kind": "option-equals",
            "parameter": "check",
            "value": True,
        },
        "relay-completed": {
            "kind": "option-equals",
            "parameter": "check",
            "value": False,
        },
        "relay-pass-failed": {"kind": "relay-pass-reported-failures"},
        "state-schema": {"kind": "state-schema-error"},
        "usage": {"kind": "parser-rejected-invocation"},
    },
    "output_authorities": {
        "state status": _STATE_STATUS_OUTPUT,
        "state upgrade": _STATE_STATUS_OUTPUT,
        "state verify": _STATE_STATUS_OUTPUT,
    },
    "version_distribution": "mango-fish",
}


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        prog="mango-fish",
        description="Relay durable CloudEvents logs to webhooks.",
    )
    result.add_argument(
        "--version",
        action="version",
        version=importlib.metadata.version("mango-fish"),
    )
    result.add_argument("--config", type=Path, required=True)
    result.add_argument("--check", action="store_true")
    result.add_argument("--once", action="store_true")
    subparsers = result.add_subparsers(dest="command")
    state = subparsers.add_parser("state", help="inspect or explicitly upgrade cursor state")
    state_subparsers = state.add_subparsers(dest="state_command", required=True)
    for command_name, help_text in (
        ("status", "show the current and required state revisions"),
        ("upgrade", "explicitly upgrade state to the current revision"),
        ("verify", "verify the current revision and exact state schema"),
    ):
        command_parser = state_subparsers.add_parser(command_name, help=help_text)
        command_parser.add_argument("--json", action="store_true", help="Emit JSON.")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    config = load_config(args.config)
    if args.command == "state":
        schema = state_schema(config.state_path)
        try:
            if args.state_command == "status":
                status = schema.status()
            elif args.state_command == "upgrade":
                status = schema.upgrade()
            else:
                status = schema.validate()
        except StateSchemaError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        payload = status.as_dict()
        if args.json:
            print(json.dumps(payload, sort_keys=True))
        else:
            print(
                f"mango-fish state: {payload['condition']} "
                f"({payload['current_revision'] or 'none'} -> {payload['head_revision']})"
            )
        return 0
    if args.check:
        print(json.dumps(summarize_config(config), sort_keys=True))
        return 0
    relay = MangoFish(config)
    if args.once:
        return 1 if relay.run_once().failures else 0
    else:
        relay.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
