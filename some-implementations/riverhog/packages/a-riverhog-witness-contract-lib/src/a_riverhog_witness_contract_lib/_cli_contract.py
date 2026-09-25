"""Shared result declaration shape for the two independent witness CLIs."""

from __future__ import annotations

from collections.abc import Mapping


def object_schema(properties: Mapping[str, object]) -> dict[str, object]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": list(properties),
        "properties": dict(properties),
    }


TEXT: dict[str, object] = {"type": "string"}
OPTIONAL_TEXT: dict[str, object] = {"type": ["string", "null"]}
COUNT: dict[str, object] = {"type": "integer", "minimum": 0}
OPTIONAL_COUNT: dict[str, object] = {"type": ["integer", "null"], "minimum": 0}
BOOLEAN: dict[str, object] = {"type": "boolean"}

STATUS = object_schema(
    {
        "name": TEXT,
        "condition": {
            "enum": ["empty", "current", "upgrade_required", "unversioned", "incompatible"]
        },
        "current_revision": OPTIONAL_TEXT,
        "head_revision": TEXT,
    }
)
PROGRESS = object_schema(
    {
        "generation": COUNT,
        "serial": COUNT,
        "phase": {"enum": ["new", "catalog", "catchup", "following", "reset_required"]},
        "source_identity": OPTIONAL_TEXT,
        "authorization_view_identity": OPTIONAL_TEXT,
        "through_revision": TEXT,
        "reset_reason": OPTIONAL_TEXT,
    }
)


def result_contract(
    distribution: str, outputs: Mapping[str, dict[str, object]]
) -> dict[str, object]:
    common = {
        "id": f"{distribution}-cli-json/v1",
        "structured_output": "always-json",
        "human_json_relationship": "not-applicable",
        "success": [
            {
                "id": "completed",
                "exit_status": 0,
                "stdout": {"json": "$command-json-output"},
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
                "id": "application-error",
                "exit_status": 1,
                "stdout": {"all": "empty"},
                "stderr": {"all": "noncontractual-diagnostic"},
            },
        ],
    }
    runtime = {
        "id": f"{distribution}-cli-runtime/v1",
        "structured_output": "none",
        "human_json_relationship": "not-applicable",
        "success": [
            {
                "id": "runtime-returned",
                "exit_status": 0,
                "stdout": {"all": "no-command-result"},
                "stderr": {"all": "empty"},
            }
        ],
        "failures": common["failures"],
    }
    return {
        "format": "riverhog-cli-result-contract/v1",
        "identity_prefix": f"{distribution}-cli-result",
        "default_profile": "json",
        "profiles": {"json": common, "runtime": runtime},
        "command_profiles": {"run": "runtime"},
        "command_overrides": {},
        "executable_groups": [],
        "outcome_selectors": {
            "completed": {"kind": "command-completed"},
            "runtime-returned": {"kind": "service-runtime-returned"},
            "usage": {"kind": "parser-rejected-invocation"},
            "application-error": {"kind": "application-error"},
        },
        "output_authorities": {
            command: {
                "kind": "cli-local-json-schema",
                "identity": f"{distribution}-cli-{command.replace(' ', '-')}/v1",
                "schema": schema,
            }
            for command, schema in outputs.items()
        },
        "version_distribution": distribution,
    }
