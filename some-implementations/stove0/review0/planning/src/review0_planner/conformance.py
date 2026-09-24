"""Combined qualification report for the deliberate review planning bridge."""

from __future__ import annotations

import argparse
import copy
import json
from collections.abc import Sequence

from a_stove0_media_sampling_contract_lib import MEDIA_SAMPLING_OBSERVER_CONTRACT
from review0_target_contracts import REVIEW_MATERIALIZE_OPERATION

_REVIEW_CONTRACT_REPORT = {
    "format": "review0-contract-report/v1",
    "observer_contract": MEDIA_SAMPLING_OBSERVER_CONTRACT.model_dump(mode="json"),
    "operation_contract": REVIEW_MATERIALIZE_OPERATION.model_dump(mode="json"),
    "source_retirement_permitted": False,
    "status": "conformant",
}
_REVIEW_CONTRACT_REPORT_OUTPUT = {
    "kind": "cli-local-exact-json",
    "identity": "review0-contract-report/v1",
    "document": _REVIEW_CONTRACT_REPORT,
}

_CLI_RESULT_CONTRACT: dict[str, object] = {
    "schema": "riverhog-cli-result-contract/v1",
    "identity_prefix": "review0-planner-cli-result",
    "default_profile": "machine-report",
    "profiles": {
        "machine-report": {
            "id": "review0-planner-cli/v1",
            "structured_output": "always-json",
            "human_json_relationship": "not-applicable",
            "success": [
                {
                    "id": "reported",
                    "exit_status": 0,
                    "stdout": {"json": _REVIEW_CONTRACT_REPORT_OUTPUT},
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
        "reported": {"kind": "contract-report-completed"},
        "usage": {"kind": "parser-rejected-invocation"},
    },
    "output_authorities": {},
    "version_distribution": None,
}


def contract_report() -> dict[str, object]:
    return copy.deepcopy(_REVIEW_CONTRACT_REPORT)


def _parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(
        prog="review0-planner",
        description="Print Review0 planning contract identities.",
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    parser.parse_args(argv)
    print(json.dumps(contract_report(), sort_keys=True, separators=(",", ":")))
    return 0


__all__ = ["contract_report", "main"]
