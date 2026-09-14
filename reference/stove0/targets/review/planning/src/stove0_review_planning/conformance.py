"""Combined qualification report for the deliberate review planning bridge."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence

from stove0_media_sampling_observer_contracts import MEDIA_SAMPLING_OBSERVER_CONTRACT
from stove0_review_target_contracts import REVIEW_MATERIALIZE_OPERATION

_CLI_RESULT_CONTRACT: dict[str, object] = {
    "schema": "riverhog-cli-result-contract/v1",
    "identity_prefix": "stove0-review-planning-cli-result",
    "default_profile": "machine-report",
    "profiles": {
        "machine-report": {
            "id": "stove0-review-planning-cli/v1",
            "structured_output": "always-json",
            "human_json_relationship": "not-applicable",
            "success": [
                {
                    "id": "reported",
                    "exit_status": 0,
                    "stdout": {"json": "stove0-review-contract-report/v1"},
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
    return {
        "format": "stove0-review-contract-report/v1",
        "observer_contract": MEDIA_SAMPLING_OBSERVER_CONTRACT.model_dump(mode="json"),
        "operation_contract": REVIEW_MATERIALIZE_OPERATION.model_dump(mode="json"),
        "source_retirement_permitted": False,
        "status": "conformant",
    }


def _parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(
        prog="stove0-review-planning",
        description="Print the maintained Stove0 review planning contract identities.",
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    parser.parse_args(argv)
    print(json.dumps(contract_report(), sort_keys=True, separators=(",", ":")))
    return 0


__all__ = ["contract_report", "main"]
