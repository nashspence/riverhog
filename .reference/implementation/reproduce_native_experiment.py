"""Replay the published captured inputs and report serial Native observations.

Reference-only command. Riverhog's maintained performance entrypoint remains
``make profile``. No elapsed threshold, speedup, durability or F1 claim is made.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import sqlite3
import subprocess
from pathlib import Path
from typing import Any

import riverhog_provenance
from native_execution_experiment import run_journal
from riverhog_provenance_contracts import ContractCatalog

BASE = "c28eceaa2dfec1b3712135469ba12230e74f5ffa"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--captured", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    implementation = Path(__file__).resolve().parent
    repository = implementation.parents[1]
    expected_package = repository / "packages/riverhog-provenance/src/riverhog_provenance"
    if Path(riverhog_provenance.__file__).resolve().parent != expected_package:
        raise RuntimeError("the Native environment does not import this audited checkout")
    subprocess.run(
        ["git", "diff", "--exit-code", BASE, "--", ".", ":(exclude).reference"],
        cwd=repository,
        check=True,
        capture_output=True,
    )
    original = json.loads(arguments.captured.read_text())
    catalog = ContractCatalog()
    cases: list[dict[str, Any]] = []
    for case in original["captured_once_comparisons"]:
        raw = bytes.fromhex(case["captured_input_hex"])
        if hashlib.sha256(raw).hexdigest() != case["input_sha256"]:
            raise ValueError("captured input hash mismatch")
        baseline = run_journal(raw, volatile=False, catalog=catalog, rows=64)
        experiment = run_journal(raw, volatile=True, catalog=catalog, rows=64)
        if baseline["outcome"] != experiment["outcome"]:
            raise AssertionError("real Native outcomes differ")
        evidence = baseline["outcome"].get("success")
        if evidence != case["complete_evidence"]:
            raise AssertionError("complete archived fixture evidence differs")
        cases.append(
            {
                "example": case["example"],
                "input_sha256": case["input_sha256"],
                "input_octets": len(raw),
                "captured_input_hex": raw.hex(),
                "rows_per_original_advance": 64,
                "same_complete_evidence": True,
                "complete_evidence": evidence,
                "baseline": baseline["measurements"],
                "experiment": experiment["measurements"],
            }
        )
    report = {
        "format": "reference-native-parent-experiment/v1",
        "audited_product_source": BASE,
        "python": platform.python_version(),
        "sqlite": sqlite3.sqlite_version,
        "scope": (
            "One serial instrumented pair per captured input, baseline first. "
            "Disposable parent replacement only; child checkpoints/transactions remain. "
            "No retained recovery, integrated policy, F1, durability or speedup claim."
        ),
        "script_sha256": hashlib.sha256(
            (implementation / "native_execution_experiment.py").read_bytes()
        ).hexdigest(),
        "test_sha256": hashlib.sha256(
            (implementation / "test_native_execution_experiment.py").read_bytes()
        ).hexdigest(),
        "reproducer_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "captured_once_comparisons": cases,
    }
    arguments.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(f"Verified {len(cases)} exact captured Native comparisons; wrote {arguments.output}")


if __name__ == "__main__":
    main()
