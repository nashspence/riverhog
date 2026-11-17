"""Run the reference witnesses once and retain their actual evidence.

Use the locked workspace Python environment from the repository root.
This producer is external reference scaffolding, not a new product command.
It does not run a product three-arm campaign or any provider operation.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from scripts.validation_identity import source_identity  # noqa: E402
from scripts.validation_numeric import (  # noqa: E402
    bind_numeric_environment,
    numeric_policy_identity,
    qualification_numeric_policy,
)


def input_hashes() -> dict[str, str]:
    return {
        path.relative_to(ROOT).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted((ROOT / ".reference/implementation").glob("*.py"))
    }


def main() -> int:
    temporary = Path(tempfile.mkdtemp(prefix="1000-reference-", dir=ROOT.parent))
    evidence = temporary / "evidence"
    evidence.mkdir()
    environment = dict(os.environ)
    policy = qualification_numeric_policy()
    bind_numeric_environment(environment, policy)
    environment.update(
        TMPDIR=str(temporary),
        RIVERHOG_CI_TIMING_DIR=str(temporary / "timing"),
        RIVERHOG_REFERENCE_SAFETY_OUTPUT=str(temporary / "faults"),
        NO_PROXY="*",
        no_proxy="*",
    )
    command = [
        sys.executable,
        "-m",
        "pytest",
        "-q",
        "-p",
        "scripts.ci_timing",
        "-o",
        "junit_family=xunit1",
        "--durations=10",
        "--basetemp",
        str(temporary / "pytest"),
        "--junitxml",
        str(evidence / "reference_final.xml"),
        ".reference/implementation",
    ]
    before = source_identity(ROOT)
    hashes_before = input_hashes()
    started_at = datetime.now(UTC).isoformat()
    started = time.monotonic()
    with (evidence / "reference_final.txt").open("w", encoding="utf-8") as output:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            env=environment,
            stdout=output,
            stderr=subprocess.STDOUT,
            check=False,
        )
    elapsed = time.monotonic() - started
    hashes_after = input_hashes()
    record = {
        "format": "riverhog-external-reference-check/v1",
        "scope": "Reference witnesses, not full product/cutover qualification",
        "command": command,
        "source_before": before,
        "source_after": source_identity(ROOT),
        "inputs_before": hashes_before,
        "inputs_after": hashes_after,
        "numeric_policy": numeric_policy_identity(policy),
        "started_at": started_at,
        "finished_at": datetime.now(UTC).isoformat(),
        "elapsed_seconds": elapsed,
        "exit_status": completed.returncode,
        "temporary_directory": str(temporary),
        "environment_overrides": {
            key: environment[key]
            for key in (
                "TMPDIR",
                "RIVERHOG_CI_TIMING_DIR",
                "RIVERHOG_REFERENCE_SAFETY_OUTPUT",
                "NO_PROXY",
                "no_proxy",
            )
        },
    }
    (evidence / "reference_final_command.json").write_text(
        json.dumps(record, indent=2) + "\n", encoding="utf-8"
    )
    if (temporary / "timing").exists():
        shutil.copytree(temporary / "timing", evidence / "reference_final_timing")
    if (temporary / "faults").exists():
        shutil.copytree(temporary / "faults", evidence / "reference_final_faults")
    print(
        json.dumps(
            {
                "exit_status": completed.returncode,
                "elapsed_seconds": elapsed,
                "input_hashes_unchanged": hashes_before == hashes_after,
                "evidence": str(evidence),
            },
            indent=2,
        )
    )
    if hashes_before != hashes_after:
        raise RuntimeError("reference input changed during its execution")
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
