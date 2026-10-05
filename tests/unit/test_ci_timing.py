from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from riverhog_canonical_json import require_canonical_json

from scripts.ci_timing import main

ROOT = Path(__file__).resolve().parents[2]


def test_lane_timing_preserves_command_failure_and_writes_canonical_observation(
    tmp_path: Path,
) -> None:
    output = tmp_path / "lane.json"
    assert (
        main(
            [
                "run",
                "--lane",
                "example",
                "--output",
                str(output),
                "--",
                sys.executable,
                "-c",
                "raise SystemExit(7)",
            ]
        )
        == 7
    )
    require_canonical_json(output.read_bytes())
    record = json.loads(output.read_bytes())
    assert record["exit_status"] == 7 and record["elapsed_seconds"] >= 0


def test_pytest_timing_measures_per_worker_session_fixture_duplication(tmp_path: Path) -> None:
    (tmp_path / "conftest.py").write_text(
        "import pytest\n@pytest.fixture(scope='session')\ndef expensive():\n    return 1\n"
    )
    for name in ("one", "two"):
        (tmp_path / f"test_{name}.py").write_text(
            "def test_fact(expensive):\n    assert expensive == 1\n"
        )
    output = tmp_path / "timings"
    observed = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-n",
            "2",
            "--dist=loadscope",
            "-p",
            "scripts.ci_timing",
            str(tmp_path),
        ],
        cwd=tmp_path,
        env={
            **os.environ,
            "PYTHONPATH": str(ROOT),
            "RIVERHOG_CI_TIMING_DIR": str(output),
            "RIVERHOG_CI_LANE": "fixture-probe",
            "RIVERHOG_CI_RUN_ID": "fixture-probe-run",
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert observed.returncode == 0, observed.stdout + observed.stderr
    records = []
    for path in output.rglob("*.json"):
        require_canonical_json(path.read_bytes())
        records.append(json.loads(path.read_bytes()))
    assert {record["worker"] for record in records} == {"gw0", "gw1", "controller"}
    assert {record["run_id"] for record in records} == {"fixture-probe-run"}
    workers = [record for record in records if record["worker"] != "controller"]
    assert sum(record["fixtures"]["expensive"]["count"] for record in workers) == 2
    assert sum(len(record["tests"]) for record in workers) == 2
    assert all(record["exit_status"] == 0 for record in records)
