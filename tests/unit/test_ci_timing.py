from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import require_canonical_json

from scripts import ci_timing
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


@pytest.mark.parametrize(
    ("seconds", "exit_status", "status"),
    [
        (599.9, 0, "met"),
        (600.0, 0, "missed"),
        (601.0, 0, "missed"),
        (1.0, 7, "not-compared"),
        (601.0, 7, "not-compared"),
    ],
)
def test_profiling_target_reports_without_changing_command_success_or_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    seconds: float,
    exit_status: int,
    status: str,
) -> None:
    clock = iter([10.0, 10.0 + seconds])
    monkeypatch.setattr(ci_timing, "time", SimpleNamespace(monotonic=lambda: next(clock)))
    monkeypatch.setattr(
        ci_timing, "source_identity", lambda _: {"source_sha": "a" * 40, "source_clean": True}
    )
    monkeypatch.setattr(
        ci_timing,
        "subprocess",
        SimpleNamespace(
            run=lambda command, **kwargs: subprocess.CompletedProcess(command, exit_status)
        ),
    )
    output = tmp_path / "profile.json"
    assert (
        main(
            [
                "run",
                "--lane",
                "scale",
                "--output",
                str(output),
                "--target-seconds",
                "600",
                "--",
                "qualification",
            ]
        )
        == exit_status
    )
    require_canonical_json(output.read_bytes())
    record = json.loads(output.read_bytes())
    assert record["elapsed_seconds"] == pytest.approx(seconds)
    assert record["exit_status"] == exit_status
    assert record["profiling_target"] == {
        "report_only": True,
        "target_seconds": 600,
        "status": status,
        "reason": "command-failed" if exit_status else "completed-command",
    }
    assert f"Profiling target {status}" in capsys.readouterr().err


@pytest.mark.parametrize("target", ["0", "-1", "nan", "inf"])
def test_profiling_target_rejects_invalid_measurement_budget(tmp_path: Path, target: str) -> None:
    with pytest.raises(SystemExit) as failure:
        main(
            [
                "run",
                "--lane",
                "scale",
                "--output",
                str(tmp_path / "profile.json"),
                "--target-seconds",
                target,
                "--",
                "qualification",
            ]
        )
    assert failure.value.code == 2
