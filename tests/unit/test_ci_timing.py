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


def test_source_identity_does_not_confuse_failed_status_with_a_clean_checkout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    results = iter(
        (
            subprocess.CompletedProcess(["git"], 0, stdout=str(ROOT)),
            subprocess.CompletedProcess(["git"], 0, stdout="a" * 40),
            subprocess.CompletedProcess(["git"], 128, stdout=""),
        )
    )
    monkeypatch.setattr(ci_timing.subprocess, "run", lambda *_args, **_kwargs: next(results))
    assert ci_timing.source_identity(ROOT) == {"source_sha": "a" * 40, "source_clean": False}


@pytest.mark.parametrize("revision", ["", "main", "a" * 39, "a" * 41])
def test_source_identity_requires_an_exact_revision(
    monkeypatch: pytest.MonkeyPatch, revision: str
) -> None:
    results = iter(
        (
            subprocess.CompletedProcess(["git"], 0, stdout=str(ROOT)),
            subprocess.CompletedProcess(["git"], 0, stdout=revision),
            subprocess.CompletedProcess(["git"], 0, stdout=""),
        )
    )
    monkeypatch.setattr(ci_timing.subprocess, "run", lambda *_args, **_kwargs: next(results))
    assert ci_timing.source_identity(ROOT) == {"source_sha": "", "source_clean": False}


def _git(root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        [
            "git",
            "-C",
            str(root),
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            *arguments,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return completed.stdout.strip()


@pytest.mark.parametrize("linked_worktree", [False, True])
def test_source_identity_binds_the_requested_checkout_and_observes_its_drift(
    tmp_path: Path, linked_worktree: bool
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    _git(source, "init", "-q")
    (source / "owned.txt").write_text("source")
    _git(source, "add", "owned.txt")
    _git(source, "commit", "-qm", "fixture")
    if linked_worktree:
        worktree = tmp_path / "worktree"
        _git(source, "worktree", "add", "--detach", "-q", str(worktree), "HEAD")
        source = worktree
    sha = _git(source, "rev-parse", "HEAD")
    assert ci_timing.source_identity(source) == {"source_sha": sha, "source_clean": True}
    (source / "owned.txt").write_text("changed source")
    assert ci_timing.source_identity(source) == {"source_sha": sha, "source_clean": False}
    _git(source, "checkout", "--", "owned.txt")
    (source / "untracked.txt").write_text("untracked source")
    assert ci_timing.source_identity(source) == {"source_sha": sha, "source_clean": False}


def test_source_identity_excludes_an_enclosing_checkout_even_when_its_head_changes(
    tmp_path: Path,
) -> None:
    enclosing = tmp_path / "enclosing"
    enclosing.mkdir()
    _git(enclosing, "init", "-q")
    (enclosing / ".gitignore").write_text("/scratch/\n")
    _git(enclosing, "add", ".gitignore")
    _git(enclosing, "commit", "-qm", "fixture")
    scratch = enclosing / "scratch"
    scratch.mkdir()
    first = ci_timing.source_identity(enclosing)
    assert first["source_clean"] is True
    assert ci_timing.source_identity(scratch) == {"source_sha": "", "source_clean": False}
    _git(enclosing, "commit", "--allow-empty", "-qm", "changed source")
    second = ci_timing.source_identity(enclosing)
    assert second["source_clean"] is True and second["source_sha"] != first["source_sha"]
    assert ci_timing.source_identity(scratch) == {"source_sha": "", "source_clean": False}


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


@pytest.mark.parametrize("elapsed", ["-1", "nan", "inf"])
def test_phase_rejects_invalid_elapsed_without_writing_a_false_observation(tmp_path, elapsed):
    output = tmp_path / "phases.jsonl"
    assert (
        main(
            (
                "phase",
                "--lane",
                "example",
                "--name",
                "cleanup",
                "--seconds",
                elapsed,
                "--status",
                "0",
                "--output",
                str(output),
            )
        )
        == 2
    )
    assert not output.exists()


def test_phase_storage_limit_preserves_the_existing_attempt(tmp_path, monkeypatch):
    output = tmp_path / "phases.jsonl"
    monkeypatch.setenv("RIVERHOG_CI_RUN_ID", "phase-attempt")
    arguments = (
        "phase",
        "--lane",
        "example",
        "--name",
        "cleanup",
        "--seconds",
        "1",
        "--status",
        "7",
        "--output",
        str(output),
    )
    assert main(arguments) == 0
    original = output.read_bytes()
    assert require_canonical_json(original.rstrip(b"\n"))["exit_status"] == 7
    monkeypatch.setenv("RIVERHOG_VALIDATION_MAX_BYTES", str(len(original)))
    assert main(arguments) == 2
    assert output.read_bytes() == original
