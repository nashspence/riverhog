from __future__ import annotations

import json
import sys
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import canonical_json_bytes

from scripts import ci_qualification as qualification
from scripts import ci_timing as timing
from tests.unit.test_validation_evidence import RUN, SOURCE, row


@pytest.fixture(autouse=True)
def isolated_make_execution_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each case supplies its own parent flags, independent of the invoking shard."""

    for variable in ("MAKEFLAGS", "MFLAGS", "MAKEOVERRIDES"):
        monkeypatch.delenv(variable, raising=False)


@pytest.mark.parametrize(
    "variable", ["TESTS", "args", "PYTEST_ADDOPTS", "MAKEFILES", "MAKEFLAGS", "MAKEOVERRIDES"]
)
def test_required_execution_rejects_unrecorded_scope_reduction(
    monkeypatch: pytest.MonkeyPatch, variable: str
) -> None:
    work = qualification.ValidationWork("unit", ("make", "unit"))
    monkeypatch.setenv(variable, "narrowed-work")
    with pytest.raises(ValueError, match="overrides"):
        qualification.validation_environment(work, RUN)


@pytest.mark.parametrize("flags", ["-j1", "--jobs=1", "w -j1", "--no-print-directory -j1"])
def test_sequential_parent_make_flags_do_not_change_required_child_work(monkeypatch, flags):
    monkeypatch.setenv("MAKEFLAGS", flags)
    monkeypatch.setenv("MFLAGS", "-j1")
    work = qualification.ValidationWork("unit", ("make", "unit"))
    environment = qualification.validation_environment(work, RUN)
    assert "MAKEFLAGS" not in environment
    assert "MFLAGS" not in environment
    assert work.command == ("make", "unit")


def test_running_attempt_records_start_and_conditions_before_execution(tmp_path, monkeypatch):
    work = qualification.ValidationWork("start-proof", ("true",))
    monkeypatch.setattr(qualification, "validation_workloads", lambda: (work,))
    monkeypatch.setattr(
        timing, "source_identity", lambda _: {"source_sha": SOURCE, "source_clean": True}
    )
    output = tmp_path / "attempt.json"

    def run(command, **_kwargs):
        record = json.loads(output.read_bytes())
        assert record["state"] == "running"
        assert record["started_at"]
        assert record["started_monotonic_seconds"] >= 0
        assert set(record["observed_conditions"]) == {"platform", "architecture", "python"}
        return 0

    monkeypatch.setattr(timing, "run_validation_child", run)
    assert timing.execute_validation(work.name, output, RUN) == 0


@pytest.mark.parametrize("name", ["lint", "unit", "build", "stove0-scale"])
def test_required_work_needs_no_target_or_approval_state(name):
    result = qualification.require_validation_work([name])
    assert result[0].name == name
    assert "timing_policy" not in result[0].definition()


def test_new_known_work_is_immediately_measurable_without_target(monkeypatch):
    monkeypatch.setattr(qualification, "REPOSITORY_TARGETS", ("new-proof",))
    assert qualification.require_validation_work(["new-proof"])[0].target_seconds is None


@pytest.mark.parametrize("names", [(), ("lint", "lint"), ("unregistered",)])
def test_invalid_actual_work_coverage_is_still_rejected(names):
    with pytest.raises(ValueError):
        qualification.require_validation_work(names)


@pytest.mark.parametrize("seconds", [1, 601, 1e12])
@pytest.mark.parametrize("target", [None, 600])
@pytest.mark.parametrize("code,growth", [(0, "pass"), (7, "pass"), (0, "fail")])
def test_check_cli_never_promotes_target_into_exit_status(
    tmp_path, monkeypatch, seconds, target, code, growth
):
    work = qualification.ValidationWork(
        "test", ("true",), target, growth_witnesses=("owner/bound",)
    )
    monkeypatch.setattr(qualification, "validation_workloads", lambda: (work,))
    path = tmp_path / "run.json"
    path.write_bytes(canonical_json_bytes(row(work, seconds, code, {"owner/bound": growth})))
    status = timing.main(
        [
            "check-evidence",
            "--workload",
            work.name,
            "--source-sha",
            SOURCE,
            "--run-id",
            RUN,
            str(path),
        ]
    )
    assert status == (1 if code or growth == "fail" else 0)


def test_cli_evidence_absence_not_optional_target_absence_fails(tmp_path, monkeypatch):
    work = qualification.ValidationWork("test", ("true",))
    monkeypatch.setattr(qualification, "validation_workloads", lambda: (work,))
    path = tmp_path / "run.json"
    path.write_bytes(canonical_json_bytes(row(work)))
    args = [
        "check-evidence",
        "--workload",
        work.name,
        "--source-sha",
        SOURCE,
        "--run-id",
        RUN,
        str(path),
    ]
    assert timing.main(args) == 0
    path.unlink()
    assert timing.main(args) == 2


@pytest.mark.parametrize("target", [None, 600])
@pytest.mark.parametrize("growth", ["pass", "fail", "missing"])
def test_runner_records_once_without_speed_gate_and_preserves_growth(
    tmp_path, monkeypatch, target, growth, capsys
):
    work = qualification.ValidationWork("test", ("true",), target, growth_witnesses=("bound",))
    monkeypatch.setattr(qualification, "validation_workloads", lambda: (work,))
    monkeypatch.setattr(
        timing, "source_identity", lambda _: {"source_sha": SOURCE, "source_clean": True}
    )
    ticks = iter([0.0, 0.0, 1e9, 1e9])
    monkeypatch.setattr(timing, "time", SimpleNamespace(monotonic=lambda: next(ticks)))
    timers, calls = [], []

    class InstantTimer:
        def __init__(self, seconds, callback):
            self.callback, self.cancelled = callback, False
            timers.append(self)

        def start(self):
            self.callback()

        def cancel(self):
            self.cancelled = True

    def run(command, **kwargs):
        calls.append(command)
        return 0

    monkeypatch.setattr(timing, "Timer", InstantTimer)
    monkeypatch.setattr(timing, "run_validation_child", run)
    result = timing.execute_validation(
        work.name,
        tmp_path / "run.json",
        RUN,
        growth_collector=lambda *_: {} if growth == "missing" else {"bound": growth},
    )
    assert result == {"pass": 0, "fail": 1, "missing": 2}[growth]
    assert len(calls) == 1
    assert all(timer.cancelled for timer in timers)
    data = json.loads((tmp_path / "run.json").read_bytes())
    assert data["elapsed_seconds"] == 1e9
    assert data["profiling_target"]["status"] == ("not-compared" if target is None else "missed")
    assert "approval" not in capsys.readouterr().err.lower()


@pytest.mark.parametrize(
    "exception,state,code", [(OSError, "launch-failed", 2), (KeyboardInterrupt, "interrupted", 130)]
)
@pytest.mark.parametrize("collection_error", [False, True])
def test_interrupted_and_failed_launches_are_not_target_outcomes(
    tmp_path, monkeypatch, exception, state, code, collection_error
):
    monkeypatch.setattr(
        timing, "source_identity", lambda _: {"source_sha": SOURCE, "source_clean": True}
    )

    def fail(*_, **__):
        raise exception()

    monkeypatch.setattr(timing, "run_validation_child", fail)

    def collect(*_):
        if collection_error:
            raise ValueError("incomplete owner growth evidence")
        return {}

    path = tmp_path / "run.json"
    assert timing.execute_validation("lint", path, RUN, growth_collector=collect) == code
    assert json.loads(path.read_bytes())["state"] == state


def test_native_subprocess_no_target_executes_once_and_no_comparison_blocks(tmp_path, monkeypatch):
    marker = tmp_path / "once"
    work = qualification.ValidationWork(
        "native",
        (
            sys.executable,
            "-c",
            f"from pathlib import Path; Path({str(marker)!r}).write_text('once')",
        ),
    )
    monkeypatch.setattr(qualification, "validation_workloads", lambda: (work,))
    monkeypatch.setattr(
        timing, "source_identity", lambda _: {"source_sha": SOURCE, "source_clean": True}
    )
    assert timing.execute_validation(work.name, tmp_path / "out.json", RUN) == 0
    assert marker.read_text() == "once"


def test_independent_safety_failure_retains_original_exit_not_speed_goal(tmp_path, monkeypatch):
    work = qualification.ValidationWork(
        "safety", (sys.executable, "-c", "raise SystemExit(8)"), 1e9
    )
    monkeypatch.setattr(qualification, "validation_workloads", lambda: (work,))
    monkeypatch.setattr(
        timing, "source_identity", lambda _: {"source_sha": SOURCE, "source_clean": True}
    )
    assert timing.execute_validation(work.name, tmp_path / "out.json", RUN) == 8


def test_recursive_make_directory_flags_are_observations_not_work_selectors(monkeypatch):
    work = qualification.ValidationWork("fixture", ("make", "unit"))
    monkeypatch.setenv("MAKEFLAGS", "w -j1")
    monkeypatch.setenv("MFLAGS", "-w -j1")
    env = qualification.validation_environment(work, "recursive-make")
    assert "MAKEFLAGS" not in env and "MFLAGS" not in env
    monkeypatch.setenv("MFLAGS", "-w -j4")
    with pytest.raises(ValueError, match="execution overrides"):
        qualification.validation_environment(work, "recursive-make")
