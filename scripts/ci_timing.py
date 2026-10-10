"""Observational lane, phase, and pytest timing; never a performance gate."""

from __future__ import annotations

import argparse
import math
import os
import subprocess
import sys
import time
from collections.abc import Generator, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from riverhog_canonical_json import canonical_json_bytes


def source_identity(root: Path) -> dict[str, object]:
    def git(*args: str) -> str:
        result = subprocess.run(
            ["git", "-C", str(root), *args], capture_output=True, text=True, check=False
        )
        return result.stdout.strip() if result.returncode == 0 else ""

    sha = git("rev-parse", "HEAD")
    return {"source_sha": sha, "source_clean": bool(sha) and not git("status", "--porcelain")}


def write_record(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_bytes(canonical_json_bytes(value))
    temporary.replace(path)


class PytestTiming:
    def __init__(self, config: pytest.Config) -> None:
        worker = getattr(config, "workerinput", {})
        self.worker_id = worker.get("workerid", "controller")
        self.run_id = worker.get(
            "ci_timing_run_id", os.environ.get("RIVERHOG_CI_RUN_ID", uuid4().hex)
        )
        self.lane = os.environ.get("RIVERHOG_CI_LANE", "unit")
        root = Path(os.environ.get("RIVERHOG_CI_TIMING_DIR", "build/ci-timing"))
        self.directory = root / self.lane / self.run_id
        self.source = source_identity(config.rootpath)
        self.started_at = datetime.now(UTC).isoformat()
        self.started = time.monotonic()
        self.fixtures: dict[str, dict[str, int | float]] = {}
        self.tests: dict[str, dict[str, dict[str, object]]] = {}

    @pytest.hookimpl(optionalhook=True)
    def pytest_configure_node(self, node: Any) -> None:
        node.workerinput["ci_timing_run_id"] = self.run_id

    @pytest.hookimpl(wrapper=True)
    def pytest_fixture_setup(
        self, fixturedef: pytest.FixtureDef[Any], request: pytest.FixtureRequest
    ) -> Generator[None, Any, Any]:
        started = time.monotonic()
        try:
            return (yield)
        finally:
            metric = self.fixtures.setdefault(fixturedef.argname, {"count": 0, "seconds": 0.0})
            metric["count"] += 1
            metric["seconds"] += time.monotonic() - started

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        self.tests.setdefault(report.nodeid, {})[report.when] = {
            "seconds": report.duration,
            "outcome": report.outcome,
        }

    def pytest_sessionfinish(self, session: pytest.Session, exitstatus: int) -> None:
        write_record(
            self.directory / f"{self.worker_id}.json",
            {
                "format": "riverhog-ci-pytest-timing/v1",
                **self.source,
                "lane": self.lane,
                "run_id": self.run_id,
                "worker": self.worker_id,
                "started_at": self.started_at,
                "elapsed_seconds": time.monotonic() - self.started,
                "exit_status": int(exitstatus),
                "fixtures": self.fixtures,
                "tests": self.tests,
            },
        )

    def pytest_terminal_summary(self, terminalreporter: Any) -> None:
        terminalreporter.write_line(f"Machine-readable pytest timings: {self.directory}")


def pytest_configure(config: pytest.Config) -> None:
    config.pluginmanager.register(PytestTiming(config), "riverhog-ci-timing-recorder")


def _positive_seconds(value: str) -> float:
    try:
        seconds = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be positive finite seconds") from exc
    if not math.isfinite(seconds) or seconds <= 0:
        raise argparse.ArgumentTypeError("must be positive finite seconds")
    return seconds


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser(
        "run", help="Time a maintained target or command without changing it."
    )
    run.add_argument("--lane", required=True)
    run.add_argument("--output", type=Path, required=True)
    run.add_argument(
        "--target-seconds",
        type=_positive_seconds,
        help="Report an under-duration target without changing the command exit status.",
    )
    run.add_argument("argv", nargs=argparse.REMAINDER)
    phase = commands.add_parser("phase", help="Append a completed shell lifecycle phase.")
    phase.add_argument("--lane", required=True)
    phase.add_argument("--name", required=True)
    phase.add_argument("--seconds", required=True, type=float)
    phase.add_argument("--status", required=True, type=int)
    phase.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    identity = source_identity(Path(__file__).resolve().parents[1])
    if args.command == "phase":
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("ab") as destination:
            destination.write(
                canonical_json_bytes(
                    {
                        "format": "riverhog-ci-phase-timing/v1",
                        **identity,
                        "lane": args.lane,
                        "run_id": os.environ.get("RIVERHOG_CI_RUN_ID", ""),
                        "phase": args.name,
                        "elapsed_seconds": args.seconds,
                        "exit_status": args.status,
                    }
                )
                + b"\n"
            )
        return 0
    command = args.argv[1:] if args.argv[:1] == ["--"] else args.argv
    if not command:
        parser.error("run requires a command after --")
    started = time.monotonic()
    run_id = uuid4().hex
    env = {**os.environ, "RIVERHOG_CI_LANE": args.lane, "RIVERHOG_CI_RUN_ID": run_id}
    result = subprocess.run(command, env=env, check=False)
    elapsed = time.monotonic() - started
    record: dict[str, object] = {
        "format": "riverhog-ci-lane-timing/v1",
        **identity,
        "lane": args.lane,
        "run_id": run_id,
        "elapsed_seconds": elapsed,
        "exit_status": result.returncode,
    }
    if args.target_seconds is not None:
        status = (
            "not-compared"
            if result.returncode != 0
            else "met"
            if elapsed < args.target_seconds
            else "missed"
        )
        record["profiling_target"] = {
            "report_only": True,
            "target_seconds": args.target_seconds,
            "status": status,
            "reason": "command-failed" if result.returncode != 0 else "completed-command",
        }
        print(
            f"Profiling target {status}: {elapsed:.3f}s observed; "
            f"under {args.target_seconds:g}s target (report only).",
            file=sys.stderr,
        )
    write_record(args.output, record)
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
