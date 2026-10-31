"""Observational lane, phase, and pytest timing; never a performance gate."""

from __future__ import annotations

import argparse
import math
import os
import platform
import subprocess
import sys
import time
from collections.abc import Callable, Generator, Sequence
from datetime import UTC, datetime
from pathlib import Path
from threading import Timer
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest
from riverhog_canonical_json import canonical_json_bytes, require_canonical_json

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.validation_identity import source_identity as source_identity
from scripts.validation_process import ValidationInterrupted, run_validation_child
from scripts.validation_resources import validation_storage_budget

if TYPE_CHECKING:
    from scripts.validation_evidence import ValidationWork


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
        self.session_id = worker.get("ci_timing_session_id", uuid4().hex)
        self.execution_span = os.environ.get("RIVERHOG_VALIDATION_ACTIVE_SPAN")
        self.lane = os.environ.get("RIVERHOG_CI_LANE", "unit")
        root = Path(os.environ.get("RIVERHOG_CI_TIMING_DIR", "build/ci-timing"))
        self.directory = root / "pytest" / self.lane / self.run_id / self.session_id
        self.source = source_identity(config.rootpath)
        self.started_at = datetime.now(UTC).isoformat()
        self.started = time.monotonic()
        self.fixtures: dict[str, dict[str, int | float]] = {}
        self.tests: dict[str, dict[str, dict[str, object]]] = {}
        self.growth_witnesses: dict[str, dict[str, object]] = {}
        self.growth_observations: dict[str, object] = {}
        self.growth_samples: dict[str, object] = {}

    def pytest_collection_finish(self, session: pytest.Session) -> None:
        import hashlib

        for item in session.items:
            marker = item.get_closest_marker("growth_witness")
            if marker is not None:
                self.growth_witnesses[item.nodeid] = {
                    "id": marker.args[0],
                    "source_sha256": hashlib.sha256(item.path.read_bytes()).hexdigest(),
                }
                operation = marker.kwargs.get("operation")
                if operation is not None:
                    from scripts.validation_witnesses import operation_projection

                    _, declaration = operation_projection(operation, root=session.config.rootpath)
                    self.growth_witnesses[item.nodeid]["declaration"] = declaration

    @pytest.hookimpl(optionalhook=True)
    def pytest_configure_node(self, node: Any) -> None:
        node.workerinput["ci_timing_run_id"] = self.run_id
        node.workerinput["ci_timing_session_id"] = self.session_id

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
            "expected_failure": bool(getattr(report, "wasxfail", False)),
        }
        if report.when == "call":
            observations = [
                value for name, value in report.user_properties if name == "growth_observations"
            ]
            if len(observations) == 1:
                self.growth_observations[report.nodeid] = observations[0]
            samples = [value for name, value in report.user_properties if name == "growth_samples"]
            if len(samples) == 1:
                self.growth_samples[report.nodeid] = samples[0]

    def pytest_sessionfinish(self, session: pytest.Session, exitstatus: int) -> None:
        write_record(
            self.directory / f"{self.worker_id}.json",
            {
                "format": "riverhog-ci-pytest-timing/v3",
                **self.source,
                "source_end": source_identity(session.config.rootpath),
                "lane": self.lane,
                "run_id": self.run_id,
                "worker": self.worker_id,
                "session_id": self.session_id,
                "execution_span": self.execution_span,
                "started_at": self.started_at,
                "elapsed_seconds": time.monotonic() - self.started,
                "exit_status": int(exitstatus),
                "fixtures": self.fixtures,
                "tests": self.tests,
                "growth_witnesses": self.growth_witnesses,
                "growth_observations": self.growth_observations,
                "growth_samples": self.growth_samples,
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


def validation_comparison(
    definition: dict[str, object],
    *,
    elapsed: object,
    exit_status: object,
    source_clean: object,
    target: object = None,
) -> dict[str, object]:
    from scripts.validation_evidence import observe_performance

    return observe_performance(
        definition,
        elapsed=elapsed,
        exit_status=exit_status,
        source_clean=source_clean,
        target=target,
    )


def execute_validation(
    name: str,
    output: Path,
    run_id: str,
    *,
    growth_collector: Callable[[ValidationWork, str], dict[str, str]] | None = None,
) -> int:
    """Run exactly the shared plan's command and retain start and terminal evidence."""
    if TYPE_CHECKING:
        from ci_qualification import ROOT, validation_environment, validation_work
    else:
        from scripts.ci_qualification import ROOT, validation_environment, validation_work

    from scripts.validation_evidence import validation_path_identity

    validation_path_identity(run_id)
    started = time.monotonic()
    started_at = datetime.now(UTC).isoformat()
    work = validation_work(name)
    if output.exists():
        raise ValueError("validation evidence must not overwrite another invocation")
    env = validation_environment(work, run_id)
    timing_root = Path(env.get("RIVERHOG_CI_TIMING_DIR", str(ROOT / "build/ci-timing")))
    lease = timing_root / "make" / run_id / (".active-" + uuid4().hex)
    lease.parent.mkdir(parents=True, exist_ok=True)
    lease.touch(exist_ok=False)
    try:
        source = source_identity(ROOT)
        definition = work.definition()
        record: dict[str, object] = {
            "format": "riverhog-ci-validation-evidence/v2",
            **source,
            "run_id": run_id,
            "definition": definition,
            "state": "running",
            "started_at": started_at,
            "started_monotonic_seconds": started,
            "observed_conditions": {
                "platform": sys.platform,
                "architecture": platform.machine(),
                "python": platform.python_version(),
            },
            "advisory_target": work.advisory_target(),
        }
        # A killed parent leaves an incomplete record, not an absent run or false success.
        write_record(output, record)
        timer = None
        if work.target_seconds is not None:
            timer = Timer(
                work.target_seconds,
                lambda: print(
                    f"Advisory speed goal exceeded while {name} is running; "
                    "investigation recommended.",
                    file=sys.stderr,
                    flush=True,
                ),
            )
            timer.daemon = True
            timer.start()
        state = "completed"
        status = 2
        execution_started = time.monotonic()
        try:
            status = run_validation_child(work.command, cwd=ROOT, env=env)
        except OSError:
            state = "launch-failed"
        except ValidationInterrupted as interrupted:
            state, status = "interrupted", 128 + interrupted.signum
        except KeyboardInterrupt:
            state, status = "interrupted", 130
        finally:
            execution_finished = time.monotonic()
            if timer is not None:
                timer.cancel()
            growth_results = {}
            if growth_collector is None and work.growth_witnesses:
                from scripts.validation_witnesses import collect_test_witnesses

                timing_root = Path(env.get("RIVERHOG_CI_TIMING_DIR", str(ROOT / "build/ci-timing")))

                def owner_collector(owned: ValidationWork, invocation: str) -> dict[str, str]:
                    evidence: list[dict[str, object]] = []
                    results = collect_test_witnesses(
                        tuple((timing_root / "pytest").glob(f"*/{invocation}/*/*.json")),
                        root=ROOT,
                        expected=owned.growth_witnesses,
                        source_sha=str(source["source_sha"]),
                        run_id=invocation,
                        evidence=evidence,
                    )
                    if evidence:
                        write_record(
                            timing_root / "growth" / f"{invocation}.json",
                            {"format": "riverhog-growth-execution-bundle/v1", "records": evidence},
                        )
                    return results

                growth_collector = owner_collector
            if growth_collector is not None:
                try:
                    growth_results = growth_collector(work, run_id)
                except Exception:
                    if state == "completed":
                        state = "evidence-failed"
            after = source_identity(ROOT)
            finished = time.monotonic()
            elapsed = finished - started
            phase_costs = {
                "preparation": execution_started - started,
                "command-including-owner-cleanup": execution_finished - execution_started,
                "evidence-collection": finished - execution_finished,
            }
            comparison = validation_comparison(
                definition,
                elapsed=elapsed,
                exit_status=status,
                source_clean=source.get("source_clean") is True and after == source,
                target=work.advisory_target(),
            )
            record.update(
                growth_results=growth_results,
                state=state,
                source_end=after,
                elapsed_seconds=elapsed,
                phase_costs=phase_costs,
                finished_at=datetime.now(UTC).isoformat(),
                exit_status=status,
                profiling_target=comparison,
            )
            write_record(output, record)
        print(
            f"{name}: {elapsed:.3f}s; target {work.target_seconds}; "
            f"{comparison['status']} ({comparison['reason']}; report only). Evidence: {output}",
            file=sys.stderr,
        )
        if state == "completed":
            try:
                report = check_validation_evidence(
                    [output], expected=[name], source_sha=str(source["source_sha"]), run_id=run_id
                )
                qualified_status = report["qualification_exit_status"]
                if type(qualified_status) is not int:
                    raise ValueError("invalid qualification result")
                assert isinstance(qualified_status, int)
                return status or qualified_status
            except (ValueError, OSError, KeyError, TypeError):
                return status or 2
        return status or 2

    finally:
        lease.unlink(missing_ok=True)


def check_validation_evidence(
    paths: Sequence[Path], *, expected: Sequence[str], source_sha: str, run_id: str
) -> dict[str, object]:
    """Check required evidence and command/growth outcomes; optional speed is advisory."""
    if TYPE_CHECKING:
        from ci_qualification import require_validation_work
    else:
        from scripts.ci_qualification import require_validation_work
    from scripts.validation_evidence import validate_evidence_records

    return validate_evidence_records(
        [require_canonical_json(path.read_bytes()) for path in paths],
        expected=require_validation_work(expected),
        source_sha=source_sha,
        run_id=run_id,
    )


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
    execute = commands.add_parser("execute", help="Run one exact maintained validation workload.")
    execute.add_argument("--workload", required=True)
    execute.add_argument("--output", type=Path, required=True)
    execute.add_argument("--run-id", default=None)
    check = commands.add_parser("check-evidence", help="Require complete evidence for one run.")
    check.add_argument("--workload", action="append", required=True)
    check.add_argument("--source-sha", required=True)
    check.add_argument("--run-id", required=True)
    check.add_argument("paths", nargs="+", type=Path)
    phase = commands.add_parser("phase", help="Append a completed shell lifecycle phase.")
    phase.add_argument("--lane", required=True)
    phase.add_argument("--name", required=True)
    phase.add_argument("--seconds", required=True, type=float)
    phase.add_argument("--status", required=True, type=int)
    phase.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    if args.command in {"execute", "check-evidence"}:
        try:
            if args.command == "execute":
                return execute_validation(args.workload, args.output, args.run_id or uuid4().hex)
            report = check_validation_evidence(
                args.paths, expected=args.workload, source_sha=args.source_sha, run_id=args.run_id
            )
            print(canonical_json_bytes(report).decode())
            qualified_status = report["qualification_exit_status"]
            if type(qualified_status) is not int:
                raise ValueError("invalid qualification result")
            assert isinstance(qualified_status, int)
            return qualified_status
        except (ValueError, OSError, KeyError, TypeError):
            print(
                "validation timing failed: invalid or incomplete required evidence", file=sys.stderr
            )
            return 2
    identity = source_identity(Path(__file__).resolve().parents[1])
    if args.command == "phase":
        if not math.isfinite(args.seconds) or args.seconds < 0:
            print("validation phase has invalid elapsed-time data", file=sys.stderr)
            return 2
        args.output.parent.mkdir(parents=True, exist_ok=True)
        encoded = (
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
        budget = validation_storage_budget()
        previous = args.output.stat().st_size if args.output.exists() else 0
        if budget < 1 or previous + len(encoded) > budget:
            print("validation phase storage budget is exhausted", file=sys.stderr)
            return 2
        with args.output.open("ab") as destination:
            destination.write(encoded)
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
