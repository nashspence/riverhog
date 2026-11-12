#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import re
import subprocess
import sys
import tempfile
import time
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import performance_measurement as performance

MIB = 1024 * 1024

SCENARIO_OPERATIONS: Mapping[str, frozenset[str]] = {
    "riverhog-ingress": frozenset(
        {"pack_upload_open", "pack_write_segment", "raw_upload_open", "raw_write_segment"}
    ),
    "stove0-derived-publication": frozenset(
        {"pack_upload_open", "pack_write_segment", "raw_upload_open", "raw_write_segment"}
    ),
    "stove0-input-read": frozenset(
        {"pack_retrieval_member", "pack_retrieval_range", "raw_retrieval_part"}
    ),
    "riverhog-retrieval": frozenset(
        {"pack_retrieval_member", "pack_retrieval_range", "raw_retrieval_part"}
    ),
    "a-riverhog-recovery-tool": frozenset(),
    "archive-upload": frozenset(
        {"pack_upload_open", "pack_write_segment", "raw_upload_open", "raw_write_segment"}
    ),
    "archive-retrieval": frozenset(
        {
            "pack_retrieval_member",
            "pack_retrieval_range",
            "raw_retrieval_part",
            "retrieval_cache_hydration",
        }
    ),
    "archive-replication": frozenset({"archive_copy_segment", "archive_copy_object"}),
}
NETWORK_SCENARIOS = frozenset(SCENARIO_OPERATIONS) - {"a-riverhog-recovery-tool"}
WORKLOADS = ("large-file", "many-small-files", "resume")
_FIELD_RE = re.compile(r"([a-z_]+)=([^ ]+)")


@dataclass(frozen=True)
class TransferLogSummary:
    bottlenecks: dict[str, int]
    operations: dict[str, int]
    phase_seconds: dict[str, float]
    plaintext_bytes: int
    records: int
    stored_bytes: int


class _NoExpectedOperations(ValueError):
    """A readable diagnostic log has no records for the selected scenario."""


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _positive_ratio(value: str) -> float:
    try:
        parsed = float(value)
    except (ValueError, OverflowError) as exc:
        raise argparse.ArgumentTypeError("must be a positive finite ratio") from exc
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive finite ratio")
    return parsed


def _parse_transfer_line(line: str) -> dict[str, str] | None:
    marker = "transfer operation="
    position = line.find(marker)
    if position < 0:
        return None
    fields = dict(_FIELD_RE.findall(line[position + len("transfer ") :]))
    return fields if fields.get("operation") else None


def summarize_transfer_log(
    text: str,
    *,
    expected_operations: frozenset[str],
) -> TransferLogSummary:
    operations: Counter[str] = Counter()
    bottlenecks: Counter[str] = Counter()
    phases: defaultdict[str, float] = defaultdict(float)
    plaintext_bytes = 0
    stored_bytes = 0
    for line in text.splitlines():
        fields = _parse_transfer_line(line)
        if fields is None or fields["operation"] not in expected_operations:
            continue
        operations[fields["operation"]] += 1
        bottlenecks[fields.get("bottleneck", "unknown")] += 1
        plaintext_bytes += int(fields["plaintext_bytes"])
        stored_bytes += int(fields["stored_bytes"])
        for phase in (
            "queue",
            "source",
            "integrity",
            "crypto",
            "processing",
            "remote",
            "checkpoint",
            "downstream",
        ):
            phases[phase] += float(fields.get(f"{phase}_seconds", "0"))
    records = sum(operations.values())
    if not records:
        names = ", ".join(sorted(expected_operations))
        raise _NoExpectedOperations(f"transfer log contains no expected operations: {names}")
    return TransferLogSummary(
        bottlenecks=dict(sorted(bottlenecks.items())),
        operations=dict(sorted(operations.items())),
        phase_seconds={name: round(phases[name], 6) for name in sorted(phases)},
        plaintext_bytes=plaintext_bytes,
        records=records,
        stored_bytes=stored_bytes,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run transfer, recovery, collection publication or Stove0 scale profiles. "
            "Commands and arguments are excluded from measurement records."
        ),
        epilog=(
            "For comparable goodput, supply --context and a measured --reference. The command "
            "receives RIVERHOG_PERFORMANCE_RUN_ID and RIVERHOG_PERFORMANCE_RECEIPT; a successful "
            "exit without an exact verified-completion receipt remains an observation. "
            "A target or comparison is report-only. For the maintained 128-file lifecycle, "
            "use --scenario stove0-scale; it uses the maintained target and measures image "
            "preparation separately."
        ),
    )
    parser.add_argument(
        "--scenario",
        choices=sorted([*SCENARIO_OPERATIONS, "stove0-scale", "collection-publication"]),
        required=True,
    )
    parser.add_argument("--workload", choices=WORKLOADS, help="transfer or recovery workload")
    parser.add_argument(
        "--publication-workload",
        type=Path,
        help="exact input geometry JSON for the maintained collection-publication profile",
    )
    parser.add_argument("--payload-bytes", type=_positive_int)
    parser.add_argument("--items", type=_positive_int)
    parser.add_argument(
        "--output", type=Path, help="directory for retained maintained profiling records"
    )
    parser.add_argument(
        "--context",
        type=Path,
        help="safe comparison-context JSON for a measured workload",
    )
    parser.add_argument(
        "--reference",
        type=Path,
        help="another measured matching v3 profile; nominal capacity is not a reference",
    )
    parser.add_argument(
        "--target-ratio",
        type=_positive_ratio,
        help="optional report-only fraction of the measured reference rate",
    )
    parser.add_argument(
        "--transfer-log",
        type=Path,
        help="server log window containing identity-safe riverhog.transfer records",
    )
    parser.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="command to profile, preceded by --",
    )
    return parser


def _validate_args(args: argparse.Namespace, parser: argparse.ArgumentParser) -> list[str]:
    if args.scenario == "collection-publication":
        fields = (
            "workload",
            "payload_bytes",
            "items",
            "context",
            "reference",
            "target_ratio",
            "transfer_log",
        )
        if args.command or any(getattr(args, field) is not None for field in fields):
            parser.error("collection-publication uses its exact --publication-workload")
        if args.publication_workload is None:
            parser.error("collection-publication requires --publication-workload")
        return []
    if args.publication_workload is not None:
        parser.error("--publication-workload requires --scenario collection-publication")
    if args.scenario == "stove0-scale":
        transfer_fields = (
            "workload",
            "payload_bytes",
            "items",
            "context",
            "reference",
            "target_ratio",
            "transfer_log",
        )
        if args.command or any(getattr(args, field) is not None for field in transfer_fields):
            parser.error("stove0-scale uses the maintained 128-file workload and lifecycle command")
        return []
    if args.output is not None:
        parser.error("--output selects a maintained preset profile directory")
    if args.workload is None or args.payload_bytes is None:
        parser.error("transfer and recovery profiles require --workload and --payload-bytes")
    if args.items is None:
        args.items = 1
    command = list(args.command)
    if command and command[0] == "--":
        command.pop(0)
    if not command:
        parser.error("a command is required after --")
    expected = SCENARIO_OPERATIONS[args.scenario]
    if args.transfer_log is not None and not expected:
        parser.error("a-riverhog-recovery-tool has no server transfer log")
    return command


def _profile_stove0_scale(output: Path) -> int:
    """Report the maintained scale fixture against a non-failing duration target."""
    from riverhog_canonical_json import canonical_json_bytes

    if TYPE_CHECKING:
        from ci_qualification import require_validation_work
        from ci_timing import check_validation_evidence
    else:
        from scripts.ci_qualification import require_validation_work
        from scripts.ci_timing import check_validation_evidence

    work = require_validation_work(["stove0-scale"])[0]
    conditions = dict(work.conditions)
    # This preset owns its exact command; parent Make's scenario selector is not a child selector.
    environment = dict(os.environ)
    for variable in ("args", "MAKEFLAGS", "MFLAGS", "MAKEOVERRIDES", "RIVERHOG_VALIDATION_SCOPE"):
        environment.pop(variable, None)
    run_id = uuid4().hex
    timing_script = Path(__file__).with_name("ci_timing.py")
    directory = output / run_id
    directory.mkdir(parents=True)
    print(f"Scale profiling records: {directory}", file=sys.stderr)
    with (directory / "images.log").open("w") as log:
        prepared = subprocess.run(
            [
                sys.executable,
                str(timing_script),
                "execute",
                "--workload",
                "stove0-scale-images",
                "--run-id",
                run_id + "-images",
                "--output",
                str(directory / "images.json"),
            ],
            check=False,
            stdout=log,
            stderr=subprocess.STDOUT,
            env=environment,
        )
    preparation = prepared.returncode
    image_timing = json.loads((directory / "images.json").read_bytes())
    lifecycle_timing = None
    status = preparation
    if preparation == 0:
        image_evidence = check_validation_evidence(
            [directory / "images.json"],
            expected=["stove0-scale-images"],
            source_sha=image_timing["source_sha"],
            run_id=run_id + "-images",
        )
        if image_timing["exit_status"] or image_evidence["qualification_exit_status"]:
            raise performance.PerformanceError("image preparation evidence contradicts execution")
        with (directory / "lifecycle.log").open("w") as log:
            completed = subprocess.run(
                [
                    sys.executable,
                    str(timing_script),
                    "execute",
                    "--workload",
                    work.name,
                    "--run-id",
                    run_id,
                    "--output",
                    str(directory / "lifecycle.json"),
                ],
                check=False,
                stdout=log,
                stderr=subprocess.STDOUT,
                env={**environment, "RIVERHOG_CI_TIMING_DIR": str(directory / "phases")},
            )
        status = completed.returncode
        lifecycle_timing = json.loads((directory / "lifecycle.json").read_bytes())
        evidence = check_validation_evidence(
            [directory / "lifecycle.json"],
            expected=[work.name],
            source_sha=image_timing["source_sha"],
            run_id=run_id,
        )
        if (lifecycle_timing["exit_status"] or evidence["qualification_exit_status"]) != status:
            raise performance.PerformanceError("lifecycle status differs from timing evidence")
    comparison = (
        lifecycle_timing["profiling_target"]
        if lifecycle_timing is not None
        else {
            "status": "not-compared",
            "reason": "image-preparation-failed",
            "target_seconds": work.target_seconds,
            "report_only": True,
        }
    )
    source = {key: image_timing[key] for key in ("source_sha", "source_clean")}
    if lifecycle_timing is not None and any(lifecycle_timing[key] != source[key] for key in source):
        comparison = {**comparison, "status": "not-compared", "reason": "source-changed"}
    report = {
        "format": "riverhog-stove0-scale-profile/v1",
        **source,
        "run_id": run_id,
        "target": {
            "scenario": "stove0-scale",
            "workload": "many-small-files",
            "audio_files": int(conditions["audio_files"]),
            "audio_frames": int(conditions["audio_frames"]),
            "sidecar_files": int(conditions["sidecar_files"]),
            "elapsed_seconds": work.target_seconds,
        },
        "observed": {
            "image_preparation_elapsed_seconds": image_timing["elapsed_seconds"],
            "image_preparation_exit_code": preparation,
            "elapsed_seconds": lifecycle_timing["elapsed_seconds"] if lifecycle_timing else None,
            "command_exit_code": lifecycle_timing["exit_status"] if lifecycle_timing else None,
            "completion_verified": lifecycle_timing is not None and status == 0,
        },
        "comparison": comparison,
    }
    (directory / "profile.json").write_bytes(canonical_json_bytes(report))
    print(
        f"Profiling target {comparison['status']} (under {work.target_seconds}s; report only).",
        file=sys.stderr,
    )
    print(canonical_json_bytes(report).decode(), flush=True)
    return status


def _profile_collection_publication(workload: Path, output: Path) -> int:
    from dataclasses import asdict

    from riverhog_canonical_json import canonical_json_bytes

    from scripts.validation_identity import source_identity
    from scripts.validation_process import run_validation_child
    from tests.support.qualification.publication_geometry import WORKLOAD_FORMAT, read_workload

    root = Path(__file__).resolve().parents[1]
    source = source_identity(root)
    if source.get("source_clean") is not True:
        raise performance.PerformanceError("collection-publication requires clean source")
    try:
        geometry = read_workload(workload)
    except (OSError, ValueError, TypeError) as error:
        raise performance.PerformanceError("publication workload is invalid") from error
    target = asdict(geometry)
    target["payload_sizes"] = list(geometry.payload_sizes)
    output.mkdir(parents=True, exist_ok=True)
    run_id = str(uuid4())
    directory = output / run_id
    directory.mkdir()

    def phase(value: dict[str, object]) -> None:
        record = {
            "format": "riverhog-publication-profile-phase/v1",
            "run_id": run_id,
            **source,
            **value,
        }
        temporary = directory / "phase.json.tmp"
        temporary.write_bytes(canonical_json_bytes(record))
        temporary.replace(directory / "phase.json")

    phase({"phase": "starting"})
    failure: dict[str, object] = {}
    started = time.perf_counter()
    try:
        # The parent owns scratch. Its receiver joins the original helper and
        # cancellation cleanup before this directory can be removed.
        with tempfile.TemporaryDirectory(prefix="riverhog-publication-profile-") as scratch:
            workspace = Path(scratch)
            selected = workspace / "workload.json"
            selected.write_bytes(canonical_json_bytes({"format": WORKLOAD_FORMAT, **target}))
            completion = workspace / "completion.json"
            fixture_root = workspace / "fixture"
            environment = dict(os.environ)
            # A helper's restored environment still names its own dummy config.
            # Unconfirmed fixture cleanup can never select deployed settings.
            environment["RIVERHOG_CONFIG"] = str(fixture_root / "riverhog.yaml")
            status = run_validation_child(
                [
                    sys.executable,
                    "-m",
                    "tests.support.qualification.publication_profile",
                    str(selected),
                    str(fixture_root),
                    str(completion),
                    str(directory / "phase.json"),
                    run_id,
                    str(source["source_sha"]),
                ],
                cwd=root,
                env=environment,
            )
            failure["child_exit_status"] = status
            failure["child_received"] = True
            if status != 0:
                progress = directory / "phase.json"
                if progress.exists():
                    failure["helper_failure"] = json.loads(progress.read_text())
                raise performance.PerformanceError("publication helper failed")
            try:
                receipt = json.loads(completion.read_text())
            except (OSError, ValueError) as error:
                raise performance.PerformanceError(
                    "publication completion receipt is invalid"
                ) from error
            if (
                not isinstance(receipt, dict)
                or not isinstance(receipt.get("observed"), dict)
                or set(receipt) != {"format", "run_id", "source", "observed"}
                or receipt["format"] != "riverhog-publication-completion/v1"
                or receipt["run_id"] != run_id
                or receipt["source"] != source
                or receipt["observed"].get("completion_verified") is not True
                or receipt["observed"].get("workload") != target
            ):
                raise performance.PerformanceError("publication completion receipt is invalid")
            observed = receipt["observed"]
        if source_identity(root) != source:
            raise performance.PerformanceError("publication profile source changed")
    except BaseException as error:
        phase({"phase": "failed", "error_type": type(error).__name__, **failure})
        raise
    report = {
        "format": "riverhog-publication-profile/v1",
        **source,
        "run_id": run_id,
        "observed": observed,
        "execution": {
            "elapsed_seconds": time.perf_counter() - started,
            "child_exit_status": status,
            "child_received": True,
            "workspace_cleanup_received": True,
        },
        "comparison": {
            "status": "not-compared",
            "reason": "no-measured-reference",
            "report_only": True,
        },
    }
    temporary = directory / "profile.json.tmp"
    temporary.write_bytes(canonical_json_bytes(report))
    temporary.replace(directory / "profile.json")
    phase({"phase": "passed", "completion_verified": True})
    print(canonical_json_bytes(report).decode(), flush=True)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    from riverhog_canonical_json import canonical_json_bytes

    parser = _parser()
    args = parser.parse_args(argv)
    command = _validate_args(args, parser)
    try:
        if args.scenario == "collection-publication":
            output = (
                args.output or Path(__file__).resolve().parents[1] / "build/publication-profile"
            )
            return _profile_collection_publication(args.publication_workload, output)
        if args.scenario == "stove0-scale":
            output = (
                args.output or Path(__file__).resolve().parents[1] / "build/stove0-scale-profile"
            )
            return _profile_stove0_scale(output)
        context = (
            performance.validate_context(performance.read_json(args.context))
            if args.context is not None
            else None
        )
        if context is not None and context["byte_domain"] != "logical-payload":
            raise performance.PerformanceError("transfer comparison requires logical payload bytes")
        reference: Mapping[str, object] | None = None
        unavailable_reason = "no-measured-reference"
        if args.reference is not None:
            prior = performance.read_json(args.reference)
            if prior.get("format") != "riverhog-transfer-profile/v3":
                raise performance.PerformanceError("reference profile format is invalid")
            reference = performance.validate_sample(prior.get("sample"))
        run_id = str(uuid4())
        with tempfile.TemporaryDirectory(prefix="riverhog-performance-") as temporary:
            receipt_path = Path(temporary) / "completion.json"
            environment = dict(os.environ)
            environment["RIVERHOG_PERFORMANCE_RUN_ID"] = run_id
            environment["RIVERHOG_PERFORMANCE_RECEIPT"] = str(receipt_path)
            started = time.perf_counter()
            try:
                completed = subprocess.run(
                    command,
                    check=False,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    env=environment,
                )
            except OSError:
                completed = None
            elapsed = time.perf_counter() - started
            verified = False
            receipt_state = "not-written"
            if completed is not None and completed.returncode == 0 and receipt_path.exists():
                try:
                    receipt = performance.read_json(receipt_path)
                    verified = (
                        set(receipt)
                        == {"format", "run_id", "completed_bytes", "completed_items", "verified"}
                        and receipt["format"] == "riverhog-performance-completion/v1"
                        and receipt["run_id"] == run_id
                        and type(receipt["completed_bytes"]) is int
                        and receipt["completed_bytes"] == args.payload_bytes
                        and type(receipt["completed_items"]) is int
                        and receipt["completed_items"] == args.items
                        and receipt["verified"] is True
                    )
                except performance.PerformanceError:
                    verified = False
                receipt_state = "verified" if verified else "invalid"
            if receipt_state == "invalid":
                raise performance.PerformanceError("completion receipt is invalid")
        target = {
            "scenario": args.scenario,
            "workload": args.workload,
            "payload_bytes": args.payload_bytes,
            "items": args.items,
            "reference_ratio": args.target_ratio,
        }
        if completed is None or completed.returncode != 0:
            print(
                canonical_json_bytes(
                    {
                        "format": "riverhog-transfer-profile/v3",
                        "target": target,
                        "observed": {
                            "command_exit_code": (
                                completed.returncode if completed is not None else None
                            ),
                            "elapsed_seconds": elapsed,
                            "completion_verified": False,
                            "launch_state": "launched" if completed is not None else "failed",
                        },
                        "sample": None,
                        "comparison": {
                            "status": "not-compared",
                            "reason": (
                                "command-failed"
                                if completed is not None
                                else "command-launch-failed"
                            ),
                            "report_only": True,
                        },
                    }
                ).decode(),
                flush=True,
            )
            return 2
        try:
            measured = performance.sample(
                scenario=args.scenario,
                workload=args.workload,
                context=context,
                completed_bytes=args.payload_bytes,
                elapsed_seconds=elapsed,
                completion_verified=verified,
                run_id=run_id,
            )
        except (performance.PerformanceError, OverflowError, ZeroDivisionError):
            print(
                canonical_json_bytes(
                    {
                        "format": "riverhog-transfer-profile/v3",
                        "target": target,
                        "observed": {
                            "command_exit_code": 0,
                            "elapsed_seconds": elapsed,
                            "completion_verified": verified,
                            "receipt_state": receipt_state,
                            "measurement_state": "unavailable",
                        },
                        "sample": None,
                        "comparison": {
                            "status": "not-compared",
                            "reason": "measurement-unavailable",
                            "report_only": True,
                        },
                    }
                ).decode(),
                flush=True,
            )
            return 2
        log_summary = None
        log_state = "not-supplied"
        if args.transfer_log is not None:
            try:
                log_summary = asdict(
                    summarize_transfer_log(
                        args.transfer_log.read_text(encoding="utf-8"),
                        expected_operations=SCENARIO_OPERATIONS[args.scenario],
                    )
                )
            except _NoExpectedOperations:
                log_state = "unavailable-or-incomplete"
            else:
                log_state = "summarized"
        result = {
            "format": "riverhog-transfer-profile/v3",
            "target": target,
            "observed": {
                "command_exit_code": 0,
                "elapsed_seconds": elapsed,
                "completion_verified": verified,
                "receipt_state": receipt_state,
            },
            "sample": measured,
            "comparison": performance.compare_sample(
                measured,
                reference if args.scenario in NETWORK_SCENARIOS else None,
                target_ratio=args.target_ratio,
                unavailable_reason=(
                    "scenario-has-no-reference"
                    if args.scenario not in NETWORK_SCENARIOS
                    else unavailable_reason
                ),
            ),
            "rate_basis": "receipt-verified" if verified else "declared-workload-unverified",
            "mib_per_second": args.payload_bytes / elapsed / MIB,
            "items": args.items,
            "items_per_second": round(args.items / elapsed, 3),
            "seconds_per_item": round(elapsed / args.items, 6),
            "transfer_log": log_summary,
            "transfer_log_state": log_state,
            "transfer_log_scope": (
                "diagnostic segment and phase totals; not unique end-to-end goodput"
            ),
        }
        print(canonical_json_bytes(result).decode(), flush=True)
        return 0
    except (
        OSError,
        UnicodeError,
        ValueError,
        KeyError,
        TypeError,
        OverflowError,
        ZeroDivisionError,
    ):
        # Never echo the command, log, environment, or potentially private input paths.
        print("transfer profile failed: invalid measurement input or execution", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
