#!/usr/bin/env python3
from __future__ import annotations

import argparse
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
        raise ValueError(f"transfer log contains no expected operations: {names}")
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
            "Run one supported transfer or recovery command and emit a secret-free JSON "
            "performance profile. The command and its arguments are never copied into the result."
        ),
        epilog=(
            "For comparable goodput, supply --context and a measured --reference. The command "
            "receives RIVERHOG_PERFORMANCE_RUN_ID and RIVERHOG_PERFORMANCE_RECEIPT; a successful "
            "exit without an exact verified-completion receipt remains an observation. "
            "A target or comparison is report-only."
        ),
    )
    parser.add_argument("--scenario", choices=sorted(SCENARIO_OPERATIONS), required=True)
    parser.add_argument("--workload", choices=WORKLOADS, required=True)
    parser.add_argument("--payload-bytes", type=_positive_int, required=True)
    parser.add_argument("--items", type=_positive_int, default=1)
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
    command = list(args.command)
    if command and command[0] == "--":
        command.pop(0)
    if not command:
        parser.error("a command is required after --")
    expected = SCENARIO_OPERATIONS[args.scenario]
    if args.transfer_log is not None and not expected:
        parser.error("a-riverhog-recovery-tool has no server transfer log")
    return command


def main(argv: Sequence[str] | None = None) -> int:
    from riverhog_canonical_json import canonical_json_bytes

    parser = _parser()
    args = parser.parse_args(argv)
    command = _validate_args(args, parser)
    try:
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
            except (OSError, UnicodeError, ValueError):
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
    except (OSError, ValueError, KeyError, TypeError, OverflowError, ZeroDivisionError):
        # Never echo the command, log, environment, or potentially private input paths.
        print("transfer profile failed: invalid measurement input or execution", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
