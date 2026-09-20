#!/usr/bin/env python3
(
    'Profile a command without publishing its arguments or asserting nominal '
    'capacity.'
)
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
from collections import Counter
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from uuid import uuid4

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import performance_objectives as performance
from scripts.performance_objectives import SCENARIO_OPERATIONS, WORKLOADS

MIB = 1024 * 1024
_FIELD_RE = re.compile(r"([a-z_]+)=([^ ]+)")


@dataclass(frozen=True)
class TransferLogSummary:
    bottlenecks: dict[str, int]
    operations: dict[str, int]
    phase_seconds: dict[str, float]
    plaintext_bytes: int
    records: int
    stored_bytes: int


def _parse_transfer_line(line: str) -> dict[str, str] | None:
    marker = "transfer operation="
    position = line.find(marker)
    if position < 0:
        return None
    fields = dict(_FIELD_RE.findall(line[position + len("transfer "):]))
    return fields if fields.get("operation") else None


def summarize_transfer_log(text: str, *, expected_operations: frozenset[str]) -> TransferLogSummary:
    operations: Counter[str] = Counter()
    bottlenecks: Counter[str] = Counter()
    phases: Counter[str] = Counter()
    plaintext_bytes = stored_bytes = 0
    for line in text.splitlines():
        fields = _parse_transfer_line(line)
        if fields is None or fields["operation"] not in expected_operations:
            continue
        operations[fields["operation"]] += 1
        bottlenecks[fields.get("bottleneck", "unknown")] += 1
        plain, stored = int(fields["plaintext_bytes"]), int(fields["stored_bytes"])
        if min(plain, stored) < 0:
            raise performance.PerformanceError("negative logged byte count")
        plaintext_bytes += plain
        stored_bytes += stored
        for phase in ("queue", "source", "integrity", "crypto", "processing", "remote",
            "checkpoint", "downstream"):
            duration = float(fields.get(f"{phase}_seconds", "0"))
            if not math.isfinite(duration) or duration < 0:
                raise performance.PerformanceError("invalid logged phase duration")
            phases[phase] += duration
    if not operations:
        raise performance.PerformanceError("transfer log contains no expected operations")
    return TransferLogSummary(dict(sorted(bottlenecks.items())), dict(sorted(operations.items())),
                              {name: round(value, 6) for name, value in sorted(phases.items())},
                              plaintext_bytes, sum(operations.values()), stored_bytes)


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=sorted(SCENARIO_OPERATIONS), required=True)
    parser.add_argument("--workload", choices=WORKLOADS, required=True)
    parser.add_argument("--payload-bytes", type=_positive_int, required=True,
                        help="newly completed useful bytes, not wire traffic or prior resume work")
    parser.add_argument("--items", type=_positive_int, default=1)
    parser.add_argument("--context", type=Path, help="safe, explicit comparison-context JSON")
    parser.add_argument("--reference", type=Path,
        help="a measured matching profile; never nominal bandwidth")
    parser.add_argument("--transfer-log", type=Path,
        help="identity-safe transfer-log window for diagnostics only")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    command = list(args.command)
    if command and command[0] == "--":
        command.pop(0)
    if not command:
        parser.error("a command is required after --")
    if args.reference is not None and args.context is None:
        parser.error("--reference requires --context")
    expected = SCENARIO_OPERATIONS[args.scenario]
    if args.transfer_log is not None and not expected:
        parser.error("reference-recovery has no server transfer log")
    try:
        context = (
            performance.validate_context(performance.read_json(args.context))
            if args.context else None
        )
        reference = performance.read_json(args.reference)["sample"] if args.reference else None
        run_id = str(uuid4())
        source_start = performance.source_identity()
        definition_start = performance.definition_sha256()
        with tempfile.TemporaryDirectory(prefix="riverhog-performance-") as directory:
            receipt_path = Path(directory) / "completion.json"
            environment = dict(os.environ)
            environment["RIVERHOG_PERFORMANCE_RUN_ID"] = run_id
            environment["RIVERHOG_PERFORMANCE_RECEIPT"] = str(receipt_path)
            started = time.perf_counter()
            completed = subprocess.run(command, check=False, stdout=subprocess.DEVNULL,
                                       stderr=subprocess.DEVNULL, env=environment)
            elapsed = time.perf_counter() - started
            if completed.returncode:
                return (
                    completed.returncode
                    if completed.returncode > 0 else 128 - completed.returncode
                )
            verified = False
            if receipt_path.exists():
                receipt = performance.read_json(receipt_path)
                if set(receipt) != {"schema", "run_id", "completed_bytes", "completed_items",
                    "verified"}:
                    raise performance.PerformanceError("completion receipt fields do not match")
                if (receipt["schema"] != "riverhog-performance-completion/v1" 
                    or receipt["run_id"] != run_id
                        or type(receipt["completed_bytes"]) is not int 
                    or receipt["completed_bytes"] != args.payload_bytes
                        or type(receipt["completed_items"]) is not int 
                    or receipt["completed_items"] != args.items
                        or receipt["verified"] is not True):
                    raise performance.PerformanceError(
                        "completion receipt is not exact and verified"
                    )
                verified = True
        source_finish = performance.source_identity()
        if definition_start != performance.definition_sha256():
            raise performance.PerformanceError("performance definitions changed during measurement")
        identity = "transfer-goodput" if args.scenario in performance.NETWORK_SCENARIOS else None
        measured = performance.sample(identity=identity, scenario=args.scenario,
            workload=args.workload,
                                      completed_bytes=args.payload_bytes, elapsed_seconds=elapsed,
                                      verified=verified, context=context, source_start=source_start,
                                      source_finish=source_finish, run_id=run_id)
        summary = (asdict(summarize_transfer_log(args.transfer_log.read_text(encoding="utf-8"),
                                                expected_operations=expected))
                   if args.transfer_log is not None else None)
        result = {"schema": "riverhog-transfer-profile/v2", "sample": measured,
                  "evaluation": performance.evaluate_sample(measured, reference),
                  "rate_basis": (
                      "receipt-declared-verified-completion"
                      if verified else "declared-workload-not-verified-goodput"
                  ),
                  "mib_per_second": measured["bytes_per_second"] / MIB,
                  "items": args.items, "items_per_second": args.items / elapsed,
                  "seconds_per_item": elapsed / args.items, "transfer_log": summary,
                  "transfer_log_scope": (
                      "diagnostic segment/phase totals; not unique goodput "
                      "or exclusive elapsed phases"
                  )}
        print(json.dumps(result, sort_keys=True, allow_nan=False), flush=True)
        return 0  # Target misses remain reports, as in the previous tool.
    except (OSError, ValueError, KeyError, TypeError):
        # Do not echo command, log text, environment, or potentially private input paths.
        print("transfer profile failed: invalid measurement input or execution", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
