"""Aggregate narrow native planning diagnostics without retaining service logs."""

from __future__ import annotations

import argparse
import math
import sys
from collections.abc import Iterable
from pathlib import Path

from riverhog_canonical_json import canonical_json_bytes, require_canonical_json


def collect(lines: Iterable[str]) -> dict[str, dict[str, int | float]]:
    totals: dict[str, dict[str, int | float]] = {}
    for line in lines:
        _, marker, body = line.partition("stove0-planning-cost ")
        if not marker:
            continue
        record = require_canonical_json(body.strip().encode())
        if not isinstance(record, dict):
            raise ValueError("planning cost must be an object")
        phase, seconds = record.get("phase"), record.get("seconds")
        if not isinstance(phase, str) or not phase:
            raise ValueError("planning cost omits its phase")
        if (
            isinstance(seconds, bool)
            or not isinstance(seconds, (int, float))
            or not math.isfinite(seconds)
            or seconds < 0
        ):
            raise ValueError("planning cost must be finite and nonnegative")
        steps = record.get("steps", 0)
        if type(steps) is not int or steps < 0:
            raise ValueError("planning steps must be nonnegative")
        row = totals.setdefault(
            phase, {"calls": 0, "complete_calls": 0, "seconds": 0.0, "max_seconds": 0.0, "steps": 0}
        )
        row["calls"] += 1
        row["complete_calls"] += record.get("outcome") == "complete"
        row["seconds"] += seconds
        row["max_seconds"] = max(row["max_seconds"], seconds)
        row["steps"] += steps
    return totals


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lane", required=True)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    totals = collect(sys.stdin)
    if not {"compiled-continuations", "observer-contact", "evidence-validation"} <= totals.keys():
        raise ValueError("required processing proof has no complete owner timing evidence")
    record = {
        "proof": "stove0-planning-cost",
        "source_sha": args.source_sha,
        "lane": args.lane,
        "phases": totals,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(canonical_json_bytes(record))
    print(canonical_json_bytes(record).decode())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
