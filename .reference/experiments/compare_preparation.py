#!/usr/bin/env python3
"""Check the paired finite work account and emit its compact observation table."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


UNCHANGED = (
    "native.advance", "native.session_advance", "native.progress",
    "native.checkpoint_load", "native.checkpoint_fence", "current.source",
    "current.worker", "current.graph_certificate", "publish.native_progress", "publish.terminal",
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    before = json.loads(args.baseline.read_text())
    after = json.loads(args.candidate.read_text())
    assert before["probe_sha256"] == after["probe_sha256"]
    assert before["fixture_sha256"] == after["fixture_sha256"]
    for name in ("native_extension_sha256", "native_build_source_sha256", "native_budget"):
        assert before["environment"][name] == after["environment"][name]
    summary = {
        "probe_sha256": before["probe_sha256"], "fixture_sha256": before["fixture_sha256"],
        "raw_sha256": {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in (
            ("baseline", args.baseline), ("candidate", args.candidate))},
        "baseline_source": before["source_files"], "candidate_source": after["source_files"],
        "observations": [], "unchanged_observed_native_calls": UNCHANGED,
        "all_probe_workspaces_removed": before["all_probe_workspaces_removed"] and after["all_probe_workspaces_removed"],
        **{name: before["environment"][name] for name in (
            "native_extension_sha256", "native_build_source_sha256", "native_budget")},
        "limitations": [
            "Only complete small actual validation-unit paths on SQLite were executed.",
            "SQL totals are SQLAlchemy execution events, including explicit snapshot BEGIN, not every driver primitive.",
            "Commit/rollback counts are actual SQLAlchemy transaction events, separate from execution-event totals.",
            "Native currentness/checkpoint counts are selected wrappers; Native SQLite statements and filesystem primitives are not counted here.",
            "Catalog event counters cover validation-unit calls; final service-close receiving is asserted but its primitives are outside these counters.",
            "No production KDF, archive publication, PostgreSQL runtime, elapsed-time or whole-path growth acceptance is established.",
            "Correction fixture adds a frame as well as a correction; it does not isolate epoch cost from frame count.",
        ],
    }
    for baseline, candidate in zip(before["observations"], after["observations"], strict=True):
        assert baseline["name"] == candidate["name"]
        assert baseline["source_sha256"] == candidate["source_sha256"]
        assert baseline["anchor"] == candidate["anchor"]
        assert baseline["service_turns"] == candidate["service_turns"]
        assert baseline["native_turns"] == candidate["native_turns"]
        assert all(baseline["calls"].get(name, 0) == candidate["calls"].get(name, 0) for name in UNCHANGED)
        assert all(baseline[key] == candidate[key] for key in (
            "original_constructions", "original_constructions_received", "original_sessions",
            "original_sessions_storage_received", "catalog_connections_checked_out", "exact_terminal_anchor",
        ))
        admitted = False
        for turn in candidate["turns"]:
            if admitted:
                assert not any(name.startswith("preparation.") for name in turn["calls"])
            if turn["calls"].get("native.advance"):
                admitted = True
        assert candidate["calls"]["prepare.binding_publication"] == 1
        cases = baseline, candidate
        row = {
            "case": baseline["name"], "source_bytes": baseline["source_bytes"],
            "frames": baseline["frames"], "corrections": baseline["corrections"], "unrelated": baseline["unrelated"],
            "service_turns": baseline["service_turns"], "native_turns": baseline["native_turns"],
            "calls": {name: [case["calls"].get(name, 0) for case in cases]
                for name in sorted(set(baseline["calls"]) | set(candidate["calls"]))},
            "catalog_application_statements": [sum(value for key, value in case["sql"].items()
                if key.startswith("all.") and key not in ("all.COMMIT", "all.ROLLBACK")) for case in cases],
            "catalog_selects": [case["sql"]["all.SELECT"] for case in cases],
            "catalog_commits": [case["sql"]["all.COMMIT"] for case in cases],
            "catalog_updates": [case["sql"].get("all.UPDATE", 0) for case in cases],
            "catalog_inserts": [case["sql"].get("all.INSERT", 0) for case in cases],
            "native_turn_currentness_and_checkpoint_counts_unchanged": True,
            "preparation_helpers_absent_after_first_native_turn": True,
            "exact_terminal_anchor": True, "all_original_constructions_and_storage_received": True,
        }
        summary["observations"].append(row)
    args.output.write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({"cases": len(summary["observations"]), "all_probe_workspaces_removed": summary["all_probe_workspaces_removed"]}))


if __name__ == "__main__":
    main()
