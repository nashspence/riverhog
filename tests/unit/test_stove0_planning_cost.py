from __future__ import annotations

import io
import json
import sys
from pathlib import Path

import pytest
from riverhog_canonical_json import canonical_json_bytes

from scripts import collect_stove0_planning_cost as costs


def _line(phase, seconds, **fields):
    return (
        "api-1 | stove0-planning-cost "
        + canonical_json_bytes({"phase": phase, "seconds": seconds, **fields}).decode()
        + "\n"
    )


def test_cost_aggregation_retains_only_bounded_counts_not_service_log_contents() -> None:
    actual = costs.collect(
        [
            "unrelated service log and private document\n",
            _line("compiled-continuations", 0.1, steps=12, outcome="complete", owner_id="opaque"),
            _line("compiled-continuations", 0.2, steps=7, outcome="MetadataPreparationPending"),
            _line("evidence-validation", 0.05, outcome="complete", task_id="private task"),
        ]
    )
    assert actual["compiled-continuations"] == {
        "calls": 2,
        "complete_calls": 1,
        "seconds": 0.1 + 0.2,
        "max_seconds": 0.2,
        "steps": 19,
    }
    encoded = canonical_json_bytes(actual)
    assert b"private" not in encoded and b"opaque" not in encoded
    assert actual["evidence-validation"]["complete_calls"] == 1


@pytest.mark.parametrize(
    "field,value",
    [("seconds", -1), ("seconds", "slow"), ("seconds", True), ("steps", -1), ("steps", "many")],
)
def test_cost_evidence_rejects_invalid_numeric_measurements(field, value) -> None:
    record = {"phase": "compiled-continuations", "seconds": 0.1, "steps": 3, field: value}
    with pytest.raises(ValueError):
        costs.collect(["stove0-planning-cost " + canonical_json_bytes(record).decode()])


def test_required_processing_cost_evidence_is_source_bound_and_complete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "cost.json"
    source = "a" * 64
    monkeypatch.setattr(
        sys,
        "argv",
        ["cost", "--lane", "processing-e2e", "--source-sha", source, "--output", str(output)],
    )
    monkeypatch.setattr(
        sys,
        "stdin",
        io.StringIO(
            "".join(
                _line(phase, 0.1, outcome="complete")
                for phase in ["compiled-continuations", "observer-contact", "evidence-validation"]
            )
        ),
    )
    assert costs.main() == 0
    record = json.loads(output.read_bytes())
    assert record["source_sha"] == source and record["lane"] == "processing-e2e"
    assert output.read_bytes() == canonical_json_bytes(record)
    monkeypatch.setattr(sys, "stdin", io.StringIO(_line("compiled-continuations", 0.1)))
    with pytest.raises(ValueError, match="no complete owner timing evidence"):
        costs.main()
