from __future__ import annotations

import copy
import json
from pathlib import Path
from uuid import uuid4

import pytest

from scripts import performance_objectives as performance


def context() -> dict[str, object]:
    return {
        "format": performance.CONTEXT_FORMAT,
        "comparison_id": str(uuid4()),
        "workload_sha256": "a" * 64,
        "environment_sha256": "b" * 64,
        "path_sha256": "c" * 64,
        "byte_domain": "logical-payload",
        "completion_boundary": "verified-command-completion",
        "cache_state": "cold",
        "concurrency": 1,
    }


def measured_pair() -> tuple[dict[str, object], dict[str, object]]:
    shared = context()
    common = {
        "objective_id": "transfer-goodput",
        "scenario": "riverhog-ingress",
        "workload": "large-file",
        "context": shared,
        "completed_bytes": 1000,
        "completion_verified": True,
    }
    reference = performance.sample(**common, elapsed_seconds=10.0, run_id=str(uuid4()))
    candidate = performance.sample(**common, elapsed_seconds=10.0, run_id=str(uuid4()))
    return candidate, reference


def test_registry_and_render_are_exact_and_noncontractual() -> None:
    performance.validate_registry()
    assert [item.id for item in performance.OBJECTIVES] == [
        "transfer-goodput",
        "storage-upload-goodput",
        "storage-read-goodput",
    ]
    assert all(item.disposition == "report-only candidate" for item in performance.OBJECTIVES)
    assert all(item.minimum_reference_fraction == 0.90 for item in performance.OBJECTIVES)
    rendered = performance.render()
    assert rendered == performance.OUTPUT.read_text(encoding="utf-8")
    assert "nominal link capacity and raw transport rate are not references" in rendered
    assert "no accepted latency target" in rendered
    assert "database-regression-guards" in rendered
    assert performance.main(["check"]) == 0


def test_matched_measured_reference_reports_absolute_rates_and_ratio() -> None:
    candidate, reference = measured_pair()
    met = performance.evaluate_sample(candidate, reference)
    assert met["status"] == "met"
    assert met["candidate_bytes_per_second"] == 100
    assert met["reference_bytes_per_second"] == 100
    candidate["elapsed_seconds"] = 20.0
    candidate["bytes_per_second"] = 50.0
    missed = performance.evaluate_sample(candidate, reference)
    assert missed["status"] == "missed"
    assert missed["reference_ratio"] == 0.5
    assert missed["minimum_reference_fraction"] == 0.9


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("completion_verified", False, "completion-unverified"),
        ("definition_sha256", "d" * 64, "definition-changed"),
        ("context", None, "comparison-context-missing"),
        ("scenario", "archive-upload", "incomparable-scenario"),
        ("workload", "resume", "incomparable-workload"),
    ],
)
def test_incomplete_or_incompatible_reference_never_passes(
    field: str, value: object, reason: str
) -> None:
    candidate, reference = measured_pair()
    reference[field] = value
    result = performance.evaluate_sample(candidate, reference)
    assert result["status"] == "not-evaluated"
    assert result["reason"] == reason


@pytest.mark.parametrize(
    "field",
    [
        "comparison_id",
        "workload_sha256",
        "environment_sha256",
        "path_sha256",
        "byte_domain",
        "completion_boundary",
        "cache_state",
        "concurrency",
    ],
)
def test_every_comparison_context_dimension_must_match(field: str) -> None:
    candidate, reference = measured_pair()
    changed = copy.deepcopy(reference)
    assert isinstance(changed["context"], dict)
    alternatives = {
        "comparison_id": str(uuid4()),
        "workload_sha256": "e" * 64,
        "environment_sha256": "e" * 64,
        "path_sha256": "e" * 64,
        "byte_domain": "stored-payload",
        "completion_boundary": "other-completion",
        "cache_state": "warm",
        "concurrency": 2,
    }
    changed["context"][field] = alternatives[field]
    if field == "byte_domain":
        with pytest.raises(performance.PerformanceError, match="byte domain"):
            performance.evaluate_sample(candidate, changed)
        return
    result = performance.evaluate_sample(candidate, changed)
    assert result["status"] == "not-evaluated"
    assert result["reason"] == "incomparable-context"


def test_no_measured_reference_and_self_comparison_never_pass() -> None:
    candidate, _ = measured_pair()
    assert performance.evaluate_sample(candidate, None)["reason"] == "no-measured-reference"
    assert performance.evaluate_sample(candidate, candidate)["reason"] == "self-comparison"


def test_changed_completed_bytes_or_forged_rate_never_passes() -> None:
    candidate, reference = measured_pair()
    reference["completed_bytes"] = 2000
    reference["bytes_per_second"] = 200.0
    assert (
        performance.evaluate_sample(candidate, reference)["reason"]
        == "incomparable-completed-bytes"
    )
    reference["bytes_per_second"] = 100.0
    with pytest.raises(performance.PerformanceError, match="rate disagrees"):
        performance.evaluate_sample(candidate, reference)


def test_overflowed_ratio_cannot_become_a_pass() -> None:
    candidate, reference = measured_pair()
    candidate["elapsed_seconds"] = 1e-300
    candidate["bytes_per_second"] = 1e303
    reference["elapsed_seconds"] = 1e306
    reference["bytes_per_second"] = 1e-303
    with pytest.raises(performance.PerformanceError, match="ratio is not finite"):
        performance.evaluate_sample(candidate, reference)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("completed_bytes", True),
        ("completed_bytes", 0),
        ("elapsed_seconds", 0),
        ("elapsed_seconds", float("inf")),
        ("completion_verified", "yes"),
        ("bytes_per_second", float("nan")),
    ],
)
def test_malformed_sample_cannot_be_evaluated(field: str, value: object) -> None:
    candidate, reference = measured_pair()
    candidate[field] = value
    with pytest.raises(performance.PerformanceError):
        performance.evaluate_sample(candidate, reference)


@pytest.mark.parametrize("payload", ['{"x":1,"x":2}', '{"x":NaN}', "[]"])
def test_measurement_json_rejects_ambiguous_input(tmp_path: Path, payload: str) -> None:
    path = tmp_path / "input.json"
    path.write_text(payload, encoding="utf-8")
    with pytest.raises(performance.PerformanceError):
        performance.read_json(path)


def test_recovery_rate_is_an_observation_without_a_target() -> None:
    observed = performance.sample(
        objective_id=None,
        scenario="a-riverhog-recovery-tool",
        workload="resume",
        context=None,
        completed_bytes=1024,
        elapsed_seconds=2,
        completion_verified=False,
        run_id=str(uuid4()),
    )
    assert performance.evaluate_sample(observed, None)["reason"] == "no-objective"


def test_nonexistent_source_cannot_be_rendered(monkeypatch: pytest.MonkeyPatch) -> None:
    item = performance.OBSERVATIONS[0]
    monkeypatch.setattr(
        performance,
        "OBSERVATIONS",
        (performance.AccountingNote(item.id, item.meaning, "missing-source.py"),),
    )
    with pytest.raises(performance.PerformanceError, match="stale accounting source"):
        performance.render()


def test_generated_view_has_no_execution_state() -> None:
    view = performance.render()
    assert "status: met" not in view.lower()
    assert "currently passing" not in view.lower()
    assert "release gate" in view
    assert json.dumps({"sample": 1}) not in view
