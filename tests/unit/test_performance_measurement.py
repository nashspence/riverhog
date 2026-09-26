from __future__ import annotations

import copy
from pathlib import Path
from uuid import uuid4

import pytest

from scripts import performance_measurement as measurement


def context() -> dict[str, object]:
    return {
        "format": measurement.CONTEXT_FORMAT,
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
        "scenario": "riverhog-ingress",
        "workload": "large-file",
        "context": shared,
        "completed_bytes": 1000,
        "completion_verified": True,
    }
    return (
        measurement.sample(**common, elapsed_seconds=20.0, run_id=str(uuid4())),
        measurement.sample(**common, elapsed_seconds=10.0, run_id=str(uuid4())),
    )


def test_measured_reference_reports_absolute_rates_and_optional_target() -> None:
    candidate, reference = measured_pair()
    compared = measurement.compare_sample(candidate, reference)
    assert compared == {
        "status": "compared",
        "reason": "matched-measured-reference",
        "report_only": True,
        "candidate_bytes_per_second": 50.0,
        "reference_bytes_per_second": 100.0,
        "reference_ratio": 0.5,
        "target_ratio": None,
    }
    assert measurement.compare_sample(candidate, reference, target_ratio=0.9)["status"] == "missed"
    assert measurement.compare_sample(candidate, reference, target_ratio=0.4)["status"] == "met"


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("completion_verified", False, "completion-unverified"),
        ("context", None, "comparison-context-missing"),
        ("scenario", "archive-upload", "incomparable-scenario"),
        ("workload", "resume", "incomparable-workload"),
    ],
)
def test_unverified_or_mismatched_reference_is_not_compared(
    field: str, value: object, reason: str
) -> None:
    candidate, reference = measured_pair()
    reference[field] = value
    compared = measurement.compare_sample(candidate, reference, target_ratio=0.9)
    assert compared["status"] == "not-compared"
    assert compared["reason"] == reason


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
    result = measurement.compare_sample(candidate, changed)
    assert result["status"] == "not-compared"
    assert result["reason"] == "incomparable-context"


def test_missing_invalid_and_self_reference_never_compare() -> None:
    candidate, _ = measured_pair()
    assert measurement.compare_sample(candidate, None)["reason"] == "no-measured-reference"
    assert measurement.compare_sample(candidate, candidate)["reason"] == "self-comparison"
    assert measurement.compare_sample(candidate, {"format": "other"})["reason"] == (
        "invalid-measured-reference"
    )


def test_forged_rate_and_overflow_cannot_become_a_comparison() -> None:
    candidate, reference = measured_pair()
    reference["bytes_per_second"] = 200.0
    assert (
        measurement.compare_sample(candidate, reference)["reason"] == "invalid-measured-reference"
    )
    candidate["elapsed_seconds"] = 1e-300
    candidate["bytes_per_second"] = 1e303
    reference["elapsed_seconds"] = 1e306
    reference["bytes_per_second"] = 1e-303
    with pytest.raises(measurement.PerformanceError, match="ratio is not finite"):
        measurement.compare_sample(candidate, reference)


@pytest.mark.parametrize("payload", ['{"x":1,"x":2}', '{"x":NaN}', "[]"])
def test_measurement_json_rejects_ambiguous_input(tmp_path: Path, payload: str) -> None:
    path = tmp_path / "input.json"
    path.write_text(payload, encoding="utf-8")
    with pytest.raises(measurement.PerformanceError):
        measurement.read_json(path)
