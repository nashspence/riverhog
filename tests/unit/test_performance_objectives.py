from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest

from scripts import performance_objectives as p


def context() -> dict:
    return {"schema": p.CONTEXT_SCHEMA, "comparison_id": str(uuid4()),
            "workload_sha256": "a" * 64, "environment_sha256": "b" * 64, "path_sha256": "c" * 64,
            "byte_domain": "logical-payload", "completion_boundary": "verified-complete",
            "cache_state": "cold", "concurrency": 1}


def pair() -> tuple[dict, dict]:
    shared = context()
    kwargs = dict(identity="transfer-goodput", scenario="riverhog-ingress", workload="large-file",
                  completed_bytes=1000, verified=True, context=shared,
                  source_start={"sha": "1" * 40, "clean": True},
                  source_finish={"sha": "1" * 40, "clean": True})
    reference = p.sample(**kwargs, elapsed_seconds=10.0, run_id=str(uuid4()))
    candidate = p.sample(**kwargs, elapsed_seconds=10.0, run_id=str(uuid4()))
    return candidate, reference


def test_registry_has_one_owner_for_every_retained_budget() -> None:
    p.validate_registry()
    assert len(p.OBJECTIVES) == 14
    assert len(p.DISPOSITIONS) == 9
    assert len(p.NETWORK_SCENARIOS) == 7
    assert len(p.WORKLOADS) == 3
    assert sum(item.rule == "reference-ratio" for item in p.OBJECTIVES) == 3
    assert len({item.budget for item in p.OBJECTIVES if item.rule == "reference-ratio"}) == 1


@pytest.mark.parametrize("name,bound,exclusive", [
    ("database-page-stream-peak", 32 * 1024**2, False),
    ("database-http-peak", 64 * 1024**2, False),
    ("listener-observation-peak", 4 * 1024**2, True),
    ("listener-runnable-peak", 1024**2, True),
    ("database-plan-temp-io", 0, False),
])
def test_exact_existing_absolute_boundaries(name, bound, exclusive) -> None:
    assert p.limit(name) == bound
    assert p.within(name, bound) is (not exclusive)
    assert not p.within(name, bound + 1)
    if bound:
        assert p.within(name, bound - 1)


@pytest.mark.parametrize("low", [0, 0.25, 1, 37, 10000])
@pytest.mark.parametrize("growth", [2, 16, 100])
def test_exact_existing_relative_inequalities(low, growth) -> None:
    limits = {
        "database-indexed-work-growth": (low + 1) * 4 + 1000 - 1,
        "database-unindexed-work-growth": (low + 1) * growth * 1.5 + 1000 - 1,
        "database-plan-latency-growth": low * max(8, growth * 4) + 100,
        "database-stream-memory-growth": low * 4 + 8 * 1024**2,
        "database-http-memory-growth": low * 4 + 8 * 1024**2,
        "database-stream-latency-growth": low * max(4, growth * 1.5) + 0.5,
    }
    for name, bound in limits.items():
        assert p.limit(name, low=low, growth=growth) == bound
        assert p.within(name, bound, low=low, growth=growth)
        assert not p.within(name, bound + 1, low=low, growth=growth)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf"), -1, True, "1", None])
def test_invalid_scalar_is_never_a_passing_check(value) -> None:
    with pytest.raises(p.PerformanceError):
        p.within("database-http-peak", value)


def test_unknown_objective_and_unqualified_goodput_scalar_are_rejected() -> None:
    with pytest.raises(p.PerformanceError):
        p.limit("not-registered")
    with pytest.raises(p.PerformanceError):
        p.within("transfer-goodput", 1)


def test_matched_reference_and_self_comparison() -> None:
    candidate, reference = pair()
    assert p.evaluate_sample(candidate, reference)["status"] == "met"
    assert p.evaluate_sample(candidate, candidate)["reason"] == "self_comparison"
    candidate["elapsed_seconds"] = 20
    candidate["bytes_per_second"] = 50
    assert p.evaluate_sample(candidate, reference)["status"] == "missed"
    candidate["elapsed_seconds"] = 1
    candidate["bytes_per_second"] = 1000
    assert p.evaluate_sample(candidate, reference)["reference_ratio"] == 10  # not clamped to one


@pytest.mark.parametrize("field,value,reason", [
    ("completion_verified", False, "completion_unverified"),
    ("definition_sha256", "d" * 64, "definition_changed"),
    ("context", None, "comparison_context_missing"),
    ("source_finish", {"sha": "2" * 40, "clean": True}, "source_not_clean_and_stable"),
    ("source_finish", {"sha": "1" * 40, "clean": False}, "source_not_clean_and_stable"),
    ("scenario", "archive-upload", "incomparable_scenario"),
    ("workload", "resume", "incomparable_workload"),
])
def test_invalid_comparison_never_passes(field, value, reason) -> None:
    candidate, reference = pair()
    reference[field] = value
    result = p.evaluate_sample(candidate, reference)
    assert result["status"] == "not_evaluated"
    assert result["reason"] == reason


@pytest.mark.parametrize("field", ["comparison_id", "workload_sha256", "environment_sha256",
        "path_sha256",
                                    "byte_domain", "completion_boundary", "cache_state",
        "concurrency"])
def test_every_comparison_dimension_must_match(field) -> None:
    candidate, reference = pair()
    replacements = {"comparison_id": str(uuid4()), "workload_sha256": "f" * 64,
                    "environment_sha256": "f" * 64, "path_sha256": "f" * 64,
                    "byte_domain": "stored-payload", "completion_boundary": "other-boundary",
                    "cache_state": "warm", "concurrency": 2}
    reference["context"][field] = replacements[field]
    assert p.evaluate_sample(candidate, reference)["reason"] == "incomparable_context"


def test_missing_reference_is_not_zero_or_pass() -> None:
    candidate, _ = pair()
    result = p.evaluate_sample(candidate, None)
    assert result["status"] == "not_evaluated"
    assert result["reference_ratio"] is None


def test_changed_numerator_and_forged_rate_are_not_accepted() -> None:
    candidate, reference = pair()
    reference["completed_bytes"] = 2000
    reference["bytes_per_second"] = 200
    assert p.evaluate_sample(candidate, reference)["reason"] == "incomparable_completed_bytes"
    reference["bytes_per_second"] = 1
    with pytest.raises(p.PerformanceError):
        p.evaluate_sample(candidate, reference)


@pytest.mark.parametrize("field,value", [("completed_bytes", True), ("completed_bytes", 0),
                                         ("elapsed_seconds", 0), ("elapsed_seconds", float("inf")),
                                         ("completion_verified", "yes"), ("bytes_per_second",
            float("nan"))])
def test_malformed_sample_is_invalid_not_a_target_miss(field, value) -> None:
    candidate, reference = pair()
    candidate[field] = value
    with pytest.raises(p.PerformanceError):
        p.evaluate_sample(candidate, reference)


@pytest.mark.parametrize("text", ['{"x":1,"x":2}', '{"x":NaN}', '{"x":Infinity}', '[]'])
def test_strict_json(tmp_path: Path, text: str) -> None:
    path = tmp_path / "input.json"
    path.write_text(text)
    with pytest.raises(p.PerformanceError):
        p.read_json(path)


def test_render_is_deterministic_and_contains_no_run_claim() -> None:
    first = p.render()
    assert first == p.render()
    assert "Non-contractual" in first
    for item in p.OBJECTIVES:
        assert first.count("### " + item.id + "\n") == 1
        assert p.rule_text(item) in first
    assert "no existing latency ceiling" in first.lower()
    assert "not RSS" in first


def test_reference_validation_uses_symbols_not_line_numbers(tmp_path: Path) -> None:
    path = tmp_path / "test_x.py"
    path.write_text("\n\ndef test_x():\n    pass\n")
    assert p._reference_exists(tmp_path, "test_x.py::test_x")
    assert not p._reference_exists(tmp_path, "test_x.py::test_missing")
    assert not p._reference_exists(tmp_path, "../test_x.py::test_x")


def test_discovery_returns_leads_not_automatic_objectives(tmp_path: Path) -> None:
    path = tmp_path / "new_test.py"
    path.write_text("assert peak < 12345\n")
    rows = p.discover(tmp_path)
    assert rows[0]["path"] == "new_test.py"
    assert len(p.OBJECTIVES) == 14


def test_nonfinite_budget_is_not_rendered_as_a_target(monkeypatch) -> None:
    replacement = dict(p.BUDGETS)
    replacement["page-stream-peak"] = {"maximum": float("nan")}
    monkeypatch.setattr(p, "BUDGETS", replacement)
    with pytest.raises(p.PerformanceError):
        p.validate_registry()


def test_duplicate_disposition_is_not_silently_rendered(monkeypatch) -> None:
    monkeypatch.setattr(p, "DISPOSITIONS", (*p.DISPOSITIONS, p.DISPOSITIONS[0]))
    with pytest.raises(p.PerformanceError):
        p.validate_registry()
