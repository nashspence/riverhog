from __future__ import annotations

import copy
from dataclasses import replace

import pytest

from scripts.validation_evidence import (
    ValidationWork,
    observe_performance,
    validate_evidence_records,
)

SOURCE = "a" * 40
RUN = "fixed-invocation"


def row(work, seconds=601, status=0, growth=None):
    return {
        "format": "riverhog-ci-validation-evidence/v2",
        "source_sha": SOURCE,
        "source_clean": True,
        "source_end": {"source_sha": SOURCE, "source_clean": True},
        "run_id": RUN,
        "definition": work.definition(),
        "state": "completed",
        "elapsed_seconds": seconds,
        "phase_costs": {
            "preparation": 0,
            "command-including-owner-cleanup": seconds,
            "evidence-collection": 0,
        },
        "exit_status": status,
        "growth_results": {} if growth is None else growth,
        "started_at": "2026-01-01T00:00:00+00:00",
        "finished_at": "2026-01-01T00:00:01+00:00",
        "started_monotonic_seconds": 0,
        "observed_conditions": {"platform": "linux", "architecture": "x86_64", "python": "3.12"},
        "advisory_target": work.advisory_target(),
    }


def check(work, record):
    return validate_evidence_records([record], expected=[work], source_sha=SOURCE, run_id=RUN)


@pytest.mark.parametrize(
    "field",
    [
        "started_at",
        "finished_at",
        "started_monotonic_seconds",
        "observed_conditions",
        "phase_costs",
    ],
)
def test_required_measurement_context_cannot_be_omitted(field):
    work = ValidationWork("test", ("true",))
    record = row(work)
    del record[field]
    with pytest.raises(ValueError):
        check(work, record)


@pytest.mark.parametrize(
    "field,value",
    [
        ("started_at", "2026-01-01T00:00:00"),
        ("finished_at", "invalid"),
        ("started_monotonic_seconds", float("nan")),
        ("observed_conditions", {}),
    ],
)
def test_invalid_required_measurement_context_is_not_neutral_metadata(field, value):
    work = ValidationWork("test", ("true",))
    record = row(work)
    record[field] = value
    with pytest.raises(ValueError):
        check(work, record)


@pytest.mark.parametrize("target", [None, 1, 600, 1000000000])
@pytest.mark.parametrize("seconds", [0, 600, 601, 1e12])
def test_valid_speed_observations_never_change_qualification(target, seconds):
    work = ValidationWork("test", ("true",), target)
    result = check(work, row(work, seconds))
    assert result["qualification_exit_status"] == 0
    assert result["evidence_complete"] is True
    comparison = result["comparisons"]["test"]
    assert comparison["report_only"] is True
    assert comparison["status"] == (
        "not-compared" if target is None else "met" if seconds < target else "missed"
    )


def test_goal_does_not_enter_required_work_definition():
    work = ValidationWork("test", ("true",), 600)
    assert work.definition() == replace(work, target_seconds=None).definition()
    assert work.definition() == replace(work, target_seconds=1).definition()
    assert "target_seconds" not in work.definition()
    assert "timing_policy" not in work.definition()


@pytest.mark.parametrize("new_target", [None, 1, 100000])
def test_changed_or_removed_current_goal_never_invalidates_prior_evidence(new_target):
    measured_work = ValidationWork("test", ("true",), 600)
    result = check(replace(measured_work, target_seconds=new_target), row(measured_work))
    assert result["qualification_exit_status"] == 0
    assert result["comparisons"]["test"]["status"] == "missed"


def test_target_and_comparison_metadata_are_optional():
    work = ValidationWork("test", ("true",), 600)
    record = row(work)
    del record["advisory_target"]
    result = check(work, record)
    assert result["qualification_exit_status"] == 0
    assert result["comparisons"]["test"]["reason"] == "no-target"


@pytest.mark.parametrize(
    "key,value", [("conditions", {"hardware": "different"}), ("boundary", "other-phase")]
)
def test_noncomparable_advisory_goal_is_neutral(key, value):
    work = ValidationWork("test", ("true",), 600)
    record = row(work)
    record["advisory_target"][key] = value
    result = check(work, record)
    assert result["qualification_exit_status"] == 0
    assert result["comparisons"]["test"]["reason"] == "conditions-not-comparable"


@pytest.mark.parametrize("target", [None, 600])
@pytest.mark.parametrize("code", [1, 7, -9, 130])
def test_command_failure_is_independent_of_goal(target, code):
    work = ValidationWork("test", ("false",), target)
    result = check(work, row(work, 1, code))
    assert result["qualification_exit_status"] != 0
    assert result["functional_exit_status"] == {"test": code}
    assert result["comparisons"]["test"]["reason"] == "command-failed"


@pytest.mark.parametrize("target", [None, 600])
@pytest.mark.parametrize("outcome", ["pass", "fail"])
def test_required_growth_result_is_independent_of_goal(target, outcome):
    work = ValidationWork("test", ("true",), target, growth_witnesses=("owner/bound",))
    result = check(work, row(work, growth={"owner/bound": outcome}))
    assert result["qualification_exit_status"] == (1 if outcome == "fail" else 0)


@pytest.mark.parametrize(
    "growth", [{}, {"owner/bound": "missing"}, {"owner/bound": "pass", "extra": "pass"}, None]
)
def test_missing_or_invalid_required_growth_result_remains_error(growth):
    work = ValidationWork("test", ("true",), growth_witnesses=("owner/bound",))
    record = row(work)
    record["growth_results"] = growth
    with pytest.raises(ValueError, match="growth"):
        check(work, record)


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_sha", "b" * 40),
        ("run_id", "wrong"),
        ("source_clean", False),
        ("state", "running"),
        ("state", "interrupted"),
        ("state", "launch-failed"),
        ("exit_status", True),
        ("elapsed_seconds", -1),
        ("elapsed_seconds", True),
        ("elapsed_seconds", float("nan")),
        ("elapsed_seconds", float("inf")),
        ("source_end", {"source_sha": "b" * 40, "source_clean": True}),
    ],
)
def test_required_execution_measurement_and_identity_integrity_still_fail(field, value):
    work = ValidationWork("test", ("true",))
    record = row(work)
    record[field] = value
    with pytest.raises(ValueError):
        check(work, record)


@pytest.mark.parametrize(
    "field,value",
    [
        ("command", ["different"]),
        ("boundary", "subset"),
        ("conditions", {"changed": "yes"}),
        ("growth_witnesses", ["silently-replaced"]),
    ],
)
def test_real_work_contract_cannot_be_rewritten(field, value):
    work = ValidationWork("test", ("true",))
    record = row(work)
    record["definition"][field] = value
    with pytest.raises(ValueError, match="required workload"):
        check(work, record)


@pytest.mark.parametrize("target", [0, -1, True, "600", float("inf"), float("nan"), 10**1000])
def test_malformed_supplied_target_is_data_error_not_a_miss(target):
    with pytest.raises(ValueError):
        ValidationWork("test", ("true",), target)


def test_fabricated_successful_comparison_is_invalid_evidence():
    work = ValidationWork("test", ("true",), 600)
    record = row(work)
    record["profiling_target"] = {"report_only": True, "status": "met"}
    with pytest.raises(ValueError, match="contradicts"):
        check(work, record)


def test_neutral_comparison_is_not_required_evidence_absence():
    work = ValidationWork("test", ("true",))
    record = row(work)
    record["profiling_target"] = observe_performance(
        work.definition(), elapsed=601, exit_status=0, source_clean=True
    )
    assert check(work, record)["qualification_exit_status"] == 0
    del record["elapsed_seconds"]
    with pytest.raises(ValueError):
        check(work, record)


def test_exact_work_coverage_and_nonmutation():
    a, b = ValidationWork("a", ("true",)), ValidationWork("b", ("true",))
    record = row(a)
    original = copy.deepcopy(record)
    with pytest.raises(ValueError, match="missing validation evidence"):
        validate_evidence_records([record], expected=[a, b], source_sha=SOURCE, run_id=RUN)
    with pytest.raises(ValueError, match="duplicate"):
        validate_evidence_records([record, record], expected=[a], source_sha=SOURCE, run_id=RUN)
    assert record == original


def test_bad_report_only_flag_is_data_error_not_target_authority():
    work = ValidationWork("test", ("true",))
    record = row(work)
    record["profiling_target"] = observe_performance(
        work.definition(), elapsed=601, exit_status=0, source_clean=True
    )
    record["profiling_target"]["report_only"] = 1
    with pytest.raises(ValueError, match="contradicts"):
        check(work, record)


@pytest.mark.parametrize(
    "phases",
    [
        {},
        {"preparation": 0},
        {"preparation": 0, "command-including-owner-cleanup": 1, "evidence-collection": 0},
        {"preparation": True, "command-including-owner-cleanup": 601, "evidence-collection": 0},
    ],
)
def test_missing_malformed_or_contradictory_phase_costs_fail_required_evidence(phases):
    work = ValidationWork("test", ("true",))
    record = row(work)
    record["phase_costs"] = phases
    with pytest.raises(ValueError):
        check(work, record)


def test_changed_normative_declaration_changes_required_work_identity():
    work = ValidationWork("test", ("true",), growth_declarations=(("owner.growth/v1", "a" * 64),))
    changed = replace(work, growth_declarations=(("owner.growth/v1", "b" * 64),))
    assert work.definition() != changed.definition()
    with pytest.raises(ValueError):
        check(changed, row(work))
    assert work.definition() == replace(work, target_seconds=1).definition()
