from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
import riverhog_growth_contracts
from riverhog_canonical_json import require_canonical_json
from riverhog_growth_contracts import NumericWorkPolicy, NumericWorkRefused

from scripts import ci_make, ci_qualification, ci_timing, validation_numeric
from scripts.validation_evidence import ValidationWork, validate_numeric_refusal
from scripts.validation_numeric import (
    NumericConfigurationError,
    bind_numeric_environment,
    numeric_environment,
    numeric_policy_conditions,
    numeric_policy_from_conditions,
    numeric_policy_from_environment,
    numeric_policy_identity,
    numeric_refusal_receipt,
    qualification_numeric_policy,
)


@pytest.fixture
def policy() -> NumericWorkPolicy:
    return NumericWorkPolicy(100_000, 64)


def test_profile_is_explicit_and_has_no_missing_file_fallback(tmp_path, monkeypatch):
    path = tmp_path / "numeric.json"
    monkeypatch.setattr(validation_numeric, "PROFILE_PATH", path)
    with pytest.raises(ValueError, match="unavailable"):
        qualification_numeric_policy()
    path.write_text(
        json.dumps(
            {
                "format": validation_numeric.PROFILE_FORMAT,
                "profile": validation_numeric.PROFILE_NAME,
                "max_work": 12345,
                "max_integer_bits": 67,
            }
        )
    )
    assert qualification_numeric_policy() == NumericWorkPolicy(12345, 67)


@pytest.mark.parametrize(
    "field,value",
    [
        ("format", "unknown"),
        ("profile", "archive-selected"),
        ("max_work", 0),
        ("max_work", True),
        ("max_work", "100"),
        ("max_integer_bits", -1),
        ("max_integer_bits", 1.0),
    ],
)
def test_invalid_owned_profile_is_not_a_resource_grant(tmp_path, monkeypatch, field, value):
    path = tmp_path / "numeric.json"
    row = {
        "format": validation_numeric.PROFILE_FORMAT,
        "profile": validation_numeric.PROFILE_NAME,
        "max_work": 12345,
        "max_integer_bits": 67,
    }
    row[field] = value
    path.write_text(json.dumps(row))
    monkeypatch.setattr(validation_numeric, "PROFILE_PATH", path)
    with pytest.raises(ValueError):
        qualification_numeric_policy()


def test_duplicate_configuration_fields_are_not_last_value_wins(tmp_path, monkeypatch):
    path = tmp_path / "numeric.json"
    path.write_text('{"max_work":1,"max_work":100000}')
    monkeypatch.setattr(validation_numeric, "PROFILE_PATH", path)
    with pytest.raises(ValueError, match="unavailable"):
        qualification_numeric_policy()


def test_owned_conditions_environment_and_receipt_have_one_identity(policy):
    conditions = dict(numeric_policy_conditions(policy))
    assert numeric_policy_from_conditions(conditions) == policy
    environment = numeric_environment(policy)
    assert numeric_policy_from_environment(environment) == policy
    identity = numeric_policy_identity(policy)
    assert identity["max_work"] == "100000"
    assert identity["max_integer_bits"] == "64"
    assert len(identity["reader_sha256"]) == 64
    assert {
        key.removeprefix("growth_numeric_"): value for key, value in conditions.items()
    } == identity


@pytest.mark.parametrize(
    "defect", ["missing", "partial", "zero", "bool", "leading-zero", "reader", "extra"]
)
def test_missing_or_invalid_environment_refuses_at_owned_boundary(policy, defect):
    environment = numeric_environment(policy)
    if defect == "missing":
        environment.clear()
    elif defect == "partial":
        environment.pop("RIVERHOG_VALIDATION_NUMERIC_MAX_WORK")
    elif defect == "reader":
        environment["RIVERHOG_VALIDATION_NUMERIC_READER_SHA256"] = "0" * 64
    elif defect == "extra":
        environment["RIVERHOG_VALIDATION_NUMERIC_UNDECLARED"] = "1"
    else:
        environment["RIVERHOG_VALIDATION_NUMERIC_MAX_WORK"] = {
            "zero": "0",
            "bool": "True",
            "leading-zero": "0100",
        }[defect]
    with pytest.raises(ValueError):
        numeric_policy_from_environment(environment)


def test_inherited_configuration_cannot_enlarge_the_selected_work(policy):
    environment = numeric_environment(
        NumericWorkPolicy(policy.max_work + 1, policy.max_integer_bits)
    )
    with pytest.raises(ValueError, match="differ"):
        bind_numeric_environment(environment, policy)
    environment = {"UNRELATED": "kept"}
    bind_numeric_environment(environment, policy)
    assert environment["UNRELATED"] == "kept"
    assert numeric_policy_from_environment(environment) == policy


def test_actual_imported_reader_must_match_the_qualification_checkout(
    tmp_path, monkeypatch, policy
):
    different = tmp_path / "old_reader.py"
    different.write_text("# a different installed implementation\n")
    monkeypatch.setattr(riverhog_growth_contracts, "__file__", str(different))
    with pytest.raises(ValueError, match="loaded numeric reader differs"):
        numeric_policy_identity(policy)


def test_growth_work_requires_conditions_before_launch(monkeypatch):
    for key in tuple(os.environ):
        if key.startswith("RIVERHOG_VALIDATION_NUMERIC_"):
            monkeypatch.delenv(key)
    work = ValidationWork("fixture", ("product",), growth_witnesses=("owned-witness",))
    with pytest.raises(ValueError, match="missing or malformed"):
        ci_qualification.validation_environment(work, "fixture-run")


@pytest.mark.parametrize("reason", ["configuration", "budget"])
def test_direct_make_does_not_swallow_numeric_admission_failure(
    tmp_path, monkeypatch, policy, reason
):
    error = (
        NumericConfigurationError("missing owned profile")
        if reason == "configuration"
        else NumericWorkRefused(
            "work_allowance_exhausted",
            operation="source_admission",
            limit=policy.max_work,
            consumed=policy.max_work,
            max_integer_bits=policy.max_integer_bits,
            requested=1,
        )
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(ci_make, "source_identity", lambda _: {})
    monkeypatch.setattr(ci_make, "retain_completed_runs", lambda *_: None)
    monkeypatch.setattr(ci_make, "recipe_plan", lambda *_: [])

    def refuse(*_):
        raise error

    calls = []
    monkeypatch.setattr(ci_make, "make_work_definitions", refuse)
    monkeypatch.setattr(ci_make, "run_validation_child", lambda *_, **__: calls.append("launched"))
    with pytest.raises(type(error)) as received:
        ci_make.observe_make(tmp_path / "Makefile", ("unit",))
    assert received.value is error
    assert calls == []
    assert not list(tmp_path.rglob("manifest.json"))


@pytest.mark.parametrize("defect", ["profile", "missing", "negative", "reason", "operation"])
def test_refusal_receipt_cannot_change_its_owned_budget_or_diagnostics(policy, defect):
    refusal = NumericWorkRefused(
        "work_allowance_exhausted",
        operation="metric_dispatch",
        limit=policy.max_work,
        consumed=policy.max_work,
        max_integer_bits=policy.max_integer_bits,
        requested=1,
    )
    value = numeric_refusal_receipt(refusal, numeric_policy=policy)
    conditions = dict(numeric_policy_conditions(policy))
    validate_numeric_refusal(value, conditions)
    if defect == "profile":
        value["max_work"] = str(policy.max_work + 1)
    elif defect == "missing":
        value.pop("consumed")
    elif defect == "negative":
        value["requested"] = "-1"
    elif defect == "reason":
        value["reason"] = "x" * 129
    else:
        value["operation"] = "x" * 129
    with pytest.raises(ValueError, match="owned resource"):
        validate_numeric_refusal(value, conditions)


def test_refused_attempt_never_becomes_completed_qualification(policy):
    from tests.unit.test_validation_evidence import check, row

    work = ValidationWork("fixture", ("product",), conditions=numeric_policy_conditions(policy))
    record = row(work)
    refusal = NumericWorkRefused(
        "work_allowance_exhausted",
        operation="metric_dispatch",
        limit=policy.max_work,
        consumed=policy.max_work,
        max_integer_bits=policy.max_integer_bits,
        requested=1,
    )
    record["numeric_refusal"] = numeric_refusal_receipt(refusal, numeric_policy=policy)
    with pytest.raises(ValueError):
        check(work, record)
    record["state"] = "numeric-refused"
    with pytest.raises(ValueError):
        check(work, record)
    record.pop("numeric_refusal")
    with pytest.raises(ValueError):
        check(work, record)


@pytest.mark.parametrize("diagnostics", ["original", "oversized", "forged", "missing"])
def test_numeric_refusal_preserves_completed_original_product_and_execution_receipt(
    policy, tmp_path: Path, monkeypatch, diagnostics
):
    for key in tuple(os.environ):
        if key.startswith("RIVERHOG_VALIDATION_NUMERIC_"):
            monkeypatch.delenv(key)
    work = ValidationWork(
        "fixture",
        ("owned-product",),
        conditions=numeric_policy_conditions(policy),
        growth_witnesses=("owned-witness",),
    )
    monkeypatch.setattr(ci_qualification, "ROOT", tmp_path)
    monkeypatch.setattr(ci_qualification, "validation_work", lambda _: work)
    monkeypatch.setattr(
        ci_timing, "source_identity", lambda _: {"source_sha": "a" * 40, "source_clean": True}
    )
    monkeypatch.setenv("RIVERHOG_CI_TIMING_DIR", str(tmp_path / "timing"))
    outcomes = []

    def product(command, *, cwd, env):
        assert tuple(command) == work.command
        assert cwd == tmp_path
        assert numeric_policy_from_environment(env) == policy
        outcomes.append("confirmed original product completion")
        return 0

    refusal = NumericWorkRefused(
        "work_allowance_exhausted",
        operation="metric_dispatch",
        limit=policy.max_work,
        consumed=policy.max_work - 1,
        max_integer_bits=policy.max_integer_bits,
        requested=10,
    )
    if diagnostics == "oversized":
        refusal.requested = 1 << 20_000
    elif diagnostics == "forged":
        refusal.limit += 1
    elif diagnostics == "missing":
        del refusal.requested

    def collect(owned, invocation):
        assert owned is work and invocation == "numeric-refusal"
        assert outcomes == ["confirmed original product completion"]
        raise refusal

    monkeypatch.setattr(ci_timing, "run_validation_child", product)
    path = tmp_path / "execution.json"
    assert (
        ci_timing.execute_validation("fixture", path, "numeric-refusal", growth_collector=collect)
        == 2
    )
    receipt = require_canonical_json(path.read_bytes())
    assert receipt["state"] == (
        "numeric-refused" if diagnostics == "original" else "evidence-failed"
    )
    assert receipt["exit_status"] == 0
    assert receipt["growth_results"] == {}
    assert receipt["source_sha"] == "a" * 40
    assert receipt["source_end"] == {"source_sha": "a" * 40, "source_clean": True}
    assert receipt["definition"]["conditions"] == dict(numeric_policy_conditions(policy))
    if diagnostics == "original":
        assert receipt["numeric_refusal"] == {
            "reason": "work_allowance_exhausted",
            "operation": "metric_dispatch",
            "max_work": str(policy.max_work),
            "max_integer_bits": str(policy.max_integer_bits),
            "consumed": str(policy.max_work - 1),
            "requested": "10",
        }
    else:
        assert "numeric_refusal" not in receipt
    assert outcomes == ["confirmed original product completion"]
    assert not list((tmp_path / "timing").rglob(".active-*"))
