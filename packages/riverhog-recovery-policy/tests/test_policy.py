from __future__ import annotations

from dataclasses import FrozenInstanceError, replace

import pytest
from riverhog_recovery_policy import (
    CheckpointCandidate,
    CheckpointDecision,
    RecoveryAccounting,
    RecoveryPolicy,
    decide_checkpoint,
    net_saved_replay_ns,
)

NS = 1_000_000_000


def candidate(**changes: object) -> CheckpointCandidate:
    return replace(
        CheckpointCandidate(
            replay_ns=120 * NS,
            restore_ns=0,
            cost_ns=NS,
            remaining_ns=None,
            replay_provenance="completed useful work",
            restore_provenance="admitted restore profile",
            cost_provenance="complete candidate lifecycle estimate",
            remaining_provenance="unknown remaining input",
        ),
        **changes,
    )


def test_defaults_and_immutable_configuration() -> None:
    policy = RecoveryPolicy()
    assert policy.recovery_mode == "adaptive"
    assert policy.replay_target_seconds == 120
    assert policy.target_ns == 120 * NS
    assert policy.max_optional_overhead_fraction == 1 / 8
    assert policy.max_optional_checkpoints is None
    assert policy.retained_checkpoint_generations == 2
    with pytest.raises(FrozenInstanceError):
        policy.recovery_mode = "restart"  # type: ignore[misc]


@pytest.mark.parametrize(
    ("saved", "admitted"), [(120 * NS - 1, False), (120 * NS, True), (120 * NS + 1, True)]
)
def test_replay_threshold_uses_completed_net_work(saved: int, admitted: bool) -> None:
    result = decide_checkpoint(
        RecoveryPolicy(), RecoveryAccounting(useful_ns=120 * NS), candidate(replay_ns=saved)
    )
    assert result.admitted is admitted
    assert result.reserved_ns == (NS if admitted else 0)
    assert result.net_saved_ns == saved


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"completion_ready": True}, "completion-ready"),
        ({"mandatory_anchor_ready": True}, "mandatory-anchor-ready"),
        ({"consistent": False}, "inconsistent-candidate"),
        ({"storage_admitted": False}, "storage-not-admitted"),
        ({"checkpoint_in_flight": True}, "checkpoint-in-flight"),
        ({"replay_ns": None}, "unknown-saved-replay"),
        ({"restore_ns": None}, "unknown-restore-cost"),
        ({"cost_ns": None}, "unknown-checkpoint-cost"),
        ({"remaining_ns": NS}, "near-completion"),
        ({"remaining_ns": NS // 2}, "near-completion"),
    ],
)
def test_suppression_preserves_required_work_and_resources(
    changes: dict[str, object], reason: str
) -> None:
    result = decide_checkpoint(
        RecoveryPolicy(), RecoveryAccounting(useful_ns=120 * NS), candidate(**changes)
    )
    assert result.reason == reason and not result.admitted and result.reserved_ns == 0


def test_completion_precedes_unknown_cost_and_unavailable_optional_resources() -> None:
    result = decide_checkpoint(
        RecoveryPolicy(),
        RecoveryAccounting(reconciled=False),
        candidate(
            completion_ready=True,
            replay_ns=None,
            cost_ns=None,
            consistent=False,
            storage_admitted=False,
        ),
    )
    assert result.reason == "completion-ready"


def test_restart_disables_only_optional_admission() -> None:
    policy = RecoveryPolicy(recovery_mode="restart")
    accounting = RecoveryAccounting(useful_ns=1000 * NS)
    assert decide_checkpoint(policy, accounting, candidate()).reason == "restart-mode"
    assert (
        decide_checkpoint(policy, accounting, candidate(mandatory_anchor_ready=True)).reason
        == "mandatory-anchor-ready"
    )


@pytest.mark.parametrize(
    ("saved", "admitted"), [(209 * NS, False), (210 * NS, True), (211 * NS, True)]
)
def test_expensive_candidate_needs_saved_work_as_well_as_workflow_credit(
    saved: int, admitted: bool
) -> None:
    decision = decide_checkpoint(
        RecoveryPolicy(),
        RecoveryAccounting(useful_ns=1000 * NS),
        candidate(replay_ns=saved, cost_ns=30 * NS),
    )
    assert decision.admitted is admitted
    if not admitted:
        assert decision.reason == "candidate-too-expensive"


@pytest.mark.parametrize(
    ("credit", "admitted"), [(119 * NS, False), (126 * NS, True), (127 * NS, True)]
)
def test_failed_cost_and_held_reservations_spend_the_same_workflow_budget(
    credit: int, admitted: bool
) -> None:
    decision = decide_checkpoint(
        RecoveryPolicy(),
        RecoveryAccounting(useful_ns=credit, charged_ns=8 * NS, reserved_ns=9 * NS),
        candidate(),
    )
    assert decision.admitted is admitted
    if not admitted:
        assert decision.reason == "workflow-budget-exhausted"


def test_unknown_accounting_cannot_fabricate_restart_credit() -> None:
    result = decide_checkpoint(
        RecoveryPolicy(), RecoveryAccounting(useful_ns=1000 * NS, reconciled=False), candidate()
    )
    assert result.reason == "unreconciled-accounting"


def test_restore_cost_reduces_saved_replay_without_negative_credit() -> None:
    assert (
        net_saved_replay_ns(restart_from_frontier_ns=150 * NS, restart_from_candidate_ns=31 * NS)
        == 119 * NS
    )
    assert (
        net_saved_replay_ns(restart_from_frontier_ns=20 * NS, restart_from_candidate_ns=30 * NS)
        == 0
    )
    result = decide_checkpoint(
        RecoveryPolicy(),
        RecoveryAccounting(useful_ns=1000 * NS),
        candidate(replay_ns=150 * NS, restore_ns=31 * NS),
    )
    assert result.reason == "below-replay-target" and result.net_saved_ns == 119 * NS


def test_zero_fraction_refuses_positive_cost_but_unknown_does_not_mean_zero() -> None:
    policy = RecoveryPolicy(max_optional_overhead_fraction=0)
    accounting = RecoveryAccounting(useful_ns=1000 * NS)
    assert decide_checkpoint(policy, accounting, candidate()).reason == "candidate-too-expensive"
    assert decide_checkpoint(policy, accounting, candidate(cost_ns=0)).admitted
    assert (
        decide_checkpoint(policy, accounting, candidate(cost_ns=None)).reason
        == "unknown-checkpoint-cost"
    )


def test_zero_count_limit_suppresses_even_zero_cost_candidates() -> None:
    assert (
        decide_checkpoint(
            RecoveryPolicy(max_optional_checkpoints=0),
            RecoveryAccounting(useful_ns=1000 * NS),
            candidate(cost_ns=0),
        ).reason
        == "count-limit"
    )


def test_count_is_distinct_from_generation_retention_and_failed_cost() -> None:
    policy = RecoveryPolicy(max_optional_checkpoints=3, retained_checkpoint_generations=1)
    accounting = RecoveryAccounting(
        useful_ns=1000 * NS, charged_ns=5 * NS, successful_checkpoints=2
    )
    assert decide_checkpoint(policy, accounting, candidate()).admitted
    assert (
        decide_checkpoint(policy, replace(accounting, successful_checkpoints=3), candidate()).reason
        == "count-limit"
    )
    held = replace(accounting, held_count_slots=1)
    assert held.successful_checkpoints == 2
    assert decide_checkpoint(policy, held, candidate()).reason == "count-limit"


def test_lifetime_costs_do_not_overflow_or_saturate() -> None:
    huge = 10**400
    result = decide_checkpoint(
        RecoveryPolicy(),
        RecoveryAccounting(useful_ns=huge, charged_ns=huge, reserved_ns=huge),
        candidate(replay_ns=huge, cost_ns=huge // 10),
    )
    assert result.reason == "overrun-debt"


@pytest.mark.parametrize("value", [True, False, float("nan"), float("inf"), -1, "120", None])
def test_invalid_policy_times_and_fractions_refuse(value: object) -> None:
    with pytest.raises(ValueError):
        RecoveryPolicy(replay_target_seconds=value)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        RecoveryPolicy(max_optional_overhead_fraction=value)  # type: ignore[arg-type]


@pytest.mark.parametrize("value", [0, 1, 1.1])
def test_policy_ranges_refuse_zero_target_and_fraction_at_or_above_one(value: float) -> None:
    if value == 0:
        with pytest.raises(ValueError):
            RecoveryPolicy(replay_target_seconds=value)
    else:
        with pytest.raises(ValueError):
            RecoveryPolicy(max_optional_overhead_fraction=value)


@pytest.mark.parametrize("value", [True, 1.0, -1, "2"])
def test_count_types_do_not_accept_booleans_or_fractional_values(value: object) -> None:
    with pytest.raises(ValueError):
        RecoveryPolicy(max_optional_checkpoints=value)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        RecoveryAccounting(successful_checkpoints=value)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        RecoveryAccounting(held_count_slots=value)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        RecoveryPolicy(retained_checkpoint_generations=value)  # type: ignore[arg-type]


@pytest.mark.parametrize("value", [True, False, 1.0, float("nan"), float("inf"), -1, "120"])
@pytest.mark.parametrize("name", ["replay", "restore", "cost", "remaining"])
def test_estimates_require_explicit_nonnegative_integer_nanoseconds(
    name: str, value: object
) -> None:
    with pytest.raises(ValueError):
        candidate(**{f"{name}_ns": value})
    with pytest.raises(ValueError):
        RecoveryAccounting(charged_ns=value)  # type: ignore[arg-type]


@pytest.mark.parametrize("name", ["replay", "restore", "cost", "remaining"])
@pytest.mark.parametrize("value", ["", " ", None, 1])
def test_known_and_unknown_estimates_require_provenance(name: str, value: object) -> None:
    for estimate in (0, None):
        with pytest.raises(ValueError):
            candidate(**{f"{name}_ns": estimate, f"{name}_provenance": value})


@pytest.mark.parametrize(
    ("reason", "reservation"),
    [
        ("unknown-reason", 0),
        ("count-limit", 1),
        ("checkpoint-admitted", -1),
        ("checkpoint-admitted", True),
    ],
)
def test_reservation_proposals_cannot_invent_negative_or_suppressed_credit(
    reason: str, reservation: object
) -> None:
    with pytest.raises(ValueError):
        CheckpointDecision(reason, reservation)  # type: ignore[arg-type]


def test_operator_mapping_preserves_defaults_and_round_trips() -> None:
    policy = RecoveryPolicy.from_mapping({})
    assert policy == RecoveryPolicy()
    assert RecoveryPolicy.from_mapping(policy.to_mapping()) == policy
    configured = RecoveryPolicy.from_mapping(
        {"recovery_mode": "restart", "max_optional_checkpoints": 0}
    )
    assert configured.recovery_mode == "restart" and configured.max_optional_checkpoints == 0


@pytest.mark.parametrize("value", [{"unknown": 1}, {1: "adaptive"}, {"recovery_mode": True}])
def test_operator_mapping_rejects_unknown_or_coerced_values(value) -> None:
    with pytest.raises(ValueError):
        RecoveryPolicy.from_mapping(value)


def test_target_conversion_rounds_up_using_the_exact_selected_numeric_value() -> None:
    assert RecoveryPolicy(replay_target_seconds=0.1).target_ns == 100_000_001
    assert RecoveryPolicy(replay_target_seconds=0.125).target_ns == 125_000_000
    integer = 2**53 + 1
    policy = RecoveryPolicy(replay_target_seconds=integer)
    assert policy.target_ns == integer * NS
    assert policy.to_mapping()["replay_target_seconds"] == integer
    assert type(policy.to_mapping()["replay_target_seconds"]) is int
    assert RecoveryPolicy(replay_target_seconds=10**400).target_ns == 10**400 * NS


def test_one_nanosecond_changes_admission_at_exact_cost_and_budget_boundaries() -> None:
    policy = RecoveryPolicy()
    accounting = RecoveryAccounting(useful_ns=140 * NS)
    exact = candidate(replay_ns=140 * NS, cost_ns=20 * NS)
    assert decide_checkpoint(policy, accounting, exact).admitted
    assert (
        decide_checkpoint(policy, accounting, replace(exact, cost_ns=20 * NS + 1)).reason
        == "candidate-too-expensive"
    )
    accounting = RecoveryAccounting(useful_ns=126 * NS, charged_ns=8 * NS, reserved_ns=9 * NS)
    assert decide_checkpoint(policy, accounting, candidate()).admitted
    assert (
        decide_checkpoint(policy, replace(accounting, useful_ns=126 * NS - 1), candidate()).reason
        == "workflow-budget-exhausted"
    )
    debt = RecoveryAccounting(useful_ns=7 * NS - 1, charged_ns=NS)
    assert decide_checkpoint(policy, debt, candidate(cost_ns=0)).reason == "overrun-debt"


def test_fraction_uses_the_selected_binary_float_ratio_without_epsilon() -> None:
    numerator, denominator = (0.1).as_integer_ratio()
    useful = denominator - numerator
    policy = RecoveryPolicy(max_optional_overhead_fraction=0.1)
    accounting = RecoveryAccounting(useful_ns=useful)
    exact = candidate(replay_ns=useful, cost_ns=numerator)
    assert decide_checkpoint(policy, accounting, exact).admitted
    assert (
        decide_checkpoint(policy, accounting, replace(exact, cost_ns=numerator + 1)).reason
        == "candidate-too-expensive"
    )
