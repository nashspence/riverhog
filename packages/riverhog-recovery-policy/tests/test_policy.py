from __future__ import annotations

import math
from dataclasses import FrozenInstanceError, replace

import pytest
from riverhog_recovery_policy import (
    CheckpointCandidate,
    CheckpointDecision,
    RecoveryAccounting,
    RecoveryPolicy,
    decide_checkpoint,
    net_saved_replay_seconds,
)


def candidate(**changes: object) -> CheckpointCandidate:
    return replace(
        CheckpointCandidate(saved_replay_seconds=120, checkpoint_cost_seconds=1),
        **changes,
    )


def test_defaults_and_immutable_configuration() -> None:
    policy = RecoveryPolicy()
    assert policy.recovery_mode == "adaptive"
    assert policy.replay_target_seconds == 120
    assert policy.max_optional_overhead_fraction == 1 / 8
    assert policy.max_optional_checkpoints is None
    assert policy.retained_checkpoint_generations == 2
    with pytest.raises(FrozenInstanceError):
        policy.recovery_mode = "restart"  # type: ignore[misc]


@pytest.mark.parametrize(
    ("saved", "admitted"),
    [
        (math.nextafter(120.0, -math.inf), False),
        (120.0, True),
        (math.nextafter(120.0, math.inf), True),
    ],
)
def test_replay_threshold_uses_completed_net_work(saved: float, admitted: bool) -> None:
    result = decide_checkpoint(
        RecoveryPolicy(),
        RecoveryAccounting(useful_elapsed_seconds=120),
        candidate(saved_replay_seconds=saved),
    )
    assert result.admitted is admitted
    assert result.reserved_seconds == (1 if admitted else 0)


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"completion_ready": True}, "completion-ready"),
        ({"mandatory_anchor_ready": True}, "mandatory-anchor-ready"),
        ({"consistent": False}, "inconsistent-candidate"),
        ({"storage_admitted": False}, "storage-not-admitted"),
        ({"checkpoint_in_flight": True}, "checkpoint-in-flight"),
        ({"saved_replay_seconds": None}, "unknown-saved-replay"),
        ({"checkpoint_cost_seconds": None}, "unknown-checkpoint-cost"),
        ({"remaining_useful_seconds": 1}, "near-completion"),
        ({"remaining_useful_seconds": 0.5}, "near-completion"),
    ],
)
def test_suppression_preserves_required_work_and_resources(
    changes: dict[str, object], reason: str
) -> None:
    result = decide_checkpoint(
        RecoveryPolicy(),
        RecoveryAccounting(useful_elapsed_seconds=120),
        candidate(**changes),
    )
    assert result.reason == reason
    assert not result.admitted
    assert result.reserved_seconds == 0


def test_completion_precedes_unknown_cost_and_unavailable_optional_resources() -> None:
    result = decide_checkpoint(
        RecoveryPolicy(),
        RecoveryAccounting(reconciled=False),
        candidate(
            completion_ready=True,
            saved_replay_seconds=None,
            checkpoint_cost_seconds=None,
            consistent=False,
            storage_admitted=False,
        ),
    )
    assert result.reason == "completion-ready"


def test_restart_disables_only_optional_admission() -> None:
    policy = RecoveryPolicy(recovery_mode="restart")
    accounting = RecoveryAccounting(useful_elapsed_seconds=1000)
    assert decide_checkpoint(policy, accounting, candidate()).reason == "restart-mode"
    assert (
        decide_checkpoint(policy, accounting, candidate(mandatory_anchor_ready=True)).reason
        == "mandatory-anchor-ready"
    )


@pytest.mark.parametrize(("saved", "admitted"), [(209.0, False), (210.0, True), (211.0, True)])
def test_expensive_candidate_needs_saved_work_as_well_as_workflow_credit(
    saved: float, admitted: bool
) -> None:
    decision = decide_checkpoint(
        RecoveryPolicy(),
        RecoveryAccounting(useful_elapsed_seconds=1000),
        candidate(saved_replay_seconds=saved, checkpoint_cost_seconds=30),
    )
    assert decision.admitted is admitted
    if not admitted:
        assert decision.reason == "candidate-too-expensive"


@pytest.mark.parametrize(("credit", "admitted"), [(119.0, False), (126.0, True), (127.0, True)])
def test_failed_cost_and_held_reservations_spend_the_same_workflow_budget(
    credit: float, admitted: bool
) -> None:
    decision = decide_checkpoint(
        RecoveryPolicy(),
        RecoveryAccounting(
            useful_elapsed_seconds=credit, charged_optional_seconds=8, reserved_optional_seconds=9
        ),
        candidate(),
    )
    assert decision.admitted is admitted
    if not admitted:
        assert decision.reason == "workflow-budget-exhausted"


def test_unknown_accounting_cannot_fabricate_restart_credit() -> None:
    result = decide_checkpoint(
        RecoveryPolicy(),
        RecoveryAccounting(useful_elapsed_seconds=1000, reconciled=False),
        candidate(),
    )
    assert result.reason == "unreconciled-accounting"


def test_restore_cost_reduces_saved_replay_and_can_make_a_candidate_unhelpful() -> None:
    assert (
        net_saved_replay_seconds(
            restart_from_frontier_seconds=150, restart_from_candidate_seconds=31
        )
        == 119
    )
    assert (
        net_saved_replay_seconds(
            restart_from_frontier_seconds=20, restart_from_candidate_seconds=30
        )
        == -10
    )
    result = decide_checkpoint(
        RecoveryPolicy(),
        RecoveryAccounting(useful_elapsed_seconds=1000),
        candidate(saved_replay_seconds=119),
    )
    assert result.reason == "below-replay-target"


def test_zero_fraction_refuses_positive_cost_but_unknown_does_not_mean_zero() -> None:
    policy = RecoveryPolicy(max_optional_overhead_fraction=0)
    accounting = RecoveryAccounting(useful_elapsed_seconds=1000)
    assert decide_checkpoint(policy, accounting, candidate()).reason == "candidate-too-expensive"
    assert decide_checkpoint(policy, accounting, candidate(checkpoint_cost_seconds=0)).admitted
    assert (
        decide_checkpoint(policy, accounting, candidate(checkpoint_cost_seconds=None)).reason
        == "unknown-checkpoint-cost"
    )


def test_zero_count_limit_suppresses_even_zero_cost_candidates() -> None:
    decision = decide_checkpoint(
        RecoveryPolicy(max_optional_checkpoints=0),
        RecoveryAccounting(useful_elapsed_seconds=1000),
        candidate(checkpoint_cost_seconds=0),
    )
    assert decision.reason == "count-limit"


def test_count_is_distinct_from_generation_retention_and_failed_cost() -> None:
    policy = RecoveryPolicy(max_optional_checkpoints=3, retained_checkpoint_generations=1)
    accounting = RecoveryAccounting(
        useful_elapsed_seconds=1000, charged_optional_seconds=5, successful_checkpoints=2
    )
    assert decide_checkpoint(policy, accounting, candidate()).admitted
    assert (
        decide_checkpoint(policy, replace(accounting, successful_checkpoints=3), candidate()).reason
        == "count-limit"
    )


def test_finite_extreme_estimates_cannot_overflow_into_budget_credit() -> None:
    result = decide_checkpoint(
        RecoveryPolicy(),
        RecoveryAccounting(
            useful_elapsed_seconds=1e308,
            charged_optional_seconds=1e308,
            reserved_optional_seconds=1e308,
        ),
        candidate(saved_replay_seconds=1e308, checkpoint_cost_seconds=1e307),
    )
    assert result.reason == "workflow-budget-exhausted"


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
        RecoveryPolicy(retained_checkpoint_generations=value)  # type: ignore[arg-type]


@pytest.mark.parametrize("value", [True, float("nan"), float("inf"), -1])
def test_cost_and_accounting_values_require_finite_nonnegative_numbers(value: object) -> None:
    with pytest.raises(ValueError):
        candidate(checkpoint_cost_seconds=value)
    with pytest.raises(ValueError):
        RecoveryAccounting(charged_optional_seconds=value)  # type: ignore[arg-type]


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
    assert configured.recovery_mode == "restart"
    assert configured.max_optional_checkpoints == 0


@pytest.mark.parametrize("value", [{"unknown": 1}, {1: "adaptive"}, {"recovery_mode": True}])
def test_operator_mapping_rejects_unknown_or_coerced_values(value) -> None:
    with pytest.raises(ValueError):
        RecoveryPolicy.from_mapping(value)
