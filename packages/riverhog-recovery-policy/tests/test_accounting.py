from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier

import pytest
from riverhog_recovery_policy import (
    CheckpointCandidate,
    CheckpointReservation,
    RecoveryAccounting,
    RecoveryBudget,
    RecoveryPolicy,
)


def candidate(cost: float = 1) -> CheckpointCandidate:
    return CheckpointCandidate(saved_replay_seconds=120, checkpoint_cost_seconds=cost)


def funded(*, seconds: float = 120, limit: int = 4, **settings: object) -> RecoveryBudget:
    root = RecoveryBudget(RecoveryPolicy(**settings), active_checkpoint_limit=limit)
    root.record_completed_interval(0, seconds)
    return root


def test_nested_domains_share_one_percentage_and_cannot_credit_elapsed() -> None:
    root = funded(seconds=7)
    sdk, core, native = (root.domain() for _ in range(3))
    assert sdk.policy is core.policy is native.policy is root.policy
    _, original = sdk.reserve(candidate())
    assert original is not None
    for child in (core, native):
        decision, grant = child.reserve(candidate())
        assert decision.reason == "workflow-budget-exhausted"
        assert grant is None
        assert not hasattr(child, "record_completed_interval")
    assert root.pending_reservations == (original,)
    assert root.accounting().reserved_optional_seconds == 1


@pytest.mark.parametrize("intervals", [[(0, 10), (5, 15)], [(5, 15), (0, 10)]])
def test_parallel_completed_intervals_never_multiply_elapsed_credit(
    intervals: list[tuple[int, int]],
) -> None:
    root = RecoveryBudget(RecoveryPolicy(), active_checkpoint_limit=2)
    for start, finish in intervals:
        root.record_completed_interval(start, finish)
    credited = root.accounting().useful_elapsed_seconds
    assert credited <= 15
    assert credited >= 10
    for start, finish in intervals:
        root.record_completed_interval(start, finish)
    assert root.accounting().useful_elapsed_seconds == credited


def test_idle_gaps_replay_and_phase_changes_cannot_reset_credit() -> None:
    root = RecoveryBudget(RecoveryPolicy(), active_checkpoint_limit=2)
    root.record_completed_interval(0, 10)
    root.domain()
    root.record_completed_interval(10, 100, repeated_replay=True)
    root.domain()
    root.record_completed_interval(100, 110)
    root.record_completed_interval(200, 210)
    assert root.accounting().useful_elapsed_seconds == 30
    root.record_completed_interval(10, 100)
    assert root.accounting().useful_elapsed_seconds == 30


def test_parallel_reservations_are_atomic_at_the_same_root_budget() -> None:
    root = funded(seconds=7, limit=8)
    barrier = Barrier(8)

    def compete(_: int) -> CheckpointReservation | None:
        child = root.domain()
        barrier.wait(timeout=10)
        return child.reserve(candidate())[1]

    with ThreadPoolExecutor(max_workers=8) as pool:
        grants = list(pool.map(compete, range(8)))
    admitted = [grant for grant in grants if grant is not None]
    assert len(admitted) == 1
    assert root.pending_reservations == tuple(admitted)
    assert root.accounting().reserved_optional_seconds == 1


def test_count_limit_reserves_possible_success_without_reporting_it_as_published() -> None:
    root = funded(max_optional_checkpoints=1)
    first, second = root.domain(), root.domain()
    _, grant = first.reserve(candidate())
    assert grant is not None
    assert root.accounting().successful_checkpoints == 0
    decision, extra = second.reserve(candidate())
    assert decision.reason == "count-limit"
    assert extra is None
    grant.receive(actual_seconds=1, published=False)
    _, retry = second.reserve(candidate())
    assert retry is not None
    retry.receive(actual_seconds=1, published=True)
    assert root.accounting().successful_checkpoints == 1
    assert first.reserve(candidate())[0].reason == "count-limit"


def test_failed_candidate_cost_survives_retry_and_actual_overrun_creates_debt() -> None:
    root = funded(seconds=21)
    child = root.domain()
    _, grant = child.reserve(candidate(2))
    assert grant is not None
    charge = grant.receive(actual_seconds=5, published=False)
    assert charge.overrun
    assert charge.reserved_seconds == 2 and charge.actual_seconds == 5
    summary = root.accounting()
    assert summary.charged_optional_seconds == 5
    assert summary.reserved_optional_seconds == 0
    assert summary.successful_checkpoints == 0
    assert child.reserve(candidate())[0].reason == "workflow-budget-exhausted"
    root.record_completed_interval(21, 42)
    decision, retry = child.reserve(candidate())
    assert decision.admitted and retry is not None
    retry.receive(actual_seconds=0.5, published=True)
    assert root.accounting().charged_optional_seconds == 5.5


def test_changed_estimate_is_not_a_refund_for_previous_failed_work() -> None:
    root = funded(seconds=28)
    child = root.domain()
    _, grant = child.reserve(candidate(3))
    assert grant is not None
    grant.receive(actual_seconds=3, published=False)
    assert child.reserve(candidate(2))[0].reason == "workflow-budget-exhausted"
    _, retry = child.reserve(candidate(1))
    assert retry is not None
    assert root.accounting().reserved_optional_seconds == 1
    assert root.accounting().charged_optional_seconds == 3


def test_unreceived_original_stays_charged_and_available_for_receiving() -> None:
    root = funded()
    child = root.domain()
    _, original = child.reserve(candidate())
    assert original is not None
    assert child.reserve(candidate())[0].reason == "checkpoint-in-flight"
    assert root.pending_reservations == (original,)
    assert child.pending_reservation is original
    assert not original.received
    with pytest.raises(ValueError):
        original.receive(actual_seconds=float("nan"), published=False)
    assert root.pending_reservations == (original,)
    original.receive(actual_seconds=1, published=True)
    assert original.received
    assert child.pending_reservation is None
    assert not root.pending_reservations
    with pytest.raises(RuntimeError):
        original.receive(actual_seconds=0, published=False)
    assert root.accounting().successful_checkpoints == 1


def test_an_unregistered_lookalike_cannot_receive_or_refund_an_original_grant() -> None:
    root = funded()
    child = root.domain()
    _, original = child.reserve(candidate())
    assert original is not None
    fabricated = CheckpointReservation(child, original.reserved_seconds)
    with pytest.raises(RuntimeError):
        fabricated.receive(actual_seconds=0, published=False)
    assert root.pending_reservations == (original,)
    assert root.accounting().reserved_optional_seconds == 1


def test_active_capacity_is_separate_from_total_lifetime_count() -> None:
    root = funded(limit=1)
    first, other = root.domain(), root.domain()
    _, original = first.reserve(candidate())
    assert original is not None
    assert other.reserve(candidate())[0].reason == "storage-not-admitted"
    original.receive(actual_seconds=1, published=True)
    _, second = other.reserve(candidate())
    assert second is not None
    second.receive(actual_seconds=1, published=True)
    assert root.accounting().successful_checkpoints == 2


@pytest.mark.parametrize("reserved", [0, 5])
def test_restored_summary_requires_reconciliation_even_without_known_reservation(
    reserved: float,
) -> None:
    saved = RecoveryAccounting(
        useful_elapsed_seconds=120,
        charged_optional_seconds=3,
        reserved_optional_seconds=reserved,
        successful_checkpoints=2,
    )
    root = RecoveryBudget(RecoveryPolicy(), active_checkpoint_limit=2, recovered=saved)
    child = root.domain()
    assert not root.accounting().reconciled
    assert root.accounting().reserved_optional_seconds == reserved
    assert child.reserve(candidate())[0].reason == "unreconciled-accounting"
    root.record_completed_interval(0, 100, repeated_replay=True)
    assert root.accounting().useful_elapsed_seconds == 120
    root.reconcile_recovered(actual_seconds=6, additional_published_checkpoints=1)
    summary = root.accounting()
    assert summary.reconciled
    assert summary.charged_optional_seconds == 9
    assert summary.successful_checkpoints == 3
    assert summary.reserved_optional_seconds == 0
    assert child.reserve(candidate())[0].admitted
    with pytest.raises(RuntimeError):
        root.reconcile_recovered(actual_seconds=0, additional_published_checkpoints=0)


def test_fractional_cost_rounding_cannot_manufacture_credit() -> None:
    root = funded(seconds=7, limit=4)
    for cost in (0.1, 0.2, 0.3):
        _, grant = root.domain().reserve(candidate(cost))
        assert grant is not None
        grant.receive(actual_seconds=cost, published=False)
    assert root.accounting().charged_optional_seconds >= 0.1 + 0.2 + 0.3
    assert root.domain().reserve(candidate(0.5))[0].reason == "workflow-budget-exhausted"


def test_finite_cost_saturation_suppresses_more_work_without_invalidating_archive() -> None:
    root = RecoveryBudget(
        RecoveryPolicy(max_optional_overhead_fraction=0.99), active_checkpoint_limit=2
    )
    root.record_completed_interval(0, 1e308)
    _, grant = root.domain().reserve(
        CheckpointCandidate(saved_replay_seconds=1e308, checkpoint_cost_seconds=1e308)
    )
    assert grant is not None
    grant.receive(actual_seconds=1e308, published=False)
    _, grant = root.domain().reserve(
        CheckpointCandidate(saved_replay_seconds=1e308, checkpoint_cost_seconds=1e308)
    )
    assert grant is not None
    grant.receive(actual_seconds=1e308, published=False)
    assert not root.accounting().reconciled
    assert root.domain().reserve(candidate())[0].reason == "unreconciled-accounting"


@pytest.mark.parametrize("value", [True, -1, float("inf"), float("nan")])
def test_invalid_measurements_never_release_original_reservation(value: object) -> None:
    root = funded()
    _, grant = root.domain().reserve(candidate())
    assert grant is not None
    with pytest.raises(ValueError):
        grant.receive(actual_seconds=value, published=False)  # type: ignore[arg-type]
    assert root.pending_reservations == (grant,)
    assert root.accounting().reserved_optional_seconds == 1


@pytest.mark.parametrize(
    "policy", [RecoveryPolicy(recovery_mode="restart"), RecoveryPolicy(max_optional_checkpoints=0)]
)
def test_disabled_optional_work_never_allocates_a_reservation(policy: RecoveryPolicy) -> None:
    root = RecoveryBudget(policy, active_checkpoint_limit=1)
    root.record_completed_interval(0, 1000)
    assert root.domain().reserve(candidate())[1] is None
    assert root.accounting() == replace(RecoveryAccounting(), useful_elapsed_seconds=1000)
    assert root.pending_reservations == ()


def test_ready_required_work_does_not_wait_for_or_refund_optional_receiving() -> None:
    root = funded()
    child = root.domain()
    _, original = child.reserve(candidate())
    assert original is not None
    decision, grant = child.reserve(replace(candidate(), completion_ready=True))
    assert decision.reason == "completion-ready" and grant is None
    assert root.pending_reservations == (original,)
    assert not original.received
    assert root.accounting().reserved_optional_seconds == 1
