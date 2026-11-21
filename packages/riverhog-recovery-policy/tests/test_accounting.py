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

NS = 1_000_000_000


def candidate(cost_ns: int = NS) -> CheckpointCandidate:
    return CheckpointCandidate(
        replay_ns=120 * NS,
        restore_ns=0,
        cost_ns=cost_ns,
        remaining_ns=None,
        replay_provenance="completed useful work",
        restore_provenance="admitted restore profile",
        cost_provenance="complete lifecycle estimate",
        remaining_provenance="unknown input extent",
    )


def funded(*, useful_ns: int = 120 * NS, limit: int = 4, **settings: object) -> RecoveryBudget:
    root = RecoveryBudget(RecoveryPolicy(**settings), active_checkpoint_limit=limit)
    root.record_completed_interval(0, useful_ns)
    return root


def test_nested_domains_share_one_percentage_and_cannot_credit_elapsed() -> None:
    root = funded(useful_ns=7 * NS)
    sdk, core, native = (root.domain() for _ in range(3))
    assert sdk.policy is core.policy is native.policy is root.policy
    _, original = sdk.reserve(candidate())
    assert original is not None
    for child in (core, native):
        decision, grant = child.reserve(candidate())
        assert decision.reason == "workflow-budget-exhausted" and grant is None
        assert not hasattr(child, "record_completed_interval")
    assert root.pending_reservations == (original,)
    assert root.accounting().reserved_ns == NS
    assert root.accounting().held_count_slots == 1


@pytest.mark.parametrize("intervals", [[(0, 10), (5, 15)], [(5, 15), (0, 10)]])
def test_parallel_completed_intervals_never_multiply_elapsed_credit(
    intervals: list[tuple[int, int]],
) -> None:
    root = RecoveryBudget(RecoveryPolicy(), active_checkpoint_limit=2)
    for start, finish in intervals:
        root.record_completed_interval(start, finish)
    credited = root.accounting().useful_ns
    assert 10 <= credited <= 15
    for start, finish in intervals:
        root.record_completed_interval(start, finish)
    assert root.accounting().useful_ns == credited


def test_idle_gaps_replay_and_phase_changes_cannot_reset_credit() -> None:
    root = RecoveryBudget(RecoveryPolicy(), active_checkpoint_limit=2)
    root.record_completed_interval(0, 10)
    root.domain()
    root.record_completed_interval(10, 100, repeated_replay=True)
    root.domain()
    root.record_completed_interval(100, 110)
    root.record_completed_interval(200, 210)
    assert root.accounting().useful_ns == 30
    root.record_completed_interval(10, 100)
    assert root.accounting().useful_ns == 30


def test_parallel_reservations_are_atomic_at_the_same_root_budget() -> None:
    root = funded(useful_ns=7 * NS, limit=8)
    barrier = Barrier(8)

    def compete(_: int) -> CheckpointReservation | None:
        child = root.domain()
        barrier.wait(timeout=10)
        return child.reserve(candidate())[1]

    with ThreadPoolExecutor(max_workers=8) as pool:
        grants = list(pool.map(compete, range(8)))
    admitted = [grant for grant in grants if grant is not None]
    assert len(admitted) == 1 and root.pending_reservations == tuple(admitted)
    assert root.accounting().reserved_ns == NS and root.accounting().held_count_slots == 1


def test_count_limit_reserves_possible_success_without_reporting_it_as_published() -> None:
    root = funded(max_optional_checkpoints=1)
    first, second = root.domain(), root.domain()
    _, grant = first.reserve(candidate())
    assert grant is not None
    assert root.accounting().successful_checkpoints == 0
    assert root.accounting().held_count_slots == 1
    decision, extra = second.reserve(candidate())
    assert decision.reason == "count-limit" and extra is None
    grant.receive(total_actual_ns=NS, nonpublication_proven=True)
    assert root.accounting().held_count_slots == 0
    _, retry = second.reserve(candidate())
    assert retry is not None
    retry.select_generation("generation")
    retry.receive(total_actual_ns=NS)
    assert root.accounting().successful_checkpoints == 1
    assert first.reserve(candidate())[0].reason == "count-limit"


def test_failed_candidate_cost_survives_retry_and_actual_overrun_creates_debt() -> None:
    root = funded(useful_ns=21 * NS)
    child = root.domain()
    _, grant = child.reserve(candidate(2 * NS))
    assert grant is not None
    charge = grant.receive(total_actual_ns=5 * NS, nonpublication_proven=True)
    assert charge.overrun and charge.reserved_ns == 2 * NS and charge.actual_ns == 5 * NS
    summary = root.accounting()
    assert summary.charged_ns == 5 * NS and summary.reserved_ns == 0
    assert summary.successful_checkpoints == 0
    assert child.reserve(candidate())[0].reason == "overrun-debt"
    root.record_completed_interval(21 * NS, 42 * NS)
    decision, retry = child.reserve(candidate())
    assert decision.admitted and retry is not None
    retry.select_generation("generation")
    retry.receive(total_actual_ns=NS // 2)
    assert root.accounting().charged_ns == 5 * NS + NS // 2


def test_changed_estimate_is_not_a_refund_for_previous_failed_work() -> None:
    root = funded(useful_ns=28 * NS)
    child = root.domain()
    _, grant = child.reserve(candidate(3 * NS))
    assert grant is not None
    grant.receive(total_actual_ns=3 * NS, nonpublication_proven=True)
    assert child.reserve(candidate(2 * NS))[0].reason == "workflow-budget-exhausted"
    _, retry = child.reserve(candidate(NS))
    assert retry is not None
    assert root.accounting().reserved_ns == NS and root.accounting().charged_ns == 3 * NS


def test_unreceived_original_stays_charged_and_available_for_receiving() -> None:
    root = funded()
    child = root.domain()
    _, original = child.reserve(candidate())
    assert original is not None and child.reserve(candidate())[0].reason == "checkpoint-in-flight"
    assert root.pending_reservations == (original,) and child.pending_reservation is original
    assert not original.received
    with pytest.raises(ValueError):
        original.receive(total_actual_ns=float("nan"), nonpublication_proven=True)  # type: ignore[arg-type]
    assert root.pending_reservations == (original,)
    original.select_generation("generation")
    original.receive(total_actual_ns=NS)
    assert original.received and child.pending_reservation is None and not root.pending_reservations
    with pytest.raises(ValueError):
        original.receive(total_actual_ns=0, nonpublication_proven=True)
    assert root.accounting().successful_checkpoints == 1


def test_an_unregistered_lookalike_cannot_receive_or_refund_an_original_grant() -> None:
    root = funded()
    child = root.domain()
    _, original = child.reserve(candidate())
    assert original is not None
    fabricated = CheckpointReservation(child, original.reserved_ns)
    with pytest.raises(RuntimeError):
        fabricated.receive(total_actual_ns=0, nonpublication_proven=True)
    assert root.pending_reservations == (original,) and root.accounting().reserved_ns == NS


def test_active_capacity_is_separate_from_total_lifetime_count() -> None:
    root = funded(limit=1)
    first, other = root.domain(), root.domain()
    _, original = first.reserve(candidate())
    assert original is not None
    assert other.reserve(candidate())[0].reason == "storage-not-admitted"
    original.select_generation("generation")
    original.receive(total_actual_ns=NS)
    _, second = other.reserve(candidate())
    assert second is not None
    second.select_generation("generation")
    second.receive(total_actual_ns=NS)
    assert root.accounting().successful_checkpoints == 2


@pytest.mark.parametrize("reserved", [0, 5 * NS])
def test_restored_summary_requires_reconciliation_even_without_known_reservation(
    reserved: int,
) -> None:
    saved = RecoveryAccounting(
        useful_ns=120 * NS,
        charged_ns=3 * NS,
        reserved_ns=reserved,
        successful_checkpoints=2,
        held_count_slots=int(reserved != 0),
    )
    root = RecoveryBudget(RecoveryPolicy(), active_checkpoint_limit=2, recovered=saved)
    child = root.domain()
    assert not root.accounting().reconciled and root.accounting().reserved_ns == reserved
    assert root.accounting().held_count_slots == int(reserved != 0)
    assert child.reserve(candidate())[0].reason == "unreconciled-accounting"
    root.record_completed_interval(0, 100, repeated_replay=True)
    assert root.accounting().useful_ns == 120 * NS
    root.reconcile_recovered(actual_ns=6 * NS, additional_published_checkpoints=1)
    summary = root.accounting()
    assert summary.reconciled and summary.charged_ns == 9 * NS
    assert (
        summary.successful_checkpoints == 3 and summary.reserved_ns == summary.held_count_slots == 0
    )
    assert child.reserve(candidate())[0].admitted
    with pytest.raises(RuntimeError):
        root.reconcile_recovered(actual_ns=0, additional_published_checkpoints=0)


def test_subsecond_costs_are_not_rounded_into_new_credit() -> None:
    root = funded(useful_ns=7 * NS, limit=4)
    for cost in (NS // 10, NS // 5, 3 * NS // 10):
        _, grant = root.domain().reserve(candidate(cost))
        assert grant is not None
        grant.receive(total_actual_ns=cost, nonpublication_proven=True)
    assert root.accounting().charged_ns == 3 * NS // 5
    assert root.domain().reserve(candidate(NS // 2))[0].reason == "workflow-budget-exhausted"


def test_lifetime_costs_remain_exact_beyond_float_and_fixed_width_ranges() -> None:
    huge = 10**400
    root = RecoveryBudget(
        RecoveryPolicy(max_optional_overhead_fraction=0.99), active_checkpoint_limit=2
    )
    root.record_completed_interval(0, huge)
    for _ in range(2):
        _, grant = root.domain().reserve(replace(candidate(huge), replay_ns=huge))
        assert grant is not None
        grant.receive(total_actual_ns=huge, nonpublication_proven=True)
    assert root.accounting().reconciled
    assert root.accounting().charged_ns == 2 * huge
    assert root.domain().reserve(candidate())[0].admitted


@pytest.mark.parametrize("value", [True, -1, 1.0, float("inf"), float("nan")])
def test_invalid_measurements_never_release_original_reservation(value: object) -> None:
    root = funded()
    _, grant = root.domain().reserve(candidate())
    assert grant is not None
    with pytest.raises(ValueError):
        grant.receive(total_actual_ns=value, nonpublication_proven=True)  # type: ignore[arg-type]
    assert root.pending_reservations == (grant,) and root.accounting().reserved_ns == NS


@pytest.mark.parametrize(
    "policy", [RecoveryPolicy(recovery_mode="restart"), RecoveryPolicy(max_optional_checkpoints=0)]
)
def test_disabled_optional_work_never_allocates_a_reservation(policy: RecoveryPolicy) -> None:
    root = RecoveryBudget(policy, active_checkpoint_limit=1)
    root.record_completed_interval(0, 1000)
    assert root.domain().reserve(candidate())[1] is None
    assert root.accounting() == replace(RecoveryAccounting(), useful_ns=1000)
    assert root.pending_reservations == ()


def test_ready_required_work_does_not_wait_for_or_refund_optional_receiving() -> None:
    root = funded()
    child = root.domain()
    _, original = child.reserve(candidate())
    assert original is not None
    decision, grant = child.reserve(replace(candidate(), completion_ready=True))
    assert decision.reason == "completion-ready" and grant is None
    assert root.pending_reservations == (original,) and not original.received
    assert root.accounting().reserved_ns == NS


@pytest.mark.parametrize("value", [True, 1.0, -1])
def test_invalid_interval_cannot_advance_watermark_or_credit(value: object) -> None:
    root = funded()
    before = root.accounting()
    with pytest.raises(ValueError):
        root.record_completed_interval(value, 130 * NS)  # type: ignore[arg-type]
    assert root.accounting() == before


def test_selected_generation_counts_before_its_original_cleanup_is_received() -> None:
    root = funded(useful_ns=140 * NS, max_optional_checkpoints=2)
    domain = root.domain()
    _, original = domain.reserve(candidate(10 * NS))
    assert original is not None
    original.select_generation("generation-one")
    original.record_cost(total_actual_ns=8 * NS, remaining_reserved_ns=2 * NS)
    summary = root.accounting()
    assert summary.successful_checkpoints == 1 and summary.held_count_slots == 0
    assert summary.charged_ns == 8 * NS and summary.reserved_ns == 2 * NS
    assert root.pending_reservations == (original,) and not original.received
    _, second = root.domain().reserve(candidate())
    assert second is not None
    assert root.accounting().held_count_slots == 1
    assert root.domain().reserve(candidate())[0].reason == "count-limit"
    original.receive(total_actual_ns=11 * NS)
    assert original.received
    assert root.accounting().charged_ns == 11 * NS
    assert root.accounting().reserved_ns == NS
    assert root.accounting().successful_checkpoints == 1


def test_selected_original_retries_do_not_duplicate_counts_costs_or_release_tail() -> None:
    root = funded(useful_ns=140 * NS)
    domain = root.domain()
    _, original = domain.reserve(candidate(10 * NS))
    assert original is not None
    original.select_generation("generation-one")
    original.record_cost(total_actual_ns=8 * NS, remaining_reserved_ns=2 * NS)
    before = root.accounting()
    original.select_generation("generation-one")
    original.record_cost(total_actual_ns=8 * NS, remaining_reserved_ns=2 * NS)
    assert root.accounting() == before
    with pytest.raises(ValueError):
        original.select_generation("another-generation")
    assert root.accounting() == before
    assert domain.reserve(candidate())[0].reason == "checkpoint-in-flight"
    assert not original.received
    charge = original.receive(total_actual_ns=11 * NS)
    after = root.accounting()
    assert charge.overrun
    assert original.receive(total_actual_ns=11 * NS) is charge
    assert root.accounting() == after
    assert root.pending_reservations == ()
    assert after.successful_checkpoints == 1 and after.held_count_slots == 0


@pytest.mark.parametrize("selected", [False, True])
def test_original_cost_cannot_be_erased_and_publication_cannot_be_retracted(selected: bool) -> None:
    root = funded()
    _, original = root.domain().reserve(candidate(10 * NS))
    assert original is not None
    if selected:
        original.select_generation("generation-one")
    original.record_cost(total_actual_ns=8 * NS, remaining_reserved_ns=2 * NS)
    before = root.accounting()
    with pytest.raises(ValueError):
        original.record_cost(total_actual_ns=7 * NS, remaining_reserved_ns=0)
    with pytest.raises(ValueError):
        original.receive(total_actual_ns=7 * NS, nonpublication_proven=not selected)
    with pytest.raises(ValueError):
        original.receive(total_actual_ns=8 * NS, nonpublication_proven=selected)
    assert root.accounting() == before and root.pending_reservations == (original,)


def test_failed_candidate_needs_known_nonpublication_and_keeps_actual_cost() -> None:
    root = funded()
    _, original = root.domain().reserve(candidate(10 * NS))
    assert original is not None
    original.record_cost(total_actual_ns=8 * NS, remaining_reserved_ns=2 * NS)
    before = root.accounting()
    with pytest.raises(ValueError):
        original.receive(total_actual_ns=8 * NS)
    assert root.accounting() == before
    original.receive(total_actual_ns=9 * NS, nonpublication_proven=True)
    summary = root.accounting()
    assert summary.charged_ns == 9 * NS
    assert summary.reserved_ns == summary.held_count_slots == summary.successful_checkpoints == 0


def test_partial_lifecycle_overrun_creates_debt_without_counting_an_unselected_candidate() -> None:
    root = funded(useful_ns=70 * NS)
    _, original = root.domain().reserve(candidate(10 * NS))
    assert original is not None
    original.record_cost(total_actual_ns=8 * NS, remaining_reserved_ns=5 * NS)
    summary = root.accounting()
    assert summary.charged_ns + summary.reserved_ns == 13 * NS
    assert summary.successful_checkpoints == 0 and summary.held_count_slots == 1
    assert root.domain().reserve(candidate(0))[0].reason == "overrun-debt"
    original.receive(total_actual_ns=14 * NS, nonpublication_proven=True)
    assert root.accounting().charged_ns == 14 * NS and root.accounting().reserved_ns == 0


def test_competing_original_selection_and_final_receipts_are_idempotent() -> None:
    root = funded()
    _, original = root.domain().reserve(candidate())
    assert original is not None

    def select(_: int) -> None:
        original.select_generation("generation-one")

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(select, range(4)))
    assert root.accounting().successful_checkpoints == 1

    def receive(_: int):
        return original.receive(total_actual_ns=NS)

    with ThreadPoolExecutor(max_workers=4) as pool:
        charges = list(pool.map(receive, range(4)))
    assert all(charge is charges[0] for charge in charges)
    assert root.accounting().charged_ns == NS
    assert root.accounting().successful_checkpoints == 1
    assert root.accounting().held_count_slots == 0
