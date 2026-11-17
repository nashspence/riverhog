"""Causal policy examples for the proposed application-owned credit seam."""

from __future__ import annotations

import itertools

import pytest
from application_credit import CohortCredit, OriginalInterval, VerifiedWorkAmounts


def meter(capacity: int = 3) -> CohortCredit:
    return CohortCredit(epoch="original-owner", max_active=capacity, now_ns=0)


def test_parallel_children_and_local_work_cannot_multiply_elapsed() -> None:
    owner = meter()
    owner.local_state(fresh_useful=True, optional=False, now_ns=0)
    first = owner.start_remote(now_ns=10)
    second = owner.start_remote(now_ns=20)
    assert owner.credit_ns == 10
    owner.receive(first, VerifiedWorkAmounts(70, 0), now_ns=90)
    assert owner.credit_ns == 80
    owner.receive(second, VerifiedWorkAmounts(60, 0), now_ns=100)
    assert owner.credit_ns == 80
    owner.local_state(fresh_useful=False, optional=False, now_ns=110)
    assert owner.credit_ns == 90


@pytest.mark.parametrize("order", tuple(itertools.permutations(range(3))))
def test_receiving_order_does_not_change_credit(order: tuple[int, ...]) -> None:
    owner = meter()
    intervals = [owner.start_remote(now_ns=0) for _ in range(3)]
    owner.optional_phase("original-save", starting=True, now_ns=90)
    amounts = (VerifiedWorkAmounts(80, 3), VerifiedWorkAmounts(60, 4), VerifiedWorkAmounts(20, 5))
    for index in order:
        owner.receive(intervals[index], amounts[index], now_ns=100)
    owner.optional_phase("original-save", starting=False, now_ns=100)
    assert owner.credit_ns == 58  # 80 minus reported 12 and conservative gate interval 10


def test_continuously_overlapping_children_do_not_reset_cohort() -> None:
    owner = meter(2)
    long = owner.start_remote(now_ns=0)
    short = owner.start_remote(now_ns=1)
    owner.receive(short, VerifiedWorkAmounts(20, 0), now_ns=30)
    later = owner.start_remote(now_ns=31)
    owner.receive(later, VerifiedWorkAmounts(30, 0), now_ns=70)
    assert owner.credit_ns == 30
    owner.receive(long, VerifiedWorkAmounts(40, 0), now_ns=100)
    assert owner.credit_ns == 40


def test_root_optional_overlap_is_removed_even_when_remote_child_was_useful() -> None:
    owner = meter()
    child = owner.start_remote(now_ns=0)
    owner.local_state(fresh_useful=True, optional=True, now_ns=20)
    owner.local_state(fresh_useful=True, optional=False, now_ns=50)
    owner.optional_phase("child-save", starting=True, now_ns=90)
    owner.receive(child, VerifiedWorkAmounts(80, 10), now_ns=100)
    owner.optional_phase("child-save", starting=False, now_ns=100)
    assert owner.credit_ns == 30


def test_unknown_receipt_zeroes_whole_cohort_without_preventing_receiving() -> None:
    owner = meter()
    first = owner.start_remote(now_ns=0)
    second = owner.start_remote(now_ns=0)
    owner.receive(second, None, now_ns=100)
    owner.receive(first, VerifiedWorkAmounts(100, 0), now_ns=101)
    assert owner.credit_ns == 0 and owner.active_count == 0


def test_root_elapsed_caps_remote_clock_and_optional_debt_floors_at_zero() -> None:
    owner = meter()
    first = owner.start_remote(now_ns=0)
    owner.optional_phase("first-save", starting=True, now_ns=90)
    owner.receive(first, VerifiedWorkAmounts(1000, 20), now_ns=100)
    owner.optional_phase("first-save", starting=False, now_ns=100)
    assert owner.credit_ns == 70
    second = owner.start_remote(now_ns=100)
    owner.optional_phase("next-save", starting=True, now_ns=100)
    owner.receive(second, VerifiedWorkAmounts(1000, 1000), now_ns=200)
    owner.optional_phase("next-save", starting=False, now_ns=200)
    assert owner.credit_ns == 70


def test_duplicate_and_old_epoch_receipts_never_earn_new_credit() -> None:
    owner = meter()
    child = owner.start_remote(now_ns=0)
    owner.receive(child, VerifiedWorkAmounts(80, 0), now_ns=100)
    assert not owner.receive(child, VerifiedWorkAmounts(80, 0), now_ns=200)
    resumed = CohortCredit(epoch="next-owner", max_active=2, now_ns=1000, durable_credit_ns=50)
    assert not resumed.receive(child, VerifiedWorkAmounts(80, 0), now_ns=2000)
    assert resumed.credit_ns == 50


def test_capacity_is_concurrent_and_does_not_limit_workflow_length() -> None:
    owner = meter(1)
    for index in range(100):
        child = owner.start_remote(now_ns=2 * index)
        with pytest.raises(RuntimeError, match="admission"):
            owner.start_remote(now_ns=2 * index)
        owner.receive(child, VerifiedWorkAmounts(1, 0), now_ns=2 * index + 1)
        assert owner.active_count == 0
    assert owner.credit_ns == 100


def test_nonuseful_wait_and_optional_work_create_no_credit() -> None:
    owner = meter()
    owner.local_state(fresh_useful=False, optional=False, now_ns=100)
    owner.local_state(fresh_useful=True, optional=True, now_ns=200)
    owner.local_state(fresh_useful=False, optional=False, now_ns=1000)
    assert owner.credit_ns == 0


@pytest.mark.parametrize("bad", [-1, True, 1.5])
def test_malformed_amounts_are_rejected(bad: int) -> None:
    with pytest.raises(ValueError):
        VerifiedWorkAmounts(bad, 0)
    with pytest.raises(ValueError):
        VerifiedWorkAmounts(0, bad)


def test_wrong_sequence_and_backward_clock_do_not_receive_original() -> None:
    owner = meter()
    child = owner.start_remote(now_ns=10)
    with pytest.raises(ValueError, match="never issued"):
        owner.receive(OriginalInterval(owner.epoch, 2), VerifiedWorkAmounts(0, 0), now_ns=20)
    with pytest.raises(ValueError, match="backward"):
        owner.receive(child, VerifiedWorkAmounts(0, 0), now_ns=5)
    assert owner.active_count == 1 and owner.credit_ns == 0


def test_first_long_remote_domain_can_reach_target_before_terminal_receiving() -> None:
    second = 1_000_000_000
    owner = meter()
    child = owner.start_remote(now_ns=0)
    owner.prefix(
        child, VerifiedWorkAmounts(120 * second, 0), prefix_sequence=1, now_ns=125 * second
    )
    assert owner.credit_ns == 120 * second and owner.active_count == 1
    # The same in-flight owner can now request a real #995 grant. This arithmetic
    # alone neither admits that grant nor marks the operation/result received.
    owner.receive(child, VerifiedWorkAmounts(180 * second, 0), now_ns=200 * second)
    assert owner.credit_ns == 180 * second and owner.active_count == 0


def test_prefixes_deduplicate_and_conflicting_or_regressing_receipts_fail() -> None:
    owner = meter()
    child = owner.start_remote(now_ns=0)
    owner.prefix(child, VerifiedWorkAmounts(50, 0), prefix_sequence=1, now_ns=60)
    assert not owner.prefix(child, VerifiedWorkAmounts(50, 0), prefix_sequence=1, now_ns=70)
    with pytest.raises(ValueError, match="conflicting"):
        owner.prefix(child, VerifiedWorkAmounts(60, 0), prefix_sequence=1, now_ns=70)
    with pytest.raises(ValueError, match="backward"):
        owner.prefix(child, VerifiedWorkAmounts(49, 0), prefix_sequence=2, now_ns=70)
    assert owner.credit_ns == 50 and owner.active_count == 1


def test_new_credit_waits_for_known_optional_costs_then_can_promote_again() -> None:
    owner = meter()
    child = owner.start_remote(now_ns=0)
    owner.prefix(child, VerifiedWorkAmounts(120, 0), prefix_sequence=1, now_ns=130)
    owner.optional_phase("generation1", starting=True, now_ns=130)
    owner.prefix(child, VerifiedWorkAmounts(180, 20), prefix_sequence=2, now_ns=210)
    assert owner.credit_ns == 120
    owner.optional_phase("generation1", starting=False, now_ns=210)
    assert owner.credit_ns == 120
    owner.prefix(child, VerifiedWorkAmounts(300, 20), prefix_sequence=3, now_ns=340)
    assert owner.credit_ns == 200 and owner.active_count == 1


def test_optional_execution_cannot_be_backfilled_after_unfrozen_credit() -> None:
    owner = meter()
    child = owner.start_remote(now_ns=0)
    with pytest.raises(ValueError, match="not announced"):
        owner.prefix(child, VerifiedWorkAmounts(10, 1), prefix_sequence=1, now_ns=20)
    assert owner.credit_ns == 0 and owner.active_count == 1


def test_unknown_later_receipt_freezes_new_credit_and_preserves_known_prefix() -> None:
    owner = meter()
    first = owner.start_remote(now_ns=0)
    second = owner.start_remote(now_ns=0)
    owner.prefix(first, VerifiedWorkAmounts(50, 0), prefix_sequence=1, now_ns=60)
    owner.receive(second, None, now_ns=70)
    owner.receive(first, VerifiedWorkAmounts(100, 0), now_ns=120)
    assert owner.credit_ns == 50 and owner.active_count == 0


def test_late_overlapping_child_cannot_recredit_an_earlier_reported_prefix() -> None:
    owner = meter()
    first = owner.start_remote(now_ns=0)
    second = owner.start_remote(now_ns=0)
    owner.prefix(first, VerifiedWorkAmounts(100, 0), prefix_sequence=1, now_ns=110)
    owner.prefix(second, VerifiedWorkAmounts(100, 0), prefix_sequence=1, now_ns=210)
    assert owner.credit_ns == 100


def test_poll_time_cannot_unclip_an_earlier_remote_clock_excess() -> None:
    owner = meter()
    child = owner.start_remote(now_ns=0)
    owner.prefix(child, VerifiedWorkAmounts(1000, 0), prefix_sequence=1, now_ns=100)
    assert owner.credit_ns == 100
    owner.local_state(fresh_useful=False, optional=False, now_ns=2000)
    owner.prefix(child, VerifiedWorkAmounts(1000, 0), prefix_sequence=2, now_ns=3000)
    assert owner.credit_ns == 100


def test_late_cleanup_of_received_child_debits_overlapping_new_child() -> None:
    owner = meter()
    original = owner.start_remote(now_ns=0)
    owner.receive(original, VerifiedWorkAmounts(0, 0), now_ns=10)
    owner.optional_phase("original-generation-cleanup", starting=True, now_ns=20)
    next_child = owner.start_remote(now_ns=30)
    owner.prefix(next_child, VerifiedWorkAmounts(10, 0), prefix_sequence=1, now_ns=40)
    assert not owner.prefix(original, VerifiedWorkAmounts(0, 10), prefix_sequence=1, now_ns=40)
    assert owner.credit_ns == 0
    owner.optional_phase("original-generation-cleanup", starting=False, now_ns=40)
    assert owner.credit_ns == 0 and owner.active_count == 1
