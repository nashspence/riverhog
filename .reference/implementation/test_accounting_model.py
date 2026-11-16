"""Reference simulation witnesses, not storage/crash/backend qualification."""

from __future__ import annotations

import unittest
from dataclasses import FrozenInstanceError, replace

from accounting_model import OwnerCapacity, Root, StaleRevision, UsefulElapsed
from replay_policy import NANOSECONDS_PER_SECOND as NS
from replay_policy import Candidate, Policy, Reason


def candidate(cost=10 * NS):
    return Candidate(
        replay_ns=140 * NS,
        restore_ns=0,
        cost_ns=cost,
        remaining_ns=None,
        replay_provenance="fixture: root-observed useful interval",
        restore_provenance="fixture: zero load",
        cost_provenance="fixture: complete cost",
        remaining_provenance="fixture: unknown remainder",
        safe_consistent=True,
        storage_admitted=True,
        completion_ready=False,
        mandatory_anchor_ready=False,
        conflicting_domain_checkpoint=False,
    )


def root(*, policy=None, credit=140 * NS, cap=4):
    value = Root("construction:stable", policy or Policy(), max_active=cap)
    value.credit_useful(value.summary.revision, credit)
    return value


def reserve(owner, domain="native", cost=10 * NS):
    decision, grant = owner.reserve(owner.summary.revision, domain, candidate(cost))
    assert decision.reason == Reason.ADMIT and grant is not None
    return grant


class AccountingTests(unittest.TestCase):
    def test_completion_blocks_new_selection_but_receives_original_work(self):
        owner = root()
        grant = reserve(owner)
        owner.finish(owner.summary.revision)
        with self.assertRaises(ValueError):
            owner.publish(owner.summary.revision, grant.identity, "too-late")
        self.assertEqual(owner.budget.successful_checkpoints, 0)
        owner.receive_cost(
            owner.summary.revision,
            grant.identity,
            total_actual_ns=8 * NS,
            remaining_reserved_ns=0,
            complete=True,
            nonpublication_proven=True,
        )
        self.assertEqual(owner.budget.charged_ns, 8 * NS)

    def test_completion_allows_reconciliation_of_already_selected_generation(self):
        owner = root()
        grant = reserve(owner)
        owner.publish(owner.summary.revision, grant.identity, "selected-before-completion")
        owner.finish(owner.summary.revision)
        owner.publish(owner.summary.revision, grant.identity, "selected-before-completion")
        self.assertEqual(owner.budget.successful_checkpoints, 1)

    def test_root_clock_credits_parallel_union_once(self):
        clock = UsefulElapsed()
        clock.event(0, fresh_delta=1)
        clock.event(5 * NS, fresh_delta=1)
        clock.event(10 * NS, fresh_delta=-1)
        self.assertEqual(clock.event(15 * NS, fresh_delta=-1), 15 * NS)
        self.assertEqual(clock.event(20 * NS), 15 * NS)  # idle/backoff is not fresh work

    def test_optional_overlap_and_replay_are_uncredited(self):
        clock = UsefulElapsed()
        clock.event(0, fresh_delta=1)
        clock.event(10 * NS, optional_delta=1)
        clock.event(15 * NS, optional_delta=-1)
        self.assertEqual(clock.event(20 * NS, fresh_delta=-1), 15 * NS)
        # Replayed work does not enter fresh activity, even though it takes time.
        self.assertEqual(clock.event(30 * NS), 15 * NS)

    def test_clock_rejects_backwards_or_unbalanced_activity(self):
        clock = UsefulElapsed(10)
        for kwargs in (
            {"now_ns": 9},
            {"now_ns": 10, "fresh_delta": -1},
            {"now_ns": 10, "optional_delta": True},
        ):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                clock.event(**kwargs)
        self.assertEqual(clock.credited_ns, 0)

    def test_cumulative_root_credit_is_not_recredited_per_child_or_phase(self):
        owner = root()
        revision = owner.summary.revision
        owner.credit_useful(owner.summary.revision, 140 * NS)
        self.assertEqual(owner.budget.useful_ns, 140 * NS)
        self.assertEqual(owner.summary.revision, revision)
        self.assertEqual(owner.summary.useful_ns, 0)
        with self.assertRaises(ValueError):
            owner.credit_useful(owner.summary.revision, 0)
        with self.assertRaises(FrozenInstanceError):
            owner.summary.policy.recovery_mode = "restart"

    def test_only_existing_anchor_cas_persists_volatile_useful_credit(self):
        owner = root()
        grant = reserve(owner)
        anchor = owner.summary
        self.assertEqual(anchor.useful_ns, 140 * NS)
        owner.credit_useful(anchor.revision, 145 * NS)
        owner.credit_useful(anchor.revision, 150 * NS)
        self.assertEqual(owner.summary, anchor)
        self.assertEqual(owner.budget.useful_ns, 150 * NS)
        restarted = Root.restore(anchor, authoritative_revision=anchor.revision)
        self.assertEqual(restarted.budget.useful_ns, 140 * NS)
        with self.assertRaises(ValueError):
            restarted.credit_useful(anchor.revision, 150 * NS)
        with self.assertRaises(ValueError):
            restarted.resume_fresh_credit(current_binding_and_frontier_proven=False)
        restarted.resume_fresh_credit(current_binding_and_frontier_proven=True)
        restarted.credit_useful(anchor.revision, 141 * NS)  # one new second, no replay credit
        self.assertEqual(restarted.summary.useful_ns, 140 * NS)
        owner.publish(anchor.revision, grant.identity, "generation:1")
        self.assertEqual(owner.summary.useful_ns, 150 * NS)

    def test_nested_subdivision_cannot_multiply_cost_or_count_allowance(self):
        owner = root(policy=Policy(max_optional_checkpoints=1))
        grant = reserve(owner)
        outer = owner.subdivision(grant.identity)
        core, sdk = outer.split(6 * NS, 4 * NS)
        native, cleanup = core.split(5 * NS, NS)
        self.assertEqual((outer.remaining_ns, core.remaining_ns), (0, 0))
        self.assertEqual(native.remaining_ns + cleanup.remaining_ns + sdk.remaining_ns, 10 * NS)
        self.assertEqual(owner.budget.reserved_ns, 10 * NS)
        self.assertEqual(owner.budget.held_count_slots, 1)
        with self.assertRaises(OwnerCapacity):
            sdk.split(4 * NS, 4 * NS)
        with self.assertRaises(OwnerCapacity):
            core.split(6 * NS)
        with self.assertRaises(OwnerCapacity):
            owner.subdivision(grant.identity)
        blocked, _ = owner.reserve(owner.summary.revision, "independent", candidate(NS))
        self.assertEqual(blocked.reason, Reason.COUNT_LIMIT)

    def test_lost_subdivisions_do_not_refund_or_reissue_original_root_grant(self):
        owner = root()
        grant = reserve(owner)
        owner.subdivision(grant.identity).split(5 * NS, 5 * NS)
        state = owner.summary
        restarted = Root.restore(state, authoritative_revision=state.revision)
        with self.assertRaises(OwnerCapacity):
            restarted.subdivision(grant.identity)
        self.assertEqual(restarted.budget.reserved_ns, 10 * NS)
        self.assertEqual(restarted.budget.held_count_slots, 1)

    def test_same_revision_has_only_one_winner_and_siblings_share_budget(self):
        owner = root()
        revision = owner.summary.revision
        first, _ = owner.reserve(revision, "native", candidate())
        self.assertTrue(first.should_checkpoint)
        with self.assertRaises(StaleRevision):
            owner.reserve(revision, "sdk", candidate())
        self.assertEqual(owner.budget.reserved_ns, 10 * NS)
        blocked, _ = owner.reserve(owner.summary.revision, "sdk", candidate(11 * NS))
        self.assertEqual(blocked.reason, Reason.WORKFLOW_OVERHEAD)
        reserve(owner, "sdk")
        self.assertEqual(owner.budget.reserved_ns, 20 * NS)
        blocked, _ = owner.reserve(owner.summary.revision, "third", candidate(NS))
        self.assertEqual(blocked.reason, Reason.WORKFLOW_OVERHEAD)

    def test_domain_and_existing_owner_capacity_are_independent(self):
        owner = root(cap=1)
        reserve(owner)
        conflict, _ = owner.reserve(owner.summary.revision, "native", candidate(NS))
        self.assertEqual(conflict.reason, Reason.DOMAIN_CONFLICT)
        with self.assertRaises(OwnerCapacity):
            owner.reserve(owner.summary.revision, "sdk", candidate(NS))

    def test_success_and_inflight_count_slots_share_lifetime_limit(self):
        owner = root(policy=Policy(max_optional_checkpoints=1))
        grant = reserve(owner)
        self.assertEqual(owner.budget.held_count_slots, 1)
        blocked, _ = owner.reserve(owner.summary.revision, "sdk", candidate(NS))
        self.assertEqual(blocked.reason, Reason.COUNT_LIMIT)
        owner.publish(owner.summary.revision, grant.identity, "generation:1")
        self.assertEqual(
            (owner.budget.successful_checkpoints, owner.budget.held_count_slots), (1, 0)
        )
        self.assertEqual(owner.budget.reserved_ns, 10 * NS)
        blocked, _ = owner.reserve(owner.summary.revision, "sdk", candidate(NS))
        self.assertEqual(blocked.reason, Reason.COUNT_LIMIT)

    def test_publish_lost_acknowledgement_is_count_idempotent(self):
        owner = root()
        grant = reserve(owner)
        owner.publish(owner.summary.revision, grant.identity, "generation:1")
        revision = owner.summary.revision
        owner.publish(revision, grant.identity, "generation:1")
        self.assertEqual(owner.summary.revision, revision)
        self.assertEqual(owner.budget.successful_checkpoints, 1)
        with self.assertRaises(ValueError):
            owner.publish(revision, grant.identity, "generation:2")

    def test_actual_eight_tail_two_then_three_costs_eleven(self):
        owner = root()
        grant = reserve(owner)
        owner.publish(owner.summary.revision, grant.identity, "generation:1")
        owner.receive_cost(
            owner.summary.revision,
            grant.identity,
            total_actual_ns=8 * NS,
            remaining_reserved_ns=2 * NS,
            complete=False,
        )
        self.assertEqual((owner.budget.charged_ns, owner.budget.reserved_ns), (8 * NS, 2 * NS))
        owner.receive_cost(
            owner.summary.revision,
            grant.identity,
            total_actual_ns=11 * NS,
            remaining_reserved_ns=0,
            complete=True,
        )
        self.assertEqual(owner.budget.optional_liability_ns, 11 * NS)
        self.assertEqual(owner.budget.successful_checkpoints, 1)

    def test_final_cost_receipt_lost_acknowledgement_is_idempotent(self):
        owner = root()
        grant = reserve(owner)
        owner.publish(owner.summary.revision, grant.identity, "generation:1")
        revision = owner.summary.revision
        actual = dict(total_actual_ns=11 * NS, remaining_reserved_ns=0, complete=True)
        self.assertEqual(owner.receive_cost(revision, grant.identity, **actual), "received")
        settled = owner.summary
        self.assertEqual(owner.receive_cost(revision, grant.identity, **actual), "already_received")
        self.assertEqual(owner.summary, settled)
        self.assertEqual(
            (owner.budget.charged_ns, owner.budget.successful_checkpoints), (11 * NS, 1)
        )
        self.assertEqual(owner.summary.grants, ())
        self.assertEqual(owner.summary.issued_grants, 1)
        with self.assertRaisesRegex(ValueError, "domain frontier/receipt"):
            owner.publish(owner.summary.revision, grant.identity, "generation:1")

    def test_settlement_retries_reject_wrong_root_never_issued_and_partial_reports(self):
        owner = root()
        grant = reserve(owner)
        owner.receive_cost(
            owner.summary.revision,
            grant.identity,
            total_actual_ns=2 * NS,
            remaining_reserved_ns=0,
            complete=True,
            nonpublication_proven=True,
        )
        for identity in (
            "another:1",
            "construction:stable:0",
            "construction:stable:2",
            "construction:stable:01",
        ):
            with self.subTest(identity=identity), self.assertRaises(ValueError):
                owner.receive_cost(
                    owner.summary.revision,
                    identity,
                    total_actual_ns=2 * NS,
                    remaining_reserved_ns=0,
                    complete=True,
                )
        with self.assertRaises(ValueError):
            owner.receive_cost(
                owner.summary.revision,
                grant.identity,
                total_actual_ns=2 * NS,
                remaining_reserved_ns=NS,
                complete=False,
            )

    def test_failed_attempt_cost_is_charged_and_slot_needs_nonpublication_proof(self):
        owner = root()
        grant = reserve(owner)
        with self.assertRaises(ValueError):
            owner.receive_cost(
                owner.summary.revision,
                grant.identity,
                total_actual_ns=4 * NS,
                remaining_reserved_ns=0,
                complete=True,
            )
        self.assertEqual(owner.budget.held_count_slots, 1)
        owner.receive_cost(
            owner.summary.revision,
            grant.identity,
            total_actual_ns=4 * NS,
            remaining_reserved_ns=0,
            complete=True,
            nonpublication_proven=True,
        )
        self.assertEqual(
            (owner.budget.successful_checkpoints, owner.budget.held_count_slots), (0, 0)
        )
        self.assertEqual(owner.budget.charged_ns, 4 * NS)

    def test_overrun_debt_blocks_optional_work_until_new_real_credit(self):
        owner = root()
        grant = reserve(owner)
        owner.receive_cost(
            owner.summary.revision,
            grant.identity,
            total_actual_ns=21 * NS,
            remaining_reserved_ns=0,
            complete=True,
            nonpublication_proven=True,
        )
        blocked, _ = owner.reserve(owner.summary.revision, "sdk", candidate(NS))
        self.assertEqual(blocked.reason, Reason.OVERRUN_DEBT)
        owner.credit_useful(owner.summary.revision, 154 * NS)
        reserve(owner, "sdk", NS)

    def test_repeated_failures_exhaust_budget_without_successful_generations(self):
        owner = root()
        for _ in range(5):
            grant = reserve(owner, cost=4 * NS)
            owner.receive_cost(
                owner.summary.revision,
                grant.identity,
                total_actual_ns=4 * NS,
                remaining_reserved_ns=0,
                complete=True,
                nonpublication_proven=True,
            )
        self.assertEqual(owner.budget.charged_ns, 20 * NS)
        self.assertEqual(owner.budget.successful_checkpoints, 0)
        blocked, _ = owner.reserve(owner.summary.revision, "native", candidate(NS))
        self.assertEqual(blocked.reason, Reason.WORKFLOW_OVERHEAD)

    def test_unknown_attempt_preserves_reservation_and_seals_until_reconciled(self):
        owner = root()
        grant = reserve(owner)
        owner.mark_unknown(owner.summary.revision, grant.identity)
        blocked, _ = owner.reserve(owner.summary.revision, "sdk", candidate(NS))
        self.assertEqual(blocked.reason, Reason.ACCOUNTING_UNCERTAIN)
        self.assertEqual((owner.budget.reserved_ns, owner.budget.held_count_slots), (10 * NS, 1))
        with self.assertRaises(ValueError):
            owner.receive_cost(
                owner.summary.revision,
                grant.identity,
                total_actual_ns=3 * NS,
                remaining_reserved_ns=7 * NS,
                complete=False,
                reconciled=True,
            )
        with self.assertRaises(ValueError):
            owner.receive_cost(
                owner.summary.revision,
                grant.identity,
                total_actual_ns=3 * NS,
                remaining_reserved_ns=0,
                complete=True,
                nonpublication_proven=True,
            )
        owner.receive_cost(
            owner.summary.revision,
            grant.identity,
            total_actual_ns=3 * NS,
            remaining_reserved_ns=0,
            complete=True,
            nonpublication_proven=True,
            reconciled=True,
        )
        self.assertFalse(owner.budget.accounting_uncertain)
        self.assertEqual(owner.budget.charged_ns, 3 * NS)

    def test_proven_nonpublication_cannot_later_select_a_generation(self):
        owner = root()
        grant = reserve(owner)
        owner.receive_cost(
            owner.summary.revision,
            grant.identity,
            total_actual_ns=3 * NS,
            remaining_reserved_ns=2 * NS,
            complete=False,
            nonpublication_proven=True,
        )
        with self.assertRaises(ValueError):
            owner.publish(owner.summary.revision, grant.identity, "impossible-generation")
        owner.receive_cost(
            owner.summary.revision,
            grant.identity,
            total_actual_ns=5 * NS,
            remaining_reserved_ns=0,
            complete=True,
        )
        self.assertEqual(owner.budget.successful_checkpoints, 0)

    def test_failed_pregrant_cas_is_charged_even_without_a_grant(self):
        owner = root()
        owner.charge_unreserved_failure(owner.summary.revision, 3 * NS)
        self.assertEqual((owner.budget.charged_ns, owner.summary.grants), (3 * NS, ()))
        owner.charge_unreserved_failure(owner.summary.revision, None)
        blocked, _ = owner.reserve(owner.summary.revision, "native", candidate(NS))
        self.assertEqual(blocked.reason, Reason.ACCOUNTING_UNCERTAIN)
        owner.reconcile_unreserved_failure(owner.summary.revision, 2 * NS)
        self.assertEqual(owner.budget.charged_ns, 5 * NS)

    def test_crash_with_no_grant_is_not_proof_no_optional_cost(self):
        owner = root()
        persisted = owner.summary
        restarted = Root.restore(persisted, authoritative_revision=persisted.revision)
        self.assertEqual(restarted.summary.grants, ())
        blocked, _ = restarted.reserve(restarted.summary.revision, "native", candidate(NS))
        self.assertEqual(blocked.reason, Reason.ACCOUNTING_UNCERTAIN)

    def test_current_root_restore_does_not_rewind_to_optional_generation(self):
        owner = root()
        first = reserve(owner)
        owner.publish(owner.summary.revision, first.identity, "generation:older")
        checkpoint_summary = owner.summary
        owner.receive_cost(
            owner.summary.revision,
            first.identity,
            total_actual_ns=8 * NS,
            remaining_reserved_ns=0,
            complete=True,
        )
        second = reserve(owner, "sdk", NS)
        current = owner.summary  # authoritative control record, newer than fallback data
        with self.assertRaises(StaleRevision):
            Root.restore(checkpoint_summary, authoritative_revision=current.revision)
        restarted = Root.restore(current, authoritative_revision=current.revision)
        self.assertEqual(restarted.budget.charged_ns, 8 * NS)
        self.assertEqual(restarted.budget.successful_checkpoints, 1)
        self.assertEqual(restarted.summary.grants[0].identity, second.identity)
        self.assertEqual(restarted.budget.reserved_ns, NS)
        self.assertTrue(restarted.budget.accounting_uncertain)

    def test_user_completion_retains_tail_in_surviving_control_root(self):
        owner = root()
        grant = reserve(owner)
        owner.publish(owner.summary.revision, grant.identity, "generation:1")
        owner.finish(owner.summary.revision)  # upload may now be deleted; root survives
        blocked, _ = owner.reserve(owner.summary.revision, "sdk", candidate(NS))
        self.assertEqual(blocked.reason, Reason.COMPLETE)
        with self.assertRaises(ValueError):
            owner.close_cost_epoch(owner.summary.revision)
        owner.receive_cost(
            owner.summary.revision,
            grant.identity,
            total_actual_ns=11 * NS,
            remaining_reserved_ns=0,
            complete=True,
        )
        owner.close_cost_epoch(owner.summary.revision)
        state = owner.summary
        restarted = Root.restore(state, authoritative_revision=state.revision)
        self.assertEqual(restarted.budget.charged_ns, 11 * NS)
        self.assertFalse(restarted.budget.accounting_uncertain)

    def test_restart_mode_never_allocates_optional_grants(self):
        owner = root(policy=Policy(recovery_mode="restart"))
        decision, grant = owner.reserve(owner.summary.revision, "native", candidate())
        self.assertEqual(decision.reason, Reason.RESTART_MODE)
        self.assertIsNone(grant)

    def test_changed_candidate_estimate_does_not_rewrite_held_liability(self):
        owner = root()
        reserve(owner)
        cheap = replace(candidate(), cost_ns=0)
        decision, grant = owner.reserve(owner.summary.revision, "native", cheap)
        self.assertEqual(decision.reason, Reason.DOMAIN_CONFLICT)
        self.assertIsNone(grant)
        self.assertEqual(owner.budget.reserved_ns, 10 * NS)


if __name__ == "__main__":
    unittest.main()
