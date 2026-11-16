"""Focused reference-model witnesses; these are not product qualification.

From the repository root:
    python3 -m unittest discover -s .reference/implementation -p test_replay_policy.py -v
"""

from __future__ import annotations

import math
import unittest
from dataclasses import FrozenInstanceError, replace

from replay_policy import (
    NANOSECONDS_PER_SECOND as NS,
)
from replay_policy import (
    Budget,
    Candidate,
    Decision,
    Policy,
    Reason,
    ReservationProposal,
    decide,
)


def candidate(**changes: object) -> Candidate:
    base = Candidate(
        replay_ns=120 * NS,
        restore_ns=0,
        cost_ns=NS,
        remaining_ns=None,
        replay_provenance="fixture: covered serial useful work",
        restore_provenance="fixture: admitted empty restore",
        cost_provenance="fixture: bounded candidate and lifecycle envelope",
        remaining_provenance="fixture: unknown input length",
        safe_consistent=True,
        storage_admitted=True,
        completion_ready=False,
        mandatory_anchor_ready=False,
        conflicting_domain_checkpoint=False,
    )
    return replace(base, **changes)


class PolicyTests(unittest.TestCase):
    def result(self, *, policy=None, budget=None, **changes) -> Decision:
        return decide(
            policy or Policy(), candidate(**changes), budget or Budget(useful_ns=120 * NS)
        )

    def test_exact_defaults_and_mapping_roundtrip(self):
        expected = {
            "recovery_mode": "adaptive",
            "replay_target_seconds": 120,
            "max_optional_overhead_fraction": 0.125,
            "max_optional_checkpoints": None,
            "retained_checkpoint_generations": 2,
        }
        policy = Policy.from_mapping({})
        self.assertEqual(policy.to_mapping(), expected)
        self.assertEqual(Policy.from_mapping(expected), policy)
        self.assertEqual((policy.fraction_numerator, policy.fraction_complement), (1, 7))

    def test_configuration_overrides_roundtrip_without_aliases(self):
        values = {
            "recovery_mode": "restart",
            "replay_target_seconds": 12.25,
            "max_optional_overhead_fraction": 0.25,
            "max_optional_checkpoints": 7,
            "retained_checkpoint_generations": 3,
        }
        self.assertEqual(Policy.from_mapping(values).to_mapping(), values)
        for bad in ({"target_seconds": 120}, {"target_ns": 0}, {1: 120}):
            with self.subTest(bad=bad), self.assertRaises((TypeError, ValueError)):
                Policy.from_mapping(bad)

    def test_below_at_above_target_to_one_nanosecond(self):
        for delta, reason in ((-1, Reason.BELOW_TARGET), (0, Reason.ADMIT), (1, Reason.ADMIT)):
            with self.subTest(delta=delta):
                self.assertEqual(self.result(replay_ns=120 * NS + delta).reason, reason)

    def test_completion_at_threshold_precedes_every_optional_condition(self):
        answer = self.result(
            policy=Policy(recovery_mode="restart", max_optional_checkpoints=0),
            budget=Budget(accounting_uncertain=True),
            completion_ready=True,
            mandatory_anchor_ready=True,
            safe_consistent=False,
            storage_admitted=False,
            conflicting_domain_checkpoint=True,
            cost_ns=None,
            replay_ns=None,
            restore_ns=None,
        )
        self.assertEqual(answer.reason, Reason.COMPLETE)
        self.assertFalse(answer.should_checkpoint)

    def test_mandatory_anchor_precedes_restart_and_unknown_cost(self):
        answer = self.result(
            policy=Policy(recovery_mode="restart"),
            mandatory_anchor_ready=True,
            cost_ns=None,
            budget=Budget(accounting_uncertain=True),
        )
        self.assertEqual(answer.reason, Reason.MANDATORY_ANCHOR)

    def test_restart_suppresses_zero_cost_too(self):
        self.assertEqual(
            self.result(policy=Policy(recovery_mode="restart"), cost_ns=0).reason,
            Reason.RESTART_MODE,
        )

    def test_storage_consistency_and_domain_are_separate_suppressions(self):
        for changes, reason in (
            ({"safe_consistent": False}, Reason.UNSAFE_CANDIDATE),
            ({"storage_admitted": False}, Reason.STORAGE_REFUSED),
            ({"conflicting_domain_checkpoint": True}, Reason.DOMAIN_CONFLICT),
        ):
            with self.subTest(reason=reason):
                self.assertEqual(self.result(**changes).reason, reason)

    def test_unknown_estimates_are_not_zero(self):
        for name, reason in (
            ("cost_ns", Reason.UNKNOWN_COST),
            ("replay_ns", Reason.UNKNOWN_REPLAY),
            ("restore_ns", Reason.UNKNOWN_RESTORE),
        ):
            with self.subTest(name=name):
                self.assertEqual(self.result(**{name: None}).reason, reason)
        self.assertEqual(self.result(cost_ns=0, remaining_ns=None).reason, Reason.ADMIT)
        self.assertEqual(self.result(restore_ns=0).reason, Reason.ADMIT)

    def test_restore_cost_is_subtracted_and_clamped_at_zero(self):
        below = self.result(replay_ns=130 * NS, restore_ns=10 * NS + 1)
        exact = self.result(replay_ns=130 * NS, restore_ns=10 * NS)
        negative = self.result(replay_ns=NS, restore_ns=2 * NS)
        self.assertEqual(below.reason, Reason.BELOW_TARGET)
        self.assertEqual(exact.reason, Reason.ADMIT)
        self.assertEqual(exact.net_saved_ns, 120 * NS)
        self.assertEqual(negative.net_saved_ns, 0)

    def test_expensive_snapshot_needs_210_seconds_at_default(self):
        for delta, reason in ((-1, Reason.CANDIDATE_OVERHEAD), (0, Reason.ADMIT)):
            with self.subTest(delta=delta):
                answer = self.result(
                    replay_ns=210 * NS + delta,
                    cost_ns=30 * NS,
                    budget=Budget(useful_ns=210 * NS),
                )
                self.assertEqual(answer.reason, reason)

    def test_candidate_limit_cannot_borrow_large_aggregate_credit(self):
        answer = self.result(cost_ns=18 * NS, budget=Budget(useful_ns=10_000 * NS))
        self.assertEqual(answer.reason, Reason.CANDIDATE_OVERHEAD)

    def test_aggregate_limit_cannot_borrow_large_candidate_savings(self):
        answer = self.result(
            replay_ns=1_000 * NS, cost_ns=10 * NS, budget=Budget(useful_ns=69 * NS)
        )
        self.assertEqual(answer.reason, Reason.WORKFLOW_OVERHEAD)
        exact = self.result(cost_ns=10 * NS, budget=Budget(useful_ns=70 * NS))
        self.assertEqual(exact.reason, Reason.ADMIT)

    def test_failed_actual_cost_and_held_reservations_both_consume_budget(self):
        for charged, reserved in ((8 * NS, 9 * NS), (17 * NS, 0), (0, 17 * NS)):
            with self.subTest(charged=charged):
                answer = self.result(
                    budget=Budget(useful_ns=120 * NS, charged_ns=charged, reserved_ns=reserved)
                )
                self.assertEqual(answer.reason, Reason.WORKFLOW_OVERHEAD)

    def test_overrun_debt_blocks_even_zero_cost_candidate(self):
        debt = Budget(useful_ns=120 * NS, charged_ns=18 * NS)
        self.assertEqual(self.result(cost_ns=0, budget=debt).reason, Reason.OVERRUN_DEBT)
        repaid = replace(debt, useful_ns=126 * NS)
        self.assertEqual(self.result(cost_ns=0, budget=repaid).reason, Reason.ADMIT)

    def test_changed_estimate_reassesses_without_erasing_costs(self):
        budget = Budget(useful_ns=140 * NS, charged_ns=10 * NS, reserved_ns=NS)
        self.assertEqual(self.result(cost_ns=9 * NS, budget=budget).reason, Reason.ADMIT)
        self.assertEqual(
            self.result(cost_ns=9 * NS + 1, budget=budget).reason, Reason.WORKFLOW_OVERHEAD
        )

    def test_uncertain_recovery_accounting_fails_closed_for_optional_work(self):
        self.assertEqual(
            self.result(budget=Budget(useful_ns=1_000 * NS, accounting_uncertain=True)).reason,
            Reason.ACCOUNTING_UNCERTAIN,
        )

    def test_near_completion_known_equal_less_greater_and_unknown(self):
        for remaining, reason in (
            (0, Reason.NEAR_COMPLETION),
            (NS, Reason.NEAR_COMPLETION),
            (NS + 1, Reason.ADMIT),
            (None, Reason.ADMIT),
        ):
            with self.subTest(remaining=remaining):
                self.assertEqual(self.result(remaining_ns=remaining).reason, reason)

    def test_success_and_held_slots_consume_count_but_attempts_are_cost_only(self):
        policy = Policy(max_optional_checkpoints=3)
        for successful, held, reason in (
            (2, 0, Reason.ADMIT),
            (2, 1, Reason.COUNT_LIMIT),
            (3, 0, Reason.COUNT_LIMIT),
        ):
            answer = self.result(
                policy=policy,
                budget=Budget(
                    useful_ns=120 * NS, successful_checkpoints=successful, held_count_slots=held
                ),
            )
            self.assertEqual(answer.reason, reason)
        self.assertEqual(
            self.result(budget=Budget(useful_ns=120 * NS, successful_checkpoints=10**12)).reason,
            Reason.ADMIT,
        )

    def test_retention_is_not_creation_count_or_permission_to_delete(self):
        policy = Policy(retained_checkpoint_generations=1)
        answer = self.result(
            policy=policy, budget=Budget(useful_ns=120 * NS, successful_checkpoints=20)
        )
        self.assertEqual(answer.reason, Reason.ADMIT)
        self.assertEqual(answer.reservation, ReservationProposal(NS, 1))

    def test_zero_count_disables_zero_cost_and_zero_fraction_allows_only_free(self):
        self.assertEqual(
            self.result(policy=Policy(max_optional_checkpoints=0), cost_ns=0).reason,
            Reason.COUNT_LIMIT,
        )
        self.assertEqual(
            self.result(policy=Policy(max_optional_overhead_fraction=0)).reason,
            Reason.CANDIDATE_OVERHEAD,
        )
        self.assertEqual(
            self.result(
                policy=Policy(max_optional_overhead_fraction=0), cost_ns=0, budget=Budget()
            ).reason,
            Reason.ADMIT,
        )

    def test_strict_public_numeric_validation(self):
        fields = {
            "replay_target_seconds": [True, False, "120", None, 0, -1, math.inf, math.nan],
            "max_optional_overhead_fraction": [True, "0.125", -0.1, 1, math.inf, math.nan],
            "max_optional_checkpoints": [True, 1.0, "1", -1, math.inf],
            "retained_checkpoint_generations": [True, 2.0, "2", None, 0, -1],
            "recovery_mode": [True, "ADAPTIVE", "auto", None],
        }
        for name, values in fields.items():
            for value in values:
                with self.subTest(name=name, value=value):
                    with self.assertRaises((TypeError, ValueError)):
                        Policy.from_mapping({name: value})

    def test_strict_ns_flags_and_provenance_validation(self):
        for name in ("replay_ns", "restore_ns", "cost_ns", "remaining_ns"):
            for value in (True, 0.0, -1, "1", math.inf):
                with self.subTest(name=name, value=value):
                    with self.assertRaises((TypeError, ValueError)):
                        candidate(**{name: value})
        for name in (
            "useful_ns",
            "charged_ns",
            "reserved_ns",
            "successful_checkpoints",
            "held_count_slots",
        ):
            with self.subTest(name=name), self.assertRaises(TypeError):
                Budget(**{name: True})
        for name in (
            "safe_consistent",
            "storage_admitted",
            "completion_ready",
            "mandatory_anchor_ready",
            "conflicting_domain_checkpoint",
        ):
            with self.subTest(name=name), self.assertRaises(TypeError):
                candidate(**{name: 1})
        with self.assertRaises(TypeError):
            Budget(accounting_uncertain=1)
        with self.assertRaises(ValueError):
            candidate(cost_ns=None, cost_provenance=" ")

    def test_float_target_rounds_up_exactly_and_never_overflows(self):
        policy = Policy(replay_target_seconds=0.1)
        numerator, denominator = (0.1).as_integer_ratio()
        self.assertEqual(policy.target_ns, (numerator * NS + denominator - 1) // denominator)
        self.assertEqual(policy.target_ns, 100_000_001)
        for target in (1e308, 10**1000):
            policy = Policy(replay_target_seconds=target)
            answer = self.result(
                policy=policy,
                replay_ns=policy.target_ns,
                cost_ns=0,
                budget=Budget(useful_ns=policy.target_ns),
            )
            self.assertEqual(answer.reason, Reason.ADMIT)

    def test_fraction_extremes_use_exact_products(self):
        for fraction in (math.nextafter(0.0, 1.0), math.nextafter(1.0, 0.0)):
            policy = Policy(max_optional_overhead_fraction=fraction)
            numerator, denominator = fraction.as_integer_ratio()
            self.assertEqual(policy.fraction_numerator, numerator)
            self.assertEqual(policy.fraction_complement, denominator - numerator)
            self.assertEqual(self.result(policy=policy, cost_ns=0).reason, Reason.ADMIT)

    def test_decision_and_inputs_are_immutable_and_no_reservation_is_allocated(self):
        policy, item, budget = Policy(), candidate(), Budget(useful_ns=120 * NS)
        before = (policy.to_mapping(), item, budget)
        first, second = decide(policy, item, budget), decide(policy, item, budget)
        self.assertEqual(first, second)
        self.assertEqual(before, (policy.to_mapping(), item, budget))
        self.assertTrue(first.should_checkpoint)
        with self.assertRaises(FrozenInstanceError):
            budget.charged_ns = NS


if __name__ == "__main__":
    unittest.main()
