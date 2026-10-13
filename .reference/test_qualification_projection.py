"""Reference regressions only. Integration must reproduce these in real consumers."""

import unittest
from dataclasses import replace

from claim_projection import Binding, Claim, Identity, Result
from qualification_projection import Oracle, Requirement, qualify, reconcile


class QualificationCases(unittest.TestCase):
    def setUp(self):
        self.identity = Identity("b" * 40, "run/attempt-2", "cursor-work", "sqlite-asgi")
        self.claim = Claim("example-owner", "feed", "continues", "local-process")
        self.binding = Binding("test_cursor[one]", "unread-exact", "product", self.claim)
        self.key = (self.binding.node, self.binding.assertion)
        self.result = Result(self.identity, self.binding.node, frozenset({self.binding.assertion}))
        self.requirement = Requirement(self.claim, frozenset({self.key}))
        self.oracle = Oracle(self.claim, "example-owner/declaration")

    def evaluate(self, **changes):
        values = dict(requirement=self.requirement, bindings=(self.binding,),
                      results=(self.result,), selected=self.identity,
                      oracles={self.key: self.oracle}, required_nodes=frozenset({self.result.node}))
        values.update(changes)
        return qualify(**values)

    def test_attribution_and_owned_expectation_can_meet_the_exact_requirement(self):
        self.assertEqual(self.evaluate()["claim_status"], "qualified")

    def test_success_without_oracle_is_not_sufficient(self):
        value = self.evaluate(oracles={})
        self.assertTrue(value["execution_passed"])
        self.assertEqual(value["observed_assertions"], [self.key])
        self.assertEqual(value["claim_status"], "not_established")

    def test_self_derived_or_unresolved_expectation_cannot_launder_behavior(self):
        for kind in ("self-derived", "unresolved"):
            with self.subTest(kind=kind):
                value = self.evaluate(oracles={self.key: replace(self.oracle, kind=kind)})
                self.assertEqual(value["claim_status"], "not_established")

    def test_agreement_only_proves_an_explicit_agreement_requirement(self):
        oracles = {self.key: replace(self.oracle, kind="agreement")}
        self.assertEqual(self.evaluate(oracles=oracles)["claim_status"], "not_established")
        self.assertEqual(self.evaluate(oracles=oracles, requirement=replace(
            self.requirement, agreement_is_promised=True))["claim_status"], "qualified")

    def test_another_claims_oracle_cannot_establish_this_claim(self):
        other = replace(self.oracle, claim=replace(self.claim, predicate="rejects"))
        self.assertEqual(self.evaluate(oracles={self.key: other})["claim_status"], "not_established")

    def test_partial_success_does_not_complete_a_multi_assertion_requirement(self):
        requirement = replace(self.requirement, assertions=self.requirement.assertions | {
            (self.result.node, "terminal-page")})
        value = self.evaluate(requirement=requirement)
        self.assertEqual(value["claim_status"], "not_established")
        self.assertEqual(value["missing_required_assertions"], [(self.result.node, "terminal-page")])

    def test_empty_proof_requirement_is_not_a_vacuous_pass(self):
        with self.assertRaises(ValueError):
            Requirement(self.claim, frozenset())

    def test_required_nonproduct_failure_blocks_the_run_not_just_its_label(self):
        policy = Binding("test_policy", "wording", "policy")
        failure = Result(self.identity, policy.node, frozenset({"wording"}),
                         phases=("passed", "failed", "passed"))
        value = self.evaluate(bindings=(self.binding, policy), results=(self.result, failure),
                              required_nodes=frozenset({self.result.node, policy.node}))
        self.assertFalse(value["execution_passed"])
        self.assertEqual(value["supported_assertions"], [self.key])
        self.assertEqual(value["claim_status"], "not_established")

    def test_failed_enclosing_run_cannot_become_success_from_one_passed_test(self):
        self.assertEqual(self.evaluate(run_exit_status=1)["claim_status"], "not_established")

    def test_removing_the_recorded_behavior_check_leaves_green_execution_without_proof(self):
        value = self.evaluate(results=(replace(self.result, assertions=frozenset()),))
        self.assertTrue(value["execution_passed"])
        self.assertEqual(value["claim_status"], "not_established")

    def test_actual_failed_synthetic_assertion_emits_no_success_record(self):
        recorded = set()
        def check(actual, expected):
            self.assertEqual(actual, expected)
            recorded.add(self.binding.assertion)
        with self.assertRaises(AssertionError):
            check(["lost-event"], ["first", "second"])
        value = self.evaluate(results=(replace(self.result, assertions=frozenset(recorded),
                                              phases=("passed", "failed", "passed")),))
        self.assertEqual(value["claim_status"], "not_established")

    def test_expectation_authority_is_not_optional_for_an_owned_oracle(self):
        with self.assertRaises(ValueError):
            replace(self.oracle, authority="")

    def accounting(self, **changes):
        values = dict(discovered_nodes=(self.binding.node,),
                      purposes={self.binding.node: frozenset({"product"})},
                      bindings=(self.binding,), claims=frozenset({self.claim}))
        values.update(changes)
        return reconcile(**values)

    def test_new_unclassified_check_stays_visible(self):
        value = self.accounting(discovered_nodes=(self.binding.node, "test_new"))
        self.assertFalse(value["classification_complete"])
        self.assertEqual(value["unclassified_nodes"], ["test_new"])

    def test_unbound_product_intent_does_not_become_implementation_only(self):
        value = self.accounting(bindings=())
        self.assertTrue(value["relationship_review_needed"])
        self.assertEqual(value["unbound_product_nodes"], [self.binding.node])

    def test_contract_without_witness_is_a_proof_gap_not_a_missing_test_label(self):
        other = replace(self.claim, subject="another-feed")
        value = self.accounting(claims=frozenset({self.claim, other}))
        self.assertTrue(value["classification_complete"])
        self.assertEqual(value["claims_without_candidate_witness"], [repr(other)])

    def test_retired_expectation_is_dangling_not_authority_to_restore_a_contract(self):
        value = self.accounting(claims=frozenset())
        self.assertTrue(value["dangling_claim_bindings"])
        self.assertTrue(value["relationship_review_needed"])

    def test_stale_removed_node_is_not_silently_dropped(self):
        with self.assertRaises(ValueError):
            self.accounting(discovered_nodes=("test_renamed",))

    def test_mixed_purposes_and_repeat_associations_do_not_multiply_assertions(self):
        other = replace(self.binding, claim=replace(self.claim, predicate="same-owned-meaning"))
        value = self.accounting(purposes={self.binding.node: frozenset({"product", "tooling"})},
                                bindings=(self.binding, other),
                                claims=frozenset({self.claim, other.claim}))
        self.assertEqual(value["counts"], {"nodes": 1, "assertion_groups": 1,
                                          "discovered_claims": 2, "bound_claims": 2})

    def test_unsupported_role_and_duplicate_collection_are_errors(self):
        with self.assertRaises(ValueError):
            self.accounting(purposes={self.binding.node: frozenset({"fast"})})
        with self.assertRaises(ValueError):
            self.accounting(discovered_nodes=(self.binding.node, self.binding.node))

    def test_reclassification_cannot_leave_a_contradictory_product_binding(self):
        with self.assertRaises(ValueError):
            self.accounting(purposes={self.binding.node: frozenset({"policy"})})

    def test_oracle_metadata_and_accounting_are_not_runtime_proof(self):
        self.assertTrue(self.accounting()["classification_complete"])
        self.assertEqual(self.evaluate(results=())["claim_status"], "not_established")


if __name__ == "__main__":
    unittest.main(verbosity=2)
