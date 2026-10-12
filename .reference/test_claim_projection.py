"""Executable synthetic counterexamples for the integration adapter to reproduce."""

import unittest
from dataclasses import replace

from claim_projection import Binding, Claim, Identity, Result, required_checks_pass, support


class ProjectionCases(unittest.TestCase):
    def setUp(self):
        self.identity = Identity("a" * 40, "run/attempt-1", "work/cursor", "sqlite-asgi")
        self.claim = Claim("example-owner", "example-feed", "continues", "local-process")
        self.binding = Binding("test_example[x]", "unread-events", "product", self.claim)
        self.result = Result(self.identity, self.binding.node, frozenset({"unread-events"}))

    def project(self, *, claim=None, bindings=None, results=None):
        return support(
            self.claim if claim is None else claim,
            (self.binding,) if bindings is None else bindings,
            (self.result,) if results is None else results,
            self.identity,
        )

    def test_exact_success_supports_only_the_scoped_claim(self):
        self.assertEqual(self.project(), {(self.binding.node, self.binding.assertion)})

    def test_candidate_without_execution_does_not_pass(self):
        self.assertFalse(self.project(results=()))

    def test_green_test_without_assertion_attribution_does_not_pass(self):
        result = replace(self.result, assertions=frozenset())
        self.assertFalse(self.project(results=(result,)))
        self.assertTrue(required_checks_pass(frozenset({result.node}), (result,), self.identity))

    def test_nonproduct_checks_do_not_prove_product_behavior(self):
        for purpose in ("implementation", "policy", "tooling", "unresolved"):
            with self.subTest(purpose=purpose):
                self.assertFalse(self.project(bindings=(replace(self.binding, purpose=purpose, claim=None),)))

    def test_contract_filename_does_not_confer_authority(self):
        binding = Binding("tests/unit/test_contract_example.py::test_ok", "shape", "tooling")
        result = replace(self.result, node=binding.node, assertions=frozenset({"shape"}))
        self.assertFalse(self.project(bindings=(binding,), results=(result,)))

    def test_mixed_test_does_not_promote_policy_assertion(self):
        policy = Binding(self.binding.node, "repo-rule", "policy")
        policy_only = replace(self.result, assertions=frozenset({"repo-rule"}))
        self.assertFalse(self.project(bindings=(self.binding, policy), results=(policy_only,)))
        both = replace(self.result, assertions=frozenset({"repo-rule", "unread-events"}))
        self.assertEqual(len(self.project(bindings=(self.binding, policy), results=(both,))), 1)

    def test_negative_behavior_can_be_a_successful_product_witness(self):
        rejection = replace(self.claim, predicate="rejects-invalid-cursor")
        binding = replace(self.binding, claim=rejection)
        self.assertTrue(self.project(claim=rejection, bindings=(binding,)))
        self.assertFalse(self.project(bindings=(binding,)))

    def test_structural_check_does_not_prove_runtime_behavior(self):
        binding = replace(self.binding, claim=replace(self.claim, predicate="schema-has-cursor"))
        self.assertFalse(self.project(bindings=(binding,)))

    def test_no_implicit_scope_owner_subject_or_predicate_widening(self):
        for field, value in (("scope", "built-postgres"), ("owner", "other-owner"),
                             ("subject", "other-feed"), ("predicate", "crash-recovery")):
            with self.subTest(field=field):
                self.assertFalse(self.project(claim=replace(self.claim, **{field: value})))

    def test_every_required_execution_phase_must_pass(self):
        for index in range(3):
            for outcome in ("failed", "skipped", "missing"):
                with self.subTest(index=index, outcome=outcome):
                    phases = list(self.result.phases)
                    phases[index] = outcome
                    self.assertFalse(self.project(results=(replace(self.result, phases=tuple(phases)),)))

    def test_xfail_and_xpass_are_not_ordinary_success(self):
        for phases in (("passed",) * 3, ("passed", "skipped", "passed")):
            self.assertFalse(self.project(results=(replace(self.result, phases=phases, was_xfail=True),)))

    def test_interrupted_execution_never_passes(self):
        self.assertFalse(self.project(results=(replace(self.result, complete=False),)))

    def test_exact_identity_is_required(self):
        for field in ("source", "run", "work", "conditions"):
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.project(results=(replace(self.result, identity=replace(self.identity, **{field: "other"})),))

    def test_invalid_supplied_evidence_is_an_error(self):
        with self.assertRaises(ValueError):
            self.project(results=(replace(self.result, valid=False),))
        with self.assertRaises(ValueError):
            self.project(results=(replace(self.result, phases=("passed", "unknown", "passed")),))

    def test_duplicate_binding_or_result_is_an_error(self):
        with self.assertRaises(ValueError):
            self.project(bindings=(self.binding, self.binding))
        with self.assertRaises(ValueError):
            self.project(results=(self.result, self.result))

    def test_unknown_node_or_assertion_cannot_enter_evidence(self):
        for change in ({"node": "unknown"}, {"assertions": frozenset({"unknown"})}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.project(results=(replace(self.result, **change),))

    def test_one_parameter_case_does_not_prove_another(self):
        binding = replace(self.binding, node="test_example[y]")
        self.assertFalse(self.project(bindings=(binding,), results=()))
        with self.assertRaises(ValueError):
            self.project(bindings=(binding,))

    def test_required_policy_failure_stays_a_validation_failure(self):
        policy = replace(self.result, node="test_repo_policy", assertions=frozenset(), phases=("passed", "failed", "passed"))
        self.assertFalse(required_checks_pass(frozenset({policy.node}), (policy,), self.identity))
        self.assertFalse(required_checks_pass(frozenset({policy.node}), (), self.identity))

    def test_optional_skip_is_not_required_success_or_product_support(self):
        skipped = replace(self.result, phases=("skipped", "missing", "missing"))
        self.assertTrue(required_checks_pass(frozenset(), (skipped,), self.identity))
        self.assertFalse(self.project(results=(skipped,)))

    def test_purpose_requires_resolution_not_an_implicit_default(self):
        for purpose, claim in (("unknown", None), ("product", None), ("policy", self.claim)):
            with self.subTest(purpose=purpose), self.assertRaises(ValueError):
                replace(self.binding, purpose=purpose, claim=claim)

    def test_display_count_is_distinct_assertions_not_link_count(self):
        other = replace(self.binding, claim=replace(self.claim, predicate="other-predicate"))
        self.assertEqual(len(self.project(bindings=(self.binding, other))), 1)

    def test_advisory_goals_are_not_required_work_or_claim_inputs(self):
        baseline = (self.project(), required_checks_pass(frozenset({self.result.node}), (self.result,), self.identity))
        for advisory in (None, {"goal": 1, "elapsed": 999999}, {"comparison": "unavailable"}):
            with self.subTest(advisory=advisory):
                # Advisory data stays outside the required-work/claim projection.
                self.assertEqual((self.project(), required_checks_pass(frozenset({self.result.node}), (self.result,), self.identity)), baseline)


if __name__ == "__main__":
    unittest.main(verbosity=2)
