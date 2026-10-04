"""Exact prepared-candidate witnesses; no cryptographic/human approval is simulated."""
import argparse
import copy
import unittest

from candidate import candidate_evidence, verify_selected_candidate
from compiler import Invalid, apply_argparse, compile_corpus, read_json, target
from test_compiler import corpus, ledger, parameter, root


def prepared():
    compiled = compile_corpus(corpus(), ledger(), final=True)
    parser = argparse.ArgumentParser(prog="synthetic-recover")
    parser.add_argument("--count", type=int, default=3)
    apply_argparse(parser, compiled["resolved"][target(root())],
                   {"count": compiled["resolved"][target(parameter())]})
    identity = {"code_sha": "1" * 40, "documentation_commit": "2" * 40, "version": "v1.0.0",
                "compiler_sha256": "3" * 64, "toolchain_sha256": "4" * 64}
    artifacts = {"synthetic-package.bin": b"synthetic prepared artifact, NOT an actual wheel"}
    observed = {"cli/recover.txt": parser.format_help().encode(),
                "render/recover.html": compiled["pages"]["reference/recover.md"]["html"].encode()}
    return compiled, identity, artifacts, observed


class CandidateTests(unittest.TestCase):
    def test_prepared_candidate_is_bound_not_autoapproved(self):
        compiled, identity, artifacts, observed = prepared()
        evidence = candidate_evidence(compiled, identity=identity, artifacts=artifacts,
                                      observed=observed, required_outputs=set(observed))
        self.assertNotIn("approved", read_json(evidence))
        self.assertIn(b"Recover one object.", observed["cli/recover.txt"])
        verify_selected_candidate(evidence, evidence, artifacts=artifacts, observed=observed)

    def test_changed_artifact_or_destination_fails_selected_candidate(self):
        compiled, identity, artifacts, observed = prepared()
        evidence = candidate_evidence(compiled, identity=identity, artifacts=artifacts,
                                      observed=observed, required_outputs=set(observed))
        for original in (artifacts, observed):
            changed = dict(original); changed[next(iter(changed))] += b"changed"
            with self.assertRaisesRegex(Invalid, "bytes changed"):
                verify_selected_candidate(evidence, evidence,
                    artifacts=changed if original is artifacts else artifacts,
                    observed=changed if original is observed else observed)

    def test_missing_destination_and_incomplete_coverage_fail(self):
        compiled, identity, artifacts, observed = prepared()
        with self.assertRaises(Invalid):
            candidate_evidence(compiled, identity=identity, artifacts=artifacts,
                               observed=observed, required_outputs=set(observed) | {"openapi/service.json"})
        compiled["coverage"]["missing"] = [parameter()]
        with self.assertRaises(Invalid):
            candidate_evidence(compiled, identity=identity, artifacts=artifacts,
                               observed=observed, required_outputs=set(observed))

    def test_changed_source_prose_policy_toolchain_or_compiler_needs_new_review(self):
        compiled, identity, artifacts, observed = prepared()
        def build(c, i):
            return candidate_evidence(c, identity=i, artifacts=artifacts,
                                      observed=observed, required_outputs=set(observed))
        selected = build(compiled, identity)
        for key in identity:
            altered = dict(identity); altered[key] = "v1.0.1" if key == "version" else "5" * len(identity[key])
            with self.subTest(key=key), self.assertRaisesRegex(Invalid, "selected candidate changed"):
                verify_selected_candidate(selected, build(compiled, altered), artifacts=artifacts, observed=observed)
        for key in compiled["inputs"]:
            altered = copy.deepcopy(compiled); altered["inputs"][key] = "6" * 64
            with self.subTest(key=key), self.assertRaisesRegex(Invalid, "selected candidate changed"):
                verify_selected_candidate(selected, build(altered, identity), artifacts=artifacts, observed=observed)

    def test_same_candidate_promotion_is_repeatable_without_rebuild(self):
        compiled, identity, artifacts, observed = prepared()
        first = candidate_evidence(compiled, identity=identity, artifacts=artifacts,
                                   observed=observed, required_outputs=set(observed))
        second = candidate_evidence(compiled, identity=identity, artifacts=artifacts,
                                    observed=observed, required_outputs=set(observed))
        self.assertEqual(first, second)
        verify_selected_candidate(first, second, artifacts=artifacts, observed=observed)


if __name__ == "__main__":
    unittest.main()
