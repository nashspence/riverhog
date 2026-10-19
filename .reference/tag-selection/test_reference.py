"""Executable design witnesses, deliberately separate from product qualification."""

import ast
import copy
import itertools
import json
from pathlib import Path
import unittest
import unicodedata

from reference import Binding, CLAUSE_MAX, Evaluation, FIELDS, Selector

ROOT = Path(__file__).resolve().parents[2]
TAG_SOURCE = ROOT / "packages/riverhog-protocol/src/riverhog_protocol/collection_tags.py"
DISCOVERY = ROOT / "packages/riverhog-protocol/src/riverhog_protocol/artifact_discovery.py"


def owner_validator():
    # Execute only the audited owner's pure validator and its literal constants.
    # This avoids copying tag semantics or pretending to load the locked product.
    source = ast.parse(TAG_SOURCE.read_text())
    wanted = {"COLLECTION_TAG_UTF8_BYTES_MAX", "_UNICODE_WHITESPACE"}
    nodes = [node for node in source.body if (
        isinstance(node, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id in wanted for t in node.targets)
    ) or (isinstance(node, ast.FunctionDef) and node.name == "validate_collection_tag")]
    namespace = {"unicodedata": unicodedata}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(TAG_SOURCE), "exec"), namespace)
    return namespace["validate_collection_tag"]


VALIDATE = owner_validator()
BINDING = Binding("source", "view", "policy", "generation", "descriptor", "1", 1, "a" * 64)


class Probe:
    def __init__(self, tags):
        self.tags = set(tags)
        self.calls = []

    def __call__(self, binding, tag):
        self.calls.append((binding, tag))
        return {"collection_id": binding.collection_id, "revision": binding.tag_revision,
                "tag_set_identity": binding.tag_set_identity, "tag": tag,
                "present": tag in self.tags}


def finish(document, tags):
    evaluation = Evaluation(Selector.parse(document, VALIDATE), BINDING)
    probe = Probe(tags)
    while evaluation.result is None:
        before = len(probe.calls)
        evaluation.step(BINDING, probe)
        assert len(probe.calls) - before <= 1
    return evaluation.result, probe, evaluation


class SelectionTests(unittest.TestCase):
    def test_exhaustive_three_tag_truth_table(self):
        subsets = [tuple(t for t, take in zip("abc", bits) if take)
                   for bits in itertools.product((False, True), repeat=3)]
        checked = 0
        for all_, any_, none_, members in itertools.product(subsets, repeat=4):
            actual, probe, _ = finish(dict(zip(FIELDS, (all_, any_, none_))), members)
            t = set(members)
            expected = set(all_) <= t and (not any_ or bool(set(any_) & t)) and not set(none_) & t
            self.assertEqual(actual, bool(expected))
            self.assertLessEqual(len(probe.calls), len(set(all_) | set(any_) | set(none_)))
            checked += 1
        self.assertEqual(checked, 4096)

    def test_explicit_empty_matches_without_probes(self):
        result, probe, _ = finish({}, ())
        self.assertTrue(result)
        self.assertEqual(probe.calls, [])

    def test_omitted_clauses_equal_explicit_empty_document(self):
        self.assertEqual(Selector.parse({}, VALIDATE).document(),
                         Selector.parse(dict.fromkeys(FIELDS, []), VALIDATE).document())

    def test_order_preserved_not_silently_canonicalized(self):
        a = Selector.parse({"tags_all": ["b", "a"]}, VALIDATE)
        b = Selector.parse({"tags_all": ["a", "b"]}, VALIDATE)
        self.assertNotEqual(a.document(), b.document())
        self.assertEqual(a.document()["tags_all"], ["b", "a"])
        self.assertEqual(finish(a.document(), "ab")[0], finish(b.document(), "ab")[0])

    def test_each_clause_accepts_256_and_rejects_257(self):
        for name in FIELDS:
            with self.subTest(name=name):
                Selector.parse({name: [f"tag-{i}" for i in range(256)]}, VALIDATE)
                with self.assertRaises(ValueError):
                    Selector.parse({name: [f"tag-{i}" for i in range(257)]}, VALIDATE)

    def test_no_smaller_aggregate_cap_and_restart_without_prefix_reprobe(self):
        document = {name: [f"{name}-{i}" for i in range(256)] for name in FIELDS}
        members = document["tags_all"] + document["tags_any"][-1:]
        evaluation = Evaluation(Selector.parse(document, VALIDATE), BINDING)
        probe = Probe(members)
        while evaluation.result is None:
            old = len(probe.calls)
            evaluation.step(BINDING, probe)
            self.assertLessEqual(len(probe.calls) - old, 1)
            evaluation = copy.deepcopy(evaluation)  # simulated process handoff, not DB proof
        self.assertTrue(evaluation.result)
        self.assertEqual(len(probe.calls), 768)
        self.assertEqual(len({tag for _, tag in probe.calls}), 768)

    def test_duplicate_in_each_clause_rejected(self):
        for name in FIELDS:
            with self.assertRaises(ValueError):
                Selector.parse({name: ["a", "a"]}, VALIDATE)

    def test_cross_clause_overlap_is_valid_and_checked_once(self):
        result, probe, _ = finish({"tags_all": ["a"], "tags_any": ["a"]}, ["a"])
        self.assertTrue(result)
        self.assertEqual(len(probe.calls), 1)
        self.assertFalse(finish({"tags_all": ["a"], "tags_none": ["a"]}, ["a"])[0])

    def test_negative_only(self):
        self.assertTrue(finish({"tags_none": ["a"]}, ["b"])[0])
        self.assertFalse(finish({"tags_none": ["a"]}, ["a"])[0])

    def test_old_shapes_unknown_fields_and_wrong_containers_fail(self):
        bad = [None, [], {"kind": "all"}, {"kind": "tags", "required": ["a"]},
               {"required": ["a"]}, {"tags_any": None}, {"tags_all": "a"},
               {"tags_none": {"a"}}, {"tags_all": [1]}, {"tags_any": [True]},
               {"description_contains": "a"}]
        for value in bad:
            with self.subTest(value=repr(value)), self.assertRaises(ValueError):
                Selector.parse(value, VALIDATE)

    def test_json_round_trip(self):
        original = Selector.parse({"tags_any": ["é", "a"]}, VALIDATE)
        self.assertEqual(Selector.parse(json.loads(json.dumps(original.document())), VALIDATE), original)

    def test_tag_byte_boundary_uses_real_owner(self):
        for tag in ("x" * 65536, "é" * 32768):
            self.assertEqual(Selector.parse({"tags_all": [tag]}, VALIDATE).tags_all, (tag,))
            with self.assertRaises(ValueError):
                Selector.parse({"tags_all": [tag + "x"]}, VALIDATE)

    def test_unicode_invalid_inputs_and_exact_case(self):
        for tag in ("", " a", "a ", "a\u00a0", "e\u0301", "\ud800", "a\n", "\ufdd0"):
            with self.subTest(tag=repr(tag)), self.assertRaises(ValueError):
                Selector.parse({"tags_all": [tag]}, VALIDATE)
        self.assertFalse(finish({"tags_all": ["A"]}, ["a"])[0])

    def test_malformed_evidence_never_becomes_negative(self):
        for field, value in (("collection_id", 1), ("revision", True), ("revision", 2),
                             ("tag_set_identity", "wrong"), ("tag", "other"),
                             ("present", None), ("present", 0), ("present", "false")):
            evaluation = Evaluation(Selector.parse({"tags_none": ["a"]}, VALIDATE), BINDING)
            valid = Probe(())(BINDING, "a")
            valid[field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                evaluation.step(BINDING, lambda b, t: valid)
            self.assertIsNone(evaluation.result)
            self.assertEqual(evaluation.evidence, {})

    def test_missing_evidence_fails(self):
        for response in (None, {}, {"present": False}):
            evaluation = Evaluation(Selector.parse({"tags_none": ["a"]}, VALIDATE), BINDING)
            with self.assertRaises(ValueError):
                evaluation.step(BINDING, lambda b, t: response)
            self.assertIsNone(evaluation.result)

    def test_failed_contact_retains_completed_probe_frontier(self):
        evaluation = Evaluation(Selector.parse({"tags_all": ["a", "b"]}, VALIDATE), BINDING)
        probe = Probe("ab")
        self.assertIsNone(evaluation.step(BINDING, probe))
        def failed(binding, tag):
            raise TimeoutError("control allowance exhausted")
        with self.assertRaises(TimeoutError):
            evaluation.step(BINDING, failed)
        self.assertEqual(evaluation.evidence, {"a": True})
        self.assertTrue(evaluation.step(BINDING, probe))
        self.assertEqual([tag for _, tag in probe.calls], ["a", "b"])

    def test_every_scope_fence_rejects_stale_progress(self):
        for name in BINDING.__dataclass_fields__:
            changed = dict(BINDING.__dict__)
            changed[name] = 2 if name == "tag_revision" else "different"
            evaluation = Evaluation(Selector.parse({}, VALIDATE), BINDING)
            with self.subTest(name=name), self.assertRaises(ValueError):
                evaluation.step(Binding(**changed), Probe(()))

    def test_decisive_truth_short_circuits_unneeded_probes(self):
        result, probe, _ = finish({"tags_none": ["blocked"], "tags_all": ["a"]}, ["blocked"])
        self.assertFalse(result)
        self.assertEqual([tag for _, tag in probe.calls], ["blocked"])

    def test_discovery_source_defaults_and_bounds_match(self):
        tree = ast.parse(DISCOVERY.read_text())
        cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "ArtifactDiscoveryRequest")
        fields = {n.target.id: n for n in cls.body if isinstance(n, ast.AnnAssign)}
        for name in FIELDS:
            keywords = {kw.arg: kw.value for kw in fields[name].value.keywords}
            self.assertEqual(ast.literal_eval(keywords["default"]), ())
            self.assertEqual(ast.literal_eval(keywords["max_length"]), CLAUSE_MAX)


if __name__ == "__main__":
    unittest.main()
