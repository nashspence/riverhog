# SPDX-License-Identifier: Apache-2.0
"""Executable diagnostic challenges; not product qualification or a new contract."""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import probe as p


class PrefixTests(unittest.TestCase):
    def test_exact_all_prefix_hashes_and_counts(self):
        for n in (0, 1, 16, 32, 64):
            result = p.hash_trial(n)
            self.assertEqual(result["naive_recovery_hash_octets"], 64 * n * (n + 1) // 2)
            self.assertEqual(result["streamed_hash_octets"], 64 * n)
            self.assertEqual(result["repeated_bad_recovery_hash_octets"], 64 * n * (n + 1) * (n + 2) // 6)

    def test_variable_lengths_and_empty_chunks_preserve_every_anchor(self):
        raw = [b"", b"a", b"b" * 63, b"", b"c" * 65]
        slow, reads = p.prefix_hashes(raw, incremental=False)
        fast, once = p.prefix_hashes(raw, incremental=True)
        self.assertEqual(slow, fast)
        self.assertEqual(once, sum(map(len, raw)))
        self.assertEqual(reads, sum(sum(map(len, raw[:i])) for i in range(1, len(raw) + 1)))

    def test_tampered_prefix_does_not_match(self):
        original = p.prefix_hashes([b"old", b"new"], incremental=True)[0]
        changed = p.prefix_hashes([b"bad", b"new"], incremental=True)[0]
        self.assertNotEqual(original[-1], changed[-1])

    def test_fixed_width_fixture_bounds_are_not_product_limits(self):
        with self.assertRaises(ValueError):
            p.frames(10**8)
        with self.assertRaises(ValueError):
            p.frames(1, 7)


class NativeRadixTests(unittest.TestCase):
    def test_point_edits_reuse_old_root(self):
        for n in (16, 128, 1024):
            result = p.tag_trial(n)
            self.assertLessEqual(result["insert"]["node_gets"], 33)
            self.assertLessEqual(result["remove"]["node_gets"], 66)

    def test_text_length_is_not_radix_depth(self):
        small = p.tag_trial(128, 32)
        large = p.tag_trial(128, 65536)
        self.assertLessEqual(large["insert"]["node_gets"], 33)
        self.assertGreater(large["insert"]["new_node_octets"], small["insert"]["new_node_octets"])
        self.assertEqual(large["operand_utf8_octets"], 65536)

    def test_actual_hash_prefix_shape(self):
        result = p.tag_trial(128, digest_prefix=True)
        self.assertGreater(result["fixture_candidates"], 128)
        self.assertLessEqual(result["insert"]["node_gets"], 33)

    def test_native_rejects_over_limit_tag(self):
        kernel = p.load_tag_kernel()
        with self.assertRaises(ValueError):
            kernel.CollectionTagSet(p.counting_store(kernel)).insert("x" * 65537)

    def test_history_replay_does_not_enumerate_every_snapshot(self):
        result = p.history_trial(128, 64)
        replay = result["baseline"]["node_gets"] + result["incremental_replay"]["node_gets"]
        self.assertLess(replay, result["repeated_snapshot_validation"]["node_gets"])

    def test_wrong_history_snapshot_fails(self):
        kernel, source, roots, operations = p.make_tag_history(16, 4)
        roots[-1] = roots[-2]
        with self.assertRaises(ValueError):
            p.verify_tag_history(kernel, source, roots, operations)

    def test_missing_committed_node_fails_without_reconstructing_it(self):
        kernel, source, roots, operations = p.make_tag_history(16, 4)
        del source.nodes[roots[1].root_sha256]
        with self.assertRaises((ValueError, kernel.CollectionTagNodeMissing)):
            p.verify_tag_history(kernel, source, roots, operations)

    def test_noop_does_not_make_new_tag_state(self):
        kernel = p.load_tag_kernel()
        store = p.counting_store(kernel)
        current = kernel.CollectionTagSet(store).insert("one")
        store.counts.clear()
        self.assertEqual(current.insert("one").root, current.root)
        self.assertEqual(store.counts["new_nodes"], 0)

    def test_source_reconciliation_is_required(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            dest = root / p.TAG_PATH
            dest.parent.mkdir(parents=True)
            dest.write_bytes((p.ROOT / p.TAG_PATH).read_bytes() + b"\n")
            with self.assertRaises(ValueError):
                p.load_tag_kernel(root)


class StructuralWalkTests(unittest.TestCase):
    def run_graph(self, graph, roots=(0,), budget=3):
        with tempfile.TemporaryDirectory() as name:
            path = Path(name) / "walk.sqlite3"
            source = p.StructuralWalk.seed(path, graph, roots)
            walk = p.StructuralWalk(path, source)
            try:
                while not walk.step(budget):
                    pass
                return walk.counts()
            finally:
                walk.close()

    def test_diamonds_visit_nodes_edges_once_and_keep_duplicate_roots(self):
        result = self.run_graph(p.diamond(64), roots=(0, 0))
        self.assertEqual(result["node_visits"], 129)
        self.assertEqual(result["edge_visits"], 254)
        self.assertEqual(result["root_selections"], 2)

    def test_high_degree_adjacency_is_paged(self):
        graph = {0: tuple(range(1, 513)), **{i: () for i in range(1, 513)}}
        result = self.run_graph(graph, budget=1)
        self.assertEqual(result["largest_step"], 1)
        self.assertEqual(result["edge_visits"], 512)

    def test_deep_history_does_not_use_python_recursion(self):
        result = self.run_graph({i: (i + 1,) for i in range(2048)} | {2048: ()}, budget=17)
        self.assertEqual(result["node_visits"], 2049)

    def test_reopen_does_not_rescan_completed_graph(self):
        uninterrupted = p.graph_trial(64)
        resumed = p.graph_trial(64, restart=True)
        for metric in ("node_visits", "edge_visits", "work_units"):
            self.assertEqual(uninterrupted[metric], resumed[metric])
        self.assertGreater(resumed["reopen_count"], 0)

    def test_cycle_is_not_misclassified_as_shared_completed_work(self):
        with self.assertRaisesRegex(ValueError, "cycle"):
            self.run_graph({0: (1,), 1: (0,)})

    def test_missing_dependency_is_not_complete(self):
        with self.assertRaisesRegex(ValueError, "missing"):
            self.run_graph({0: (1,)})

    def test_changed_selection_rejects_scratch_reuse(self):
        with tempfile.TemporaryDirectory() as name:
            path = Path(name) / "walk.sqlite3"
            p.StructuralWalk.seed(path, {0: ()}, (0,))
            with self.assertRaises(ValueError):
                p.StructuralWalk(path, "different")

    def test_empty_work_is_complete_without_fake_node(self):
        result = self.run_graph({}, roots=())
        self.assertEqual(result.get("node_visits", 0), 0)

    def test_source_fixture_rejects_nonprogressing_cursor_domain(self):
        with tempfile.TemporaryDirectory() as name:
            with self.assertRaises(ValueError):
                p.StructuralWalk.seed(Path(name) / "walk.sqlite3", {-1: ()}, (-1,))

    def test_process_exit_retains_committed_frontier_not_uncommitted_counter(self):
        with tempfile.TemporaryDirectory() as name:
            path = Path(name) / "walk.sqlite3"
            source = p.StructuralWalk.seed(path, p.diamond(16), (0,))
            code = ("import sys,os; sys.path.insert(0,sys.argv[1]); "
                    "from probe import StructuralWalk; from pathlib import Path; "
                    "w=StructuralWalk(Path(sys.argv[2]),sys.argv[3]); w.step(7); "
                    "w.db.execute(\"UPDATE counts SET value=value+10000\"); os._exit(77)")
            run = subprocess.run([sys.executable, "-c", code, str(Path(p.__file__).parent), str(path), source], check=False)
            self.assertEqual(run.returncode, 77)
            walk = p.StructuralWalk(path, source)
            try:
                while not walk.step(7):
                    pass
                self.assertEqual(walk.counts()["node_visits"], 33)
                self.assertEqual(walk.counts()["edge_visits"], 62)
            finally:
                walk.close()


class ReplicationTests(unittest.TestCase):
    def test_same_checkpoint_needs_no_suffix_walk(self):
        objects, ids = p.chain(128)
        self.assertEqual(p.missing_suffix(objects, ids[-1], ids[-1]), 0)

    def test_lag_not_total_history_controls_suffix_discovery(self):
        objects, ids = p.chain(4096)
        for lag in (1, 16, 128):
            self.assertEqual(p.missing_suffix(objects, ids[-1], ids[-lag - 1]), lag)

    def test_new_copy_requires_full_ancestry(self):
        objects, ids = p.chain(128)
        self.assertEqual(p.missing_suffix(objects, ids[-1], None), 128)

    def test_divergence_is_not_a_revision_comparison(self):
        objects, ids = p.chain(16)
        foreign, other = p.chain(16, label="other")
        with self.assertRaises(ValueError):
            p.missing_suffix(objects | foreign, ids[-1], other[-1])

    def test_corruption_is_not_a_shorter_success(self):
        objects, ids = p.chain(16)
        objects[ids[-2]] += b" "
        with self.assertRaises(ValueError):
            p.missing_suffix(objects, ids[-1], None)


class ExistingG22DiagnosticTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # This is a real dependency: absence is an error, not a substituted backend.
        cls.sha = p.load_g22()

    def test_exact_continuation_across_boundaries(self):
        result = p.continuation_trial()
        self.assertEqual(result["largest_state_octets"], 108)

    def test_state_shape_corruption_rejected(self):
        with self.assertRaises(ValueError):
            self.sha.restore(b"wrong")

    def test_shape_valid_state_is_not_authenticated_evidence(self):
        state = self.sha()
        state.update(b"x" * 128)
        saved = bytearray(state.export())
        saved[20] ^= 1
        restored = self.sha.restore(bytes(saved))
        self.assertNotEqual(restored.digest(), hashlib.sha256(b"x" * 128).digest())

    def test_finalizing_prefix_does_not_change_continuation(self):
        state = self.sha()
        state.update(b"x" * 56)
        before = state.export()
        state.digest()
        self.assertEqual(before, state.export())
        state.update(b"suffix")
        self.assertEqual(state.digest(), hashlib.sha256(b"x" * 56 + b"suffix").digest())


if __name__ == "__main__":
    unittest.main(verbosity=2)
