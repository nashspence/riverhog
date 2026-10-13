# SPDX-License-Identifier: Apache-2.0
"""Finite diagnostic challenges, NOT production/asymptotic conformance proof."""
from __future__ import annotations

import hashlib
import inspect
import sys
import unittest
from pathlib import Path

import basis as b

REFERENCE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REFERENCE / "growth"))
import probe as old_probe
sys.path.insert(0, str(REFERENCE))
from annotation_reference import Copy


def edit(**overrides):
    args = dict(authored_octets=16, tag=False, path=33, node_octets=131072,
                control_octets=128, grants=0, grant_octets=0, scheduled_copies=0,
                population=64, key_width=32)
    args.update(overrides)
    return b.edit(**args)


class ArithmeticTests(unittest.TestCase):
    def test_exact_logarithm_boundary_and_large_integers(self):
        for n in (0, 1, 2, 3, 6, 7, 14, 15, 2**63 - 1, 2**1000):
            self.assertLess(2 ** (b.ell(n) - 1), n + 2)
            self.assertGreaterEqual(2 ** b.ell(n), n + 2)

    def test_invalid_dimensions_do_not_coerce_bool_float_or_negative(self):
        for invalid in (True, -1, 1.0, "1"):
            with self.assertRaises(ValueError):
                b.ell(invalid)
            with self.assertRaises(ValueError):
                edit(authored_octets=invalid)

    def test_native_edit_has_no_history_member_or_tag_count_dimension(self):
        names = inspect.signature(b.edit).parameters
        for forbidden in ("history", "members", "tag_count", "validation_cost"):
            self.assertNotIn(forbidden, names)

    def test_description_does_not_pay_for_tag_node_envelope(self):
        self.assertEqual(edit(), edit(path=3300, node_octets=13107200))

    def test_tag_change_counts_existing_node_bytes_not_just_operand(self):
        self.assertEqual(edit(tag=True).byte_visits - edit().byte_visits, 33 * 131072)
        self.assertEqual(edit(tag=True).index_operations - edit().index_operations, 33)

    def test_grants_and_copy_scheduling_are_independent(self):
        a = edit(grants=100, grant_octets=6400)
        c = edit(scheduled_copies=100)
        self.assertEqual(a.index_operations, 101)
        self.assertEqual(c.index_operations, 101)
        self.assertNotEqual(a.byte_visits, c.byte_visits)

    def test_population_changes_index_work_not_native_byte_visits(self):
        small, large = edit(population=16), edit(population=2**40)
        self.assertEqual(small.byte_visits, large.byte_visits)
        self.assertEqual(small.index_operations, large.index_operations)
        self.assertGreater(large.key_comparisons, small.key_comparisons)

    def test_variable_key_width_is_not_free(self):
        a, z = edit(key_width=32), edit(key_width=65536)
        self.assertEqual(a.key_comparisons, z.key_comparisons)
        self.assertEqual(z.comparison_octets, a.comparison_octets * 2048)

    def test_sequence_is_sum_of_attempts_not_final_committed_history(self):
        each = edit(tag=True, grants=3, grant_octets=80, scheduled_copies=2)
        total = b.attempts(request_octets=1600, attempt_count=100, tag_attempts=100,
                           path=33, node_octets=131072, control_octets=128,
                           total_grants=300, total_grant_octets=8000,
                           copy_scheduling_visits=200, population=64, key_width=32)
        for key in vars(each):
            self.assertEqual(getattr(total, key), 100 * getattr(each, key))

    def test_many_noops_have_nonzero_cost_without_creating_history(self):
        a = b.attempts(request_octets=1000, attempt_count=1000, tag_attempts=0,
                       path=33, node_octets=131072, control_octets=128,
                       total_grants=0, total_grant_octets=0, copy_scheduling_visits=0,
                       population=1000, key_width=32)
        self.assertEqual(a.index_operations, 1000)
        self.assertGreater(a.byte_visits, 1000)

    def test_subset_dimensions_fail_closed(self):
        with self.assertRaises(ValueError):
            b.retained(initial_tags=0, initial_octets=0, changes=1, tag_changes=2,
                       authored_octets=1, path=33, node_octets=131072, control_octets=128)

    def test_retained_storage_is_additive_not_initial_tags_times_history(self):
        def size(tags, history):
            return b.retained(initial_tags=tags, initial_octets=tags * 64,
                              changes=history, tag_changes=0, authored_octets=history * 10,
                              path=33, node_octets=131072, control_octets=128)[1]
        self.assertEqual(size(1000, 5000) - size(1000, 10),
                         size(10, 5000) - size(10, 10))

    def test_recovery_retains_semantic_selections_not_only_physical_objects(self):
        def value(selections):
            return b.recovery(selected_octets=1000, objects=10, edges=20,
                              selections=selections, changes=5, tag_changes=2,
                              path=33, node_octets=131072, output_octets=100,
                              population=1000, key_width=32)
        a, z = value(1), value(1001)
        self.assertEqual(a.byte_visits, z.byte_visits)
        self.assertEqual(z.index_operations - a.index_operations, 1000)

    def test_restart_budgets_do_not_mix_bytes_and_visits(self):
        vector, visits = b.restart(interruptions=7, byte_budget=65536,
                                   visit_budget=3, index_budget=4,
                                   population=1000, key_width=32)
        self.assertEqual(vector.byte_visits, 7 * 65536)
        self.assertEqual(visits, 21)
        self.assertEqual(vector.index_operations, 35)

    def test_zero_interruptions_add_no_replay_work(self):
        vector, visits = b.restart(interruptions=0, byte_budget=65536,
                                   visit_budget=3, index_budget=4,
                                   population=1000, key_width=32)
        self.assertEqual(vector.byte_visits + vector.index_operations + visits, 0)


class PrimitiveWitnessTests(unittest.TestCase):
    def test_short_edit_can_copy_large_old_leaf(self):
        kernel = old_probe.load_tag_kernel()
        store = old_probe.counting_store(kernel)
        long_tag = "L" * 65536
        old = kernel.CollectionTagSet(store).insert(long_tag)
        store.counts.clear()
        two = old.insert("x")
        insertion = dict(store.counts)
        store.counts.clear()
        restored = two.discard("x")
        removal = dict(store.counts)
        self.assertEqual(restored.root, old.root)
        self.assertGreater(insertion["node_put_octets"], 65536)
        self.assertGreater(removal["node_put_octets"], 65536)
        self.assertLessEqual(insertion["node_gets"], 33)
        self.assertLessEqual(insertion["node_puts"], 35)
        self.assertLessEqual(removal["node_gets"], 66)
        self.assertLessEqual(removal["node_puts"], 33)

    def test_native_path_and_byte_envelopes_across_independent_sizes(self):
        kernel = old_probe.load_tag_kernel()
        u = kernel.COLLECTION_TAG_NODE_BYTES_MAX
        for count in (0, 1, 32, 256):
            store = old_probe.counting_store(kernel)
            tree = kernel.CollectionTagSet(store)
            for i in range(count):
                tree = tree.insert(f"initial-{i}")
            for operand in ("x", "y" * 65536):
                store.counts.clear()
                edited = tree.insert(operand)
                self.assertLessEqual(store.counts["node_gets"], 33)
                self.assertLessEqual(store.counts["node_puts"], 35)
                self.assertLessEqual(store.counts["node_put_octets"], 35 * u)
                self.assertEqual(set(edited.iter_tags()), set(tree.iter_tags()) | {operand})

    def test_streamed_prefix_identity_and_actual_hash_input(self):
        chunks = [b"", b"a", b"b" * 63, b"c" * 64, b"d" * 65, b""]
        state = hashlib.sha256()
        prefix = bytearray()
        counted = 0
        for chunk in chunks:
            state.update(chunk)
            counted += len(chunk)
            prefix.extend(chunk)
            self.assertEqual(state.copy().digest(), hashlib.sha256(prefix).digest())
        self.assertEqual(counted, sum(map(len, chunks)))

    def test_fresh_native_annotation_profile_is_not_general_graph_insertion(self):
        source = Copy("a" * 64)
        source.mutate("init", "initial")
        source.mutate("tag", "tag_add", "reviewed")
        source.mutate("description", "description_set", "Context only")
        from annotation_reference import parse_frame
        frames = [parse_frame(raw) for raw in source.recover().frames]
        for row in frames:
            groups = row["body"]["assertions"]
            self.assertLessEqual(set(groups), {"agents", "contexts", "activities"})
            self.assertEqual(len(groups["activities"]), 1)
            self.assertEqual(groups["activities"][0]["kind"], "metadata_update")
            self.assertNotEqual(row["entry_kind"], "correction")
        # Shape evidence only: full core runtime/schema qualification is separate.
        self.assertEqual(len(frames), 3)

    def test_standalone_output_expansion_is_counted_sink_bytes(self):
        blocks = [b"history" * 13, b"provenance" * 7]
        shared = sum(map(len, blocks))
        source_verified = sum(map(len, blocks))
        class Sink:
            octets = 0

            def write(self, block):
                self.octets += len(block)
                return len(block)

        sink = Sink()
        for _ in range(7):
            for block in blocks:
                self.assertEqual(sink.write(block), len(block))
        self.assertEqual(source_verified, shared)
        self.assertEqual(sink.octets, 7 * shared)


if __name__ == "__main__":
    unittest.main(verbosity=2)
