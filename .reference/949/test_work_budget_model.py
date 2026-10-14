"""State cardinality/byte, indexed selection and restart/saturation witnesses."""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from semantic_model import Deferred, Rejected
from work_budget_model import MAX_RECORD_BYTES, MAX_SCALAR, WorkBudget


class WorkBudgetTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "control.sqlite"
        self.store = WorkBudget(self.path, capacity=8, maximum_pins=2, quantum=4, interval=10)

    def tearDown(self):
        self.store.close()
        self.directory.cleanup()

    def fill(self):
        for ordinal in range(1, 9):
            self.store.admit(ordinal, f"object-{ordinal}")

    def settle(self, ordinal):
        self.store.finish_parent(ordinal)
        self.store.confirm_release(ordinal)

    def test_exact_capacity_and_next_identity_backpressure(self):
        self.fill()
        with self.assertRaises(Deferred):
            self.store.admit(9, "next")
        self.assertEqual(self.store.used, 8)

    def test_repeated_admission_does_not_grow_records(self):
        self.store.admit(1, "object")
        for _ in range(1000):
            self.store.admit(1, "object")
        self.assertEqual(self.store.used, 1)
        self.assertEqual(len(self.store.encoded_rows()), 1)

    def test_changed_request_rejected_without_new_record(self):
        self.store.admit(1, "object")
        with self.assertRaises(Rejected):
            self.store.admit(1, "changed")
        self.assertEqual(self.store.used, 1)

    def test_terminal_parent_does_not_free_cleanup_capacity(self):
        self.fill()
        self.store.finish_parent(1)
        self.assertEqual(self.store.collect(limit=4), 0)
        with self.assertRaises(Deferred):
            self.store.admit(9, "next")

    def test_cleanup_remains_possible_at_full_capacity(self):
        self.fill()
        self.settle(1)
        self.assertEqual(self.store.collect(limit=4), 1)
        self.store.admit(9, "next")
        self.assertEqual(self.store.used, 8)

    def test_active_pins_block_release_and_retirement(self):
        self.store.admit(1, "object")
        self.store.pin(1)
        self.store.finish_parent(1)
        with self.assertRaises(Deferred):
            self.store.confirm_release(1)
        self.assertEqual(self.store.collect(limit=4), 0)
        self.store.unpin(1)
        self.store.confirm_release(1)
        self.assertEqual(self.store.collect(limit=4), 1)

    def test_pin_bound_and_next_pin(self):
        self.store.admit(1, "object")
        self.store.pin(1)
        self.store.pin(1)
        with self.assertRaises(Deferred):
            self.store.pin(1)

    def test_retired_replay_cannot_readmit(self):
        self.store.admit(1, "object")
        self.settle(1)
        self.store.collect(limit=4)
        with self.assertRaises(Rejected):
            self.store.admit(1, "object")
        self.assertEqual(self.store.used, 0)

    def test_prefix_gap_is_not_unsafe_capacity_reclamation(self):
        self.store.admit(2, "object")
        self.settle(2)
        self.assertEqual(self.store.collect(limit=4), 0)
        self.assertEqual(self.store.retired, 0)

    def test_unsettled_low_identity_applies_explicit_backpressure(self):
        self.fill()
        for ordinal in range(2, 9):
            self.settle(ordinal)
        self.assertEqual(self.store.collect(limit=4), 0)
        with self.assertRaises(Deferred):
            self.store.admit(9, "next")
        self.assertEqual(self.store.used, 8)

    def test_retirement_quantum_is_an_actual_work_limit(self):
        self.fill()
        for ordinal in range(1, 9):
            self.settle(ordinal)
        self.assertEqual(self.store.collect(limit=3), 3)
        self.assertEqual((self.store.used, self.store.retired), (5, 3))

    def test_scheduler_quantum_and_fixed_clock_rate_bound(self):
        self.fill()
        seen = self.store.tick(0, limit=3) + self.store.tick(0, limit=3) + self.store.tick(0, limit=3)
        self.assertEqual(seen, list(range(1, 9)))
        for _ in range(100):
            self.assertEqual(self.store.tick(9, limit=4), [])
        self.assertEqual(self.store.visited, 8)
        self.assertEqual(self.store.tick(10, limit=4), [1, 2, 3, 4])

    def test_large_requested_batch_is_rejected_not_hidden(self):
        self.fill()
        with self.assertRaises(Rejected):
            self.store.tick(0, limit=5)
        with self.assertRaises(Rejected):
            self.store.collect(limit=5)

    def test_polling_keeps_one_row_not_an_attempt_history(self):
        self.store.admit(1, "object")
        for tick in range(0, 10000, 10):
            self.assertEqual(self.store.tick(tick, limit=1), [1])
        self.assertEqual(self.store.used, 1)
        self.assertLess(len(self.store.encoded_rows()[0]), MAX_RECORD_BYTES)

    def test_terminal_cleanup_is_still_scheduled(self):
        self.store.admit(1, "object")
        self.store.finish_parent(1)
        self.assertEqual(self.store.tick(0, limit=1), [1])
        self.assertEqual(self.store.used, 1)

    def test_restart_preserves_debt_capacity_pins_and_due_time(self):
        self.fill()
        self.store.pin(1)
        self.store.finish_parent(1)
        self.store.tick(0, limit=4)
        self.store.close()
        self.store = WorkBudget(self.path, capacity=8, maximum_pins=2, quantum=4, interval=10)
        self.assertEqual(self.store.used, 8)
        with self.assertRaises(Deferred):
            self.store.confirm_release(1)
        self.assertEqual(self.store.tick(1, limit=4), [5, 6, 7, 8])
        self.assertEqual(self.store.tick(1, limit=4), [])

    def test_restart_preserves_retirement_fence(self):
        self.store.admit(1, "object")
        self.settle(1)
        self.store.collect(limit=4)
        self.store.close()
        self.store = WorkBudget(self.path, capacity=8, maximum_pins=2, quantum=4, interval=10)
        with self.assertRaises(Rejected):
            self.store.admit(1, "object")

    def test_utf8_and_encoded_scalar_bounds(self):
        self.store.admit(1, "é" * 64)
        self.store.admit(2, "\\" * 128)
        for text in ("é" * 65, "s" * 129, "\x00" * 128):
            with self.assertRaises(Rejected):
                self.store.admit(3, text)
        self.assertTrue(all(len(row) <= MAX_RECORD_BYTES for row in self.store.encoded_rows()))

    def test_identity_and_clock_exhaustion_are_explicit(self):
        for ordinal in (True, 0, MAX_SCALAR + 1):
            with self.assertRaises(Rejected):
                self.store.admit(ordinal, "object")
        for tick in (-1, MAX_SCALAR, True):
            with self.assertRaises(Rejected):
                self.store.tick(tick, limit=1)
        self.assertEqual(self.store.used, 0)

    def test_churn_does_not_accumulate_exact_history(self):
        max_rows = 0
        for ordinal in range(1, 1001):
            self.store.admit(ordinal, "object")
            max_rows = max(max_rows, len(self.store.encoded_rows()))
            self.settle(ordinal)
            self.assertEqual(self.store.collect(limit=1), 1)
        type(self).churn_evidence = {"consumers": 1000, "peak_exact_records": max_rows,
                                     "retired_through": self.store.retired, "remaining": self.store.used}
        self.assertEqual((max_rows, self.store.retired, self.store.used), (1, 1000, 0))
        for ordinal in range(1, 1001):
            with self.assertRaises(Rejected):
                self.store.admit(ordinal, "object")

    def test_due_selection_uses_index_not_all_unready_records(self):
        measurements = []
        for total in (32, 512, 2048):
            db = WorkBudget(":memory:", capacity=total, quantum=4)
            try:
                for ordinal in range(1, total + 1):
                    db.admit(ordinal, "object")
                db.db.execute("UPDATE work SET due=1000")
                db.db.execute("UPDATE work SET due=0 WHERE ordinal<=4")
                db.db.commit()
                plan = db.db.execute("EXPLAIN QUERY PLAN SELECT ordinal FROM work WHERE due<=0 ORDER BY due,ordinal LIMIT 4").fetchall()
                self.assertIn("due_work", str(plan))
                steps = db.count_vm_steps(lambda: self.assertEqual(db.tick(0, limit=4), [1, 2, 3, 4]))
                measurements.append(steps)
                self.assertEqual(db.visited, 4)
            finally:
                db.close()
        # Counted SQLite VM instructions, NOT a page-I/O or B-tree-depth proof.
        type(self).selection_evidence = measurements
        self.assertLessEqual(max(measurements), min(measurements) + 32)

    def test_naive_rescan_per_page_has_quadratic_growth(self):
        # Negative witness: small output pages do not bound work spent finding them.
        def visits(total):
            return sum(total for _ in range(0, total, 4))
        self.assertEqual(visits(128), 4 * visits(64))

    def test_all_encoded_work_is_metadata_not_body(self):
        self.fill()
        self.assertTrue(all(len(row) <= MAX_RECORD_BYTES for row in self.store.encoded_rows()))
        columns = [row[1] for row in self.store.db.execute("PRAGMA table_info(work)")]
        self.assertEqual(columns, ["ordinal", "object_ref", "terminal", "released", "pins", "due"])


if __name__ == "__main__":
    unittest.main()
