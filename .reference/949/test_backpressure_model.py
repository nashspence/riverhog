"""Executable body-read, retry, transient-memory and linear-work witnesses."""
from __future__ import annotations

import hashlib
import io
import json
import unittest

from backpressure_model import Budget, Destination, Part, Upload
from semantic_model import Deferred, Rejected


def part(data: bytes, number: int = 1) -> Part:
    return Part("s", number, len(data), hashlib.sha256(data).hexdigest())


class Body:
    def __init__(self, data: bytes):
        self.data = data
        self.created = 0
        self.read_calls = 0
        self.bytes_read = 0
        self.maximum_read = 0

    def factory(self):
        self.created += 1
        stream = io.BytesIO(self.data)
        owner = self
        class Counted:
            def read(self, maximum):
                owner.maximum_read = max(owner.maximum_read, maximum)
                result = stream.read(maximum)
                owner.read_calls += 1
                owner.bytes_read += len(result)
                return result
        return Counted()


class BackpressureTests(unittest.TestCase):
    def setUp(self):
        self.data = b"x" * 256
        self.source = Body(self.data)
        self.destination = Destination()
        self.budget = Budget(128, 2)
        self.upload = Upload(part(self.data), chunk_bytes=64)

    def attempt(self, **kwargs):
        return self.upload.attempt(self.destination, self.budget, self.source.factory, **kwargs)

    def test_unavailable_means_zero_application_body_consumption(self):
        self.assertEqual(self.attempt(), "waiting")
        self.assertEqual((self.source.created, self.source.bytes_read), (0, 0))
        self.assertEqual(self.destination.effects, 0)
        self.assertEqual((self.budget.used_bytes, self.budget.calls), (0, 0))

    def test_long_wait_does_not_create_payload_or_retry_history(self):
        self.attempt()
        snapshot = json.dumps(self.upload.snapshot(), sort_keys=True)
        for _ in range(1000):
            self.assertEqual(self.attempt(), "waiting")
        self.assertEqual(snapshot, json.dumps(self.upload.snapshot(), sort_keys=True))
        self.assertEqual(self.source.bytes_read, 0)
        self.assertLess(len(snapshot), 400)
        # Calls grow with poll attempts, NOT with the object's declared bytes.
        self.assertEqual(self.destination.admissions, 1001)

    def test_wait_snapshot_contains_metadata_only(self):
        self.attempt()
        restored = Upload.restore(json.loads(json.dumps(self.upload.snapshot())))
        self.destination.ready = True
        self.assertEqual(restored.attempt(self.destination, self.budget, self.source.factory), "accepted")
        self.assertEqual(self.destination.committed_bytes, 256)

    def test_memory_capacity_checked_before_source_factory(self):
        self.destination.ready = True
        self.budget.reserve(128)
        self.assertEqual(self.attempt(), "waiting")
        self.assertEqual(self.source.created, 0)
        self.assertEqual(self.budget.used_bytes, 128)
        self.budget.release(128)
        self.assertEqual(self.attempt(), "accepted")

    def test_concurrency_capacity_checked_before_source_factory(self):
        self.destination.ready = True
        self.budget.reserve(1)
        self.budget.reserve(1)
        self.assertEqual(self.attempt(), "waiting")
        self.assertEqual(self.source.created, 0)
        self.budget.release(1)
        self.budget.release(1)

    def test_preflight_can_become_stale_without_reading_body(self):
        self.destination.ready = True
        self.assertEqual(self.attempt(after_admission=lambda: setattr(self.destination, "ready", False)), "waiting")
        self.assertEqual(self.source.created, 0)

    def test_loss_after_one_chunk_stops_further_reads(self):
        self.destination.ready = True
        self.destination.after_chunk = lambda: setattr(self.destination, "ready", False)
        self.assertEqual(self.attempt(), "waiting")
        self.assertEqual(self.source.bytes_read, 64)
        self.assertIsNone(self.destination.last)
        self.assertEqual(self.budget.used_bytes, 0)

    def test_loss_before_final_receipt_is_not_success(self):
        self.destination.ready = True
        self.destination.after_chunk = lambda: setattr(
            self.destination, "ready", self.destination.attempted_bytes != len(self.data))
        self.assertEqual(self.attempt(), "waiting")
        self.assertEqual(self.source.bytes_read, 256)
        self.assertEqual(self.destination.committed_bytes, 0)

    def test_partial_wait_replays_only_current_bounded_unit(self):
        self.destination.ready = True
        self.destination.after_chunk = lambda: setattr(self.destination, "ready", False)
        self.attempt()
        self.destination.after_chunk = lambda: None
        self.destination.ready = True
        self.assertEqual(self.attempt(), "accepted")
        self.assertEqual(self.source.bytes_read, 320)
        self.assertEqual(self.destination.committed_bytes, 256)

    def test_lost_receipt_reconciles_offline_without_body(self):
        self.destination.ready = True
        self.destination.lose_reply = True
        self.assertEqual(self.attempt(), "reconcile")
        restored = Upload.restore(json.loads(json.dumps(self.upload.snapshot())))
        self.destination.ready = False
        self.assertEqual(restored.attempt(self.destination, self.budget, self.source.factory), "accepted")
        self.assertEqual(self.source.created, 1)
        self.assertEqual(self.destination.committed_bytes, 256)

    def test_identical_accepted_retry_does_not_read_again(self):
        self.destination.ready = True
        self.assertEqual(self.attempt(), "accepted")
        self.destination.ready = False
        self.assertEqual(self.attempt(), "accepted")
        self.assertEqual(self.source.bytes_read, 256)

    def test_identity_conflict_does_not_consume_new_body(self):
        self.destination.ready = True
        self.attempt()
        changed = Upload(part(b"y" * 256))
        before = self.source.bytes_read
        with self.assertRaises(Rejected):
            changed.attempt(self.destination, self.budget, self.source.factory)
        self.assertEqual(self.source.bytes_read, before)

    def test_digest_conflict_is_not_waiting(self):
        self.destination.ready = True
        bad = Body(b"z" * 256)
        with self.assertRaises(Rejected):
            self.upload.attempt(self.destination, self.budget, bad.factory)
        self.assertEqual(self.upload.phase, "failed")
        self.assertIsNone(self.destination.last)
        self.assertEqual(self.budget.used_bytes, 0)

    def test_short_source_releases_transient_reservation(self):
        self.destination.ready = True
        with self.assertRaises(Rejected):
            self.upload.attempt(self.destination, self.budget, Body(b"x").factory)
        self.assertEqual(self.budget.used_bytes, 0)

    def test_large_source_chunk_is_rejected_no_receipt(self):
        self.destination.ready = True
        class BadSource:
            def read(self, maximum):
                return b"x" * (maximum + 1)
        with self.assertRaises(Rejected):
            self.upload.attempt(self.destination, self.budget, BadSource)
        self.assertEqual(self.destination.committed_bytes, 0)

    def test_source_timeout_remains_uncertain_not_no_effect(self):
        self.destination.ready = True
        class FailedSource:
            def read(self, maximum):
                raise TimeoutError("transport")
        self.assertEqual(self.upload.attempt(self.destination, self.budget, FailedSource), "reconcile")
        self.assertEqual(self.budget.used_bytes, 0)

    def test_cancellation_releases_only_returned_local_execution(self):
        self.destination.ready = True
        class Canceled(BaseException):
            pass
        def canceled():
            raise Canceled()
        with self.assertRaises(Canceled):
            self.attempt(after_admission=canceled)
        self.assertEqual(self.budget.used_bytes, 0)
        self.assertIsNone(self.destination.last)

    def test_unit_plus_one_rejected_before_any_source(self):
        with self.assertRaises(Rejected):
            Upload(part(self.data), maximum_unit=255, chunk_bytes=64)
        self.assertEqual(self.source.created, 0)

    def test_scalar_and_encoded_identity_edges(self):
        valid = Part("s" * 128, 2**63 - 1, 1, "0" * 64)
        valid.validate(1)
        for invalid in (Part("s" * 129, 1, 1, "0" * 64),
                        Part("s", 2**63, 1, "0" * 64),
                        Part("s", True, 1, "0" * 64),
                        Part("é" * 65, 1, 1, "0" * 64)):
            with self.assertRaises(Rejected):
                invalid.validate(1)

    def test_distinct_ready_work_proceeds_while_other_waits(self):
        self.assertEqual(self.attempt(), "waiting")
        other = Destination()
        other.ready = True
        result = Upload(part(self.data)).attempt(other, self.budget, self.source.factory)
        self.assertEqual(result, "accepted")
        self.assertEqual(self.upload.phase, "waiting")

    def test_linear_growth_counts_and_peak_memory(self):
        observations = []
        for count in (64, 128, 256):
            target = Destination()
            target.ready = True
            budget = Budget(128, 1)
            source = Body(self.data)
            for number in range(1, count + 1):
                upload = Upload(part(self.data, number))
                self.assertEqual(upload.attempt(target, budget, source.factory), "accepted")
            observations.append((source.bytes_read, source.read_calls, target.lookups,
                                 target.admissions, target.effects, budget.peak_bytes))
            self.assertEqual(budget.used_bytes, 0)
        type(self).growth_evidence = observations
        for previous, current in zip(observations, observations[1:]):
            self.assertEqual(current[:5], tuple(x * 2 for x in previous[:5]))
            self.assertEqual(current[5], 128)

    def test_repeated_progress_queries_need_no_source(self):
        self.destination.ready = True
        self.attempt()
        for _ in range(1000):
            self.assertTrue(self.destination.lookup(self.upload.part))
        self.assertEqual(self.source.created, 1)

    def test_naive_body_first_order_is_a_counterexample(self):
        # Negative witness: buffering one bounded unit first is still wrong when
        # unavailability was known before admission, even though it is not disk I/O.
        source = self.source.factory()
        body = source.read(256)
        with self.assertRaises(Deferred):
            self.destination.admit()
        self.assertEqual(len(body), 256)
        self.assertGreater(self.source.bytes_read, 0)


if __name__ == "__main__":
    unittest.main()
