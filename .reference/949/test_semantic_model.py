"""Executable semantic witnesses, not tests of Riverhog's production implementation."""
from __future__ import annotations

import itertools
import unittest

from semantic_model import (
    AdapterModel, ConsumerModel, Deferred, LostReply, ReadDemand, Rejected, disposition,
)

I = "fixture-incarnation-A"
ROUTES = {"a@1": "resource-1", "b@1": "resource-2", "c@1": "resource-1", "new@1": "resource-2"}


class SemanticTests(unittest.TestCase):
    def setUp(self) -> None:
        self.adapter = AdapterModel(I, ROUTES, ("a@1", "b@1", "c@1"))
        self.demand = ReadDemand(I, "consumer-A", "a@1")

    def begin(self) -> None:
        self.adapter.begin_write(I, "write-A", "new@1", 4, "exact-inert-identity")

    def test_read_replay_and_polling_have_no_duplicate_effects(self) -> None:
        self.adapter.prepare_read(self.demand)
        before = self.adapter.snapshot()
        for _ in range(100):
            self.adapter.prepare_read(self.demand)
            self.assertEqual(self.adapter.read_status(self.demand), "requested")
        self.assertEqual(before, self.adapter.snapshot())

    def test_same_consumer_cannot_change_exact_object(self) -> None:
        self.adapter.prepare_read(self.demand)
        with self.assertRaises(Rejected):
            self.adapter.prepare_read(ReadDemand(I, "consumer-A", "b@1"))

    def test_read_fences_cover_prepare_status_open_and_release(self) -> None:
        wrong = ReadDemand("other-incarnation", "consumer-A", "a@1")
        for action in (self.adapter.prepare_read, self.adapter.read_status, self.adapter.release_read):
            with self.subTest(action=action.__name__), self.assertRaises(Rejected):
                action(wrong)
        with self.assertRaises(Rejected):
            self.adapter.open_read(wrong, "stream")
        self.assertEqual(self.adapter.state["reads"], {})

    def test_independent_consumers_do_not_release_each_other(self) -> None:
        second = ReadDemand(I, "consumer-B", "a@1")
        self.adapter.prepare_read(self.demand)
        self.adapter.prepare_read(second)
        self.adapter.release_read(self.demand)
        self.assertTrue(self.adapter.busy("resource-1"))
        self.assertEqual(self.adapter.read_status(second), "requested")
        self.adapter.release_read(second)
        self.assertFalse(self.adapter.busy("resource-1"))

    def test_other_object_on_same_resource_blocks_quiescence(self) -> None:
        second = ReadDemand(I, "consumer-B", "c@1")
        self.adapter.prepare_read(self.demand)
        self.adapter.prepare_read(second)
        self.adapter.release_read(self.demand)
        self.assertFalse(self.adapter.quiesce("resource-1"))

    def test_release_before_prepare_fences_delayed_request(self) -> None:
        self.adapter.release_read(self.demand)
        self.adapter = AdapterModel.restore(self.adapter.snapshot())
        before = self.adapter.snapshot()
        self.adapter.prepare_read(self.demand)
        self.assertEqual(self.adapter.read_status(self.demand), "released")
        self.assertEqual(before, self.adapter.snapshot())

    def test_new_consumption_requires_fresh_consumer_identity(self) -> None:
        self.adapter.release_read(self.demand)
        self.adapter.prepare_read(ReadDemand(I, "consumer-new", "a@1"))
        self.assertTrue(self.adapter.busy("resource-1"))
        self.assertEqual(self.adapter.read_status(self.demand), "released")

    def test_release_does_not_close_an_inflight_stream(self) -> None:
        self.adapter.observe_online("resource-1")
        self.adapter.prepare_read(self.demand)
        self.adapter.open_read(self.demand, "stream-A")
        self.adapter.release_read(self.demand)
        self.assertFalse(self.adapter.quiesce("resource-1"))
        self.adapter.close_read("stream-A")
        self.adapter.close_read("stream-A")
        self.assertTrue(self.adapter.quiesce("resource-1"))

    def test_released_consumer_cannot_open_new_stream(self) -> None:
        self.adapter.observe_online("resource-1")
        self.adapter.release_read(self.demand)
        with self.assertRaises(Rejected):
            self.adapter.open_read(self.demand, "stream-A")

    def test_readiness_observation_does_not_remove_read_race(self) -> None:
        self.adapter.prepare_read(self.demand)
        self.adapter.observe_online("resource-1")
        self.assertEqual(self.adapter.read_status(self.demand), "ready")
        self.adapter.lose_resource("resource-1")
        with self.assertRaises(Deferred):
            self.adapter.open_read(self.demand, "stream-A")
        self.assertEqual(self.adapter.state["pins"], {})

    def test_new_demand_after_quiescence_waits_for_reactivation(self) -> None:
        self.adapter.observe_online("resource-1")
        self.assertTrue(self.adapter.quiesce("resource-1"))
        self.adapter.prepare_read(self.demand)
        with self.assertRaises(Deferred):
            self.adapter.open_read(self.demand, "stream-A")

    def test_restart_retains_intent_but_not_availability_assumption(self) -> None:
        self.adapter.prepare_read(self.demand)
        self.adapter.observe_online("resource-1")
        self.adapter = AdapterModel.restore(self.adapter.snapshot())
        self.assertEqual(self.adapter.read_status(self.demand), "requested")
        self.assertTrue(self.adapter.busy("resource-1"))

    def test_restart_does_not_silently_forget_stream_pins(self) -> None:
        self.adapter.prepare_read(self.demand)
        self.adapter.observe_online("resource-1")
        self.adapter.open_read(self.demand, "stream-A")
        self.adapter.release_read(self.demand)
        self.adapter = AdapterModel.restore(self.adapter.snapshot())
        self.assertFalse(self.adapter.quiesce("resource-1"))

    def test_offline_is_not_missing(self) -> None:
        self.assertTrue(self.adapter.head("a@1"))
        self.adapter.prepare_read(self.demand)
        self.assertEqual(self.adapter.read_status(self.demand), "requested")
        with self.assertRaises(Rejected):
            self.adapter.prepare_read(ReadDemand(I, "missing", "new@1"))

    def test_lost_prepare_reply_reuses_durable_caller_identity(self) -> None:
        caller = ConsumerModel(self.demand)
        with self.assertRaises(LostReply):
            caller.prepare(self.adapter, lose_reply=True)
        caller = ConsumerModel.restore(caller.snapshot())
        self.adapter = AdapterModel.restore(self.adapter.snapshot())
        caller.prepare(self.adapter)
        self.assertEqual(len(self.adapter.state["reads"]), 1)
        self.assertEqual(len(self.adapter.state["events"]), 1)

    def test_terminal_outcomes_keep_cleanup_until_acknowledged(self) -> None:
        for outcome in ("succeeded", "failed", "canceled"):
            with self.subTest(outcome=outcome):
                adapter = AdapterModel(I, ROUTES, ("a@1",))
                caller = ConsumerModel(self.demand)
                caller.prepare(adapter)
                caller.settle(outcome)
                caller = ConsumerModel.restore(caller.snapshot())
                self.assertTrue(caller.state["cleanup_pending"])
                with self.assertRaises(LostReply):
                    caller.cleanup(adapter, lose_reply=True)
                caller = ConsumerModel.restore(caller.snapshot())
                adapter = AdapterModel.restore(adapter.snapshot())
                self.assertTrue(caller.state["cleanup_pending"])
                caller.cleanup(adapter)
                caller.cleanup(adapter)
                self.assertFalse(caller.state["cleanup_pending"])
                self.assertEqual(caller.state["outcome"], outcome)
                self.assertFalse(adapter.busy("resource-1"))

    def test_cancel_before_first_prepare_still_fences_late_effect(self) -> None:
        caller = ConsumerModel(self.demand)
        caller.settle("canceled")
        caller.cleanup(self.adapter)
        self.adapter.prepare_read(self.demand)  # Delayed request, not a new demand.
        self.assertEqual(self.adapter.read_status(self.demand), "released")
        with self.assertRaises(Rejected):
            caller.prepare(self.adapter)

    def test_pending_consumer_cannot_cleanup(self) -> None:
        with self.assertRaises(Rejected):
            ConsumerModel(self.demand).cleanup(self.adapter)

    def test_changed_incarnation_does_not_discard_cleanup_obligation(self) -> None:
        caller = ConsumerModel(self.demand)
        caller.settle("canceled")
        replacement = AdapterModel("replacement", ROUTES, ("a@1",))
        with self.assertRaises(Rejected):
            caller.cleanup(replacement)
        self.assertTrue(caller.state["cleanup_pending"])
        self.assertEqual(replacement.state["reads"], {})

    def test_terminal_outcome_cannot_be_changed(self) -> None:
        caller = ConsumerModel(self.demand)
        caller.settle("succeeded")
        caller.settle("succeeded")
        with self.assertRaises(Rejected):
            caller.settle("canceled")

    def test_aggregate_readiness_counterexample_for_one_resource_slot(self) -> None:
        demands = (self.demand, ReadDemand(I, "consumer-B", "b@1"))
        for demand in demands:
            self.adapter.prepare_read(demand)
        for online in (set(), {"resource-1"}, {"resource-2"}):
            self.adapter.online = online
            self.assertFalse(all(self.adapter.read_status(d) == "ready" for d in demands))

    def test_one_object_window_makes_progress_with_one_resource_slot(self) -> None:
        consumed = []
        for index, key in enumerate(("a@1", "b@1", "c@1")):
            resource = ROUTES[key]
            caller = ConsumerModel(ReadDemand(I, f"consumer-{index}", key))
            caller.prepare(self.adapter)
            self.adapter.observe_online(resource)
            self.assertEqual(len(self.adapter.online), 1)
            self.adapter.open_read(caller.demand, "stream")
            # Simulate verified ciphertext cache commit, not consumer delivery.
            consumed.append(key)
            self.adapter.close_read("stream")
            caller.settle("succeeded")
            caller.cleanup(self.adapter)
            self.assertTrue(self.adapter.quiesce(resource))
        self.assertEqual(consumed, ["a@1", "b@1", "c@1"])

    def test_waiting_write_is_persisted_before_accepting_bytes(self) -> None:
        self.begin()
        self.adapter = AdapterModel.restore(self.adapter.snapshot())
        self.begin()
        self.assertEqual(self.adapter.write_status(I, "write-A"), "requested")
        with self.assertRaises(Deferred):
            self.adapter.segment(I, "write-A", 1, 4, "digest")
        self.assertEqual(self.adapter.state["writes"]["write-A"]["segments"], {})

    def test_write_replay_rejects_changed_identity(self) -> None:
        self.begin()
        with self.assertRaises(Rejected):
            self.adapter.begin_write(I, "write-A", "new@1", 5, "changed")
        with self.assertRaises(Rejected):
            self.adapter.begin_write(I, "write-B", "new@1", 4, "other")

    def test_write_fences_cover_every_continuation(self) -> None:
        self.begin()
        for action in (
            lambda: self.adapter.write_status("other", "write-A"),
            lambda: self.adapter.segment("other", "write-A", 1, 4, "digest"),
            lambda: self.adapter.complete("other", "write-A"),
            lambda: self.adapter.abort("other", "write-A"),
        ):
            with self.assertRaises(Rejected):
                action()

    def test_write_readiness_does_not_remove_segment_race(self) -> None:
        self.begin()
        self.adapter.observe_online("resource-2")
        self.assertEqual(self.adapter.write_status(I, "write-A"), "ready")
        self.adapter.lose_resource("resource-2")
        with self.assertRaises(Deferred):
            self.adapter.segment(I, "write-A", 1, 4, "digest")

    def test_accepted_segment_reconciles_offline_without_duplicate_bytes(self) -> None:
        self.begin()
        self.adapter.observe_online("resource-2")
        self.adapter.segment(I, "write-A", 1, 4, "digest")
        self.adapter = AdapterModel.restore(self.adapter.snapshot())
        self.adapter.segment(I, "write-A", 1, 4, "digest")
        self.assertEqual(len(self.adapter.state["writes"]["write-A"]["segments"]), 1)
        with self.assertRaises(Rejected):
            self.adapter.segment(I, "write-A", 1, 4, "different-digest")

    def test_completion_wait_is_not_premature_durability_receipt(self) -> None:
        self.begin()
        self.adapter.observe_online("resource-2")
        self.adapter.segment(I, "write-A", 1, 4, "digest")
        self.adapter.lose_resource("resource-2")
        with self.assertRaises(Deferred):
            self.adapter.complete(I, "write-A")
        self.assertFalse(self.adapter.head("new@1"))
        self.assertEqual(self.adapter.state["writes"]["write-A"]["state"], "active")
        self.adapter.observe_online("resource-2")
        self.adapter.complete(I, "write-A")
        self.adapter = AdapterModel.restore(self.adapter.snapshot())
        self.adapter.complete(I, "write-A")  # Reconcile a lost completion reply offline.
        self.assertTrue(self.adapter.head("new@1"))
        self.assertEqual(self.adapter.write_status(I, "write-A"), "completed")

    def test_invalid_completion_is_not_classified_as_wait(self) -> None:
        self.begin()
        with self.assertRaises(Rejected) as context:
            self.adapter.complete(I, "write-A")
        self.assertEqual(disposition(context.exception), "fail")

    def test_write_length_and_contiguous_segment_invariants(self) -> None:
        self.begin()
        self.adapter.observe_online("resource-2")
        with self.assertRaises(Rejected):
            self.adapter.segment(I, "write-A", 1, 5, "digest")
        self.adapter.segment(I, "write-A", 2, 4, "digest")
        with self.assertRaises(Rejected):
            self.adapter.complete(I, "write-A")

    def test_active_write_blocks_resource_quiescence(self) -> None:
        self.begin()
        self.assertFalse(self.adapter.quiesce("resource-2"))
        self.adapter.abort(I, "write-A")  # Empty write can be released while offline.
        self.assertTrue(self.adapter.quiesce("resource-2"))
        with self.assertRaises(Rejected):
            self.adapter.segment(I, "write-A", 1, 4, "digest")

    def test_abort_retains_cleanup_until_required_media_return(self) -> None:
        self.begin()
        self.adapter.observe_online("resource-2")
        self.adapter.segment(I, "write-A", 1, 4, "digest")
        self.adapter.lose_resource("resource-2")
        with self.assertRaises(Deferred):
            self.adapter.abort(I, "write-A")
        self.adapter = AdapterModel.restore(self.adapter.snapshot())
        self.assertEqual(self.adapter.write_status(I, "write-A"), "aborting")
        self.assertFalse(self.adapter.quiesce("resource-2"))
        with self.assertRaises(Rejected):
            self.adapter.segment(I, "write-A", 2, 1, "digest")
        self.adapter.observe_online("resource-2")
        self.adapter.abort(I, "write-A")
        self.adapter.abort(I, "write-A")
        self.assertEqual(self.adapter.write_status(I, "write-A"), "aborted")

    def test_offline_delete_does_not_pretend_bytes_were_reclaimed(self) -> None:
        with self.assertRaises(Deferred):
            self.adapter.delete(I, "a@1")
        self.assertTrue(self.adapter.head("a@1"))
        self.adapter.observe_online("resource-1")
        self.adapter.delete(I, "a@1")
        self.adapter.lose_resource("resource-1")
        self.adapter.delete(I, "a@1")  # Already-applied effect is replayable.
        self.assertFalse(self.adapter.head("a@1"))

    def test_delete_does_not_cross_live_consumption(self) -> None:
        self.adapter.prepare_read(self.demand)
        self.adapter.observe_online("resource-1")
        with self.assertRaises(Deferred):
            self.adapter.delete(I, "a@1")
        self.assertTrue(self.adapter.head("a@1"))

    def test_known_wait_ambiguous_effect_and_failure_are_distinct(self) -> None:
        self.assertEqual(disposition(Deferred()), "wait")
        self.assertEqual(disposition(LostReply()), "reconcile")
        self.assertEqual(disposition(TimeoutError()), "reconcile")
        self.assertEqual(disposition(Rejected()), "fail")
        self.assertEqual(disposition(RuntimeError()), "fail")

    def test_all_180_release_replay_interleavings_survive_restarts(self) -> None:
        schedules = set(itertools.permutations(("PA", "PA", "RA", "PB", "RB", "RB")))
        self.assertEqual(len(schedules), 180)
        for schedule in sorted(schedules):
            with self.subTest(schedule=schedule):
                adapter = AdapterModel(I, ROUTES, ("a@1",))
                for action in schedule:
                    demand = ReadDemand(I, "consumer-" + action[1], "a@1")
                    if action[0] == "P":
                        adapter.prepare_read(demand)
                    else:
                        adapter.release_read(demand)
                    adapter = AdapterModel.restore(adapter.snapshot())
                self.assertFalse(adapter.busy("resource-1"))
                self.assertLessEqual(len(adapter.state["events"]), 4)


if __name__ == "__main__":
    unittest.main()
