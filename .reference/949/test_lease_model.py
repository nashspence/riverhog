"""Executable finite-lease/replay witnesses; NOT production integration tests."""
from __future__ import annotations

import itertools
import unittest
from dataclasses import replace

from lease_model import Consumption, LeaseAdapterModel, LeaseConsumerModel, Retired
from semantic_model import AdapterModel, Deferred, LostReply, ReadDemand, Rejected

I = "fixture-incarnation"
ROUTES = {"a@1": "resource-A", "a@2": "resource-A", "b@1": "resource-B"}


class LeaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.adapter = LeaseAdapterModel(I, ROUTES, ("caller-A", "caller-B"), window=4)
        self.identity = Consumption(I, "caller-A", 1, "a@1")

    def restart(self) -> None:
        self.adapter = LeaseAdapterModel.restore(self.adapter.snapshot())

    def records(self) -> dict:
        return self.adapter.state["scopes"]["caller-A"]["records"]

    def test_offline_prepare_is_finite_and_replay_does_not_renew(self) -> None:
        first = self.adapter.prepare(self.identity, 10)
        self.adapter.advance_to(5)
        self.assertEqual(self.adapter.prepare(self.identity, 10), first)
        self.assertEqual(first.expires_at, 10)
        self.assertEqual(len(self.records()), 1)

    def test_invalid_lease_bounds_are_rejected(self) -> None:
        for duration in (0, -1, 31, True, 1.5, None):
            with self.subTest(duration=duration), self.assertRaises(Rejected):
                self.adapter.prepare(self.identity, duration)
        self.assertFalse(self.records())

    def test_exact_prepare_identity_includes_parameters_and_revision(self) -> None:
        self.adapter.prepare(self.identity, 10)
        for identity, seconds in ((self.identity, 11), (replace(self.identity, object_key="a@2"), 10)):
            with self.subTest(identity=identity, seconds=seconds), self.assertRaises(Rejected):
                self.adapter.prepare(identity, seconds)
        self.assertEqual(self.adapter.status(self.identity).expires_at, 10)

    def test_namespaces_do_not_share_ownership_or_retirement(self) -> None:
        other = replace(self.identity, namespace="caller-B")
        self.adapter.prepare(self.identity, 10)
        self.adapter.prepare(other, 10)
        self.adapter.release(self.identity)
        self.adapter.collect("caller-A", limit=4)
        self.assertEqual(self.adapter.status(other).phase, "active")
        self.assertTrue(self.adapter.busy("resource-A"))

    def test_incarnation_mismatch_never_consumes_cleanup_or_old_work(self) -> None:
        wrong = replace(self.identity, incarnation="other")
        actions = (
            lambda: self.adapter.prepare(wrong, 10), lambda: self.adapter.status(wrong),
            lambda: self.adapter.renew(wrong, 0, 20), lambda: self.adapter.release(wrong),
            lambda: self.adapter.open_read(wrong, 1),
            lambda: self.adapter.confirm_stream_closed(wrong, 1),
        )
        for action in actions:
            with self.assertRaises(Rejected):
                action()
        self.assertFalse(self.records())

    def test_unknown_scopes_and_invalid_ordinals_are_not_auto_admitted(self) -> None:
        for identity in (
            replace(self.identity, namespace="unregistered"), replace(self.identity, ordinal=0),
            replace(self.identity, ordinal=True), replace(self.identity, ordinal=2**63),
        ):
            with self.subTest(identity=identity), self.assertRaises(Rejected):
                self.adapter.prepare(identity, 10)

    def test_exact_expiry_blocks_new_prepares_renewals_and_streams(self) -> None:
        self.adapter.prepare(self.identity, 10)
        self.adapter.online.add("resource-A")
        self.adapter.advance_to(10)
        for action in (
            lambda: self.adapter.prepare(self.identity, 10),
            lambda: self.adapter.renew(self.identity, 0, 20),
            lambda: self.adapter.open_read(self.identity, 1),
        ):
            with self.assertRaises(Retired):
                action()
        self.assertEqual(self.adapter.status(self.identity).phase, "expired")

    def test_expired_ownership_requires_fresh_identity_for_reacquisition(self) -> None:
        self.adapter.prepare(self.identity, 10)
        self.adapter.advance_to(11)
        fresh = replace(self.identity, ordinal=2)
        self.adapter.prepare(fresh, 10)
        self.adapter.release(self.identity)
        self.assertEqual(self.adapter.status(fresh).phase, "active")

    def test_renewal_lost_reply_replays_without_moving_the_deadline(self) -> None:
        caller = LeaseConsumerModel(self.identity, 10)
        caller.prepare(self.adapter)
        self.adapter.advance_to(5)
        with self.assertRaises(LostReply):
            caller.renew(self.adapter, 20, lose_reply=True)
        self.restart()
        caller = LeaseConsumerModel.restore(caller.snapshot())
        self.adapter.advance_to(6)
        caller.renew(self.adapter, 20)
        self.assertEqual(caller.state["lease"]["expires_at"], 20)
        self.assertEqual(caller.state["lease"]["revision"], 1)
        self.assertIsNone(caller.state["pending_renewal"])

    def test_changed_pending_renewal_must_be_reconciled(self) -> None:
        caller = LeaseConsumerModel(self.identity, 10)
        caller.prepare(self.adapter)
        with self.assertRaises(LostReply):
            caller.renew(self.adapter, 20, lose_reply=True)
        with self.assertRaises(Rejected):
            caller.renew(self.adapter, 21)
        self.assertEqual(self.adapter.status(self.identity).expires_at, 20)

    def test_stale_or_competing_renewal_cannot_change_newer_grant(self) -> None:
        self.adapter.prepare(self.identity, 10)
        self.adapter.renew(self.identity, 0, 20)
        for revision, until in ((0, 21), (2, 25), (True, 25)):
            with self.subTest(revision=revision, until=until), self.assertRaises(Rejected):
                self.adapter.renew(self.identity, revision, until)
        self.adapter.advance_to(5)
        self.adapter.renew(self.identity, 1, 30)
        with self.assertRaises(Rejected):
            self.adapter.renew(self.identity, 0, 20)
        self.assertEqual(self.adapter.status(self.identity).expires_at, 30)

    def test_renewal_cannot_shorten_or_create_infinite_lease(self) -> None:
        self.adapter.prepare(self.identity, 10)
        for until in (10, 9, 31, float("inf"), True):
            with self.subTest(until=until), self.assertRaises(Rejected):
                self.adapter.renew(self.identity, 0, until)

    def test_polling_and_exact_replays_never_renew(self) -> None:
        self.adapter.prepare(self.identity, 10)
        for now in range(10):
            self.adapter.advance_to(now)
            self.adapter.prepare(self.identity, 10)
            self.assertEqual(self.adapter.status(self.identity).expires_at, 10)
        self.adapter.advance_to(10)
        self.assertEqual(self.adapter.status(self.identity).phase, "expired")

    def test_long_operator_wait_uses_bounded_renewals_not_failure(self) -> None:
        self.adapter.prepare(self.identity, 10)
        for number in range(1, 101):
            self.adapter.advance_to(number * 5)
            grant = self.adapter.renew(self.identity, number - 1, number * 5 + 10)
            self.assertEqual(grant.phase, "active")
            self.assertEqual(len(self.records()), 1)
            self.assertEqual(len(self.records()["1"]["last_renewal"]), 2)
        self.assertFalse(self.adapter.online)

    def test_release_before_prepare_survives_reclamation_and_restart(self) -> None:
        self.adapter.release(self.identity)
        self.assertEqual(self.adapter.collect("caller-A", limit=1), 1)
        self.restart()
        self.assertFalse(self.records())
        with self.assertRaises(Retired):
            self.adapter.prepare(self.identity, 10)
        self.assertEqual(self.adapter.status(self.identity).phase, "retired")

    def test_naive_tombstone_deletion_counterexample_recreates_old_demand(self) -> None:
        # Deliberately UNSAFE mutation of the initial witness: forget the release
        # without retaining a watermark or another rejecting validity rule.
        old = AdapterModel(I, ROUTES, ("a@1",))
        demand = ReadDemand(I, "old-consumer", "a@1")
        old.release_read(demand)
        del old.state["reads"][demand.consumer]
        old.prepare_read(demand)
        self.assertTrue(old.busy("resource-A"))  # The safety failure is observable.
        # The revised witness removes exact rows, but cannot repeat that failure.
        self.adapter.release(self.identity)
        self.adapter.collect("caller-A", limit=1)
        with self.assertRaises(Retired):
            self.adapter.prepare(self.identity, 10)
        self.assertFalse(self.adapter.busy("resource-A"))

    def test_released_identity_rejects_delayed_renewal_before_and_after_gc(self) -> None:
        self.adapter.prepare(self.identity, 10)
        self.adapter.release(self.identity)
        for collect in (False, True):
            if collect:
                self.adapter.collect("caller-A", limit=4)
            with self.assertRaises(Retired):
                self.adapter.renew(self.identity, 0, 20)

    def test_collected_status_is_retirement_evidence_not_historical_receipt(self) -> None:
        self.adapter.release(self.identity)  # Never prepared at all.
        self.adapter.collect("caller-A", limit=1)
        status = self.adapter.status(self.identity)
        self.assertEqual(status.phase, "retired")
        self.assertIsNone(status.expires_at)
        self.assertIsNone(status.revision)
        # Even a different payload cannot use the retired sequence to create work.
        with self.assertRaises(Retired):
            self.adapter.prepare(replace(self.identity, object_key="a@2"), 10)

    def test_reclamation_cannot_skip_unobserved_prepare_gap(self) -> None:
        second = replace(self.identity, ordinal=2)
        self.adapter.release(second)
        self.assertEqual(self.adapter.collect("caller-A", limit=4), 0)
        self.adapter.prepare(self.identity, 10)  # Valid delayed message.
        self.assertEqual(self.adapter.collect("caller-A", limit=4), 0)
        self.adapter.release(self.identity)
        self.assertEqual(self.adapter.collect("caller-A", limit=4), 2)

    def test_bounded_window_backpressures_instead_of_forgetting_live_history(self) -> None:
        self.adapter.prepare(self.identity, 10)
        for ordinal in range(2, 5):
            self.adapter.release(replace(self.identity, ordinal=ordinal))
        with self.assertRaises(Deferred):
            self.adapter.prepare(replace(self.identity, ordinal=5), 10)
        self.assertEqual(len(self.records()), 4)
        self.assertEqual(self.adapter.collect("caller-A", limit=4), 0)
        self.adapter.advance_to(10)
        self.assertEqual(self.adapter.collect("caller-A", limit=4), 4)
        self.adapter.prepare(replace(self.identity, ordinal=5), 10)

    def test_release_of_far_future_unknown_identity_does_not_grow_state(self) -> None:
        with self.assertRaises(Deferred):
            self.adapter.release(replace(self.identity, ordinal=1_000_000))
        self.assertFalse(self.records())

    def test_bounded_collector_limits_each_step(self) -> None:
        for ordinal in range(1, 5):
            self.adapter.release(replace(self.identity, ordinal=ordinal))
        self.assertEqual(self.adapter.collect("caller-A", limit=2), 2)
        self.assertEqual(len(self.records()), 2)
        self.assertEqual(self.adapter.collect("caller-A", limit=1), 1)

    def test_thousand_retirements_keep_constant_record_cardinality(self) -> None:
        peak = 0
        for ordinal in range(1, 1001):
            identity = replace(self.identity, ordinal=ordinal)
            self.adapter.prepare(identity, 10)
            self.adapter.release(identity)
            peak = max(peak, len(self.records()))
            self.assertEqual(self.adapter.collect("caller-A", limit=1), 1)
            self.restart()
        self.assertEqual(peak, 1)
        self.assertEqual(self.adapter.state["scopes"]["caller-A"]["retired_through"], 1000)
        for ordinal in range(1, 1001):
            with self.assertRaises(Retired):
                self.adapter.prepare(replace(self.identity, ordinal=ordinal), 10)
        self.assertFalse(self.records())

    def test_expiry_and_gc_never_drop_an_active_stream_pin(self) -> None:
        self.adapter.prepare(self.identity, 10)
        self.adapter.online.add("resource-A")
        self.adapter.open_read(self.identity, 1)
        self.adapter.advance_to(100)
        self.assertEqual(self.adapter.collect("caller-A", limit=4), 0)
        self.assertFalse(self.adapter.quiesce("resource-A"))
        with self.assertRaises(Deferred):
            self.adapter.release(self.identity)
        self.restart()
        self.assertFalse(self.adapter.quiesce("resource-A"))
        self.adapter.confirm_stream_closed(self.identity, 1)
        self.adapter.release(self.identity)
        self.assertEqual(self.adapter.collect("caller-A", limit=1), 1)
        self.assertTrue(self.adapter.quiesce("resource-A"))

    def test_restart_is_not_stream_cessation_or_readiness_evidence(self) -> None:
        self.adapter.prepare(self.identity, 10)
        self.adapter.online.add("resource-A")
        self.adapter.open_read(self.identity, 1)
        self.restart()
        self.assertFalse(self.adapter.online)
        self.assertTrue(self.adapter.busy("resource-A"))
        self.assertEqual(self.records()["1"]["pins"], [1])

    def test_stream_admission_is_bounded_and_cannot_replay_after_close(self) -> None:
        self.adapter.prepare(self.identity, 10)
        self.adapter.online.add("resource-A")
        self.adapter.open_read(self.identity, 1)
        self.adapter.open_read(self.identity, 2)
        with self.assertRaises(Deferred):
            self.adapter.open_read(self.identity, 3)
        with self.assertRaises(Rejected):
            self.adapter.open_read(self.identity, 2)  # Duplicate I/O is not a retry.
        self.adapter.confirm_stream_closed(self.identity, 1)
        with self.assertRaises(Retired):
            self.adapter.open_read(self.identity, 1)
        self.adapter.open_read(self.identity, 3)
        self.adapter.confirm_stream_closed(self.identity, 1)  # Delayed close.
        self.assertEqual(self.records()["1"]["pins"], [2, 3])

    def test_readiness_loss_at_open_does_not_admit_stream(self) -> None:
        self.adapter.prepare(self.identity, 10)
        self.adapter.online.add("resource-A")
        self.assertEqual(self.adapter.status(self.identity).phase, "active")
        self.adapter.online.clear()
        with self.assertRaises(Deferred):
            self.adapter.open_read(self.identity, 1)
        self.assertEqual(self.records()["1"]["pins"], [])

    def test_expired_cleanup_effect_remains_unsettled_while_offline(self) -> None:
        self.adapter.prepare(self.identity, 10)
        self.adapter.record_cleanup_effect(self.identity)
        self.adapter.advance_to(10)
        with self.assertRaises(Deferred):
            self.adapter.release(self.identity)
        with self.assertRaises(Deferred):
            self.adapter.perform_cleanup(self.identity)
        self.assertEqual(self.adapter.collect("caller-A", limit=4), 0)
        self.assertFalse(self.adapter.quiesce("resource-A"))
        self.adapter.online.add("resource-A")
        self.adapter.perform_cleanup(self.identity)
        self.adapter.release(self.identity)
        self.assertEqual(self.adapter.collect("caller-A", limit=4), 1)

    def test_cleanup_waits_for_stream_fencing_even_when_provider_available(self) -> None:
        self.adapter.prepare(self.identity, 10)
        self.adapter.record_cleanup_effect(self.identity)
        self.adapter.online.add("resource-A")
        self.adapter.open_read(self.identity, 1)
        with self.assertRaises(Deferred):
            self.adapter.release(self.identity)
        with self.assertRaises(Deferred):
            self.adapter.perform_cleanup(self.identity)
        self.adapter.confirm_stream_closed(self.identity, 1)
        self.adapter.perform_cleanup(self.identity)
        self.adapter.release(self.identity)

    def test_lost_prepare_reply_has_preexisting_durable_cleanup_obligation(self) -> None:
        caller = LeaseConsumerModel(self.identity, 10)
        before_call = caller.snapshot()
        with self.assertRaises(LostReply):
            caller.prepare(self.adapter, lose_reply=True)
        caller = LeaseConsumerModel.restore(before_call)
        self.assertTrue(caller.state["cleanup_pending"])
        caller.settle("canceled")
        caller.cleanup(self.adapter)
        self.assertFalse(caller.state["cleanup_pending"])

    def test_all_terminal_outcomes_retain_cleanup_through_lost_reply_and_gc(self) -> None:
        for outcome in ("succeeded", "failed", "canceled", "expired"):
            with self.subTest(outcome=outcome):
                adapter = LeaseAdapterModel(I, ROUTES, ("caller-A",))
                caller = LeaseConsumerModel(self.identity, 10)
                caller.prepare(adapter)
                caller.settle(outcome)
                with self.assertRaises(LostReply):
                    caller.cleanup(adapter, lose_reply=True)
                adapter.collect("caller-A", limit=1)
                adapter = LeaseAdapterModel.restore(adapter.snapshot())
                caller = LeaseConsumerModel.restore(caller.snapshot())
                self.assertTrue(caller.state["cleanup_pending"])
                caller.cleanup(adapter)  # Compact settlement proof is sufficient.
                self.assertFalse(caller.state["cleanup_pending"])
                self.assertEqual(caller.state["outcome"], outcome)

    def test_expiry_does_not_let_caller_infer_cleanup_completion(self) -> None:
        caller = LeaseConsumerModel(self.identity, 10)
        caller.prepare(self.adapter)
        self.adapter.record_cleanup_effect(self.identity)
        self.adapter.advance_to(10)
        caller.settle("expired")
        self.assertEqual(self.adapter.status(self.identity).phase, "expired")
        with self.assertRaises(Deferred):
            caller.cleanup(self.adapter)
        self.assertTrue(caller.state["cleanup_pending"])

    def test_unknown_status_does_not_clear_debt_or_fence_a_delayed_prepare(self) -> None:
        caller = LeaseConsumerModel(self.identity, 10)
        self.assertEqual(self.adapter.status(self.identity).phase, "unknown")
        caller.settle("canceled")
        self.assertTrue(caller.state["cleanup_pending"])
        caller.cleanup(self.adapter)  # Required release-before-prepare fence.
        with self.assertRaises(Retired):
            self.adapter.prepare(self.identity, 10)

    def test_replacement_adapter_cannot_acknowledge_old_cleanup(self) -> None:
        caller = LeaseConsumerModel(self.identity, 10)
        caller.settle("canceled")
        replacement = LeaseAdapterModel("replacement", ROUTES, ("caller-A",))
        with self.assertRaises(Rejected):
            caller.cleanup(replacement)
        self.assertTrue(caller.state["cleanup_pending"])

    def test_clock_regression_after_restart_fails_closed(self) -> None:
        self.adapter.prepare(self.identity, 10)
        self.adapter.advance_to(10)
        self.adapter.status(self.identity)
        self.restart()
        with self.assertRaises(Rejected):
            self.adapter.advance_to(9)
        self.assertEqual(self.adapter.status(self.identity).phase, "expired")

    def test_large_clock_jump_cannot_establish_stream_quiescence(self) -> None:
        self.adapter.prepare(self.identity, 10)
        self.adapter.online.add("resource-A")
        self.adapter.open_read(self.identity, 1)
        self.adapter.advance_to(10**12)
        self.assertEqual(self.adapter.status(self.identity).phase, "expired")
        self.assertFalse(self.adapter.quiesce("resource-A"))

    def test_sequence_exhaustion_fails_instead_of_reusing_old_identity(self) -> None:
        adapter = LeaseAdapterModel(I, ROUTES, ("caller-A",), maximum_ordinal=2)
        for ordinal in (1, 2):
            adapter.release(replace(self.identity, ordinal=ordinal))
            adapter.collect("caller-A", limit=1)
        with self.assertRaises(Rejected):
            adapter.prepare(replace(self.identity, ordinal=3), 10)
        with self.assertRaises(Retired):
            adapter.prepare(self.identity, 10)

    def test_retirement_watermark_survives_arbitrarily_late_replay(self) -> None:
        self.adapter.release(self.identity)
        self.adapter.collect("caller-A", limit=1)
        self.adapter.advance_to(10**15)
        self.restart()
        for action in (
            lambda: self.adapter.prepare(self.identity, 10),
            lambda: self.adapter.renew(self.identity, 0, 10**15 + 10),
            lambda: self.adapter.open_read(self.identity, 1),
        ):
            with self.assertRaises(Retired):
                action()
        self.assertFalse(self.records())

    def test_new_demand_after_gate_closure_waits_for_fresh_availability(self) -> None:
        self.adapter.online.add("resource-A")
        self.assertTrue(self.adapter.quiesce("resource-A"))
        self.adapter.prepare(self.identity, 10)
        with self.assertRaises(Deferred):
            self.adapter.open_read(self.identity, 1)
        self.adapter.online.add("resource-A")
        self.adapter.open_read(self.identity, 1)

    def test_120_renew_release_expire_collect_open_orders_do_not_resurrect(self) -> None:
        # Every ordering starts with a committed preparation. A fixed expiry tick
        # is after either possible grant; snapshots after EVERY action preserve pins.
        schedules = tuple(itertools.permutations(("renew", "release", "expire", "collect", "open")))
        self.assertEqual(len(schedules), 120)
        for order in schedules:
            with self.subTest(order=order):
                adapter = LeaseAdapterModel(I, ROUTES, ("caller-A",))
                adapter.prepare(self.identity, 10)
                fenced = False
                for operation in order:
                    adapter.online.add("resource-A")  # Explicit re-observation.
                    try:
                        if operation == "renew":
                            adapter.renew(self.identity, 0, 20)
                        elif operation == "release":
                            fenced = True
                            adapter.release(self.identity)
                        elif operation == "expire":
                            fenced = True
                            adapter.advance_to(30)
                        elif operation == "collect":
                            adapter.collect("caller-A", limit=2)
                        else:
                            adapter.open_read(self.identity, 1)
                    except (Rejected, Deferred):
                        pass
                    adapter = LeaseAdapterModel.restore(adapter.snapshot())
                    if fenced:
                        self.assertNotEqual(adapter.status(self.identity).phase, "active")
                        with self.assertRaises(Rejected):
                            adapter.open_read(self.identity, 2)
                    records = adapter.state["scopes"]["caller-A"]["records"]
                    if any(r["pins"] for r in records.values()):
                        self.assertFalse(adapter.quiesce("resource-A"))
                adapter.confirm_stream_closed(self.identity, 1)
                adapter.release(self.identity)
                adapter.collect("caller-A", limit=2)
                self.assertEqual(adapter.status(self.identity).phase, "retired")
                self.assertFalse(adapter.state["scopes"]["caller-A"]["records"])

    def test_24_prepare_release_renew_collect_orders_include_unseen_prepare(self) -> None:
        schedules = tuple(itertools.permutations(("prepare", "release", "renew", "collect")))
        self.assertEqual(len(schedules), 24)
        for order in schedules:
            with self.subTest(order=order):
                adapter = LeaseAdapterModel(I, ROUTES, ("caller-A",))
                released = False
                for operation in order:
                    try:
                        if operation == "prepare":
                            adapter.prepare(self.identity, 10)
                        elif operation == "release":
                            adapter.release(self.identity)
                            released = True
                        elif operation == "renew":
                            adapter.renew(self.identity, 0, 20)
                        else:
                            adapter.collect("caller-A", limit=1)
                    except Rejected:
                        pass
                    adapter = LeaseAdapterModel.restore(adapter.snapshot())
                    if released:
                        self.assertNotEqual(adapter.status(self.identity).phase, "active")
                adapter.collect("caller-A", limit=1)
                with self.assertRaises(Retired):
                    adapter.prepare(self.identity, 10)


if __name__ == "__main__":
    unittest.main()
