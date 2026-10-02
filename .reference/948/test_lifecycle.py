"""Design-model tests only; these do not qualify production Stove0 or NVENC."""
import tempfile
import unittest
from pathlib import Path

from lifecycle import Binding, KINDS, Permit, Service, Store


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "jobs.sqlite"
        self.store = Store(self.path)
        self.addCleanup(lambda: self.store.close())
        self.service = Service(self.store)

    def accept(self, kind="transform", name="encode"):
        binding = Binding(kind, name)
        self.service.accept(binding)
        return binding

    def start(self, binding, now=0):
        ticket = self.service.prepare(binding.key, authority_expires=now + 100, now=now)
        self.assertIsNotNone(ticket)
        permit = Permit(str(ticket))
        self.assertTrue(self.service.admit(ticket, permit, now=now))
        return ticket, permit

    def test_control_calls_do_not_execute_payload_for_any_kind(self):
        for kind in sorted(KINDS):
            with self.subTest(kind=kind):
                binding = self.accept(kind)
                self.assertEqual(self.service.status(binding.key)["state"], "queued")
                self.service.accept(binding)
                self.service.cancel(binding.key)
        self.assertEqual(self.service.starts, [])
        self.assertEqual(self.service.active, {})
        self.assertEqual(self.service.probes, {})

    def test_acceptance_survives_database_reopen(self):
        binding = self.accept()
        self.store.close()
        self.store = Store(self.path)
        self.service = Service(self.store)
        self.assertEqual(self.service.accept(binding)["state"], "queued")
        self.assertEqual(self.service.starts, [])

    def test_indeterminate_gpu_wait_does_not_hold_execution_capacity(self):
        gpu = self.accept(name="nvidia-encode")
        for turn in range(500):
            self.service.accept(gpu)
            ticket = self.service.prepare(gpu.key, authority_expires=turn + 10, now=turn)
            self.assertFalse(self.service.admit(ticket, None, now=turn))
            self.assertEqual(self.service.active, {})
            self.assertEqual(self.service.probes, {})
            self.assertEqual(self.service.status(gpu.key)["attempt"], 1)
        self.assertEqual(self.service.starts, [])
        # The gate can grant later; there is no lifetime/queue timeout in the model.
        ticket, permit = self.start(gpu, now=10000)
        self.service.finish(ticket, outcome="complete", result="sealed-fixture-result",
                            workers_stopped=True)
        self.assertTrue(permit.released)

    def test_other_work_progresses_while_gpu_never_grants(self):
        gpu = self.accept(name="nvidia-encode")
        for turn in range(20):
            ticket = self.service.prepare(gpu.key, authority_expires=turn + 10, now=turn)
            self.service.admit(ticket, None, now=turn)
            other = self.accept("observer", f"ready-observation-{turn}")
            started, _ = self.start(other, now=turn)
            self.service.finish(started, outcome="complete", result="observed",
                                workers_stopped=True)
        self.assertEqual(self.service.status(gpu.key)["state"], "queued")
        self.assertEqual(len(self.service.starts), 20)

    def test_no_external_probe_when_local_capacity_is_full(self):
        first = self.accept(name="first")
        self.start(first)
        second = self.accept(name="second")
        self.assertIsNone(self.service.prepare(second.key, authority_expires=10, now=0))
        self.assertEqual(self.service.probes, {})

    def test_one_admission_probe_per_job(self):
        binding = self.accept()
        self.assertIsNotNone(self.service.prepare(binding.key, authority_expires=10, now=0))
        self.assertIsNone(self.service.prepare(binding.key, authority_expires=10, now=0))

    def test_probe_deadline_frees_reservation_and_late_grant_is_released(self):
        binding = self.accept()
        old = self.service.prepare(binding.key, authority_expires=100, now=0)
        self.service.expire_probes(2)
        new = self.service.prepare(binding.key, authority_expires=100, now=2)
        late = Permit("old-probe")
        self.assertFalse(self.service.admit(old, late, now=2))
        self.assertTrue(late.released)
        self.assertEqual(self.service.probes[binding.key], new)

    def test_expired_runtime_authority_requires_refresh_not_new_identity(self):
        binding = self.accept()
        self.assertIsNone(self.service.prepare(binding.key, authority_expires=1, now=2))
        self.service.accept(binding)
        ticket = self.service.prepare(binding.key, authority_expires=20, now=2)
        self.assertEqual(ticket.key, binding.key)
        self.assertEqual(ticket.attempt, 1)

    def test_authority_expiring_during_probe_does_not_launch(self):
        binding = self.accept()
        ticket = self.service.prepare(binding.key, authority_expires=0.5, now=0)
        permit = Permit("grant")
        self.assertFalse(self.service.admit(ticket, permit, now=0.5))
        self.assertTrue(permit.released)
        self.assertEqual(self.service.status(binding.key)["state"], "queued")

    def test_queued_cancel_is_terminal_without_executor(self):
        binding = self.accept()
        self.assertEqual(self.service.cancel(binding.key)["state"], "canceled")
        self.assertEqual(self.service.accept(binding)["state"], "canceled")
        self.assertIsNone(self.service.prepare(binding.key, authority_expires=10, now=0))

    def test_cancel_during_probe_releases_late_grant(self):
        binding = self.accept()
        ticket = self.service.prepare(binding.key, authority_expires=10, now=0)
        self.service.cancel(binding.key)
        permit = Permit("late")
        self.assertFalse(self.service.admit(ticket, permit, now=0))
        self.assertTrue(permit.released)
        self.assertEqual(self.service.starts, [])

    def test_cancel_running_does_not_report_stopped_or_release_early(self):
        binding = self.accept()
        ticket, permit = self.start(binding)
        self.assertEqual(self.service.cancel(binding.key)["state"], "canceling")
        self.assertFalse(permit.released)
        with self.assertRaises(ValueError):
            self.service.finish(ticket, outcome="canceled", workers_stopped=False)
        self.service.finish(ticket, outcome="canceled", workers_stopped=True)
        self.assertTrue(permit.released)

    def test_duplicate_grant_cannot_release_live_permit(self):
        binding = self.accept()
        ticket, permit = self.start(binding)
        self.assertFalse(self.service.admit(ticket, permit, now=0))
        self.assertFalse(permit.released)
        self.assertEqual(len(self.service.starts), 1)

    def test_repeated_accepts_cannot_start_duplicate_execution(self):
        binding = self.accept()
        self.start(binding)
        for _ in range(20):
            self.assertEqual(self.service.accept(binding)["state"], "running")
            self.assertIsNone(self.service.prepare(binding.key, authority_expires=10, now=0))
        self.assertEqual(len(self.service.starts), 1)

    def test_lost_success_response_replays_same_terminal_evidence(self):
        binding = self.accept()
        ticket, _ = self.start(binding)
        expected = self.service.finish(ticket, outcome="complete", result="exact-receipt",
                                       workers_stopped=True)
        self.assertEqual(self.service.accept(binding), expected)
        self.assertEqual(self.service.finish(ticket, outcome="complete", result="exact-receipt",
                                            workers_stopped=True), expected)
        with self.assertRaises(ValueError):
            self.service.finish(ticket, outcome="complete", result="different",
                                workers_stopped=True)

    def test_no_pending_result_is_misrepresented_as_evidence(self):
        binding = self.accept()
        self.assertIsNone(self.service.status(binding.key)["result"])
        ticket, _ = self.start(binding)
        with self.assertRaises(ValueError):
            self.service.finish(ticket, outcome="complete", workers_stopped=True)

    def test_queued_effects_remain_unstarted_after_recovery(self):
        bindings = [self.accept(kind) for kind in sorted(KINDS)]
        self.service.recover(workers_stopped=True)
        self.service = Service(self.store)
        for binding in bindings:
            with self.subTest(kind=binding.kind):
                self.assertEqual(self.service.status(binding.key)["state"], "queued")
                self.assertEqual(self.service.status(binding.key)["attempt"], 1)
                ticket, _ = self.start(binding)
                self.service.finish(ticket, outcome="complete", result="exact-result",
                                    workers_stopped=True)

    def test_started_effects_are_not_automatically_replayed_after_crash(self):
        for kind in ("effect", "departure"):
            binding = self.accept(kind)
            self.start(binding)
            self.service.recover(workers_stopped=True)
            self.service = Service(self.store)
            self.assertEqual(self.service.accept(binding)["state"], "uncertain")
            self.assertIsNone(self.service.prepare(binding.key, authority_expires=10, now=0))
            with self.assertRaises(ValueError):
                self.service.resume(binding.key)

    def test_replay_safe_resume_advances_attempt_not_semantic_identity(self):
        binding = self.accept("observer")
        old, _ = self.start(binding)
        self.service.recover(workers_stopped=True)
        self.service = Service(self.store)
        self.assertEqual(self.service.status(binding.key)["state"], "interrupted")
        resumed = self.service.resume(binding.key)
        self.assertEqual(resumed["attempt"], 2)
        new, _ = self.start(binding)
        with self.assertRaises(ValueError):
            self.service.finish(old, outcome="complete", result="old-result", workers_stopped=True)
        self.service.finish(new, outcome="complete", result="new-result", workers_stopped=True)

    def test_recovery_requires_old_consumers_stopped(self):
        binding = self.accept()
        _, permit = self.start(binding)
        with self.assertRaises(ValueError):
            self.service.recover(workers_stopped=False)
        self.assertFalse(permit.released)

    def test_old_service_cannot_publish_after_recovery(self):
        binding = self.accept()
        old = self.service
        ticket, _ = self.start(binding)
        old.recover(workers_stopped=True)
        self.service = Service(self.store)
        with self.assertRaises(RuntimeError):
            old.finish(ticket, outcome="complete", result="old-result", workers_stopped=True)

    def test_old_probe_after_recovery_releases_only_its_own_grant(self):
        binding = self.accept()
        old = self.service
        ticket = old.prepare(binding.key, authority_expires=10, now=0)
        old.recover(workers_stopped=True)
        self.service = Service(self.store)
        _, live = self.start(binding)
        late = Permit("old-owner")
        self.assertFalse(old.admit(ticket, late, now=0))
        self.assertTrue(late.released)
        self.assertFalse(live.released)

    def test_lease_loss_is_not_permission_to_publish_new_success(self):
        binding = self.accept()
        ticket, permit = self.start(binding)
        permit.valid = False
        with self.assertRaises(ValueError):
            self.service.finish(ticket, outcome="complete", result="unverified-success",
                                workers_stopped=True)
        self.service.finish(ticket, outcome="failed", workers_stopped=True)
        self.assertTrue(permit.released)

    def test_effect_failure_or_cancel_requires_noncommit_proof(self):
        for kind in ("effect", "departure"):
            for outcome in ("failed", "canceled"):
                binding = self.accept(kind, f"{kind}-{outcome}")
                ticket, permit = self.start(binding)
                with self.assertRaises(ValueError):
                    self.service.finish(ticket, outcome=outcome, workers_stopped=True)
                self.service.finish(ticket, outcome="uncertain", workers_stopped=True)
                self.assertTrue(permit.released)

    def test_confirmed_precommit_effect_cancellation_can_finish(self):
        binding = self.accept("effect")
        ticket, _ = self.start(binding)
        self.service.cancel(binding.key)
        row = self.service.finish(ticket, outcome="canceled", workers_stopped=True, no_effect=True)
        self.assertEqual(row["state"], "canceled")

    def test_terminal_success_wins_over_late_cancel(self):
        binding = self.accept("effect")
        ticket, _ = self.start(binding)
        self.service.finish(ticket, outcome="complete", result="exact-receipt", workers_stopped=True)
        self.assertEqual(self.service.cancel(binding.key)["state"], "complete")

    def test_changed_claim_generation_is_not_same_execution_identity(self):
        old = Binding("observer", "same-semantic-request", fence=1)
        new = Binding("observer", "same-semantic-request", fence=2)
        self.assertEqual(old.semantic_id, new.semantic_id)
        self.assertNotEqual(old.key, new.key)
        self.service.accept(old)
        with self.assertRaises(ValueError):
            self.service.accept(new, key=old.key)

    def test_backpressure_does_not_accept_and_forget(self):
        self.service = Service(self.store, backlog_limit=1)
        first = self.accept(name="first")
        second = Binding("transform", "second")
        with self.assertRaises(OverflowError):
            self.service.accept(second)
        with self.assertRaises(KeyError):
            self.store.get(second.key)
        self.assertEqual(self.service.accept(first)["state"], "queued")


if __name__ == "__main__":
    unittest.main()
