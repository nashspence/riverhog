from __future__ import annotations

import asyncio
import threading
from datetime import timedelta

import pytest
from riverhog_api import app as api_app
from riverhog_core.services import physical_execution

from tests.unit.test_archive_maintenance_wake import _idle_container
from tests.unit.test_collection_upload_preparation import _case
from tests.unit.test_collection_uploads import _bind_primary
from tests.unit.test_finalization_claim_readiness import _seal


def test_idle_reaper_activates_committed_work_and_keeps_original_until_join(
    tmp_path, monkeypatch, record_property
):
    with _case(tmp_path, "segment", artifact_count=1) as case:
        _bind_primary(case.service, case.collection_id, format(1, "064x"), case.content)
        service = case.service
        pool = service._archive_execution
        release = threading.Event()
        captured = []
        received = []
        joins = []
        notifications = []
        original_thread = physical_execution.Thread
        original_submit = pool.try_submit
        original_collect = pool.collect

        async def exercise():
            loop = asyncio.get_running_loop()
            idle, returned, receiving, swept_after_hint = (asyncio.Event() for _ in range(4))
            original_wake = api_app._ArchiveMaintenanceWake
            original_process = api_app._process_archive_maintenance
            sweeps = []
            hint_sweep_target = None

            class ObservedWake(original_wake):
                def notify(self):
                    notifications.append("hint")
                    super().notify()

                async def wait(self, delay_seconds):
                    if delay_seconds > 1000:
                        idle.set()
                    elif delay_seconds > 0 and captured and captured[0].future.done():
                        receiving.set()
                    return await super().wait(delay_seconds)

            class HeldExitThread(original_thread):
                def run(self):
                    super().run()
                    loop.call_soon_threadsafe(returned.set)
                    assert release.wait(10), "test did not release the original worker exit"

            def submit(key, **arguments):
                if arguments.get("owner") is service._archive_finalizers and not captured:
                    with monkeypatch.context() as patch:
                        patch.setattr(physical_execution, "Thread", HeldExitThread)
                        outcome = original_submit(key, **arguments)
                    if outcome == "submitted":
                        captured.append(pool._slots[key])
                    return outcome
                return original_submit(key, **arguments)

            def collect(*, limit, owner):
                outcomes = original_collect(limit=limit, owner=owner)
                for outcome in outcomes:
                    if captured and outcome._future is captured[0].future:
                        received.append(outcome)
                return outcomes

            def process(container, *, startup_recovery=False, execution_only=False):
                progressed = original_process(
                    container, startup_recovery=startup_recovery, execution_only=execution_only
                )
                sweeps.append((startup_recovery, execution_only, progressed))
                if (
                    hint_sweep_target is not None
                    and sum(not turn[1] for turn in sweeps) >= hint_sweep_target
                ):
                    loop.call_soon_threadsafe(swept_after_hint.set)
                return progressed

            monkeypatch.setattr(api_app, "_ArchiveMaintenanceWake", ObservedWake)
            monkeypatch.setattr(api_app, "_process_archive_maintenance", process)
            monkeypatch.setattr(pool, "try_submit", submit)
            monkeypatch.setattr(pool, "collect", collect)
            container = _idle_container()
            container.collection_uploads = service
            task = asyncio.create_task(
                api_app._run_archive_upload_reaper(
                    lambda: container,
                    sweep_interval=timedelta(days=1),
                    receive_interval=timedelta(milliseconds=10),
                    operation_lock=asyncio.Lock(),
                )
            )
            try:
                await asyncio.wait_for(idle.wait(), timeout=10)
                assert not captured and not pool.has_unreceived_work()
                idle_sweeps = len(sweeps)
                # These are the real complete and payload-unit entry points.
                # The last one commits point readiness and emits the hint.
                await asyncio.to_thread(_seal, case)
                await asyncio.wait_for(returned.wait(), timeout=10)
                assert len(captured) == 1 and notifications == ["hint"]
                original = captured[0]
                assert original.future.done() and original.worker.is_alive()
                result = original.future.result()
                assert result.progressed
                assert pool._slots[original.key] is original and received == []
                await asyncio.wait_for(receiving.wait(), timeout=10)

                # A further wake reconsiders ordinary work, but cannot treat the
                # original Future's result as a physical worker receipt.
                hint_sweep_target = sum(not turn[1] for turn in sweeps) + 1
                service._archive_maintenance_wakeup()
                await asyncio.wait_for(swept_after_hint.wait(), timeout=10)
                assert pool._slots[original.key] is original
                assert original.worker.is_alive() and received == []
                assert service.has_unreceived_archive_work()
                record_property(
                    "idle_activation",
                    {
                        "fallback_seconds": 86400,
                        "idle_sweeps": idle_sweeps,
                        "durable_transition_hints": 1,
                        "fresh_claim_workers": len(captured),
                        "held_original_preserved_after_another_hint": True,
                    },
                )
            finally:
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                release.set()
            assert service._archive_maintenance_wakeup is None

        try:
            asyncio.run(exercise())
            original = captured[0]
            original.worker.join(timeout=10)
            assert not original.worker.is_alive()
            actual_join = original.worker.join

            def joined(timeout=None):
                joins.append(timeout)
                actual_join(timeout)

            monkeypatch.setattr(original.worker, "join", joined)
            assert service.process_due_archive_uploads(limit=1) >= 1
            assert len(received) == 1
            assert received[0]._future is original.future
            assert received[0].result() is original.future.result()
            assert joins == [0]
            assert original.key not in pool._slots
            assert not pool.has_unreceived_work()
            record_property("original_physical_join_and_receiving", True)
        finally:
            release.set()
