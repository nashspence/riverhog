from __future__ import annotations

import asyncio
import threading
from contextlib import contextmanager
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from riverhog_api import app as api_app


class _Uploads:
    def __init__(self, *, active=False):
        self.active = active
        self.callback = None
        self.bindings = 0
        self.detachments = 0

    @contextmanager
    def archive_maintenance_wakeup(self, callback):
        assert self.callback is None
        self.callback = callback
        self.bindings += 1
        try:
            yield
        finally:
            assert self.callback is callback
            self.callback = None
            self.detachments += 1

    def has_unreceived_archive_work(self):
        return self.active

    def notify(self):
        assert self.callback is not None
        self.callback()


def _start(container_provider, *, interval=86400, receive=0.01):
    return asyncio.create_task(
        api_app._run_archive_upload_reaper(
            container_provider,
            sweep_interval=timedelta(seconds=interval),
            receive_interval=timedelta(seconds=receive),
            operation_lock=asyncio.Lock(),
        )
    )


async def _cancel(task):
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.parametrize("before_wait", (False, True))
def test_thread_notification_survives_wait_registration(before_wait):
    async def exercise():
        wake = api_app._ArchiveMaintenanceWake()
        try:
            if before_wait:
                await asyncio.to_thread(wake.notify)
            waiting = asyncio.create_task(wake.wait(86400))
            await asyncio.sleep(0)
            if not before_wait:
                assert not waiting.done()
                await asyncio.to_thread(wake.notify)
            assert await asyncio.wait_for(waiting, timeout=5)
            assert not await wake.wait(0)
        finally:
            wake.close()

    asyncio.run(exercise())


def test_burst_coalesces_before_delivery_and_stale_delivery_cannot_repeat_it(monkeypatch):
    async def exercise():
        loop = asyncio.get_running_loop()
        wake = api_app._ArchiveMaintenanceWake()
        queued = []
        with monkeypatch.context() as patch:
            patch.setattr(loop, "call_soon_threadsafe", lambda callback: queued.append(callback))
            for _ in range(1024):
                wake.notify()
            assert len(queued) == 1
            assert await wake.wait(0)
            queued.pop()()
            assert not await wake.wait(0)
            wake.notify()
            assert len(queued) == 1
            queued.pop()()
            assert await wake.wait(0)
            assert not await wake.wait(0)
        wake.close()

    asyncio.run(exercise())


def test_wait_cancellation_preserves_a_later_notification():
    async def exercise():
        wake = api_app._ArchiveMaintenanceWake()
        waiting = asyncio.create_task(wake.wait(86400))
        await asyncio.sleep(0)
        await _cancel(waiting)
        wake.notify()
        assert await asyncio.wait_for(wake.wait(86400), timeout=5)
        assert not await wake.wait(0)
        wake.close()

    asyncio.run(exercise())


def test_closed_wake_ignores_queued_and_late_notifications(monkeypatch):
    async def exercise():
        loop = asyncio.get_running_loop()
        wake = api_app._ArchiveMaintenanceWake()
        queued = []
        with monkeypatch.context() as patch:
            patch.setattr(loop, "call_soon_threadsafe", lambda callback: queued.append(callback))
            wake.notify()
            assert len(queued) == 1
            wake.close()
            queued.pop()()
            wake.notify()
            assert not queued
            assert not await wake.wait(0)

    asyncio.run(exercise())


def test_idle_wait_retains_its_timer_fallback():
    async def exercise():
        wake = api_app._ArchiveMaintenanceWake()
        try:
            assert not await asyncio.wait_for(wake.wait(0.001), timeout=5)
        finally:
            wake.close()

    asyncio.run(exercise())


def test_notification_during_a_sweep_remains_pending_for_the_next_full_sweep(monkeypatch):
    async def exercise():
        loop = asyncio.get_running_loop()
        uploads = _Uploads()
        container = SimpleNamespace(collection_uploads=uploads)
        entered, finished = asyncio.Event(), asyncio.Event()
        release = threading.Event()
        turns = []

        def process(selected, *, startup_recovery=False, execution_only=False):
            assert selected is container
            turns.append((startup_recovery, execution_only))
            if len(turns) == 1:
                loop.call_soon_threadsafe(entered.set)
                assert release.wait(5)
            else:
                loop.call_soon_threadsafe(finished.set)
            return False

        monkeypatch.setattr(api_app, "_process_archive_maintenance", process)
        task = _start(lambda: container)
        try:
            await asyncio.wait_for(entered.wait(), timeout=5)
            uploads.notify()
            release.set()
            await asyncio.wait_for(finished.wait(), timeout=5)
            await asyncio.sleep(0.02)
            assert turns == [(True, False), (False, False)]
        finally:
            release.set()
            await _cancel(task)
        assert uploads.bindings == uploads.detachments == 1

    asyncio.run(exercise())


def test_fresh_hint_interrupts_receiving_cadence_with_a_full_sweep(monkeypatch):
    async def exercise():
        loop = asyncio.get_running_loop()
        uploads = _Uploads(active=True)
        container = SimpleNamespace(collection_uploads=uploads)
        receiving, finished = asyncio.Event(), asyncio.Event()
        turns = []

        def process(selected, *, startup_recovery=False, execution_only=False):
            turns.append("receive" if execution_only else "sweep")
            if len(turns) == 2:
                loop.call_soon_threadsafe(receiving.set)
            if len(turns) > 2 and not execution_only:
                uploads.active = False
                loop.call_soon_threadsafe(finished.set)
            return False

        monkeypatch.setattr(api_app, "_process_archive_maintenance", process)
        task = _start(lambda: container, receive=0.1)
        try:
            await asyncio.wait_for(receiving.wait(), timeout=5)
            uploads.notify()
            await asyncio.wait_for(finished.wait(), timeout=5)
            await asyncio.sleep(0.02)
            assert turns == ["sweep", "receive", "sweep"]
        finally:
            await _cancel(task)

    asyncio.run(exercise())


def _idle_container():
    uploads = _Uploads()
    for name in (
        "requeue_interrupted_finalizations_for_startup",
        "requeue_interrupted_orphan_discards_for_startup",
        "process_due_provenance_journal_validations",
        "process_due_custody_receipts",
        "process_due_finalizations",
        "reap_expired_custody_transfers",
    ):
        setattr(uploads, name, Mock(return_value=0))

    def service(*names):
        return SimpleNamespace(**{name: Mock(return_value=0) for name in names})

    return SimpleNamespace(
        collection_uploads=uploads,
        collection_workflows=service(
            "requeue_interrupted_disposition_sets_for_startup",
            "reap_expired_claims",
            "process_due_disposition_sets",
            "process_due_outcome_sets",
        ),
        archive_copy_jobs=service("requeue_interrupted_jobs_for_startup", "process_due"),
        collection_deletions=service("process_due"),
        collection_descriptions=service("requeue_interrupted_for_startup", "process_due"),
        collection_tags=service("requeue_interrupted_for_startup", "process_due"),
        retrieval=service(
            "request_cache_accounting_reconciliation_for_startup",
            "process_cache_accounting_reconciliation",
        ),
        lifecycle_events=service("reap_expired_contexts"),
        catalog_sync=service("reap_expired_history"),
    )


def test_stale_burst_runs_one_full_bounded_sweep_including_unrelated_owners(
    monkeypatch, record_property
):
    async def exercise():
        loop = asyncio.get_running_loop()
        container = _idle_container()
        registrations = SimpleNamespace(process_due=Mock(return_value=0))
        monkeypatch.setattr(api_app, "CollectionRegistrationOwner", lambda service: registrations)
        swept = asyncio.Event()

        def finished_sweep():
            loop.call_soon_threadsafe(swept.set)
            return 0

        container.lifecycle_events.reap_expired_contexts.side_effect = finished_sweep
        task = _start(lambda: container)
        try:
            await asyncio.wait_for(swept.wait(), timeout=5)
            swept.clear()
            for _ in range(1024):
                container.collection_uploads.notify()
            await asyncio.wait_for(swept.wait(), timeout=5)
            await asyncio.sleep(0.02)
            steady = [
                registrations.process_due,
                container.collection_uploads.process_due_provenance_journal_validations,
                container.collection_uploads.process_due_custody_receipts,
                container.catalog_sync.reap_expired_history,
                container.collection_uploads.process_due_finalizations,
                container.collection_uploads.reap_expired_custody_transfers,
                container.collection_workflows.reap_expired_claims,
                container.collection_workflows.process_due_disposition_sets,
                container.collection_workflows.process_due_outcome_sets,
                container.collection_deletions.process_due,
                container.retrieval.process_cache_accounting_reconciliation,
                container.archive_copy_jobs.process_due,
                container.collection_descriptions.process_due,
                container.collection_tags.process_due,
                container.lifecycle_events.reap_expired_contexts,
            ]
            assert len(steady) == 15 and [call.call_count for call in steady] == [2] * 15
            record_property("coalesced_notifications", 1024)
            record_property("full_sweeps_including_startup", 2)
            record_property("steady_owner_method_calls", sum(call.call_count for call in steady))
            assert (
                container.collection_uploads.requeue_interrupted_finalizations_for_startup.call_count
                == 1
            )
            registrations.process_due.assert_called_with(limit=1)
            container.collection_uploads.process_due_finalizations.assert_called_with(limit=1)
        finally:
            await _cancel(task)

    asyncio.run(exercise())


def test_timer_finds_new_work_without_a_notification(monkeypatch):
    async def exercise():
        loop = asyncio.get_running_loop()
        uploads = _Uploads()
        container = SimpleNamespace(collection_uploads=uploads)
        found, followed_up = loop.create_future(), loop.create_future()
        ready = False
        turns = []
        waits, notifications = [], []

        class ObservedWake(api_app._ArchiveMaintenanceWake):
            async def wait(self, delay_seconds):
                nonlocal ready
                waits.append(delay_seconds)
                if len(waits) == 2:
                    # Introduce work after startup returns, before the real timer wait.
                    assert delay_seconds > 0
                    assert turns == [(True, False)]
                    ready = True
                notified = await super().wait(delay_seconds)
                notifications.append(notified)
                return notified

        def process(selected, *, startup_recovery=False, execution_only=False):
            nonlocal ready
            turns.append((startup_recovery, execution_only))
            if ready:
                ready = False
                loop.call_soon_threadsafe(found.set_result, tuple(turns))
                return True
            if len(turns) == 3:
                loop.call_soon_threadsafe(followed_up.set_result, tuple(turns))
            return False

        monkeypatch.setattr(api_app, "_ArchiveMaintenanceWake", ObservedWake)
        monkeypatch.setattr(api_app, "_process_archive_maintenance", process)
        task = _start(lambda: container, interval=0.1)
        try:
            assert await asyncio.wait_for(found, timeout=5) == ((True, False), (False, False))
            assert await asyncio.wait_for(followed_up, timeout=5) == (
                (True, False),
                (False, False),
                (False, False),
            )
            assert waits[0] == waits[2] == 0
            assert waits[1] > 0
            assert notifications[:3] == [False, False, False]
        finally:
            await _cancel(task)

    asyncio.run(exercise())


def test_provider_change_rebinds_owner_and_shutdown_ignores_late_hint(monkeypatch):
    async def exercise():
        loop = asyncio.get_running_loop()
        first = SimpleNamespace(collection_uploads=_Uploads())
        second = SimpleNamespace(collection_uploads=_Uploads())
        selected = first
        swept = asyncio.Event()
        turns = []

        def process(container, *, startup_recovery=False, execution_only=False):
            turns.append((container, startup_recovery, execution_only))
            loop.call_soon_threadsafe(swept.set)
            return False

        monkeypatch.setattr(api_app, "_process_archive_maintenance", process)
        task = _start(lambda: selected)
        try:
            await asyncio.wait_for(swept.wait(), timeout=5)
            swept.clear()
            selected = second
            first.collection_uploads.notify()
            await asyncio.wait_for(swept.wait(), timeout=5)
            assert turns == [(first, True, False), (second, False, False)]
            assert first.collection_uploads.callback is None
            assert first.collection_uploads.bindings == first.collection_uploads.detachments == 1
            assert second.collection_uploads.bindings == 1
            late = second.collection_uploads.callback
        finally:
            await _cancel(task)
        assert second.collection_uploads.detachments == 1
        assert second.collection_uploads.callback is None
        late()
        await asyncio.sleep(0)
        assert len(turns) == 2

    asyncio.run(exercise())


def test_failed_provider_binding_can_return_to_and_rebind_the_original_owner(monkeypatch):
    async def exercise():
        loop = asyncio.get_running_loop()
        first = SimpleNamespace(collection_uploads=_Uploads())
        second = SimpleNamespace(collection_uploads=_Uploads())
        selected = first
        swept, attempted = asyncio.Event(), asyncio.Event()
        foreign_callback = Mock()
        turns = []

        def provider():
            if selected is second:
                attempted.set()
            return selected

        def process(container, *, startup_recovery=False, execution_only=False):
            turns.append(container)
            assert not execution_only
            loop.call_soon_threadsafe(swept.set)
            return False

        monkeypatch.setattr(api_app, "_process_archive_maintenance", process)
        with second.collection_uploads.archive_maintenance_wakeup(foreign_callback):
            task = _start(provider, interval=0.1)
            try:
                await asyncio.wait_for(swept.wait(), timeout=5)
                swept.clear()
                selected = second
                first.collection_uploads.notify()
                await asyncio.wait_for(attempted.wait(), timeout=5)
                assert first.collection_uploads.callback is None
                assert second.collection_uploads.callback is foreign_callback
                selected = first
                await asyncio.wait_for(swept.wait(), timeout=5)
                assert turns == [first, first]
                assert first.collection_uploads.bindings == 2
                assert first.collection_uploads.detachments == 1
                assert first.collection_uploads.callback is not None
            finally:
                await _cancel(task)
            assert first.collection_uploads.detachments == 2
            assert second.collection_uploads.callback is foreign_callback
            assert second.collection_uploads.detachments == 0
            foreign_callback.assert_not_called()
        assert second.collection_uploads.detachments == 1

    asyncio.run(exercise())
