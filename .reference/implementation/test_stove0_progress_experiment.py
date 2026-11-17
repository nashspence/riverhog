"""Real-owner correctness boundaries for the #998 advisory progress proposal."""

from __future__ import annotations

import os
import selectors
import sys
import threading
from unittest.mock import patch

import pytest
from pydantic import ValidationError
from stove0_extension_support import ExclusiveStateOwner
from stove0_extension_support import subprocess as owned_subprocess
from stove0_extension_support.consumers import consumer_scope
from stove0_progress_experiment import (
    VolatileProgressTarget,
    observe_work,
    open_target,
    run_pair,
    seed_original_running,
    source_fixture,
)
from stove0_target_protocol import TargetProgress
from stove0_target_support.persistent import TargetExecutionCanceled


@pytest.mark.parametrize("callbacks", [1, 7, 31])
def test_same_original_result_without_per_callback_durable_progress(tmp_path, callbacks):
    report = run_pair(tmp_path / "pair", callbacks=callbacks)
    current, proposed = (report["samples"][key] for key in ("current", "proposed"))
    assert current["advisory_callbacks"]["state_writes"] == callbacks
    assert current["advisory_callbacks"]["fsync_calls"] == 2 * callbacks
    assert proposed["advisory_callbacks"]["state_writes"] == 0
    assert proposed["advisory_callbacks"]["fsync_calls"] == 0
    for phase in ("original_acceptance_and_running", "original_result_and_retry"):
        assert current[phase]["state_writes"] == proposed[phase]["state_writes"] > 0
        assert current[phase]["fsync_calls"] == proposed[phase]["fsync_calls"] > 0
    assert current["status_reads"]["state_writes"] == proposed["status_reads"]["state_writes"] == 0
    assert current["terminal_status"] == proposed["terminal_status"]


def test_generation_bound_callback_rejects_old_same_attempt_owner(tmp_path):
    service = open_target(tmp_path / "state", proposed=True)
    assert isinstance(service, VolatileProgressTarget)
    try:
        _, running = seed_original_running(service)
        old = service.bind_progress(running.job_id, 1)
        current = service.bind_progress(running.job_id, 1)
        current(TargetProgress(phase="current", completed=3))
        with observe_work() as sample:
            old(TargetProgress(phase="stale", completed=999))
            assert service.get_job(running.job_id).progress.phase == "current"
        assert sample.counts["state_writes"] == sample.counts["fsync_calls"] == 0
    finally:
        service.close()


def test_old_attempt_does_not_overwrite_new_attempt_or_mutate_authority(tmp_path):
    service = open_target(tmp_path / "state", proposed=True)
    try:
        _, running = seed_original_running(service)
        old = service.bind_progress(running.job_id, 1)
        newer = running.model_copy(update={"attempt": 2})
        service._commit_status(newer)
        with pytest.raises(ValueError, match="current original"):
            service.bind_progress(running.job_id, 1)
        current = service.bind_progress(running.job_id, 2)
        current(TargetProgress(phase="second", completed=4))
        old(TargetProgress(phase="old", completed=100))
        assert service.get_job(running.job_id).attempt == 2
        assert service.get_job(running.job_id).progress.completed == 4
        assert service._load_status(running.job_id) == newer
    finally:
        service.close()


def test_progress_snapshot_cannot_be_changed_by_callers_after_reporting(tmp_path):
    service = open_target(tmp_path / "state", proposed=True)
    try:
        _, running = seed_original_running(service)
        callback = service.bind_progress(running.job_id, 1)
        value = TargetProgress(phase="selected", completed=5)
        callback(value)
        with pytest.raises(ValidationError, match="frozen"):
            value.completed = 99
        # A bypass of the model's public immutability must still not mutate
        # the owner's independent snapshot.
        value.__dict__["completed"] = 99
        observed = service.get_job(running.job_id)
        assert observed.progress.completed == 5
        observed.progress.__dict__["completed"] = 101
        assert service.get_job(running.job_id).progress.completed == 5
    finally:
        service.close()


def test_cancellation_tombstone_wins_late_progress_without_becoming_quiescence(tmp_path):
    service = open_target(tmp_path / "state", proposed=True)
    try:
        request, running = seed_original_running(service)
        callback = service.bind_progress(running.job_id, 1)
        callback(TargetProgress(phase="before-cancel", completed=2))
        with observe_work() as cancel:
            selected = service.cancel_job(request.accepted())
        assert selected.state == "canceling"
        assert service._load_cancellation(running.job_id) == request.accepted()
        assert cancel.counts["state_writes"] == 2 and cancel.counts["fsync_calls"] == 4
        with observe_work() as late:
            callback(TargetProgress(phase="after-cancel", completed=999))
            assert service.get_job(running.job_id) == selected
        assert late.counts["state_writes"] == 0
        # This isolated callback test has no worker. Only the actual dispatcher
        # receiving witness below proves when a running original has stopped.
    finally:
        service.close()


def test_successful_original_result_survives_lost_write_reply_and_cold_reopen(tmp_path):
    fixture = source_fixture()
    operation, _, request = fixture._request()
    success = fixture._success_status(operation, request)
    root = tmp_path / "state"
    service = open_target(root, proposed=True)
    try:
        _, running = seed_original_running(service)
        callback = service.bind_progress(running.job_id, 1)
        writer = service._write_model

        def lose_reply(path, value):
            writer(path, value)
            raise OSError("original terminal file selected, reply lost")

        with patch.object(service, "_write_model", lose_reply):
            with pytest.raises(OSError, match="reply lost"):
                service._commit_status(success)
        with observe_work() as retry:
            callback(TargetProgress(phase="late", completed=1))
            assert service.get_job(running.job_id) == success
            assert service._commit_status(success) == success
            assert service._commit_status(running) == success
        assert retry.counts["state_writes"] == 0
    finally:
        service.close()
    restarted = open_target(root, proposed=True)
    try:
        assert restarted.get_job(request.declaration.job_id) == success
        assert restarted._load_accepted(request.declaration.job_id) == request.accepted()
    finally:
        restarted.close()


def test_restart_discards_advisory_only_and_retains_original_interrupted_attempt(tmp_path):
    root = tmp_path / "state"
    service = open_target(root, proposed=True)
    try:
        request, running = seed_original_running(service)
        callback = service.bind_progress(running.job_id, 1)
        callback(TargetProgress(phase="volatile", completed=42))
        assert service.get_job(running.job_id).progress.completed == 42
        assert service._load_status(running.job_id) == running
    finally:
        service.close()
    with observe_work() as recovery:
        restarted = open_target(root, proposed=True)
    try:
        observed = restarted.get_job(running.job_id)
        assert observed.state == "interrupted" and observed.progress.completed == 0
        assert observed.attempt == running.attempt
        assert observed.request_sha256 == request.request_sha256
        assert restarted._load_accepted(running.job_id) == request.accepted()
        assert recovery.counts["state_writes"] == 1  # Original interrupted-execution marker.
        assert not restarted._live_progress
        with observe_work() as late:
            callback(TargetProgress(phase="closed-owner", completed=100))
            assert restarted.get_job(running.job_id) == observed
        assert late.counts["state_writes"] == 0
    finally:
        restarted.close()


def test_progress_memory_follows_existing_active_worker_admission(tmp_path):
    fixture = source_fixture()
    service = open_target(tmp_path / "state", proposed=True)
    try:
        request, running = seed_original_running(service)
        service.bind_progress(running.job_id, 1)
        second = fixture._request_at_fence(request, request.declaration.fence + 1)
        assert second.declaration.job_id != request.declaration.job_id
        service._write_model(service._accepted_path(second.declaration.job_id), second.accepted())
        service._commit_status(
            service._status(second, state="running", attempt=1, phase="starting")
        )
        with pytest.raises(ValueError, match="active-worker admission"):
            service.bind_progress(second.declaration.job_id, 1)
        assert len(service._live_progress) == service.maximum_workers == 1
    finally:
        service.close()


@pytest.mark.parametrize("complete_before_cancel", [False, True])
def test_actual_dispatcher_keeps_original_owner_until_worker_received(
    tmp_path, complete_before_cancel
):
    fixture = source_fixture()
    operation, _, request = fixture._request()
    success = fixture._success_status(operation, request)
    entered, release, closing, closed = (threading.Event() for _ in range(4))
    close_failure = []

    def execute(_request, _attempt, _canceled, session):
        session.report_progress(TargetProgress(phase="held-worker", completed=1))
        if complete_before_cancel:
            session.record_completed(success)
        entered.set()
        if not release.wait(5):
            raise AssertionError("test did not receive the original worker")
        if complete_before_cancel:
            return success
        raise TargetExecutionCanceled("owned cancellation observed")

    root = tmp_path / "state"
    service = open_target(root, proposed=True, execute=execute)

    def close_original():
        closing.set()
        try:
            service.close()
        except BaseException as error:
            close_failure.append(error)
        finally:
            closed.set()

    thread = threading.Thread(target=close_original)
    try:
        # This is the maintained fixture's declared synthetic-claim seam. No
        # authenticated Core access is claimed or attempted by this experiment.
        with patch.object(service, "_validate_live_authority", lambda *_a, **_kw: None):
            service.put_job(request)
            assert entered.wait(5)
        selected = service.cancel_job(request.accepted())
        assert selected.state == ("succeeded" if complete_before_cancel else "canceling")
        thread.start()
        assert closing.wait(1)
        assert not closed.wait(0.05)
        with pytest.raises(RuntimeError, match="active owner"):
            ExclusiveStateOwner(root)
    finally:
        release.set()
        if thread.ident is None:
            service.close()
        else:
            thread.join(5)
    assert closed.is_set() and not close_failure
    replacement = open_target(root, proposed=True)
    try:
        assert replacement.get_job(request.declaration.job_id).state == (
            "succeeded" if complete_before_cancel else "canceled"
        )
    finally:
        replacement.close()


@pytest.mark.skipif(os.name != "posix", reason="maintained native supervisor is POSIX only")
def test_real_supervised_process_is_received_before_original_lock_is_released(tmp_path):
    root = tmp_path / "owner"
    root.mkdir(mode=0o700)
    owner = ExclusiveStateOwner(root)
    canceled = threading.Event()
    process = None
    try:
        with consumer_scope(owner, canceled):
            process = owned_subprocess.Popen(
                [sys.executable, "-c", "import time; print('ready', flush=True); time.sleep(30)"],
                stdout=owned_subprocess.PIPE,
                text=True,
            )
            with selectors.DefaultSelector() as ready:
                ready.register(process.stdout, selectors.EVENT_READ)
                assert ready.select(5), "original supervised child did not announce readiness"
            assert process.stdout.readline() == "ready\n"
            with pytest.raises(RuntimeError, match="active owner"):
                ExclusiveStateOwner(root)
            canceled.set()
            assert process.wait(timeout=5) != 0
        assert process.returncode is not None
    finally:
        if process is not None:
            process.terminate()
            process.wait(timeout=5)
            if process.stdout is not None:
                process.stdout.close()
        owner.close()
    replacement = ExclusiveStateOwner(root)
    replacement.close()
