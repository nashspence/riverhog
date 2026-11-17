"""Real target-owner progress experiment for #998; no product-source changes.

The reference subclass changes only advisory callback/read projection. Original
acceptance, execution markers, cancellation, terminal result persistence, process
owner and dispatcher receiving remain the current implementation. Fixture
requests/results come from the maintained target-support tests. No real claim,
external tool transformation, archive publication or remote effect is performed.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
import time
from collections import Counter
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import lru_cache, partial
from pathlib import Path
from types import ModuleType
from unittest.mock import patch

from stove0_target_protocol import TargetJobStatus, TargetProgress
from stove0_target_support.execution import TargetExecutionSession
from stove0_target_support.persistent import PersistentTargetService

REPO = Path(__file__).resolve().parents[2]
SOURCE_TEST = (
    REPO / "some-implementations/stove0/packages/target-support/tests/test_target_support.py"
)


@lru_cache(maxsize=1)
def source_fixture() -> ModuleType:
    """Use the current source's exact canonical request/result fixture builders."""
    if str(REPO) not in sys.path:
        sys.path.insert(0, str(REPO))
    module_name = "stove0_998_maintained_target_fixture"
    spec = importlib.util.spec_from_file_location(module_name, SOURCE_TEST)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@dataclass
class _LiveProgress:
    identity: tuple[int, str, str]
    progress: TargetProgress | None = None


class VolatileProgressTarget(PersistentTargetService):
    """One bounded volatile sample per active original invocation generation.

    This is an explicit proposal on the actual owner, not a new durable executor.
    Production should construct the generation-bound callback directly in
    _prepare_dispatch; the reference replaces that private callback after the
    original constructor returns, before its executor can run.
    """

    def __init__(self, **kwargs):
        self._live_progress: dict[str, _LiveProgress] = {}
        super().__init__(**kwargs)

    @staticmethod
    def _identity(status: TargetJobStatus) -> tuple[int, str, str]:
        return status.attempt, status.request_sha256, status.plan_sha256

    def bind_progress(self, job_id: str, attempt: int) -> Callable[[TargetProgress], None]:
        with self._lock:
            current = self._load_status(job_id)
            if current is None or current.state != "running" or current.attempt != attempt:
                raise ValueError("progress requires the current original running attempt")
            if (
                job_id not in self._live_progress
                and len(self._live_progress) >= self.maximum_workers
            ):
                raise ValueError("progress exceeds the original active-worker admission")
            slot = _LiveProgress(self._identity(current))
            self._live_progress[job_id] = slot

        def report(progress: TargetProgress) -> None:
            with self._lock:
                if self._closing or self._live_progress.get(job_id) is not slot:
                    return
                # Snapshot only the four advisory fields. No file encoding,
                # journal, authoring, provider action or persistence happens here.
                slot.progress = TargetProgress.model_validate(progress.model_dump())

        return report

    def _prepare_dispatch(self, request, attempt, cancellation, permit):
        with self._lock:
            execute = super()._prepare_dispatch(request, attempt, cancellation, permit)
            if execute is not None:
                job_id = request.declaration.job_id
                self._sessions[job_id]._progress_callback = self.bind_progress(job_id, attempt)
            return execute

    def _commit_status(self, status: TargetJobStatus) -> TargetJobStatus:
        with self._lock:
            try:
                selected = super()._commit_status(status)
            except BaseException:
                # An uncertain original write cannot leave an advisory authority.
                self._live_progress.pop(status.job_id, None)
                raise
            slot = self._live_progress.get(status.job_id)
            if slot is not None:
                if selected.state != "running" or self._identity(selected) != slot.identity:
                    self._live_progress.pop(status.job_id, None)
                else:
                    slot.progress = None
            return selected

    def get_job(self, job_id: str) -> TargetJobStatus:
        with self._lock:
            status = super().get_job(job_id)
            slot = self._live_progress.get(job_id)
            if slot is None:
                return status
            if status.state != "running" or self._identity(status) != slot.identity:
                self._live_progress.pop(job_id, None)
                return status
            if slot.progress is None:
                return status
            return status.model_copy(update={"progress": slot.progress.model_copy(deep=True)})

    def close(self) -> None:
        try:
            super().close()
        finally:
            self._live_progress.clear()


def open_target(root: Path, *, proposed: bool, execute=None) -> PersistentTargetService:
    fixtures = source_fixture()
    operation, descriptor, _request = fixtures._request()

    def no_dispatch(*_args):
        raise AssertionError("the isolated progress experiment cannot dispatch an external effect")

    selected = VolatileProgressTarget if proposed else PersistentTargetService
    return selected(
        descriptor=descriptor,
        operations={operation.id: operation},
        primary_state=fixtures.PRIMARY_STATE_POLICY,
        state_root=root,
        execute=execute or no_dispatch,
    )


def seed_original_running(service: PersistentTargetService, *, attempt: int = 1):
    """Install only the real owner's original acceptance and execution marker."""
    _operation, _descriptor, request = source_fixture()._request()
    service._write_model(service._accepted_path(request.declaration.job_id), request.accepted())
    status = service._status(request, state="running", attempt=attempt, phase="starting")
    service._commit_status(status)
    return request, status


@dataclass
class WorkSample:
    counts: Counter = field(default_factory=Counter)
    elapsed_ns: int = 0

    def document(self):
        counts = dict.fromkeys(("state_writes", "status_writes", "state_bytes", "fsync_calls"), 0)
        counts.update(self.counts)
        return {"elapsed_ns": self.elapsed_ns, **counts}


@contextmanager
def observe_work():
    """Observe real writer/fsync calls; counts are not physical-device I/O."""
    sample = WorkSample()
    write_model, fsync = PersistentTargetService._write_model, os.fsync

    def wrote(service, path, model):
        sample.counts["state_write_attempts"] += 1
        result = write_model(service, path, model)
        sample.counts["state_writes"] += 1
        sample.counts["status_writes"] += int(path.name.endswith(".status.json"))
        sample.counts["state_bytes"] += path.stat().st_size
        return result

    def synced(descriptor):
        sample.counts["fsync_calls"] += 1
        return fsync(descriptor)

    with (
        patch.object(PersistentTargetService, "_write_model", wrote),
        patch.object(os, "fsync", synced),
    ):
        started = time.perf_counter_ns()
        try:
            yield sample
        finally:
            sample.elapsed_ns = time.perf_counter_ns() - started


def run_pair(root: Path, *, callbacks: int = 64) -> dict:
    if type(callbacks) is not int or callbacks < 1:
        raise ValueError("the fixture callback count must be a positive exact integer")
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    fixtures = source_fixture()
    operation, _descriptor, request = fixtures._request()
    success = fixtures._success_status(operation, request)
    fixtures.validate_status_against_request(success, request, operation)
    samples = {}
    for mode in ("current", "proposed"):
        service = open_target(root / mode, proposed=mode == "proposed")
        try:
            with observe_work() as mandatory_start:
                seeded, running = seed_original_running(service)
                assert seeded == request
            if isinstance(service, VolatileProgressTarget):
                callback = service.bind_progress(running.job_id, running.attempt)
            else:
                callback = partial(service._report_progress, running.job_id, running.attempt)
            session = TargetExecutionSession(
                request,
                running.attempt,
                service._runtime_registry,
                state_root=service.state_root,
                primary_state=fixtures.PRIMARY_STATE_POLICY,
                progress_callback=callback,
                completion_callback=service._commit_status,
            )
            with observe_work() as progress:
                for completed in range(1, callbacks + 1):
                    session.report_progress(
                        TargetProgress(phase="transforming", completed=completed, total=callbacks)
                    )
            with observe_work() as status_reads:
                for _ in range(4):
                    assert service.get_job(running.job_id).progress.completed == callbacks
            with observe_work() as mandatory_result:
                session.record_completed(success)
                # A duplicate original result stays idempotent; stale progress cannot replace it.
                session.record_completed(success)
                session.report_progress(TargetProgress(phase="late", completed=0))
                assert service.get_job(running.job_id) == success
            samples[mode] = {
                "original_acceptance_and_running": mandatory_start.document(),
                "advisory_callbacks": progress.document(),
                "status_reads": status_reads.document(),
                "original_result_and_retry": mandatory_result.document(),
                "terminal_status": service.get_job(running.job_id).model_dump(mode="json"),
            }
        finally:
            service.close()
    assert samples["current"]["terminal_status"] == samples["proposed"]["terminal_status"]
    assert samples["current"]["advisory_callbacks"]["state_writes"] == callbacks
    assert samples["proposed"]["advisory_callbacks"]["state_writes"] == 0
    return {
        "format": "riverhog-998-stove0-progress-experiment/v1",
        "callbacks": callbacks,
        "source_scope": (
            "actual PersistentTargetService, TargetExecutionSession and POSIX state writer"
        ),
        "fixture_scope": (
            "maintained exact request/result builders; no Core claim or external effect"
        ),
        "limits": (
            "reference volatile overlay only; no optional checkpoint, full workflow "
            "or physical I/O claim"
        ),
        "samples": samples,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--callbacks", type=int, default=64)
    arguments = parser.parse_args()
    print(
        json.dumps(
            run_pair(arguments.root, callbacks=arguments.callbacks), indent=2, sort_keys=True
        )
    )
