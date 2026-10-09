"""Component-owned, restart-safe delivery of exact observation invocations."""

from __future__ import annotations

import logging
import os
import threading
import time
from collections.abc import Callable
from pathlib import Path

from pydantic import BaseModel
from stove0_extension_support import ExclusiveStateOwner, ExecutionAdmission, ExecutionPermit
from stove0_extension_support.authority import validate_live_root_authority
from stove0_extension_support.dispatch import BoundedExecutionDispatcher, ExecutionDispatch
from stove0_extension_support.retention import TerminalStateRetention
from stove0_extension_support.state import write_state_model
from stove0_observer_protocol import (
    AcceptedObservationJob,
    ContentObservationInvocation,
    ObservationJobStatus,
    ObserverDescriptor,
    SemanticValidatorProvider,
    accept_observation_result,
    require_semantic_validators,
    validate_observation_request,
    validate_observation_status,
)

from stove0_observer_support.configuration import (
    terminal_state_retention_seconds as configured_retention_seconds,
)
from stove0_observer_support.results import ContentObservationResultBuilder
from stove0_observer_support.runtime import ContentObservationRuntime, ContentObserver

_LOG = logging.getLogger(__name__)


class ObserverServiceError(RuntimeError):
    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


class ObservationExecutionCanceled(RuntimeError):
    """Cancellation observed by the running, read-only observation."""


class PersistentObserverService:
    """Store declarations and outcomes, keeping credentials only in memory.

    A queued restart needs a fresh PUT before dispatch. A started read-only
    observation may be retried only by an explicit refreshed PUT. Pending jobs
    hold no runtime, workspace, worker, or execution permit.
    """

    def __init__(
        self,
        observer: ContentObserver,
        *,
        state_root: Path,
        semantic_validators: SemanticValidatorProvider | None = None,
        maximum_workers: int = 1,
        maximum_pending_jobs: int = 256,
        execution_admission: ExecutionAdmission | None = None,
        admission_probe_seconds: float = 1.0,
        admission_retry_seconds: float = 1.0,
        terminal_state_retention_seconds: int | None = None,
        runtime_factory: Callable[
            ..., ContentObservationRuntime
        ] = ContentObservationRuntime.from_invocation,
    ) -> None:
        if isinstance(maximum_pending_jobs, bool) or maximum_pending_jobs < 1:
            raise ValueError("observer queue budget must be positive")
        retention = (
            configured_retention_seconds()
            if terminal_state_retention_seconds is None
            else terminal_state_retention_seconds
        )
        if isinstance(retention, bool) or retention < 1:
            raise ValueError("observer terminal-state retention must be positive")
        self.terminal_state_retention_seconds = retention
        if state_root.is_symlink():
            raise ValueError("observer state root must not be a symlink")
        self.state_root = state_root.resolve()
        self.state_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.state_root, 0o700)
        self.observer = observer
        self._descriptor = observer.descriptor()
        require_semantic_validators(semantic_validators, self._descriptor)
        self.semantic_validators = semantic_validators
        self.maximum_pending_jobs = maximum_pending_jobs
        self._runtime_factory = runtime_factory
        self._lock = threading.RLock()
        self._closing = False
        self._metadata_shutdown: list[Callable[[], None]] = []
        self._pending: dict[str, ContentObservationInvocation] = {}
        self._invocations: dict[str, ContentObservationInvocation] = {}
        self._runtime_contexts: dict[str, dict[str, object]] = {}
        self._runtimes: dict[str, ContentObservationRuntime] = {}
        self._shutdown: set[str] = set()
        self._nonterminal = 0
        self._owner = ExclusiveStateOwner(self.state_root)
        try:
            self._recover()
            self._nonterminal = sum(
                ObservationJobStatus.model_validate_json(path.read_bytes()).state != "completed"
                for path in self.state_root.glob("*.status.json")
            )
            self.prune_terminal_state()
            self._dispatch = BoundedExecutionDispatcher(
                state_owner=self._owner,
                maximum_workers=maximum_workers,
                admission=execution_admission,
                probe_seconds=admission_probe_seconds,
                retry_seconds=admission_retry_seconds,
            )
            self._retention = TerminalStateRetention(
                self.state_root,
                self._prune_terminal_path,
                interval_seconds=min(60, self.terminal_state_retention_seconds),
            )
        except BaseException:
            self._owner.close()
            raise

    def descriptor(self) -> ObserverDescriptor:
        return self._descriptor

    def _validate(self, accepted: AcceptedObservationJob) -> None:
        try:
            validate_observation_request(accepted.request, self._descriptor)
        except ValueError as exc:
            raise ObserverServiceError(400, "invalid_observation_request", str(exc)) from exc

    def put_job(self, invocation: ContentObservationInvocation) -> ObservationJobStatus:
        accepted = invocation.accepted()
        self._validate(accepted)
        with self._lock:
            if self._closing:
                raise ObserverServiceError(
                    503, "admission_unavailable", "observer service is closing"
                )
            existing = self._load(accepted.job_id, "accepted", AcceptedObservationJob)
            if existing is not None and existing != accepted:
                raise ObserverServiceError(
                    409, "job_request_mismatch", "observer invocation changed"
                )
            status = self._load(accepted.job_id, "status", ObservationJobStatus)
            if status is not None and status.state == "completed":
                return status
            context = invocation.runtime.model_dump(mode="json", exclude={"capability_token"})
            prior = self._runtime_contexts.get(accepted.job_id)
            if prior is not None and prior != context:
                raise ObserverServiceError(
                    409, "observer_runtime_mismatch", "active observer runtime changed"
                )
            if existing is None:
                if self._nonterminal >= self.maximum_pending_jobs:
                    raise ObserverServiceError(
                        503, "admission_unavailable", "observer admission queue is full"
                    )
                self._write(accepted.job_id, "accepted", accepted)
            if status is None:
                status = self._commit(
                    ObservationJobStatus(
                        job_id=accepted.job_id,
                        request_id=accepted.request.request_id,
                        attempt=1,
                        state="queued",
                    )
                )
            canceled = self._load(accepted.job_id, "cancel", AcceptedObservationJob)
            if canceled is not None:
                return self._cancel_stopped(accepted, status)
            self._runtime_contexts[accepted.job_id] = context
            self._invocations[accepted.job_id] = invocation
            runtime = self._runtimes.get(accepted.job_id)
            if runtime is not None:
                runtime.refresh_capability(invocation.runtime.capability_token)
            if status.state == "interrupted":
                status = self._commit(
                    status.model_copy(update={"state": "queued", "attempt": status.attempt + 1})
                )
            if status.state == "queued":
                self._pending[accepted.job_id] = invocation
                self._enqueue(invocation, status.attempt)
            return status

    def get_job(self, job_id: str) -> ObservationJobStatus:
        with self._lock:
            status = self._load(job_id, "status", ObservationJobStatus)
            if status is None:
                raise ObserverServiceError(404, "job_not_found", "observer job was not found")
            return status

    def cancel_job(self, accepted: AcceptedObservationJob) -> ObservationJobStatus:
        self._validate(accepted)
        with self._lock:
            existing = self._load(accepted.job_id, "accepted", AcceptedObservationJob)
            if existing is not None and existing != accepted:
                raise ObserverServiceError(
                    409, "job_request_mismatch", "observer cancellation changed"
                )
            status = self._load(accepted.job_id, "status", ObservationJobStatus)
            if status is not None and status.state == "completed":
                return status
            # Persist before acceptance so a delayed first PUT cannot launch.
            self._write(accepted.job_id, "cancel", accepted)
            if existing is None:
                self._write(accepted.job_id, "accepted", accepted)
            if status is None:
                status = ObservationJobStatus(
                    job_id=accepted.job_id,
                    request_id=accepted.request.request_id,
                    attempt=1,
                    state="queued",
                )
            self._dispatch.cancel(accepted.job_id)
            if status.state in {"queued", "interrupted"}:
                self._pending.pop(accepted.job_id, None)
                self._runtime_contexts.pop(accepted.job_id, None)
                self._invocations.pop(accepted.job_id, None)
                return self._cancel_stopped(accepted, status)
            return self._commit(status.model_copy(update={"state": "canceling"}))

    def _cancel_stopped(
        self, accepted: AcceptedObservationJob, status: ObservationJobStatus
    ) -> ObservationJobStatus:
        if status.state not in {"queued", "interrupted"}:
            return status
        result = ContentObservationResultBuilder(self._descriptor, accepted.request).canceled()
        return self._commit(status.model_copy(update={"state": "completed", "result": result}))

    def _enqueue(self, invocation: ContentObservationInvocation, attempt: int) -> None:
        job_id = invocation.job_id
        self._dispatch.enqueue(
            job_id,
            ExecutionDispatch(
                invocation_id=job_id,
                attempt=attempt,
                authorize=lambda deadline: self._validate_live_authority(
                    invocation, deadline=deadline
                ),
                prepare=lambda canceled, permit: self._prepare(
                    invocation, attempt, canceled, permit
                ),
                deferred=lambda: None,
                failed=lambda error: self._dispatch_failed(invocation, attempt, error),
                finished=lambda: self._finished(job_id),
            ),
        )

    def _validate_live_authority(
        self, invocation: ContentObservationInvocation, *, deadline: float
    ) -> None:
        validate_live_root_authority(
            base_url=invocation.runtime.riverhog_base_url,
            capability_token=invocation.runtime.capability_token,
            allow_insecure_http=invocation.runtime.allow_insecure_http,
            root=invocation.request.subjects[0].collection.to_identity(),
            deadline=deadline,
        )

    def _prepare(
        self,
        invocation: ContentObservationInvocation,
        attempt: int,
        canceled: threading.Event,
        _permit: ExecutionPermit,
    ) -> Callable[[], object] | None:
        job_id = invocation.job_id
        with self._lock:
            status = self._load(job_id, "status", ObservationJobStatus)
            if (
                self._closing
                or status is None
                or status.state != "queued"
                or status.attempt != attempt
                or self._pending.get(job_id) != invocation
            ):
                return None
            if (
                canceled.is_set()
                or self._load(job_id, "cancel", AcceptedObservationJob) is not None
            ):
                self._cancel_stopped(invocation.accepted(), status)
                return None
            self._pending.pop(job_id, None)
            self._commit(status.model_copy(update={"state": "running"}))
        return lambda: self._run(invocation, attempt, canceled)

    def _run(
        self, invocation: ContentObservationInvocation, attempt: int, canceled: threading.Event
    ) -> None:
        job_id = invocation.job_id
        deadline = time.monotonic() + invocation.request.timeout_seconds

        def check() -> None:
            if canceled.is_set():
                raise ObservationExecutionCanceled("observer execution was canceled")
            if time.monotonic() >= deadline:
                raise TimeoutError("observer execution exceeded its deadline")

        builder = ContentObservationResultBuilder(self._descriptor, invocation.request)
        result = None
        try:
            check()
            with self._lock:
                invocation = self._invocations.get(job_id, invocation)
            with self._runtime_factory(invocation, cancellation_check=check) as runtime:
                with self._lock:
                    fresh = self._invocations.get(job_id, invocation)
                    if fresh.runtime.capability_token != invocation.runtime.capability_token:
                        runtime.refresh_capability(fresh.runtime.capability_token)
                    self._runtimes[job_id] = runtime
                produced = self.observer.observe(invocation.request, runtime)
                accept_observation_result(
                    produced, invocation.request, self._descriptor, self.semantic_validators
                )
                result = produced
                # Complete validated evidence wins cancellation and cleanup errors.
                with self._lock:
                    self._commit(
                        ObservationJobStatus(
                            job_id=job_id,
                            request_id=invocation.request.request_id,
                            attempt=attempt,
                            state="completed",
                            result=result,
                        )
                    )
        except ObservationExecutionCanceled:
            result = builder.canceled()
        except Exception as exc:
            if result is None:
                result = builder.failed(
                    code="observer-execution",
                    message=f"observer execution failed: {type(exc).__name__}",
                    retryable=True,
                )
            else:
                _LOG.warning("observer cleanup failed job=%s cause=%s", job_id, type(exc).__name__)
        finally:
            with self._lock:
                self._runtimes.pop(job_id, None)
                self._runtime_contexts.pop(job_id, None)
                self._invocations.pop(job_id, None)
                if (
                    job_id in self._shutdown
                    and self._load(job_id, "cancel", AcceptedObservationJob) is None
                ):
                    self._commit(
                        ObservationJobStatus(
                            job_id=job_id,
                            request_id=invocation.request.request_id,
                            attempt=attempt,
                            state="interrupted",
                        )
                    )
                elif result is not None:
                    if canceled.is_set() and result.state != "observed":
                        result = builder.canceled()
                    self._commit(
                        ObservationJobStatus(
                            job_id=job_id,
                            request_id=invocation.request.request_id,
                            attempt=attempt,
                            state="completed",
                            result=result,
                        )
                    )

    def _dispatch_failed(
        self, invocation: ContentObservationInvocation, attempt: int, error: BaseException
    ) -> None:
        with self._lock:
            status = self._load(invocation.job_id, "status", ObservationJobStatus)
            if (
                status is not None
                and status.attempt == attempt
                and status.state in {"running", "canceling"}
            ):
                self._commit(status.model_copy(update={"state": "interrupted"}))
        _LOG.warning(
            "observer dispatch interrupted job=%s cause=%s", invocation.job_id, type(error).__name__
        )

    def _finished(self, job_id: str) -> None:
        with self._lock:
            self._shutdown.discard(job_id)
        self._retention.wake()

    def prune_terminal_state(self, *, now: float | None = None) -> dict[str, int]:
        observed = time.time() if now is None else now
        totals = {"jobs": 0, "bytes": 0}
        for path in self.state_root.glob("*.status.json"):
            removed = self._prune_terminal_path(path, observed)
            for key in totals:
                totals[key] += removed[key]
        return totals

    def _prune_terminal_path(self, path: Path, now: float) -> dict[str, int]:
        with self._lock:
            if path.is_symlink():
                raise ValueError("observer state paths must not be symlinks")
            try:
                identity = path.stat()
            except FileNotFoundError:
                return {"jobs": 0, "bytes": 0}
            if identity.st_mtime > now - self.terminal_state_retention_seconds:
                return {"jobs": 0, "bytes": 0}
            job_id = path.name.removesuffix(".status.json")
            dispatch = getattr(self, "_dispatch", None)
            if dispatch is not None and job_id in dispatch.active_keys:
                return {"jobs": 0, "bytes": 0}
            status = self._load(job_id, "status", ObservationJobStatus)
            if status is None or status.state != "completed":
                return {"jobs": 0, "bytes": 0}
            removed = 0
            for kind in ("status", "accepted", "cancel"):
                selected = self._path(job_id, kind)
                if selected.is_symlink():
                    raise ValueError("observer state paths must not be symlinks")
                if selected.exists():
                    removed += selected.stat().st_size
                    selected.unlink()
            directory = os.open(self.state_root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            return {"jobs": 1, "bytes": removed}

    def register_metadata_shutdown(self, close: Callable[[], None]) -> None:
        with self._lock:
            if self._closing:
                raise RuntimeError("component metadata ownership is shutting down")
            self._metadata_shutdown.append(close)

    def close(self) -> None:
        with self._lock:
            self._closing = True
            self._shutdown.update(self._dispatch.active_keys)
        self._retention.close()
        try:
            for close in self._metadata_shutdown:
                close()
            self._dispatch.close()
        finally:
            with self._lock:
                self._pending.clear()
                self._runtime_contexts.clear()
                self._invocations.clear()
            self._owner.close()

    def _path(self, job_id: str, kind: str) -> Path:
        if len(job_id) != 64 or any(c not in "0123456789abcdef" for c in job_id):
            raise ValueError("observation job ID must be a lowercase SHA-256")
        return self.state_root / f"{job_id}.{kind}.json"

    def _load[Model: BaseModel](self, job_id: str, kind: str, model: type[Model]) -> Model | None:
        path = self._path(job_id, kind)
        if path.is_symlink():
            raise ValueError("observer state paths must not be symlinks")
        return model.model_validate_json(path.read_bytes()) if path.exists() else None

    def _write(self, job_id: str, kind: str, model: BaseModel) -> None:
        write_state_model(self.state_root, self._path(job_id, kind), model)

    def _commit(self, status: ObservationJobStatus) -> ObservationJobStatus:
        with self._lock:
            current = self._load(status.job_id, "status", ObservationJobStatus)
            if current is not None and (
                current.state == "completed" or current.attempt > status.attempt
            ):
                return current
            if current == status:
                return current
            self._write(status.job_id, "status", status)
            self._nonterminal += int(status.state != "completed") - int(
                current is not None and current.state != "completed"
            )
            return status

    def _recover(self) -> None:
        for path in self.state_root.glob("*.cancel.json"):
            if path.is_symlink():
                raise ValueError("observer cancellation paths must not be symlinks")
            accepted = AcceptedObservationJob.model_validate_json(path.read_bytes())
            if path != self._path(accepted.job_id, "cancel"):
                raise ValueError("observer cancellation identity differs from its path")
            existing = self._load(accepted.job_id, "accepted", AcceptedObservationJob)
            if existing is None:
                self._write(accepted.job_id, "accepted", accepted)
            elif existing != accepted:
                raise ValueError("observer cancellation differs from its accepted declaration")
        for path in self.state_root.glob("*.accepted.json"):
            if path.is_symlink():
                raise ValueError("observer accepted paths must not be symlinks")
            accepted = AcceptedObservationJob.model_validate_json(path.read_bytes())
            if path != self._path(accepted.job_id, "accepted"):
                raise ValueError("observer accepted identity differs from its path")
            self._validate(accepted)
            status = self._load(accepted.job_id, "status", ObservationJobStatus)
            if status is None:
                status = ObservationJobStatus(
                    job_id=accepted.job_id,
                    request_id=accepted.request.request_id,
                    attempt=1,
                    state="queued",
                )
                self._write(accepted.job_id, "status", status)
            validate_observation_status(
                status, accepted, self._descriptor, self.semantic_validators
            )
            if status.state in {"running", "canceling"}:
                if (
                    status.state == "canceling"
                    and self._load(accepted.job_id, "cancel", AcceptedObservationJob) is None
                ):
                    self._write(accepted.job_id, "cancel", accepted)
                self._write(
                    accepted.job_id, "status", status.model_copy(update={"state": "interrupted"})
                )
        for path in self.state_root.glob("*.status.json"):
            if path.is_symlink():
                raise ValueError("observer status paths must not be symlinks")
            status = ObservationJobStatus.model_validate_json(path.read_bytes())
            retained = self._load(status.job_id, "accepted", AcceptedObservationJob)
            if path != self._path(status.job_id, "status") or retained is None:
                raise ValueError("observer status has no exact accepted declaration")
