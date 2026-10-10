"""Reusable restart-safe execution kernel for independently owned targets."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import secrets
import subprocess
import threading
import time
import traceback
from collections import OrderedDict
from collections.abc import Callable, Mapping
from itertools import chain
from pathlib import Path
from typing import Any, Final

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError as JsonSchemaValidationError
from riverhog_client.processing import ClaimedCollectionRuntimeRegistry
from riverhog_protocol import DownloadAllowanceExceeded, RiverhogError, ServiceUnavailable
from stove0_extension_support import (
    ExclusiveStateOwner,
    ExecutionAdmission,
    ExecutionPermit,
)
from stove0_extension_support.authority import validate_live_root_authority
from stove0_extension_support.dispatch import BoundedExecutionDispatcher, ExecutionDispatch
from stove0_extension_support.retention import TerminalStateRetention
from stove0_target_protocol import (
    EFFECT_TARGET_PROTOCOL,
    JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
    AcceptedTargetJob,
    EffectPlan,
    EffectPlanPayload,
    OperationContract,
    TargetDeclaration,
    TargetDescriptor,
    TargetFailure,
    TargetInapplicable,
    TargetJobRequest,
    TargetJobStatus,
    TargetOperationSupport,
    TargetPreflightRequest,
    TargetPreflightResponse,
    TargetProgress,
    TransformPlan,
    TransformPlanPayload,
    validate_declaration_against_operation,
)

from stove0_target_support.completion_checkpoint import TargetCompletionCheckpoint
from stove0_target_support.execution import TargetExecutionSession
from stove0_target_support.http_binding import TargetServiceError
from stove0_target_support.output_checkpoint import TargetOutputCheckpoint
from stove0_target_support.runtime import TargetExecutionRuntime

_TERMINAL_STATES: Final = frozenset({"inapplicable", "succeeded", "failed", "canceled"})
DEFAULT_TERMINAL_STATE_RETENTION_SECONDS: Final = 30 * 24 * 60 * 60
_LOGGER = logging.getLogger(__name__)

JobExecutor = Callable[
    [TargetJobRequest, int, threading.Event, TargetExecutionSession],
    TargetJobStatus,
]
IntentSemanticValidator = Callable[[Mapping[str, object]], None]


class TargetExecutionCanceled(RuntimeError):
    """The owning target observed cancellation before publishing output."""


class TargetExecutionInapplicable(RuntimeError):
    """The exact declared input cannot be handled by this target."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class TargetExecutionFailure(RuntimeError):
    """The target classified a non-content execution failure explicitly."""

    def __init__(self, code: str, message: str, *, retryable: bool) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable


class TargetEffectCommitUncertain(RuntimeError):
    """An external-effect attempt may have committed and must not be repeated."""


class PersistentTargetService:
    """Persist non-secret job identity and converge identical restart requests.

    Capability tokens remain only in process memory. Accepted declarations and
    statuses are atomically persisted beneath one target-owned state root.
    Queued jobs remain dormant through restart and require refreshed invocation
    authority before dispatch. A started job found after process loss becomes
    ``interrupted``; uncertain effects are never automatically repeated.
    """

    def __init__(
        self,
        *,
        descriptor: TargetDescriptor,
        operations: Mapping[str, OperationContract],
        state_root: Path,
        execute: JobExecutor,
        intent_semantic_validators: Mapping[str, IntentSemanticValidator] | None = None,
        maximum_workers: int = 1,
        maximum_pending_jobs: int = 256,
        execution_admission: ExecutionAdmission | None = None,
        admission_probe_seconds: float = 1.0,
        admission_retry_seconds: float = 1.0,
        terminal_state_retention_seconds: int = DEFAULT_TERMINAL_STATE_RETENTION_SECONDS,
    ) -> None:
        self._descriptor = descriptor
        self._operations = dict(operations)
        if set(self._operations) != {item.operation_id for item in descriptor.operations}:
            raise ValueError(
                "target operation implementations differ from the advertised descriptor"
            )
        for support in descriptor.operations:
            operation = self._operations[support.operation_id]
            if (
                operation.contract_sha256 != support.operation_contract_sha256
                or operation.result_kind != support.result_kind
            ):
                raise ValueError("target operation implementation differs from its support binding")
        self._intent_semantic_validators = dict(intent_semantic_validators or {})
        required_semantic_validators = {
            operation.intent_semantics.profile_sha256
            for operation in self._operations.values()
            if operation.intent_semantics.profile_sha256
            != JSON_SCHEMA_ONLY_SEMANTIC_PROFILE.profile_sha256
        }
        if set(self._intent_semantic_validators) != required_semantic_validators:
            raise ValueError(
                "target semantic validators differ from the advertised operation profiles"
            )
        self.state_root = state_root.resolve()
        self.state_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.state_root, 0o700)
        if self.state_root.is_symlink():
            raise ValueError("target state root must not be a symlink")
        if (
            isinstance(terminal_state_retention_seconds, bool)
            or terminal_state_retention_seconds < 1
        ):
            raise ValueError("target terminal-state retention must be positive")
        self.terminal_state_retention_seconds = terminal_state_retention_seconds
        if isinstance(maximum_workers, bool) or maximum_workers < 1:
            raise ValueError("target execution concurrency must be positive")
        if isinstance(maximum_pending_jobs, bool) or maximum_pending_jobs < 1:
            raise ValueError("target queue budget must be positive")
        self.maximum_workers = maximum_workers
        self.maximum_pending_jobs = maximum_pending_jobs
        self._state_owner = ExclusiveStateOwner(self.state_root)
        self._execute = execute
        self._lock = threading.RLock()
        self._accepted_cache: OrderedDict[tuple[str, str], tuple[int, AcceptedTargetJob]] = (
            OrderedDict()
        )
        self._accepted_cache_bytes = 0
        self._accepted_cache_budget = 8 * 1024 * 1024
        self._accepted_cache_entries = 16
        self._cancel: dict[str, threading.Event] = {}
        self._operator_canceled: set[str] = set()
        self._shutdown_interrupted: set[str] = set()
        self._pending_submissions: dict[str, tuple[TargetJobRequest, int]] = {}
        self._nonterminal_job_count = 0
        self._closing = False
        self._metadata_shutdown: list[Callable[[], None]] = []
        self._runtime_registry = ClaimedCollectionRuntimeRegistry()
        self._runtime_contexts: dict[str, dict[str, object]] = {}
        self._runtime_token_fingerprints: dict[str, bytes] = {}
        self._sessions: dict[str, TargetExecutionSession] = {}
        try:
            self._recover_interrupted()
        except BaseException:
            self._state_owner.close()
            raise
        self._nonterminal_job_count = sum(
            TargetJobStatus.model_validate_json(path.read_text(encoding="utf-8")).state
            not in _TERMINAL_STATES
            for path in self.state_root.glob("*.status.json")
        )
        self.prune_terminal_state()
        self._dispatch = BoundedExecutionDispatcher(
            state_owner=self._state_owner,
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

    def descriptor(self) -> TargetDescriptor:
        return self._descriptor

    def preflight(self, request: TargetPreflightRequest) -> TargetPreflightResponse:
        return self._seal_preflight(request, execution_parameters={})

    def _seal_preflight(
        self, request: TargetPreflightRequest, *, execution_parameters: Mapping[str, Any]
    ) -> TargetPreflightResponse:
        if request.protocol != self._descriptor.protocol:
            raise TargetServiceError(409, "target_protocol_mismatch", "target protocol changed")
        operation = self._operation(request.operation_id)
        support = self._descriptor.support_for(request.operation_id)
        if (
            request.operation_contract_sha256
            not in {
                operation.contract_sha256,
                support.operation_contract_sha256,
            }
            or operation.contract_sha256 != support.operation_contract_sha256
        ):
            raise TargetServiceError(409, "operation_contract_mismatch", "operation changed")
        self._validate_operation_request(request, operation, support)
        plan_fields: dict[str, object] = {
            "invocation_sha256": request.invocation_sha256,
            "execution_parameters": dict(execution_parameters),
            "operation_id": request.operation_id,
            "operation_contract_sha256": request.operation_contract_sha256,
            "inputs": request.inputs,
            "intent": request.intent,
            "target_options": request.target_options,
            "input_groups": request.input_groups,
            "target_implementation_id": self._descriptor.implementation_id,
            "target_descriptor_sha256": self._descriptor.descriptor_sha256,
            "observation_result_sha256s": tuple(
                sorted(item.result.result_sha256 for item in request.observations)
            ),
        }
        plan = (
            EffectPlan.seal(EffectPlanPayload.model_validate(plan_fields))
            if self._descriptor.protocol == EFFECT_TARGET_PROTOCOL
            else TransformPlan.seal(TransformPlanPayload.model_validate(plan_fields))
        )
        return TargetPreflightResponse(descriptor=self._descriptor, plan=plan)

    def put_job(self, request: TargetJobRequest) -> TargetJobStatus:
        job_id = request.declaration.job_id
        self._validate_accepted(request.accepted())
        with self._lock:
            if self._closing:
                raise TargetServiceError(503, "admission_unavailable", "target service is closing")
            existing = self._load_accepted(job_id)
            if existing is not None and not secrets.compare_digest(
                existing.request_sha256, request.request_sha256
            ):
                raise TargetServiceError(
                    409,
                    "job_request_mismatch",
                    "target job identity is already bound to another declaration",
                )
            if existing is None:
                if self._nonterminal_job_count >= self.maximum_pending_jobs:
                    raise TargetServiceError(
                        503, "admission_unavailable", "target admission queue is full"
                    )
                self._write_model(self._accepted_path(job_id), request.accepted())
            status = self._load_status(job_id)
            if self._load_cancellation(job_id) is not None:
                if status is None:
                    status = self._commit_status(
                        self._status(request, state="canceled", attempt=1, phase="canceled")
                    )
                return self._cancel_stopped(request.accepted(), status)
            return self._refresh_and_enqueue(request, status)

    def _validate_accepted(self, accepted: AcceptedTargetJob) -> None:
        plan = accepted.declaration.plan
        if (
            plan.target_descriptor_sha256 != self._descriptor.descriptor_sha256
            or plan.target_implementation_id != self._descriptor.implementation_id
            or plan.protocol != self._descriptor.protocol
        ):
            raise TargetServiceError(409, "target_descriptor_mismatch", "target descriptor changed")
        operation = self._operation(plan.operation_id)
        support = self._descriptor.support_for(plan.operation_id)
        if (
            plan.operation_contract_sha256
            not in {
                operation.contract_sha256,
                support.operation_contract_sha256,
            }
            or operation.contract_sha256 != support.operation_contract_sha256
        ):
            raise TargetServiceError(409, "operation_contract_mismatch", "operation changed")
        self._validate_operation_request(plan, operation, support)

    def _refresh_and_enqueue(
        self, request: TargetJobRequest, status: TargetJobStatus | None
    ) -> TargetJobStatus:
        job_id = request.declaration.job_id
        with self._lock:
            if status is None or status.state not in _TERMINAL_STATES:
                runtime_context = request.runtime.model_dump(
                    mode="json",
                    exclude={"capability_token"},
                )
                prior_context = self._runtime_contexts.get(job_id)
                if prior_context is not None and prior_context != runtime_context:
                    raise TargetServiceError(
                        409,
                        "target_runtime_mismatch",
                        "active target runtime endpoint changed",
                    )
                session = self._sessions.get(job_id)
                if session is not None:
                    try:
                        session.refresh_callback_access(request.callback_access)
                    except ValueError as exc:
                        raise TargetServiceError(409, "target_runtime_mismatch", str(exc)) from exc
                self._runtime_contexts[job_id] = runtime_context
                token_fingerprint = hashlib.sha256(
                    request.runtime.capability_token.encode()
                ).digest()
                if self._runtime_token_fingerprints.get(job_id) != token_fingerprint:
                    self._runtime_token_fingerprints[job_id] = token_fingerprint
                    self._runtime_registry.refresh(
                        job_id,
                        request.runtime.capability_token,
                    )
                pending = self._pending_submissions.get(job_id)
                if pending is not None:
                    self._pending_submissions[job_id] = (request, pending[1])
            if status is None:
                status = self._status(request, state="queued", attempt=1, phase="queued")
                status = self._commit_status(status)
                self._submit(request, status.attempt)
            elif status.state == "queued":
                self._submit(request, status.attempt)
            elif status.state == "interrupted":
                if self._load_cancellation(job_id) is not None:
                    return self._cancel_stopped(request.accepted(), status)
                if request.declaration.plan.protocol == EFFECT_TARGET_PROTOCOL:
                    return status
                status = self._status(
                    request,
                    state="queued",
                    attempt=status.attempt + 1,
                    phase="restarting",
                )
                status = self._commit_status(status)
                self._submit(request, status.attempt)
            return status

    def get_job(self, job_id: str) -> TargetJobStatus:
        with self._lock:
            status = self._load_status(job_id)
            if status is None:
                raise TargetServiceError(404, "job_not_found", "target job was not found")
            return status

    def cancel_job(self, request: AcceptedTargetJob) -> TargetJobStatus:
        self._validate_accepted(request)
        job_id = request.declaration.job_id
        with self._lock:
            accepted = self._load_accepted(job_id)
            if accepted is not None and accepted != request:
                raise TargetServiceError(
                    409, "job_request_mismatch", "target cancellation differs from accepted request"
                )
            status = self._load_status(job_id)
            if status is not None and status.state in _TERMINAL_STATES:
                return status
            # The exact non-secret tombstone also fences a delayed first PUT.
            self._write_model(self._cancellation_path(job_id), request)
            if accepted is None:
                self._write_model(self._accepted_path(job_id), request)
                accepted = request
            if status is None:
                return self._commit_status(
                    TargetJobStatus(
                        protocol=accepted.declaration.plan.protocol,
                        job_id=job_id,
                        state="canceled",
                        attempt=1,
                        request_sha256=accepted.request_sha256,
                        plan_sha256=accepted.declaration.plan.plan_sha256,
                        progress=TargetProgress(phase="canceled", completed=0),
                    )
                )
            if status.state == "interrupted":
                return self._cancel_stopped(accepted, status)
            self._operator_canceled.add(job_id)
            self._shutdown_interrupted.discard(job_id)
            self._cancel.setdefault(job_id, threading.Event()).set()
            self._dispatch.cancel(job_id)
            if status.state == "queued" and job_id not in self._dispatch.active_keys:
                self._pending_submissions.pop(job_id, None)
                self._cancel.pop(job_id, None)
                self._operator_canceled.discard(job_id)
                self._runtime_contexts.pop(job_id, None)
                self._runtime_token_fingerprints.pop(job_id, None)
                self._runtime_registry.discard(job_id)
                return self._cancel_stopped(accepted, status)
            canceling = TargetJobStatus(
                protocol=accepted.declaration.plan.protocol,
                job_id=job_id,
                state="canceling",
                attempt=status.attempt,
                request_sha256=accepted.request_sha256,
                plan_sha256=accepted.declaration.plan.plan_sha256,
                progress=TargetProgress(phase="canceling", completed=0),
            )
            return self._commit_status(canceling)

    def register_metadata_shutdown(self, close: Callable[[], None]) -> None:
        with self._lock:
            if self._closing:
                raise RuntimeError("component metadata ownership is shutting down")
            self._metadata_shutdown.append(close)

    def close(self) -> None:
        with self._lock:
            self._closing = True
            for job_id in self._dispatch.active_keys:
                if job_id not in self._operator_canceled:
                    self._shutdown_interrupted.add(job_id)
        self._retention.close()
        for close in self._metadata_shutdown:
            close()
        self._dispatch.close()
        with self._lock:
            for job_id in self._pending_submissions:
                self._runtime_contexts.pop(job_id, None)
                self._runtime_token_fingerprints.pop(job_id, None)
                self._runtime_registry.discard(job_id)
            self._pending_submissions.clear()
        self._state_owner.close()

    def prune_terminal_state(self, *, now: float | None = None) -> dict[str, int]:
        """Remove expired terminal jobs; one job owns each state critical section."""
        observed = time.time() if now is None else now
        totals = {"jobs": 0, "bytes": 0}
        for path in self.state_root.glob("*.status.json"):
            removed = self._prune_terminal_path(path, observed)
            for key in totals:
                totals[key] += removed[key]
        return totals

    def _prune_terminal_path(self, status_path: Path, now: float) -> dict[str, int]:
        cutoff = now - self.terminal_state_retention_seconds
        removed_jobs = removed_bytes = 0
        with self._lock:
            if status_path.is_symlink():
                raise ValueError("target state paths must not be symlinks")
            job_id = status_path.name.removesuffix(".status.json")
            dispatch = getattr(self, "_dispatch", None)
            if dispatch is not None and job_id in dispatch.active_keys:
                return {"jobs": 0, "bytes": 0}
            try:
                stat = status_path.stat()
            except FileNotFoundError:
                return {"jobs": 0, "bytes": 0}
            if stat.st_mtime > cutoff:
                return {"jobs": 0, "bytes": 0}
            status = TargetJobStatus.model_validate_json(status_path.read_text(encoding="utf-8"))
            if status.state not in _TERMINAL_STATES:
                return {"jobs": 0, "bytes": 0}
            if status.protocol == EFFECT_TARGET_PROTOCOL and status.state == "succeeded":
                return {"jobs": 0, "bytes": 0}
            completion_path = TargetCompletionCheckpoint.manifest_path(self.state_root, job_id)
            if status.state != "succeeded" and (
                completion_path.exists()
                or TargetOutputCheckpoint.has_job_records(self.state_root, job_id)
            ):
                return {"jobs": 0, "bytes": 0}
            accepted_path = self._accepted_path(job_id)
            removed_bytes += stat.st_size
            if accepted_path.exists():
                if accepted_path.is_symlink():
                    raise ValueError("target state paths must not be symlinks")
                removed_bytes += accepted_path.stat().st_size
            if completion_path.exists():
                if completion_path.is_symlink():
                    raise ValueError("target checkpoint paths must not be symlinks")
                for checkpoint_path in chain(
                    (completion_path,), self.state_root.glob(f"{job_id}.execution-*.bin")
                ):
                    if checkpoint_path.is_symlink():
                        raise ValueError("target checkpoint paths must not be symlinks")
                    removed_bytes += checkpoint_path.stat().st_size
                    checkpoint_path.unlink()
            for checkpoint_path in self.state_root.glob(f"{job_id}.step-*.bin"):
                if checkpoint_path.is_symlink() or not checkpoint_path.is_file():
                    raise ValueError("target step checkpoint must be a regular file")
                removed_bytes += checkpoint_path.stat().st_size
                checkpoint_path.unlink()
            for suffix in ("outputs", "output-members"):
                directory = self.state_root / f"{job_id}.{suffix}"
                if directory.is_symlink():
                    raise ValueError("target output checkpoint directory must not be a symlink")
                if not directory.exists():
                    continue
                with os.scandir(directory) as checkpoints:
                    for entry in checkpoints:
                        if not entry.is_file(follow_symlinks=False):
                            raise ValueError("target output checkpoint must be a regular file")
                        removed_bytes += entry.stat(follow_symlinks=False).st_size
                        os.unlink(entry.path)
                directory.rmdir()
            status_path.unlink(missing_ok=True)
            accepted_path.unlink(missing_ok=True)
            cancellation_path = self._cancellation_path(job_id)
            if cancellation_path.exists():
                if cancellation_path.is_symlink():
                    raise ValueError("target cancellation path must not be a symlink")
                removed_bytes += cancellation_path.stat().st_size
                cancellation_path.unlink()
            removed_jobs += 1
            if removed_jobs:
                directory_descriptor = os.open(
                    self.state_root,
                    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
                )
                try:
                    os.fsync(directory_descriptor)
                finally:
                    os.close(directory_descriptor)
        return {"jobs": removed_jobs, "bytes": removed_bytes}

    def _operation(self, operation_id: str) -> OperationContract:
        try:
            return self._operations[operation_id]
        except KeyError as exc:
            raise TargetServiceError(
                400,
                "unsupported_operation",
                f"target does not support operation: {operation_id}",
            ) from exc

    def _validate_operation_request(
        self,
        declaration: TargetDeclaration,
        operation: OperationContract,
        support: TargetOperationSupport,
    ) -> None:
        try:
            validate_declaration_against_operation(declaration, operation)
            Draft202012Validator(operation.intent_schema.document).validate(declaration.intent)
            Draft202012Validator(support.options_schema.document).validate(
                declaration.target_options
            )
            semantic_validator = self._intent_semantic_validators.get(
                operation.intent_semantics.profile_sha256
            )
            if semantic_validator is not None:
                semantic_validator(declaration.intent)
        except (JsonSchemaValidationError, ValueError) as exc:
            raise TargetServiceError(
                400,
                "invalid_target_request",
                str(exc),
            ) from exc

    def _submit(self, request: TargetJobRequest, attempt: int) -> None:
        job_id = request.declaration.job_id
        self._pending_submissions[job_id] = (request, attempt)
        self._dispatch.enqueue(
            job_id,
            ExecutionDispatch(
                invocation_id=request.request_sha256,
                attempt=attempt,
                authorize=lambda deadline: self._validate_live_authority(
                    request, deadline=deadline
                ),
                prepare=lambda canceled, permit: self._prepare_dispatch(
                    request, attempt, canceled, permit
                ),
                deferred=lambda: self._admission_deferred(job_id, attempt),
                failed=lambda error: self._dispatch_failed(request, attempt, error),
                finished=lambda: self._execution_finished(job_id),
            ),
        )

    def _admission_deferred(self, job_id: str, attempt: int) -> None:
        with self._lock:
            status = self._load_status(job_id)
            if status is not None and status.state == "queued" and status.attempt == attempt:
                self._commit_status(
                    status.model_copy(
                        update={
                            "progress": TargetProgress(phase="waiting-for-admission", completed=0)
                        }
                    )
                )

    def _dispatch_failed(
        self, request: TargetJobRequest, attempt: int, error: BaseException
    ) -> None:
        with self._lock:
            current = self._load_status(request.declaration.job_id)
            if (
                current is not None
                and current.state in {"running", "canceling"}
                and current.attempt == attempt
            ):
                self._commit_status(
                    self._status(
                        request, state="interrupted", attempt=attempt, phase="dispatch-interrupted"
                    )
                )
        _LOGGER.warning(
            "target dispatch interrupted job=%s cause=%s",
            request.declaration.job_id,
            type(error).__name__,
        )

    def _validate_live_authority(self, request: TargetJobRequest, *, deadline: float) -> None:
        root = request.declaration.controller_evidence.execution_envelope.workflow_plan.work.inputs[
            0
        ]
        validate_live_root_authority(
            base_url=request.runtime.riverhog_base_url,
            capability_token=request.runtime.capability_token,
            allow_insecure_http=request.runtime.allow_insecure_http,
            root=root.to_identity(),
            deadline=deadline,
        )

    def _prepare_dispatch(
        self,
        request: TargetJobRequest,
        attempt: int,
        cancellation: threading.Event,
        permit: ExecutionPermit,
    ) -> Callable[[], object] | None:
        job_id = request.declaration.job_id
        with self._lock:
            status = self._load_status(job_id)
            if (
                self._closing
                or status is None
                or status.state != "queued"
                or status.attempt != attempt
            ):
                return None
            if self._pending_submissions.get(job_id) != (request, attempt):
                return None
            if cancellation.is_set() or self._load_cancellation(job_id) is not None:
                self._cancel_stopped(request.accepted(), status)
                return None
            self._pending_submissions.pop(job_id, None)
            self._cancel[job_id] = cancellation
            self._operator_canceled.discard(job_id)
            self._shutdown_interrupted.discard(job_id)
            self._runtime_registry.refresh(job_id, request.runtime.capability_token)
            session = TargetExecutionSession(
                request,
                attempt,
                self._runtime_registry,
                state_root=self.state_root,
                progress_callback=lambda progress: self._report_progress(job_id, attempt, progress),
                completion_callback=self._commit_status,
            )
            self._sessions[job_id] = session
            # This durable marker precedes any possible payload execution.
            self._commit_status(
                self._status(request, state="running", attempt=attempt, phase="starting")
            )
        return lambda: self._run(request, attempt, cancellation, session, permit)

    def _report_progress(self, job_id: str, attempt: int, progress: TargetProgress) -> None:
        with self._lock:
            current = self._load_status(job_id)
            if current is not None and current.attempt == attempt and current.state == "running":
                self._commit_status(current.model_copy(update={"progress": progress}))

    def _execution_finished(self, job_id: str) -> None:
        with self._lock:
            pending = self._pending_submissions.get(job_id)
            status = self._load_status(job_id)
            if pending is not None and status is not None and status.state == "canceling":
                self._pending_submissions.pop(job_id)
                self._dispatch.cancel(job_id)
                self._commit_status(self._stop_status(pending[0], pending[1]))
            self._cancel.pop(job_id, None)
            self._operator_canceled.discard(job_id)
            self._shutdown_interrupted.discard(job_id)
        self._retention.wake()

    def _run(
        self,
        request: TargetJobRequest,
        attempt: int,
        cancellation: threading.Event,
        session: TargetExecutionSession,
        permit: ExecutionPermit,
    ) -> TargetJobStatus:
        try:
            if cancellation.is_set():
                return self._commit_status(self._stop_status(request, attempt))
            active = self._commit_status(
                self._status(
                    request,
                    state="running",
                    attempt=attempt,
                    phase="preparing-inputs",
                )
            )
            if active.state == "canceling" or cancellation.is_set():
                return self._commit_status(self._stop_status(request, attempt))
            try:
                checkpoint = session.completion_checkpoint(request)
                if checkpoint is None:
                    terminal = self._execute(request, attempt, cancellation, session)
                else:
                    terminal = self._resume_publication(
                        request, attempt, cancellation, session, checkpoint
                    )
            except TargetExecutionCanceled:
                terminal = self._stop_status(request, attempt)
            except TargetExecutionInapplicable as exc:
                terminal = TargetJobStatus(
                    protocol=request.declaration.plan.protocol,
                    job_id=request.declaration.job_id,
                    state="inapplicable",
                    attempt=attempt,
                    request_sha256=request.request_sha256,
                    plan_sha256=request.declaration.plan.plan_sha256,
                    progress=TargetProgress(phase="inapplicable", completed=0),
                    inapplicable=TargetInapplicable(code=exc.code, message=exc.message),
                )
            except TargetEffectCommitUncertain:
                terminal = self._status(
                    request,
                    state="interrupted",
                    attempt=attempt,
                    phase="external-commit-uncertain",
                )
            except Exception as exc:
                terminal = _failure_status(request, attempt=attempt, failure=exc)
                if (
                    terminal.failure is not None
                    and terminal.failure.retryable
                    and (
                        TargetCompletionCheckpoint.manifest_path(
                            self.state_root, request.declaration.job_id
                        ).exists()
                        or TargetOutputCheckpoint.has_job_records(
                            self.state_root, request.declaration.job_id
                        )
                    )
                ):
                    locations = ",".join(
                        f"{Path(frame.filename).name}:{frame.lineno}:{frame.name}"
                        for frame in traceback.extract_tb(exc.__traceback__, limit=-8)
                    )
                    _LOGGER.warning(
                        "Target publication interrupted job=%s attempt=%d code=%s cause=%s at=%s",
                        request.declaration.job_id,
                        attempt,
                        terminal.failure.code,
                        type(exc).__name__,
                        locations,
                    )
                    terminal = self._status(
                        request,
                        state="interrupted",
                        attempt=attempt,
                        phase="publication-interrupted",
                    )
            completed = session.completed_status
            if completed is not None:
                terminal = completed
            elif cancellation.is_set() and terminal.state not in {"succeeded", "interrupted"}:
                terminal = self._stop_status(request, attempt)
            return self._commit_status(terminal)
        finally:
            with self._lock:
                if self._sessions.get(request.declaration.job_id) is session:
                    self._sessions.pop(request.declaration.job_id, None)
                self._runtime_contexts.pop(request.declaration.job_id, None)
                self._runtime_token_fingerprints.pop(request.declaration.job_id, None)
                self._runtime_registry.discard(request.declaration.job_id)

    def _resume_publication(
        self,
        request: TargetJobRequest,
        attempt: int,
        cancellation: threading.Event,
        session: TargetExecutionSession,
        checkpoint: TargetCompletionCheckpoint,
    ) -> TargetJobStatus:
        def check() -> None:
            if cancellation.is_set():
                raise TargetExecutionCanceled("target publication resume was canceled")

        with TargetExecutionRuntime.from_request(
            request,
            cancellation_check=check,
            session=session,
            producer_version=checkpoint.implementation.implementation_version,
        ) as execution:
            publication = execution.open_collection_publication(
                implementation=checkpoint.implementation,
                source_context=checkpoint.source_context,
            )
            return publication.finish_success(
                operation=checkpoint.operation,
                execution_sha256=checkpoint.execution.sha256,
                execution_preimage=checkpoint.execution,
                attempt=attempt,
                runtime_evidence=checkpoint.pre_root.execution_evidence.runtime,
            )

    def _stop_status(self, request: TargetJobRequest, attempt: int) -> TargetJobStatus:
        job_id = request.declaration.job_id
        with self._lock:
            interrupted = (
                job_id in self._shutdown_interrupted and job_id not in self._operator_canceled
            )
        state = "interrupted" if interrupted else "canceled"
        return self._status(request, state=state, attempt=attempt, phase=state)

    @staticmethod
    def _status(
        request: TargetJobRequest,
        *,
        state: str,
        attempt: int,
        phase: str,
    ) -> TargetJobStatus:
        return TargetJobStatus(
            protocol=request.declaration.plan.protocol,
            job_id=request.declaration.job_id,
            state=state,  # type: ignore[arg-type]
            attempt=attempt,
            request_sha256=request.request_sha256,
            plan_sha256=request.declaration.plan.plan_sha256,
            progress=TargetProgress(phase=phase, completed=0),
        )

    def _recover_interrupted(self) -> None:
        for path in self.state_root.glob("*.cancel.json"):
            if path.is_symlink():
                raise ValueError("target cancellation paths must not be symlinks")
            canceled = AcceptedTargetJob.model_validate_json(path.read_text(encoding="utf-8"))
            job_id = canceled.declaration.job_id
            if path != self._cancellation_path(job_id):
                raise ValueError("target cancellation identity differs from its state path")
            accepted = self._load_accepted(job_id)
            if accepted is None:
                self._write_model(self._accepted_path(job_id), canceled)
            elif accepted != canceled:
                raise ValueError("target cancellation differs from its accepted declaration")
        for path in self.state_root.glob("*.accepted.json"):
            if path.is_symlink():
                raise ValueError("target accepted paths must not be symlinks")
            accepted = AcceptedTargetJob.model_validate_json(path.read_text(encoding="utf-8"))
            if path != self._accepted_path(accepted.declaration.job_id):
                raise ValueError("target accepted identity differs from its state path")
            if not self._status_path(accepted.declaration.job_id).exists():
                self._commit_status(
                    TargetJobStatus(
                        protocol=accepted.declaration.plan.protocol,
                        job_id=accepted.declaration.job_id,
                        state="canceled"
                        if self._load_cancellation(accepted.declaration.job_id)
                        else "queued",
                        attempt=1,
                        request_sha256=accepted.request_sha256,
                        plan_sha256=accepted.declaration.plan.plan_sha256,
                        progress=TargetProgress(
                            phase="canceled"
                            if self._load_cancellation(accepted.declaration.job_id)
                            else "queued",
                            completed=0,
                        ),
                    )
                )
        for path in self.state_root.glob("*.status.json"):
            if path.is_symlink():
                raise ValueError("target state paths must not be symlinks")
            status = TargetJobStatus.model_validate_json(path.read_text(encoding="utf-8"))
            if path != self._status_path(status.job_id):
                raise ValueError("target status identity differs from its state path")
            accepted = self._load_accepted(status.job_id)
            if accepted is None or accepted.request_sha256 != status.request_sha256:
                raise ValueError("target status differs from its accepted declaration")
            if status.state == "canceling" and self._load_cancellation(status.job_id) is None:
                self._write_model(self._cancellation_path(status.job_id), accepted)
            if status.state in {"running", "canceling"}:
                self._commit_status(
                    status.model_copy(
                        update={
                            "state": "interrupted",
                            "progress": TargetProgress(phase="interrupted", completed=0),
                        }
                    )
                )

    def _cancel_stopped(
        self, accepted: AcceptedTargetJob, status: TargetJobStatus
    ) -> TargetJobStatus:
        job_id = accepted.declaration.job_id
        if status.state not in {"queued", "interrupted"}:
            return status
        if (status.protocol == EFFECT_TARGET_PROTOCOL and status.state != "queued") or (
            TargetCompletionCheckpoint.manifest_path(self.state_root, job_id).exists()
            or TargetOutputCheckpoint.has_job_records(self.state_root, job_id)
        ):
            # A stopped payload does not establish that an effect or an early
            # publication never committed. Keep its exact outcome unresolved.
            return status
        return self._commit_status(
            status.model_copy(
                update={
                    "state": "canceled",
                    "progress": TargetProgress(phase="canceled", completed=0),
                }
            )
        )

    def _cancellation_path(self, job_id: str) -> Path:
        return self.state_root / f"{_job_id(job_id)}.cancel.json"

    def _load_cancellation(self, job_id: str) -> AcceptedTargetJob | None:
        path = self._cancellation_path(job_id)
        if not path.exists():
            return None
        if path.is_symlink():
            raise ValueError("target cancellation path must not be a symlink")
        intent = AcceptedTargetJob.model_validate_json(path.read_text(encoding="utf-8"))
        if intent != self._load_accepted(job_id):
            raise ValueError("target cancellation differs from its accepted declaration")
        return intent

    def _accepted_path(self, job_id: str) -> Path:
        return self.state_root / f"{_job_id(job_id)}.accepted.json"

    def _status_path(self, job_id: str) -> Path:
        return self.state_root / f"{_job_id(job_id)}.status.json"

    def _load_accepted(self, job_id: str) -> AcceptedTargetJob | None:
        path = self._accepted_path(job_id)
        if path.is_symlink():
            raise ValueError("target accepted path must not be a symlink")
        if not path.exists():
            return None
        raw = path.read_bytes()
        key = (job_id, hashlib.sha256(raw).hexdigest())
        with self._lock:
            cached = self._accepted_cache.get(key)
            if cached is not None:
                self._accepted_cache.move_to_end(key)
                # Nested JSON values are mutable even in frozen protocol models.
                # Each caller owns its view of this exact non-secret declaration.
                return cached[1].model_copy(deep=True)
            accepted = AcceptedTargetJob.model_validate_json(raw)
            if len(raw) <= self._accepted_cache_budget:
                while self._accepted_cache and (
                    len(self._accepted_cache) >= self._accepted_cache_entries
                    or self._accepted_cache_bytes + len(raw) > self._accepted_cache_budget
                ):
                    _, (size, _) = self._accepted_cache.popitem(last=False)
                    self._accepted_cache_bytes -= size
                self._accepted_cache[key] = (len(raw), accepted.model_copy(deep=True))
                self._accepted_cache_bytes += len(raw)
            return accepted

    def _load_status(self, job_id: str) -> TargetJobStatus | None:
        path = self._status_path(job_id)
        return (
            None
            if not path.exists()
            else TargetJobStatus.model_validate_json(path.read_text(encoding="utf-8"))
        )

    def _commit_status(self, status: TargetJobStatus) -> TargetJobStatus:
        with self._lock:
            current = self._load_status(status.job_id)
            if current == status:
                return current
            if current is not None:
                if current.state in _TERMINAL_STATES:
                    return current
                if current.attempt > status.attempt:
                    return current
                if current.state == "canceling" and status.state in {"queued", "running"}:
                    return current
            self._write_model(self._status_path(status.job_id), status)
            self._nonterminal_job_count += int(status.state not in _TERMINAL_STATES) - int(
                current is not None and current.state not in _TERMINAL_STATES
            )
            return status

    def _write_model(self, path: Path, model: Any) -> None:
        if path.is_symlink():
            raise ValueError("target state paths must not be symlinks")
        temporary = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.part")
        # This file is restart state, not an identity-bearing protocol document.
        # Preserve JSON scalar types exactly: RFC 8785 deliberately renders an
        # integral float such as ``45.0`` as ``45``, while an embedded authority
        # owned by another protocol may distinguish those values in its digest.
        encoded = json.dumps(
            model.model_dump(mode="json", by_alias=True, exclude_none=True),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(self.state_root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory)
        finally:
            os.close(directory)


def _job_id(value: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError("target job ID must be a lowercase SHA-256")
    return value


def _failure_status(
    request: TargetJobRequest,
    *,
    attempt: int,
    failure: Exception,
) -> TargetJobStatus:
    if isinstance(failure, TargetExecutionFailure):
        code = failure.code
        message = failure.message
        retryable = failure.retryable
    elif isinstance(failure, RiverhogError):
        status = failure.observed_status
        if status in {401, 403} or failure.code in {"unauthorized", "forbidden"}:
            code = "target-authorization"
            retryable = True
        elif status == 409 or failure.code in {"conflict", "invalid_state"}:
            code = "target-conflict"
            retryable = True
        elif isinstance(failure, DownloadAllowanceExceeded):
            code = "target-download-allowance"
            retryable = True
        elif isinstance(failure, ServiceUnavailable) or (status is not None and status >= 500):
            code = "target-infrastructure"
            retryable = True
        else:
            code = "target-api"
            retryable = False
        message = f"{type(failure).__name__}: {failure}"
    elif isinstance(failure, (OSError, TimeoutError, subprocess.TimeoutExpired)):
        code = "target-infrastructure"
        message = f"{type(failure).__name__}: {failure}"
        retryable = True
    else:
        code = "target-software"
        message = f"{type(failure).__name__}: {failure}"
        retryable = True
    return TargetJobStatus(
        protocol=request.declaration.plan.protocol,
        job_id=request.declaration.job_id,
        state="failed",
        attempt=attempt,
        request_sha256=request.request_sha256,
        plan_sha256=request.declaration.plan.plan_sha256,
        progress=TargetProgress(phase="failed", completed=0),
        failure=TargetFailure(
            code=code,
            message=message[:1000],
            retryable=retryable,
        ),
    )


__all__ = [
    "DEFAULT_TERMINAL_STATE_RETENTION_SECONDS",
    "JobExecutor",
    "IntentSemanticValidator",
    "PersistentTargetService",
    "TargetEffectCommitUncertain",
    "TargetExecutionCanceled",
    "TargetExecutionFailure",
    "TargetExecutionInapplicable",
]
