"""Target execution control and target-owned publication restart evidence."""

from __future__ import annotations

import hashlib
import threading
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, JsonValue
from riverhog_canonical_json import canonical_json_bytes
from riverhog_client.canonical_completion import CompletionRecord
from riverhog_client.processing import ClaimedCollectionRuntimeRegistry
from stove0_target_client import TargetCallbackClient
from stove0_target_protocol import (
    OperationContract,
    TargetCallbackAccess,
    TargetDescriptor,
    TargetJobRequest,
    TargetJobStatus,
    TargetPreRootResult,
)

from stove0_target_support.completion_checkpoint import TargetCompletionCheckpoint, _immutable_file


class _StepValue(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    job_id: str
    request_sha256: str
    plan_sha256: str
    value: dict[str, JsonValue]


class TargetExecutionSession:
    """Bind refreshable runtime authority and finalized success to one attempt.

    Capabilities stay in memory. Target-owned completion checkpoints retain exact
    sealed evidence before archive finalization, so a restarted attempt can finish
    publication without executing the operation again or rereading released files.
    These checkpoints do not establish Riverhog custody or settlement authority.
    """

    def __init__(
        self,
        request: TargetJobRequest,
        attempt: int,
        runtime_registry: ClaimedCollectionRuntimeRegistry,
        *,
        state_root: Path | None = None,
    ) -> None:
        self.job_id = request.declaration.job_id
        self.request_sha256 = request.request_sha256
        self.plan_sha256 = request.declaration.plan.plan_sha256
        self.attempt = attempt
        self.runtime_registry = runtime_registry
        self.state_root = state_root
        self._lock = threading.RLock()
        self._completed_status: TargetJobStatus | None = None
        self._callback_access = request.callback_access
        self._callback_client: TargetCallbackClient | None = None

    def callback_client(self) -> TargetCallbackClient:
        """Bind queued and running callback reads to the latest transient access."""
        with self._lock:
            if self._callback_client is None:
                self._callback_client = TargetCallbackClient(self._callback_access)
            return self._callback_client

    def refresh_callback_access(self, access: TargetCallbackAccess) -> None:
        """Keep callback refresh scoped to the already accepted endpoint."""
        with self._lock:
            if access.model_dump(exclude={"token"}) != self._callback_access.model_dump(
                exclude={"token"}
            ):
                raise ValueError("active target callback endpoint changed")
            if self._callback_client is not None:
                self._callback_client.refresh_access(access)
            self._callback_access = access

    def completion_checkpoint(self, request: TargetJobRequest) -> TargetCompletionCheckpoint | None:
        if self.state_root is None:
            return None
        return TargetCompletionCheckpoint.load(self.state_root, request=request)

    def _step_path(self, key: str) -> Path | None:
        if self.state_root is None:
            return None
        suffix = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return self.state_root / f"{self.job_id}.step-{suffix}.bin"

    def load_step(self, key: str, *, maximum_bytes: int = 4 * 1024 * 1024) -> bytes | None:
        path = self._step_path(key)
        if path is None:
            return None
        if path.is_symlink():
            raise ValueError("target step checkpoint must not be a symlink")
        try:
            with path.open("rb") as stream:
                content = stream.read(maximum_bytes + 1)
        except FileNotFoundError:
            return None
        if len(content) > maximum_bytes:
            raise ValueError("target step checkpoint exceeds the accepted record budget")
        return content

    def retain_step(
        self, key: str, content: bytes, *, maximum_bytes: int = 4 * 1024 * 1024
    ) -> None:
        if len(content) > maximum_bytes:
            raise ValueError("target step checkpoint exceeds the accepted record budget")
        path = self._step_path(key)
        if path is not None:
            _immutable_file(path, (content,))

    def step_value(
        self, key: str, create: Callable[[], dict[str, JsonValue]]
    ) -> dict[str, JsonValue]:
        """Retain component-owned execution state before releasing related bytes."""
        raw = self.load_step(key)
        if raw is None:
            record = _StepValue(
                job_id=self.job_id,
                request_sha256=self.request_sha256,
                plan_sha256=self.plan_sha256,
                value=create(),
            )
            self.retain_step(key, canonical_json_bytes(record.model_dump(mode="json")))
        else:
            record = _StepValue.model_validate_json(raw)
        if (
            record.job_id != self.job_id
            or record.request_sha256 != self.request_sha256
            or record.plan_sha256 != self.plan_sha256
        ):
            raise ValueError("target step checkpoint differs from accepted execution")
        return record.value

    def stored_step_value(self, key: str) -> dict[str, JsonValue] | None:
        raw = self.load_step(key)
        if raw is None:
            return None
        record = _StepValue.model_validate_json(raw)
        if (
            record.job_id != self.job_id
            or record.request_sha256 != self.request_sha256
            or record.plan_sha256 != self.plan_sha256
        ):
            raise ValueError("target step checkpoint differs from accepted execution")
        return record.value

    def retain_completion(
        self,
        *,
        request: TargetJobRequest,
        implementation: TargetDescriptor,
        operation: OperationContract,
        pre_root: TargetPreRootResult,
        execution: CompletionRecord,
        source_context: Mapping[str, Any],
    ) -> TargetPreRootResult:
        if self.state_root is None:
            return pre_root
        with self._lock:
            saved = TargetCompletionCheckpoint.retain(
                self.state_root,
                request=request,
                implementation=implementation,
                operation=operation,
                pre_root=pre_root,
                execution=execution,
                source_context=source_context,
            )
            return saved.pre_root

    def record_completed(self, status: TargetJobStatus) -> None:
        """Retain one exact, validated successful target result."""

        if (
            status.state != "succeeded"
            or status.job_id != self.job_id
            or status.request_sha256 != self.request_sha256
            or status.plan_sha256 != self.plan_sha256
            or status.attempt != self.attempt
        ):
            raise ValueError("completed target status differs from the active attempt")
        with self._lock:
            if self._completed_status is not None and self._completed_status != status:
                raise RuntimeError("target attempt produced two different terminal outcomes")
            self._completed_status = status

    @property
    def completed_status(self) -> TargetJobStatus | None:
        with self._lock:
            return self._completed_status


__all__ = ["TargetExecutionSession"]
