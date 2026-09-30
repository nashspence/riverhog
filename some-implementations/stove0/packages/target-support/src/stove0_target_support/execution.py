"""Target execution control and target-owned publication restart evidence."""

from __future__ import annotations

import threading
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from riverhog_client.canonical_completion import CompletionRecord
from riverhog_client.processing import ClaimedCollectionRuntimeRegistry
from stove0_target_protocol import (
    OperationContract,
    TargetDescriptor,
    TargetJobRequest,
    TargetJobStatus,
    TargetPreRootResult,
)

from stove0_target_support.completion_checkpoint import TargetCompletionCheckpoint


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

    def completion_checkpoint(self, request: TargetJobRequest) -> TargetCompletionCheckpoint | None:
        if self.state_root is None:
            return None
        return TargetCompletionCheckpoint.load(self.state_root, request=request)

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
