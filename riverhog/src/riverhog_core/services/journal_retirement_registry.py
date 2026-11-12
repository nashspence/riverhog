"""Retain original Native journal cleanup in shared workspace execution."""

from __future__ import annotations

import hashlib
from dataclasses import asdict
from datetime import timedelta
from threading import Lock
from typing import TYPE_CHECKING

from riverhog_canonical_json import canonical_json_bytes
from riverhog_protocol.errors import Conflict
from riverhog_provenance import JournalValidationCapacity
from workspace_reclamation import FileReclamationOwner

from riverhog_core.incremental_emission import VolumeEmissionCapacity
from riverhog_core.journal_validation_workspace import LostJournalValidationWorkspace
from riverhog_core.retained_workspace_handles import RetainedWorkspaceHandles
from riverhog_core.services.archive_construction_retention import ArchiveConstructionRetention
from riverhog_core.services.catalog_retirement import metadata_row_bytes
from riverhog_core.services.physical_execution import (
    PhysicalExecutionOwner,
    PhysicalWorkspaceExecutionKey,
)
from riverhog_core.services.upload_journal_validation_jobs import ValidationJob
from riverhog_core.services.workspace_retirement_registry import (
    WorkspaceRetirementRegistry,
    _Closed,
    _Context,
    _ReceivingPending,
)

if TYPE_CHECKING:
    from .upload_journal_validation_retirement import UploadJournalValidationRetirement


def format_retirement_blocker(error: BaseException) -> str:
    """Format the complete error on its worker before the existing detached slice."""
    return str(error)[:1000]


class JournalRetirementTurn:
    """One admitted original; resource receiving never reconstructs its work."""

    def __init__(self, cleanup: UploadJournalValidationRetirement, job: ValidationJob) -> None:
        self.cleanup, self.job = cleanup, job
        self._files = FileReclamationOwner()
        self._handles = RetainedWorkspaceHandles()
        self._execution = Lock()
        self._attempted = False
        self._closed = False

    @property
    def received(self) -> bool:
        return self._closed

    def _receive(self) -> None:
        self._files.close()
        self._handles.close()
        self._closed = True

    def close(self) -> None:
        if not self._execution.acquire(blocking=False):
            raise BlockingIOError("the original Native retirement executor still uses custody")
        try:
            self._receive()
        finally:
            self._execution.release()

    def advance(self, *, octet_budget: int) -> bool:
        if type(octet_budget) is not int or octet_budget < 1:
            raise ValueError("retirement requires a positive file-extent admission")
        if not self._execution.acquire(blocking=False):
            raise BlockingIOError("the original Native retirement executor still uses custody")
        try:
            if self._attempted or self._closed:
                raise Conflict("original Native retirement work cannot be reconstructed")
            self._attempted = True
            try:
                result = self.cleanup._advance(self.job, octet_budget, turn=self)
            except (JournalValidationCapacity, LostJournalValidationWorkspace) as error:
                self._receive()
                return self.cleanup._blocked(self.job, format_retirement_blocker(error))
            self._receive()
            return result
        finally:
            self._execution.release()


class JournalRetirementRegistry(WorkspaceRetirementRegistry[ValidationJob, bool]):
    """Original Native job identity owns scope; the shared controller owns resources."""

    def __init__(
        self,
        cleanup: UploadJournalValidationRetirement,
        execution: PhysicalExecutionOwner,
        retention: ArchiveConstructionRetention,
    ) -> None:
        super().__init__(execution, retention)
        self._cleanup = cleanup

    def collect(self, *, limit: int) -> int:
        return super().collect(limit=limit)

    def resume_ready(self, *, limit: int, retry_after: timedelta) -> int:
        return super().resume_ready(limit=limit, retry_after=retry_after)

    def retained(self, workspace_id: str) -> bool:
        return super().retained(workspace_id)

    def submit(
        self,
        job: ValidationJob,
        *,
        octet_budget: int,
        metadata_budget: int,
        retry_after: timedelta,
    ) -> int:
        return super().submit(
            job,
            octet_budget=octet_budget,
            metadata_budget=metadata_budget,
            retry_after=retry_after,
        )

    def _run(
        self,
        context: _Context[ValidationJob, bool],
    ) -> bool | _Closed | _ReceivingPending:
        return super()._run(context)

    def _scope(
        self, job: ValidationJob, metadata_budget: int
    ) -> tuple[PhysicalWorkspaceExecutionKey, str, int]:
        if job.collection_id is not None or job.journal_id is not None:
            raise Conflict("Native cleanup cannot adopt linked source authority")
        target = "native-journal:" + str(self._cleanup.root / job.workspace_id)
        controls = {
            **asdict(job),
            "root": str(self._cleanup.root),
            "target_incarnation": target,
        }
        required = metadata_row_bytes(controls)
        if required > metadata_budget:
            raise VolumeEmissionCapacity(
                dimension="metadata_bytes", required=required, admitted=metadata_budget
            )
        exact = {
            name: str(value) if type(value) is int else value for name, value in controls.items()
        }
        scope = hashlib.sha256(canonical_json_bytes(exact)).hexdigest()
        return PhysicalWorkspaceExecutionKey(job.workspace_id, scope), target, required

    def _accept_progress(self, job: ValidationJob, result: object) -> bool:
        if type(result) is not bool:
            raise Conflict("Native retirement returned another original result type")
        return result

    def _create(self, job: ValidationJob) -> JournalRetirementTurn:
        actor = JournalRetirementTurn(self._cleanup, job)
        if actor.job != job or actor.cleanup is not self._cleanup:
            raise Conflict("Native retirement constructor returned another original")
        return actor
