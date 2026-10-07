"""Typed operational continuation for read-only planning, with CAS ownership."""

from __future__ import annotations

import threading
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field
from stove0_protocol import (
    BranchSetDecision,
    BranchTargetPreview,
    PlanningJobRequest,
    PlanningJobStatus,
    WorkflowPreview,
)
from stove0_protocol.planning_jobs import PlanningJobPhase
from time_formats import CanonicalUtcTimestamp, utc_timestamp_now

from stove0_core.metadata_steps import PreviewScan
from stove0_core.work_state import ClaimBinding, ConcurrentWorkUpdate


class PreviewRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    job: PlanningJobRequest
    revision: int = Field(default=1, ge=1)
    phase: PlanningJobPhase = "queued"
    claim: ClaimBinding | None = None
    claim_renew_at: CanonicalUtcTimestamp | None = None
    contact_at: CanonicalUtcTimestamp = Field(default_factory=utc_timestamp_now)
    observation_cursors: dict[str, str] = Field(default_factory=dict)
    decision: BranchSetDecision | None = None
    target_plans: tuple[BranchTargetPreview, ...] = ()
    result: WorkflowPreview | None = None
    canceled: bool = False

    def status(self) -> PlanningJobStatus:
        return PlanningJobStatus(
            job_id=self.job.job_id,
            work_id=self.job.work.work_id,
            state=self.phase,
            result=self.result if self.phase == "completed" else None,
        )


class PreviewStore(Protocol):
    def create_preview(self, record: PreviewRecord) -> PreviewRecord: ...
    def load_preview(self, job_id: str) -> PreviewRecord | None: ...
    def compare_and_swap_preview(
        self, *, expected_revision: int, replacement: PreviewRecord
    ) -> PreviewRecord: ...
    def scan_previews(
        self, *, after_id: str = "", limit: int = 25, maintenance: bool = False
    ) -> tuple[PreviewScan, ...]: ...


class InMemoryPreviewStore:
    def __init__(self) -> None:
        self._preview_lock = threading.RLock()
        self._previews: dict[str, PreviewRecord] = {}

    def create_preview(self, record: PreviewRecord) -> PreviewRecord:
        with self._preview_lock:
            current = self._previews.get(record.job.job_id)
            if current is not None:
                if current.job != record.job:
                    raise ValueError("planning job identity was rebound")
                return current
            self._previews[record.job.job_id] = record
            return record

    def load_preview(self, job_id: str) -> PreviewRecord | None:
        with self._preview_lock:
            return self._previews.get(job_id)

    def compare_and_swap_preview(
        self, *, expected_revision: int, replacement: PreviewRecord
    ) -> PreviewRecord:
        with self._preview_lock:
            current = self._previews.get(replacement.job.job_id)
            if current is None or current.revision != expected_revision:
                raise ConcurrentWorkUpdate("planning job changed concurrently")
            if replacement.job != current.job or replacement.revision != current.revision + 1:
                raise ValueError("planning continuation changed its exact invocation")
            self._previews[current.job.job_id] = replacement
            return replacement

    def scan_previews(
        self, *, after_id: str = "", limit: int = 25, maintenance: bool = False
    ) -> tuple[PreviewScan, ...]:
        if limit < 1 or limit > 100:
            raise ValueError("planning page size must be between 1 and 100")
        now = utc_timestamp_now()
        with self._preview_lock:
            return tuple(
                PreviewScan(item.job.job_id, item.phase, item.revision)
                for key, item in sorted(self._previews.items())
                if key > after_id
                and item.phase != "completed"
                and (
                    item.claim_renew_at is not None and item.claim_renew_at <= now
                    if maintenance
                    else item.contact_at <= now
                )
            )[:limit]


__all__ = ["PreviewRecord", "PreviewStore", "InMemoryPreviewStore"]
