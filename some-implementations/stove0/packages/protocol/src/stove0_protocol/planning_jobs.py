"""Exact, pollable control computations preceding work admission."""

from __future__ import annotations

from typing import Literal, Self

from pydantic import model_validator

from stove0_protocol.fork_join import WorkflowPreview
from stove0_protocol.jcs import canonical_json_sha256
from stove0_protocol.models import Sha256, Stove0ProtocolModel, WorkIdentity

PlanningJobPhase = Literal[
    "queued",
    "observing",
    "planning",
    "preflight",
    "canceling",
    "abandoning",
    "admitting",
    "completed",
]


class PlanningJobPayload(Stove0ProtocolModel):
    format: Literal["stove0-planning-job/v1"] = "stove0-planning-job/v1"
    invocation_id: Sha256
    work: WorkIdentity
    accepted_preview_sha256: Sha256 | None = None


class PlanningJobRequest(PlanningJobPayload):
    job_id: Sha256

    @classmethod
    def seal(cls, payload: PlanningJobPayload) -> PlanningJobRequest:
        document = payload.model_dump(mode="json", by_alias=True, exclude_none=True)
        return cls.model_validate({**document, "job_id": canonical_json_sha256(document)})

    @model_validator(mode="after")
    def exact_identity(self) -> Self:
        if self.job_id != canonical_json_sha256(
            self.model_dump(mode="json", by_alias=True, exclude_none=True, exclude={"job_id"})
        ):
            raise ValueError("planning job identity differs from its exact invocation")
        return self


class PlanningJobStatus(Stove0ProtocolModel):
    format: Literal["stove0-planning-status/v1"] = "stove0-planning-status/v1"
    job_id: Sha256
    work_id: Sha256
    state: PlanningJobPhase
    result: WorkflowPreview | None = None

    @model_validator(mode="after")
    def exact_result(self) -> Self:
        if (self.state == "completed") != (self.result is not None):
            raise ValueError("only a completed planning job contains accepted preview evidence")
        if self.result is not None and self.result.work.work_id != self.work_id:
            raise ValueError("planning result differs from the job's work identity")
        return self


__all__ = ["PlanningJobPayload", "PlanningJobRequest", "PlanningJobStatus"]
