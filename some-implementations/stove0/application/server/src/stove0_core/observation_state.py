"""Durable controller delivery state, separate from observation testimony."""

from __future__ import annotations

from typing import Protocol, Self

from pydantic import BaseModel, ConfigDict, model_validator
from stove0_observer_protocol import AcceptedObservationJob, ObservationJobStatus, Sha256
from stove0_operator_contracts import PlanningOwnerKind

ObservationOwnerKind = PlanningOwnerKind


class ObservationDeliveryRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    owner_kind: ObservationOwnerKind
    owner_id: Sha256
    accepted: AcceptedObservationJob
    status: ObservationJobStatus | None = None

    @model_validator(mode="after")
    def exact_delivery(self) -> Self:
        if self.status is not None and (
            self.status.job_id != self.accepted.job_id
            or self.status.request_id != self.accepted.request.request_id
        ):
            raise ValueError("observation delivery status differs from its exact invocation")
        return self


class ObservationDeliveryPort(Protocol):
    def ensure_observation_delivery(
        self, record: ObservationDeliveryRecord
    ) -> ObservationDeliveryRecord: ...

    def update_observation_delivery(
        self,
        owner_kind: ObservationOwnerKind,
        owner_id: str,
        job_id: str,
        status: ObservationJobStatus,
    ) -> ObservationDeliveryRecord: ...


def updated_delivery(
    record: ObservationDeliveryRecord, status: ObservationJobStatus
) -> ObservationDeliveryRecord:
    replacement = ObservationDeliveryRecord.model_validate(
        {**record.model_dump(mode="python"), "status": status}
    )
    current = record.status
    if current is not None and (
        current.state == "completed"
        or current.attempt > status.attempt
        or (current.state == "canceling" and status.state in {"queued", "running"})
    ):
        return record
    return replacement
