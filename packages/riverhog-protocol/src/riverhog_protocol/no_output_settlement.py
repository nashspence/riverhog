"""Content-opaque successful processing without a material or effect result."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, cast

from riverhog_canonical_json import canonical_json_bytes, canonical_json_sha256, format_scalar

from riverhog_protocol.collection_workflows import (
    ArtifactDispositionSetIdentity,
    OperationIdentity,
    _json_object,
    _positive_decimal,
    _positive_uint,
    _sha256,
)

NO_OUTPUT_SETTLEMENT_FORMAT = "riverhog-no-output-settlement/v1"
NO_OUTPUT_DECISION_MAX_BYTES = 4 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class NoOutputSettlement:
    """One controller decision committed with the shared disposition authority."""

    claim_id: str
    fence: int
    execution_id: str
    operation: OperationIdentity
    input_set_sha256: str
    artifact_set_sha256: str
    controller_evidence_sha256: str
    decision: Mapping[str, object]
    decision_sha256: str
    disposition_set: ArtifactDispositionSetIdentity
    status: Literal["succeeded"] = "succeeded"

    def __post_init__(self) -> None:
        for name in (
            "claim_id",
            "execution_id",
            "input_set_sha256",
            "artifact_set_sha256",
            "controller_evidence_sha256",
            "decision_sha256",
        ):
            _sha256(getattr(self, name), name)
        _positive_uint(self.fence, "fence")
        if self.status != "succeeded":
            raise ValueError("only a successful no-output decision can settle")
        decision = _json_object(self.decision, "no-output decision")
        if not decision or len(canonical_json_bytes(decision)) > NO_OUTPUT_DECISION_MAX_BYTES:
            raise ValueError("no-output decision is missing or exceeds its byte limit")
        if canonical_json_sha256(decision) != self.decision_sha256:
            raise ValueError("no-output decision differs from its canonical identity")
        if self.disposition_set.output_edge_count or self.disposition_set.output_artifact_count:
            raise ValueError("no-output decision cannot bind material successors")

    def as_dict(self) -> dict[str, object]:
        return {
            "format": NO_OUTPUT_SETTLEMENT_FORMAT,
            "claim_id": self.claim_id,
            "fence": format_scalar("nonnegative", self.fence),
            "execution_id": self.execution_id,
            "operation": self.operation.as_dict(),
            "input_set_sha256": self.input_set_sha256,
            "artifact_set_sha256": self.artifact_set_sha256,
            "controller_evidence_sha256": self.controller_evidence_sha256,
            "decision": dict(self.decision),
            "decision_sha256": self.decision_sha256,
            "disposition_set": self.disposition_set.as_dict(),
            "status": self.status,
        }

    @property
    def sha256(self) -> str:
        return canonical_json_sha256(self.as_dict())

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> NoOutputSettlement:
        keys = {
            "format",
            "claim_id",
            "fence",
            "execution_id",
            "operation",
            "input_set_sha256",
            "artifact_set_sha256",
            "controller_evidence_sha256",
            "decision",
            "decision_sha256",
            "disposition_set",
            "status",
        }
        if set(value) != keys or value.get("format") != NO_OUTPUT_SETTLEMENT_FORMAT:
            raise ValueError("no-output settlement fields are invalid")
        operation, disposition = value.get("operation"), value.get("disposition_set")
        if not isinstance(operation, Mapping) or not isinstance(disposition, Mapping):
            raise ValueError("no-output operation and disposition set must be objects")
        return cls(
            claim_id=str(value["claim_id"]),
            fence=_positive_decimal(value["fence"], "fence"),
            execution_id=str(value["execution_id"]),
            operation=OperationIdentity.from_mapping(operation),
            input_set_sha256=str(value["input_set_sha256"]),
            artifact_set_sha256=str(value["artifact_set_sha256"]),
            controller_evidence_sha256=str(value["controller_evidence_sha256"]),
            decision=_json_object(value.get("decision"), "no-output decision"),
            decision_sha256=str(value["decision_sha256"]),
            disposition_set=ArtifactDispositionSetIdentity.from_mapping(disposition),
            status=cast(Literal["succeeded"], value["status"]),
        )


__all__ = ["NO_OUTPUT_DECISION_MAX_BYTES", "NO_OUTPUT_SETTLEMENT_FORMAT", "NoOutputSettlement"]
