"""Generic, content-opaque external-effect settlement authorities.

Only the authenticated processing controller submits these attestations. It
validates operation-specific success evidence; Riverhog verifies the canonical
receipt and its exact sealed claim binding, not the external delivery mechanism.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, cast

from riverhog_canonical_json import canonical_json_bytes, canonical_json_sha256, format_scalar

from riverhog_protocol.collection_workflows import (
    OperationIdentity,
    _positive_decimal,
    _positive_uint,
    _sha256,
)

EFFECT_SETTLEMENT_FORMAT = "riverhog-external-effect-settlement/v1"
EFFECT_RECEIPT_MAX_BYTES = 4 * 1024 * 1024
OPERATION_CONTRACT_MAX_BYTES = 4 * 1024 * 1024


def operation_retirement_permission(
    operation: OperationIdentity,
    result_kind: str,
    declaration: Mapping[str, object] | None,
) -> bool:
    """Verify a controller-selected, hash-bound generic operation declaration.

    Only the generic identity, result kind and retirement permission are read.
    Remaining operation semantics are deliberately opaque to Riverhog. Absence
    of a declaration cannot authorize retirement or an external effect.
    """
    if result_kind not in {"collection", "external-effect"}:
        raise ValueError("unknown processing result kind")
    if declaration is None:
        if result_kind == "external-effect":
            raise ValueError("external effects require an exact operation declaration")
        return False
    if len(canonical_json_bytes(declaration)) > OPERATION_CONTRACT_MAX_BYTES:
        raise ValueError("operation declaration exceeds its canonical byte limit")
    if canonical_json_sha256(declaration) != operation.sha256:
        raise ValueError("operation declaration identity differs from the selected operation")
    if declaration.get("id") != operation.id or declaration.get("result_kind") != result_kind:
        raise ValueError("operation declaration differs from the selected operation or result kind")
    permitted = declaration.get("source_collection_retirement_permitted")
    if type(permitted) is not bool:
        raise ValueError("operation retirement permission must be an explicit boolean")
    return permitted


@dataclass(frozen=True, slots=True)
class ExternalEffectSettlement:
    """An exact successful effect attestation, not a target deletion capability."""

    claim_id: str
    fence: int
    execution_id: str
    execution_sha256: str
    operation: OperationIdentity
    input_set_sha256: str
    artifact_set_sha256: str
    controller_evidence_sha256: str
    receipt: Mapping[str, object]
    receipt_sha256: str
    status: Literal["succeeded"] = "succeeded"

    def __post_init__(self) -> None:
        for name in (
            "claim_id",
            "execution_id",
            "execution_sha256",
            "input_set_sha256",
            "artifact_set_sha256",
            "controller_evidence_sha256",
            "receipt_sha256",
        ):
            _sha256(getattr(self, name), name)
        _positive_uint(self.fence, "fence")
        if self.status != "succeeded":
            raise ValueError("only an exact successful external effect can settle")
        if not isinstance(self.receipt, Mapping):
            raise ValueError("effect receipt must be an object")
        if len(canonical_json_bytes(self.receipt)) > EFFECT_RECEIPT_MAX_BYTES:
            raise ValueError("effect receipt exceeds its canonical byte limit")
        if canonical_json_sha256(self.receipt) != self.receipt_sha256:
            raise ValueError("effect receipt identity does not match its canonical JSON")

    def as_dict(self) -> dict[str, object]:
        return {
            "format": EFFECT_SETTLEMENT_FORMAT,
            "claim_id": self.claim_id,
            "fence": format_scalar("nonnegative", self.fence),
            "execution_id": self.execution_id,
            "execution_sha256": self.execution_sha256,
            "operation": self.operation.as_dict(),
            "input_set_sha256": self.input_set_sha256,
            "artifact_set_sha256": self.artifact_set_sha256,
            "controller_evidence_sha256": self.controller_evidence_sha256,
            "receipt": dict(self.receipt),
            "receipt_sha256": self.receipt_sha256,
            "status": self.status,
        }

    @property
    def sha256(self) -> str:
        return canonical_json_sha256(self.as_dict())

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> ExternalEffectSettlement:
        keys = {
            "format",
            "claim_id",
            "fence",
            "execution_id",
            "execution_sha256",
            "operation",
            "input_set_sha256",
            "artifact_set_sha256",
            "controller_evidence_sha256",
            "receipt",
            "receipt_sha256",
            "status",
        }
        if set(value) != keys or value.get("format") != EFFECT_SETTLEMENT_FORMAT:
            raise ValueError("external effect settlement fields are invalid")
        operation, receipt = value.get("operation"), value.get("receipt")
        if not isinstance(operation, Mapping) or not isinstance(receipt, Mapping):
            raise ValueError("operation and receipt must be objects")
        return cls(
            claim_id=str(value["claim_id"]),
            fence=_positive_decimal(value["fence"], "fence"),
            execution_id=str(value["execution_id"]),
            execution_sha256=str(value["execution_sha256"]),
            operation=OperationIdentity.from_mapping(operation),
            input_set_sha256=str(value["input_set_sha256"]),
            artifact_set_sha256=str(value["artifact_set_sha256"]),
            controller_evidence_sha256=str(value["controller_evidence_sha256"]),
            receipt=dict(receipt),
            receipt_sha256=str(value["receipt_sha256"]),
            status=cast(Literal["succeeded"], value["status"]),
        )
