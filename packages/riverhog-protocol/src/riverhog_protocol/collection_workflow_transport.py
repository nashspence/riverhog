"""Typed HTTP documents for Riverhog-owned collection work.

Application work and controller evidence remain opaque canonical JSON.  The
surrounding identities, custody roots, capabilities, claims, settlement, and
derivation documents are Riverhog contracts and are represented exactly here.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Any, Literal, Self, cast

from http_api_contracts import BrowsePageToken
from pydantic import BaseModel, ConfigDict, Field, model_validator
from riverhog_canonical_json import format_scalar, scalar_schema
from time_formats import CanonicalUtcTimestamp

from riverhog_protocol.collection_workflows import (
    ArtifactDisposition,
    ArtifactDispositionOutput,
    ArtifactDispositionSetIdentity,
    CollectionArtifactIdentity,
    CollectionDerivation,
    CollectionProcessingOutcomeIdentity,
    CollectionRootIdentity,
    OperationIdentity,
    RecipeIdentity,
    SourceCollectionRetirementPolicy,
    canonical_json_bytes,
    canonical_json_sha256,
)
from riverhog_protocol.effect_settlement import (
    EFFECT_RECEIPT_MAX_BYTES,
    OPERATION_CONTRACT_MAX_BYTES,
    ExternalEffectSettlement,
    operation_retirement_permission,
)
from riverhog_protocol.exact_scalar import NonnegativeDecimal
from riverhog_protocol.list_controls import ClaimState, ProcessingClaimSort, SortOrder
from riverhog_protocol.no_output_settlement import (
    NO_OUTPUT_DECISION_MAX_BYTES,
    NoOutputSettlement,
)
from riverhog_protocol.output_collection_policy import OutputCollectionPolicy
from riverhog_protocol.paths import CanonicalRelPath, CollectionId
from riverhog_protocol.principal_ids import ApplicationName, PrincipalId

SHA256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
ProcessingClaimId = SHA256
SemanticId = Annotated[
    str,
    Field(pattern=r"^[a-z0-9](?:[a-z0-9._/-]{0,158}[a-z0-9])?$"),
]
CapabilityAction = Literal["read-inputs", "write-output"]

WORK_DOCUMENT_MAX_BYTES = 4 * 1024 * 1024
CONTROLLER_EVIDENCE_MAX_BYTES = 16 * 1024 * 1024
DISPOSITION_BATCH_MAX = 128
CONSIDERATION_EVIDENCE_MAX_BYTES = 16 * 1024 * 1024
WORKFLOW_SET_BATCH_MAX = 128
ControllerEvidenceDocument = Annotated[
    dict[str, Any],
    Field(
        json_schema_extra={
            "x-riverhog-extent": {
                "policy": "contract_max",
                "reason": "bounded-controller-evidence-envelope",
            },
            "x-riverhog-encoded-bytes-max": CONTROLLER_EVIDENCE_MAX_BYTES,
        }
    ),
]
WorkDocument = Annotated[
    dict[str, Any],
    Field(
        json_schema_extra={
            "x-riverhog-extent": {
                "policy": "contract_max",
                "reason": "bounded-work-document-envelope",
            },
            "x-riverhog-encoded-bytes-max": WORK_DOCUMENT_MAX_BYTES,
        }
    ),
]

EffectReceiptDocument = Annotated[
    dict[str, Any],
    Field(
        json_schema_extra={
            "x-riverhog-extent": {"policy": "contract_max", "reason": "bounded-effect-receipt"},
            "x-riverhog-encoded-bytes-max": EFFECT_RECEIPT_MAX_BYTES,
        }
    ),
]
OperationContractDocument = Annotated[
    dict[str, Any],
    Field(
        json_schema_extra={
            "x-riverhog-extent": {
                "policy": "contract_max",
                "reason": "bounded-operation-declaration",
            },
            "x-riverhog-encoded-bytes-max": OPERATION_CONTRACT_MAX_BYTES,
        }
    ),
]


class RiverhogWorkflowDocument(BaseModel):
    """Closed JSON document with deliberate key lookup convenience."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    def __getitem__(self, key: str) -> Any:
        return self.model_dump(mode="json", exclude_none=False)[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.model_dump(mode="json", exclude_none=False).get(key, default)


def _validate_opaque_document(
    value: Mapping[str, Any],
    digest: str,
    *,
    label: str,
    maximum_bytes: int,
) -> None:
    encoded = canonical_json_bytes(value)
    if len(encoded) > maximum_bytes:
        raise ValueError(f"{label} exceeds {maximum_bytes} canonical JSON bytes")
    if canonical_json_sha256(value) != digest:
        raise ValueError(f"{label} identity does not match its canonical JSON")


class CollectionRootIdentityDocument(RiverhogWorkflowDocument):
    collection_id: CollectionId
    archive_root_sha256: SHA256
    content_identity: SHA256

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        CollectionRootIdentity.from_mapping(self.model_dump(mode="json"))
        return self


class CollectionArtifactIdentityDocument(RiverhogWorkflowDocument):
    collection: CollectionRootIdentityDocument
    path: CanonicalRelPath
    bytes: NonnegativeDecimal
    sha256: SHA256

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        CollectionArtifactIdentity.from_mapping(self.model_dump(mode="json"))
        return self


class ExactSetIdentityDocument(RiverhogWorkflowDocument):
    """Small immutable identity for an exact canonically ordered logical set."""

    count: NonnegativeDecimal = Field(ge=1)
    sha256: SHA256


class ArtifactSetIdentityDocument(ExactSetIdentityDocument):
    total_bytes: NonnegativeDecimal = Field(ge=0)


class ReceivingSetDocument(RiverhogWorkflowDocument):
    state: Literal["receiving", "sealed"]
    count: NonnegativeDecimal = Field(ge=0)
    identity: ExactSetIdentityDocument | None = None

    @model_validator(mode="after")
    def validate_state(self) -> Self:
        if (self.state == "sealed") != (self.identity is not None):
            raise ValueError("set identity is inconsistent with state")
        if self.identity is not None and self.identity.count != self.count:
            raise ValueError("set identity count differs from staged count")
        return self


class OutcomeSetDocument(RiverhogWorkflowDocument):
    state: Literal["receiving", "sealing", "sealed", "failed"]
    count: NonnegativeDecimal = Field(ge=0)
    identity: ExactSetIdentityDocument | None = None
    failure: str | None = Field(default=None, min_length=1, max_length=1000)

    @model_validator(mode="after")
    def validate_state(self) -> Self:
        if (self.state == "sealed") != (self.identity is not None):
            raise ValueError("outcome identity is inconsistent with state")
        if (self.state == "failed") != (self.failure is not None):
            raise ValueError("outcome failure is inconsistent with state")
        if self.identity is not None and self.identity.count != self.count:
            raise ValueError("outcome identity count differs from staged count")
        return self


class ArtifactReceivingSetDocument(RiverhogWorkflowDocument):
    state: Literal["receiving", "sealed"]
    count: NonnegativeDecimal = Field(ge=0)
    total_bytes: NonnegativeDecimal = Field(ge=0)
    identity: ArtifactSetIdentityDocument | None = None

    @model_validator(mode="after")
    def validate_state(self) -> Self:
        if (self.state == "sealed") != (self.identity is not None):
            raise ValueError("artifact identity is inconsistent with state")
        if self.identity is not None and (
            self.identity.count != self.count or self.identity.total_bytes != self.total_bytes
        ):
            raise ValueError("artifact identity totals differ from staged totals")
        return self


class CollectionRootBatchDocument(RiverhogWorkflowDocument):
    fence: NonnegativeDecimal = Field(ge=1)
    start_ordinal: NonnegativeDecimal = Field(ge=0)
    inputs: list[CollectionRootIdentityDocument] = Field(
        min_length=1,
        max_length=WORKFLOW_SET_BATCH_MAX,
        json_schema_extra={
            "uniqueItems": True,
            "x-riverhog-extent": {
                "policy": "segmented_no_total_max",
                "reason": "bounded-set-append",
                "progression": "start_ordinal",
            },
        },
    )


class CollectionRootPageDocument(RiverhogWorkflowDocument):
    identity: ExactSetIdentityDocument
    start_ordinal: NonnegativeDecimal = Field(ge=0)
    next_ordinal: NonnegativeDecimal | None = Field(default=None, ge=1)
    inputs: list[CollectionRootIdentityDocument] = Field(
        max_length=WORKFLOW_SET_BATCH_MAX,
        json_schema_extra={
            "x-riverhog-extent": {
                "policy": "segmented_no_total_max",
                "reason": "bounded-identity-page",
                "progression": "identity-bound-start_ordinal",
            }
        },
    )


class CollectionArtifactBatchDocument(RiverhogWorkflowDocument):
    fence: NonnegativeDecimal = Field(ge=1)
    start_ordinal: NonnegativeDecimal = Field(ge=0)
    artifacts: list[CollectionArtifactIdentityDocument] = Field(
        min_length=1,
        max_length=WORKFLOW_SET_BATCH_MAX,
        json_schema_extra={
            "uniqueItems": True,
            "x-riverhog-extent": {
                "policy": "segmented_no_total_max",
                "reason": "bounded-set-append",
                "progression": "start_ordinal",
            },
        },
    )


class CollectionArtifactPageDocument(RiverhogWorkflowDocument):
    identity: ArtifactSetIdentityDocument
    start_ordinal: NonnegativeDecimal = Field(ge=0)
    next_ordinal: NonnegativeDecimal | None = Field(default=None, ge=1)
    artifacts: list[CollectionArtifactIdentityDocument] = Field(
        max_length=WORKFLOW_SET_BATCH_MAX,
        json_schema_extra={
            "x-riverhog-extent": {
                "policy": "segmented_no_total_max",
                "reason": "bounded-identity-page",
                "progression": "identity-bound-start_ordinal",
            }
        },
    )


class ProcessingOutcomePageDocument(RiverhogWorkflowDocument):
    identity: ExactSetIdentityDocument
    start_ordinal: NonnegativeDecimal = Field(ge=0)
    next_ordinal: NonnegativeDecimal | None = Field(default=None, ge=1)
    outcomes: list[ProcessingOutcomeIdentityDocument] = Field(
        max_length=WORKFLOW_SET_BATCH_MAX,
        json_schema_extra={
            "x-riverhog-extent": {
                "policy": "segmented_no_total_max",
                "reason": "bounded-identity-page",
                "progression": "identity-bound-start_ordinal",
            }
        },
    )


class OperationIdentityDocument(RiverhogWorkflowDocument):
    id: SemanticId
    sha256: SHA256

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        OperationIdentity.from_mapping(self.model_dump(mode="json"))
        return self


class RecipeIdentityDocument(RiverhogWorkflowDocument):
    id: SemanticId
    revision: NonnegativeDecimal = Field(ge=1)
    sha256: SHA256

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        RecipeIdentity.from_mapping(self.model_dump(mode="json"))
        return self


class ProcessingOutcomeIdentityDocument(RiverhogWorkflowDocument):
    model_config = ConfigDict(
        json_schema_extra={
            "oneOf": [
                {
                    "properties": {
                        "result_kind": {"const": "collection"},
                        "output_collection": {"type": "object"},
                        "derivation_sha256": {"type": "string"},
                        "effect_receipt_sha256": {"type": "null"},
                        "effect_settlement_sha256": {"type": "null"},
                        "no_output_settlement_sha256": {"type": "null"},
                    },
                    "required": ["output_collection", "derivation_sha256"],
                },
                {
                    "properties": {
                        "result_kind": {"const": "external-effect"},
                        "output_collection": {"type": "null"},
                        "derivation_sha256": {"type": "null"},
                        "effect_receipt_sha256": {"type": "string"},
                        "effect_settlement_sha256": {"type": "string"},
                        "no_output_settlement_sha256": {"type": "null"},
                    },
                    "required": ["effect_receipt_sha256", "effect_settlement_sha256"],
                },
                {
                    "properties": {
                        "result_kind": {"const": "no-output"},
                        "output_collection": {"type": "null"},
                        "derivation_sha256": {"type": "null"},
                        "effect_receipt_sha256": {"type": "null"},
                        "effect_settlement_sha256": {"type": "null"},
                        "no_output_settlement_sha256": {"type": "string"},
                    },
                    "required": ["no_output_settlement_sha256"],
                },
            ]
        }
    )
    outcome_id: SemanticId
    source_claim_id: ProcessingClaimId
    source_fence: NonnegativeDecimal = Field(ge=1)
    execution_id: SHA256
    result_kind: Literal["collection", "external-effect", "no-output"]
    output_collection: CollectionRootIdentityDocument | None = None
    derivation_sha256: SHA256 | None = None
    effect_receipt_sha256: SHA256 | None = None
    effect_settlement_sha256: SHA256 | None = None
    no_output_settlement_sha256: SHA256 | None = None

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        CollectionProcessingOutcomeIdentity.from_mapping(
            self.model_dump(mode="json", exclude_none=True)
        )
        return self


class ExternalEffectSettlementDocument(RiverhogWorkflowDocument):
    format: Literal["riverhog-external-effect-settlement/v1"]
    claim_id: ProcessingClaimId
    fence: NonnegativeDecimal = Field(ge=1)
    execution_id: SHA256
    execution_sha256: SHA256
    operation: OperationIdentityDocument
    input_set_sha256: SHA256
    artifact_set_sha256: SHA256
    controller_evidence_sha256: SHA256
    receipt: EffectReceiptDocument
    receipt_sha256: SHA256
    disposition_set: ArtifactDispositionSetIdentityDocument
    status: Literal["succeeded"]

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        ExternalEffectSettlement.from_mapping(self.model_dump(mode="json"))
        return self


class ArtifactDispositionInputDocument(RiverhogWorkflowDocument):
    collection_id: CollectionId
    archive_root_sha256: SHA256
    path: CanonicalRelPath


class ArtifactDispositionFailureDocument(RiverhogWorkflowDocument):
    code: SemanticId
    message: str = Field(min_length=1, max_length=500)


class ArtifactDispositionDecisionDocument(RiverhogWorkflowDocument):
    code: SemanticId
    message: str = Field(min_length=1, max_length=1000)


class ArtifactDiscardApprovalDocument(RiverhogWorkflowDocument):
    controller_id: ApplicationName
    rule_sha256: SHA256
    evidence: dict[str, Any]
    evidence_sha256: SHA256


class ConsiderationEvidencePutDocument(RiverhogWorkflowDocument):
    fence: NonnegativeDecimal = Field(ge=1)
    sha256: SHA256
    document: dict[str, Any] = Field(
        json_schema_extra={
            "x-riverhog-extent": {
                "policy": "contract_max",
                "reason": "bounded-observation-evidence-document",
            },
            "x-riverhog-encoded-bytes-max": CONSIDERATION_EVIDENCE_MAX_BYTES,
        }
    )

    @model_validator(mode="after")
    def exact_document(self) -> Self:
        encoded = canonical_json_bytes(self.document)
        if len(encoded) > CONSIDERATION_EVIDENCE_MAX_BYTES:
            raise ValueError("consideration evidence document exceeds its byte limit")
        if canonical_json_sha256(self.document) != self.sha256:
            raise ValueError("consideration evidence identity differs from its document")
        return self


class ConsiderationEvidenceOutDocument(RiverhogWorkflowDocument):
    sha256: SHA256


class ConsiderationEvidenceReadDocument(RiverhogWorkflowDocument):
    sha256: SHA256
    document: dict[str, Any]

    @model_validator(mode="after")
    def exact_document(self) -> Self:
        if canonical_json_sha256(self.document) != self.sha256:
            raise ValueError("retained consideration evidence differs from its identity")
        return self


class ArtifactDispositionDocument(RiverhogWorkflowDocument):
    model_config = ConfigDict(
        json_schema_extra={
            "oneOf": [
                {
                    "properties": {
                        "status": {"enum": ["transformed", "preserved"]},
                        "failure": {"type": "null"},
                        "decision": {"type": "null"},
                        "effect_receipt_sha256": {"type": "null"},
                    }
                },
                {
                    "properties": {
                        "status": {"const": "effect-applied"},
                        "failure": {"type": "null"},
                        "decision": {"type": "null"},
                        "effect_receipt_sha256": {"type": "string"},
                    },
                    "required": ["effect_receipt_sha256"],
                },
                {
                    "properties": {
                        "status": {"const": "not-carried-forward"},
                        "failure": {"type": "null"},
                        "decision": {"type": "object"},
                        "effect_receipt_sha256": {"type": "null"},
                    },
                    "required": ["decision"],
                },
                {
                    "properties": {
                        "status": {"enum": ["omitted", "rejected"]},
                        "failure": {"type": "object"},
                        "decision": {"type": "null"},
                        "effect_receipt_sha256": {"type": "null"},
                    },
                    "required": ["failure"],
                },
            ]
        }
    )

    input: ArtifactDispositionInputDocument
    status: Literal[
        "transformed", "preserved", "effect-applied", "not-carried-forward", "omitted", "rejected"
    ]
    failure: ArtifactDispositionFailureDocument | None = None
    decision: ArtifactDispositionDecisionDocument | None = None
    effect_receipt_sha256: SHA256 | None = None
    discard_approval: ArtifactDiscardApprovalDocument | None = None
    retain_required: bool | None = None

    @model_validator(mode="after")
    def validate_disposition(self) -> Self:
        ArtifactDisposition.from_mapping(self.model_dump(mode="json", exclude_none=True))
        return self


class ArtifactDispositionOutputDocument(RiverhogWorkflowDocument):
    input: ArtifactDispositionInputDocument
    output_path: CanonicalRelPath

    @model_validator(mode="after")
    def validate_output(self) -> Self:
        ArtifactDispositionOutput.from_mapping(self.model_dump(mode="json"))
        return self


class ArtifactDispositionBatchDocument(RiverhogWorkflowDocument):
    fence: NonnegativeDecimal = Field(ge=1)
    dispositions: list[ArtifactDispositionDocument] = Field(
        min_length=1,
        max_length=DISPOSITION_BATCH_MAX,
        json_schema_extra={
            "uniqueItems": True,
            "x-riverhog-extent": {
                "policy": "segmented_no_total_max",
                "reason": "bounded-disposition-append",
                "progression": "sealed-disposition-identity",
            },
        },
    )


class ArtifactDispositionOutputBatchDocument(RiverhogWorkflowDocument):
    fence: NonnegativeDecimal = Field(ge=1)
    outputs: list[ArtifactDispositionOutputDocument] = Field(
        min_length=1,
        max_length=DISPOSITION_BATCH_MAX,
        json_schema_extra={
            "uniqueItems": True,
            "x-riverhog-extent": {
                "policy": "segmented_no_total_max",
                "reason": "bounded-disposition-append",
                "progression": "sealed-disposition-identity",
            },
        },
    )


class ArtifactDispositionSetIdentityDocument(RiverhogWorkflowDocument):
    disposition_count: NonnegativeDecimal = Field(ge=1)
    output_edge_count: NonnegativeDecimal = Field(ge=0)
    output_artifact_count: NonnegativeDecimal = Field(ge=0)
    sha256: SHA256

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        ArtifactDispositionSetIdentity.from_mapping(self.model_dump(mode="json"))
        return self


class NoOutputSettlementDocument(RiverhogWorkflowDocument):
    format: Literal["riverhog-no-output-settlement/v1"]
    claim_id: ProcessingClaimId
    fence: NonnegativeDecimal = Field(ge=1)
    execution_id: SHA256
    operation: OperationIdentityDocument
    input_set_sha256: SHA256
    artifact_set_sha256: SHA256
    controller_evidence_sha256: SHA256
    decision: dict[str, Any] = Field(
        json_schema_extra={
            "x-riverhog-extent": {
                "policy": "contract_max",
                "reason": "bounded-no-output-controller-decision",
            },
            "x-riverhog-encoded-bytes-max": NO_OUTPUT_DECISION_MAX_BYTES,
        }
    )
    decision_sha256: SHA256
    disposition_set: ArtifactDispositionSetIdentityDocument
    status: Literal["succeeded"]

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        NoOutputSettlement.from_mapping(self.model_dump(mode="json"))
        return self


class ArtifactDispositionSetDocument(RiverhogWorkflowDocument):
    claim_id: ProcessingClaimId
    state: Literal["receiving", "sealing", "sealed", "failed"]
    disposition_count: NonnegativeDecimal = Field(ge=0)
    output_edge_count: NonnegativeDecimal = Field(ge=0)
    output_artifact_count: NonnegativeDecimal = Field(ge=0)
    identity: ArtifactDispositionSetIdentityDocument | None = None
    failure: str | None = Field(default=None, min_length=1, max_length=1000)

    @model_validator(mode="after")
    def validate_state(self) -> Self:
        if (self.state == "sealed") != (self.identity is not None):
            raise ValueError("sealed disposition set identity is inconsistent with state")
        if (self.state == "failed") != (self.failure is not None):
            raise ValueError("disposition set failure is inconsistent with state")
        if self.identity is not None and (
            self.disposition_count != self.identity.disposition_count
            or self.output_edge_count != self.identity.output_edge_count
            or self.output_artifact_count != self.identity.output_artifact_count
        ):
            raise ValueError("disposition set counts differ from its sealed identity")
        return self


class ArtifactDispositionPageDocument(RiverhogWorkflowDocument):
    identity: ArtifactDispositionSetIdentityDocument
    start_ordinal: NonnegativeDecimal = Field(ge=0)
    next_ordinal: NonnegativeDecimal | None = Field(default=None, ge=1)
    dispositions: list[ArtifactDispositionDocument] = Field(
        max_length=DISPOSITION_BATCH_MAX,
        json_schema_extra={
            "x-riverhog-extent": {
                "policy": "segmented_no_total_max",
                "reason": "bounded-identity-page",
                "progression": "identity-bound-start_ordinal",
            }
        },
    )


class ArtifactDispositionOutputPageDocument(RiverhogWorkflowDocument):
    identity: ArtifactDispositionSetIdentityDocument
    start_ordinal: NonnegativeDecimal = Field(ge=0)
    next_ordinal: NonnegativeDecimal | None = Field(default=None, ge=1)
    outputs: list[ArtifactDispositionOutputDocument] = Field(
        max_length=DISPOSITION_BATCH_MAX,
        json_schema_extra={
            "x-riverhog-extent": {
                "policy": "segmented_no_total_max",
                "reason": "bounded-identity-page",
                "progression": "identity-bound-start_ordinal",
            }
        },
    )


class ClaimFenceDocument(RiverhogWorkflowDocument):
    id: ProcessingClaimId
    fence: NonnegativeDecimal = Field(ge=1)


class CollectionDerivationDocument(RiverhogWorkflowDocument):
    format: Literal["riverhog-collection-derivation/v1"]
    execution_id: SHA256
    claim: ClaimFenceDocument
    recipe: RecipeIdentityDocument
    operation: OperationIdentityDocument
    input_set_sha256: SHA256
    artifact_set_sha256: SHA256
    execution_envelope_sha256: SHA256
    execution_sha256: SHA256
    controller_evidence: ControllerEvidenceDocument
    controller_evidence_sha256: SHA256
    disposition_set: ArtifactDispositionSetIdentityDocument

    @model_validator(mode="after")
    def validate_derivation(self) -> Self:
        CollectionDerivation.from_mapping(self.model_dump(mode="json"))
        _validate_opaque_document(
            self.controller_evidence,
            self.controller_evidence_sha256,
            label="controller evidence",
            maximum_bytes=CONTROLLER_EVIDENCE_MAX_BYTES,
        )
        return self


class ProcessingClaimCreateDocument(RiverhogWorkflowDocument):
    work_id: SHA256
    work_document: WorkDocument
    work_document_sha256: SHA256
    lease_seconds: int = Field(default=1800, ge=30, le=86400)
    purpose: str = Field(default="collection-work/v1", min_length=1, max_length=160)

    @model_validator(mode="after")
    def validate_claim(self) -> Self:
        _validate_opaque_document(
            self.work_document,
            self.work_document_sha256,
            label="work document",
            maximum_bytes=WORK_DOCUMENT_MAX_BYTES,
        )
        return self


class ProcessingClaimRenewDocument(RiverhogWorkflowDocument):
    fence: NonnegativeDecimal = Field(ge=1)
    lease_seconds: int = Field(default=1800, ge=30, le=86400)


class ProcessingClaimRestartDocument(ProcessingClaimRenewDocument):
    pass


class ProcessingClaimPlanSealDocument(RiverhogWorkflowDocument):
    model_config = ConfigDict(
        json_schema_extra={
            "if": {"properties": {"source_collection_retirement_policy": {"const": "retain"}}},
            "then": {"properties": {"source_collection_retirement_grace_seconds": {"const": "0"}}},
        }
    )

    fence: NonnegativeDecimal = Field(ge=1)
    execution_id: SHA256
    controller_evidence: ControllerEvidenceDocument
    controller_evidence_sha256: SHA256
    operation: OperationIdentityDocument
    result_kind: Literal["collection", "external-effect", "no-output"] = "collection"
    operation_contract: OperationContractDocument | None = None
    output_policy: OutputCollectionPolicy = Field(default_factory=OutputCollectionPolicy)
    source_collection_retirement_policy: SourceCollectionRetirementPolicy = Field(
        default="retain",
        description=(
            "Retain source collections, or permit their permanent deletion after exact "
            "settlement, the grace period, and collection deletion checks."
        ),
    )
    source_collection_retirement_grace_seconds: NonnegativeDecimal = Field(
        default_factory=lambda: 0, ge=0
    )

    @model_validator(mode="after")
    def validate_plan(self) -> Self:
        _validate_opaque_document(
            self.controller_evidence,
            self.controller_evidence_sha256,
            label="controller evidence",
            maximum_bytes=CONTROLLER_EVIDENCE_MAX_BYTES,
        )
        if (
            self.source_collection_retirement_policy == "retain"
            and self.source_collection_retirement_grace_seconds
        ):
            raise ValueError("retained source collections cannot declare retirement grace")
        permitted = operation_retirement_permission(
            OperationIdentity.from_mapping(self.operation.model_dump(mode="json")),
            self.result_kind,
            self.operation_contract,
        )
        if self.source_collection_retirement_policy == "retire-after-settlement" and not permitted:
            raise ValueError("recipe retirement requires an exact permitting operation declaration")
        if self.result_kind != "collection" and self.output_policy != OutputCollectionPolicy():
            raise ValueError("noncollection claim cannot publish a collection")
        return self


def _default_capability_actions() -> list[CapabilityAction]:
    return ["read-inputs"]


class ProcessingCapabilityCreateDocument(RiverhogWorkflowDocument):
    fence: NonnegativeDecimal = Field(ge=1)
    audience: str = Field(pattern=r"^[a-z0-9][a-z0-9._:/-]{0,299}$")
    actions: list[CapabilityAction] = Field(
        default_factory=_default_capability_actions,
        min_length=1,
        json_schema_extra={
            "oneOf": [
                {"const": ["read-inputs"]},
                {"const": ["read-inputs", "write-output"]},
            ]
        },
    )
    ttl_seconds: int = Field(default=900, ge=30, le=86400)

    @model_validator(mode="after")
    def validate_capability(self) -> Self:
        if self.actions not in (["read-inputs"], ["read-inputs", "write-output"]):
            raise ValueError(
                "capability actions must be read-inputs, optionally followed by write-output"
            )
        if self.actions != sorted(set(self.actions)):
            raise ValueError("capability actions must be unique and canonically ordered")
        return self


class ProcessingOutcomeBindingDocument(RiverhogWorkflowDocument):
    claim_id: ProcessingClaimId
    fence: NonnegativeDecimal = Field(ge=1)
    outcome_id: SemanticId


class ProcessingClaimSettleDocument(RiverhogWorkflowDocument):
    fence: NonnegativeDecimal = Field(ge=1)
    output_collection_id: CollectionId
    derivation: CollectionDerivationDocument
    outcome: ProcessingOutcomeBindingDocument | None = None


class ProcessingClaimEffectSettleDocument(RiverhogWorkflowDocument):
    fence: NonnegativeDecimal = Field(ge=1)
    settlement: ExternalEffectSettlementDocument
    outcome: ProcessingOutcomeBindingDocument | None = None

    @model_validator(mode="after")
    def validate_fence(self) -> Self:
        if self.fence != self.settlement.fence:
            raise ValueError("effect settlement differs from the requested fence")
        return self


class ProcessingClaimNoOutputSettleDocument(RiverhogWorkflowDocument):
    fence: NonnegativeDecimal = Field(ge=1)
    settlement: NoOutputSettlementDocument
    outcome: ProcessingOutcomeBindingDocument | None = None

    @model_validator(mode="after")
    def validate_fence(self) -> Self:
        if self.fence != self.settlement.fence:
            raise ValueError("no-output settlement differs from the requested fence")
        return self


class ProcessingClaimOutcomesAppendDocument(RiverhogWorkflowDocument):
    fence: NonnegativeDecimal = Field(ge=1)
    outcomes: list[ProcessingOutcomeIdentityDocument] = Field(
        min_length=1,
        max_length=WORKFLOW_SET_BATCH_MAX,
        json_schema_extra={
            "x-riverhog-extent": {
                "policy": "segmented_no_total_max",
                "reason": "bounded-outcome-append",
                "progression": "immutable-outcome-label",
            }
        },
    )


class ProcessingClaimOutcomesSettleDocument(RiverhogWorkflowDocument):
    model_config = ConfigDict(
        json_schema_extra={
            "if": {"properties": {"source_collection_retirement_policy": {"const": "retain"}}},
            "then": {"properties": {"source_collection_retirement_grace_seconds": {"const": "0"}}},
        }
    )

    fence: NonnegativeDecimal = Field(ge=1)
    outcomes: ExactSetIdentityDocument
    source_collection_retirement_policy: SourceCollectionRetirementPolicy = Field(
        default="retain",
        description=(
            "Retain source collections, or permit their permanent deletion after exact "
            "settlement, the grace period, and collection deletion checks."
        ),
    )
    source_collection_retirement_grace_seconds: NonnegativeDecimal = Field(
        default_factory=lambda: 0, ge=0
    )

    @model_validator(mode="after")
    def validate_outcomes(self) -> Self:
        if (
            self.source_collection_retirement_policy == "retain"
            and self.source_collection_retirement_grace_seconds
        ):
            raise ValueError("retained source collections cannot declare retirement grace")
        return self


class ProcessingClaimFenceDocument(RiverhogWorkflowDocument):
    fence: NonnegativeDecimal = Field(ge=1)


class ProcessingClaimAbandonDocument(ProcessingClaimFenceDocument):
    reason: str = Field(min_length=1, max_length=1000)


class ProcessingClaimConsumerDocument(RiverhogWorkflowDocument):
    app: ApplicationName
    key_id: str | None = Field(default=None, min_length=1, max_length=300)


class ProcessingClaimPlanDocument(RiverhogWorkflowDocument):
    model_config = ConfigDict(
        json_schema_extra={
            "if": {"properties": {"source_collection_retirement_policy": {"const": "retain"}}},
            "then": {"properties": {"source_collection_retirement_grace_seconds": {"const": "0"}}},
        }
    )

    execution_id: SHA256
    controller_evidence: ControllerEvidenceDocument
    controller_evidence_sha256: SHA256
    operation: OperationIdentityDocument
    result_kind: Literal["collection", "external-effect", "no-output"] = "collection"
    operation_contract: OperationContractDocument | None = None
    output_policy: OutputCollectionPolicy = Field(default_factory=OutputCollectionPolicy)
    inputs: ExactSetIdentityDocument
    artifacts: ArtifactSetIdentityDocument
    source_collection_retirement_policy: SourceCollectionRetirementPolicy
    source_collection_retirement_grace_seconds: NonnegativeDecimal = Field(ge=0)
    sealed_at: CanonicalUtcTimestamp

    @model_validator(mode="after")
    def validate_plan(self) -> Self:
        ProcessingClaimPlanSealDocument.model_validate(
            {
                "fence": "1",
                "execution_id": self.execution_id,
                "controller_evidence": self.controller_evidence,
                "controller_evidence_sha256": self.controller_evidence_sha256,
                "operation": self.operation.model_dump(mode="json"),
                "result_kind": self.result_kind,
                "operation_contract": self.operation_contract,
                "output_policy": self.output_policy,
                "source_collection_retirement_policy": self.source_collection_retirement_policy,
                "source_collection_retirement_grace_seconds": format_scalar(
                    "nonnegative", self.source_collection_retirement_grace_seconds
                ),
            }
        )
        return self


class ProcessingClaimOutcomeSettlementDocument(RiverhogWorkflowDocument):
    model_config = ConfigDict(
        json_schema_extra={
            "if": {"properties": {"source_collection_retirement_policy": {"const": "retain"}}},
            "then": {"properties": {"source_collection_retirement_grace_seconds": {"const": "0"}}},
        }
    )

    outcomes: ExactSetIdentityDocument
    source_collection_retirement_policy: SourceCollectionRetirementPolicy
    source_collection_retirement_grace_seconds: NonnegativeDecimal = Field(ge=0)

    @model_validator(mode="after")
    def validate_retirement(self) -> Self:
        if (
            self.source_collection_retirement_policy == "retain"
            and self.source_collection_retirement_grace_seconds
        ):
            raise ValueError("retained source collections cannot declare retirement grace")
        return self


class SourceCollectionRetirementClaimReferenceDocument(RiverhogWorkflowDocument):
    """Exact Riverhog settlement authorizing one source deletion plan."""

    model_config = ConfigDict(
        json_schema_extra={
            "oneOf": [
                {
                    "properties": {
                        "execution_id": {"type": "string"},
                        "output_collection_id": cast(Any, scalar_schema("sequence63")),
                        "effect_settlement_sha256": {"type": "null"},
                        "no_output_settlement_sha256": {"type": "null"},
                        "outcomes": {"type": "null"},
                    },
                    "required": ["execution_id", "output_collection_id"],
                },
                {
                    "properties": {
                        "execution_id": {"type": "string"},
                        "output_collection_id": {"type": "null"},
                        "effect_settlement_sha256": {"type": "string"},
                        "no_output_settlement_sha256": {"type": "null"},
                        "outcomes": {"type": "null"},
                    },
                    "required": ["execution_id", "effect_settlement_sha256"],
                },
                {
                    "properties": {
                        "execution_id": {"type": "string"},
                        "output_collection_id": {"type": "null"},
                        "effect_settlement_sha256": {"type": "null"},
                        "no_output_settlement_sha256": {"type": "string"},
                        "outcomes": {"type": "null"},
                    },
                    "required": ["execution_id", "no_output_settlement_sha256"],
                },
                {
                    "properties": {
                        "execution_id": {"type": "null"},
                        "output_collection_id": {"type": "null"},
                        "effect_settlement_sha256": {"type": "null"},
                        "no_output_settlement_sha256": {"type": "null"},
                        "outcomes": {"type": "object"},
                    },
                    "required": ["outcomes"],
                },
            ]
        }
    )
    claim_id: ProcessingClaimId
    fence: NonnegativeDecimal = Field(ge=1)
    work_id: SHA256
    execution_id: SHA256 | None = None
    output_collection_id: CollectionId | None = None
    effect_settlement_sha256: SHA256 | None = None
    no_output_settlement_sha256: SHA256 | None = None
    outcomes: ExactSetIdentityDocument | None = None

    @model_validator(mode="after")
    def validate_settlement_form(self) -> Self:
        collection = self.output_collection_id is not None
        effect = self.effect_settlement_sha256 is not None
        no_output = self.no_output_settlement_sha256 is not None
        delegated = self.outcomes is not None
        if sum((collection, effect, no_output, delegated)) != 1:
            raise ValueError("retirement requires exactly one direct or delegated settlement")
        if (collection or effect or no_output) != (self.execution_id is not None):
            raise ValueError("retirement execution differs from its settlement form")
        return self


class ProcessingClaimDocument(RiverhogWorkflowDocument):
    model_config = ConfigDict(
        json_schema_extra={
            "allOf": [
                {
                    "if": {"properties": {"state": {"enum": ["settled", "retiring", "released"]}}},
                    "then": {
                        "required": ["settled_at"],
                        "properties": {"settled_at": {"type": "string"}},
                    },
                    "else": {"properties": {"settled_at": {"type": "null"}}},
                },
                {
                    "if": {"properties": {"state": {"const": "abandoned"}}},
                    "then": {
                        "required": ["abandoned_at", "abandonment_reason"],
                        "properties": {
                            "abandoned_at": {"type": "string"},
                            "abandonment_reason": {"type": "string"},
                        },
                    },
                    "else": {
                        "properties": {
                            "abandoned_at": {"type": "null"},
                            "abandonment_reason": {"type": "null"},
                        }
                    },
                },
                {
                    "if": {"properties": {"state": {"const": "released"}}},
                    "then": {
                        "required": ["released_at"],
                        "properties": {"released_at": {"type": "string"}},
                    },
                    "else": {"properties": {"released_at": {"type": "null"}}},
                },
            ]
        }
    )

    format: Literal["riverhog-processing-claim/v1"]
    id: ProcessingClaimId
    work_id: SHA256
    consumer: ProcessingClaimConsumerDocument
    purpose: str = Field(min_length=1, max_length=160)
    state: ClaimState
    fence: NonnegativeDecimal = Field(ge=1)
    expires_at: CanonicalUtcTimestamp
    created_at: CanonicalUtcTimestamp
    updated_at: CanonicalUtcTimestamp
    settled_at: CanonicalUtcTimestamp | None = None
    abandoned_at: CanonicalUtcTimestamp | None = None
    abandonment_reason: str | None = Field(default=None, min_length=1, max_length=1000)
    released_at: CanonicalUtcTimestamp | None = None
    output_collection_id: CollectionId | None = None
    effect_settlement_sha256: SHA256 | None = None
    no_output_settlement_sha256: SHA256 | None = None
    work_document: WorkDocument
    work_document_sha256: SHA256
    inputs: ReceivingSetDocument
    plan: ProcessingClaimPlanDocument | None = None
    outcomes: OutcomeSetDocument
    outcome_settlement: ProcessingClaimOutcomeSettlementDocument | None = None

    @model_validator(mode="after")
    def validate_claim(self) -> Self:
        ProcessingClaimCreateDocument(
            work_id=self.work_id,
            work_document=self.work_document,
            work_document_sha256=self.work_document_sha256,
            purpose=self.purpose,
        )
        if self.outcome_settlement is not None:
            if self.outcomes.identity != self.outcome_settlement.outcomes:
                raise ValueError("processing outcome settlement identity is invalid")
            if self.plan is not None or self.state not in {"settled", "retiring", "released"}:
                raise ValueError("processing outcome settlement is inconsistent with claim state")
        settled = self.state in {"settled", "retiring", "released"}
        if settled != (self.settled_at is not None):
            raise ValueError("claim settlement timestamp is inconsistent with claim state")
        abandoned = self.state == "abandoned"
        if abandoned != (self.abandoned_at is not None):
            raise ValueError("claim abandonment timestamp is inconsistent with claim state")
        if abandoned != (self.abandonment_reason is not None):
            raise ValueError("claim abandonment reason is inconsistent with claim state")
        if self.abandonment_reason is not None and (
            self.abandonment_reason.strip() != self.abandonment_reason
        ):
            raise ValueError("claim abandonment reason must be canonical")
        released = self.state == "released"
        if released != (self.released_at is not None):
            raise ValueError("claim release timestamp is inconsistent with claim state")
        if self.plan is not None and self.outcomes.count:
            raise ValueError("direct collection work cannot retain delegated outcomes")
        if settled:
            if self.plan is not None:
                if self.outcome_settlement is not None:
                    raise ValueError("direct claim cannot contain delegated settlement")
                if self.plan.result_kind == "external-effect":
                    if (
                        self.effect_settlement_sha256 is None
                        or self.output_collection_id is not None
                        or self.no_output_settlement_sha256 is not None
                    ):
                        raise ValueError("direct effect settlement evidence is incomplete")
                elif self.plan.result_kind == "no-output":
                    if (
                        self.no_output_settlement_sha256 is None
                        or self.output_collection_id is not None
                        or self.effect_settlement_sha256 is not None
                    ):
                        raise ValueError("direct no-output settlement evidence is incomplete")
                elif (
                    self.output_collection_id is None
                    or self.effect_settlement_sha256 is not None
                    or self.no_output_settlement_sha256 is not None
                ):
                    raise ValueError("direct collection settlement evidence is incomplete")
            elif (
                self.output_collection_id is not None
                or self.effect_settlement_sha256 is not None
                or self.no_output_settlement_sha256 is not None
                or self.outcomes.identity is None
                or self.outcome_settlement is None
            ):
                raise ValueError("delegated claim settlement evidence is incomplete")
        elif (
            self.output_collection_id is not None
            or self.outcome_settlement is not None
            or self.effect_settlement_sha256 is not None
            or self.no_output_settlement_sha256 is not None
        ):
            raise ValueError("unsettled claim cannot publish settlement evidence")
        return self


class ProcessingClaimFiltersDocument(RiverhogWorkflowDocument):
    state: ClaimState | None = None


class ProcessingClaimPageDocument(RiverhogWorkflowDocument):
    page_size: int = Field(ge=1, le=100)
    next_page_token: BrowsePageToken | None
    sort: ProcessingClaimSort
    order: SortOrder
    filters: ProcessingClaimFiltersDocument
    claims: list[ProcessingClaimDocument]


class ProcessingCapabilityDocument(RiverhogWorkflowDocument):
    format: Literal["riverhog-processing-capability/v1"]
    id: str = Field(min_length=1, max_length=160)
    claim_id: ProcessingClaimId
    fence: NonnegativeDecimal = Field(ge=1)
    audience: str = Field(pattern=r"^[a-z0-9][a-z0-9._:/-]{0,299}$")
    actions: list[CapabilityAction] = Field(
        min_length=1,
        json_schema_extra={
            "oneOf": [
                {"const": ["read-inputs"]},
                {"const": ["read-inputs", "write-output"]},
            ]
        },
    )
    state: Literal["receiving", "active"]
    principal_id: PrincipalId = Field(max_length=300)
    expires_at: CanonicalUtcTimestamp
    artifacts: ArtifactReceivingSetDocument
    token: str = Field(pattern=r"^rhc_[A-Za-z0-9_-]+$")

    @model_validator(mode="after")
    def validate_capability(self) -> Self:
        ProcessingCapabilityCreateDocument.model_validate(
            {
                "fence": format_scalar("nonnegative", self.fence),
                "audience": self.audience,
                "actions": self.actions,
            }
        )
        return self


class CollectionDerivationResponseDocument(RiverhogWorkflowDocument):
    collection_id: CollectionId
    document_sha256: SHA256
    derivation: CollectionDerivationDocument

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        document = CollectionDerivation.from_mapping(self.derivation.model_dump(mode="json"))
        if document.sha256 != self.document_sha256:
            raise ValueError("collection derivation identity does not match its document")
        return self


__all__ = [
    "ProcessingClaimOutcomesAppendDocument",
    "ProcessingClaimEffectSettleDocument",
    "ProcessingClaimNoOutputSettleDocument",
    "ExternalEffectSettlementDocument",
    "NoOutputSettlementDocument",
    "CONTROLLER_EVIDENCE_MAX_BYTES",
    "CONSIDERATION_EVIDENCE_MAX_BYTES",
    "DISPOSITION_BATCH_MAX",
    "ArtifactDispositionBatchDocument",
    "ConsiderationEvidencePutDocument",
    "ConsiderationEvidenceOutDocument",
    "ConsiderationEvidenceReadDocument",
    "CapabilityAction",
    "ClaimState",
    "ArtifactDispositionDocument",
    "ArtifactDispositionOutputPageDocument",
    "ArtifactDispositionOutputBatchDocument",
    "ArtifactDispositionOutputDocument",
    "ArtifactDispositionPageDocument",
    "ArtifactDispositionSetDocument",
    "ArtifactDispositionSetIdentityDocument",
    "ArtifactReceivingSetDocument",
    "ArtifactSetIdentityDocument",
    "CollectionArtifactBatchDocument",
    "CollectionArtifactIdentityDocument",
    "CollectionArtifactPageDocument",
    "CollectionDerivationDocument",
    "CollectionDerivationResponseDocument",
    "CollectionRootIdentityDocument",
    "CollectionRootBatchDocument",
    "CollectionRootPageDocument",
    "ExactSetIdentityDocument",
    "OperationIdentityDocument",
    "OutcomeSetDocument",
    "ProcessingClaimAbandonDocument",
    "ProcessingClaimCreateDocument",
    "ProcessingClaimDocument",
    "ProcessingClaimFenceDocument",
    "ProcessingClaimOutcomesSettleDocument",
    "ProcessingClaimId",
    "ProcessingClaimPageDocument",
    "ProcessingClaimPlanSealDocument",
    "ProcessingClaimRenewDocument",
    "ProcessingClaimRestartDocument",
    "ProcessingClaimSettleDocument",
    "ProcessingOutcomeBindingDocument",
    "ProcessingOutcomeIdentityDocument",
    "ProcessingOutcomePageDocument",
    "RecipeIdentityDocument",
    "ReceivingSetDocument",
    "SourceCollectionRetirementClaimReferenceDocument",
    "RiverhogWorkflowDocument",
    "ProcessingCapabilityCreateDocument",
    "ProcessingCapabilityDocument",
    "WORK_DOCUMENT_MAX_BYTES",
    "WORKFLOW_SET_BATCH_MAX",
]
