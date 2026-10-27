"""Exact current controls for application-owned prepared-primary state."""

from __future__ import annotations

from typing import Annotated, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    StringConstraints,
    field_validator,
    model_validator,
)
from riverhog_canonical_json import canonical_json_bytes
from riverhog_protocol import (
    ArtifactId,
    ArtifactMemberIdentityDocument,
    CollectionArtifactProvenanceBindingDocument,
    CollectionUploadArtifactCustodyReceiptDocument,
    validate_collection_upload_artifact_custody_receipt,
)
from riverhog_protocol.exact_scalar import NonnegativeDecimal
from riverhog_protocol.paths import CollectionId
from riverhog_provenance_contracts import ProvenanceId

from riverhog_client._prepared_primary_types import PrimaryByteFile, primary_file_name

Sha256 = Annotated[str, StringConstraints(strict=True, pattern=r"^[0-9a-f]{64}$")]
Identifier = Annotated[str, StringConstraints(strict=True, min_length=1)]


class PrimaryControl(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    format: Literal["riverhog-prepared-primary/v1"] = "riverhog-prepared-primary/v1"


class ByteCommitment(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    bytes: NonnegativeDecimal
    sha256: Sha256


class ByteFile(ByteCommitment):
    name: str

    @field_validator("name")
    @classmethod
    def exact_name(cls, value: str) -> str:
        return primary_file_name(value)

    def byte_file(self) -> PrimaryByteFile:
        return PrimaryByteFile(self.name, self.bytes, self.sha256)

    def commitment(self) -> ByteCommitment:
        return ByteCommitment.model_validate({"bytes": str(self.bytes), "sha256": self.sha256})


class PrimaryScope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    service: Identifier
    account: Identifier


class PrimaryAllocation(PrimaryControl):
    kind: Literal["allocation"] = "allocation"
    allocation_id: Identifier
    scope: PrimaryScope
    control_record_bytes: NonnegativeDecimal


class PrimaryCreating(PrimaryControl):
    kind: Literal["creating"] = "creating"
    allocation_sha256: Sha256
    request: ByteFile


class PrimarySession(PrimaryControl):
    kind: Literal["session"] = "session"
    allocation_sha256: Sha256
    create_request: ByteCommitment
    collection_id: CollectionId
    delivery_context_id: ProvenanceId
    server_construction_identity_sha256: Sha256
    attribution_construction_identity_sha256: Sha256
    completion_requirement_sha256: Sha256 | None


class PrimaryObservation(PrimaryControl):
    kind: Literal["observation"] = "observation"
    artifact_id: ProvenanceId
    occurrence_id: ProvenanceId
    state_id: ProvenanceId
    capture_id: ProvenanceId
    observation_id: ProvenanceId
    observer_agent_id: ProvenanceId
    catalog_sha256: Sha256
    graph: ByteFile


class PreparedPrimary(PrimaryControl):
    kind: Literal["prepared"] = "prepared"
    session_sha256: Sha256
    member: ArtifactMemberIdentityDocument
    request: ByteFile
    observation: ByteFile
    graph: ByteFile
    journal: ByteFile
    decision: ByteFile
    primary: CollectionArtifactProvenanceBindingDocument
    completion_requirement_sha256: Sha256 | None

    @model_validator(mode="after")
    def exact_primary(self) -> Self:
        if (
            self.primary.artifact_id != self.member.artifact_id
            or self.primary.journal.prefix_bytes != self.journal.bytes
            or self.primary.journal.prefix_sha256 != self.journal.sha256
        ):
            raise ValueError("prepared-primary binding differs from its member or journal")
        if len({file.name for file in self.byte_files}) != len(self.byte_files):
            raise ValueError("prepared-primary byte files must be distinct")
        return self

    @property
    def byte_files(self) -> tuple[ByteFile, ...]:
        return self.request, self.observation, self.graph, self.journal, self.decision


class PrimaryDecisionAccepted(PrimaryControl):
    kind: Literal["decision-accepted"] = "decision-accepted"
    prepared_sha256: Sha256
    decision: ByteCommitment
    response: ByteCommitment

    @model_validator(mode="after")
    def exact_response(self) -> Self:
        if self.response != self.decision:
            raise ValueError("materialization acknowledgement differs from its exact decision")
        return self


class PrimaryCustody(PrimaryControl):
    kind: Literal["custody"] = "custody"
    collection_id: CollectionId
    prepared_sha256: Sha256
    prepared: PreparedPrimary
    receipt: CollectionUploadArtifactCustodyReceiptDocument

    @model_validator(mode="after")
    def exact_receipt(self) -> Self:
        import hashlib

        if hashlib.sha256(primary_control_bytes(self.prepared)).hexdigest() != self.prepared_sha256:
            raise ValueError("custody differs from its retained preparation")
        validate_collection_upload_artifact_custody_receipt(
            self.collection_id, self.prepared.member, self.receipt
        )
        if (
            self.receipt.primary != self.prepared.primary
            or self.receipt.completion_requirement_sha256
            != self.prepared.completion_requirement_sha256
        ):
            raise ValueError("custody differs from the retained primary construction")
        return self


class PrimaryInventory(BaseModel):
    """Canonical newline rows committed by an ordered, length-framed hash chain."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    format: Literal["riverhog-prepared-inventory-chain/v1"] = "riverhog-prepared-inventory-chain/v1"
    name: Literal["inventory"] = "inventory"
    bytes: NonnegativeDecimal
    chain_sha256: Sha256


class PrimaryInventoryEntry(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    member: ArtifactMemberIdentityDocument
    prepared_sha256: Sha256


class PrimaryInventoryCursor(PrimaryControl):
    session_sha256: Sha256
    inventory: PrimaryInventory
    members: NonnegativeDecimal
    after_member: NonnegativeDecimal
    after_offset: NonnegativeDecimal
    last_member: ArtifactId | None
    manifest_chain_sha256: Sha256

    @model_validator(mode="after")
    def within_inventory(self) -> Self:
        if self.after_member > self.members or self.after_offset > self.inventory.bytes:
            raise ValueError("primary closing cursor exceeds its exact inventory")
        if (self.last_member is None) != (self.after_member == 0):
            raise ValueError("primary closing member cursor has no exact last identity")
        return self


class PrimaryClosing(PrimaryInventoryCursor):
    kind: Literal["closing"] = "closing"


class PrimaryCustodyClosing(PrimaryInventoryCursor):
    kind: Literal["custody-closing"] = "custody-closing"


class PrimaryCustodySettled(PrimaryControl):
    kind: Literal["custody-settled"] = "custody-settled"
    session_sha256: Sha256
    inventory: PrimaryInventory
    members: NonnegativeDecimal
    manifest_chain_sha256: Sha256


class PrimaryFinishReady(PrimaryControl):
    kind: Literal["finish-ready"] = "finish-ready"
    session_sha256: Sha256
    inventory: PrimaryInventory
    members: NonnegativeDecimal
    manifest_chain_sha256: Sha256


def primary_control_bytes(document: BaseModel) -> bytes:
    return canonical_json_bytes(document.model_dump(mode="json"))


class PrimaryUsage(PrimaryControl):
    kind: Literal["usage"] = "usage"
    sequence: NonnegativeDecimal = 0
    pending_bytes: NonnegativeDecimal = 0
    inventory_bytes: NonnegativeDecimal = 0
    inventory_reserved_bytes: NonnegativeDecimal = 0
    inventory_selected: bool = False
    control_bytes: NonnegativeDecimal = 0
    members: NonnegativeDecimal = 0


class PrimaryTransition(PrimaryControl):
    kind: Literal["transition"] = "transition"
    operation: Literal["prepare", "reclaim"]
    member: ArtifactMemberIdentityDocument
    before: PrimaryUsage
    after: PrimaryUsage

    @model_validator(mode="after")
    def advances_once(self) -> Self:
        if self.after.sequence != self.before.sequence + 1:
            raise ValueError("prepared-primary transition must advance once")
        return self


class PrimaryReclaimed(PrimaryControl):
    kind: Literal["reclaimed"] = "reclaimed"
    prepared_sha256: Sha256


class PrimaryClosingIntent(PrimaryControl):
    kind: Literal["closing-intent"] = "closing-intent"
    session_sha256: Sha256
    members: NonnegativeDecimal
    inventory: PrimaryInventory | None = None
    before: PrimaryUsage | None = None
    after: PrimaryUsage | None = None

    @model_validator(mode="after")
    def exact_accounting(self) -> Self:
        if self.inventory is None:
            if self.before is not None or self.after is not None:
                raise ValueError("unselected inventory cannot change admission")
        elif self.before is None or self.after is None:
            raise ValueError("selected inventory requires complete admission accounting")
        elif (
            self.before.members != self.members
            or self.after.members != self.members
            or self.after.sequence != self.before.sequence + 1
            or self.before.inventory_bytes != 0
            or self.before.inventory_selected
            or not self.after.inventory_selected
            or self.after.inventory_bytes != self.inventory.bytes
            or self.after.inventory_reserved_bytes != 0
            or self.after.inventory_bytes > self.before.inventory_reserved_bytes
            or self.before.pending_bytes != self.after.pending_bytes
            or self.before.control_bytes != self.after.control_bytes
        ):
            raise ValueError("inventory admission changed the prepared allocation")
        return self
