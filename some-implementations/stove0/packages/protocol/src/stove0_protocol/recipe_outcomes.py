"""Exact successful no-output definitions and per-member loss obligations."""

from pydantic import Field, JsonValue

from stove0_protocol.jcs import canonical_json_sha256
from stove0_protocol.models import SemanticId, Stove0ProtocolModel
from stove0_protocol.predicates import Pointer, ViewName


class LossVerdict(Stove0ProtocolModel):
    path: Pointer
    equals: JsonValue


class SourceLossEvidence(Stove0ProtocolModel):
    view: ViewName
    verdict: LossVerdict


class SourceLossRule(Stove0ProtocolModel):
    id: SemanticId
    evidence: tuple[SourceLossEvidence, ...] = Field(min_length=1)

    @property
    def sha256(self) -> str:
        return canonical_json_sha256(self.model_dump(mode="json", by_alias=True))


class NoOutputDefinition(Stove0ProtocolModel):
    code: SemanticId
    message: str = Field(min_length=1, max_length=1000)
    source_loss: SourceLossRule | None = None
