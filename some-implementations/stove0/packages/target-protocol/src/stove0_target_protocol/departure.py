"""Artifact-free, idempotent catalog-departure effect contract."""

from __future__ import annotations

from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator
from riverhog_protocol import CatalogSyncDescriptor, CatalogSyncIdentity, CatalogSyncRevision
from stove0_protocol import OciImageId, SemanticId
from stove0_protocol.jcs import canonical_json_bytes, canonical_json_sha256

from stove0_target_protocol.protocol import Sha256

MAX_DEPARTURE_RESULT_BYTES = 64 * 1024


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class DepartureEffectTargetDescriptorPayload(_Model):
    format: Literal["stove0-departure-effect-target/v1"] = "stove0-departure-effect-target/v1"
    implementation_id: SemanticId
    implementation_version: str = Field(min_length=1, max_length=120)
    source_revision: str = Field(min_length=1, max_length=200)
    image_id: OciImageId
    scope_identity: Sha256


class DepartureEffectTargetDescriptor(DepartureEffectTargetDescriptorPayload):
    target_identity: Sha256

    @classmethod
    def seal(
        cls, payload: DepartureEffectTargetDescriptorPayload
    ) -> DepartureEffectTargetDescriptor:
        document = payload.model_dump(mode="json")
        return cls.model_validate({**document, "target_identity": canonical_json_sha256(document)})

    @model_validator(mode="after")
    def exact_identity(self) -> Self:
        payload = self.model_dump(mode="json", exclude={"target_identity"})
        if self.target_identity != canonical_json_sha256(payload):
            raise ValueError("departure target identity differs from its descriptor")
        return self


class DepartureEffectIntentPayload(_Model):
    format: Literal["stove0-departure-effect-intent/v1"] = "stove0-departure-effect-intent/v1"
    policy_id: str = Field(pattern=r"^[a-z0-9](?:[a-z0-9._-]{0,158}[a-z0-9])?$")
    policy_revision: int = Field(ge=1)
    policy_sha256: Sha256
    target_registration_id: str = Field(min_length=1, max_length=160)
    target_identity: Sha256
    source_identity: CatalogSyncIdentity
    authorization_view_identity: CatalogSyncIdentity
    last_collection: CatalogSyncDescriptor
    departure_cause: Literal["collection_deleted", "visibility_lost"]
    departure_revision: CatalogSyncRevision

    @model_validator(mode="after")
    def later_than_last_seen(self) -> Self:
        if int(self.departure_revision) <= int(self.last_collection.revision):
            raise ValueError("departure must follow the last visible descriptor")
        return self


class DepartureEffectIntent(DepartureEffectIntentPayload):
    departure_id: Sha256

    @classmethod
    def seal(cls, payload: DepartureEffectIntentPayload) -> DepartureEffectIntent:
        document = payload.model_dump(mode="json")
        return cls.model_validate({**document, "departure_id": canonical_json_sha256(document)})

    @model_validator(mode="after")
    def exact_identity(self) -> Self:
        payload = self.model_dump(mode="json", exclude={"departure_id"})
        if self.departure_id != canonical_json_sha256(payload):
            raise ValueError("departure effect identity differs from its sealed payload")
        return self


class DepartureEffectReceiptPayload(_Model):
    format: Literal["stove0-departure-effect-receipt/v1"] = "stove0-departure-effect-receipt/v1"
    departure_id: Sha256
    target_identity: Sha256
    result: dict[str, JsonValue] = Field(
        json_schema_extra={
            "x-riverhog-extent": {
                "policy": "contract_max",
                "reason": "bounded-departure-effect-receipt",
            },
            "x-riverhog-encoded-bytes-max": MAX_DEPARTURE_RESULT_BYTES,
        }
    )

    @model_validator(mode="after")
    def bounded_result(self) -> Self:
        if len(canonical_json_bytes(self.result)) > MAX_DEPARTURE_RESULT_BYTES:
            raise ValueError("departure effect receipt result exceeds its bound")
        return self


class DepartureEffectReceipt(DepartureEffectReceiptPayload):
    receipt_sha256: Sha256

    @classmethod
    def seal(cls, payload: DepartureEffectReceiptPayload) -> DepartureEffectReceipt:
        document = payload.model_dump(mode="json")
        return cls.model_validate({**document, "receipt_sha256": canonical_json_sha256(document)})

    @model_validator(mode="after")
    def exact_identity(self) -> Self:
        payload = self.model_dump(mode="json", exclude={"receipt_sha256"})
        if self.receipt_sha256 != canonical_json_sha256(payload):
            raise ValueError("departure effect receipt differs from its canonical payload")
        return self


__all__ = [
    "DepartureEffectTargetDescriptor",
    "DepartureEffectTargetDescriptorPayload",
    "DepartureEffectIntent",
    "DepartureEffectIntentPayload",
    "DepartureEffectReceipt",
    "DepartureEffectReceiptPayload",
    "MAX_DEPARTURE_RESULT_BYTES",
]
