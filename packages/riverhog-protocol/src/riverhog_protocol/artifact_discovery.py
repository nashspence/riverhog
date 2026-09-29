"""Closed, pathless canonical provenance discovery request contract."""

from __future__ import annotations

import base64
import binascii
import hashlib
import re
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from riverhog_canonical_json import canonical_json_bytes
from riverhog_provenance_contracts.codec import require_portable_json

from riverhog_protocol.artifact_identity import ArtifactId
from riverhog_protocol.collection_tags import validate_collection_tag
from riverhog_protocol.paths import validate_collection_id

type ProvenanceScope = Literal["member", "input-history", "collection", "recorded-history"]
type AssertionStateSelector = Literal["effective", "retracted", "any"]
type ValueOperator = Literal["equals", "contains", "bytes-equals"]
type TextMode = Literal["exact", "ascii-fold"]

_SHA256 = r"^[0-9a-f]{64}$"
_POINTER_ESCAPE = re.compile(r"~(?![01])")


class _DiscoveryDocument(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    def identity(self) -> str:
        return hashlib.sha256(
            canonical_json_bytes(self.model_dump(mode="json", exclude_none=True))
        ).hexdigest()


class ProfilePin(_DiscoveryDocument):
    """One exact contract, pack digest and schema section."""

    contract_id: str = Field(min_length=1, max_length=1024)
    contract_sha256: str = Field(pattern=_SHA256)
    schema_id: str = Field(min_length=1, max_length=1024)


class ValuePredicate(_DiscoveryDocument):
    """One typed, literal test against a scalar or byte representation."""

    pointer: str | None = Field(default=None, max_length=4096)
    operator: ValueOperator = "contains"
    value: str | int | bool
    text_mode: TextMode = "exact"

    @model_validator(mode="after")
    def validate_predicate(self) -> Self:
        if self.pointer is not None:
            if self.pointer and not self.pointer.startswith("/"):
                raise ValueError("predicate pointer must be an exact JSON Pointer")
            if any(_POINTER_ESCAPE.search(part) for part in self.pointer.split("/")[1:]):
                raise ValueError("predicate pointer has an invalid escape")
        if self.operator != "equals" and type(self.value) is not str:
            raise ValueError("contains and bytes-equals require a string query")
        if type(self.value) is str:
            if not self.value or len(self.value) > 256 or len(self.value.encode("utf-8")) > 1024:
                raise ValueError("query literal exceeds its bounded domain")
        if self.operator == "bytes-equals":
            assert isinstance(self.value, str)
            try:
                raw = base64.b64decode(self.value, validate=True)
            except (binascii.Error, ValueError) as exc:
                raise ValueError("byte query must use canonical base64") from exc
            if base64.b64encode(raw).decode("ascii") != self.value or self.text_mode != "exact":
                raise ValueError("byte query must use canonical base64 and exact mode")
        if type(self.value) is not str and self.text_mode != "exact":
            raise ValueError("ascii-fold applies only to text")
        require_portable_json(self.value)
        return self


class AssertionClause(_DiscoveryDocument):
    """All value tests apply to one assertion and one pinned profile section."""

    scopes: tuple[ProvenanceScope, ...] = ("member", "input-history", "collection")
    assertion_state: AssertionStateSelector = "effective"
    kind: str | None = Field(default=None, min_length=1, max_length=160)
    profile: ProfilePin | None = None
    values: tuple[ValuePredicate, ...] = Field(min_length=1, max_length=16)

    @field_validator("scopes", "values", mode="before")
    @classmethod
    def array_to_tuple(cls, value: Any) -> Any:
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def validate_scopes(self) -> Self:
        if not self.scopes or len(self.scopes) != len(set(self.scopes)):
            raise ValueError("provenance scopes must be nonempty and unique")
        return self


class ArtifactDiscoveryRequest(_DiscoveryDocument):
    """Closed native member query; all outer filters are conjunctive."""

    format: Literal["riverhog-artifact-discovery/v1"] = "riverhog-artifact-discovery/v1"
    collections: tuple[str, ...] = Field(default=(), max_length=256)
    tags_all: tuple[str, ...] = Field(default=(), max_length=256)
    tags_any: tuple[str, ...] = Field(default=(), max_length=256)
    tags_none: tuple[str, ...] = Field(default=(), max_length=256)
    description_contains: str | None = Field(default=None, min_length=1, max_length=256)
    artifact_id: ArtifactId | None = None
    payload_sha256: str | None = Field(default=None, pattern=_SHA256)
    provenance_all: tuple[AssertionClause, ...] = Field(default=(), max_length=16)
    page_size: int = Field(default=50, ge=1, le=200)

    @field_validator(
        "collections", "tags_all", "tags_any", "tags_none", "provenance_all", mode="before"
    )
    @classmethod
    def array_to_tuple(cls, value: Any) -> Any:
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def validate_selectors(self) -> Self:
        for collection in self.collections:
            if not collection.isascii() or not collection.isdigit():
                raise ValueError("collection selector must be a canonical ID")
            if str(validate_collection_id(int(collection))) != collection:
                raise ValueError("collection selector must be a canonical ID")
        for values in (self.collections, self.tags_all, self.tags_any, self.tags_none):
            if len(values) != len(set(values)) or any(not item for item in values):
                raise ValueError("selectors must be unique and nonempty")
        for tags in (self.tags_all, self.tags_any, self.tags_none):
            for tag in tags:
                validate_collection_tag(tag)
        self.identity()
        return self


__all__ = [
    "ArtifactDiscoveryRequest",
    "AssertionClause",
    "AssertionStateSelector",
    "ProfilePin",
    "ProvenanceScope",
    "TextMode",
    "ValueOperator",
    "ValuePredicate",
]
