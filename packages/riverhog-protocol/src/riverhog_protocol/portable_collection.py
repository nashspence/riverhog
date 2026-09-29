from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Literal

from http_api_contracts import canonical_json_bytes
from pydantic import BaseModel, ConfigDict, Field, model_validator
from riverhog_canonical_json import format_scalar, parse_scalar

from riverhog_protocol.artifact_identity import ArtifactId, ArtifactMemberIdentityDocument
from riverhog_protocol.exact_scalar import NonnegativeDecimal
from riverhog_protocol.paths import CollectionId

PORTABLE_COLLECTION_FORMAT: Literal["riverhog-collection/v1"] = "riverhog-collection/v1"
PORTABLE_COLLECTION_INVENTORY_PAGE_FORMAT: Literal["riverhog-collection-inventory-page/v1"] = (
    "riverhog-collection-inventory-page/v1"
)
_SHA256_RE = re.compile(r"[0-9a-f]{64}")


class PortableCollectionError(ValueError):
    """The document is not the canonical portable Riverhog collection contract."""


def _sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise PortableCollectionError(f"{label} is not a SHA-256 identity")
    return value


def _nonnegative_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise PortableCollectionError(f"{label} must be a non-negative integer")
    return value


@dataclass(frozen=True, slots=True)
class PortableCollectionArtifact:
    artifact_id: ArtifactId
    bytes: int
    sha256: str

    def __post_init__(self) -> None:
        try:
            object.__setattr__(self, "artifact_id", ArtifactId(self.artifact_id))
        except ValueError as exc:
            raise PortableCollectionError("portable collection artifact ID is invalid") from exc
        if _nonnegative_int(self.bytes, "portable collection artifact bytes") >= 1 << 63:
            raise PortableCollectionError("portable collection artifact bytes exceed sequence63")
        _sha256(self.sha256, "portable collection artifact sha256")

    @classmethod
    def from_mapping(cls, value: object) -> PortableCollectionArtifact:
        if not isinstance(value, Mapping) or set(value) != {"artifact_id", "bytes", "sha256"}:
            raise PortableCollectionError("portable collection artifact fields are invalid")
        try:
            raw_artifact_id = value["artifact_id"]
            if not isinstance(raw_artifact_id, str):
                raise ValueError("artifact ID must be text")
            artifact_id = ArtifactId(raw_artifact_id)
        except ValueError as exc:
            raise PortableCollectionError("portable collection artifact ID is invalid") from exc
        try:
            byte_count = parse_scalar("nonnegative", value["bytes"])
        except ValueError as exc:
            raise PortableCollectionError("portable collection artifact bytes are invalid") from exc
        return cls(
            artifact_id=artifact_id,
            bytes=byte_count,
            sha256=_sha256(value["sha256"], "portable collection artifact sha256"),
        )

    def to_mapping(self) -> dict[str, object]:
        return {
            "artifact_id": str(self.artifact_id),
            "bytes": format_scalar("nonnegative", self.bytes),
            "sha256": self.sha256,
        }


class PortableCollectionHeader(BaseModel):
    """Bounded immutable metadata that owns one portable artifact inventory."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    format: Literal["riverhog-collection/v1"] = PORTABLE_COLLECTION_FORMAT
    collection: CollectionId
    artifact_set_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    encryption_format: str = Field(min_length=1)
    passphrase_id: str = Field(pattern=r"^[A-Za-z0-9_-]{16,128}$")
    provenance_identity: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_provenance_binding(self) -> PortableCollectionHeader:
        if self.encryption_format.strip() != self.encryption_format:
            raise ValueError("portable collection encryption format is not canonical")
        return self


class PortableCollectionInventoryAuthority(BaseModel):
    """The immutable authority shared by every bounded inventory page."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    header: PortableCollectionHeader
    inventory_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    artifact_count: NonnegativeDecimal = Field(ge=1)
    artifact_bytes: NonnegativeDecimal


class PortableCollectionInventoryPage(BaseModel):
    """One bounded, canonically ordered slice of an immutable inventory."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    format: Literal["riverhog-collection-inventory-page/v1"] = (
        PORTABLE_COLLECTION_INVENTORY_PAGE_FORMAT
    )
    authority: PortableCollectionInventoryAuthority
    artifacts: list[ArtifactMemberIdentityDocument] = Field(
        max_length=1000,
        json_schema_extra={
            "x-riverhog-extent": {
                "policy": "segmented_no_total_max",
                "reason": "bounded-portable-inventory-page",
                "progression": "authority-bound-cursor",
            }
        },
    )
    next_cursor: str | None = Field(default=None, min_length=1, max_length=8192)
    complete: bool

    @model_validator(mode="after")
    def validate_page(self) -> PortableCollectionInventoryPage:
        artifact_ids = tuple(item.artifact_id for item in self.artifacts)
        if artifact_ids != tuple(sorted(set(artifact_ids))):
            raise ValueError("portable inventory page artifacts are not canonical")
        if self.complete != (self.next_cursor is None):
            raise ValueError("portable inventory page continuation is inconsistent")
        return self


class PortableCollectionIdentityBuilder:
    """Incrementally seal one canonically ordered immutable artifact inventory."""

    def __init__(self, header: PortableCollectionHeader) -> None:
        self.header = header
        self._digest = hashlib.sha256()
        self._digest.update(canonical_json_bytes(header.model_dump(mode="json")))
        self._previous_artifact_id: ArtifactId | None = None
        self.artifacts = 0
        self.bytes = 0

    def add(self, artifact: PortableCollectionArtifact) -> None:
        if (
            self._previous_artifact_id is not None
            and artifact.artifact_id <= self._previous_artifact_id
        ):
            raise PortableCollectionError("portable collection artifacts are not canonical")
        encoded = canonical_json_bytes(artifact.to_mapping())
        self._digest.update(len(encoded).to_bytes(8, "big"))
        self._digest.update(encoded)
        self._previous_artifact_id = artifact.artifact_id
        self.artifacts += 1
        self.bytes += artifact.bytes

    @property
    def identity(self) -> str:
        if self.artifacts < 1:
            raise PortableCollectionError("portable collection artifacts must not be empty")
        return self._digest.hexdigest()


def portable_collection_inventory_identity(
    header: PortableCollectionHeader,
    artifacts: Iterable[PortableCollectionArtifact],
) -> str:
    builder = PortableCollectionIdentityBuilder(header)
    for artifact in artifacts:
        builder.add(artifact)
    return builder.identity


__all__ = [
    "PORTABLE_COLLECTION_FORMAT",
    "PORTABLE_COLLECTION_INVENTORY_PAGE_FORMAT",
    "PortableCollectionError",
    "PortableCollectionArtifact",
    "PortableCollectionHeader",
    "PortableCollectionInventoryAuthority",
    "PortableCollectionIdentityBuilder",
    "PortableCollectionInventoryPage",
    "portable_collection_inventory_identity",
]
