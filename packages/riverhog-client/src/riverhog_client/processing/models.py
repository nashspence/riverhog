"""Immutable public values for the collection processing data plane."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from riverhog_protocol.artifact_identity import ArtifactId
from riverhog_protocol.collection_workflows import (
    CollectionDerivation,
    CollectionRootIdentity,
    OperationIdentity,
    RecipeIdentity,
)
from riverhog_protocol.output_collection_policy import OutputCollectionPolicy
from riverhog_protocol.paths import (
    CollectionId,
    validate_collection_id,
)


def _sha256(value: str, label: str) -> str:
    digest = value.casefold()
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise ValueError(f"{label} must be a lowercase SHA-256")
    return digest


@dataclass(frozen=True, slots=True)
class DerivedCollectionSpec:
    """Controller-sealed authorities needed to create one derived collection.

    The data-plane runtime deliberately does not depend on an orchestration
    application or workflow-specific intent model. It receives only exact
    immutable input roots and opaque recipe/operation identities.
    """

    inputs: tuple[CollectionRootIdentity, ...]
    recipe: RecipeIdentity
    operation: OperationIdentity
    output_policy: OutputCollectionPolicy = OutputCollectionPolicy()

    def __post_init__(self) -> None:
        normalized_inputs = tuple(sorted(self.inputs))
        if not normalized_inputs or normalized_inputs != self.inputs:
            raise ValueError("derived collection inputs must be nonempty and canonical")
        if len({item.collection_id for item in normalized_inputs}) != len(normalized_inputs):
            raise ValueError("derived collection inputs must be unique")


@dataclass(frozen=True, order=True, slots=True)
class ClaimedArtifact:
    """One immutable opaque member authorized by a processing claim."""

    root: CollectionRootIdentity
    artifact_id: ArtifactId
    bytes: int
    sha256: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "artifact_id", ArtifactId(self.artifact_id))
        if isinstance(self.bytes, bool) or self.bytes < 0:
            raise ValueError("claimed artifact byte count must be non-negative")
        object.__setattr__(self, "sha256", _sha256(self.sha256, "claimed artifact identity"))

    @property
    def key(self) -> tuple[int, str]:
        return self.root.collection_id, self.artifact_id

    def as_dict(self) -> dict[str, object]:
        return {
            "collection": self.root.as_dict(),
            "artifact_id": self.artifact_id,
            "bytes": self.bytes,
            "sha256": self.sha256,
        }


@dataclass(frozen=True, slots=True)
class DerivedCollectionReceipt:
    """Finalized output identity returned by a transform target."""

    collection_id: CollectionId
    archive_root_sha256: str
    artifact_set_identity: str
    derivation: CollectionDerivation

    def __post_init__(self) -> None:
        object.__setattr__(self, "collection_id", validate_collection_id(self.collection_id))
        object.__setattr__(
            self,
            "archive_root_sha256",
            _sha256(self.archive_root_sha256, "derived collection archive-root identity"),
        )
        object.__setattr__(
            self,
            "artifact_set_identity",
            _sha256(self.artifact_set_identity, "derived collection content identity"),
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "collection_id": self.collection_id,
            "archive_root_sha256": self.archive_root_sha256,
            "artifact_set_identity": self.artifact_set_identity,
            "derivation": self.derivation.as_dict(),
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> DerivedCollectionReceipt:
        if set(value) != {
            "collection_id",
            "archive_root_sha256",
            "artifact_set_identity",
            "derivation",
        }:
            raise ValueError("derived collection receipt fields are invalid")
        derivation = value.get("derivation")
        if not isinstance(derivation, Mapping):
            raise ValueError("derived collection receipt has no derivation")
        collection_id = value.get("collection_id")
        if isinstance(collection_id, bool) or not isinstance(collection_id, int):
            raise ValueError("derived collection receipt id is invalid")
        return cls(
            collection_id=collection_id,
            archive_root_sha256=str(value.get("archive_root_sha256") or ""),
            artifact_set_identity=str(value.get("artifact_set_identity") or ""),
            derivation=CollectionDerivation.from_mapping(derivation),
        )


__all__ = ["ClaimedArtifact", "DerivedCollectionReceipt", "DerivedCollectionSpec"]
