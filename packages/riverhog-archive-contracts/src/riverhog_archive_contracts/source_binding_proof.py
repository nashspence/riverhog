"""Exact source archive/member binding proof carried with inherited histories."""

from __future__ import annotations

import base64
import hashlib
from dataclasses import dataclass
from typing import Any

from riverhog_canonical_json import (
    canonical_json_bytes,
    format_scalar,
    parse_scalar,
    require_canonical_json,
)

from .archive_manifest import CollectionArchiveManifest
from .member_binding_tree import verify_binding_inclusion
from .member_history import MemberHistoryBinding, MemberHistoryImport
from .provenance_archive import ProvenanceRootDocument

SOURCE_BINDING_PROOF_FORMAT = "riverhog-source-member-history-proof/v1"
SOURCE_BINDING_PROOF_BYTES_MAX = 256 * 1024


@dataclass(frozen=True, slots=True)
class SourceMemberHistoryBindingProof:
    source_identity: str
    collection_id: int
    archive_root: bytes
    provenance_root: bytes
    binding: MemberHistoryBinding
    index: int
    siblings: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            not isinstance(self.source_identity, str)
            or len(self.source_identity) != 64
            or any(c not in "0123456789abcdef" for c in self.source_identity)
        ):
            raise ValueError("source identity is invalid")
        if type(self.collection_id) is not int or not 0 < self.collection_id < 2**63:
            raise ValueError("source collection identity is invalid")
        if not isinstance(self.siblings, tuple) or len(self.siblings) > 256:
            raise ValueError("source binding proof exceeds its representation bound")
        if not isinstance(self.archive_root, bytes) or not isinstance(self.provenance_root, bytes):
            raise ValueError("source root preimages must be exact bytes")
        if not isinstance(self.binding, MemberHistoryBinding):
            raise ValueError("source member binding is invalid")
        archive = CollectionArchiveManifest.from_json_bytes(self.archive_root)
        provenance = ProvenanceRootDocument.from_json_bytes(self.provenance_root)
        if (
            archive.provenance.identity != hashlib.sha256(self.provenance_root).hexdigest()
            or archive.provenance.root.plaintext_bytes != len(self.provenance_root)
            or archive.archive_generation != provenance.archive_generation
            or archive.artifact_set_sha256 != provenance.artifact_set_sha256
            or archive.artifacts != provenance.binding_count
        ):
            raise ValueError("source archive and provenance root authority differs")
        verify_binding_inclusion(
            self.binding,
            count=provenance.binding_count,
            index=self.index,
            siblings=self.siblings,
            expected_root_sha256=provenance.binding_tree_sha256,
        )
        if len(self.to_json_bytes()) > SOURCE_BINDING_PROOF_BYTES_MAX:
            raise ValueError("source binding proof exceeds its byte bound")

    @property
    def identity(self) -> str:
        return hashlib.sha256(self.to_json_bytes()).hexdigest()

    def to_mapping(self) -> dict[str, object]:
        return {
            "format": SOURCE_BINDING_PROOF_FORMAT,
            "source_identity": self.source_identity,
            "collection_id": format_scalar("sequence63", self.collection_id),
            "archive_root_base64": base64.b64encode(self.archive_root).decode("ascii"),
            "provenance_root_base64": base64.b64encode(self.provenance_root).decode("ascii"),
            "binding": self.binding.to_mapping(),
            "index": format_scalar("nonnegative", self.index),
            "siblings": list(self.siblings),
        }

    def to_json_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_mapping())

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> SourceMemberHistoryBindingProof:
        if len(raw) > SOURCE_BINDING_PROOF_BYTES_MAX:
            raise ValueError("source binding proof exceeds its byte bound")
        value: Any = require_canonical_json(raw)
        if (
            not isinstance(value, dict)
            or set(value)
            != {
                "format",
                "source_identity",
                "collection_id",
                "archive_root_base64",
                "provenance_root_base64",
                "binding",
                "index",
                "siblings",
            }
            or value["format"] != SOURCE_BINDING_PROOF_FORMAT
        ):
            raise ValueError("source binding proof fields are invalid")
        siblings = value["siblings"]
        if not isinstance(siblings, list) or not all(isinstance(item, str) for item in siblings):
            raise ValueError("source binding proof siblings are invalid")

        def decode(field: str) -> bytes:
            encoded = value[field]
            if not isinstance(encoded, str):
                raise ValueError("source root preimage is invalid")
            decoded = base64.b64decode(encoded, validate=True)
            if base64.b64encode(decoded).decode("ascii") != encoded:
                raise ValueError("source root preimage is not canonical base64")
            return decoded

        return cls(
            source_identity=value["source_identity"],
            collection_id=parse_scalar("sequence63", value["collection_id"]),
            archive_root=decode("archive_root_base64"),
            provenance_root=decode("provenance_root_base64"),
            binding=MemberHistoryBinding.from_mapping(value["binding"]),
            index=parse_scalar("nonnegative", value["index"]),
            siblings=tuple(siblings),
        )

    def verify_import(self, imported: MemberHistoryImport) -> None:
        archive = CollectionArchiveManifest.from_json_bytes(self.archive_root)
        if (
            self.identity != imported.source_binding_proof_sha256
            or self.source_identity != imported.source_identity
            or self.collection_id != imported.source_collection_id
            or hashlib.sha256(self.archive_root).hexdigest() != imported.source_archive_root_sha256
            or archive.artifact_set_sha256 != imported.source_artifact_set_sha256
            or self.binding.artifact_id != imported.source_artifact_id
            or self.binding.history_sha256 != imported.source_history_sha256
        ):
            raise ValueError("inherited history source binding proof differs")


__all__ = [
    "SOURCE_BINDING_PROOF_FORMAT",
    "SOURCE_BINDING_PROOF_BYTES_MAX",
    "SourceMemberHistoryBindingProof",
]
