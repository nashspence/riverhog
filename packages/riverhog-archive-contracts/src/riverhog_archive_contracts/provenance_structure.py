"""Deterministic identities for bounded, encrypted provenance structure objects."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Literal

from riverhog_canonical_json import require_canonical_json

from .archive_manifest import format_archive_sequence
from .member_history import (
    MEMBER_HISTORY_FORMAT,
    MemberHistoryDocument,
    history_record_page_object_path,
    member_history_object_path,
    source_binding_proof_object_path,
)
from .source_binding_proof import SOURCE_BINDING_PROOF_FORMAT, SourceMemberHistoryBindingProof
from .structural_record_set import PAGE_BYTES_MAX, RECORD_PAGE_FORMAT, RecordPage


@dataclass(frozen=True, slots=True)
class ProvenanceStructureIdentity:
    kind: Literal["history", "record-page", "source-proof"]
    object_id: str
    relative_path: str
    bytes: int
    sha256: str


def provenance_structure_identity(content: bytes) -> ProvenanceStructureIdentity:
    """Validate exact structure before assigning its deterministic archive location."""

    if not isinstance(content, bytes) or len(content) > PAGE_BYTES_MAX:
        raise ValueError("provenance structure exceeds its bounded object contract")
    value = require_canonical_json(content)
    if not isinstance(value, dict):
        raise ValueError("provenance structure is not an object")
    digest = hashlib.sha256(content).hexdigest()
    if value.get("format") == MEMBER_HISTORY_FORMAT:
        history = MemberHistoryDocument.from_json_bytes(content)
        return ProvenanceStructureIdentity(
            "history",
            "provenance-history-" + history.identity,
            member_history_object_path(history.identity),
            len(content),
            digest,
        )
    if value.get("format") == RECORD_PAGE_FORMAT:
        page = RecordPage.from_json_bytes(content)
        return ProvenanceStructureIdentity(
            "record-page",
            "provenance-record-page-"
            + page.authority.records_sha256
            + "-"
            + format_archive_sequence(page.ordinal),
            history_record_page_object_path(page.authority.records_sha256, page.ordinal),
            len(content),
            digest,
        )
    if value.get("format") == SOURCE_BINDING_PROOF_FORMAT:
        proof = SourceMemberHistoryBindingProof.from_json_bytes(content)
        return ProvenanceStructureIdentity(
            "source-proof",
            "provenance-source-proof-" + proof.identity,
            source_binding_proof_object_path(proof.identity),
            len(content),
            digest,
        )
    raise ValueError("unsupported provenance structure format")


__all__ = ["ProvenanceStructureIdentity", "provenance_structure_identity"]


def provenance_structure_object_path(object_id: str) -> str:
    if match := re.fullmatch(r"provenance-history-([0-9a-f]{64})", object_id):
        return member_history_object_path(match[1])
    if match := re.fullmatch(r"provenance-source-proof-([0-9a-f]{64})", object_id):
        return source_binding_proof_object_path(match[1])
    if match := re.fullmatch(r"provenance-record-page-([0-9a-f]{64})-([0-9a-f]{64})", object_id):
        return history_record_page_object_path(match[1], int(match[2], 16))
    raise ValueError("invalid provenance structure object identity")


def provenance_structure_object_id(path: str) -> str:
    parts = path.split("/")
    if len(parts) == 4 and parts[0] == "provenance" and parts[1] in ("history", "source-proofs"):
        identity = parts[3].removesuffix(".json.age")
        prefix = "provenance-history-" if parts[1] == "history" else "provenance-source-proof-"
        object_id = prefix + identity
    elif len(parts) == 5 and parts[:2] == ["provenance", "sets"]:
        object_id = "provenance-record-page-" + parts[3] + "-" + parts[4].removesuffix(".json.age")
    else:
        raise ValueError("invalid provenance structure path")
    if provenance_structure_object_path(object_id) != path:
        raise ValueError("noncanonical provenance structure path")
    return object_id


__all__ += ["provenance_structure_object_path", "provenance_structure_object_id"]
