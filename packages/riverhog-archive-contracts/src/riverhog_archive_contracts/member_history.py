"""Authenticated structural entrypoint to one member's selected canonical history."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal

from riverhog_canonical_json import (
    canonical_json_bytes,
    format_scalar,
    parse_scalar,
    require_canonical_json,
)

from .archive_manifest import format_archive_sequence
from .structural_record_set import RecordPage, RecordSetCommitment, RecordSetRef

MEMBER_HISTORY_FORMAT = "riverhog-member-history/v1"
MEMBER_HISTORY_ROOTS_SCHEMA = "riverhog-member-history-roots/v1"
MEMBER_HISTORY_IMPORTS_SCHEMA = "riverhog-member-history-imports/v1"
BOUND_HISTORY_EXTENT = "bound-and-required-history"
RETAINED_HISTORY_EXTENT = "complete-retained-history"
MEMBER_HISTORY_BYTES_MAX = 64 * 1024
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_UUID = re.compile(r"urn:uuid:[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\Z")


def _sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA.fullmatch(value) is None:
        raise ValueError(f"{label} must be lowercase SHA-256")
    return value


def _uuid(value: object, label: str) -> str:
    if not isinstance(value, str) or _UUID.fullmatch(value) is None:
        raise ValueError(f"{label} must be a canonical UUID URN")
    return value


def member_history_object_path(history_sha256: str) -> str:
    identity = _sha256(history_sha256, "member history")
    return f"provenance/history/{identity[:2]}/{identity}.json.age"


def history_record_page_object_path(records_sha256: str, ordinal: int) -> str:
    identity = _sha256(records_sha256, "history record set")
    return f"provenance/sets/{identity[:2]}/{identity}/{format_archive_sequence(ordinal)}.json.age"


def source_binding_proof_object_path(proof_sha256: str) -> str:
    identity = _sha256(proof_sha256, "source binding proof")
    return f"provenance/source-proofs/{identity[:2]}/{identity}.json.age"


@dataclass(frozen=True, slots=True)
class HistoryJournalAnchor:
    journal_id: str
    through_entry_id: str
    through_sequence: int
    through_json_sha256: str
    prefix_sha256: str
    prefix_bytes: int

    def __post_init__(self) -> None:
        _uuid(self.journal_id, "history journal")
        _uuid(self.through_entry_id, "history through entry")
        if type(self.through_sequence) is not int or self.through_sequence < 0:
            raise ValueError("history through sequence is invalid")
        _sha256(self.through_json_sha256, "history through entry")
        _sha256(self.prefix_sha256, "history prefix")
        if type(self.prefix_bytes) is not int or self.prefix_bytes < 1:
            raise ValueError("history prefix byte count is invalid")

    def to_mapping(self) -> dict[str, object]:
        return {
            "journal_id": self.journal_id,
            "through": {
                "entry_id": self.through_entry_id,
                "sequence": format_scalar("nonnegative", self.through_sequence),
                "json_sha256": self.through_json_sha256,
            },
            "prefix_sha256": self.prefix_sha256,
            "prefix_bytes": format_scalar("nonnegative", self.prefix_bytes),
        }

    @classmethod
    def from_mapping(cls, value: object) -> HistoryJournalAnchor:
        if not isinstance(value, dict) or set(value) != {
            "journal_id",
            "through",
            "prefix_sha256",
            "prefix_bytes",
        }:
            raise ValueError("history journal anchor fields are invalid")
        through = value["through"]
        if not isinstance(through, dict) or set(through) != {"entry_id", "sequence", "json_sha256"}:
            raise ValueError("history through entry fields are invalid")
        return cls(
            journal_id=value["journal_id"],
            through_entry_id=through["entry_id"],
            through_sequence=parse_scalar("nonnegative", through["sequence"]),
            through_json_sha256=through["json_sha256"],
            prefix_sha256=value["prefix_sha256"],
            prefix_bytes=parse_scalar("nonnegative", value["prefix_bytes"]),
        )


@dataclass(frozen=True, slots=True)
class MemberHistoryPrimary:
    journal: HistoryJournalAnchor
    delivery_association_id: str

    def __post_init__(self) -> None:
        _uuid(self.delivery_association_id, "primary delivery association")

    def to_mapping(self) -> dict[str, object]:
        return {
            "journal": self.journal.to_mapping(),
            "delivery_association_id": self.delivery_association_id,
        }

    @classmethod
    def from_mapping(cls, value: object) -> MemberHistoryPrimary:
        if not isinstance(value, dict) or set(value) != {"journal", "delivery_association_id"}:
            raise ValueError("member history primary fields are invalid")
        return cls(
            journal=HistoryJournalAnchor.from_mapping(value["journal"]),
            delivery_association_id=value["delivery_association_id"],
        )


@dataclass(frozen=True, slots=True)
class MemberHistoryDocument:
    artifact_id: str
    bytes: int
    sha256: str
    primary: MemberHistoryPrimary
    roots: RecordSetRef
    imports: RecordSetRef

    def __post_init__(self) -> None:
        _sha256(self.artifact_id, "member artifact ID")
        _sha256(self.sha256, "member content")
        if type(self.bytes) is not int or not 0 <= self.bytes < 2**63:
            raise ValueError("member history byte extent is invalid")
        if self.roots.schema_id != MEMBER_HISTORY_ROOTS_SCHEMA or self.roots.record_count < 1:
            raise ValueError("member history requires an exact root selection")
        if self.imports.schema_id != MEMBER_HISTORY_IMPORTS_SCHEMA:
            raise ValueError("member history import set schema is invalid")
        if len(self.to_json_bytes()) > MEMBER_HISTORY_BYTES_MAX:
            raise ValueError("member history descriptor exceeds its byte bound")

    def to_mapping(self) -> dict[str, object]:
        return {
            "format": MEMBER_HISTORY_FORMAT,
            "artifact_id": self.artifact_id,
            "bytes": format_scalar("nonnegative", self.bytes),
            "sha256": self.sha256,
            "primary": self.primary.to_mapping(),
            "roots": self.roots.to_mapping(),
            "imports": self.imports.to_mapping(),
        }

    def to_json_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_mapping())

    @property
    def identity(self) -> str:
        return hashlib.sha256(self.to_json_bytes()).hexdigest()

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> MemberHistoryDocument:
        if len(raw) > MEMBER_HISTORY_BYTES_MAX:
            raise ValueError("member history descriptor exceeds its byte bound")
        value = require_canonical_json(raw)
        if (
            not isinstance(value, dict)
            or set(value)
            != {"format", "artifact_id", "bytes", "sha256", "primary", "roots", "imports"}
            or value["format"] != MEMBER_HISTORY_FORMAT
        ):
            raise ValueError("member history descriptor fields are invalid")
        return cls(
            artifact_id=value["artifact_id"],
            bytes=parse_scalar("nonnegative", value["bytes"]),
            sha256=value["sha256"],
            primary=MemberHistoryPrimary.from_mapping(value["primary"]),
            roots=RecordSetRef.from_mapping(value["roots"]),
            imports=RecordSetRef.from_mapping(value["imports"]),
        )


@dataclass(frozen=True, slots=True)
class MemberHistoryBinding:
    """Final root-bound member lookup, separate from the early primary upload binding."""

    artifact_id: str
    bytes: int
    sha256: str
    history_sha256: str
    history_bytes: int

    def __post_init__(self) -> None:
        _sha256(self.artifact_id, "history binding artifact ID")
        _sha256(self.sha256, "history binding content")
        _sha256(self.history_sha256, "history descriptor")
        if type(self.bytes) is not int or not 0 <= self.bytes < 2**63:
            raise ValueError("history binding member byte extent is invalid")
        if (
            type(self.history_bytes) is not int
            or not 1 <= self.history_bytes <= MEMBER_HISTORY_BYTES_MAX
        ):
            raise ValueError("history descriptor byte extent is invalid")

    def to_mapping(self) -> dict[str, object]:
        return {
            "artifact_id": self.artifact_id,
            "bytes": format_scalar("nonnegative", self.bytes),
            "sha256": self.sha256,
            "history_sha256": self.history_sha256,
            "history_bytes": format_scalar("nonnegative", self.history_bytes),
        }

    @classmethod
    def from_mapping(cls, value: object) -> MemberHistoryBinding:
        if not isinstance(value, dict) or set(value) != {
            "artifact_id",
            "bytes",
            "sha256",
            "history_sha256",
            "history_bytes",
        }:
            raise ValueError("history binding fields are invalid")
        return cls(
            artifact_id=value["artifact_id"],
            bytes=parse_scalar("nonnegative", value["bytes"]),
            sha256=value["sha256"],
            history_sha256=value["history_sha256"],
            history_bytes=parse_scalar("nonnegative", value["history_bytes"]),
        )

    def verify_descriptor(self, raw: bytes) -> MemberHistoryDocument:
        if len(raw) != self.history_bytes or hashlib.sha256(raw).hexdigest() != self.history_sha256:
            raise ValueError("member history descriptor differs from its root binding")
        descriptor = MemberHistoryDocument.from_json_bytes(raw)
        if (descriptor.artifact_id, descriptor.bytes, descriptor.sha256) != (
            self.artifact_id,
            self.bytes,
            self.sha256,
        ):
            raise ValueError("member history descriptor names another member")
        return descriptor


@dataclass(frozen=True, slots=True)
class MemberHistoryRoot:
    journal: HistoryJournalAnchor
    inclusion: Literal["bound", "retained"]

    def __post_init__(self) -> None:
        if self.inclusion not in ("bound", "retained"):
            raise ValueError("member history root inclusion is invalid")

    @property
    def key(self) -> str:
        # Group every selected head for one journal in the ordered record set.
        # This permits constant-space duplicate and conflicting-head checks.
        return f"{self.journal.journal_id}/{self.journal.prefix_sha256}"

    def to_mapping(self) -> dict[str, object]:
        return {"journal": self.journal.to_mapping(), "inclusion": self.inclusion}

    @classmethod
    def from_mapping(cls, value: object) -> MemberHistoryRoot:
        if not isinstance(value, dict) or set(value) != {"journal", "inclusion"}:
            raise ValueError("member history root fields are invalid")
        return cls(
            journal=HistoryJournalAnchor.from_mapping(value["journal"]),
            inclusion=value["inclusion"],
        )


@dataclass(frozen=True, slots=True)
class MemberHistoryImport:
    """Source-qualified transfer of an exact prior member-history selection."""

    source_collection_id: int
    source_archive_root_sha256: str
    source_artifact_set_sha256: str
    source_artifact_id: str
    source_history_sha256: str
    source_binding_proof_sha256: str
    extent: Literal["bound-and-required-history", "complete-retained-history"]
    input_state: dict[str, object]

    def __post_init__(self) -> None:
        if type(self.source_collection_id) is not int or not 0 < self.source_collection_id < 2**63:
            raise ValueError("source collection identity is invalid")
        for label, value in (
            ("source archive root", self.source_archive_root_sha256),
            ("source artifact set", self.source_artifact_set_sha256),
            ("source artifact ID", self.source_artifact_id),
            ("source history", self.source_history_sha256),
            ("source binding proof", self.source_binding_proof_sha256),
        ):
            _sha256(value, label)
        if self.extent not in (BOUND_HISTORY_EXTENT, RETAINED_HISTORY_EXTENT):
            raise ValueError("inherited history extent is invalid")
        reference = self.input_state
        if (
            not isinstance(reference, dict)
            or set(reference)
            != {"scope", "journal_id", "entry", "assertion_id", "object_id", "object_type"}
            or reference["scope"] != "external"
            or reference["object_type"] != "state"
        ):
            raise ValueError("inherited history needs an exact external State reference")
        _uuid(reference["journal_id"], "inherited State journal")
        _uuid(reference["assertion_id"], "inherited State assertion")
        _uuid(reference["object_id"], "inherited State")
        entry = reference["entry"]
        if not isinstance(entry, dict) or set(entry) != {"entry_id", "sequence", "json_sha256"}:
            raise ValueError("inherited State entry reference is invalid")
        _uuid(entry["entry_id"], "inherited State entry")
        parse_scalar("nonnegative", entry["sequence"])
        _sha256(entry["json_sha256"], "inherited State entry")

    @property
    def key(self) -> str:
        # The same source member/history cannot be silently selected twice at
        # different extents or with conflicting State/proof evidence.
        return (
            f"{self.source_archive_root_sha256}/"
            f"{self.source_artifact_id}/{self.source_history_sha256}"
        )

    def to_mapping(self) -> dict[str, object]:
        return {
            "source": {
                "collection_id": format_scalar("sequence63", self.source_collection_id),
                "archive_root_sha256": self.source_archive_root_sha256,
                "artifact_set_identity": self.source_artifact_set_sha256,
                "artifact_id": self.source_artifact_id,
            },
            "history_sha256": self.source_history_sha256,
            "source_binding_proof_sha256": self.source_binding_proof_sha256,
            "extent": self.extent,
            "input_state": self.input_state,
        }

    @classmethod
    def from_mapping(cls, value: object) -> MemberHistoryImport:
        if not isinstance(value, dict) or set(value) != {
            "source",
            "history_sha256",
            "source_binding_proof_sha256",
            "extent",
            "input_state",
        }:
            raise ValueError("member history import fields are invalid")
        source = value["source"]
        if not isinstance(source, dict) or set(source) != {
            "collection_id",
            "archive_root_sha256",
            "artifact_set_identity",
            "artifact_id",
        }:
            raise ValueError("member history import source fields are invalid")
        return cls(
            source_collection_id=parse_scalar("sequence63", source["collection_id"]),
            source_archive_root_sha256=source["archive_root_sha256"],
            source_artifact_set_sha256=source["artifact_set_identity"],
            source_artifact_id=source["artifact_id"],
            source_history_sha256=value["history_sha256"],
            source_binding_proof_sha256=value["source_binding_proof_sha256"],
            extent=value["extent"],
            input_state=value["input_state"],
        )


def _verify_selected_pages(
    authority: RecordSetRef, pages: Iterable[RecordPage], *,
    primary: HistoryJournalAnchor | None = None,
) -> None:
    commitment = RecordSetCommitment(authority.schema_id)
    terminal_seen = False
    previous_journal_id: str | None = None
    bound_head_seen = False
    primary_seen = False
    for ordinal, page in enumerate(pages):
        if terminal_seen or page.ordinal != ordinal or page.authority != authority:
            raise ValueError("member history page sequence or authority changed")
        for row in page.records:
            if set(row) != {"key", "value"} or not isinstance(row["value"], dict):
                raise ValueError("member history record fields are invalid")
            if authority.schema_id == MEMBER_HISTORY_ROOTS_SCHEMA:
                root = MemberHistoryRoot.from_mapping(row["value"])
                if row["key"] != root.key:
                    raise ValueError("member history root record key differs")
                if root.journal.journal_id != previous_journal_id:
                    previous_journal_id = root.journal.journal_id
                    bound_head_seen = False
                if root.inclusion == "bound":
                    if bound_head_seen:
                        raise ValueError("member history has conflicting bound heads")
                    bound_head_seen = True
                if root.journal == primary:
                    if root.inclusion != "bound":
                        raise ValueError("primary history root must be bound")
                    primary_seen = True
            elif authority.schema_id == MEMBER_HISTORY_IMPORTS_SCHEMA:
                inherited = MemberHistoryImport.from_mapping(row["value"])
                if row["key"] != inherited.key:
                    raise ValueError("member history import record key differs")
            else:
                raise ValueError("member history record set schema is invalid")
            commitment.update(row["key"], row["value"])
        terminal_seen = page.terminal
    if not terminal_seen or commitment.ref() != authority:
        raise ValueError("member history record set is incomplete or changed")
    if primary is not None and not primary_seen:
        raise ValueError("member history omits its exact primary root")


def verify_member_history_sets(
    descriptor: MemberHistoryDocument,
    *,
    root_pages: Iterable[RecordPage],
    import_pages: Iterable[RecordPage],
) -> None:
    """Verify exact bounded selections before their assertions are exposed.

    Callers may stream pages from durable storage, then re-read only the selected
    records they need. Neither set needs to be materialized as a collection.
    """
    _verify_selected_pages(
        descriptor.roots, root_pages, primary=descriptor.primary.journal
    )
    _verify_selected_pages(descriptor.imports, import_pages)


__all__ = [
    "BOUND_HISTORY_EXTENT",
    "MEMBER_HISTORY_BYTES_MAX",
    "MEMBER_HISTORY_FORMAT",
    "MEMBER_HISTORY_IMPORTS_SCHEMA",
    "MEMBER_HISTORY_ROOTS_SCHEMA",
    "RETAINED_HISTORY_EXTENT",
    "HistoryJournalAnchor",
    "MemberHistoryBinding",
    "MemberHistoryDocument",
    "MemberHistoryImport",
    "MemberHistoryPrimary",
    "MemberHistoryRoot",
    "history_record_page_object_path",
    "member_history_object_path",
    "source_binding_proof_object_path",
    "verify_member_history_sets",
]
