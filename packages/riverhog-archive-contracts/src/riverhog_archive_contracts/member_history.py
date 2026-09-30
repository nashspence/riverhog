"""Authenticated structural entrypoint to one member's selected canonical history."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Literal

from riverhog_canonical_json import (
    canonical_json_bytes,
    format_scalar,
    parse_scalar,
    require_canonical_json,
)

from .structural_record_set import RecordSetRef

MEMBER_HISTORY_FORMAT = "riverhog-member-history/v1"
MEMBER_HISTORY_ROOTS_SCHEMA = "riverhog-member-history-roots/v1"
MEMBER_HISTORY_IMPORTS_SCHEMA = "riverhog-member-history-imports/v1"
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
        return hashlib.sha256(canonical_json_bytes(self.journal.to_mapping())).hexdigest()

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


__all__ = [
    "MEMBER_HISTORY_BYTES_MAX",
    "MEMBER_HISTORY_FORMAT",
    "MEMBER_HISTORY_IMPORTS_SCHEMA",
    "MEMBER_HISTORY_ROOTS_SCHEMA",
    "HistoryJournalAnchor",
    "MemberHistoryBinding",
    "MemberHistoryDocument",
    "MemberHistoryPrimary",
    "MemberHistoryRoot",
]
