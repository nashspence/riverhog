"""Bounded structural custody for canonical journals and member bindings.

These documents address encrypted archive objects. They do not define provenance
assertions, member names, or a second semantic history representation.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Literal, Protocol, cast

from riverhog_canonical_json import (
    canonical_json_bytes,
    format_scalar,
    parse_scalar,
    require_canonical_json,
)

from .archive_manifest import format_archive_sequence, parse_archive_sequence

PROVENANCE_VOLUME_FORMAT = "riverhog-archive-provenance-volume/v1"
PROVENANCE_TERMINAL_FORMAT = "riverhog-archive-provenance-terminal/v1"
PROVENANCE_ROOT_FORMAT = "riverhog-archive-provenance-root/v1"
PROVENANCE_BINDINGS_FORMAT = "riverhog-archive-member-history-bindings/v1"
PROVENANCE_BINDING_PAGE_MEMBERS_MAX = 512
PROVENANCE_BINDING_PAGE_BYTES_MAX = 4 * 1024 * 1024
PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX = 8 * 1024 * 1024
PROVENANCE_METADATA_BYTES_MAX = 64 * 1024

_HEX = re.compile(r"[0-9a-f]{64}\Z")
_UUID = re.compile(r"urn:uuid:[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\Z")
PROVENANCE_SEQUENCE_DOMAIN = b"riverhog-canonical-provenance-volumes/v1\x00"


class ProvenanceArchiveError(ValueError):
    """A structural provenance custody document is invalid."""


def _sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _HEX.fullmatch(value) is None:
        raise ProvenanceArchiveError(f"{label} must be lowercase SHA-256 hex")
    return value


def _uuid(value: object, label: str) -> str:
    if not isinstance(value, str) or _UUID.fullmatch(value) is None:
        raise ProvenanceArchiveError(f"{label} must be a canonical UUID URN")
    return value


def _count(value: object, label: str, *, positive: bool = False) -> int:
    try:
        number = parse_scalar("nonnegative", value)
    except (TypeError, ValueError) as exc:
        raise ProvenanceArchiveError(f"{label} must be an exact decimal string") from exc
    if positive and number == 0:
        raise ProvenanceArchiveError(f"{label} must be positive")
    return number


def _object(raw: bytes, label: str, fields: set[str] | None) -> Mapping[str, Any]:
    if len(raw) > PROVENANCE_METADATA_BYTES_MAX:
        raise ProvenanceArchiveError(f"{label} exceeds its metadata byte bound")
    try:
        value = require_canonical_json(raw)
    except ValueError as exc:
        raise ProvenanceArchiveError(f"{label} is not JCS") from exc
    if not isinstance(value, dict) or fields is not None and set(value) != fields:
        raise ProvenanceArchiveError(f"{label} has invalid fields")
    return value


@dataclass(frozen=True, slots=True)
class ProvenancePayload:
    kind: Literal["bindings", "journal"]
    sequence: int
    bytes: int
    sha256: str

    def __post_init__(self) -> None:
        if self.kind not in ("bindings", "journal"):
            raise ProvenanceArchiveError("provenance payload kind is invalid")
        format_archive_sequence(self.sequence)
        limit = (
            PROVENANCE_BINDING_PAGE_BYTES_MAX
            if self.kind == "bindings"
            else PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX
        )
        if type(self.bytes) is not int or not 1 <= self.bytes <= limit:
            raise ProvenanceArchiveError("provenance payload exceeds its segment bound")
        _sha256(self.sha256, "provenance payload")

    @property
    def path(self) -> str:
        return f"provenance/payloads/volume-{format_archive_sequence(self.sequence)}.bin.age"

    def to_mapping(self) -> dict[str, object]:
        return {
            "kind": self.kind,
            "path": self.path,
            "bytes": format_scalar("nonnegative", self.bytes),
            "sha256": self.sha256,
        }

    @classmethod
    def from_mapping(cls, row: object, *, sequence: int) -> ProvenancePayload:
        if not isinstance(row, dict) or set(row) != {"kind", "path", "bytes", "sha256"}:
            raise ProvenanceArchiveError("provenance payload fields are invalid")
        payload = cls(
            kind=cast(Literal["bindings", "journal"], row["kind"]),
            sequence=sequence,
            bytes=_count(row["bytes"], "provenance payload bytes", positive=True),
            sha256=_sha256(row["sha256"], "provenance payload"),
        )
        if row["path"] != payload.path:
            raise ProvenanceArchiveError("provenance payload path is not canonical")
        return payload


@dataclass(frozen=True, slots=True)
class ProvenanceVolumeDocument:
    archive_generation: str
    artifact_set_sha256: str
    sequence: int
    payload: ProvenancePayload
    first_artifact_id: str | None = None
    last_artifact_id: str | None = None
    binding_count: int | None = None
    journal_id: str | None = None
    journal_offset: int | None = None
    journal_bytes: int | None = None
    journal_sha256: str | None = None

    def __post_init__(self) -> None:
        _sha256(self.archive_generation, "archive generation")
        _sha256(self.artifact_set_sha256, "artifact set")
        if self.sequence != self.payload.sequence:
            raise ProvenanceArchiveError("provenance payload sequence differs from metadata")
        if self.payload.kind == "bindings":
            first = _sha256(self.first_artifact_id, "first binding artifact ID")
            last = _sha256(self.last_artifact_id, "last binding artifact ID")
            if first > last:
                raise ProvenanceArchiveError("binding page ID range is reversed")
            if (
                type(self.binding_count) is not int
                or not 1 <= self.binding_count <= PROVENANCE_BINDING_PAGE_MEMBERS_MAX
                or any(
                    value is not None
                    for value in (
                        self.journal_id,
                        self.journal_offset,
                        self.journal_bytes,
                        self.journal_sha256,
                    )
                )
            ):
                raise ProvenanceArchiveError("binding page metadata is invalid")
        else:
            if any(
                value is not None
                for value in (self.first_artifact_id, self.last_artifact_id, self.binding_count)
            ):
                raise ProvenanceArchiveError("journal segment carries binding range")
            _uuid(self.journal_id, "journal ID")
            total = self.journal_bytes
            offset = self.journal_offset
            if (
                type(total) is not int
                or total < 1
                or type(offset) is not int
                or offset < 0
                or offset + self.payload.bytes > total
            ):
                raise ProvenanceArchiveError("journal segment exceeds declared journal")
            _sha256(self.journal_sha256, "journal")

    @property
    def metadata_path(self) -> str:
        return f"provenance/metadata/volume-{format_archive_sequence(self.sequence)}.json.age"

    def to_mapping(self) -> dict[str, object]:
        result: dict[str, object] = {
            "format": PROVENANCE_VOLUME_FORMAT,
            "archive_generation": self.archive_generation,
            "artifact_set_sha256": self.artifact_set_sha256,
            "sequence": format_archive_sequence(self.sequence),
            "payload": self.payload.to_mapping(),
        }
        if self.payload.kind == "bindings":
            assert self.binding_count is not None
            result["binding_range"] = {
                "first_artifact_id": self.first_artifact_id,
                "last_artifact_id": self.last_artifact_id,
                "count": format_scalar("nonnegative", self.binding_count),
            }
        else:
            assert self.journal_offset is not None and self.journal_bytes is not None
            result["journal_range"] = {
                "journal_id": self.journal_id,
                "offset": format_scalar("nonnegative", self.journal_offset),
                "bytes": format_scalar("nonnegative", self.journal_bytes),
                "sha256": self.journal_sha256,
            }
        return result

    def to_json_bytes(self) -> bytes:
        raw = canonical_json_bytes(self.to_mapping())
        if len(raw) > PROVENANCE_METADATA_BYTES_MAX:
            raise ProvenanceArchiveError("provenance volume metadata exceeds its byte bound")
        return raw

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> ProvenanceVolumeDocument:
        value = _object(raw, "provenance volume", None)
        base = {"format", "archive_generation", "artifact_set_sha256", "sequence", "payload"}
        if set(value) not in (base | {"binding_range"}, base | {"journal_range"}):
            raise ProvenanceArchiveError("provenance volume fields are invalid")
        if value["format"] != PROVENANCE_VOLUME_FORMAT:
            raise ProvenanceArchiveError("provenance volume format is unsupported")
        sequence = parse_archive_sequence(value["sequence"])
        payload = ProvenancePayload.from_mapping(value["payload"], sequence=sequence)
        common = {
            "archive_generation": value["archive_generation"],
            "artifact_set_sha256": value["artifact_set_sha256"],
            "sequence": sequence,
            "payload": payload,
        }
        if payload.kind == "bindings":
            if "binding_range" not in value:
                raise ProvenanceArchiveError("binding payload needs a binding range")
            row = value["binding_range"]
            if not isinstance(row, dict) or set(row) != {
                "first_artifact_id",
                "last_artifact_id",
                "count",
            }:
                raise ProvenanceArchiveError("binding range fields are invalid")
            return cls(
                **common,
                first_artifact_id=row["first_artifact_id"],
                last_artifact_id=row["last_artifact_id"],
                binding_count=_count(row["count"], "binding count", positive=True),
            )
        if "journal_range" not in value:
            raise ProvenanceArchiveError("journal payload needs a journal range")
        row = value["journal_range"]
        if not isinstance(row, dict) or set(row) != {"journal_id", "offset", "bytes", "sha256"}:
            raise ProvenanceArchiveError("journal range fields are invalid")
        return cls(
            **common,
            journal_id=row["journal_id"],
            journal_offset=_count(row["offset"], "journal offset"),
            journal_bytes=_count(row["bytes"], "journal bytes", positive=True),
            journal_sha256=row["sha256"],
        )


@dataclass(frozen=True, slots=True)
class ProvenanceTerminalDocument:
    archive_generation: str
    artifact_set_sha256: str
    sequence: int

    def __post_init__(self) -> None:
        _sha256(self.archive_generation, "archive generation")
        _sha256(self.artifact_set_sha256, "artifact set")
        format_archive_sequence(self.sequence)
        if self.sequence < 1:
            raise ProvenanceArchiveError("provenance terminal requires a nonempty sequence")

    @property
    def metadata_path(self) -> str:
        return f"provenance/metadata/volume-{format_archive_sequence(self.sequence)}.json.age"

    def to_mapping(self) -> dict[str, object]:
        return {
            "format": PROVENANCE_TERMINAL_FORMAT,
            "archive_generation": self.archive_generation,
            "artifact_set_sha256": self.artifact_set_sha256,
            "sequence": format_archive_sequence(self.sequence),
            "kind": "terminal",
        }

    def to_json_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_mapping())

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> ProvenanceTerminalDocument:
        value = _object(
            raw,
            "provenance terminal",
            {"format", "archive_generation", "artifact_set_sha256", "sequence", "kind"},
        )
        if value["format"] != PROVENANCE_TERMINAL_FORMAT or value["kind"] != "terminal":
            raise ProvenanceArchiveError("provenance terminal format is unsupported")
        return cls(
            archive_generation=value["archive_generation"],
            artifact_set_sha256=value["artifact_set_sha256"],
            sequence=parse_archive_sequence(value["sequence"]),
        )


class _DigestUpdater(Protocol):
    def update(self, data: bytes) -> object: ...


def update_provenance_commitment(
    digest: _DigestUpdater,
    document: ProvenanceVolumeDocument | ProvenanceTerminalDocument,
) -> None:
    raw = document.to_json_bytes()
    digest.update(len(raw).to_bytes(8, "big"))
    digest.update(raw)


def ordered_provenance_commitment(
    documents: Iterable[ProvenanceVolumeDocument | ProvenanceTerminalDocument],
) -> str:
    """Commit the complete contiguous volume sequence without retaining its rows."""

    digest = hashlib.sha256(PROVENANCE_SEQUENCE_DOMAIN)
    expected = 0
    terminal_seen = False
    authority: tuple[str, str] | None = None
    for document in documents:
        if terminal_seen or document.sequence != expected:
            raise ProvenanceArchiveError("provenance volume sequence is not contiguous")
        current = (document.archive_generation, document.artifact_set_sha256)
        if authority is not None and current != authority:
            raise ProvenanceArchiveError("provenance volume authority changes within sequence")
        authority = current
        terminal_seen = isinstance(document, ProvenanceTerminalDocument)
        update_provenance_commitment(digest, document)
        expected += 1
    if not terminal_seen:
        raise ProvenanceArchiveError("provenance terminal is missing")
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class ProvenanceRootDocument:
    archive_generation: str
    artifact_set_sha256: str
    delivery_context_id: str
    binding_count: int
    binding_tree_sha256: str
    journal_count: int
    ordered_volume_sha256: str

    def __post_init__(self) -> None:
        _sha256(self.archive_generation, "archive generation")
        _sha256(self.artifact_set_sha256, "artifact set")
        _uuid(self.delivery_context_id, "delivery context")
        if type(self.binding_count) is not int or self.binding_count < 1:
            raise ProvenanceArchiveError("provenance root needs bound members")
        _sha256(self.binding_tree_sha256, "member history binding tree")
        if type(self.journal_count) is not int or self.journal_count < 1:
            raise ProvenanceArchiveError("provenance root needs canonical journals")
        _sha256(self.ordered_volume_sha256, "ordered provenance volume")

    def to_mapping(self) -> dict[str, object]:
        result: dict[str, object] = {
            "format": PROVENANCE_ROOT_FORMAT,
            "archive_generation": self.archive_generation,
            "artifact_set_sha256": self.artifact_set_sha256,
            "delivery_context_id": self.delivery_context_id,
            "binding_count": format_scalar("nonnegative", self.binding_count),
            "binding_tree_sha256": self.binding_tree_sha256,
            "journal_count": format_scalar("nonnegative", self.journal_count),
            "volume_sequence": {"sha256": self.ordered_volume_sha256},
        }
        return result

    def to_json_bytes(self) -> bytes:
        raw = canonical_json_bytes(self.to_mapping())
        if len(raw) > PROVENANCE_METADATA_BYTES_MAX:
            raise ProvenanceArchiveError("provenance root exceeds its byte bound")
        return raw

    @property
    def identity(self) -> str:
        return hashlib.sha256(self.to_json_bytes()).hexdigest()

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> ProvenanceRootDocument:
        value = _object(
            raw,
            "provenance root",
            None,
        )
        expected = {
            "format",
            "archive_generation",
            "artifact_set_sha256",
            "delivery_context_id",
            "binding_count",
            "binding_tree_sha256",
            "journal_count",
            "volume_sequence",
        }
        if set(value) != expected:
            raise ProvenanceArchiveError("provenance root has invalid fields")
        if value["format"] != PROVENANCE_ROOT_FORMAT:
            raise ProvenanceArchiveError("provenance root format is unsupported")
        sequence = value["volume_sequence"]
        if not isinstance(sequence, dict) or set(sequence) != {"sha256"}:
            raise ProvenanceArchiveError("provenance root volume sequence is invalid")
        return cls(
            archive_generation=value["archive_generation"],
            artifact_set_sha256=value["artifact_set_sha256"],
            delivery_context_id=value["delivery_context_id"],
            binding_count=_count(value["binding_count"], "binding count", positive=True),
            binding_tree_sha256=value["binding_tree_sha256"],
            journal_count=_count(value["journal_count"], "journal count", positive=True),
            ordered_volume_sha256=sequence["sha256"],
        )


__all__ = [
    "PROVENANCE_BINDING_PAGE_BYTES_MAX",
    "PROVENANCE_BINDING_PAGE_MEMBERS_MAX",
    "PROVENANCE_BINDINGS_FORMAT",
    "PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX",
    "PROVENANCE_METADATA_BYTES_MAX",
    "PROVENANCE_ROOT_FORMAT",
    "PROVENANCE_SEQUENCE_DOMAIN",
    "PROVENANCE_TERMINAL_FORMAT",
    "PROVENANCE_VOLUME_FORMAT",
    "ProvenanceArchiveError",
    "ProvenancePayload",
    "ProvenanceRootDocument",
    "ProvenanceTerminalDocument",
    "ProvenanceVolumeDocument",
    "ordered_provenance_commitment",
    "update_provenance_commitment",
]
