"""Storage-neutral journal octet segmentation; no archive layout is prescribed."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from riverhog_provenance_contracts import ContractCatalog, canonical_document

from .errors import ProvenanceValidationError
from .journal import validate_journal

PROVENANCE_VOLUME_FORMAT = "riverhog-provenance-volume/v1"
PROVENANCE_ROOT_FORMAT = "riverhog-provenance-root/v1"
PROVENANCE_BINDING_SEGMENT_FORMAT = "riverhog-provenance-bindings/v1"
VOLUME_SCHEMA = (
    "https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-volume-v1.schema.json"
)
ROOT_SCHEMA = (
    "https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-root-v1.schema.json"
)


@dataclass(frozen=True, slots=True)
class JournalSegment:
    object_id: str
    segment_index: int
    offset: int
    object_bytes: int
    object_sha256: str
    content: bytes

    def descriptor(self) -> dict[str, Any]:
        return {
            "format": PROVENANCE_VOLUME_FORMAT,
            "object_id": self.object_id,
            "segment_index": str(self.segment_index),
            "offset": str(self.offset),
            "bytes": str(len(self.content)),
            "sha256": hashlib.sha256(self.content).hexdigest(),
            "object_bytes": str(self.object_bytes),
            "object_sha256": self.object_sha256,
        }


def segment_journal(
    raw: bytes,
    *,
    maximum_segment_bytes: int = 8 * 1024 * 1024,
    catalog: ContractCatalog | None = None,
) -> tuple[JournalSegment, ...]:
    if type(maximum_segment_bytes) is not int or maximum_segment_bytes < 1:
        raise ValueError("maximum_segment_bytes must be positive")
    summary = validate_journal(raw, catalog=catalog)
    return tuple(
        JournalSegment(
            summary.journal_id,
            index,
            start,
            len(raw),
            summary.journal_sha256,
            raw[start : start + maximum_segment_bytes],
        )
        for index, start in enumerate(range(0, len(raw), maximum_segment_bytes))
    )


def ordered_segment_commitment(descriptors: Iterable[Mapping[str, Any]]) -> str:
    digest = hashlib.sha256(b"riverhog-provenance-segments/v1\x00")
    for descriptor in descriptors:
        encoded = canonical_document(descriptor)
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def reassemble_journal(
    segments: Iterable[JournalSegment], *, catalog: ContractCatalog | None = None
) -> bytes:
    selected = catalog or ContractCatalog()
    values = tuple(segments)
    if not values:
        raise ProvenanceValidationError("journal segment set is empty")
    offset = 0
    first = values[0]
    for index, segment in enumerate(values):
        selected.validate(VOLUME_SCHEMA, segment.descriptor())
        if not segment.content or segment.offset != offset or segment.segment_index != index:
            raise ProvenanceValidationError(
                "journal segments overlap, have a gap, or are out of order"
            )
        if (segment.object_id, segment.object_bytes, segment.object_sha256) != (
            first.object_id,
            first.object_bytes,
            first.object_sha256,
        ):
            raise ProvenanceValidationError("journal segment commitments disagree")
        offset += len(segment.content)
    raw = b"".join(segment.content for segment in values)
    if len(raw) != first.object_bytes or hashlib.sha256(raw).hexdigest() != first.object_sha256:
        raise ProvenanceValidationError(
            "journal segment payload does not match whole-object commitment"
        )
    summary = validate_journal(raw, catalog=selected)
    if summary.journal_id != first.object_id:
        raise ProvenanceValidationError("segment identity does not identify the enclosed journal")
    return raw
