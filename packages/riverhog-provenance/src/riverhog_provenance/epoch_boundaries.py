"""Private correction-boundary facts over exact journal frames.

These facts accelerate execution planning only. Native ordered replay retains
all schema, prefix, identity, reference, graph and semantic acceptance authority.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from .constants import PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
from .errors import ProvenanceValidationError
from .journal import JournalFrame

JOURNAL_EPOCH_BOUNDARY_ALGORITHM = "riverhog-journal-epoch-boundaries/v1"


@dataclass(frozen=True, slots=True)
class JournalEpochBoundary:
    ordinal: int
    epoch: int
    entry_kind: str
    json_sha256: str


def classify_epoch_boundary(
    json_bytes: bytes, *, ordinal: int, previous_epoch: int
) -> JournalEpochBoundary:
    """Classify one entry-bounded atom without accepting any semantic prefix."""
    if type(ordinal) is not int or ordinal < 0:
        raise ValueError("invalid epoch frame ordinal")
    if type(previous_epoch) is not int or not 0 <= previous_epoch <= ordinal:
        raise ValueError("invalid epoch boundary cursor")
    if type(json_bytes) is not bytes:
        raise TypeError("journal frame must be exact bytes")
    if len(json_bytes) > PROVENANCE_JOURNAL_ENTRY_BYTES_MAX:
        raise ProvenanceValidationError("journal entry exceeds the admission byte limit")
    document = JournalFrame(json_bytes).document
    kind = document.get("entry_kind")
    if kind not in ("journal_init", "assertion", "correction", "checkpoint"):
        raise ProvenanceValidationError("journal frame has no known entry kind")
    epoch = previous_epoch + int(ordinal > 0 and kind == "correction")
    return JournalEpochBoundary(ordinal, epoch, kind, hashlib.sha256(json_bytes).hexdigest())
