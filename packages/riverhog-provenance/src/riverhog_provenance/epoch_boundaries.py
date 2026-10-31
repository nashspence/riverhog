"""Private correction-boundary facts over exact journal frames.

These facts accelerate execution planning only. Native ordered replay retains
all schema, prefix, identity, reference, graph and semantic acceptance authority.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)

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


def _epoch_boundary_declaration() -> Declaration:
    def constant(value: int, unit: tuple[tuple[str, int], ...]) -> Expression:
        return Expression("constant", value=value, unit=unit)

    return Declaration(
        id="riverhog-provenance.epoch-classification.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance.epoch_boundaries.classify_epoch_boundary",),
        scope="one exact entry classification, including actual rejected inputs",
        starts_at="receive exact entry bytes and the ordinal/previous-epoch controls",
        ends_at="return the private boundary fact or propagate the existing rejection",
        dimensions=(
            Dimension(
                "input_octets",
                OCTETS,
                (
                    "Full supplied bytes, including "
                    "malformed or over-capacity input; "
                    "zero for rejected non-byte types."
                ),
            ),
        ),
        bounds=(
            Bound(
                "json_decodes",
                COUNT,
                "upper",
                constant(1, COUNT),
                ("Canonical parsing enters the JSON decoder at most once."),
            ),
            Bound(
                "json_decoded_octets",
                OCTETS,
                "upper",
                Expression("dimension", name="input_octets"),
                ("The decoder receives the complete actual source text."),
            ),
            Bound(
                "json_encodes",
                COUNT,
                "upper",
                constant(1, COUNT),
                ("One canonical comparison, including real noncanonical numeric normalization."),
            ),
            Bound(
                "json_encoded_octets",
                OCTETS,
                "upper",
                Expression(
                    "product", terms=(constant(6, ()), Expression("dimension", name="input_octets"))
                ),
                (
                    "Canonical input "
                    "preserves width; "
                    "compact numeric "
                    "lexemes can expand "
                    "up to sixfold before"
                    " rejection."
                ),
            ),
            Bound(
                "hashed_octets",
                OCTETS,
                "upper",
                Expression("dimension", name="input_octets"),
                ("Only an accepted known-kind private fact hashes the exact entry once."),
            ),
            Bound(
                "returned_facts",
                COUNT,
                "upper",
                constant(1, COUNT),
                ("Return one classification, without schema or semantic acceptance."),
            ),
        ),
        conditions=(
            (
                "The existing entry-byte capacity and exact ordinal/epoch input "
                "checks remain in force."
            ),
        ),
        invariants=(
            ("Classification never accepts a semantic prefix or grants archive authority."),
            (
                "The previous epoch "
                "advances only for a "
                "non-initial "
                "correction; the "
                "digest commits to "
                "the exact original "
                "entry bytes."
            ),
        ),
        limitations=(
            (
                "Source lookup, framing, durable publication and ordered native "
                "replay remain separate work."
            ),
            (
                "Codec/byte "
                "primitives exclude "
                "internal object-key "
                "comparisons, "
                "recursive "
                "portable-value "
                "checks, allocations "
                "and CPU latency."
            ),
        ),
    )


declare_growth(_epoch_boundary_declaration())(classify_epoch_boundary)
