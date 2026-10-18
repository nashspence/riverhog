"""Resumable framing over an owner's retained exact source.

Positions are private execution evidence. Framing does not decode documents or
accept a semantic prefix; ordered native validation owns those decisions.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from itertools import islice

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

JOURNAL_FRAMING_ALGORITHM = "riverhog-journal-framing/v1"


@dataclass(frozen=True, slots=True)
class JournalFramePosition:
    ordinal: int
    byte_offset: int
    json_bytes: int


@dataclass(frozen=True, slots=True)
class JournalFramingCursor:
    byte_offset: int = 0
    frame_start: int | None = None
    frame_count: int = 0

    def __post_init__(self) -> None:
        if (
            type(self.byte_offset) is not int
            or self.byte_offset < 0
            or type(self.frame_count) is not int
            or self.frame_count < 0
            or self.frame_start is not None
            and (type(self.frame_start) is not int or not 0 <= self.frame_start < self.byte_offset)
        ):
            raise ValueError("invalid journal framing cursor")


@dataclass(frozen=True, slots=True)
class JournalFramingAdvance:
    cursor: JournalFramingCursor
    positions: tuple[JournalFramePosition, ...]
    complete: bool
    failure: str | None


def _octets(chunk: bytes) -> Iterator[tuple[int, int]]:
    yield from enumerate(chunk)


@declare_growth(
    Declaration(
        id="riverhog-provenance.journal-framing.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance.framing.advance_journal_framing",),
        scope="one-detached-framing-step",
        starts_at="scan one byte-bounded retained source range at its exact cursor",
        ends_at="return private frame positions and the next cursor",
        dimensions=(
            Dimension("consumed_octets", OCTETS, "Source octets advanced by this call."),
            Dimension("byte_budget", OCTETS, "Maximum source octets visited by this call."),
            Dimension("frame_budget", COUNT, "Maximum positions admitted by this call."),
        ),
        bounds=(
            Bound(
                "visited_octets",
                OCTETS,
                "equal",
                Expression("dimension", name="consumed_octets"),
                "Actual source octets visited by the framing iterator.",
            ),
            Bound(
                "consumed_octets",
                OCTETS,
                "upper",
                Expression("dimension", name="byte_budget"),
                "Exact source cursor advancement.",
            ),
            Bound(
                "returned_positions",
                COUNT,
                "upper",
                Expression("dimension", name="frame_budget"),
                "Private complete-frame position records returned.",
            ),
            Bound(
                "retained_content_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "Content retained in the continuation cursor.",
            ),
        ),
        conditions=("The owner supplies an exact byte-bounded range at this cursor.",),
        invariants=("Every advanced octet is visited once; capture never accepts semantics.",),
        limitations=(
            "Owner source lookup, durable writes, retries and cleanup.",
            "Canonical decoding, schema, graph and semantic prefix validation.",
            "Physical CPU time and allocation costs for returned position records.",
        ),
    )
)
def advance_journal_framing(
    cursor: JournalFramingCursor,
    chunk: bytes,
    *,
    byte_budget: int,
    frame_budget: int,
    at_end: bool,
    max_entry_bytes: int = PROVENANCE_JOURNAL_ENTRY_BYTES_MAX,
) -> JournalFramingAdvance:
    """Scan at most the supplied range and position budget, without content copies.

    The owner caps the range before loading it. A failure is speculative and must
    not outrank an earlier native semantic failure. Resume requires the same exact
    retained source; this cursor provides neither source custody nor authentication.
    """
    if type(chunk) is not bytes:
        raise TypeError("journal chunks must be bytes")
    if type(byte_budget) is not int or byte_budget < 1:
        raise ValueError("byte_budget must be positive")
    if type(frame_budget) is not int or frame_budget < 1:
        raise ValueError("frame_budget must be positive")
    if type(max_entry_bytes) is not int or max_entry_bytes < 1:
        raise ValueError("max_entry_bytes must be positive")
    if type(at_end) is not bool:
        raise TypeError("at_end must be boolean")
    start = cursor.frame_start
    if start is not None and cursor.byte_offset - start - 1 > max_entry_bytes:
        raise ValueError("framing cursor exceeds the entry byte limit")
    offset, count = cursor.byte_offset, cursor.frame_count
    positions: list[JournalFramePosition] = []
    failure = None
    for _, octet in islice(_octets(chunk), byte_budget):
        position = offset
        offset += 1
        if start is None:
            if octet != 0x1E:
                failure = "bytes outside RFC 7464 frames"
                break
            start = position
        elif octet == 0x1E:
            failure = "unframed record separator before line feed"
            break
        elif octet == 0x0A:
            positions.append(JournalFramePosition(count, start, position - start - 1))
            count += 1
            start = None
            if len(positions) == frame_budget:
                break
        elif position - start > max_entry_bytes:
            failure = "journal entry exceeds the admission byte limit"
            break
    complete = at_end and offset == cursor.byte_offset + len(chunk) and failure is None
    if complete and start is not None:
        failure = "incomplete final journal frame"
    elif complete and count == 0:
        failure = "journal is empty"
    return JournalFramingAdvance(
        JournalFramingCursor(offset, start, count),
        tuple(positions),
        complete and failure is None,
        failure,
    )
