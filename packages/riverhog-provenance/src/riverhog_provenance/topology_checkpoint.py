"""Canonical fixed-shape execution controls for private topology capture."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from riverhog_canonical_json import canonical_json_bytes, format_scalar, parse_scalar
from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)

from .topology_facts import TopologyArrayCursor, TopologyFrameCursor


def validate_checkpoint_digest(value: object) -> str:
    """Require one exact lowercase SHA-256 control string without granting authority."""
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError("invalid topology checkpoint digest")
    return value


def _array(value: object) -> TopologyArrayCursor:
    if not isinstance(value, list) or len(value) != 4:
        raise ValueError("invalid topology array checkpoint shape")
    offset, end, index, count = (parse_scalar("nonnegative", item) for item in value)
    if offset > end or index > count:
        raise ValueError("invalid topology array checkpoint extent")
    return TopologyArrayCursor(offset, end, index, count)


@dataclass(frozen=True, slots=True)
class TopologyCaptureCheckpoint:
    """Detached positions and counters, without source or acceptance authority.

    The caller binds this private execution state to its actual source and
    algorithm, fences storage/currentness and publishes facts with this cursor.
    No payload, connection, graph, historical list or permission is retained.
    """

    active: TopologyFrameCursor | None
    facts: int
    last: tuple[int, str, int, str] | None

    def to_json(self) -> str:
        def array(value: TopologyArrayCursor) -> list[str]:
            if value.offset > value.end or value.index > value.count:
                raise ValueError("invalid topology array checkpoint extent")
            return [
                format_scalar("nonnegative", item)
                for item in (value.offset, value.end, value.index, value.count)
            ]

        active, last = self.active, self.last
        if (self.facts == 0) != (last is None):
            raise ValueError("topology checkpoint has inconsistent last-fact controls")
        if last is not None and (len(last) != 4 or last[1] not in {"retracts", "relations"}):
            raise ValueError("invalid topology last fact checkpoint shape")
        return canonical_json_bytes(
            {
                "active": None
                if active is None
                else {
                    "json_sha256": validate_checkpoint_digest(active.json_sha256),
                    "retracts": array(active.retracts),
                    "relations": array(active.relations),
                },
                "facts": format_scalar("nonnegative", self.facts),
                "last": None
                if last is None
                else [
                    format_scalar("nonnegative", last[0]),
                    last[1],
                    format_scalar("nonnegative", last[2]),
                    validate_checkpoint_digest(last[3]),
                ],
            }
        ).decode()

    @classmethod
    def from_json(cls, raw: str) -> TopologyCaptureCheckpoint:
        try:
            if type(raw) is not str:
                raise ValueError("topology checkpoint must be canonical text")
            value = json.loads(raw)
            if not isinstance(value, dict) or set(value) != {"active", "facts", "last"}:
                raise ValueError("invalid topology checkpoint shape")
            active = value["active"]
            if active is not None:
                if not isinstance(active, dict) or set(active) != {
                    "json_sha256",
                    "retracts",
                    "relations",
                }:
                    raise ValueError("invalid topology frame checkpoint shape")
                active = TopologyFrameCursor(
                    validate_checkpoint_digest(active["json_sha256"]),
                    _array(active["retracts"]),
                    _array(active["relations"]),
                )
            last = value["last"]
            if last is not None:
                if (
                    not isinstance(last, list)
                    or len(last) != 4
                    or last[1] not in {"retracts", "relations"}
                ):
                    raise ValueError("invalid topology last fact checkpoint shape")
                last = (
                    parse_scalar("nonnegative", last[0]),
                    last[1],
                    parse_scalar("nonnegative", last[2]),
                    validate_checkpoint_digest(last[3]),
                )
            header = cls(active, parse_scalar("nonnegative", value["facts"]), last)
            if header.to_json() != raw:
                raise ValueError("topology checkpoint is noncanonical")
            return header
        except (ValueError, TypeError, KeyError, OverflowError) as error:
            raise RuntimeError("invalid retained topology checkpoint") from error


def _declare_growth() -> None:
    for decoding in (False, True):
        operation: Any = (
            TopologyCaptureCheckpoint.__dict__["from_json"].__func__
            if decoding
            else TopologyCaptureCheckpoint.to_json
        )
        width = Expression("dimension", name="control_octets")

        def count(value: int) -> Expression:
            return Expression("constant", value=value, unit=COUNT)

        declare_growth(
            Declaration(
                id="riverhog-provenance.topology-checkpoint-"
                + ("decode" if decoding else "encode")
                + ".growth/v1",
                authority="riverhog-provenance",
                subjects=(operation.__module__ + "." + operation.__qualname__,),
                scope="complete fixed-shape private topology checkpoint codec",
                starts_at="receive complete checkpoint text or detached typed controls",
                ends_at="return one canonical encoding or immutable checkpoint, or reject",
                dimensions=(
                    Dimension(
                        "control_octets",
                        OCTETS,
                        "Complete text or canonical typed controls, including all scalar digits.",
                    ),
                    Dimension(
                        "retained_headers", COUNT, "Independent historical checkpoint count."
                    ),
                ),
                bounds=(
                    Bound(
                        "json_decodes",
                        COUNT,
                        "upper",
                        count(int(decoding)),
                        "Decode at most this one complete control value.",
                    ),
                    Bound(
                        "json_encodes",
                        COUNT,
                        "upper",
                        count(1),
                        "One canonical encoding or comparison.",
                    ),
                    Bound(
                        "json_decoded_octets",
                        OCTETS,
                        "upper",
                        width if decoding else Expression("constant", value=0, unit=OCTETS),
                        "Complete actual input enters the decoder.",
                    ),
                    Bound(
                        "json_encoded_octets",
                        OCTETS,
                        "upper",
                        width,
                        "All fixed keys and complete scalar strings are encoded.",
                    ),
                    Bound(
                        "scalar_decodes",
                        COUNT,
                        "upper",
                        count(11 if decoding else 0),
                        "Two four-field array cursors and three documentary counters.",
                    ),
                    Bound(
                        "scalar_encodes",
                        COUNT,
                        "upper",
                        count(11),
                        "Serialize the exact canonical nonnegative counter domain.",
                    ),
                    Bound(
                        "scalar_decoded_octets",
                        OCTETS,
                        "upper",
                        width,
                        "Each input scalar string is decoded once.",
                    ),
                    Bound(
                        "scalar_encoded_octets",
                        OCTETS,
                        "upper",
                        width,
                        "Each output scalar string is encoded once.",
                    ),
                    Bound(
                        "digest_character_visits",
                        COUNT,
                        "upper",
                        count(256 if decoding else 128),
                        "At most four or two fixed 64-character SHA-256 controls.",
                    ),
                    Bound("database_calls", COUNT, "equal", count(0), "The codec owns no storage."),
                    Bound(
                        "source_reads",
                        COUNT,
                        "equal",
                        count(0),
                        "No documentary payload or historical source is read.",
                    ),
                ),
                conditions=(
                    "Encoding dimensions include the complete canonical typed-control encoding.",
                    "Decode dimensions include rejected text; non-text has zero input octets.",
                    "The caller owns source/algorithm binding, currentness and atomic publication.",
                ),
                invariants=(
                    "Only fixed-shape cursors and scalar counters enter the checkpoint.",
                    "Canonical scalar domains are preserved without a history or count ceiling.",
                    "A decoded checkpoint grants no semantic, graph, storage or source authority.",
                    "No older checkpoint, source frame or database is traversed.",
                ),
                limitations=(
                    "Source validation, planning, publication and acceptance are separate.",
                    "Logical codec bytes exclude parser internals, allocation and machine latency.",
                ),
            )
        )(operation)


_declare_growth()


declare_growth(
    Declaration(
        id="riverhog-provenance.checkpoint-digest-shape.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance.topology_checkpoint.validate_checkpoint_digest",),
        scope="fixed SHA-256 control shape without source or cryptographic authority",
        starts_at="receive one complete proposed control value",
        ends_at="return the same exact lowercase digest text or reject its shape",
        dimensions=(
            Dimension("supplied_octets", OCTETS, "Complete supplied text, zero for non-text."),
        ),
        bounds=(
            Bound(
                "digest_character_visits",
                COUNT,
                "upper",
                Expression("constant", value=64, unit=COUNT),
                "Only an exact 64-character string enters the fixed alphabet check.",
            ),
            Bound(
                "returned_octets",
                OCTETS,
                "upper",
                Expression("constant", value=64, unit=OCTETS),
                "Only the unchanged 64-byte ASCII control can be returned.",
            ),
        ),
        conditions=("The input's complete width is retained even when rejected before iteration.",),
        invariants=(
            "Shape alone grants no source binding, integrity proof or semantic acceptance.",
        ),
        limitations=(
            "Logical character visits exclude interpreter internals and physical CPU time.",
        ),
    )
)(validate_checkpoint_digest)
