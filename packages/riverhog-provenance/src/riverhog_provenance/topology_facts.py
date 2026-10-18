"""Private source positions and documentary topology facts for ordered replay."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from typing import Any

from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import (
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)
from riverhog_provenance_contracts import decode_document

from .constants import PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
from .journal import JournalFrame

TOPOLOGY_CAPTURE_ALGORITHM = "riverhog-journal-topology-facts/v1"
_READ_OCTETS = 65536


@dataclass(frozen=True, slots=True)
class TopologyArrayCursor:
    offset: int
    end: int
    index: int
    count: int


@dataclass(frozen=True, slots=True)
class TopologyFrameCursor:
    json_sha256: str
    retracts: TopologyArrayCursor
    relations: TopologyArrayCursor


@dataclass(frozen=True, slots=True)
class JournalTopologyFact:
    array: str
    index: int
    byte_offset: int
    json_bytes: int
    json_sha256: str
    assertion_id: str
    kind: str
    source_id: str | None = None
    target_id: str | None = None


@dataclass(frozen=True, slots=True)
class TopologyFrameAdvance:
    cursor: TopologyFrameCursor
    facts: tuple[JournalTopologyFact, ...]
    inspected_items: int
    inspected_octets: int

    @property
    def complete(self) -> bool:
        return all(c.index == c.count for c in (self.cursor.retracts, self.cursor.relations))


def _ranges(value: Mapping[str, Any], start: int) -> dict[str, tuple[int, int]]:
    """Locate exact canonical values with one entry-bounded serialization pass."""
    result = {}
    offset = start + 1
    for key in sorted(value, key=lambda k: k.encode("utf-16be")):
        offset += len(canonical_json_bytes(key)) + 1
        end = offset + len(canonical_json_bytes(value[key]))
        result[key] = (offset, end)
        offset = end + 1
    return result


def plan_topology_frame(json_bytes: bytes) -> TopologyFrameCursor:
    """Decode one admitted entry atom once; retain only two fixed-shape cursors."""
    if type(json_bytes) is not bytes or len(json_bytes) > PROVENANCE_JOURNAL_ENTRY_BYTES_MAX:
        raise ValueError("invalid topology source entry extent")
    document = JournalFrame(json_bytes).document
    body = document.get("body")
    if not isinstance(body, dict):
        raise ValueError("topology source has no body object")
    root_ranges = _ranges(document, 0)
    body_ranges = _ranges(body, root_ranges["body"][0])
    assertions = body.get("assertions", {})
    if not isinstance(assertions, dict):
        raise ValueError("topology source assertions are not an object")
    assertion_ranges = (
        _ranges(assertions, body_ranges["assertions"][0]) if "assertions" in body else {}
    )

    def array(value: object, extent: tuple[int, int] | None) -> TopologyArrayCursor:
        if not isinstance(value, list):
            raise ValueError("topology source item directory is not an array")
        if extent is None:
            return TopologyArrayCursor(0, 0, 0, 0)
        start, end = extent
        if json_bytes[start : start + 1] != b"[" or json_bytes[end - 1 : end] != b"]":
            raise RuntimeError("topology source array positions differ from exact bytes")
        return TopologyArrayCursor(start + 1, end, 0, len(value))

    retracts = (
        array(body.get("retracts", []), body_ranges.get("retracts"))
        if document.get("entry_kind") == "correction"
        else TopologyArrayCursor(0, 0, 0, 0)
    )
    relations = array(assertions.get("relations", []), assertion_ranges.get("relations"))
    return TopologyFrameCursor(hashlib.sha256(json_bytes).hexdigest(), retracts, relations)


@declare_growth(
    Declaration(
        id="riverhog-provenance.topology-item-scan.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance.topology_facts._scan_item",),
        scope="successful-exact-one-item-structural-scan",
        starts_at="seek the owner-bound exact next array item position",
        ends_at="return that item and its next delimiter cursor",
        dimensions=(Dimension("item_octets", OCTETS, "Exact JSON octets in this item."),),
        bounds=(
            Bound(
                "visited_octets",
                OCTETS,
                "equal",
                Expression(
                    "sum",
                    terms=(
                        Expression("dimension", name="item_octets"),
                        Expression("constant", value=1, unit=OCTETS),
                    ),
                ),
                "Visit each item octet once and its one closing array delimiter.",
            ),
            Bound(
                "returned_octets",
                OCTETS,
                "equal",
                Expression("dimension", name="item_octets"),
                "Return only this original item, without earlier array contents.",
            ),
            Bound(
                "source_range_octets",
                OCTETS,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        Expression("dimension", name="item_octets"),
                        Expression("constant", value=65536, unit=OCTETS),
                    ),
                ),
                "At most one final read window extends beyond the item boundary.",
            ),
        ),
        conditions=(
            "The owner supplies authenticated exact ranges at this retained cursor.",
            "The source is an already canonical entry within its existing entry-byte envelope.",
        ),
        invariants=(
            "No committed preceding item is read or decoded by this operation.",
            "Private documentary positions do not supply semantic acceptance.",
        ),
        limitations=(
            "Initial entry planning, owner page lookup and hashing, item decoding and hashing, "
            "durable publication, retries and reclamation are separate costs.",
            "Allocation/copy CPU, graph construction, graph validation and semantic replay "
            "are outside this structural scan.",
        ),
    )
)
def _scan_item(read: Callable[[int, int], bytes], cursor: TopologyArrayCursor) -> tuple[bytes, int]:
    """Read one exact JSON array item without revisiting any preceding item."""
    start = cursor.offset
    if not 0 <= cursor.index < cursor.count or not start < cursor.end:
        raise RuntimeError("invalid topology item source cursor")
    offset, depth = start, 0
    quoted = escaped = False
    pieces = []
    while offset < cursor.end:
        block = read(offset, min(_READ_OCTETS, cursor.end - offset))
        if not block or len(block) > min(_READ_OCTETS, cursor.end - offset):
            raise RuntimeError("topology source item range is unavailable")
        for index, octet in enumerate(block):
            if not quoted and depth == 0 and octet in (44, 93):
                pieces.append(block[:index])
                raw = b"".join(pieces)
                next_offset = offset + index + 1
                last = cursor.index + 1 == cursor.count
                if not raw or (octet == 93) != last or (last and next_offset != cursor.end):
                    raise RuntimeError("topology item count differs from its source array")
                return raw, next_offset
            if quoted:
                if escaped:
                    escaped = False
                elif octet == 92:
                    escaped = True
                elif octet == 34:
                    quoted = False
            elif octet == 34:
                quoted = True
            elif octet in (123, 91):
                depth += 1
            elif octet in (125, 93):
                depth -= 1
                if depth < 0:
                    raise RuntimeError("topology item has an invalid container extent")
        pieces.append(block)
        offset += len(block)
        if offset - start > PROVENANCE_JOURNAL_ENTRY_BYTES_MAX:
            raise RuntimeError("topology item exceeds its admitted entry extent")
    raise RuntimeError("topology source array lost its item delimiter")


def _fact(array: str, cursor: TopologyArrayCursor, raw: bytes) -> JournalTopologyFact | None:
    value = decode_document(raw)
    if not isinstance(value, dict):
        raise ValueError("topology source item is not an object")
    if array == "retracts":
        kind = "retraction"
    else:
        relation_kind = value.get("type")
        if not isinstance(relation_kind, str) or relation_kind not in (
            "derivation",
            "specialization",
        ):
            return None
        kind = relation_kind
    identity = value.get("assertion_id")
    if not isinstance(identity, str):
        raise ValueError("topology source item has no documentary identity")
    source = target = None
    if kind != "retraction":
        left, right = (
            ("used_state", "generated_state") if kind == "derivation" else ("specific", "general")
        )
        source_ref, target_ref = value.get(left), value.get(right)
        if not isinstance(source_ref, dict) or not isinstance(target_ref, dict):
            raise ValueError("topology source edge has no object references")
        source, target = source_ref.get("object_id"), target_ref.get("object_id")
        if not isinstance(source, str) or not isinstance(target, str):
            raise ValueError("topology source edge has no endpoint identities")
    return JournalTopologyFact(
        array,
        cursor.index,
        cursor.offset,
        len(raw),
        hashlib.sha256(raw).hexdigest(),
        identity,
        kind,
        source,
        target,
    )


def advance_topology_frame(
    cursor: TopologyFrameCursor,
    read: Callable[[int, int], bytes],
    *,
    item_budget: int = 64,
    octet_budget: int = 65536,
) -> TopologyFrameAdvance:
    """Advance bounded items, plus one existing entry-bounded final item atom.

    The source owner supplies only the exact authenticated frame ranges. Full
    entry planning, source-page verification and item canonical decoding are
    separate costs. No graph or semantic acceptance is inferred from these facts.
    """
    if type(item_budget) is not int or not 1 <= item_budget <= 64:
        raise ValueError("invalid topology item budget")
    if type(octet_budget) is not int or not 1 <= octet_budget <= 65536:
        raise ValueError("invalid topology octet budget")
    facts = []
    inspected = octets = 0
    for name in ("retracts", "relations"):
        array = getattr(cursor, name)
        while array.index < array.count and inspected < item_budget and octets < octet_budget:
            raw, next_offset = _scan_item(read, array)
            fact = _fact(name, array, raw)
            if fact is not None:
                facts.append(fact)
            inspected += 1
            octets += next_offset - array.offset
            array = replace(array, index=array.index + 1, offset=next_offset)
        cursor = replace(cursor, **{name: array})
    return TopologyFrameAdvance(cursor, tuple(facts), inspected, octets)
