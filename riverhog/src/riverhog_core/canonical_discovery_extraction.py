"""Schema-defined lexical projection of a canonical provenance assertion.

The records returned here are rebuildable search data. The journal remains the
authority for every assertion and exact byte sequence.
"""

from __future__ import annotations

import base64
import hashlib
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any, Literal

from riverhog_canonical_json import canonical_json_bytes
from riverhog_protocol.artifact_discovery import AssertionClause, ProfilePin, ValuePredicate
from riverhog_provenance_contracts.codec import require_portable_json

type Representation = Literal["json-scalar", "declared-text", "byte-escape", "exact-bytes"]

_TEXT_CHUNK_CODEPOINTS = 4096
_TEXT_CHUNK_OVERLAP = 255
_ASCII_FOLD = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")


@dataclass(frozen=True, slots=True)
class ScalarPosting:
    pointer: str
    value: str | int | bool | None
    representation: Representation
    source_value_sha256: str


@dataclass(frozen=True, slots=True)
class ProfileSection:
    pointer: str
    profile: ProfilePin


def literal_chunks(value: str) -> Iterator[tuple[int, str]]:
    """Cover every <=256-codepoint literal, including a boundary crossing."""
    step = _TEXT_CHUNK_CODEPOINTS - _TEXT_CHUNK_OVERLAP
    for offset in range(0, len(value), step):
        yield offset, value[offset : offset + _TEXT_CHUNK_CODEPOINTS]


def ascii_fold(value: str) -> str:
    return value.translate(_ASCII_FOLD)


def _pointer_part(value: str) -> str:
    return value.replace("~", "~0").replace("/", "~1")


def scalar_postings(value: Any, pointer: str = "") -> Iterator[ScalarPosting]:
    """Visit scalar VALUES, not keys, preserving their exact JSON type and pointer."""
    if isinstance(value, dict):
        for key, child in sorted(value.items()):
            yield from scalar_postings(child, f"{pointer}/{_pointer_part(key)}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from scalar_postings(child, f"{pointer}/{index}")
    elif value is None or type(value) in (str, int, bool):
        yield ScalarPosting(
            pointer=pointer,
            value=value,
            representation="json-scalar",
            source_value_sha256=hashlib.sha256(canonical_json_bytes(value)).hexdigest(),
        )
    else:
        raise TypeError("canonical assertion contains a nonportable scalar")


def _name_slots(row: Mapping[str, Any]) -> Iterator[tuple[str, Mapping[str, Any]]]:
    """Only core schema `$defs/name` locations; never opaque profile lookalikes."""
    for index, evidence in enumerate(row.get("evidence", ())):
        source = evidence.get("source_identifier")
        if isinstance(source, dict):
            yield f"/evidence/{index}/source_identifier/value", source["value"]
    for index, identifier in enumerate(row.get("identifiers", ())):
        yield f"/identifiers/{index}/value", identifier["value"]
    consistency = row.get("consistency")
    if isinstance(consistency, dict) and "snapshot_identifier" in consistency:
        yield "/consistency/snapshot_identifier/value", consistency["snapshot_identifier"]["value"]
    for index, record in enumerate(row.get("metadata", ())):
        yield f"/metadata/{index}/name", record["name"]
        source = record.get("source")
        if isinstance(source, dict) and "field" in source:
            yield f"/metadata/{index}/source/field", source["field"]
    if row["type"] == "delivery_association" and "slot" in row:
        yield "/slot", row["slot"]
    if row["type"] == "locator_binding":
        locator = row["locator"]
        for key in {
            "filesystem_path": ("name",),
            "object_key": ("key", "version"),
            "repository_id": ("identifier", "version"),
            "opaque": ("value",),
        }.get(locator["kind"], ()):
            if key in locator:
                yield f"/locator/{key}", locator[key]


def _escaped_bytes(value: bytes) -> str:
    return "".join(
        chr(byte) if 32 <= byte < 127 and byte != 92 else "\\\\" if byte == 92 else f"\\x{byte:02x}"
        for byte in value
    )


def name_postings(row: Mapping[str, Any]) -> Iterator[ScalarPosting]:
    for pointer, name in _name_slots(row):
        if name.get("kind") != "bytes":
            continue
        exact = base64.b64decode(name["bytes"]["data"], validate=True)
        source_sha256 = hashlib.sha256(canonical_json_bytes(name)).hexdigest()
        encoded = base64.b64encode(exact).decode("ascii")
        yield ScalarPosting(pointer, encoded, "exact-bytes", source_sha256)
        yield ScalarPosting(pointer, _escaped_bytes(exact), "byte-escape", source_sha256)
        if name["encoding"] in {"utf-8", "utf-16le"}:
            try:
                decoded = exact.decode(name["encoding"], "strict")
                require_portable_json(decoded)
            except (UnicodeError, ValueError):
                continue
            yield ScalarPosting(pointer, decoded, "declared-text", source_sha256)


def declared_profile_sections(row: Mapping[str, Any]) -> Iterator[ProfileSection]:
    """Enumerate all and only profileValue slots in the pinned core schema."""
    kind = row["type"]
    if kind == "extension" and row["value"]["type"] == "json":
        yield ProfileSection(
            "/value/value", ProfilePin.model_validate(row["value"]["value"]["profile"])
        )
    if kind in {"context", "observation", "reported_description"}:
        for index, item in enumerate(row.get("profiles", ())):
            yield ProfileSection(f"/profiles/{index}", ProfilePin.model_validate(item["profile"]))
    if kind == "activity":
        for index, item in enumerate(row.get("details", ())):
            yield ProfileSection(f"/details/{index}", ProfilePin.model_validate(item["profile"]))
        if "configuration" in row:
            yield ProfileSection(
                "/configuration", ProfilePin.model_validate(row["configuration"]["profile"])
            )
    if kind == "locator_binding" and row["locator"]["kind"] == "database_selector":
        yield ProfileSection(
            "/locator/selector", ProfilePin.model_validate(row["locator"]["selector"]["profile"])
        )


def assertion_postings(row: Mapping[str, Any]) -> Iterator[ScalarPosting]:
    yield from scalar_postings(row)
    yield from name_postings(row)


def predicate_matches(posting: ScalarPosting, predicate: ValuePredicate) -> bool:
    if predicate.pointer is not None and posting.pointer != predicate.pointer:
        return False
    if predicate.operator == "bytes-equals":
        return posting.representation == "exact-bytes" and posting.value == predicate.value
    if posting.representation == "exact-bytes":
        return False
    if predicate.operator == "equals":
        if type(posting.value) is not type(predicate.value):
            return False
        if isinstance(posting.value, str) and predicate.text_mode == "ascii-fold":
            assert isinstance(predicate.value, str)
            return ascii_fold(posting.value) == ascii_fold(predicate.value)
        return posting.value == predicate.value
    if type(posting.value) is not str:
        return False
    assert isinstance(predicate.value, str)
    haystack = posting.value
    needle = predicate.value
    if predicate.text_mode == "ascii-fold":
        haystack, needle = ascii_fold(haystack), ascii_fold(needle)
    return needle in haystack


def assertion_clause_matches(
    row: Mapping[str, Any],
    clause: AssertionClause,
    *,
    assertion_state: Literal["effective", "retracted"],
) -> tuple[ScalarPosting, ...] | None:
    """All predicates share one assertion and, when pinned, one profile section."""
    if clause.kind is not None and row["type"] != clause.kind:
        return None
    if clause.assertion_state != "any" and clause.assertion_state != assertion_state:
        return None
    postings = tuple(assertion_postings(row))
    groups: tuple[tuple[ScalarPosting, ...], ...] = (postings,)
    if clause.profile is not None:
        groups = tuple(
            tuple(
                posting
                for posting in postings
                if posting.pointer.startswith(section.pointer + "/data/")
                or posting.pointer == section.pointer + "/data"
            )
            for section in declared_profile_sections(row)
            if section.profile == clause.profile
        )
    for group in groups:
        found = tuple(
            next((posting for posting in group if predicate_matches(posting, predicate)), None)
            for predicate in clause.values
        )
        if all(posting is not None for posting in found):
            return tuple(posting for posting in found if posting is not None)
    return None


__all__ = [
    "ProfileSection",
    "ScalarPosting",
    "ascii_fold",
    "assertion_clause_matches",
    "assertion_postings",
    "declared_profile_sections",
    "literal_chunks",
    "predicate_matches",
]
