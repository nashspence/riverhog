"""Read original documentary-reference facts for shared dependency closure."""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from typing import Any

from riverhog_canonical_json import require_canonical_json
from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    OCTETS_PER_COUNT,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)
from riverhog_provenance import inspect_documentary_node

from riverhog_core.canonical_discovery_dependency_source import _completed_source
from riverhog_core.canonical_discovery_fact_work import (
    FACT_FRAGMENT_BYTES,
    FactSegmentConflict,
    StaleFactTask,
    _identity,
    _one,
)
from riverhog_core.canonical_discovery_journal_work import JournalAssertion, JournalTaskKey


class DocumentaryReferenceCapacity(ValueError):
    """The next complete reference exceeds the supplied execution capacity."""


@dataclass(frozen=True, slots=True)
class DocumentaryPosition:
    assertion: JournalAssertion
    node: int


@dataclass(frozen=True, slots=True)
class DocumentaryReference:
    position: DocumentaryPosition
    reference: dict[str, Any]


def _declaration() -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dim(name: str) -> Expression:
        return Expression("dimension", name=name)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value, unit=()), term))

    def total(*terms: Expression) -> Expression:
        return Expression("sum", terms=terms)

    metadata = dim("metadata_octets")
    payload = dim("reference_octets")
    fragments = dim("fragments")
    return Declaration(
        id="riverhog-server.discovery-documentary-reference-next.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_dependency_facts.next_documentary_reference",),
        scope="one original assertion's next complete documentary-reference stream",
        starts_at="receive a completed original assertion and an owner-controlled node cursor",
        ends_at="close every cursor and detach one fully verified Native reference or absence",
        dimensions=(
            Dimension("registered_rows", COUNT, "All retained work rows, including other sources."),
            Dimension("metadata_octets", OCTETS, "Maximum full input/header/cursor metadata row."),
            Dimension("reference_octets", OCTETS, "Complete next reference bytes, or zero at end."),
            Dimension("fragments", COUNT, "Physical fragments of that complete reference."),
        ),
        bounds=(
            Bound("sql_statements", COUNT, "upper", count(4), "Three fences/seeks and one range."),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                total(count(4), fragments),
                "Three metadata rows and the exact fragment range, with one excess-row probe.",
            ),
            Bound(
                "parameter_octets",
                OCTETS,
                "upper",
                scale(24, metadata),
                "Complete exact source, assertion and cursor identities.",
            ),
            Bound(
                "returned_octets",
                OCTETS,
                "upper",
                total(
                    scale(16, metadata),
                    payload,
                    Expression(
                        "product",
                        terms=(
                            Expression("constant", value=128, unit=OCTETS_PER_COUNT),
                            fragments,
                        ),
                    ),
                ),
                "Full metadata, exact payload and physical fragment metadata.",
            ),
            Bound(
                "hashed_octets",
                OCTETS,
                "upper",
                payload,
                "Every selected fragment is hashed once; no unrelated facts are hashed.",
            ),
            *(
                Bound(
                    metric,
                    OCTETS,
                    "upper",
                    total(metadata, payload),
                    "Complete canonical origin and selected-reference decoding/re-encoding.",
                )
                for metric in ("decoded_octets", "encoded_octets")
            ),
            Bound(
                "native_reference_calls",
                COUNT,
                "upper",
                count(1),
                "Native owns interpretation of the selected complete reference.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                total(
                    count(2048),
                    Expression(
                        "product",
                        terms=(
                            count(512),
                            Expression(
                                "log2_ceil", terms=(total(dim("registered_rows"), count(2)),)
                            ),
                        ),
                    ),
                    scale(128, fragments),
                ),
                "Indexed source/header seeks and one contiguous physical fragment range.",
            ),
        ),
        conditions=(
            "The owner preserves qualified source/fact terminals and exact producer node keys.",
            "The node cursor belongs to this original assertion and is owner-controlled.",
            "Each physical fragment contains at most 4096 bytes.",
            "The supplied octet budget bounds the complete next reference explicitly.",
        ),
        invariants=(
            "No source assertion, other logical stream or preceding node is replayed.",
            "Original node keys and exact source/worker coordinates survive reopen.",
            "An oversized, incomplete or corrupt reference never becomes end-of-input.",
            "Native owns documentary grammar; opaque lexical values are not interpreted.",
        ),
        limitations=(
            "This read grants no closure, selection-freeze, causal or access authority.",
            "The closure owner must atomically persist the node with its dependency effects.",
            "Reference resolution, source admission and transaction commits have separate costs.",
            "SQLite VM constants depend on the engine; elapsed time remains an observation.",
        ),
    )


def _require_assertion(db: sqlite3.Connection, assertion: JournalAssertion) -> int:
    key = assertion.key
    _identity(key.task_id)
    _identity(assertion.assertion_id)
    entries = _completed_source(db, JournalTaskKey(key.build_id, key.source_key, key.worker_id))
    row = _one(
        db,
        "SELECT a.task_id,a.born_sequence,a.source_group,a.source_item,"
        "a.origin_entry_json,a.canonical_stream_ref,a.acknowledged,"
        "t.source_key,t.worker_id,t.assertion_id,t.origin_sha256,t.canonical_stream_ref,"
        "t.version,t.open_streams,t.complete,t.terminal_version "
        "FROM discovery_journal_assertions AS a JOIN discovery_fact_tasks AS t "
        "ON t.build_id=a.build_id AND t.task_id=a.task_id "
        "WHERE a.build_id=? AND a.source_key=? AND a.assertion_id=?",
        (key.build_id, key.source_key, assertion.assertion_id),
    )
    if (
        type(assertion.sequence) is not int
        or not 0 <= assertion.sequence < entries
        or type(assertion.item) is not int
        or assertion.item < 0
        or row is None
        or row[:7]
        != (
            key.task_id,
            str(assertion.sequence),
            assertion.group,
            assertion.item,
            assertion.origin_entry_json,
            assertion.canonical_stream_ref,
            1,
        )
        or row[7:10] != (key.source_key, key.worker_id, assertion.assertion_id)
        or row[11] != assertion.canonical_stream_ref
        or row[13:15] != (0, 1)
        or type(row[15]) is not int
        or row[15] < 0
        or row[12] != row[15] + 1
    ):
        raise StaleFactTask("documentary assertion has no exact completed original binding")
    origin = require_canonical_json(assertion.origin_entry_json.encode())
    if not isinstance(origin, dict) or origin.get("json_sha256") != row[10]:
        raise StaleFactTask("documentary assertion changed its original entry identity")
    return int(row[15])


@declare_growth(_declaration())
def next_documentary_reference(
    db: sqlite3.Connection,
    assertion: JournalAssertion,
    *,
    octet_budget: int,
    after: DocumentaryPosition | None = None,
) -> DocumentaryReference | None:
    """Seek one source-qualified reference, verifying its entire segmented value.

    Node keys use the producer's canonical decimal text index order. The owner
    resumes that order directly, without expanding paths or reparsing preceding
    assertions. Capacity failure preserves the same next obligation for retry.
    """
    if type(octet_budget) is not int or octet_budget < 1:
        raise ValueError("documentary execution capacity must be positive octets")
    if after is not None and (
        after.assertion != assertion or type(after.node) is not int or after.node < 0
    ):
        raise StaleFactTask("documentary node cursor belongs to another original assertion")
    terminal = _require_assertion(db, assertion)
    key = assertion.key
    identity = (key.build_id, key.task_id, "documentary-reference")
    row = _one(
        db,
        "SELECT record_key,fragments,octets,last_fragment_bytes,complete,terminal_version "
        "FROM discovery_fact_streams WHERE build_id=? AND task_id=? AND kind=?"
        + (" AND record_key>?" if after is not None else "")
        + " ORDER BY record_key LIMIT 1",
        (*identity, str(after.node)) if after is not None else identity,
    )
    if row is None:
        return None
    record_key, fragments, octets, tail, complete, stream_terminal = row
    try:
        node = int(record_key)
    except (TypeError, ValueError) as error:
        raise FactSegmentConflict("documentary node is not an original decimal key") from error
    if node < 0 or str(node) != record_key:
        raise FactSegmentConflict("documentary node is not an original decimal key")
    if (
        complete != 1
        or type(stream_terminal) is not int
        or not 0 <= stream_terminal < terminal
        or type(octets) is not int
        or octets < 1
        or fragments != (octets + FACT_FRAGMENT_BYTES - 1) // FACT_FRAGMENT_BYTES
        or tail != (octets - 1) % FACT_FRAGMENT_BYTES + 1
    ):
        raise FactSegmentConflict("documentary stream has no exact complete terminal")
    if octets > octet_budget:
        raise DocumentaryReferenceCapacity("next complete documentary reference exceeds capacity")
    cursor = db.execute(
        "SELECT ordinal,exact_bytes,sha256,staged_version FROM discovery_fact_segments "
        "WHERE build_id=? AND task_id=? AND kind=? AND record_key=? "
        "ORDER BY ordinal LIMIT ?",
        (*identity, record_key, fragments + 1),
    )
    parts = []
    previous_version = -1
    try:
        for ordinal in range(fragments):
            part = cursor.fetchone()
            expected_bytes = tail if ordinal == fragments - 1 else FACT_FRAGMENT_BYTES
            if (
                part is None
                or part[0] != ordinal
                or type(part[1]) is not bytes
                or len(part[1]) != expected_bytes
                or type(part[3]) is not int
                or not previous_version < part[3] < stream_terminal
                or hashlib.sha256(part[1]).hexdigest() != part[2]
            ):
                raise FactSegmentConflict("documentary fragment differs from its exact terminal")
            previous_version = part[3]
            parts.append(part[1])
        if cursor.fetchone() is not None:
            raise FactSegmentConflict("documentary stream has excess fragments")
    finally:
        cursor.close()
    raw = b"".join(parts)
    selected = inspect_documentary_node(require_canonical_json(raw)).reference
    if selected is None:
        raise FactSegmentConflict("documentary fact is not a Native external reference")
    return DocumentaryReference(DocumentaryPosition(assertion, node), selected)
