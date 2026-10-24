"""Prepare canonical/scalar facts from one owner-admitted immutable assertion.

The native source owner supplies its exact qualified source and verified entry
identity. Admission, canonicalization and indexing retain at most that one
permitted atom; they do not validate a journal or cache authorization. Prepared
pages hold only their action budget and commit through the fact-work owner.
"""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass, field, replace
from typing import Any

from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    OCTETS_PER_COUNT,
    SCALAR,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)

from riverhog_core.canonical_discovery_fact_work import (
    FACT_FRAGMENT_BYTES,
    FactExtractionCursor,
    FactSegmentWrite,
    FactStackFrame,
    FactStreamKey,
    FactStreamTerminal,
    FactTaskKey,
    FactTaskProgress,
    StaleFactTask,
    advance_assertion,
    fact_assertion_binding,
    fact_extraction_cursor,
    fact_stack_frame,
)


@dataclass(slots=True)
class _Container:
    value: dict[str, Any] | list[Any]
    node: int
    keys: tuple[str, ...] | None
    key_positions: dict[str, int]
    child_nodes: list[int] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.value)

    def child(self, position: int) -> tuple[str, Any]:
        if self.keys is None:
            assert isinstance(self.value, list)
            return str(position), self.value[position]
        assert isinstance(self.value, dict)
        token = self.keys[position]
        return token, self.value[token]


@dataclass(frozen=True, slots=True)
class SourceValue:
    node: int
    parent_node: int | None
    token: str
    value: Any
    container: int | None


class AdmittedAssertionSource:
    """Direct lexical coordinates over one already admitted decoded assertion.

    Container references and numeric child ordinals index the original JSON
    tree, without constructing postings or expanded pointers. Object-key sorting
    follows the existing lexical extractor's Unicode order; canonical JSON has
    its separately owned RFC 8785 order. Reconstruction after process replacement
    charges one current admitted atom, not completed posting generation.
    """

    def __init__(self, source_key: str, origin_sha256: str, row: dict[str, Any]) -> None:
        for source_identity in (source_key, origin_sha256):
            if (
                type(source_identity) is not str
                or len(source_identity) != 64
                or any(char not in "0123456789abcdef" for char in source_identity)
            ):
                raise ValueError("qualified source and entry identities must be lowercase SHA-256")
        self.source_key = source_key
        self.origin_sha256 = origin_sha256
        self.assertion_id: str = row["assertion_id"]
        self.row = row
        self.canonical_json = canonical_json_bytes(row)
        self.canonical_sha256 = hashlib.sha256(self.canonical_json).hexdigest()
        self.containers: list[_Container] = []
        self._container_ids: dict[int, int] = {}
        self._add_container(row, 0)
        pending = [(0, 0)]
        next_node = 1
        while pending:
            identity, position = pending[-1]
            container = self.containers[identity]
            if position == len(container):
                pending.pop()
                continue
            pending[-1] = identity, position + 1
            _, value = container.child(position)
            container.child_nodes.append(next_node)
            next_node += 1
            if type(value) in (dict, list):
                if id(value) in self._container_ids:
                    raise ValueError("source must be an immutable decoded JSON tree")
                child = self._add_container(value, next_node - 1)
                pending.append((child, 0))
            elif value is not None and type(value) not in (str, int, bool):
                raise TypeError("assertion contains a nonportable scalar")
        self.node_count = next_node
        self.scalar_count = next_node - len(self.containers)
        self.lexical_stream_count = 1 + 2 * self.node_count + 2 * self.scalar_count
        self._scalar_node: int | None = None
        self._scalar_bytes = b""
        self._scalar_sha256 = ""
        self._payload_key: tuple[int, int] | None = None
        self._payload_bytes = b""

    def _add_container(self, value: Any, node: int) -> int:
        keys = tuple(sorted(value)) if type(value) is dict else None
        container = _Container(
            value, node, keys, {} if keys is None else {key: i for i, key in enumerate(keys)}
        )
        identity = len(self.containers)
        self._container_ids[id(value)] = identity
        self.containers.append(container)
        return identity

    def child(self, container_id: int, position: int) -> SourceValue:
        container = self.containers[container_id]
        if not 0 <= position < len(container):
            raise ValueError("source child coordinate is outside its admitted container")
        token, value = container.child(position)
        return SourceValue(
            container.child_nodes[position],
            container.node,
            token,
            value,
            self._container_ids[id(value)] if type(value) in (dict, list) else None,
        )

    def locate(self, *tokens: str | int) -> SourceValue:
        """Resolve a declared grammar path, with direct child-coordinate lookups."""
        current = SourceValue(0, None, "", self.row, 0)
        for token in tokens:
            if current.container is None:
                raise KeyError(token)
            container = self.containers[current.container]
            if container.keys is None:
                if type(token) is not int or token < 0:
                    raise KeyError(token)
                position = token
            else:
                if type(token) is not str:
                    raise KeyError(token)
                position = container.key_positions[token]
            current = self.child(current.container, position)
        return current

    def at_cursor(self, cursor: FactExtractionCursor) -> SourceValue:
        if cursor.source_position == 0:
            if cursor.source_node != 0:
                raise ValueError("only the assertion root has source position zero")
            return SourceValue(0, None, "", self.row, 0)
        return self.child(cursor.source_node, cursor.source_position - 1)

    def scalar(self, value: SourceValue) -> tuple[bytes, str]:
        if value.container is not None:
            raise TypeError("container is not a scalar posting")
        if self._scalar_node != value.node:
            self._scalar_node = value.node
            self._scalar_bytes = canonical_json_bytes(value.value)
            self._scalar_sha256 = hashlib.sha256(self._scalar_bytes).hexdigest()
        return self._scalar_bytes, self._scalar_sha256

    def payload(self, value: SourceValue, section: int) -> bytes:
        key = value.node, section
        if self._payload_key == key:
            return self._payload_bytes
        if section == 0:
            payload = canonical_json_bytes(
                {
                    "node": value.node,
                    "parent": value.parent_node,
                    "kind": (
                        "object"
                        if type(value.value) is dict
                        else "array"
                        if type(value.value) is list
                        else "scalar"
                    ),
                }
            )
        elif section == 1:
            payload = value.token.replace("~", "~0").replace("/", "~1").encode("utf-8")
        elif section == 2:
            _, digest = self.scalar(value)
            kind = (
                "null"
                if value.value is None
                else "boolean"
                if type(value.value) is bool
                else "integer"
                if type(value.value) is int
                else "string"
            )
            payload = canonical_json_bytes(
                {
                    "path_node": value.node,
                    "representation": "json-scalar",
                    "json_type": kind,
                    "source_value_sha256": digest,
                }
            )
        elif section == 3:
            payload, _ = self.scalar(value)
        else:
            raise ValueError("unknown lexical stream section")
        self._payload_key, self._payload_bytes = key, payload
        return payload


@dataclass(frozen=True, slots=True)
class PreparedSourceStep:
    next_cursor: FactExtractionCursor
    segment: FactSegmentWrite | None = None
    terminal: FactStreamTerminal | None = None
    stack_action: str = "none"
    stack_frame: FactStackFrame | None = None


@dataclass(frozen=True, slots=True)
class PreparedSourcePage:
    key: FactTaskKey
    expected_version: int
    expected_cursor: FactExtractionCursor
    actions: tuple[PreparedSourceStep, ...]
    category_stream_totals: tuple[tuple[str, int], ...]


def prepare_stream_action(
    cursor: FactExtractionCursor,
    stream: FactStreamKey,
    payload: bytes,
    terminal_cursor: FactExtractionCursor,
) -> PreparedSourceStep:
    offset, ordinal = cursor.value_position, cursor.fragment_ordinal
    if offset != min(ordinal * FACT_FRAGMENT_BYTES, len(payload)):
        raise StaleFactTask("source fragment checkpoint is not an exact physical position")
    if offset < len(payload):
        fragment = payload[offset : offset + FACT_FRAGMENT_BYTES]
        return PreparedSourceStep(
            replace(cursor, value_position=offset + len(fragment), fragment_ordinal=ordinal + 1),
            segment=FactSegmentWrite(stream, ordinal, fragment),
        )
    return PreparedSourceStep(
        replace(terminal_cursor, value_position=0, fragment_ordinal=0),
        terminal=FactStreamTerminal(stream, ordinal, len(payload)),
    )


def prepare_lexical_source_step(
    source: AdmittedAssertionSource,
    cursor: FactExtractionCursor,
    stack_top: FactStackFrame | None,
) -> PreparedSourceStep:
    """Prepare one canonical/scalar fragment or one explicit traversal-stack edit."""
    if cursor.hash_state:
        raise StaleFactTask("lexical source checkpoint has an unknown hash continuation")
    if cursor.category == "canonical":
        if (
            cursor.stack_depth
            or cursor.source_node
            or cursor.source_position
            or cursor.section_ordinal
        ):
            raise StaleFactTask("canonical source has an unexpected traversal coordinate")
        return prepare_stream_action(
            cursor,
            FactStreamKey("canonical", "0"),
            source.canonical_json,
            replace(
                cursor,
                category="scalars",
                source_node=0,
                source_position=0,
                record_ordinal=0,
                section_ordinal=0,
            ),
        )
    if cursor.category != "scalars":
        raise ValueError("lexical preparation owns only canonical and scalar source categories")
    if bool(cursor.stack_depth) != (stack_top is not None):
        raise StaleFactTask("source traversal stack depth has no matching frame")
    if stack_top is not None and (
        not 0 <= stack_top.source_node < len(source.containers)
        or source.containers[stack_top.source_node].node != stack_top.path_node
    ):
        raise StaleFactTask("source stack frame has another exact path coordinate")
    section = cursor.section_ordinal
    if section in (4, 5) and (cursor.value_position or cursor.fragment_ordinal):
        raise StaleFactTask("source control checkpoint still has an unfinished value position")
    if section == 5:
        if stack_top is None:
            if cursor.stack_depth:
                raise StaleFactTask("source traversal has no matching stack frame")
            if cursor.record_ordinal != source.node_count - 1:
                raise StaleFactTask("source traversal does not cover its admitted node terminal")
            return PreparedSourceStep(
                replace(
                    cursor,
                    category="names",
                    record_ordinal=source.node_count,
                    source_node=0,
                    source_position=0,
                    section_ordinal=0,
                    value_position=0,
                    fragment_ordinal=0,
                )
            )
        container = source.containers[stack_top.source_node]
        if stack_top.next_child < len(container):
            child = source.child(stack_top.source_node, stack_top.next_child)
            return PreparedSourceStep(
                replace(
                    cursor,
                    source_node=stack_top.source_node,
                    source_position=stack_top.next_child + 1,
                    record_ordinal=child.node,
                    section_ordinal=0,
                ),
                stack_action="replace",
                stack_frame=replace(stack_top, next_child=stack_top.next_child + 1),
            )
        if stack_top.next_child != len(container):
            raise StaleFactTask("source child checkpoint exceeds its admitted container")
        return PreparedSourceStep(
            replace(cursor, stack_depth=cursor.stack_depth - 1), stack_action="pop"
        )
    value = source.at_cursor(cursor)
    if cursor.source_position and (
        stack_top is None
        or stack_top.source_node != cursor.source_node
        or stack_top.next_child != cursor.source_position
    ):
        raise StaleFactTask("source value is not the exact consumed child of its saved frame")
    if value.node != cursor.record_ordinal:
        raise StaleFactTask("source coordinate changed its lexical ordinal")
    if section == 4:
        if value.container is None:
            raise StaleFactTask("source traversal cannot push a scalar")
        return PreparedSourceStep(
            replace(cursor, section_ordinal=5, stack_depth=cursor.stack_depth + 1),
            stack_action="push",
            stack_frame=FactStackFrame(value.container, 0, value.node),
        )
    if section not in (0, 1, 2, 3):
        raise StaleFactTask("unknown lexical source section")
    kind = ("path-node", "path-token", "scalar-header", "scalar-value")[section]
    next_section = section + 1
    if section == 1 and value.container is not None:
        next_section = 4
    elif section == 3:
        next_section = 5
    return prepare_stream_action(
        cursor,
        FactStreamKey(kind, str(value.node)),
        source.payload(value, section),
        replace(cursor, section_ordinal=next_section),
    )


def _page_declaration() -> Declaration:
    budget = Expression("dimension", name="action_budget")
    accesses = Expression(
        "sum",
        terms=(
            Expression("constant", value=4, unit=COUNT),
            Expression(
                "product",
                terms=(Expression("constant", value=3, unit=SCALAR), budget),
            ),
        ),
    )
    return Declaration(
        id="riverhog-server.discovery-source-page.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_source_work.prepare_lexical_source_page",),
        scope="canonical-and-scalar-prepared-physical-page",
        starts_at="receive one indexed admitted atom and its exact durable source obligation",
        ends_at="return at most the requested prepared actions with detached frame reads",
        dimensions=(
            Dimension("action_budget", COUNT, "Positive physical action budget for this call."),
            Dimension(
                "atom_octets", OCTETS, "Exact canonical octets in the one admitted assertion."
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                accesses,
                "Four binding/cursor reads plus at most three point reads per touched frame.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                accesses,
                "Every SELECT returns at most its one exact bound row.",
            ),
            Bound(
                "prepared_actions",
                COUNT,
                "upper",
                budget,
                "Data, terminal and control transitions consume the same action budget.",
            ),
            Bound(
                "prepared_fragment_octets",
                OCTETS,
                "upper",
                Expression(
                    "product",
                    terms=(
                        Expression("constant", value=4096, unit=OCTETS_PER_COUNT),
                        budget,
                    ),
                ),
                "No prepared data action binds more than one representation fragment.",
            ),
            Bound(
                "hashed_octets",
                OCTETS,
                "upper",
                Expression("dimension", name="atom_octets"),
                "Scalar SHA preimages are a subset of this atom's exact scalar bytes.",
            ),
        ),
        conditions=(
            "The owner supplies an admitted immutable JSON tree without container aliases.",
            "Retain the atom index across pages; reconstruct the current atom on restart.",
            "Preparation runs outside the owner's write transaction.",
        ),
        invariants=(
            "Resume source/value positions and point-addressed stack frames.",
            "Completed postings are not generated again to skip to the saved source position.",
            "Paths share parent/token identities instead of expanded pointer copies.",
        ),
        limitations=(
            "Atom admission, canonicalization and initial structural indexing are separate costs.",
            "SQL, SHA preimages and physical output are measured; total CPU/I/O is excluded.",
            "Encoding, backend access, commit and source completeness are separate costs.",
            "Only canonical/scalar categories are prepared by this owner.",
        ),
    )


@declare_growth(_page_declaration())
def prepare_lexical_source_page(
    db: sqlite3.Connection,
    key: FactTaskKey,
    source: AdmittedAssertionSource,
    *,
    action_budget: int,
) -> PreparedSourcePage:
    """Detach a bounded page, loading only saved frames actually touched by it.

    Source encoding and hashing happen before the caller opens its write
    transaction. A virtual stack caches only this page's touched frames. A new
    process reconstructs at most its current admitted source atom, then starts
    directly at the durable source/value/frame position.
    """
    if db.in_transaction:
        raise ValueError("prepare source work outside the owner write transaction")
    if type(action_budget) is not int or action_budget <= 0:
        raise ValueError("source action budget must be a positive integer")
    binding = fact_assertion_binding(db, key)
    if (
        key.source_key != source.source_key
        or binding.assertion_id != source.assertion_id
        or binding.origin_sha256 != source.origin_sha256
        or binding.canonical_stream_ref != source.canonical_sha256
    ):
        raise StaleFactTask("admitted atom does not match the exact assertion obligation")
    progress, initial = fact_extraction_cursor(db, key)
    cursor = initial
    frames: dict[int, FactStackFrame] = {}
    actions = []
    for _ in range(action_budget):
        if cursor.category not in {"canonical", "scalars"}:
            break
        top = None
        if cursor.stack_depth:
            top = frames.get(cursor.stack_depth)
            if top is None:
                top = fact_stack_frame(db, key, depth=cursor.stack_depth)
                frames[cursor.stack_depth] = top
        action = prepare_lexical_source_step(source, cursor, top)
        if action.next_cursor.record_ordinal < cursor.record_ordinal:
            raise StaleFactTask("source traversal would repeat a completed lexical ordinal")
        if action.stack_action in {"push", "replace"}:
            assert action.stack_frame is not None
            frames[action.next_cursor.stack_depth] = action.stack_frame
        elif action.stack_action == "pop":
            frames.pop(cursor.stack_depth, None)
        actions.append(action)
        cursor = action.next_cursor
    return PreparedSourcePage(
        key,
        progress.version,
        initial,
        tuple(actions),
        (("scalars", 1), ("names", source.lexical_stream_count)),
    )


def apply_source_page(
    db: sqlite3.Connection, key: FactTaskKey, page: PreparedSourcePage
) -> FactTaskProgress:
    """Commit prepared facts, frames and source-category terminals under one owner CAS."""
    if not db.in_transaction:
        raise ValueError("source mutation requires an owner-managed transaction")
    if key != page.key:
        raise StaleFactTask("prepared source page belongs to another exact task")
    db.execute("SAVEPOINT discovery_source_page")
    try:
        progress, cursor = fact_extraction_cursor(db, key)
        if progress.version != page.expected_version or cursor != page.expected_cursor:
            raise StaleFactTask("prepared source page has a stale checkpoint")
        expected_totals = dict(page.category_stream_totals)
        previous_category = cursor.category
        for action in page.actions:
            progress = advance_assertion(
                db,
                key,
                expected_version=progress.version,
                next_cursor=action.next_cursor,
                segment=action.segment,
                terminal=action.terminal,
                stack_action=action.stack_action,
                stack_frame=action.stack_frame,
            )
            current_category = action.next_cursor.category
            if current_category != previous_category:
                expected_streams = expected_totals.get(current_category)
                if expected_streams is None or progress.streams != expected_streams:
                    raise StaleFactTask("fact streams do not cover the admitted source terminal")
            previous_category = current_category
    except BaseException:
        db.execute("ROLLBACK TO discovery_source_page")
        db.execute("RELEASE discovery_source_page")
        raise
    else:
        db.execute("RELEASE discovery_source_page")
    return progress
