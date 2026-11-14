"""Direct declared-name, profile and edge facts from one admitted assertion.

Grammar positions address original source slots. They do not recursively interpret
opaque profiles, rebuild prior postings, or create a separate build lifecycle.
"""

from __future__ import annotations

import base64
import sqlite3
from dataclasses import dataclass, replace
from typing import Any

from riverhog_canonical_json import canonical_json_bytes
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

from riverhog_core.canonical_discovery_extraction import (
    NameRepresentation,
    declared_name_text,
    name_representations,
)
from riverhog_core.canonical_discovery_fact_work import (
    FactExtractionCursor,
    FactStreamKey,
    FactTaskKey,
    StaleFactTask,
    fact_assertion_binding,
    fact_extraction_cursor,
)
from riverhog_core.canonical_discovery_rows import (
    _ID_EDGES,
    _REFERENCE_EDGES,
    CoreEdge,
    _edge_from_reference,
)
from riverhog_core.canonical_discovery_source_work import (
    AdmittedAssertionSource,
    PreparedSourcePage,
    PreparedSourceStep,
    SourceValue,
    prepare_stream_action,
)


@dataclass(frozen=True, slots=True)
class _Rule:
    path: tuple[str, ...]
    array: bool = False
    suffixes: tuple[tuple[str, ...], ...] = ((),)


@dataclass(frozen=True, slots=True)
class _EdgeRule:
    path: tuple[str, ...]
    kind: str
    role: str
    target_type: str = ""
    array: bool = False


def _name_rules(row: dict[str, Any]) -> tuple[_Rule, ...]:
    rules = [
        _Rule(("evidence",), True, (("source_identifier", "value"),)),
        _Rule(("identifiers",), True, (("value",),)),
        _Rule(("consistency",), False, (("snapshot_identifier", "value"),)),
        _Rule(("metadata",), True, (("name",), ("source", "field"))),
    ]
    if row["type"] == "delivery_association":
        rules.append(_Rule((), False, (("slot",),)))
    if row["type"] == "locator_binding":
        keys = {
            "filesystem_path": ("name",),
            "object_key": ("key", "version"),
            "repository_id": ("identifier", "version"),
            "opaque": ("value",),
        }.get(row["locator"]["kind"], ())
        rules.extend(_Rule(("locator",), False, ((key,),)) for key in keys)
    return tuple(rules)


def _profile_rules(row: dict[str, Any]) -> tuple[_Rule, ...]:
    kind = row["type"]
    if kind == "extension" and row["value"]["type"] == "json":
        return (_Rule(("value", "value")),)
    if kind in {"context", "observation", "reported_description"}:
        return (_Rule(("profiles",), True),)
    if kind == "activity":
        return (_Rule(("details",), True), _Rule(("configuration",)))
    if kind == "locator_binding" and row["locator"]["kind"] == "database_selector":
        return (_Rule(("locator", "selector")),)
    return ()


def _edge_rules(row: dict[str, Any]) -> tuple[_EdgeRule, ...]:
    kind = row["type"]
    rules = [
        _EdgeRule((role,), "id", role, target) for role, target in _ID_EDGES.get(kind, {}).items()
    ]
    rules.extend(_EdgeRule((role,), "reference", role) for role in _REFERENCE_EDGES.get(kind, ()))
    if kind == "activity":
        rules.extend(
            (
                _EdgeRule(("associations",), "association", "association", "agent", True),
                _EdgeRule(("contexts",), "context", "", "context", True),
            )
        )
    if kind == "extension" and row["value"]["type"] == "reference":
        rules.append(_EdgeRule(("value", "value"), "reference", "value"))
    return tuple(rules)


def _control(cursor: FactExtractionCursor, **changes: Any) -> PreparedSourceStep:
    if cursor.value_position or cursor.fragment_ordinal:
        raise StaleFactTask("grammar control cannot abandon an unfinished physical stream")
    return PreparedSourceStep(replace(cursor, **changes))


class DeclaredAssertionSource:
    """Fixed grammar indexes over the same single admitted source atom.

    Initial counting/classification charges a bounded admitted-atom pass. It
    retains numeric name positions and fixed rule prefixes, not all projections.
    Only the current name, profile or edge payload is cached during preparation.
    """

    def __init__(self, source: AdmittedAssertionSource) -> None:
        self.source = source
        self.name_rules = _name_rules(source.row)
        self.profile_rules = _profile_rules(source.row)
        self.edge_rules = _edge_rules(source.row)
        self.name_positions: dict[int, int] = {}
        self.name_count = 0
        for rule in self.name_rules:
            for position in range(self.items(rule)):
                for suffix in rule.suffixes:
                    value = self.location(rule, position, suffix)
                    if value is None or value.value.get("kind") != "bytes":
                        continue
                    if value.node in self.name_positions:
                        raise ValueError("declared name grammar selected a source slot twice")
                    self.name_positions[value.node] = self.name_count
                    exact = base64.b64decode(value.value["bytes"]["data"], validate=True)
                    self.name_count += 2 + int(
                        declared_name_text(exact, value.value["encoding"]) is not None
                    )
        self.profile_prefixes, self.profile_count = self.prefixes(self.profile_rules)
        self.edge_prefixes, self.edge_count = self.prefixes(self.edge_rules)
        lexical = source.lexical_stream_count
        self.category_stream_totals = (
            ("profiles", lexical + 2 * self.name_count),
            ("edges", lexical + 2 * self.name_count + 2 * self.profile_count),
            ("done", lexical + 2 * self.name_count + 2 * self.profile_count + 4 * self.edge_count),
        )
        self._name_node: int | None = None
        self._names: tuple[NameRepresentation, ...] = ()
        self._edge_key: tuple[int, int] | None = None
        self._edge_value: CoreEdge | None = None
        self._payload_key: tuple[str, int, int] | None = None
        self._payload = b""

    def items(self, rule: _Rule | _EdgeRule) -> int:
        try:
            value = self.source.locate(*rule.path)
        except KeyError:
            return 0
        if not rule.array:
            return 1
        if type(value.value) is not list:
            raise TypeError("admitted grammar array has another source type")
        return len(value.value)

    def location(
        self, rule: _Rule | _EdgeRule, position: int, suffix: tuple[str, ...] = ()
    ) -> SourceValue | None:
        tokens: tuple[str | int, ...] = (
            (*rule.path, position, *suffix) if rule.array else (*rule.path, *suffix)
        )
        try:
            return self.source.locate(*tokens)
        except KeyError:
            return None

    def prefixes(
        self, rules: tuple[_Rule, ...] | tuple[_EdgeRule, ...]
    ) -> tuple[tuple[int, ...], int]:
        prefixes, total = [], 0
        for rule in rules:
            prefixes.append(total)
            total += self.items(rule)
        return tuple(prefixes), total

    def names(self, value: SourceValue) -> tuple[NameRepresentation, ...]:
        if value.node != self._name_node:
            self._name_node = value.node
            self._names = tuple(name_representations(value.value))
        return self._names

    def edge(self, rule_index: int, position: int, value: SourceValue) -> CoreEdge:
        key = rule_index, position
        if self._edge_key != key:
            rule = self.edge_rules[rule_index]
            if rule.kind == "id":
                edge = CoreEdge(rule.role, value.value, rule.target_type)
            elif rule.kind == "reference":
                edge = _edge_from_reference(rule.role, value.value)
            elif rule.kind == "association":
                edge = CoreEdge("association", value.value["agent_id"], "agent")
            else:
                edge = CoreEdge(
                    "context:" + value.value["role"], value.value["context_id"], "context"
                )
            self._edge_key, self._edge_value = key, edge
        assert self._edge_value is not None
        return self._edge_value

    def payload(self, category: str, ordinal: int, section: int, value: SourceValue) -> bytes:
        key = category, ordinal, section
        if self._payload_key == key:
            return self._payload
        if category == "names":
            item = self.names(value)[ordinal - self.name_positions[value.node]]
            payload = canonical_json_bytes(
                {
                    "path_node": value.node,
                    "ordinal": ordinal,
                    "representation": item.representation,
                    "source_value_sha256": item.source_value_sha256,
                }
                if section == 0
                else item.value
            )
        elif category == "profiles":
            payload = canonical_json_bytes(
                {
                    "section_node": value.node,
                    "data_node": self.source.locate(
                        *self._profile_tokens(value, ordinal), "data"
                    ).node,
                    "ordinal": ordinal,
                }
                if section == 0
                else value.value["profile"]
            )
        else:
            assert self._edge_value is not None
            edge = self._edge_value
            payload = (
                canonical_json_bytes(
                    {
                        "path_node": value.node,
                        "ordinal": ordinal,
                        "target_type": edge.target_type,
                        "target_scope": edge.target_scope,
                    }
                )
                if section == 0
                else edge.role.encode("utf-8")
                if section == 1
                else edge.target_id.encode("utf-8")
                if section == 2
                else edge.exact_foreign_reference or b""
            )
        self._payload_key, self._payload = key, payload
        return payload

    def _profile_tokens(self, value: SourceValue, ordinal: int) -> tuple[str | int, ...]:
        # The rule count is a fixed native grammar constant, independent of the
        # number of sections. No earlier section or profile payload is replayed.
        for index, rule in enumerate(self.profile_rules):
            position = ordinal - self.profile_prefixes[index]
            if 0 <= position < self.items(rule):
                return (*rule.path, position) if rule.array else rule.path
        raise StaleFactTask("profile ordinal has no exact source rule")

    def handoff(self, cursor: FactExtractionCursor) -> PreparedSourceStep:
        base = self.source.node_count
        expected = {
            "names": base + self.name_count,
            "profiles": base + self.name_count + self.profile_count,
            "edges": base + self.name_count + self.profile_count + self.edge_count,
        }[cursor.category]
        if cursor.record_ordinal != expected:
            raise StaleFactTask("declared records do not cover their admitted source terminal")
        next_category = {"names": "profiles", "profiles": "edges", "edges": "done"}[cursor.category]
        return _control(
            cursor, category=next_category, source_node=0, source_position=0, section_ordinal=0
        )


def _next_slot(cursor: FactExtractionCursor, rule: _Rule, subslot: int) -> FactExtractionCursor:
    if subslot + 1 < len(rule.suffixes):
        return replace(cursor, section_ordinal=(subslot + 1) * 6)
    return replace(cursor, source_position=cursor.source_position + 1, section_ordinal=0)


def prepare_declared_source_step(
    source: DeclaredAssertionSource, cursor: FactExtractionCursor
) -> PreparedSourceStep:
    if cursor.stack_depth or cursor.hash_state:
        raise StaleFactTask("declared source has an unexpected traversal or hash continuation")
    category = cursor.category
    if category not in {"names", "profiles", "edges"}:
        raise ValueError("declared preparation owns name, profile and edge categories")
    rules = (
        source.name_rules
        if category == "names"
        else source.profile_rules
        if category == "profiles"
        else source.edge_rules
    )
    if cursor.source_node == len(rules):
        if cursor.source_position or cursor.section_ordinal:
            raise StaleFactTask("completed grammar still has an unfinished source position")
        return source.handoff(cursor)
    if not 0 <= cursor.source_node < len(rules):
        raise StaleFactTask("source rule is outside the fixed native grammar")
    rule = rules[cursor.source_node]
    count = source.items(rule)
    position = cursor.source_position
    if position == count:
        if cursor.section_ordinal:
            raise StaleFactTask("completed grammar array still has an unfinished record")
        return _control(
            cursor, source_node=cursor.source_node + 1, source_position=0, section_ordinal=0
        )
    if not 0 <= position < count:
        raise StaleFactTask("source item is outside its exact admitted grammar array")
    if category == "names":
        assert isinstance(rule, _Rule)
        subslot, representation, phase = (
            cursor.section_ordinal // 6,
            cursor.section_ordinal % 6 // 2,
            cursor.section_ordinal % 2,
        )
        if not 0 <= subslot < len(rule.suffixes):
            raise StaleFactTask("name subslot is outside its native grammar")
        value = source.location(rule, position, rule.suffixes[subslot])
        if value is None or value.value.get("kind") != "bytes":
            if representation or phase:
                raise StaleFactTask("absent name has an unfinished representation")
            next_cursor = _next_slot(cursor, rule, subslot)
            return _control(
                cursor,
                source_position=next_cursor.source_position,
                section_ordinal=next_cursor.section_ordinal,
            )
        names = source.names(value)
        if not 0 <= representation < len(names):
            raise StaleFactTask("name representation is absent from the exact source value")
        ordinal = source.name_positions[value.node] + representation
        if cursor.record_ordinal != source.source.node_count + ordinal:
            raise StaleFactTask("name cursor skipped or repeated an exact source slot")
        if phase == 0:
            terminal = replace(cursor, section_ordinal=cursor.section_ordinal + 1)
        else:
            terminal = (
                replace(cursor, section_ordinal=cursor.section_ordinal + 1)
                if representation + 1 < len(names)
                else _next_slot(cursor, rule, subslot)
            )
            terminal = replace(terminal, record_ordinal=cursor.record_ordinal + 1)
        kind = ("name-header", "name-value")[phase]
        payload = source.payload(category, ordinal, phase, value)
    else:
        value = source.location(rule, position)
        if value is None:
            raise StaleFactTask("declared source slot is absent")
        phase = cursor.section_ordinal
        kinds: tuple[str, ...]
        if category == "profiles":
            ordinal = source.profile_prefixes[cursor.source_node] + position
            base = source.source.node_count + source.name_count
            kinds = ("profile-header", "profile-pin")
        else:
            ordinal = source.edge_prefixes[cursor.source_node] + position
            base = source.source.node_count + source.name_count + source.profile_count
            source.edge(cursor.source_node, position, value)
            kinds = ("edge-header", "edge-role", "edge-target", "edge-foreign")
        if cursor.record_ordinal != base + ordinal or not 0 <= phase < len(kinds):
            raise StaleFactTask("declared cursor skipped or repeated its exact source ordinal")
        terminal = (
            replace(
                cursor,
                source_position=position + 1,
                section_ordinal=0,
                record_ordinal=cursor.record_ordinal + 1,
            )
            if phase + 1 == len(kinds)
            else replace(cursor, section_ordinal=phase + 1)
        )
        kind = kinds[phase]
        payload = source.payload(category, ordinal, phase, value)
    return prepare_stream_action(cursor, FactStreamKey(kind, str(ordinal)), payload, terminal)


@declare_growth(
    Declaration(
        id="riverhog-server.discovery-declared-source-page.growth/v1",
        authority="riverhog-server",
        subjects=(
            "riverhog_core.canonical_discovery_declared_source_work.prepare_declared_source_page",
        ),
        scope="declared-name-profile-edge-physical-page",
        starts_at="receive one indexed admitted atom and its fixed native grammar index",
        ends_at="return at most the requested prepared actions with no held database cursor",
        dimensions=(
            Dimension("action_budget", COUNT, "Positive physical action budget."),
            Dimension("atom_octets", OCTETS, "Canonical octets in the one admitted assertion."),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                Expression("constant", value=4, unit=COUNT),
                "Two binding and two cursor reads, independent of grammar array extent.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                Expression("constant", value=4, unit=COUNT),
                "Each indexed read returns at most one fixed-shape row.",
            ),
            Bound(
                "prepared_actions",
                COUNT,
                "upper",
                Expression("dimension", name="action_budget"),
                "Each data/end/control action consumes the same page budget.",
            ),
            Bound(
                "prepared_fragment_octets",
                OCTETS,
                "upper",
                Expression(
                    "product",
                    terms=(
                        Expression("constant", value=4096, unit=OCTETS_PER_COUNT),
                        Expression("dimension", name="action_budget"),
                    ),
                ),
                "One representation fragment per prepared data action.",
            ),
            Bound(
                "hashed_octets",
                OCTETS,
                "upper",
                Expression("dimension", name="atom_octets"),
                "Distinct declared-name canonical preimages are disjoint atom subtrees.",
            ),
        ),
        conditions=(
            "One owner-admitted immutable native assertion, with an exact work/source binding.",
            "The fixed grammar index and one-current-value caches persist across pages.",
            "Preparation runs outside the owner write transaction.",
        ),
        invariants=(
            "Saved rule/item/representation positions address original source slots directly.",
            "Metadata name/source-field order is retained within each original record.",
            "Opaque profile data is never a declared name, profile section or graph edge.",
        ),
        limitations=(
            "Initial atom/grammar classification, encoding and backend costs are separate.",
            "The measured SQL/SHA/output metrics do not assert total CPU or memory conformance.",
            "Apply/commit, journal/source-union completeness and publication are excluded.",
        ),
    )
)
def prepare_declared_source_page(
    db: sqlite3.Connection,
    key: FactTaskKey,
    source: DeclaredAssertionSource,
    *,
    action_budget: int,
) -> PreparedSourcePage:
    if db.in_transaction:
        raise ValueError("prepare source work outside the owner write transaction")
    if type(action_budget) is not int or action_budget <= 0:
        raise ValueError("source action budget must be a positive integer")
    atom = source.source
    binding = fact_assertion_binding(db, key)
    if (
        key.source_key != atom.source_key
        or binding.assertion_id != atom.assertion_id
        or binding.origin_sha256 != atom.origin_sha256
        or binding.canonical_stream_ref != atom.canonical_sha256
    ):
        raise StaleFactTask("admitted atom does not match the exact assertion obligation")
    progress, initial = fact_extraction_cursor(db, key)
    cursor = initial
    actions = []
    for _ in range(action_budget):
        if cursor.category not in {"names", "profiles", "edges"}:
            break
        action = prepare_declared_source_step(source, cursor)
        actions.append(action)
        cursor = action.next_cursor
    return PreparedSourcePage(
        key, progress.version, initial, tuple(actions), source.category_stream_totals
    )
