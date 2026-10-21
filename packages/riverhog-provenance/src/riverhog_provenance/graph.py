"""Executable application constraints for the formal, attributable assertion graph.

This validates declared semantics, not historical truth or cryptographic witness
trust. It does not pretend to implement every entailment of general PROV-Constraints.
"""

from __future__ import annotations

import base64
import binascii
import copy
import hashlib
import json
import re
from collections import defaultdict
from collections.abc import Iterable, Mapping, MutableMapping, MutableSequence, MutableSet, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol, cast

from riverhog_provenance_contracts import (
    CATEGORY_TYPES,
    PROFILE,
    ContractCatalog,
    canonical_document,
)

from ._observation_coverage import ObservationCoverage
from ._semantic_program import ROW_TYPES
from .common import fingerprint
from .errors import ProvenanceValidationError

ENTITY_TYPES = frozenset(
    {"artifact", "occurrence", "state", "context", "observation", "reported_description"}
)
IDENTITY_TYPES = frozenset({"artifact", "occurrence", "state", "context", "agent"})


def time_ns(value: str) -> int:
    """Calendar-valid UTC comparison without microsecond truncation."""
    match = re.fullmatch(
        r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?Z",
        value,
    )
    if match is None:
        raise ProvenanceValidationError("noncanonical UTC instant")
    try:
        instant = datetime.strptime(match[1], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=UTC)
    except ValueError as exc:
        raise ProvenanceValidationError("invalid calendar instant") from exc
    delta = instant - datetime(1970, 1, 1, tzinfo=UTC)
    return (delta.days * 86400 + delta.seconds) * 1_000_000_000 + int(
        (match[2] or "").ljust(9, "0")
    )


def temporal_scope(value: Mapping[str, Any]) -> None:
    if value["kind"] == "instant":
        time_ns(value["at"])
    elif value["kind"] == "interval":
        for field in ("start", "end"):
            if field in value:
                time_ns(value[field])
        if "start" in value and "end" in value and time_ns(value["start"]) >= time_ns(value["end"]):
            raise ProvenanceValidationError("validity interval must have start < end")


def iter_assertions(graph: Mapping[str, Any]) -> Iterable[tuple[str, dict[str, Any]]]:
    for category in CATEGORY_TYPES:
        for item in graph.get(category, []):
            yield category, item


def graph_from_assertions(items: Iterable[tuple[str, Mapping[str, Any]]]) -> dict[str, Any]:
    graph: dict[str, Any] = {}
    for category, item in items:
        graph.setdefault(category, []).append(copy.deepcopy(dict(item)))
    # Arrays are semantic sets; this materialization has an explicit stable order.
    for rows in graph.values():
        rows.sort(key=lambda row: row["assertion_id"])
    return graph


def _name_identity(value: Mapping[str, Any]) -> dict[str, Any]:
    if value["kind"] == "text":
        return {"kind": "text", "text": value["text"]}
    return {"kind": "bytes", "encoding": value["encoding"], "bytes": value["bytes"]}


def _source_identity(value: Mapping[str, Any]) -> dict[str, Any]:
    return {**value, "field": _name_identity(value["field"])}


def _acyclic(edges: Iterable[tuple[str, str]], label: str) -> None:
    successors: dict[str, set[str]] = defaultdict(set)
    indegree: dict[str, int] = {}
    for source, target in edges:
        if source == target:
            raise ProvenanceValidationError(f"{label} cannot relate an entity to itself")
        indegree.setdefault(source, 0)
        indegree.setdefault(target, 0)
        if target not in successors[source]:
            successors[source].add(target)
            indegree[target] += 1
    ready = [node for node, degree in indegree.items() if degree == 0]
    consumed = 0
    while ready:
        current = ready.pop()
        consumed += 1
        for target in successors[current]:
            indegree[target] -= 1
            if indegree[target] == 0:
                ready.append(target)
    if consumed != len(indegree):
        raise ProvenanceValidationError(f"{label} graph contains a cycle")


class GraphStore(Protocol):
    @property
    def view(self) -> Mapping[str, Sequence[dict[str, Any]]]: ...

    @property
    def objects(self) -> Mapping[str, dict[str, Any]]: ...

    @property
    def external_references(self) -> Sequence[dict[str, Any]]: ...

    @property
    def unresolved_profiles(self) -> Sequence[dict[str, str]]: ...

    @property
    def findings(self) -> Sequence[str]: ...

    def iter_assertions(self) -> Iterable[tuple[str, dict[str, Any]]]: ...


@dataclass(frozen=True, slots=True)
class GraphValidation:
    """Isolated materialized view plus explicit unresolved dependencies/findings."""

    _graph_json: bytes
    _externals_json: bytes
    _profiles_json: bytes
    _findings: tuple[str, ...] = ()
    _store: GraphStore | None = None

    @property
    def graph(self) -> dict[str, Any]:
        if self._store is not None:
            return graph_from_assertions(self._store.iter_assertions())
        return cast(dict[str, Any], json.loads(self._graph_json))

    @property
    def view(self) -> Mapping[str, Sequence[dict[str, Any]]]:
        """Read effective categories without materializing the entire snapshot."""
        return self.graph if self._store is None else self._store.view

    @property
    def external_references(self) -> Sequence[dict[str, Any]]:
        if self._store is not None:
            return self._store.external_references
        return tuple(json.loads(self._externals_json))

    @property
    def unresolved_profiles(self) -> Sequence[dict[str, str]]:
        if self._store is not None:
            return self._store.unresolved_profiles
        return tuple(json.loads(self._profiles_json))

    @property
    def profiles_verified(self) -> bool:
        return not self.unresolved_profiles

    @property
    def objects(self) -> Mapping[str, dict[str, Any]]:
        if self._store is not None:
            return self._store.objects
        return {row["id"]: row for _, row in iter_assertions(self.graph)}

    @property
    def findings(self) -> Sequence[str]:
        return self._findings if self._store is None else self._store.findings

    def iter_assertions(self) -> Iterable[tuple[str, dict[str, Any]]]:
        if self._store is not None:
            return self._store.iter_assertions()
        return iter_assertions(self.graph)


class _Validator:
    def __init__(
        self,
        graph: Mapping[str, Any],
        *,
        catalog: ContractCatalog,
        journal_id: str | None,
        require_profiles: bool,
    ) -> None:
        self.graph = graph
        self.catalog = catalog
        self.journal_id = journal_id
        self.require_profiles = require_profiles
        self.objects: Mapping[str, dict[str, Any]] = {}
        self.assertions: MutableSet[str] = set()
        self.externals: MutableMapping[bytes, dict[str, Any]] = {}
        self.profiles: MutableMapping[bytes, dict[str, str]] = {}
        self.findings: MutableSequence[str] = []
        self.capture_targets: MutableMapping[str, str] = {}
        self._observer_agents: dict[str, set[str]] = {}
        self.semantic_edges: dict[str, Any] = {"derivation": [], "specialization": []}
        self.binding_ends: MutableSet[str] = set()
        self.delivery_slots: MutableSet[tuple[str, bytes]] = set()
        self.lifecycle: dict[str, MutableMapping[str, dict[str, Any]]] = {
            "generation": {},
            "invalidation": {},
        }
        for _, row in iter_assertions(graph):
            if row["id"] in self.objects:
                raise ProvenanceValidationError("duplicate effective entity/record identity")
            if row["assertion_id"] in self.assertions:
                raise ProvenanceValidationError("duplicate assertion identity")
            self.objects[row["id"]] = row
            self.assertions.add(row["assertion_id"])
        if self.assertions & self.objects.keys():
            raise ProvenanceValidationError(
                "assertion and entity identity domains must be distinct"
            )

    def node(self, identity: str, *types: str) -> dict[str, Any]:
        value = self.objects.get(identity)
        if value is None:
            raise ProvenanceValidationError(f"unresolved local identity: {identity}")
        return self._node_type(value, *types)

    def _graph_edge(self, kind: str, source: str, target: str) -> None:
        self.semantic_edges[kind].append((source, target))

    def _comparison_node(self, identity: str) -> dict[str, Any]:
        return self.node(identity, "observation", "reported_description")

    def _relation_node(self, identity: str, expected: str) -> dict[str, Any]:
        return self.node(identity, expected)

    def _lifecycle_activity(self, identity: str) -> dict[str, Any]:
        return self.node(identity, "activity")

    def _binding_time(self, identity: str) -> str | None:
        scope = self.node(identity, "locator_binding")["temporal_scope"]
        return cast(str | None, scope.get("at", scope.get("start")))

    def _description_node(self, identity: str, *types: str) -> dict[str, Any]:
        return self.node(identity, *types)

    def _observation_node(self, identity: str, expected: str) -> dict[str, Any]:
        return self.node(identity, expected)

    def _observation_observer(self, identity: str, agent_id: str) -> bool:
        if identity not in self._observer_agents:
            capture = self.node(identity, "activity")
            self._observer_agents[identity] = {item["agent_id"] for item in capture["associations"]}
        return agent_id in self._observer_agents[identity]

    def _observation_coverage_status(
        self, coverage: ObservationCoverage, item: Mapping[str, Any]
    ) -> str | None:
        return coverage.status(item)

    def _delivery_context(self, identity: str) -> dict[str, Any]:
        return self.node(identity, "context")

    def _locator_observation(self, identity: str) -> dict[str, Any]:
        return self.node(identity, "observation")

    def _delivery_observation(self, identity: str) -> dict[str, Any]:
        return self.node(identity, "observation")

    @staticmethod
    def _node_type(value: dict[str, Any], *types: str) -> dict[str, Any]:
        if types and value["type"] not in types:
            raise ProvenanceValidationError(
                f"wrong reference type: {value['type']}; expected {types}"
            )
        return value

    def reference(self, value: Mapping[str, Any], *types: str) -> dict[str, Any] | None:
        if types and value["object_type"] not in types:
            raise ProvenanceValidationError("reference declares an incompatible object type")
        if value["scope"] == "local":
            return self.node(value["object_id"], value["object_type"])
        if value["journal_id"] == self.journal_id:
            raise ProvenanceValidationError(
                "an external reference cannot point into its own journal"
            )
        if value["object_id"] in self.objects:
            self.node(value["object_id"], value["object_type"])
        self.externals[canonical_document(value)] = dict(value)
        return None

    def profile(self, value: Mapping[str, Any]) -> None:
        from ._semantic_program import walk

        walk(self, "profile", value)

    def filesystem_values(self, value: Any) -> None:
        from ._semantic_program import walk

        walk(self, "filesystem", value)

    def source(self, value: Mapping[str, Any]) -> None:
        from ._semantic_program import walk

        walk(self, "source", value)

    def bytes(self, value: Mapping[str, Any]) -> bytes:
        try:
            raw = base64.b64decode(value["data"], validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ProvenanceValidationError("invalid base64") from exc
        if base64.b64encode(raw).decode("ascii") != value["data"]:
            raise ProvenanceValidationError("noncanonical base64 padding bits")
        if len(raw) != int(value["byte_length"]):
            raise ProvenanceValidationError("native byte length does not match evidence")
        for item in value.get("digests", []):
            name = {"sha-256": "sha256", "sha-512": "sha512"}.get(item["algorithm"])
            if name and hashlib.new(name, raw).hexdigest() != item["value"]:
                raise ProvenanceValidationError("native evidence digest mismatch")
        return raw

    def name(self, value: Mapping[str, Any]) -> None:
        if value["kind"] == "bytes":
            self.bytes(value["bytes"])

    def identifier(self, value: Mapping[str, Any]) -> None:
        from ._semantic_program import walk

        walk(self, "identifier", value)

    def content(self, value: Mapping[str, Any]) -> None:
        from ._semantic_program import walk

        walk(self, "content", value)

    def typed(self, value: Mapping[str, Any]) -> None:
        from ._semantic_program import walk

        walk(self, "typed", value)

    def native(self, value: Mapping[str, Any]) -> None:
        from ._semantic_program import walk

        walk(self, "native", value)

    def generic(self, value: Mapping[str, Any]) -> None:
        from ._semantic_program import walk

        walk(self, "generic", value)

    def row_semantics(self, value: Mapping[str, Any]) -> None:
        from ._semantic_program import walk

        walk(self, "row", value)

    def aggregates(self) -> tuple[Any, ...]:
        return {}, {}, [], [], {}, set(), set()

    def validation_rows(self) -> Iterable[dict[str, Any]]:
        return self.objects.values()

    def run(self) -> None:
        (
            generation,
            invalidation,
            derivations,
            specializations,
            capture_targets,
            ends,
            slots,
        ) = self.aggregates()
        self.lifecycle = {"generation": generation, "invalidation": invalidation}
        self.semantic_edges = {"derivation": derivations, "specialization": specializations}
        self.capture_targets = capture_targets
        self.binding_ends = ends
        self.delivery_slots = slots
        for row in self.validation_rows():
            self.generic(row)
            kind = row["type"]
            if kind in ROW_TYPES:
                self.row_semantics(row)
        self.finish(generation, invalidation, derivations, specializations)

    def finish(
        self,
        generation: Mapping[str, Any],
        invalidation: Mapping[str, Any],
        derivations: Iterable[tuple[str, str]],
        specializations: Iterable[tuple[str, str]],
    ) -> None:
        _acyclic(derivations, "derivation")
        _acyclic(specializations, "specialization")
        self._lifecycle_times(generation, invalidation)
        self._address_completeness()
        self._evidence_conflicts()

    @staticmethod
    def _within_activity(at: str, activity: Mapping[str, Any]) -> None:
        if "started_at" in activity and time_ns(at) < time_ns(activity["started_at"]):
            raise ProvenanceValidationError("event precedes activity start")
        if "ended_at" in activity and time_ns(at) > time_ns(activity["ended_at"]):
            raise ProvenanceValidationError("event follows activity end")

    @staticmethod
    def _unique_lifecycle(
        index: MutableMapping[str, dict[str, Any]], row: dict[str, Any], name: str
    ) -> None:
        key = row["state"]["object_id"]
        previous = index.get(key)
        if previous and (
            previous["activity_id"] != row["activity_id"] or previous.get("at") != row.get("at")
        ):
            raise ProvenanceValidationError(f"conflicting known {name} events for one state")
        index[key] = row

    def _lifecycle_times(
        self, generation: Mapping[str, Any], invalidation: Mapping[str, Any]
    ) -> None:
        for row in self.objects.values():
            if row["type"] not in ("usage", "invalidation") or "at" not in row:
                continue
            key = row["state"]["object_id"]
            generated = generation.get(key)
            if generated and "at" in generated and time_ns(row["at"]) < time_ns(generated["at"]):
                raise ProvenanceValidationError("state used or invalidated before known generation")
            ended = invalidation.get(key)
            if (
                row["type"] == "usage"
                and ended
                and "at" in ended
                and time_ns(row["at"]) > time_ns(ended["at"])
            ):
                raise ProvenanceValidationError("state used after known invalidation")

    def _address_completeness(self) -> None:
        linked = {
            r["observation_id"]
            for r in self.objects.values()
            if r["type"] == "locator_binding" and "observation_id" in r
        }
        for row in self.objects.values():
            if (
                row["type"] == "observation"
                and row["address_status"] == "known"
                and row["id"] not in linked
            ):
                raise ProvenanceValidationError(
                    "known address requires an observed locator binding"
                )

    def _evidence_conflicts(self) -> None:
        # Conflicting reports are evidence about a claim, not a reason to discard
        # that historical testimony. Findings never choose a latest or true report.
        by_state: dict[str, set[tuple[int, str]]] = defaultdict(set)
        for row in self.objects.values():
            if row["type"] in ("observation", "reported_description") and "content" in row:
                by_state[row["state"]["object_id"]].add(fingerprint(row["content"]))
        for state, claims in by_state.items():
            if len(claims) > 1:
                self.findings.append(
                    f"{state}: conflicting primary-content descriptions; no value selected"
                )


def validate_graph(
    graph: Mapping[str, Any],
    *,
    catalog: ContractCatalog | None = None,
    journal_id: str | None = None,
    require_profiles: bool = True,
) -> GraphValidation:
    """Validate a complete effective graph, not a fragment with unresolved locals."""
    selected = catalog or ContractCatalog()
    try:
        selected.validate(PROFILE + "/graph-fragment.schema.json", dict(graph))
        validator = _Validator(
            graph, catalog=selected, journal_id=journal_id, require_profiles=require_profiles
        )
        validator.run()
    except ProvenanceValidationError:
        raise
    except (ValueError, TypeError, KeyError, OverflowError) as exc:
        raise ProvenanceValidationError(str(exc)) from exc
    return GraphValidation(
        canonical_document(graph),
        json.dumps(list(validator.externals.values()), sort_keys=True).encode(),
        json.dumps(list(validator.profiles.values()), sort_keys=True).encode(),
        tuple(validator.findings),
    )
