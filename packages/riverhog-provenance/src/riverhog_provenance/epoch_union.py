"""Bounded private epoch-union deltas, without semantic acceptance authority."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from riverhog_growth_contracts import (
    COUNT,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)
from riverhog_provenance_contracts import require_canonical_uuid_urn

EPOCH_UNION_ALGORITHM = "riverhog-journal-epoch-union/v1"
GRAPH_KINDS = ("derivation", "specialization")
ArcKey = tuple[str, str, str]
VertexKey = tuple[str, str]


@dataclass(frozen=True, slots=True)
class TopologyOccurrence:
    assertion_id: str
    kind: str
    source_id: str
    target_id: str
    frame_ordinal: int
    item_ordinal: int

    def checked(self) -> TopologyOccurrence:
        if self.kind not in GRAPH_KINDS:
            raise ValueError("unknown private topology graph kind")
        for value in (self.assertion_id, self.source_id, self.target_id):
            require_canonical_uuid_urn(value)
        if any(
            type(value) is not int or value < 0 for value in (self.frame_ordinal, self.item_ordinal)
        ):
            raise ValueError("invalid topology occurrence position")
        return self


@dataclass(frozen=True, slots=True)
class EpochUnionDelta:
    assertions: tuple[TopologyOccurrence, ...]
    arcs: tuple[ArcKey, ...]
    vertices: tuple[tuple[VertexKey, int], ...]
    counts: dict[str, tuple[int, int]]


@declare_growth(
    Declaration(
        id="riverhog-provenance.epoch-union-delta.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance.epoch_union.plan_epoch_union_additions",),
        scope="one-bounded-private-epoch-union-addition-delta",
        starts_at="receive detached occurrences and their indexed point observations",
        ends_at="return only changed unique arcs, endpoint degrees and documentary insertions",
        dimensions=(Dimension("occurrences", COUNT, "Documentary edges admitted by this unit."),),
        bounds=(
            Bound(
                "visited_occurrences",
                COUNT,
                "equal",
                Expression("dimension", name="occurrences"),
                "Each supplied occurrence is visited once.",
            ),
            Bound(
                "returned_arcs",
                COUNT,
                "upper",
                Expression("dimension", name="occurrences"),
                "Parallel documentary assertions do not create duplicate arcs.",
            ),
            Bound(
                "degree_lookups",
                COUNT,
                "upper",
                Expression(
                    "product",
                    terms=(
                        Expression("constant", value=2),
                        Expression("dimension", name="occurrences"),
                    ),
                ),
                "Only the endpoints of newly unique arcs need degree observations.",
            ),
            Bound(
                "returned_vertices",
                COUNT,
                "upper",
                Expression(
                    "product",
                    terms=(
                        Expression("constant", value=2),
                        Expression("dimension", name="occurrences"),
                    ),
                ),
                "Only changed or newly declared endpoints are returned.",
            ),
            Bound(
                "returned_assertions",
                COUNT,
                "upper",
                Expression("dimension", name="occurrences"),
                "Carried assertions are borrowed; new documentary identities remain separate.",
            ),
        ),
        conditions=(
            "At most 64 occurrences and their complete bounded point observations are supplied.",
            "Identity keys use the native canonical UUID domain; "
            "the owner fences all observations.",
        ),
        invariants=(
            "Degree changes exactly once for a unique graph-kind/source/target arc.",
            "No speculative graph fact accepts a semantic prefix or a correction reference.",
        ),
        limitations=(
            "Indexed reads, durable publication, correction retirement, carry traversal, "
            "retries and reclamation are separate costs.",
            "Key bytes, physical allocation/CPU and native graph/schema replay "
            "are not measured here.",
        ),
    )
)
def plan_epoch_union_additions(
    occurrences: tuple[TopologyOccurrence, ...],
    *,
    known_assertions: set[str],
    existing_arcs: set[ArcKey],
    degrees: Mapping[VertexKey, int | None],
    counts: Mapping[str, tuple[int, int]],
    carried: bool,
) -> EpochUnionDelta:
    if len(occurrences) > 64:
        raise ValueError("private epoch addition budget exceeded")
    seen = set(known_assertions)
    added_arcs: set[ArcKey] = set()
    changed: dict[VertexKey, int] = {}
    updated = dict(counts)
    for edge in occurrences:
        edge.checked()
        if not carried:
            if edge.assertion_id in seen:
                raise ValueError("duplicate private documentary edge identity")
            seen.add(edge.assertion_id)
        arc = (edge.kind, edge.source_id, edge.target_id)
        if arc in existing_arcs or arc in added_arcs:
            continue
        added_arcs.add(arc)
        vertex_count, arc_count = updated[edge.kind]
        for vertex in dict.fromkeys(((edge.kind, edge.source_id), (edge.kind, edge.target_id))):
            if vertex not in changed:
                previous = degrees[vertex]
                if previous is None:
                    vertex_count += 1
                    changed[vertex] = 0
                else:
                    changed[vertex] = previous
        changed[(edge.kind, edge.target_id)] += 1
        updated[edge.kind] = (vertex_count, arc_count + 1)
    return EpochUnionDelta(
        () if carried else occurrences,
        tuple(sorted(added_arcs)),
        tuple(sorted(changed.items())),
        updated,
    )
