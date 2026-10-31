"""Source-fenced, bounded private ranking of retained real epoch unions."""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
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
from riverhog_provenance.epoch_rank import EPOCH_RANK_ALGORITHM, decrement_rank_degrees
from riverhog_provenance.epoch_union import GRAPH_KINDS
from riverhog_provenance_contracts import require_canonical_uuid_urn
from sqlalchemy import Select, select, update
from sqlalchemy.orm import Session
from state_schema import read_snapshot

from riverhog_core.catalog_db import SessionFactory
from riverhog_core.catalog_models import CollectionUploadProvenanceEpochArcRecord as Arc
from riverhog_core.catalog_models import CollectionUploadProvenanceEpochGraphRecord as Graph
from riverhog_core.catalog_models import CollectionUploadProvenanceEpochRankGraphRecord as Result
from riverhog_core.catalog_models import CollectionUploadProvenanceEpochRankVertexRecord as Ranked
from riverhog_core.catalog_models import CollectionUploadProvenanceEpochScanRecord as Scan
from riverhog_core.catalog_models import CollectionUploadProvenanceEpochVertexRecord as Vertex
from riverhog_core.catalog_models import CollectionUploadProvenanceJournalRecord as Journal

from . import collection_uploads as source_owner
from . import upload_journal_union as union

_BUDGET = 64


@dataclass(frozen=True, slots=True)
class _Header:
    epoch: int = 0
    kind: str = GRAPH_KINDS[0]
    phase: str = "graph"
    seed_after: str | None = None
    active: str | None = None
    edge_after: str | None = None
    vertices: int = 0
    arcs: int = 0
    seeded: int = 0
    settled: int = 0
    visited: int = 0
    ready: int = 0
    last_vertex: tuple[int, str, str, int, int | None, bool] | None = None
    last_arc: tuple[int, str, str, str] | None = None
    last_graph: tuple[int, str, str, int, int, int, int] | None = None


def _encode(header: _Header) -> str:
    value = {name: getattr(header, name) for name in _Header.__dataclass_fields__}
    for name in ("epoch", "vertices", "arcs", "seeded", "settled", "visited", "ready"):
        value[name] = format_scalar("nonnegative", value[name])
    if header.last_vertex is not None:
        epoch, kind, vertex, degree, rank, ready = header.last_vertex
        value["last_vertex"] = [
            format_scalar("nonnegative", epoch),
            kind,
            vertex,
            format_scalar("nonnegative", degree),
            None if rank is None else format_scalar("nonnegative", rank),
            ready,
        ]
    if header.last_arc is not None:
        value["last_arc"] = [format_scalar("nonnegative", header.last_arc[0]), *header.last_arc[1:]]
    if header.last_graph is not None:
        epoch, kind, state, *counts = header.last_graph
        value["last_graph"] = [
            format_scalar("nonnegative", epoch),
            kind,
            state,
            *(format_scalar("nonnegative", n) for n in counts),
        ]
    return canonical_json_bytes(value).decode()


def _decode(raw: str) -> _Header:
    try:
        value = json.loads(raw)
        if not isinstance(value, dict) or set(value) != set(_Header.__dataclass_fields__):
            raise ValueError("invalid private rank checkpoint shape")
        if value["kind"] not in GRAPH_KINDS or value["phase"] not in {
            "graph",
            "seed",
            "pop",
            "edges",
        }:
            raise ValueError("invalid private rank phase")
        for name in ("epoch", "vertices", "arcs", "seeded", "settled", "visited", "ready"):
            value[name] = parse_scalar("nonnegative", value[name])
        for name in ("seed_after", "active", "edge_after"):
            if value[name] is not None:
                value[name] = require_canonical_uuid_urn(value[name])
        last = value["last_vertex"]
        if last is not None:
            if (
                not isinstance(last, list)
                or len(last) != 6
                or last[1] not in GRAPH_KINDS
                or type(last[5]) is not bool
            ):
                raise ValueError("invalid private rank vertex marker")
            value["last_vertex"] = (
                parse_scalar("nonnegative", last[0]),
                last[1],
                require_canonical_uuid_urn(last[2]),
                parse_scalar("nonnegative", last[3]),
                None if last[4] is None else parse_scalar("nonnegative", last[4]),
                last[5],
            )
        arc = value["last_arc"]
        if arc is not None:
            if not isinstance(arc, list) or len(arc) != 4 or arc[1] not in GRAPH_KINDS:
                raise ValueError("invalid private rank arc marker")
            value["last_arc"] = (
                parse_scalar("nonnegative", arc[0]),
                arc[1],
                require_canonical_uuid_urn(arc[2]),
                require_canonical_uuid_urn(arc[3]),
            )
        graph = value["last_graph"]
        if graph is not None:
            if (
                not isinstance(graph, list)
                or len(graph) != 7
                or graph[1] not in GRAPH_KINDS
                or graph[2] not in {"acyclic", "cyclic"}
            ):
                raise ValueError("invalid private rank graph marker")
            value["last_graph"] = (
                parse_scalar("nonnegative", graph[0]),
                graph[1],
                graph[2],
                *(parse_scalar("nonnegative", n) for n in graph[3:]),
            )
        header = _Header(**value)
        if (
            not 0 <= header.settled <= header.seeded <= header.vertices
            or not 0 <= header.ready <= header.seeded - header.settled
            or header.visited > header.arcs
        ):
            raise ValueError("inconsistent private rank counters")
        if header.phase == "graph" and any(
            (
                header.vertices,
                header.arcs,
                header.seeded,
                header.settled,
                header.visited,
                header.ready,
                header.seed_after,
                header.active,
                header.edge_after,
            )
        ):
            raise ValueError("nonempty private rank graph boundary")
        if (header.phase == "edges") != (header.active is not None):
            raise ValueError("invalid private rank active vertex")
        if header.phase != "edges" and header.edge_after is not None:
            raise ValueError("private rank adjacency without active vertex")
        if header.phase in {"pop", "edges"} and header.seeded != header.vertices:
            raise ValueError("private rank seed directory is incomplete")
        if header.phase == "seed" and (header.settled or header.visited):
            raise ValueError("private rank replay precedes seed completion")
        if _encode(header) != raw:
            raise ValueError("noncanonical private rank checkpoint")
        return header
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError) as exc:
        raise RuntimeError("invalid retained epoch rank checkpoint") from exc


@dataclass(frozen=True, slots=True)
class _Work:
    parent: union._Work
    state: str
    cursor_json: str
    header: _Header

    @property
    def source(self) -> source_owner._UploadJournalValidationWork:
        return self.parent.source


def _fence(work: _Work) -> tuple[Any, ...]:
    return (
        *union._fence(work.parent),
        Scan.rank_algorithm == EPOCH_RANK_ALGORITHM,
        Scan.rank_state == work.state,
        Scan.rank_cursor_json == work.cursor_json,
    )


def _current(session: Session, work: _Work) -> bool:
    return (
        session.scalar(
            select(Scan.collection_id)
            .join(
                Journal,
                (Scan.collection_id == Journal.collection_id)
                & (Scan.journal_id == Journal.journal_id),
            )
            .where(*_fence(work), *source_owner._upload_journal_validation_fence(work.source))
        )
        is not None
    )


def _begin(
    factory: SessionFactory, source: source_owner._UploadJournalValidationWork
) -> _Work | None:
    parent = union._begin(factory, source)
    if parent is None or parent.state not in {"complete", "deferred"}:
        return None
    with read_snapshot(factory) as session:
        row = session.get(Scan, (source.collection_id, source.journal_id))
        if row is None or row.version != parent.version or row.attempt_id != source.attempt_id:
            return None
        if row.rank_algorithm != EPOCH_RANK_ALGORITHM:
            raise RuntimeError("epoch rank algorithm differs from its retained attempt")
        header = _decode(row.rank_cursor_json)
        work = _Work(parent, row.rank_state, row.rank_cursor_json, header)
        if parent.state == "deferred":
            return replace(work, state="deferred")
        if not _current(session, work):
            return None
        terminal = (
            header.epoch == parent.header.epoch + 1
            and header.kind == GRAPH_KINDS[0]
            and header.phase == "graph"
        )
        if header.epoch > parent.header.epoch + 1 or (work.state == "complete") != terminal:
            raise RuntimeError("epoch rank extent differs from its captured unions")
        if header.phase != "graph":
            graph = session.get(Graph, union._key(source, header.epoch, header.kind))
            if (
                graph is None
                or graph.state != "complete"
                or (graph.vertex_count, graph.arc_count) != (header.vertices, header.arcs)
            ):
                raise RuntimeError("epoch rank lost its declared union counters")
        if header.last_vertex is not None:
            epoch, kind, vertex, degree, position, ready = header.last_vertex
            retained_vertex = session.get(Ranked, union._key(source, epoch, kind, vertex))
            if retained_vertex is None or (
                retained_vertex.remaining_degree,
                retained_vertex.rank,
                retained_vertex.ready,
            ) != (degree, position, ready):
                raise RuntimeError("epoch rank lost its last published vertex")
        if (
            header.last_arc is not None
            and session.get(Arc, union._key(source, *header.last_arc)) is None
        ):
            raise RuntimeError("epoch rank lost its last processed arc")
        if header.last_graph is not None:
            epoch, kind = header.last_graph[:2]
            retained_graph = session.get(Result, union._key(source, epoch, kind))
            if (
                retained_graph is None
                or (
                    retained_graph.state,
                    retained_graph.vertex_count,
                    retained_graph.arc_count,
                    retained_graph.settled_vertices,
                    retained_graph.visited_arcs,
                )
                != header.last_graph[2:]
            ):
                raise RuntimeError("epoch rank lost its last graph result")
        if header.active is not None:
            retained = session.get(
                Ranked, union._key(source, header.epoch, header.kind, header.active)
            )
            if (
                retained is None
                or retained.remaining_degree != 0
                or retained.ready
                or retained.rank != header.settled - 1
            ):
                raise RuntimeError("epoch rank lost its active adjacency vertex")
    return work


@declare_growth(
    Declaration(
        id="riverhog-server.sqlite-epoch-rank-page.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.services.upload_journal_rank._page",),
        scope="native-sqlite-successful-fenced-rank-seed-ready-or-adjacency-page",
        starts_at="probe exact source/cursor and seek the selected private rank index",
        ends_at="return only the detached bounded page after closing snapshot resources",
        dimensions=(
            Dimension(
                "retained_rows", COUNT, "Rows in the selected vertex, ready or adjacency relation."
            ),
            Dimension("requested_rows", COUNT, "Maximum rows admitted by this call."),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "equal",
                Expression("constant", value=2, unit=COUNT),
                "One source/cursor fence and one indexed page query.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                Expression("dimension", name="requested_rows"),
                "Ready pop returns at most one row; seed/adjacency respect the requested page.",
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "Rank pages use only private fixed-domain graph metadata.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        Expression("constant", value=256, unit=COUNT),
                        Expression(
                            "product",
                            terms=(
                                Expression("constant", value=128),
                                Expression("dimension", name="requested_rows"),
                            ),
                        ),
                        Expression(
                            "product",
                            terms=(
                                Expression("constant", value=64, unit=COUNT),
                                Expression(
                                    "log2_ceil",
                                    terms=(
                                        Expression(
                                            "sum",
                                            terms=(
                                                Expression("constant", value=2, unit=COUNT),
                                                Expression("dimension", name="retained_rows"),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Native instruction envelope for maintained seed, ready and adjacency indexes.",
            ),
        ),
        conditions=(
            "Exact source/attempt/algorithm/cursor fences and the owning indexes are present.",
            "Requested rows are positive and at most 64; identity keys use the native UUID domain.",
        ),
        invariants=(
            "An active vertex retains its last outgoing edge key across bounded adjacency steps.",
            "Private rank completion never accepts journal semantics "
            "or overrides native error order.",
        ),
        limitations=(
            "Degree point reads, computation, CAS writes, durable storage, "
            "graph transitions and cleanup are separate costs.",
            "SQLite instructions exclude internal key comparisons, page I/O and CPU; "
            "other engines require their own qualification.",
        ),
    )
)
def _page(
    factory: SessionFactory, work: _Work, *, row_budget: int = _BUDGET
) -> tuple[Any, ...] | None:
    if type(row_budget) is not int or not 1 <= row_budget <= _BUDGET:
        raise ValueError("invalid private rank page budget")
    h = work.header
    query: Select[Any]
    if h.phase == "seed":
        query = (
            select(Vertex.vertex_id, Vertex.indegree)
            .where(
                *union._prefix(Vertex, work.source),
                Vertex.epoch == h.epoch,
                Vertex.kind == h.kind,
                Vertex.vertex_id > (h.seed_after or ""),
            )
            .order_by(Vertex.vertex_id)
            .limit(row_budget)
        )
    elif h.phase == "pop":
        query = (
            select(Ranked.vertex_id, Ranked.remaining_degree, Ranked.rank, Ranked.ready)
            .where(
                *union._prefix(Ranked, work.source),
                Ranked.epoch == h.epoch,
                Ranked.kind == h.kind,
                Ranked.ready.is_(True),
            )
            .order_by(Ranked.vertex_id)
            .limit(1)
        )
    elif h.phase == "edges":
        query = (
            select(Arc.target_id)
            .where(
                *union._prefix(Arc, work.source),
                Arc.epoch == h.epoch,
                Arc.kind == h.kind,
                Arc.source_id == h.active,
                Arc.target_id > (h.edge_after or ""),
            )
            .order_by(Arc.target_id)
            .limit(row_budget)
        )
    else:
        raise ValueError("private rank page requires a seed, pop or adjacency phase")
    with read_snapshot(factory) as session:
        if not _current(session, work):
            return None
        with session.execute(query) as rows:
            return tuple(rows)


@dataclass(frozen=True, slots=True)
class _Vertex:
    vertex_id: str
    remaining_degree: int
    rank: int | None
    ready: bool


@dataclass(frozen=True, slots=True)
class _Change:
    before: _Vertex | None
    after: _Vertex


@dataclass(frozen=True, slots=True)
class _Advance:
    header: _Header
    state: str = "ranking"
    changes: tuple[_Change, ...] = ()
    result: str | None = None
    work_items: int = 0


def _advance(
    header: _Header, *, changes: tuple[_Change, ...] = (), work_items: int = 0
) -> _Advance:
    if changes:
        row = changes[-1].after
        header = replace(
            header,
            last_vertex=(
                header.epoch,
                header.kind,
                row.vertex_id,
                row.remaining_degree,
                row.rank,
                row.ready,
            ),
        )
    return _Advance(header, changes=changes, work_items=work_items)


def _finish(work: _Work) -> _Advance:
    h = work.header
    if h.ready != 0:
        raise RuntimeError("epoch rank ready directory is incomplete")
    state = "acyclic" if h.settled == h.vertices else "cyclic"
    if state == "acyclic" and h.visited != h.arcs:
        raise RuntimeError("epoch rank adjacency directory is incomplete")
    last = (h.epoch, h.kind, state, h.vertices, h.arcs, h.settled, h.visited)
    next_kind = GRAPH_KINDS[1] if h.kind == GRAPH_KINDS[0] else GRAPH_KINDS[0]
    epoch = h.epoch + int(next_kind == GRAPH_KINDS[0])
    header = _Header(
        epoch=epoch, kind=next_kind, last_vertex=h.last_vertex, last_arc=h.last_arc, last_graph=last
    )
    return _Advance(
        header, "complete" if epoch > work.parent.header.epoch else "ranking", result=state
    )


def _compute(factory: SessionFactory, work: _Work, *, row_budget: int = _BUDGET) -> _Advance | None:
    h = work.header
    if h.phase == "graph":
        with read_snapshot(factory) as session:
            if not _current(session, work):
                return None
            graph = session.get(Graph, union._key(work.source, h.epoch, h.kind))
            if (
                graph is None
                or graph.state != "complete"
                or session.get(Result, union._key(work.source, h.epoch, h.kind)) is not None
            ):
                raise RuntimeError("epoch rank lost an unprocessed complete union")
            vertices, arcs = graph.vertex_count, graph.arc_count
        return _advance(replace(h, phase="seed", vertices=vertices, arcs=arcs))
    rows = _page(factory, work, row_budget=row_budget)
    if rows is None:
        return None
    if h.phase == "seed":
        changes = tuple(
            _Change(None, _Vertex(require_canonical_uuid_urn(vertex), degree, None, degree == 0))
            for vertex, degree in rows
        )
        seeded = h.seeded + len(rows)
        if seeded > h.vertices or (len(rows) < row_budget and seeded != h.vertices):
            raise RuntimeError("epoch rank vertex directory is incomplete")
        header = replace(
            h,
            phase="pop" if len(rows) < row_budget else "seed",
            seeded=seeded,
            seed_after=rows[-1][0] if rows else h.seed_after,
            ready=h.ready + sum(change.after.ready for change in changes),
        )
        return _advance(header, changes=changes, work_items=len(rows))
    if h.phase == "pop":
        if not rows:
            return _finish(work)
        before = _Vertex(*rows[0])
        if (
            h.ready == 0
            or before.remaining_degree != 0
            or before.rank is not None
            or not before.ready
        ):
            raise RuntimeError("epoch rank ready row differs from its counters")
        after = replace(before, rank=h.settled, ready=False)
        return _advance(
            replace(
                h,
                phase="edges",
                active=after.vertex_id,
                edge_after=None,
                settled=h.settled + 1,
                ready=h.ready - 1,
            ),
            changes=(_Change(before, after),),
            work_items=1,
        )
    active = h.active
    if active is None:
        raise RuntimeError("epoch rank adjacency has no active vertex")
    targets = tuple(row[0] for row in rows)
    with read_snapshot(factory) as session:
        if not _current(session, work):
            return None
        points = {}
        for target in targets:
            row = session.get(Ranked, union._key(work.source, h.epoch, h.kind, target))
            if row is None or row.rank is not None or row.ready:
                raise RuntimeError("epoch rank adjacency lost an unsettled target")
            points[target] = _Vertex(row.vertex_id, row.remaining_degree, row.rank, row.ready)
    degrees = decrement_rank_degrees(
        targets, {vertex: row.remaining_degree for vertex, row in points.items()}
    )
    changes = tuple(
        _Change(points[vertex], replace(points[vertex], remaining_degree=degree, ready=degree == 0))
        for vertex, degree in degrees
    )
    visited = h.visited + len(targets)
    if visited > h.arcs:
        raise RuntimeError("epoch rank visited more than its declared arcs")
    finished = len(rows) < row_budget
    header = replace(
        h,
        phase="pop" if finished else "edges",
        active=None if finished else h.active,
        edge_after=None if finished else targets[-1],
        visited=visited,
        ready=h.ready + sum(change.after.ready for change in changes),
        last_arc=(h.epoch, h.kind, active, targets[-1]) if targets else h.last_arc,
    )
    return _advance(header, changes=changes, work_items=len(rows))


def _commit(factory: SessionFactory, work: _Work, advance: _Advance) -> bool:
    source = work.source
    with factory() as session, session.begin():
        if (
            session.scalar(
                select(Journal.collection_id)
                .where(*source_owner._upload_journal_validation_fence(source))
                .with_for_update(skip_locked=True)
            )
            is None
        ):
            return False
        changed = session.execute(
            update(Scan)
            .where(*_fence(work))
            .values(
                version=work.parent.version + 1,
                rank_state=advance.state,
                rank_cursor_json=_encode(advance.header),
            )
            .returning(Scan.collection_id)
        ).scalar_one_or_none()
        if changed is None:
            return False
        base = dict(
            collection_id=source.collection_id,
            journal_id=source.journal_id,
            attempt_id=source.attempt_id,
            epoch=work.header.epoch,
            kind=work.header.kind,
        )
        for change in advance.changes:
            row = change.after
            values = dict(remaining_degree=row.remaining_degree, rank=row.rank, ready=row.ready)
            if change.before is None:
                session.add(Ranked(**base, vertex_id=row.vertex_id, **values))
            else:
                before = change.before
                changed_vertex = session.execute(
                    update(Ranked)
                    .where(
                        *union._prefix(Ranked, source),
                        Ranked.epoch == work.header.epoch,
                        Ranked.kind == work.header.kind,
                        Ranked.vertex_id == row.vertex_id,
                        Ranked.remaining_degree == before.remaining_degree,
                        Ranked.rank.is_(None)
                        if before.rank is None
                        else Ranked.rank == before.rank,
                        Ranked.ready == before.ready,
                    )
                    .values(**values)
                    .returning(Ranked.vertex_id)
                ).scalar_one_or_none()
                if changed_vertex is None:
                    raise RuntimeError("epoch rank publication lost its observed vertex")
        if advance.result is not None:
            h = work.header
            session.add(
                Result(
                    **base,
                    state=advance.result,
                    vertex_count=h.vertices,
                    arc_count=h.arcs,
                    settled_vertices=h.settled,
                    visited_arcs=h.visited,
                )
            )
        session.flush()
    return True


def advance_upload_journal_epoch_rank(
    factory: SessionFactory, source: source_owner._UploadJournalValidationWork
) -> bool:
    """Advance at most 64 vertex/edge items and sixteen bounded phase transitions."""
    remaining = _BUDGET
    for _ in range(16):
        work = _begin(factory, source)
        if work is None:
            return False
        if work.state in {"complete", "deferred"}:
            return True
        advance = _compute(factory, work, row_budget=remaining)
        if advance is None or not _commit(factory, work, advance):
            return False
        remaining -= advance.work_items
        if advance.state == "complete":
            return True
        if remaining == 0:
            return False
    return False
