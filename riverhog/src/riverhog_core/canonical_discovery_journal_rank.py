"""Bounded private ranks over the exact original-source discovery epoch unions."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import dataclass, replace
from typing import Any

from riverhog_canonical_json import format_scalar, parse_scalar
from riverhog_provenance.epoch_rank import EPOCH_RANK_ALGORITHM, decrement_rank_degrees
from riverhog_provenance.epoch_union import GRAPH_KINDS
from riverhog_provenance_contracts import require_canonical_uuid_urn

from riverhog_core import canonical_discovery_journal_union as union
from riverhog_core.canonical_discovery_archive_work import ArchiveTaskKey, StaleArchiveTask
from riverhog_core.canonical_discovery_history_inputs import _one
from riverhog_core.canonical_discovery_journal_rank_growth import declare_growth_for_journal_rank


@dataclass(frozen=True, slots=True)
class DiscoveryJournalRankProgress:
    version: int = 0
    state: str = "building"
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


@dataclass(frozen=True, slots=True)
class DiscoveryRankVertex:
    identity: str
    degree: int
    rank: int | None
    ready: bool


@dataclass(frozen=True, slots=True)
class DiscoveryRankChange:
    before: DiscoveryRankVertex | None
    after: DiscoveryRankVertex


@dataclass(frozen=True, slots=True)
class PreparedDiscoveryJournalRank:
    key: ArchiveTaskKey
    expected: DiscoveryJournalRankProgress
    progress: DiscoveryJournalRankProgress
    changes: tuple[DiscoveryRankChange, ...] = ()
    result: str | None = None
    work_items: int = 0


class DiscoveryJournalRankWork:
    """One retained original union owner and one owned bounded publication atom."""

    def __init__(self, original: union.DiscoveryJournalUnionWork) -> None:
        self.union = original
        self.binding_json = original.binding_json
        self._prepared: PreparedDiscoveryJournalRank | None = None


_BASE = (
    "version",
    "state",
    "epoch",
    "kind",
    "phase",
    "seed_after",
    "active",
    "edge_after",
    "vertices",
    "arcs",
    "seeded",
    "settled",
    "visited",
    "ready",
)
_NUMBERS = {"version", "epoch", "vertices", "arcs", "seeded", "settled", "visited", "ready"}
_COLUMNS = (
    *_BASE,
    "last_vertex_epoch",
    "last_vertex_kind",
    "last_vertex_id",
    "last_vertex_degree",
    "last_vertex_rank",
    "last_vertex_ready",
    "last_arc_epoch",
    "last_arc_kind",
    "last_arc_source",
    "last_arc_target",
    "last_graph_epoch",
    "last_graph_kind",
    "last_graph_state",
    "last_graph_vertices",
    "last_graph_arcs",
    "last_graph_settled",
    "last_graph_visited",
)


def _natural(value: object) -> int:
    try:
        return parse_scalar("nonnegative", value)
    except (TypeError, ValueError):
        raise StaleArchiveTask("journal rank counter is not canonical") from None


def _key(key: ArchiveTaskKey, work: DiscoveryJournalRankWork) -> tuple[str, str, str]:
    return key.build_id, key.admission_sha256, work.union.topology.epochs.source.binding.journal_id


def _graph_key(
    key: ArchiveTaskKey, work: DiscoveryJournalRankWork, epoch: int, kind: str
) -> tuple[str, str, str, str, str]:
    return (*_key(key, work), format_scalar("nonnegative", epoch), kind)


def _controls(progress: DiscoveryJournalRankProgress) -> tuple[object, ...]:
    values = tuple(
        format_scalar("nonnegative", getattr(progress, name))
        if name in _NUMBERS
        else getattr(progress, name)
        for name in _BASE
    )
    vertex, arc, graph = progress.last_vertex, progress.last_arc, progress.last_graph
    return (
        *values,
        *(
            (None,) * 6
            if vertex is None
            else (
                format_scalar("nonnegative", vertex[0]),
                vertex[1],
                vertex[2],
                format_scalar("nonnegative", vertex[3]),
                None if vertex[4] is None else format_scalar("nonnegative", vertex[4]),
                int(vertex[5]),
            )
        ),
        *((None,) * 4 if arc is None else (format_scalar("nonnegative", arc[0]), *arc[1:])),
        *(
            (None,) * 7
            if graph is None
            else (
                format_scalar("nonnegative", graph[0]),
                graph[1],
                graph[2],
                *(format_scalar("nonnegative", value) for value in graph[3:]),
            )
        ),
    )


def _source(
    db: sqlite3.Connection, key: ArchiveTaskKey, work: DiscoveryJournalRankWork
) -> union.DiscoveryJournalUnionProgress:
    source = union.discovery_journal_union_progress(db, key, work.union)
    if source.state not in {"complete", "deferred"}:
        raise StaleArchiveTask("journal rank requires terminal original union preparation")
    return source


def _graph(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: DiscoveryJournalRankWork,
    epoch: int,
    kind: str,
) -> tuple[int, int]:
    row = _one(
        db,
        "SELECT vertices,arcs,state FROM discovery_journal_union_graphs "
        "WHERE build_id=? AND admission_sha256=? AND journal_id=? AND epoch=? AND kind=?",
        _graph_key(key, work, epoch, kind),
    )
    if row is None or row[2] != "complete":
        raise StaleArchiveTask("journal rank lost its completed original union")
    return _natural(row[0]), _natural(row[1])


def _vertex_row(row: tuple[Any, ...]) -> DiscoveryRankVertex:
    if row[3] not in (0, 1):
        raise StaleArchiveTask("journal rank ready control changed")
    return DiscoveryRankVertex(
        require_canonical_uuid_urn(row[0]),
        _natural(row[1]),
        None if row[2] is None else _natural(row[2]),
        bool(row[3]),
    )


def _vertex(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: DiscoveryJournalRankWork,
    epoch: int,
    kind: str,
    identity: str,
) -> DiscoveryRankVertex:
    row = _one(
        db,
        "SELECT vertex_id,degree,rank,ready FROM discovery_journal_rank_vertices "
        "WHERE build_id=? AND admission_sha256=? AND journal_id=? "
        "AND epoch=? AND kind=? AND vertex_id=?",
        (*_graph_key(key, work, epoch, kind), identity),
    )
    if row is None:
        raise StaleArchiveTask("journal rank lost its published vertex")
    return _vertex_row(row)


def _decode(row: tuple[Any, ...]) -> DiscoveryJournalRankProgress:
    if len(row) != len(_COLUMNS):
        raise StaleArchiveTask("journal rank cursor shape changed")
    for first, end in ((14, 20), (20, 24), (24, 31)):
        if row[first] is None and any(value is not None for value in row[first:end]):
            raise StaleArchiveTask("journal rank marker shape changed")
    v, a, g = row[14:20], row[20:24], row[24:31]
    vertex = (
        None
        if v[0] is None
        else (
            _natural(v[0]),
            v[1],
            require_canonical_uuid_urn(v[2]),
            _natural(v[3]),
            None if v[4] is None else _natural(v[4]),
            bool(v[5]),
        )
    )
    if v[0] is not None and v[5] not in (0, 1):
        raise StaleArchiveTask("journal rank vertex marker changed")
    arc = (
        None
        if a[0] is None
        else (
            _natural(a[0]),
            a[1],
            require_canonical_uuid_urn(a[2]),
            require_canonical_uuid_urn(a[3]),
        )
    )
    graph = (
        None
        if g[0] is None
        else (
            _natural(g[0]),
            g[1],
            g[2],
            _natural(g[3]),
            _natural(g[4]),
            _natural(g[5]),
            _natural(g[6]),
        )
    )
    return DiscoveryJournalRankProgress(
        _natural(row[0]),
        row[1],
        _natural(row[2]),
        row[3],
        row[4],
        None if row[5] is None else require_canonical_uuid_urn(row[5]),
        None if row[6] is None else require_canonical_uuid_urn(row[6]),
        None if row[7] is None else require_canonical_uuid_urn(row[7]),
        _natural(row[8]),
        _natural(row[9]),
        _natural(row[10]),
        _natural(row[11]),
        _natural(row[12]),
        _natural(row[13]),
        vertex,
        arc,
        graph,
    )


def discovery_journal_rank_progress(
    db: sqlite3.Connection, key: ArchiveTaskKey, work: DiscoveryJournalRankWork
) -> DiscoveryJournalRankProgress:
    source = _source(db, key, work)
    row = _one(
        db,
        "SELECT worker_id,directory_id,algorithm,binding_json,union_version,"
        + ",".join(_COLUMNS)
        + " FROM discovery_journal_rank "
        "WHERE build_id=? AND admission_sha256=? AND journal_id=?",
        _key(key, work),
    )
    if row is None or row[:5] != (
        key.worker_id,
        work.union.topology.epochs.source.binding.directory_id,
        EPOCH_RANK_ALGORITHM,
        work.binding_json,
        format_scalar("nonnegative", source.version),
    ):
        raise StaleArchiveTask("journal rank checkpoint has another original binding")
    try:
        p = _decode(row[5:])
    except (TypeError, ValueError):
        raise StaleArchiveTask("journal rank checkpoint controls changed") from None
    terminal = p.epoch == source.epoch + 1 and p.kind == GRAPH_KINDS[0] and p.phase == "graph"
    if (
        p.state not in {"building", "complete", "deferred"}
        or p.kind not in GRAPH_KINDS
        or p.phase not in {"graph", "seed", "pop", "edges"}
        or p.epoch > source.epoch + 1
        or not 0 <= p.settled <= p.seeded <= p.vertices
        or not 0 <= p.ready <= p.seeded - p.settled
        or p.visited > p.arcs
        or (p.phase == "edges") != (p.active is not None)
        or p.phase != "edges"
        and p.edge_after is not None
        or p.phase in {"pop", "edges"}
        and p.seeded != p.vertices
        or p.phase == "seed"
        and (p.settled or p.visited)
        or p.phase == "graph"
        and any(
            (
                p.seed_after,
                p.active,
                p.edge_after,
                p.vertices,
                p.arcs,
                p.seeded,
                p.settled,
                p.visited,
                p.ready,
            )
        )
        or source.state == "deferred"
        and p.state != "deferred"
        or source.state == "complete"
        and (p.state == "deferred" or (p.state == "complete") != terminal)
    ):
        raise StaleArchiveTask("journal rank checkpoint extent changed")
    if p.phase != "graph" and _graph(db, key, work, p.epoch, p.kind) != (p.vertices, p.arcs):
        raise StaleArchiveTask("journal rank graph counters changed")
    if p.last_vertex is not None:
        epoch, kind, identity, degree, position, ready = p.last_vertex
        if (
            kind not in GRAPH_KINDS
            or epoch > source.epoch
            or _vertex(db, key, work, epoch, kind, identity)
            != DiscoveryRankVertex(identity, degree, position, ready)
        ):
            raise StaleArchiveTask("journal rank lost its last vertex marker")
    if p.last_arc is not None:
        epoch, kind, left, right = p.last_arc
        if (
            kind not in GRAPH_KINDS
            or epoch > source.epoch
            or _one(
                db,
                "SELECT 1 FROM discovery_journal_union_arcs WHERE build_id=? "
                "AND admission_sha256=? AND journal_id=? AND epoch=? AND kind=? "
                "AND source_id=? AND target_id=?",
                (*_graph_key(key, work, epoch, kind), left, right),
            )
            is None
        ):
            raise StaleArchiveTask("journal rank lost its last adjacency marker")
    if p.last_graph is not None:
        epoch, kind, state, vertices, arcs, settled, visited = p.last_graph
        result = _one(
            db,
            "SELECT state,vertices,arcs,settled,visited FROM discovery_journal_rank_graphs "
            "WHERE build_id=? AND admission_sha256=? AND journal_id=? AND epoch=? AND kind=?",
            _graph_key(key, work, epoch, kind),
        )
        if (
            kind not in GRAPH_KINDS
            or epoch > source.epoch
            or state not in {"acyclic", "cyclic"}
            or result
            != (
                state,
                *(
                    format_scalar("nonnegative", n)
                    for n in (
                        vertices,
                        arcs,
                        settled,
                        visited,
                    )
                ),
            )
        ):
            raise StaleArchiveTask("journal rank lost its last graph marker")
    if p.active is not None and _vertex(db, key, work, p.epoch, p.kind, p.active) != (
        DiscoveryRankVertex(p.active, 0, p.settled - 1, False)
    ):
        raise StaleArchiveTask("journal rank lost its active adjacency vertex")
    return p


def begin_discovery_journal_rank(
    db: sqlite3.Connection, key: ArchiveTaskKey, work: DiscoveryJournalRankWork
) -> DiscoveryJournalRankProgress:
    if not db.in_transaction:
        raise ValueError("journal rank initialization requires an owner transaction")
    source = _source(db, key, work)
    initial = DiscoveryJournalRankProgress(
        state="deferred" if source.state == "deferred" else "building"
    )
    db.execute(
        "INSERT OR IGNORE INTO discovery_journal_rank VALUES("
        + ",".join("?" for _ in range(8 + len(_COLUMNS)))
        + ")",
        (
            *_key(key, work),
            key.worker_id,
            work.union.topology.epochs.source.binding.directory_id,
            EPOCH_RANK_ALGORITHM,
            work.binding_json,
            format_scalar("nonnegative", source.version),
            *_controls(initial),
        ),
    ).close()
    return discovery_journal_rank_progress(db, key, work)


def _prepared(
    key: ArchiveTaskKey,
    expected: DiscoveryJournalRankProgress,
    updated: DiscoveryJournalRankProgress,
    *,
    changes: tuple[DiscoveryRankChange, ...] = (),
    result: str | None = None,
    work_items: int = 0,
) -> PreparedDiscoveryJournalRank:
    if changes:
        last = changes[-1].after
        updated = replace(
            updated,
            last_vertex=(
                expected.epoch,
                expected.kind,
                last.identity,
                last.degree,
                last.rank,
                last.ready,
            ),
        )
    return PreparedDiscoveryJournalRank(
        key,
        expected,
        replace(updated, version=expected.version + 1),
        changes,
        result,
        work_items,
    )


def _page(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: DiscoveryJournalRankWork,
    p: DiscoveryJournalRankProgress,
) -> tuple[tuple[Any, ...], ...]:
    prefix = _graph_key(key, work, p.epoch, p.kind)
    parameters: tuple[object, ...]
    if p.phase == "seed":
        sql = (
            "SELECT vertex_id,degree FROM discovery_journal_union_vertices "
            "WHERE build_id=? AND admission_sha256=? AND journal_id=? AND epoch=? AND kind=? "
            "AND vertex_id>? ORDER BY vertex_id LIMIT 64"
        )
        parameters = (*prefix, p.seed_after or "")
    elif p.phase == "pop":
        sql = (
            "SELECT vertex_id,degree,rank,ready FROM discovery_journal_rank_vertices "
            "INDEXED BY discovery_journal_rank_ready "
            "WHERE build_id=? AND admission_sha256=? AND journal_id=? AND epoch=? AND kind=? "
            "AND ready=1 ORDER BY vertex_id LIMIT 1"
        )
        parameters = prefix
    elif p.phase == "edges":
        sql = (
            "SELECT target_id FROM discovery_journal_union_arcs "
            "WHERE build_id=? AND admission_sha256=? AND journal_id=? AND epoch=? AND kind=? "
            "AND source_id=? AND target_id>? ORDER BY target_id LIMIT 64"
        )
        parameters = (*prefix, p.active, p.edge_after or "")
    else:
        raise ValueError("journal rank page requires seed, ready or adjacency work")
    with closing(db.execute(sql, parameters)) as rows:
        return tuple(rows.fetchall())


def _finish(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: DiscoveryJournalRankWork,
    p: DiscoveryJournalRankProgress,
) -> PreparedDiscoveryJournalRank:
    if p.ready:
        raise StaleArchiveTask("journal rank ready directory is incomplete")
    state = "acyclic" if p.settled == p.vertices else "cyclic"
    if state == "acyclic" and p.visited != p.arcs:
        raise StaleArchiveTask("journal rank adjacency directory is incomplete")
    kind = GRAPH_KINDS[1] if p.kind == GRAPH_KINDS[0] else GRAPH_KINDS[0]
    epoch = p.epoch + int(kind == GRAPH_KINDS[0])
    source = _source(db, key, work)
    updated = DiscoveryJournalRankProgress(
        version=p.version,
        state="complete" if epoch > source.epoch else "building",
        epoch=epoch,
        kind=kind,
        last_vertex=p.last_vertex,
        last_arc=p.last_arc,
        last_graph=(p.epoch, p.kind, state, p.vertices, p.arcs, p.settled, p.visited),
    )
    return _prepared(key, p, updated, result=state)


def _step(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: DiscoveryJournalRankWork,
    p: DiscoveryJournalRankProgress,
) -> PreparedDiscoveryJournalRank:
    if p.phase == "graph":
        vertices, arcs = _graph(db, key, work, p.epoch, p.kind)
        if (
            _one(
                db,
                "SELECT 1 FROM discovery_journal_rank_graphs WHERE build_id=? "
                "AND admission_sha256=? AND journal_id=? AND epoch=? AND kind=?",
                _graph_key(key, work, p.epoch, p.kind),
            )
            is not None
        ):
            raise StaleArchiveTask("journal rank encountered a completed new graph")
        return _prepared(key, p, replace(p, phase="seed", vertices=vertices, arcs=arcs))
    rows = _page(db, key, work, p)
    if p.phase == "seed":
        changes = tuple(
            DiscoveryRankChange(
                None,
                DiscoveryRankVertex(
                    require_canonical_uuid_urn(identity),
                    _natural(degree),
                    None,
                    _natural(degree) == 0,
                ),
            )
            for identity, degree in rows
        )
        seeded = p.seeded + len(rows)
        if seeded > p.vertices or len(rows) < 64 and seeded != p.vertices:
            raise StaleArchiveTask("journal rank seed directory is incomplete")
        return _prepared(
            key,
            p,
            replace(
                p,
                seeded=seeded,
                phase="pop" if len(rows) < 64 else "seed",
                seed_after=rows[-1][0] if rows else p.seed_after,
                ready=p.ready + sum(change.after.ready for change in changes),
            ),
            changes=changes,
            work_items=len(rows),
        )
    if p.phase == "pop":
        if not rows:
            return _finish(db, key, work, p)
        before = _vertex_row(rows[0])
        if not p.ready or before.degree != 0 or before.rank is not None or not before.ready:
            raise StaleArchiveTask("journal rank ready row differs from its counters")
        after = replace(before, rank=p.settled, ready=False)
        return _prepared(
            key,
            p,
            replace(
                p,
                phase="edges",
                active=after.identity,
                edge_after=None,
                settled=p.settled + 1,
                ready=p.ready - 1,
            ),
            changes=(DiscoveryRankChange(before, after),),
            work_items=1,
        )
    if p.active is None:
        raise StaleArchiveTask("journal rank adjacency has no active vertex")
    targets = tuple(require_canonical_uuid_urn(row[0]) for row in rows)
    points = {target: _vertex(db, key, work, p.epoch, p.kind, target) for target in targets}
    if any(row.rank is not None or row.ready for row in points.values()):
        raise StaleArchiveTask("journal rank adjacency lost an unsettled target")
    degrees = decrement_rank_degrees(targets, {k: row.degree for k, row in points.items()})
    changes = tuple(
        DiscoveryRankChange(
            points[identity], replace(points[identity], degree=degree, ready=degree == 0)
        )
        for identity, degree in degrees
    )
    visited = p.visited + len(rows)
    if visited > p.arcs:
        raise StaleArchiveTask("journal rank visited more than its declared arcs")
    return _prepared(
        key,
        p,
        replace(
            p,
            phase="pop" if len(rows) < 64 else "edges",
            active=None if len(rows) < 64 else p.active,
            edge_after=None if len(rows) < 64 else targets[-1],
            visited=visited,
            ready=p.ready + sum(change.after.ready for change in changes),
            last_arc=(p.epoch, p.kind, p.active, targets[-1]) if targets else p.last_arc,
        ),
        changes=changes,
        work_items=len(rows),
    )


def prepare_discovery_journal_rank(
    db: sqlite3.Connection, key: ArchiveTaskKey, work: DiscoveryJournalRankWork
) -> PreparedDiscoveryJournalRank | None:
    if db.in_transaction:
        raise ValueError("journal rank preparation requires no write transaction")
    progress = discovery_journal_rank_progress(db, key, work)
    if progress.state != "building":
        return None
    cached = work._prepared
    if cached is not None and cached.key == key and cached.expected == progress:
        return cached
    prepared = _step(db, key, work, progress)
    work._prepared = prepared
    return prepared


def _vertex_controls(vertex: DiscoveryRankVertex) -> tuple[object, ...]:
    return (
        format_scalar("nonnegative", vertex.degree),
        None if vertex.rank is None else format_scalar("nonnegative", vertex.rank),
        int(vertex.ready),
    )


def apply_discovery_journal_rank(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: DiscoveryJournalRankWork,
    prepared: PreparedDiscoveryJournalRank,
) -> bool:
    if not db.in_transaction:
        raise ValueError("journal rank publication requires an owner transaction")
    if prepared is not work._prepared or prepared.key != key:
        raise StaleArchiveTask("journal rank publication has no owned prepared atom")
    if discovery_journal_rank_progress(db, key, work) != prepared.expected:
        return False
    p = prepared.expected
    prefix = _graph_key(key, work, p.epoch, p.kind)
    for change in prepared.changes:
        after = change.after
        if change.before is None:
            db.execute(
                "INSERT INTO discovery_journal_rank_vertices VALUES(?,?,?,?,?,?,?,?,?)",
                (*prefix, after.identity, *_vertex_controls(after)),
            ).close()
        else:
            with closing(
                db.execute(
                    "UPDATE discovery_journal_rank_vertices SET degree=?,rank=?,ready=? "
                    "WHERE build_id=? AND admission_sha256=? AND journal_id=? AND epoch=? "
                    "AND kind=? AND vertex_id=? AND degree IS ? AND rank IS ? AND ready IS ?",
                    (
                        *_vertex_controls(after),
                        *prefix,
                        after.identity,
                        *_vertex_controls(change.before),
                    ),
                )
            ) as changed:
                if changed.rowcount != 1:
                    raise StaleArchiveTask("journal rank publication lost its observed vertex")
    if prepared.result is not None:
        db.execute(
            "INSERT INTO discovery_journal_rank_graphs VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                *prefix,
                prepared.result,
                *(
                    format_scalar("nonnegative", n)
                    for n in (
                        p.vertices,
                        p.arcs,
                        p.settled,
                        p.visited,
                    )
                ),
            ),
        ).close()
    with closing(
        db.execute(
            "UPDATE discovery_journal_rank SET "
            + ",".join(name + "=?" for name in _COLUMNS)
            + " WHERE build_id=? AND admission_sha256=? AND journal_id=? AND "
            + " AND ".join(name + " IS ?" for name in _COLUMNS),
            (*_controls(prepared.progress), *_key(key, work), *_controls(prepared.expected)),
        )
    ) as changed:
        if changed.rowcount != 1:
            raise StaleArchiveTask("journal rank publication lost its exact checkpoint")
    work._prepared = None
    return True


declare_growth_for_journal_rank(globals())
