"""Source-fenced bounded private unions over original journal topology facts."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import dataclass, replace

from riverhog_canonical_json import format_scalar, parse_scalar
from riverhog_provenance.epoch_union import (
    EPOCH_UNION_ALGORITHM,
    GRAPH_KINDS,
    ArcKey,
    EpochUnionDelta,
    TopologyOccurrence,
    VertexKey,
    plan_epoch_union_additions,
)
from riverhog_provenance.topology_checkpoint import validate_checkpoint_digest
from riverhog_provenance_contracts import require_canonical_uuid_urn

from riverhog_core import canonical_discovery_journal_epochs as epochs
from riverhog_core import canonical_discovery_journal_topology as topology
from riverhog_core.canonical_discovery_archive_work import ArchiveTaskKey, StaleArchiveTask
from riverhog_core.canonical_discovery_history_inputs import _one
from riverhog_core.canonical_discovery_journal_union_growth import (
    declare_growth_for_journal_union,
)


@dataclass(frozen=True, slots=True)
class DiscoveryJournalUnionProgress:
    version: int = 0
    frame: int = 0
    state: str = "building"
    epoch: int = 0
    phase: str = "frame"
    consumed: int = 0
    seed_after: str | None = None
    active_count: int = 0
    seeded_count: int = 0
    last_source_sha256: str | None = None
    last_arc: tuple[int, str, str, str] | None = None


@dataclass(frozen=True, slots=True)
class DiscoveryUnionGraphPatch:
    epoch: int
    kind: str
    vertices: int
    arcs: int
    state: str
    new: bool = False


@dataclass(frozen=True, slots=True)
class PreparedDiscoveryJournalUnion:
    key: ArchiveTaskKey
    expected: DiscoveryJournalUnionProgress
    progress: DiscoveryJournalUnionProgress
    graphs: tuple[DiscoveryUnionGraphPatch, ...] = ()
    delta: EpochUnionDelta | None = None
    retired: tuple[str, ...] = ()
    work_items: int = 0


class DiscoveryJournalUnionWork:
    """One exact original topology owner and at most one bounded prepared delta."""

    def __init__(self, original: topology.DiscoveryJournalTopologyWork) -> None:
        self.topology = original
        self.binding_json = original.binding_json
        self._prepared: PreparedDiscoveryJournalUnion | None = None


_COLUMNS = (
    "version",
    "frame",
    "state",
    "epoch",
    "phase",
    "consumed",
    "seed_after",
    "active_count",
    "seeded_count",
    "last_source_sha256",
    "last_arc_epoch",
    "last_arc_kind",
    "last_arc_source",
    "last_arc_target",
)


def _natural(value: object) -> int:
    try:
        return parse_scalar("nonnegative", value)
    except (ValueError, TypeError):
        raise StaleArchiveTask("journal union counter is not canonical") from None


def _key(key: ArchiveTaskKey, work: DiscoveryJournalUnionWork) -> tuple[str, str, str]:
    return key.build_id, key.admission_sha256, work.topology.epochs.source.binding.journal_id


def _controls(progress: DiscoveryJournalUnionProgress) -> tuple[object, ...]:
    arc = progress.last_arc
    return (
        format_scalar("nonnegative", progress.version),
        format_scalar("nonnegative", progress.frame),
        progress.state,
        format_scalar("nonnegative", progress.epoch),
        progress.phase,
        format_scalar("nonnegative", progress.consumed),
        progress.seed_after,
        format_scalar("nonnegative", progress.active_count),
        format_scalar("nonnegative", progress.seeded_count),
        progress.last_source_sha256,
        *(
            (None, None, None, None)
            if arc is None
            else (
                format_scalar("nonnegative", arc[0]),
                *arc[1:],
            )
        ),
    )


def _source(
    db: sqlite3.Connection, key: ArchiveTaskKey, work: DiscoveryJournalUnionWork
) -> topology.DiscoveryJournalTopologyProgress:
    progress = topology.discovery_journal_topology_progress(db, key, work.topology)
    if progress.phase not in {"complete", "deferred"}:
        raise StaleArchiveTask("journal union requires terminal original topology preparation")
    return progress


def _graphs(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: DiscoveryJournalUnionWork,
    epoch: int,
    *,
    state: str = "building",
) -> dict[str, tuple[int, int]]:
    result = {}
    for kind in GRAPH_KINDS:
        row = _one(
            db,
            "SELECT vertices,arcs,state FROM discovery_journal_union_graphs "
            "WHERE build_id=? AND admission_sha256=? AND journal_id=? AND epoch=? AND kind=?",
            (*_key(key, work), format_scalar("nonnegative", epoch), kind),
        )
        if row is None or row[2] != state:
            raise StaleArchiveTask("journal union lost its exact graph counters")
        result[kind] = (_natural(row[0]), _natural(row[1]))
    return result


def discovery_journal_union_progress(
    db: sqlite3.Connection, key: ArchiveTaskKey, work: DiscoveryJournalUnionWork
) -> DiscoveryJournalUnionProgress:
    original = _source(db, key, work)
    row = _one(
        db,
        "SELECT worker_id,directory_id,algorithm,binding_json,topology_version,"
        + ",".join(_COLUMNS)
        + " FROM discovery_journal_union "
        "WHERE build_id=? AND admission_sha256=? AND journal_id=?",
        _key(key, work),
    )
    if row is None or row[:5] != (
        key.worker_id,
        work.topology.epochs.source.binding.directory_id,
        EPOCH_UNION_ALGORITHM,
        work.binding_json,
        format_scalar("nonnegative", original.version),
    ):
        raise StaleArchiveTask("journal union checkpoint has another original binding")
    try:
        arc = (
            None
            if row[15] is None
            else (
                _natural(row[15]),
                row[16],
                require_canonical_uuid_urn(row[17]),
                require_canonical_uuid_urn(row[18]),
            )
        )
        progress = DiscoveryJournalUnionProgress(
            _natural(row[5]),
            _natural(row[6]),
            row[7],
            _natural(row[8]),
            row[9],
            _natural(row[10]),
            None if row[11] is None else require_canonical_uuid_urn(row[11]),
            _natural(row[12]),
            _natural(row[13]),
            None if row[14] is None else validate_checkpoint_digest(row[14]),
            arc,
        )
    except (ValueError, TypeError):
        raise StaleArchiveTask("journal union checkpoint controls changed") from None
    if (
        progress.state not in {"building", "complete", "deferred"}
        or progress.phase not in {"frame", "retracts", "seed", "add"}
        or not 0 <= progress.frame <= original.next_frame
        or not 0 <= progress.seeded_count <= progress.active_count <= progress.consumed
        or not 0 <= progress.consumed <= original.checkpoint.facts
        or progress.epoch > progress.frame
        or (progress.consumed == 0) != (progress.last_source_sha256 is None)
        or progress.seed_after is not None
        and progress.phase != "seed"
        or progress.state == "complete"
        and (
            original.phase != "complete"
            or progress.frame != original.next_frame
            or progress.phase != "frame"
            or progress.consumed != original.checkpoint.facts
        )
        or original.phase == "deferred"
        and progress.state != "deferred"
    ):
        raise StaleArchiveTask("journal union checkpoint extent changed")
    if progress.consumed:
        values = topology.discovery_journal_topology_page(
            db, key, work.topology, progress.consumed - 1, fact_budget=1
        )
        if (
            not values
            or values[0].item.json_sha256 != progress.last_source_sha256
            or values[0].frame > progress.frame
            or values[0].frame == progress.frame
            and progress.phase == "frame"
        ):
            raise StaleArchiveTask("journal union lost its last original fact")
    if progress.last_arc is not None:
        epoch, kind, left, right = progress.last_arc
        if (
            kind not in GRAPH_KINDS
            or epoch > progress.epoch
            or _one(
                db,
                "SELECT 1 FROM discovery_journal_union_arcs WHERE build_id=? "
                "AND admission_sha256=? AND journal_id=? AND epoch=? AND kind=? "
                "AND source_id=? AND target_id=?",
                (*_key(key, work), format_scalar("nonnegative", epoch), kind, left, right),
            )
            is None
        ):
            raise StaleArchiveTask("journal union lost its last published arc")
    if (
        progress.seed_after is not None
        and _one(
            db,
            "SELECT 1 FROM discovery_journal_active_edges WHERE build_id=? "
            "AND admission_sha256=? AND journal_id=? AND active=1 AND assertion_id=?",
            (*_key(key, work), progress.seed_after),
        )
        is None
    ):
        raise StaleArchiveTask("journal union lost its active carry cursor")
    if progress.state == "complete":
        _graphs(db, key, work, progress.epoch, state="complete")
    return progress


def begin_discovery_journal_union(
    db: sqlite3.Connection, key: ArchiveTaskKey, work: DiscoveryJournalUnionWork
) -> DiscoveryJournalUnionProgress:
    if not db.in_transaction:
        raise ValueError("journal union initialization requires an owner transaction")
    original = _source(db, key, work)
    initial = DiscoveryJournalUnionProgress(
        state="deferred" if original.phase == "deferred" else "building"
    )
    db.execute(
        "INSERT OR IGNORE INTO discovery_journal_union VALUES("
        + ",".join("?" for _ in range(22))
        + ")",
        (
            *_key(key, work),
            key.worker_id,
            work.topology.epochs.source.binding.directory_id,
            EPOCH_UNION_ALGORITHM,
            work.binding_json,
            format_scalar("nonnegative", original.version),
            *_controls(initial),
        ),
    ).close()
    return discovery_journal_union_progress(db, key, work)


def _prepared(
    key: ArchiveTaskKey,
    expected: DiscoveryJournalUnionProgress,
    updated: DiscoveryJournalUnionProgress,
    *,
    graphs: tuple[DiscoveryUnionGraphPatch, ...] = (),
    delta: EpochUnionDelta | None = None,
    retired: tuple[str, ...] = (),
    work_items: int = 0,
) -> PreparedDiscoveryJournalUnion:
    return PreparedDiscoveryJournalUnion(
        key,
        expected,
        replace(updated, version=expected.version + 1),
        graphs,
        delta,
        retired,
        work_items,
    )


def _frame_step(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: DiscoveryJournalUnionWork,
    progress: DiscoveryJournalUnionProgress,
) -> PreparedDiscoveryJournalUnion:
    original = _source(db, key, work)
    if progress.frame == original.next_frame:
        if progress.consumed != original.checkpoint.facts:
            raise StaleArchiveTask("journal union source fact directory is incomplete")
        counts = _graphs(db, key, work, progress.epoch)
        patches = tuple(
            DiscoveryUnionGraphPatch(progress.epoch, kind, *counts[kind], "complete")
            for kind in GRAPH_KINDS
        )
        return _prepared(key, progress, replace(progress, state="complete"), graphs=patches)
    _, boundary = epochs.discovery_journal_epoch_frame(
        db, key, work.topology.epochs, progress.frame
    )
    initial = progress.frame == 0
    if initial and boundary.entry_kind != "journal_init":
        return _prepared(key, progress, replace(progress, state="deferred"))
    if boundary.epoch != progress.epoch + int(not initial and boundary.entry_kind == "correction"):
        raise StaleArchiveTask("journal union correction differs from the original boundary")
    patches = ()
    if initial or boundary.epoch != progress.epoch:
        if not initial:
            counts = _graphs(db, key, work, progress.epoch)
            patches = tuple(
                DiscoveryUnionGraphPatch(progress.epoch, kind, *counts[kind], "complete")
                for kind in GRAPH_KINDS
            )
        for kind in GRAPH_KINDS:
            if (
                _one(
                    db,
                    "SELECT 1 FROM discovery_journal_union_graphs WHERE build_id=? "
                    "AND admission_sha256=? AND journal_id=? AND epoch=? AND kind=?",
                    (*_key(key, work), format_scalar("nonnegative", boundary.epoch), kind),
                )
                is not None
            ):
                raise StaleArchiveTask("journal union encountered an initialized new epoch")
        patches += tuple(
            DiscoveryUnionGraphPatch(boundary.epoch, kind, 0, 0, "building", True)
            for kind in GRAPH_KINDS
        )
    else:
        _graphs(db, key, work, progress.epoch)
    return _prepared(
        key,
        progress,
        replace(
            progress,
            epoch=boundary.epoch,
            phase="retracts" if boundary.entry_kind == "correction" else "add",
            seed_after=None,
            seeded_count=0 if boundary.entry_kind == "correction" else progress.seeded_count,
        ),
        graphs=patches,
    )


def _active_seed(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: DiscoveryJournalUnionWork,
    progress: DiscoveryJournalUnionProgress,
) -> tuple[TopologyOccurrence, ...]:
    with closing(
        db.execute(
            "SELECT assertion_id,kind,source_id,target_id,frame,item "
            "FROM discovery_journal_active_edges INDEXED BY discovery_journal_active_seed "
            "WHERE build_id=? AND admission_sha256=? AND journal_id=? AND active=1 "
            "AND assertion_id>? ORDER BY assertion_id LIMIT 64",
            (*_key(key, work), progress.seed_after or ""),
        )
    ) as result:
        rows = tuple(result.fetchall())
    try:
        return tuple(
            TopologyOccurrence(
                row[0], row[1], row[2], row[3], _natural(row[4]), _natural(row[5])
            ).checked()
            for row in rows
        )
    except (ValueError, TypeError):
        raise StaleArchiveTask("journal union active carry controls changed") from None


@dataclass(frozen=True, slots=True)
class _Points:
    counts: dict[str, tuple[int, int]]
    assertions: dict[str, bool | None]
    arcs: set[ArcKey]
    degrees: dict[VertexKey, int | None]


def _points(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: DiscoveryJournalUnionWork,
    progress: DiscoveryJournalUnionProgress,
    edges: tuple[TopologyOccurrence, ...],
    identities: tuple[str, ...],
) -> _Points:
    counts = _graphs(db, key, work, progress.epoch)
    assertions: dict[str, bool | None] = {}
    for identity in dict.fromkeys(identities):
        row = _one(
            db,
            "SELECT active FROM discovery_journal_active_edges WHERE build_id=? "
            "AND admission_sha256=? AND journal_id=? AND assertion_id=?",
            (*_key(key, work), identity),
        )
        assertions[identity] = None if row is None else bool(row[0])
    existing_arcs, checked = set(), set()
    degrees: dict[VertexKey, int | None] = {}
    for edge in edges:
        arc = (edge.kind, edge.source_id, edge.target_id)
        if arc not in checked:
            if (
                _one(
                    db,
                    "SELECT 1 FROM discovery_journal_union_arcs WHERE build_id=? "
                    "AND admission_sha256=? AND journal_id=? AND epoch=? AND kind=? "
                    "AND source_id=? AND target_id=?",
                    (*_key(key, work), format_scalar("nonnegative", progress.epoch), *arc),
                )
                is not None
            ):
                existing_arcs.add(arc)
            checked.add(arc)
        for vertex in ((edge.kind, edge.source_id), (edge.kind, edge.target_id)):
            if vertex not in degrees:
                row = _one(
                    db,
                    "SELECT degree FROM discovery_journal_union_vertices WHERE build_id=? "
                    "AND admission_sha256=? AND journal_id=? AND epoch=? "
                    "AND kind=? AND vertex_id=?",
                    (*_key(key, work), format_scalar("nonnegative", progress.epoch), *vertex),
                )
                degrees[vertex] = None if row is None else _natural(row[0])
    return _Points(counts, assertions, existing_arcs, degrees)


def _row_step(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: DiscoveryJournalUnionWork,
    progress: DiscoveryJournalUnionProgress,
) -> PreparedDiscoveryJournalUnion:
    carried = progress.phase == "seed"
    rows = (
        ()
        if carried
        else topology.discovery_journal_topology_page(
            db,
            key,
            work.topology,
            progress.consumed,
            frame=progress.frame,
            array="retracts" if progress.phase == "retracts" else "relations",
        )
    )
    try:
        edges = (
            _active_seed(db, key, work, progress)
            if carried
            else tuple(
                TopologyOccurrence(
                    row.item.assertion_id,
                    row.item.kind,
                    row.item.source_id or "",
                    row.item.target_id or "",
                    row.frame,
                    row.item.index,
                ).checked()
                for row in rows
                if progress.phase == "add"
            )
        )
        identities = (
            tuple(edge.assertion_id for edge in edges)
            if progress.phase == "add"
            else tuple(require_canonical_uuid_urn(row.item.assertion_id) for row in rows)
        )
    except (ValueError, TypeError):
        return _prepared(key, progress, replace(progress, state="deferred"))
    points = _points(db, key, work, progress, edges, identities)
    count = len(edges) if carried else len(rows)
    updated = progress
    if rows:
        updated = replace(
            progress,
            consumed=progress.consumed + len(rows),
            last_source_sha256=rows[-1].item.json_sha256,
        )
    if progress.phase == "retracts":
        retired, seen = [], set()
        for identity in identities:
            if identity in seen or points.assertions[identity] is False:
                return _prepared(key, progress, replace(progress, state="deferred"))
            seen.add(identity)
            if points.assertions[identity] is True:
                retired.append(identity)
        updated = replace(updated, active_count=progress.active_count - len(retired))
        if updated.active_count < 0:
            raise StaleArchiveTask("journal union active identity count changed")
        if count < 64:
            updated = replace(updated, phase="seed", seed_after=None)
        return _prepared(key, progress, updated, retired=tuple(retired), work_items=count)
    try:
        delta = plan_epoch_union_additions(
            edges,
            known_assertions={
                identity for identity, active in points.assertions.items() if active is not None
            },
            existing_arcs=points.arcs,
            degrees=points.degrees,
            counts=points.counts,
            carried=carried,
        )
    except ValueError:
        return _prepared(key, progress, replace(progress, state="deferred"))
    if not carried:
        updated = replace(updated, active_count=progress.active_count + len(delta.assertions))
    if delta.arcs:
        updated = replace(updated, last_arc=(progress.epoch, *delta.arcs[-1]))
    if carried:
        updated = replace(
            updated,
            seed_after=edges[-1].assertion_id if edges else progress.seed_after,
            seeded_count=progress.seeded_count + len(edges),
        )
        if (
            updated.seeded_count > updated.active_count
            or count < 64
            and updated.seeded_count != updated.active_count
        ):
            raise StaleArchiveTask("journal union active seed directory is incomplete")
        if count < 64:
            updated = replace(updated, phase="add", seed_after=None)
    elif count < 64:
        updated = replace(updated, frame=progress.frame + 1, phase="frame", seed_after=None)
        original = _source(db, key, work)
        if updated.frame == original.next_frame:
            if updated.consumed != original.checkpoint.facts:
                raise StaleArchiveTask("journal union source fact directory is incomplete")
            updated = replace(updated, state="complete")
    patches = tuple(
        DiscoveryUnionGraphPatch(
            progress.epoch,
            kind,
            *delta.counts[kind],
            "complete" if updated.state == "complete" else "building",
        )
        for kind in GRAPH_KINDS
    )
    return _prepared(key, progress, updated, graphs=patches, delta=delta, work_items=count)


def prepare_discovery_journal_union(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: DiscoveryJournalUnionWork,
) -> PreparedDiscoveryJournalUnion | None:
    if db.in_transaction:
        raise ValueError("journal union preparation requires no write transaction")
    progress = discovery_journal_union_progress(db, key, work)
    if progress.state != "building":
        return None
    cached = work._prepared
    if cached is not None and cached.key == key and cached.expected == progress:
        return cached
    prepared = (
        _frame_step(db, key, work, progress)
        if progress.phase == "frame"
        else _row_step(db, key, work, progress)
    )
    work._prepared = prepared
    return prepared


def apply_discovery_journal_union(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: DiscoveryJournalUnionWork,
    prepared: PreparedDiscoveryJournalUnion,
) -> bool:
    if not db.in_transaction:
        raise ValueError("journal union publication requires an owner transaction")
    if prepared is not work._prepared or prepared.key != key:
        raise StaleArchiveTask("journal union publication has no owned prepared atom")
    if discovery_journal_union_progress(db, key, work) != prepared.expected:
        return False
    prefix = _key(key, work)
    for patch in prepared.graphs:
        values = (
            format_scalar("nonnegative", patch.vertices),
            format_scalar("nonnegative", patch.arcs),
            patch.state,
        )
        graph = (*prefix, format_scalar("nonnegative", patch.epoch), patch.kind)
        if patch.new:
            db.execute(
                "INSERT INTO discovery_journal_union_graphs VALUES(?,?,?,?,?,?,?,?)",
                (*graph, *values),
            ).close()
        else:
            with closing(
                db.execute(
                    "UPDATE discovery_journal_union_graphs SET vertices=?,arcs=?,state=? "
                    "WHERE build_id=? AND admission_sha256=? AND journal_id=? AND epoch=? "
                    "AND kind=? AND state='building'",
                    (*values, *graph),
                )
            ) as result:
                if result.rowcount != 1:
                    raise StaleArchiveTask("journal union publication lost its open graph")
    for identity in prepared.retired:
        with closing(
            db.execute(
                "UPDATE discovery_journal_active_edges SET active=0 WHERE build_id=? "
                "AND admission_sha256=? AND journal_id=? AND assertion_id=? AND active=1",
                (*prefix, identity),
            )
        ) as result:
            if result.rowcount != 1:
                raise StaleArchiveTask("journal union retirement lost its active identity")
    if prepared.delta is not None:
        for edge in prepared.delta.assertions:
            db.execute(
                "INSERT INTO discovery_journal_active_edges VALUES(?,?,?,?,?,?,?,?,?,1)",
                (
                    *prefix,
                    edge.assertion_id,
                    edge.kind,
                    edge.source_id,
                    edge.target_id,
                    format_scalar("nonnegative", edge.frame_ordinal),
                    format_scalar("nonnegative", edge.item_ordinal),
                ),
            ).close()
        for kind, left, right in prepared.delta.arcs:
            db.execute(
                "INSERT INTO discovery_journal_union_arcs VALUES(?,?,?,?,?,?,?)",
                (*prefix, format_scalar("nonnegative", prepared.expected.epoch), kind, left, right),
            ).close()
        for (kind, vertex), degree in prepared.delta.vertices:
            db.execute(
                "INSERT INTO discovery_journal_union_vertices VALUES(?,?,?,?,?,?,?) "
                "ON CONFLICT(build_id,admission_sha256,journal_id,epoch,kind,vertex_id) "
                "DO UPDATE SET degree=excluded.degree",
                (
                    *prefix,
                    format_scalar("nonnegative", prepared.expected.epoch),
                    kind,
                    vertex,
                    format_scalar("nonnegative", degree),
                ),
            ).close()
    with closing(
        db.execute(
            "UPDATE discovery_journal_union SET "
            + ",".join(name + "=?" for name in _COLUMNS)
            + " WHERE build_id=? AND admission_sha256=? AND journal_id=? AND "
            + " AND ".join(name + " IS ?" for name in _COLUMNS),
            (*_controls(prepared.progress), *prefix, *_controls(prepared.expected)),
        )
    ) as result:
        if result.rowcount != 1:
            raise StaleArchiveTask("journal union publication lost its exact checkpoint")
    work._prepared = None
    return True


declare_growth_for_journal_union(globals())
