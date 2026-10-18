"""Bounded, source-fenced private graph unions for real journal epochs."""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from typing import Any

from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)
from riverhog_provenance.epoch_union import (
    EPOCH_UNION_ALGORITHM,
    GRAPH_KINDS,
    ArcKey,
    EpochUnionDelta,
    TopologyOccurrence,
    VertexKey,
    plan_epoch_union_additions,
)
from riverhog_provenance_contracts import require_canonical_uuid_urn
from sqlalchemy import select, update
from sqlalchemy.orm import Session
from state_schema import read_snapshot

from riverhog_core.catalog_db import SessionFactory
from riverhog_core.catalog_models import CollectionUploadProvenanceActiveEdgeRecord as Active
from riverhog_core.catalog_models import CollectionUploadProvenanceEpochArcRecord as Arc
from riverhog_core.catalog_models import CollectionUploadProvenanceEpochGraphRecord as Graph
from riverhog_core.catalog_models import CollectionUploadProvenanceEpochScanRecord as Scan
from riverhog_core.catalog_models import CollectionUploadProvenanceEpochVertexRecord as Vertex
from riverhog_core.catalog_models import CollectionUploadProvenanceFrameRecord as Frame
from riverhog_core.catalog_models import CollectionUploadProvenanceJournalRecord as Journal
from riverhog_core.catalog_models import CollectionUploadProvenanceTopologyFactRecord as Fact

from . import collection_uploads as source_owner
from . import upload_journal_topology as facts_owner

_BUDGET = 64


@dataclass(frozen=True, slots=True)
class _Header:
    epoch: int = 0
    phase: str = "frame"
    item: int = 0
    seed_after: str | None = None
    consumed: int = 0
    last_source: tuple[int, str, int, str] | None = None
    last_arc: tuple[int, str, str, str] | None = None
    active_count: int = 0
    seeded_count: int = 0


def _encode(header: _Header) -> str:
    return canonical_json_bytes(
        {
            "active": str(header.active_count),
            "seeded": str(header.seeded_count),
            "epoch": str(header.epoch),
            "phase": header.phase,
            "item": str(header.item),
            "seed_after": header.seed_after,
            "consumed": str(header.consumed),
            "last_source": None
            if header.last_source is None
            else [
                str(header.last_source[0]),
                header.last_source[1],
                str(header.last_source[2]),
                header.last_source[3],
            ],
            "last_arc": None
            if header.last_arc is None
            else [str(header.last_arc[0]), *header.last_arc[1:]],
        }
    ).decode()


def _decode(raw: str) -> _Header:
    try:
        value = json.loads(raw)
        if (
            not isinstance(value, dict)
            or set(value)
            != {
                "active",
                "seeded",
                "epoch",
                "phase",
                "item",
                "seed_after",
                "consumed",
                "last_source",
                "last_arc",
            }
            or value["phase"] not in {"frame", "retracts", "seed", "add"}
        ):
            raise ValueError("invalid private union checkpoint shape")
        after = value["seed_after"]
        if after is not None:
            require_canonical_uuid_urn(after)
        last = value["last_source"]
        if last is not None:
            if (
                not isinstance(last, list)
                or len(last) != 4
                or last[1] not in {"relations", "retracts"}
            ):
                raise ValueError("invalid private union source marker")
            last = (
                facts_owner._number(last[0]),
                last[1],
                facts_owner._number(last[2]),
                facts_owner._digest(last[3]),
            )
        arc = value["last_arc"]
        if arc is not None:
            if not isinstance(arc, list) or len(arc) != 4 or arc[1] not in GRAPH_KINDS:
                raise ValueError("invalid private union arc marker")
            arc = (
                facts_owner._number(arc[0]),
                arc[1],
                require_canonical_uuid_urn(arc[2]),
                require_canonical_uuid_urn(arc[3]),
            )
        header = _Header(
            facts_owner._number(value["epoch"]),
            value["phase"],
            facts_owner._number(value["item"]),
            after,
            facts_owner._number(value["consumed"]),
            last,
            arc,
            facts_owner._number(value["active"]),
            facts_owner._number(value["seeded"]),
        )
        if (
            (header.consumed == 0) != (last is None)
            or _encode(header) != raw
            or not (0 <= header.seeded_count <= header.active_count <= header.consumed)
        ):
            raise ValueError("inconsistent private union checkpoint")
        return header
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError) as exc:
        raise RuntimeError("invalid retained epoch union checkpoint") from exc


@dataclass(frozen=True, slots=True)
class _Work:
    source: source_owner._UploadJournalValidationWork
    version: int
    frame: int
    state: str
    cursor_json: str
    header: _Header
    fact_count: int


def _prefix(model: Any, source: source_owner._UploadJournalValidationWork) -> tuple[Any, ...]:
    return (
        model.collection_id == source.collection_id,
        model.journal_id == source.journal_id,
        model.attempt_id == source.attempt_id,
    )


def _key(source: source_owner._UploadJournalValidationWork, *parts: Any) -> tuple[Any, ...]:
    return (source.collection_id, source.journal_id, source.attempt_id, *parts)


def _fence(work: _Work) -> tuple[Any, ...]:
    source = work.source
    return (
        *_prefix(Scan, source),
        Scan.directory_id == source.directory_id,
        Scan.union_algorithm == EPOCH_UNION_ALGORITHM,
        Scan.version == work.version,
        Scan.union_next_frame == work.frame,
        Scan.union_state == work.state,
        Scan.union_cursor_json == work.cursor_json,
        Scan.state == "complete",
        Scan.topology_state == "complete",
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
    with factory() as session, session.begin():
        if (
            session.scalar(
                select(Journal.collection_id)
                .where(*source_owner._upload_journal_validation_fence(source))
                .with_for_update(skip_locked=True)
            )
            is None
        ):
            return None
        row = session.get(Scan, (source.collection_id, source.journal_id), with_for_update=True)
        if row is None or row.attempt_id != source.attempt_id:
            return None
        if row.directory_id != source.directory_id or row.union_algorithm != EPOCH_UNION_ALGORITHM:
            raise RuntimeError("epoch union checkpoint binding differs from its source")
        header = _decode(row.union_cursor_json)
        fact_count = facts_owner._decode(row.topology_cursor_json).facts
        work = _Work(
            source,
            row.version,
            row.union_next_frame,
            row.union_state,
            row.union_cursor_json,
            header,
            fact_count,
        )
        if (
            not 0 <= work.frame <= source.cursor.frame_count
            or not 0 <= header.consumed <= fact_count
        ):
            raise RuntimeError("epoch union checkpoint extent differs from its source")
        if work.state == "complete" and (
            work.frame != source.cursor.frame_count
            or header.phase != "frame"
            or header.consumed != fact_count
        ):
            raise RuntimeError("completed epoch union checkpoint differs from its source")
        if header.last_source is not None:
            frame, array, item, digest = header.last_source
            fact = session.get(Fact, _key(source, frame, array, item))
            if (
                frame > work.frame
                or fact is None
                or fact.directory_id != source.directory_id
                or fact.json_sha256 != digest
            ):
                raise RuntimeError("epoch union checkpoint lost its last source fact")
        if header.last_arc is not None and session.get(Arc, _key(source, *header.last_arc)) is None:
            raise RuntimeError("epoch union checkpoint lost its last published arc")
        if row.state == "deferred" or row.topology_state == "deferred":
            return replace(work, state="deferred")
        if row.state != "complete" or row.topology_state != "complete":
            return None
        return work


@dataclass(frozen=True, slots=True)
class _GraphPatch:
    epoch: int
    kind: str
    vertices: int
    arcs: int
    state: str
    new: bool = False


@dataclass(frozen=True, slots=True)
class _Advance:
    frame: int
    state: str
    header: _Header
    graphs: tuple[_GraphPatch, ...] = ()
    delta: EpochUnionDelta | None = None
    retired: tuple[str, ...] = ()
    work_items: int = 0


def _graphs(session: Session, work: _Work) -> dict[str, tuple[int, int]]:
    counts = {}
    for kind in GRAPH_KINDS:
        row = session.get(Graph, _key(work.source, work.header.epoch, kind))
        if row is None or row.state != "building":
            raise RuntimeError("epoch union lost its open graph counters")
        counts[kind] = (row.vertex_count, row.arc_count)
    return counts


def _frame_step(factory: SessionFactory, work: _Work) -> _Advance | None:
    source, header = work.source, work.header
    initial = work.frame == 0 and header.last_source is None and header.last_arc is None
    with read_snapshot(factory) as session:
        if not _current(session, work):
            return None
        if work.frame == source.cursor.frame_count:
            counts = _graphs(session, work)
            if header.consumed != work.fact_count:
                raise RuntimeError("epoch union source fact directory is incomplete")
            patches = tuple(
                _GraphPatch(header.epoch, kind, *counts[kind], "complete") for kind in GRAPH_KINDS
            )
            return _Advance(work.frame, "complete", header, patches)
        frame = session.get(
            Frame, (source.collection_id, source.journal_id, source.directory_id, work.frame)
        )
        if (
            frame is None
            or frame.epoch_attempt_id != source.attempt_id
            or frame.epoch_ordinal is None
        ):
            raise RuntimeError("epoch union lost its captured frame boundary")
        epoch, kind = frame.epoch_ordinal, frame.epoch_entry_kind
        if initial and kind != "journal_init":
            return _Advance(work.frame, "deferred", header)
        if epoch != header.epoch + int(not initial and kind == "correction"):
            raise RuntimeError("epoch union correction boundary differs from its source")
        patches = ()
        if initial or epoch != header.epoch:
            if not initial:
                counts = _graphs(session, work)
                patches = tuple(
                    _GraphPatch(header.epoch, name, *counts[name], "complete")
                    for name in GRAPH_KINDS
                )
            for name in GRAPH_KINDS:
                if session.get(Graph, _key(source, epoch, name)) is not None:
                    raise RuntimeError("epoch union encountered an already initialized epoch")
            patches += tuple(
                _GraphPatch(epoch, name, 0, 0, "building", True) for name in GRAPH_KINDS
            )
        else:
            _graphs(session, work)
    return _Advance(
        work.frame,
        "building",
        replace(
            header,
            epoch=epoch,
            phase="retracts" if kind == "correction" else "add",
            item=0,
            seed_after=None,
            seeded_count=0 if kind == "correction" else header.seeded_count,
        ),
        patches,
    )


def _fact_rows(factory: SessionFactory, work: _Work) -> tuple[Fact, ...] | None:
    array = "retracts" if work.header.phase == "retracts" else "relations"
    with read_snapshot(factory) as session:
        if not _current(session, work):
            return None
        with session.scalars(
            select(Fact)
            .where(
                *_prefix(Fact, work.source),
                Fact.frame_ordinal == work.frame,
                Fact.array_kind == array,
                Fact.item_ordinal >= work.header.item,
            )
            .order_by(Fact.item_ordinal)
            .limit(_BUDGET)
        ) as rows:
            values = tuple(rows)
    if any(
        row.directory_id != work.source.directory_id or row.epoch != work.header.epoch
        for row in values
    ):
        raise RuntimeError("epoch union fact binding differs from its frame")
    return values


@declare_growth(
    Declaration(
        id="riverhog-server.sqlite-epoch-active-seed.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.services.upload_journal_union._active_seed",),
        scope="native-sqlite-successful-fenced-active-edge-keyset-page",
        starts_at="probe the exact source/cursor then seek the active documentary edge index",
        ends_at="return a detached bounded page after closing snapshot resources",
        dimensions=(
            Dimension(
                "retained_assertions", COUNT, "All active and retired private edge identities."
            ),
            Dimension("requested_rows", COUNT, "Maximum active rows admitted by this call."),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "equal",
                Expression("constant", value=2, unit=COUNT),
                "One fence probe and one indexed active-page query.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                Expression("dimension", name="requested_rows"),
                "At most the requested active identity rows.",
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "Seeding uses documentary metadata, never source payload.",
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
                                                Expression("dimension", name="retained_assertions"),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Maintained native instruction envelope for the selected active-row keyset plan.",
            ),
        ),
        conditions=(
            "The owning active-identity index and exact source/attempt/cursor fences are present.",
            "Requested rows are positive and at most 64; identity keys use the native UUID domain.",
        ),
        invariants=(
            "Each real correction seeds carried active identities once, "
            "preserving parallel assertions.",
            "These private graph rows never authorize correction or semantic acceptance.",
        ),
        limitations=(
            "Point graph reads, unique-arc writes, degree changes, durable checkpoints "
            "and cleanup are separate costs.",
            "SQLite instructions exclude internal B-tree comparisons, page I/O and CPU; "
            "other engines require their own qualification.",
        ),
    )
)
def _active_seed(
    factory: SessionFactory, work: _Work, *, row_budget: int = _BUDGET
) -> tuple[TopologyOccurrence, ...] | None:
    if type(row_budget) is not int or not 1 <= row_budget <= _BUDGET:
        raise ValueError("invalid private active seed budget")
    with read_snapshot(factory) as session:
        if not _current(session, work):
            return None
        with session.execute(
            select(
                Active.assertion_id,
                Active.kind,
                Active.source_id,
                Active.target_id,
                Active.frame_ordinal,
                Active.item_ordinal,
            )
            .where(
                *_prefix(Active, work.source),
                Active.active.is_(True),
                Active.assertion_id > (work.header.seed_after or ""),
            )
            .order_by(Active.assertion_id)
            .limit(row_budget)
        ) as rows:
            return tuple(TopologyOccurrence(*row) for row in rows)


@dataclass(frozen=True, slots=True)
class _Points:
    counts: dict[str, tuple[int, int]]
    assertions: dict[str, bool | None]
    arcs: set[ArcKey]
    degrees: dict[VertexKey, int | None]


def _points(
    factory: SessionFactory,
    work: _Work,
    edges: tuple[TopologyOccurrence, ...],
    identities: tuple[str, ...],
) -> _Points | None:
    with read_snapshot(factory) as session:
        if not _current(session, work):
            return None
        counts = _graphs(session, work)
        assertions = {}
        for identity in dict.fromkeys(identities):
            active_row = session.get(Active, _key(work.source, identity))
            assertions[identity] = None if active_row is None else active_row.active
        arcs: set[ArcKey] = set()
        checked_arcs = set()
        degrees: dict[VertexKey, int | None] = {}
        for edge in edges:
            arc = (edge.kind, edge.source_id, edge.target_id)
            if arc not in checked_arcs:
                if session.get(Arc, _key(work.source, work.header.epoch, *arc)) is not None:
                    arcs.add(arc)
                checked_arcs.add(arc)
            for vertex in ((edge.kind, edge.source_id), (edge.kind, edge.target_id)):
                if vertex not in degrees:
                    vertex_row = session.get(Vertex, _key(work.source, work.header.epoch, *vertex))
                    degrees[vertex] = None if vertex_row is None else vertex_row.indegree
    return _Points(counts, assertions, arcs, degrees)


def _row_step(factory: SessionFactory, work: _Work) -> _Advance | None:
    header = work.header
    carried = header.phase == "seed"
    rows = None if carried else _fact_rows(factory, work)
    if not carried and rows is None:
        return None
    try:
        edges = (
            _active_seed(factory, work)
            if carried
            else tuple(
                TopologyOccurrence(
                    row.assertion_id,
                    row.kind,
                    row.source_id or "",
                    row.target_id or "",
                    row.frame_ordinal,
                    row.item_ordinal,
                ).checked()
                for row in (rows or ())
                if header.phase == "add"
            )
        )
        if edges is None:
            return None
        if carried:
            for edge in edges:
                edge.checked()
        identities = (
            tuple(edge.assertion_id for edge in edges)
            if header.phase == "add"
            else tuple(require_canonical_uuid_urn(row.assertion_id) for row in (rows or ()))
        )
    except (ValueError, TypeError, AttributeError) as exc:
        if carried:
            raise RuntimeError("private active edge ledger has invalid identities") from exc
        return _Advance(work.frame, "deferred", header)
    points = _points(factory, work, edges, identities)
    if points is None:
        return None
    count = len(edges) if carried else len(rows or ())
    updated = header
    if rows:
        last = rows[-1]
        updated = replace(
            header,
            consumed=header.consumed + len(rows),
            item=last.item_ordinal + 1,
            last_source=(work.frame, last.array_kind, last.item_ordinal, last.json_sha256),
        )
    if header.phase == "retracts":
        retired = []
        seen = set()
        for identity in identities:
            if identity in seen or points.assertions[identity] is False:
                return _Advance(work.frame, "deferred", header)
            seen.add(identity)
            if points.assertions[identity] is True:
                retired.append(identity)
        updated = replace(updated, active_count=header.active_count - len(retired))
        if updated.active_count < 0:
            raise RuntimeError("epoch union active identity counter is inconsistent")
        if count < _BUDGET:
            updated = replace(updated, phase="seed", item=0, seed_after=None)
        return _Advance(work.frame, "building", updated, retired=tuple(retired), work_items=count)
    try:
        delta = plan_epoch_union_additions(
            edges,
            known_assertions={key for key, value in points.assertions.items() if value is not None},
            existing_arcs=points.arcs,
            degrees=points.degrees,
            counts=points.counts,
            carried=carried,
        )
    except ValueError:
        return _Advance(work.frame, "deferred", header)
    if not carried:
        updated = replace(updated, active_count=header.active_count + len(delta.assertions))
    if delta.arcs:
        updated = replace(updated, last_arc=(header.epoch, *delta.arcs[-1]))
    frame, state = work.frame, "building"
    if carried:
        updated = replace(
            updated,
            seed_after=edges[-1].assertion_id if edges else header.seed_after,
            seeded_count=header.seeded_count + len(edges),
        )
        if updated.seeded_count > updated.active_count or (
            count < _BUDGET and updated.seeded_count != updated.active_count
        ):
            raise RuntimeError("epoch union active seed directory is incomplete")
        if count < _BUDGET:
            updated = replace(updated, phase="add", item=0, seed_after=None)
    elif count < _BUDGET:
        frame += 1
        updated = replace(updated, phase="frame", item=0, seed_after=None)
        if frame == work.source.cursor.frame_count:
            if updated.consumed != work.fact_count:
                raise RuntimeError("epoch union source fact directory is incomplete")
            state = "complete"
    patches = tuple(
        _GraphPatch(
            header.epoch,
            kind,
            *delta.counts[kind],
            "complete" if state == "complete" else "building",
        )
        for kind in GRAPH_KINDS
    )
    return _Advance(frame, state, updated, patches, delta, work_items=count)


def _compute(factory: SessionFactory, work: _Work) -> _Advance | None:
    return _frame_step(factory, work) if work.header.phase == "frame" else _row_step(factory, work)


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
                version=work.version + 1,
                union_next_frame=advance.frame,
                union_state=advance.state,
                union_cursor_json=_encode(advance.header),
            )
            .returning(Scan.collection_id)
        ).scalar_one_or_none()
        if changed is None:
            return False
        base = {
            "collection_id": source.collection_id,
            "journal_id": source.journal_id,
            "attempt_id": source.attempt_id,
        }
        for patch in advance.graphs:
            values = {"vertex_count": patch.vertices, "arc_count": patch.arcs, "state": patch.state}
            if patch.new:
                session.add(Graph(**base, epoch=patch.epoch, kind=patch.kind, **values))
            else:
                changed = session.execute(
                    update(Graph)
                    .where(
                        *_prefix(Graph, source),
                        Graph.epoch == patch.epoch,
                        Graph.kind == patch.kind,
                        Graph.state == "building",
                    )
                    .values(**values)
                    .returning(Graph.epoch)
                ).scalar_one_or_none()
                if changed is None:
                    raise RuntimeError("epoch union publication lost its open graph")
        for identity in advance.retired:
            retired_identity = session.execute(
                update(Active)
                .where(
                    *_prefix(Active, source),
                    Active.assertion_id == identity,
                    Active.active.is_(True),
                )
                .values(active=False)
                .returning(Active.assertion_id)
            ).scalar_one_or_none()
            if retired_identity is None:
                raise RuntimeError("epoch union retirement lost its active documentary identity")
        if advance.delta is not None:
            for edge in advance.delta.assertions:
                session.add(
                    Active(
                        **base,
                        assertion_id=edge.assertion_id,
                        kind=edge.kind,
                        source_id=edge.source_id,
                        target_id=edge.target_id,
                        frame_ordinal=edge.frame_ordinal,
                        item_ordinal=edge.item_ordinal,
                        active=True,
                    )
                )
            for kind, left, right in advance.delta.arcs:
                session.add(
                    Arc(**base, epoch=work.header.epoch, kind=kind, source_id=left, target_id=right)
                )
            for (kind, vertex), degree in advance.delta.vertices:
                row = session.get(Vertex, _key(source, work.header.epoch, kind, vertex))
                if row is None:
                    session.add(
                        Vertex(
                            **base,
                            epoch=work.header.epoch,
                            kind=kind,
                            vertex_id=vertex,
                            indegree=degree,
                        )
                    )
                else:
                    row.indegree = degree
        session.flush()
    return True


def advance_upload_journal_epoch_union(
    factory: SessionFactory, source: source_owner._UploadJournalValidationWork
) -> bool:
    """Publish one bounded edge page, with at most four empty phase transitions."""
    for _ in range(4):
        work = _begin(factory, source)
        if work is None:
            return False
        if work.state in {"complete", "deferred"}:
            return True
        advance = _compute(factory, work)
        if advance is None or not _commit(factory, work, advance):
            return False
        if advance.state in {"complete", "deferred"}:
            return True
        if advance.work_items:
            return False
    return False
