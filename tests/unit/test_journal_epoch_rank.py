from __future__ import annotations

import hashlib
import sqlite3
import subprocess
import sys
from contextlib import contextmanager
from dataclasses import replace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.services import upload_journal_rank as rank
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import parse_journal, validate_journal
from riverhog_provenance.epoch_rank import decrement_rank_degrees
from sqlalchemy import delete, event, select, update

from tests.unit.test_journal_capture import retained
from tests.unit.test_journal_epoch_scan import ObservedWindow
from tests.unit.test_journal_epoch_union import correction, edge, raw_edges, ready, uid
from tests.unit.test_journal_epoch_union import finish as finish_union
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_journal_topology_capture import entry, raw_journal
from tests.unit.test_journal_validation_handoff import native_outcome, service_for, terminal
from tests.unit.test_staged_journal_range import JOURNAL, Journal

Scan, Ranked, Result = rank.Scan, rank.Ranked, rank.Result


def prepared(factory):
    source_work = ready(factory)
    finish_union(factory, source_work)
    return source_work


def checkpoint(factory):
    with factory() as session:
        row = session.get(Scan, (1, JOURNAL))
        return row.version, row.rank_state, row.rank_cursor_json


def snapshot(factory):
    with factory() as session:
        return {
            "vertices": [
                (row.epoch, row.kind, row.vertex_id, row.remaining_degree, row.rank, row.ready)
                for row in session.scalars(
                    select(Ranked).order_by(Ranked.epoch, Ranked.kind, Ranked.vertex_id)
                )
            ],
            "graphs": [
                (
                    row.epoch,
                    row.kind,
                    row.vertex_count,
                    row.arc_count,
                    row.settled_vertices,
                    row.visited_arcs,
                    row.state,
                )
                for row in session.scalars(select(Result).order_by(Result.epoch, Result.kind))
            ],
        }


def finish(factory, source_work):
    for calls in range(1, 5000):
        done = rank.advance_upload_journal_epoch_rank(factory, source_work)
        assert terminal(factory)["state"] == "validating" and terminal(factory)["anchor"] is None
        if done:
            return calls
    pytest.fail("bounded rank work stopped progressing")


def at_phase(factory, source_work, phase):
    for _ in range(5000):
        work = rank._begin(factory, source_work)
        if work.header.phase == phase or (
            phase == "result" and work.header.phase == "pop" and work.header.ready == 0
        ):
            return work
        advance = rank._compute(factory, work)
        assert rank._commit(factory, work, advance)
    pytest.fail("rank phase was not reached")


@pytest.mark.parametrize("shape", ["deep", "wide", "parallel", "cycle", "self"])
def test_private_ranks_cover_unique_arcs_or_quarantine_the_cyclic_union(
    tmp_path, shape, monkeypatch
):
    rows = {
        "deep": [edge(n, n, n + 1) for n in range(129)],
        "wide": [edge(n) for n in range(1025)],
        "parallel": [
            edge(0, 0, 1),
            edge(1, 0, 1),
            edge(2, 1, 2),
            edge(3, 2, 0, kind="specialization"),
        ],
        "cycle": [edge(0, 0, 1), edge(1, 1, 0)],
        "self": [edge(0, 0, 0)],
    }[shape]
    with retained(tmp_path, raw_edges(rows)) as (engine, factory):
        source_work = prepared(factory)
        reads, writes, detached = [], [], []
        original_page, original_commit, original_decrement = (
            rank._page,
            rank._commit,
            rank.decrement_rank_degrees,
        )

        def page(factory, work, **kwargs):
            result = original_page(factory, work, **kwargs)
            assert engine.pool.checkedout() == 0
            reads.append(
                (work.header.phase, work.header.active, work.header.edge_after, len(result))
            )
            return result

        def commit(factory, work, advance):
            assert len(advance.changes) <= 64 and advance.work_items <= 64
            writes.append(advance.work_items)
            return original_commit(factory, work, advance)

        def decrement(*args):
            assert engine.pool.checkedout() == 0
            detached.append(True)
            return original_decrement(*args)

        monkeypatch.setattr(rank, "_page", page)
        monkeypatch.setattr(rank, "_commit", commit)
        monkeypatch.setattr(rank, "decrement_rank_degrees", decrement)
        finish(factory, source_work)
        data = snapshot(factory)
        assert checkpoint(factory)[1] == "complete"
        expected = "cyclic" if shape in {"cycle", "self"} else "acyclic"
        assert data["graphs"][0][-1] == expected and data["graphs"][1][-1] == "acyclic"
        positions = {
            (epoch, kind, vertex): position
            for epoch, kind, vertex, _degree, position, _ready in data["vertices"]
        }
        with factory() as session:
            for arc in session.scalars(select(rank.Arc)):
                if expected == "acyclic":
                    assert (
                        positions[(arc.epoch, arc.kind, arc.source_id)]
                        < positions[(arc.epoch, arc.kind, arc.target_id)]
                    )
            for graph in session.scalars(select(rank.Graph)):
                result = session.get(
                    Result, (1, JOURNAL, source_work.attempt_id, graph.epoch, graph.kind)
                )
                assert (result.vertex_count, result.arc_count) == (
                    graph.vertex_count,
                    graph.arc_count,
                )
                if result.state == "acyclic":
                    assert (
                        result.settled_vertices == graph.vertex_count
                        and result.visited_arcs == graph.arc_count
                    )
        assert max(writes) <= 64 and all(count <= 64 for _phase, _active, _after, count in reads)
        if shape == "wide":
            adjacency = [
                (active, after, count)
                for phase, active, after, count in reads
                if phase == "edges" and active == uid(0)
            ]
            assert len(adjacency) >= 17 and sum(row[2] for row in adjacency) == 1025
            assert adjacency[0][1] is None and all(
                after is not None for _active, after, _count in adjacency[1:]
            )
        assert engine.pool.checkedout() == 0


def test_real_corrections_keep_legal_reversal_and_earlier_cycle_in_separate_results(tmp_path):
    raw = raw_journal(
        entry([edge(0, 0, 1)], kind="journal_init"),
        correction([0], [edge(1, 1, 0)]),
        entry([edge(2, 0, 1)]),
        correction([1]),
    )
    with retained(tmp_path, raw) as (_, factory):
        source_work = prepared(factory)
        finish(factory, source_work)
        assert [
            (epoch, state)
            for epoch, kind, *_counts, state in snapshot(factory)["graphs"]
            if kind == "derivation"
        ] == [(0, "acyclic"), (1, "cyclic"), (2, "acyclic")]


@pytest.mark.parametrize("phase", ["graph", "seed", "pop", "edges", "result"])
@pytest.mark.parametrize("committed", [False, True])
def test_process_death_preserves_only_committed_rank_work(tmp_path, phase, committed):
    with retained(tmp_path, raw_edges([edge(n) for n in range(129)])) as (engine, factory):
        source_work = prepared(factory)
        at_phase(factory, source_work, phase)
        before = checkpoint(factory), snapshot(factory)
        code = """import os,sys
from sqlalchemy import create_engine,event
from sqlalchemy.orm import sessionmaker
from riverhog_core.services import collection_uploads as owner
from riverhog_core.services import upload_journal_rank as rank
engine=create_engine(sys.argv[1])
factory=sessionmaker(engine)
source=owner._read_upload_journal_validation(factory,1,sys.argv[2])
work=rank._begin(factory,source)
advance=rank._compute(factory,work)
if sys.argv[3]=='before':
    event.listen(engine,'commit',lambda _connection: os._exit(73))
assert rank._commit(factory,work,advance)
os._exit(73)
"""
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                code,
                str(engine.url),
                JOURNAL,
                "after" if committed else "before",
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 73, result.stderr
        engine.dispose()
        if not committed:
            assert (checkpoint(factory), snapshot(factory)) == before
        else:
            assert checkpoint(factory)[0] == before[0][0] + 1
            header = rank._decode(checkpoint(factory)[-1])
            if phase == "seed":
                assert header.seeded == 64 and header.seed_after is not None
            if phase == "edges":
                assert header.visited == 64 and header.edge_after is not None
            if phase == "result":
                assert header.last_graph is not None
        finish(factory, source_work)
        result = snapshot(factory)["graphs"][0]
        assert result[-1] == "acyclic" and result[4:6] == (130, 129)


@pytest.mark.parametrize(
    "changed", ["capture_attempt_id", "capture_directory_id", "capture_version", "sha256"]
)
def test_stale_rank_worker_cannot_publish_any_rank_output(tmp_path, changed):
    with retained(tmp_path, raw_edges([edge(0)])) as (_, factory):
        source_work = prepared(factory)
        work = at_phase(factory, source_work, "seed")
        advance = rank._compute(factory, work)
        before = checkpoint(factory), snapshot(factory)
        with factory() as session, session.begin():
            session.execute(
                update(Journal).values({changed: 99 if changed == "capture_version" else "f" * 64})
            )
        assert not rank._commit(factory, work, advance)
        assert (checkpoint(factory), snapshot(factory)) == before


def test_duplicate_worker_cannot_decrement_the_same_adjacency_twice(tmp_path):
    with retained(tmp_path, raw_edges([edge(n) for n in range(129)])) as (_, factory):
        source_work = prepared(factory)
        work = at_phase(factory, source_work, "edges")
        advance = rank._compute(factory, work)
        assert rank._commit(factory, work, advance)
        before = checkpoint(factory), snapshot(factory)
        assert not rank._commit(factory, work, advance)
        assert (checkpoint(factory), snapshot(factory)) == before
        finish(factory, source_work)
        assert snapshot(factory)["graphs"][0][-1] == "acyclic"


@pytest.mark.parametrize("phase", ["seed", "edges"])
def test_rank_output_failure_rolls_back_degrees_ready_rows_and_cursor(tmp_path, phase):
    with retained(tmp_path, raw_edges([edge(n) for n in range(129)])) as (engine, factory):
        source_work = prepared(factory)
        work = at_phase(factory, source_work, phase)
        advance = rank._compute(factory, work)
        before = checkpoint(factory), snapshot(factory)
        writes = []

        def fail(_conn, _cursor, statement, _parameters, _context, _many):
            if statement.startswith(
                (
                    "INSERT INTO collection_upload_provenance_epoch_rank_vertices",
                    "UPDATE collection_upload_provenance_epoch_rank_vertices",
                )
            ):
                writes.append(statement)
                if phase == "seed" or len(writes) == 2:
                    raise RuntimeError("injected private rank publication failure")

        event.listen(engine, "before_cursor_execute", fail)
        try:
            with pytest.raises(RuntimeError, match="injected"):
                rank._commit(factory, work, advance)
        finally:
            event.remove(engine, "before_cursor_execute", fail)
        assert writes and (checkpoint(factory), snapshot(factory)) == before
        finish(factory, source_work)


@pytest.mark.parametrize(
    "fault",
    [
        "vertex-marker",
        "arc-marker",
        "graph-marker",
        "missing-seed",
        "missing-ready",
        "algorithm",
        "cursor",
    ],
)
def test_rank_reopen_fails_closed_on_missing_checkpoint_links(tmp_path, fault):
    with retained(tmp_path, raw_edges([edge(n) for n in range(129)])) as (_, factory):
        source_work = prepared(factory)
        if fault in {"vertex-marker", "arc-marker"}:
            work = at_phase(factory, source_work, "edges")
            assert rank._commit(factory, work, rank._compute(factory, work))
        elif fault == "graph-marker":
            finish(factory, source_work)
        else:
            at_phase(factory, source_work, "seed" if fault == "missing-seed" else "pop")
        header = rank._decode(checkpoint(factory)[-1])
        with factory() as session, session.begin():
            if fault == "vertex-marker":
                session.execute(delete(Ranked).where(Ranked.vertex_id == header.last_vertex[2]))
            elif fault == "arc-marker":
                session.execute(
                    delete(rank.Arc).where(
                        rank.Arc.source_id == header.last_arc[2],
                        rank.Arc.target_id == header.last_arc[3],
                    )
                )
            elif fault == "graph-marker":
                session.execute(delete(Result).where(Result.kind == header.last_graph[1]))
            elif fault == "missing-seed":
                session.execute(delete(rank.Vertex).where(rank.Vertex.vertex_id == uid(0)))
            elif fault == "missing-ready":
                session.execute(delete(Ranked).where(Ranked.ready.is_(True)))
            elif fault == "algorithm":
                session.execute(update(Scan).values(rank_algorithm="different"))
            else:
                session.execute(update(Scan).values(rank_cursor_json='{"phase":"pop"}'))
        with pytest.raises(RuntimeError):
            finish(factory, source_work)
        assert terminal(factory)["anchor"] is None


def test_native_acceptance_follows_private_rank_completion(tmp_path, monkeypatch):
    raw = source(journal_id=JOURNAL)
    with retained(tmp_path, raw) as (engine, factory):
        prepared(factory)
        service = service_for(factory)
        native = service._journal_validation_owner.advance
        observed = []

        def detached(*args, **kwargs):
            assert checkpoint(factory)[1] == "complete"
            assert engine.pool.checkedout() == 0 and terminal(factory)["anchor"] is None
            observed.append(True)
            return native(*args, **kwargs)

        monkeypatch.setattr(service._journal_validation_owner, "advance", detached)
        try:
            for _ in range(10000):
                service._advance_provenance_journal_validation(1, JOURNAL)
                if terminal(factory)["state"] == "sealed":
                    break
            else:
                pytest.fail("small native handoff did not complete")
            assert len(observed) > 1
            with validate_journal(raw) as expected:
                assert terminal(factory)["anchor"] == expected.anchor
        finally:
            service.close()


def test_cyclic_private_result_never_outranks_an_earlier_native_error(tmp_path):
    document = parse_journal(source(journal_id=JOURNAL))[0].document
    document["sequence"] = "7"
    raw = raw_journal(canonical_json_bytes(document), entry([edge(0, 0, 1), edge(1, 1, 0)]))
    with pytest.raises(ValueError) as expected:
        validate_journal(raw)
    with retained(tmp_path, raw) as (_, factory):
        work = prepared(factory)
        finish(factory, work)
        assert snapshot(factory)["graphs"][0][-1] == "cyclic"
        assert native_outcome(factory, work).failure == str(expected.value)


def test_rank_headers_use_the_actual_scalar_domain_and_neutral_changes_visit_only_targets():
    value = 2**200
    header = rank._Header(phase="seed", vertices=value, arcs=value)
    assert rank._decode(rank._encode(header)) == header
    visits = []

    class Degrees(dict):
        def __getitem__(self, key):
            visits.append(key)
            return super().__getitem__(key)

        def __iter__(self):
            raise AssertionError("whole degree map traversal")

    degrees = Degrees({uid(n): n + 1 for n in range(1025)})
    assert decrement_rank_degrees((uid(0), uid(1024)), degrees) == ((uid(0), 0), (uid(1024), 1024))
    assert visits == [uid(0), uid(1024)]
    with pytest.raises(RuntimeError):
        decrement_rank_degrees((uid(0),), {uid(0): 0})
    with pytest.raises(ValueError):
        decrement_rank_degrees((uid(0), uid(0)), degrees)


@pytest.mark.growth_witness(
    "riverhog-epoch-rank-page-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_rank._page",
)
def test_actual_indexed_seed_ready_and_adjacency_pages(tmp_path, monkeypatch, record_property):
    binding = growth_binding(rank._page)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    samples = []
    cases = [
        (phase, count, requested, after)
        for phase in ("seed", "pop", "edges")
        for count, requested, after in (
            [(0, 1, None), (17, 1, None), (129, 1, None), (1025, 1, None)]
            if phase == "pop"
            else [(0, 1, None), (17, 16, None), (1025, 64, None), (1025, 16, 900)]
        )
    ]
    for phase, count, requested, after in cases:
        with retained(
            tmp_path / f"{phase}-{count}-{requested}-{after}", source(journal_id=JOURNAL)
        ) as (engine, factory):
            source_work = prepared(factory)
            work = rank._begin(factory, source_work)
            base = dict(
                collection_id=1,
                journal_id=JOURNAL,
                attempt_id=source_work.attempt_id,
                epoch=0,
                kind="derivation",
            )
            header = replace(
                work.header,
                phase=phase,
                vertices=count + int(phase == "edges"),
                arcs=count if phase == "edges" else 0,
                seeded=0 if phase == "seed" else count + int(phase == "edges"),
                settled=int(phase == "edges"),
                ready=int(phase == "pop" and count > 0),
                seed_after=uid(after + 1) if phase == "seed" and after is not None else None,
                active=uid(0) if phase == "edges" else None,
                edge_after=uid(after + 1) if phase == "edges" and after is not None else None,
            )
            raw = rank._encode(header)
            with factory() as session, session.begin():
                if phase == "seed":
                    session.add_all(
                        rank.Vertex(**base, vertex_id=uid(n + 1), indegree=1) for n in range(count)
                    )
                elif phase == "pop":
                    session.add_all(
                        Ranked(
                            **base,
                            vertex_id=uid(n + 1),
                            remaining_degree=0 if n == count - 1 else 1,
                            rank=None,
                            ready=n == count - 1,
                        )
                        for n in range(count)
                    )
                else:
                    session.add_all(
                        rank.Arc(**base, source_id=uid(0), target_id=uid(n + 1))
                        for n in range(count)
                    )
                session.execute(update(Scan).values(rank_cursor_json=raw))
            work = replace(work, header=header, cursor_json=raw)
            observed = {"returned_positions": 0, "closed": False}
            snapshot_reader = rank.read_snapshot

            @contextmanager
            def observed_snapshot(factory, base=snapshot_reader, counts=observed):
                with base(factory) as session:
                    execute = session.execute

                    def wrapped(*args, **kwargs):
                        return ObservedWindow(execute(*args, **kwargs), counts)

                    monkeypatch.setattr(session, "execute", wrapped)
                    yield session

            with engine.connect() as connection:
                native = connection.connection.driver_connection
                assert isinstance(native, sqlite3.Connection)
            statements, instructions, payload_reads = [], [0], []

            def sql(_conn, _cursor, statement, parameters, _context, _many, output=statements):
                output.append((statement, parameters))

            def tick(counter=instructions):
                counter[0] += 1
                return 0

            def authorize(action, table, column, _database, _trigger, reads=payload_reads):
                if action == sqlite3.SQLITE_READ and column == "content":
                    reads.append((table, column))
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            event.listen(engine, "before_cursor_execute", sql)
            native.set_authorizer(authorize)
            native.set_progress_handler(tick, 1)
            try:
                with monkeypatch.context() as patch:
                    patch.setattr(rank, "read_snapshot", observed_snapshot)
                    actual = rank._page(factory, work, row_budget=requested)
            finally:
                native.set_progress_handler(None, 0)
                native.set_authorizer(None)
                event.remove(engine, "before_cursor_execute", sql)
            expected = (
                ([uid(count)] if count else [])
                if phase == "pop"
                else [uid(n + 1) for n in range(count) if after is None or n > after][:requested]
            )
            assert [row[0] for row in actual] == expected
            assert len(actual) == observed["returned_positions"]
            assert observed["closed"] and not payload_reads and engine.pool.checkedout() == 0
            for statement, parameters in statements:
                with engine.connect() as connection:
                    result = connection.exec_driver_sql(
                        "EXPLAIN QUERY PLAN " + statement, parameters
                    )
                    try:
                        plan = " ".join(row[3] for row in result)
                    finally:
                        result.close()
                assert "SEARCH" in plan and "SCAN" not in plan and "USE TEMP" not in plan
            sample = {
                "declaration": binding.declaration.id,
                "declaration_sha256": digest,
                "dimensions": {"retained_rows": count, "requested_rows": requested},
                "metrics": {
                    "sql_statements": len(statements),
                    "returned_rows": observed["returned_positions"],
                    "source_payload_octets": 0,
                    "sqlite_vm_instructions": instructions[0],
                },
                "delegates": {},
            }
            validate_execution_samples(projection, binding.declaration.id, [sample])
            samples.append(sample)
    record_property("growth_samples", samples)
