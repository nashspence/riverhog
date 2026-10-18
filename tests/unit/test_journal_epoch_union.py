from __future__ import annotations

import hashlib
import sqlite3
import subprocess
import sys
from contextlib import contextmanager
from dataclasses import replace
from uuid import UUID

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.services import collection_uploads as owner
from riverhog_core.services import upload_journal_union as union
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import epoch_union as neutral
from riverhog_provenance import parse_journal, validate_journal
from sqlalchemy import delete, event, select, update

from tests.unit.test_journal_capture import retained
from tests.unit.test_journal_epoch_scan import ObservedWindow
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_journal_topology_capture import entry, raw_journal
from tests.unit.test_journal_topology_capture import finish as finish_facts
from tests.unit.test_journal_topology_capture import ready as boundary_ready
from tests.unit.test_journal_validation_handoff import service_for, terminal
from tests.unit.test_staged_journal_range import JOURNAL, Journal

Active, Arc, Graph, Vertex, Scan = union.Active, union.Arc, union.Graph, union.Vertex, union.Scan


def uid(number):
    return f"urn:uuid:{UUID(int=number + 1)}"


def edge(number, left=0, right=None, *, kind="derivation"):
    first, second = (
        ("used_state", "generated_state") if kind == "derivation" else ("specific", "general")
    )
    return {
        "assertion_id": uid(10000 + number),
        "type": kind,
        first: {"object_id": uid(left)},
        second: {"object_id": uid(number + 100 if right is None else right)},
    }


def raw_edges(rows):
    return raw_journal(entry(rows, kind="journal_init"))


def correction(targets, rows=()):
    return entry(
        rows,
        kind="correction",
        retracts=[{"assertion_id": uid(10000 + n), "entry": {"id": uid(999)}} for n in targets],
    )


def ready(factory):
    work = boundary_ready(factory)
    finish_facts(factory, work)
    return work


def checkpoint(factory):
    with factory() as session:
        row = session.get(Scan, (1, JOURNAL))
        return row.version, row.union_next_frame, row.union_state, row.union_cursor_json


def snapshot(factory):
    with factory() as session:
        return {
            "active": [
                (r.assertion_id, r.kind, r.source_id, r.target_id, r.active)
                for r in session.scalars(select(Active).order_by(Active.assertion_id))
            ],
            "arcs": [
                (r.epoch, r.kind, r.source_id, r.target_id)
                for r in session.scalars(
                    select(Arc).order_by(Arc.epoch, Arc.kind, Arc.source_id, Arc.target_id)
                )
            ],
            "vertices": [
                (r.epoch, r.kind, r.vertex_id, r.indegree)
                for r in session.scalars(
                    select(Vertex).order_by(Vertex.epoch, Vertex.kind, Vertex.vertex_id)
                )
            ],
            "graphs": [
                (r.epoch, r.kind, r.vertex_count, r.arc_count, r.state)
                for r in session.scalars(select(Graph).order_by(Graph.epoch, Graph.kind))
            ],
        }


def finish(factory, work):
    calls = 0
    while True:
        calls += 1
        done = union.advance_upload_journal_epoch_union(factory, work)
        assert terminal(factory)["anchor"] is None and terminal(factory)["state"] == "validating"
        if done:
            return calls
        assert calls < 200


def assert_exact_counters(data):
    for epoch, kind, vertices, arcs, state in data["graphs"]:
        selected = [(left, right) for e, k, left, right in data["arcs"] if (e, k) == (epoch, kind)]
        endpoints = {vertex for pair in selected for vertex in pair}
        assert state == "complete" and arcs == len(selected) and vertices == len(endpoints)
        degrees = {
            vertex: degree for e, k, vertex, degree in data["vertices"] if (e, k) == (epoch, kind)
        }
        assert degrees == {
            vertex: sum(right == vertex for _, right in selected) for vertex in endpoints
        }


def test_parallel_assertions_are_separate_from_unique_arcs_and_graph_kinds(tmp_path):
    raw = raw_edges([edge(0, 0, 1), edge(1, 0, 1), edge(2, 0, 1, kind="specialization")])
    with retained(tmp_path, raw) as (_, factory):
        work = ready(factory)
        finish(factory, work)
        data = snapshot(factory)
        assert len(data["active"]) == 3 and len(data["arcs"]) == 2
        assert_exact_counters(data)
        header = union._decode(checkpoint(factory)[-1])
        assert header.active_count == 3 and header.consumed == 3


def test_retraction_of_one_parallel_assertion_preserves_the_carried_arc(tmp_path):
    raw = raw_journal(entry([edge(0, 0, 1), edge(1, 0, 1)], kind="journal_init"), correction([0]))
    with retained(tmp_path, raw) as (_, factory):
        work = ready(factory)
        finish(factory, work)
        data = snapshot(factory)
        assert data["arcs"] == [
            (0, "derivation", uid(0), uid(1)),
            (1, "derivation", uid(0), uid(1)),
        ]
        assert [row[-1] for row in data["active"]] == [False, True]
        assert_exact_counters(data)


def test_legal_reversal_has_separate_real_epoch_unions(tmp_path):
    raw = raw_journal(entry([edge(0, 0, 1)], kind="journal_init"), correction([0], [edge(1, 1, 0)]))
    with retained(tmp_path, raw) as (_, factory):
        work = ready(factory)
        finish(factory, work)
        data = snapshot(factory)
        assert data["arcs"] == [
            (0, "derivation", uid(0), uid(1)),
            (1, "derivation", uid(1), uid(0)),
        ]
        assert_exact_counters(data)


def test_a_later_correction_does_not_erase_a_cyclic_earlier_union(tmp_path):
    raw = raw_journal(entry([edge(0, 0, 1), edge(1, 1, 0)], kind="journal_init"), correction([1]))
    with retained(tmp_path, raw) as (_, factory):
        work = ready(factory)
        finish(factory, work)
        data = snapshot(factory)
        assert len([row for row in data["arcs"] if row[0] == 0]) == 2
        assert len([row for row in data["arcs"] if row[0] == 1]) == 1
        assert_exact_counters(data)


def test_high_degree_and_retired_history_use_bounded_carry_pages(tmp_path, monkeypatch):
    rows = [edge(n) for n in range(1025)]
    raw = raw_journal(entry(rows, kind="journal_init"), correction(range(513)))
    with retained(tmp_path, raw) as (engine, factory):
        work = ready(factory)
        sizes, seeded = [], []
        plan, read = union.plan_epoch_union_additions, union._active_seed

        def detached(edges, **kwargs):
            assert engine.pool.checkedout() == 0
            sizes.append(len(edges))
            return plan(edges, **kwargs)

        def observed(*args, **kwargs):
            result = read(*args, **kwargs)
            seeded.extend(edge.assertion_id for edge in result)
            return result

        monkeypatch.setattr(union, "plan_epoch_union_additions", detached)
        monkeypatch.setattr(union, "_active_seed", observed)
        finish(factory, work)
        assert max(sizes) == 64 and len(seeded) == 512 and len(set(seeded)) == 512
        data = snapshot(factory)
        assert len(data["arcs"]) == 1025 + 512
        assert sum(row[-1] for row in data["active"]) == 512
        assert_exact_counters(data)


def test_correction_storm_seeds_only_real_carried_views(tmp_path, monkeypatch):
    raw = raw_journal(
        entry([edge(0)], kind="journal_init"),
        *[correction([n - 1], [edge(n)]) for n in range(1, 18)],
    )
    with retained(tmp_path, raw) as (_, factory):
        work = ready(factory)
        finish(factory, work)
        data = snapshot(factory)
        assert len(data["graphs"]) == 36 and len(data["arcs"]) == 18
        assert sum(row[-1] for row in data["active"]) == 1
        assert_exact_counters(data)


def test_duplicate_documentary_ids_defer_to_native_replay(tmp_path):
    with retained(tmp_path, raw_edges([edge(0, 0, 1), edge(0, 1, 2)])) as (_, factory):
        work = ready(factory)
        finish(factory, work)
        assert checkpoint(factory)[2] == "deferred"
        assert not snapshot(factory)["active"] and not snapshot(factory)["arcs"]


@pytest.mark.parametrize(
    "changed", ["capture_attempt_id", "capture_directory_id", "capture_version", "sha256"]
)
def test_stale_union_worker_cannot_publish_any_graph_output(tmp_path, changed):
    with retained(tmp_path, raw_edges([edge(0)])) as (_, factory):
        source_work = ready(factory)
        work = union._begin(factory, source_work)
        assert union._commit(factory, work, union._compute(factory, work))
        work = union._begin(factory, source_work)
        advance = union._compute(factory, work)
        before = checkpoint(factory), snapshot(factory)
        with factory() as session, session.begin():
            session.execute(
                update(Journal).values({changed: 99 if changed == "capture_version" else "f" * 64})
            )
        assert not union._commit(factory, work, advance)
        assert (checkpoint(factory), snapshot(factory)) == before


def test_two_computations_cannot_publish_the_same_unique_arc_twice(tmp_path):
    with retained(tmp_path, raw_edges([edge(n) for n in range(129)])) as (_, factory):
        source_work = ready(factory)
        work = union._begin(factory, source_work)
        assert union._commit(factory, work, union._compute(factory, work))
        work = union._begin(factory, source_work)
        advance = union._compute(factory, work)
        assert union._commit(factory, work, advance)
        before = checkpoint(factory), snapshot(factory)
        assert not union._commit(factory, work, advance)
        assert (checkpoint(factory), snapshot(factory)) == before
        finish(factory, source_work)
        assert len(snapshot(factory)["active"]) == 129
        assert_exact_counters(snapshot(factory))


def test_late_vertex_write_failure_rolls_back_all_graph_output_and_cursor(tmp_path):
    with retained(tmp_path, raw_edges([edge(0), edge(1)])) as (engine, factory):
        source_work = ready(factory)
        work = union._begin(factory, source_work)
        assert union._commit(factory, work, union._compute(factory, work))
        work = union._begin(factory, source_work)
        advance = union._compute(factory, work)
        before = checkpoint(factory), snapshot(factory)

        def fail(_conn, _cursor, statement, _parameters, _context, _many):
            if statement.startswith("INSERT INTO collection_upload_provenance_epoch_vertices"):
                raise RuntimeError("injected endpoint publication failure")

        event.listen(engine, "before_cursor_execute", fail)
        try:
            with pytest.raises(RuntimeError, match="injected"):
                union._commit(factory, work, advance)
        finally:
            event.remove(engine, "before_cursor_execute", fail)
        assert (checkpoint(factory), snapshot(factory)) == before


@pytest.mark.parametrize(
    "fault", ["last-arc", "last-source", "cursor", "algorithm", "missing-source", "missing-active"]
)
def test_reopen_or_terminal_counters_fail_closed_on_missing_work(tmp_path, fault):
    raw = raw_journal(entry([edge(n) for n in range(129)], kind="journal_init"), correction([]))
    with retained(tmp_path, raw) as (_, factory):
        work = ready(factory)
        assert not union.advance_upload_journal_epoch_union(factory, work)
        with factory() as session, session.begin():
            if fault == "last-arc":
                marker = union._decode(checkpoint(factory)[-1]).last_arc
                session.execute(
                    delete(Arc).where(
                        Arc.epoch == marker[0],
                        Arc.kind == marker[1],
                        Arc.source_id == marker[2],
                        Arc.target_id == marker[3],
                    )
                )
            elif fault == "last-source":
                session.execute(delete(union.Fact).where(union.Fact.item_ordinal == 63))
            elif fault == "cursor":
                session.execute(update(Scan).values(union_cursor_json='{"phase":"frame"}'))
            elif fault == "algorithm":
                session.execute(update(Scan).values(union_algorithm="different"))
            elif fault == "missing-source":
                session.execute(delete(union.Fact).where(union.Fact.item_ordinal == 128))
            else:
                session.execute(delete(Active).where(Active.assertion_id == uid(10001)))
        with pytest.raises(RuntimeError):
            finish(factory, work)
        assert terminal(factory)["anchor"] is None


@pytest.mark.parametrize("commit", [False, True])
def test_process_death_retains_only_committed_graph_deltas_and_resumes_item_cursor(
    tmp_path, commit, monkeypatch
):
    with retained(tmp_path, raw_edges([edge(n) for n in range(129)])) as (engine, factory):
        source_work = ready(factory)
        work = union._begin(factory, source_work)
        assert union._commit(factory, work, union._compute(factory, work))
        before = checkpoint(factory), snapshot(factory)
        code = """import os,sys
from sqlalchemy import create_engine,event
from sqlalchemy.orm import sessionmaker
from riverhog_core.services import collection_uploads as owner
from riverhog_core.services import upload_journal_union as union
engine=create_engine(sys.argv[1])
factory=sessionmaker(engine)
source=owner._read_upload_journal_validation(factory,1,sys.argv[2])
work=union._begin(factory,source)
advance=union._compute(factory,work)
if sys.argv[3]=='before':
    event.listen(engine,'commit',lambda _connection: os._exit(73))
assert union._commit(factory,work,advance)
os._exit(73)
"""
        result = subprocess.run(
            [sys.executable, "-c", code, str(engine.url), JOURNAL, "after" if commit else "before"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 73, result.stderr
        engine.dispose()
        if commit:
            assert len(snapshot(factory)["active"]) == 64
            read = union._fact_rows

            def no_prefix(factory, work):
                if work.frame == 0:
                    assert work.header.item >= 64
                return read(factory, work)

            monkeypatch.setattr(union, "_fact_rows", no_prefix)
        else:
            assert (checkpoint(factory), snapshot(factory)) == before
        finish(factory, source_work)
        assert len(snapshot(factory)["active"]) == 129
        assert_exact_counters(snapshot(factory))


def test_native_acceptance_follows_private_union_completion(tmp_path, monkeypatch):
    raw = source(journal_id=JOURNAL)
    with retained(tmp_path, raw) as (engine, factory):
        ready(factory)
        native = owner._compute_upload_journal_validation
        observed = []

        def detached(*args):
            assert checkpoint(factory)[2] == "complete"
            assert engine.pool.checkedout() == 0 and terminal(factory)["anchor"] is None
            observed.append(True)
            return native(*args)

        monkeypatch.setattr(owner, "_compute_upload_journal_validation", detached)
        service_for(factory)._advance_provenance_journal_validation(1, JOURNAL)
        assert observed == [True]
        with validate_journal(raw) as expected:
            assert terminal(factory)["anchor"] == expected.anchor


def test_cyclic_later_union_does_not_replace_an_earlier_native_prefix_error(tmp_path):
    document = parse_journal(source(journal_id=JOURNAL))[0].document
    document["sequence"] = "7"
    raw = raw_journal(canonical_json_bytes(document), entry([edge(0, 0, 1), edge(1, 1, 0)]))
    with pytest.raises(ValueError) as expected:
        validate_journal(raw)
    with retained(tmp_path, raw) as (_, factory):
        work = ready(factory)
        finish(factory, work)
        assert checkpoint(factory)[2] == "complete"
        assert owner._compute_upload_journal_validation(factory, work).failure == str(
            expected.value
        )


@pytest.mark.growth_witness(
    "riverhog-epoch-union-delta-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.epoch_union.plan_epoch_union_additions",
)
def test_actual_neutral_delta_work(record_property):
    binding = growth_binding(neutral.plan_epoch_union_additions)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    samples = []
    for count, carried, parallel, present in [
        (0, False, False, False),
        (1, False, False, False),
        (17, False, False, False),
        (64, False, False, False),
        (17, False, True, False),
        (64, False, True, False),
        (17, True, False, False),
        (64, True, False, True),
    ]:
        values = [
            neutral.TopologyOccurrence(
                uid(10000 + n), "derivation", uid(0), uid(1 if parallel else n + 1), 0, n
            )
            for n in range(count)
        ]
        keys = {(row.kind, row.source_id, row.target_id) for row in values}
        vertices = {
            vertex for kind, left, right in keys for vertex in ((kind, left), (kind, right))
        }
        metrics = {"visited_occurrences": 0, "degree_lookups": 0}

        class Occurrences(tuple):
            observed = metrics

            def __iter__(self):
                for row in super().__iter__():
                    self.observed["visited_occurrences"] += 1
                    yield row

        class Degrees(dict):
            observed = metrics

            def __getitem__(self, key):
                self.observed["degree_lookups"] += 1
                return super().__getitem__(key)

        counts = {kind: (0, 0) for kind in neutral.GRAPH_KINDS}
        if present:
            counts["derivation"] = (len(vertices), len(keys))
        actual = neutral.plan_epoch_union_additions(
            Occurrences(values),
            known_assertions=set(),
            existing_arcs=keys if present else set(),
            degrees=Degrees(
                {
                    key: sum(kind == key[0] and right == key[1] for kind, _left, right in keys)
                    if present
                    else None
                    for key in vertices
                }
            ),
            counts=counts,
            carried=carried,
        )
        assert len(actual.arcs) == (0 if present else len(keys))
        sample = {
            "declaration": binding.declaration.id,
            "declaration_sha256": digest,
            "dimensions": {"occurrences": count},
            "metrics": {
                **metrics,
                "returned_arcs": len(actual.arcs),
                "returned_vertices": len(actual.vertices),
                "returned_assertions": len(actual.assertions),
            },
            "delegates": {},
        }
        validate_execution_samples(projection, binding.declaration.id, [sample])
        samples.append(sample)
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-epoch-active-seed-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_union._active_seed",
)
def test_actual_indexed_active_seed_work(tmp_path, monkeypatch, record_property):
    binding = growth_binding(union._active_seed)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    samples = []
    for count, requested, after in [
        (0, 1, None),
        (1, 1, None),
        (17, 1, None),
        (129, 16, None),
        (1025, 64, None),
        (1025, 16, 900),
        (1025, 1, 1023),
        (1025, 64, 1024),
    ]:
        with retained(tmp_path / f"{count}-{requested}-{after}", source(journal_id=JOURNAL)) as (
            engine,
            factory,
        ):
            source_work = ready(factory)
            with factory() as session, session.begin():
                session.add_all(
                    Active(
                        collection_id=1,
                        journal_id=JOURNAL,
                        attempt_id=source_work.attempt_id,
                        assertion_id=uid(10000 + n),
                        kind="derivation",
                        source_id=uid(0),
                        target_id=uid(n + 1),
                        frame_ordinal=0,
                        item_ordinal=n,
                        active=n % 2 == 0,
                    )
                    for n in range(count)
                )
            work = union._begin(factory, source_work)
            if after is not None:
                header = replace(work.header, seed_after=uid(10000 + after))
                raw = union._encode(header)
                with factory() as session, session.begin():
                    session.execute(update(Scan).values(union_cursor_json=raw))
                work = replace(work, header=header, cursor_json=raw)
            observed = {"returned_positions": 0, "closed": False}
            snapshot_reader = union.read_snapshot

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
                    patch.setattr(union, "read_snapshot", observed_snapshot)
                    actual = union._active_seed(factory, work, row_budget=requested)
            finally:
                native.set_progress_handler(None, 0)
                native.set_authorizer(None)
                event.remove(engine, "before_cursor_execute", sql)
            expected = [
                uid(10000 + n) for n in range(count) if n % 2 == 0 and (after is None or n > after)
            ][:requested]
            assert [row.assertion_id for row in actual] == expected
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
                "dimensions": {"retained_assertions": count, "requested_rows": requested},
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
