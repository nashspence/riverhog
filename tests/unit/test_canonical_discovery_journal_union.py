from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from uuid import UUID

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core import canonical_discovery_journal_epochs as epochs
from riverhog_core import canonical_discovery_journal_topology as topology
from riverhog_core import canonical_discovery_journal_union as owner
from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask
from riverhog_core.services.discovery_validation_owner import DiscoveryValidationOwner

from tests.unit.test_canonical_discovery_journal_epochs import begin as begin_epochs
from tests.unit.test_canonical_discovery_journal_epochs import finish as finish_epochs
from tests.unit.test_canonical_discovery_journal_topology import begin as begin_topology
from tests.unit.test_canonical_discovery_journal_topology import finish as finish_topology
from tests.unit.test_canonical_discovery_journal_topology import topology_case
from tests.unit.test_discovery_journal_owner import prepared as owned
from tests.unit.test_discovery_journal_owner import quantum


def uid(number):
    return "urn:uuid:" + str(UUID(int=number + 1))


def edge(number, left=0, right=1, *, kind="derivation"):
    first, second = (
        ("used_state", "generated_state") if kind == "derivation" else ("specific", "general")
    )
    return {
        "assertion_id": uid(10000 + number),
        "type": kind,
        first: {"object_id": uid(left)},
        second: {"object_id": uid(right)},
    }


def frame(rows=(), *, kind="assertion", retracts=()):
    body = {"assertions": {"relations": list(rows)}}
    if kind == "correction":
        body["retracts"] = [{"assertion_id": uid(10000 + number)} for number in retracts]
    return b"\x1e" + canonical_json_bytes({"entry_kind": kind, "body": body}) + b"\n"


@contextmanager
def union_case(path, raw, *, retained=1):
    with topology_case(path, raw, retained=retained) as (case, original):
        begin_topology(case.metadata, original)
        finish_topology(case.metadata, original)
        yield case, owner.DiscoveryJournalUnionWork(original)


def begin(metadata, task):
    with metadata.transaction() as (db, key):
        return owner.begin_discovery_journal_union(db, key, task)


def progress(metadata, task):
    with metadata.read() as (db, key):
        return owner.discovery_journal_union_progress(db, key, task)


def prepare(metadata, task):
    with metadata.read() as (db, key):
        return owner.prepare_discovery_journal_union(db, key, task)


def apply(metadata, task, page, *, wrapper=lambda db: db):
    with metadata.transaction() as (db, key):
        return owner.apply_discovery_journal_union(wrapper(db), key, task, page)


def finish(metadata, task):
    calls = 0
    while page := prepare(metadata, task):
        assert page.work_items <= 64
        assert apply(metadata, task, page)
        calls += 1
    return progress(metadata, task), calls


def snapshot(metadata):
    with metadata.read() as (db, _):
        return {
            "graphs": db.execute(
                "SELECT epoch,kind,vertices,arcs,state FROM discovery_journal_union_graphs "
                "ORDER BY epoch,kind"
            ).fetchall(),
            "arcs": db.execute(
                "SELECT epoch,kind,source_id,target_id FROM discovery_journal_union_arcs "
                "ORDER BY epoch,kind,source_id,target_id"
            ).fetchall(),
            "vertices": db.execute(
                "SELECT epoch,kind,vertex_id,degree FROM discovery_journal_union_vertices "
                "ORDER BY epoch,kind,vertex_id"
            ).fetchall(),
            "active": db.execute(
                "SELECT assertion_id,kind,source_id,target_id,active "
                "FROM discovery_journal_active_edges ORDER BY assertion_id"
            ).fetchall(),
        }


def exact_counts(data):
    for epoch, kind, vertices, arcs, state in data["graphs"]:
        selected = [(a, b) for e, k, a, b in data["arcs"] if (e, k) == (epoch, kind)]
        endpoints = {vertex for pair in selected for vertex in pair}
        assert int(arcs) == len(selected) and int(vertices) == len(endpoints)
        assert state == "complete"
        assert {v: int(n) for e, k, v, n in data["vertices"] if (e, k) == (epoch, kind)} == {
            vertex: sum(b == vertex for _, b in selected) for vertex in endpoints
        }


def test_parallel_assertions_and_graph_kinds_preserve_unique_arc_degrees(tmp_path, monkeypatch):
    raw = frame([edge(0), edge(1), edge(2, kind="specialization")], kind="journal_init")
    with union_case(tmp_path / "source", raw) as (case, task):
        begin(case.metadata, task)
        actual, calls = owner.plan_epoch_union_additions, []

        def plan(edges, **kwargs):
            assert not case.metadata._db.in_transaction and len(edges) <= 64
            calls.append(len(edges))
            return actual(edges, **kwargs)

        monkeypatch.setattr(owner, "plan_epoch_union_additions", plan)
        final, _ = finish(case.metadata, task)
        data = snapshot(case.metadata)
        assert final.state == "complete" and final.active_count == final.consumed == 3
        assert len(data["active"]) == 3 and len(data["arcs"]) == 2 and calls
        exact_counts(data)


@pytest.mark.parametrize("reverse", [False, True])
def test_real_correction_epochs_retire_one_assertion_and_keep_other_views(tmp_path, reverse):
    rows = [edge(0)] if reverse else [edge(0), edge(1)]
    additions = [edge(1, 1, 0)] if reverse else []
    raw = frame(rows, kind="journal_init") + frame(additions, kind="correction", retracts=[0])
    with union_case(tmp_path / "source", raw) as (case, task):
        begin(case.metadata, task)
        final, _ = finish(case.metadata, task)
        data = snapshot(case.metadata)
        assert final.state == "complete"
        assert data["arcs"] == [
            ("0", "derivation", uid(0), uid(1)),
            ("1", "derivation", uid(1 if reverse else 0), uid(0 if reverse else 1)),
        ]
        assert [row[-1] for row in data["active"]] == [0, 1]
        exact_counts(data)


def test_later_correction_keeps_the_earlier_cyclic_union(tmp_path):
    raw = frame([edge(0), edge(1, 1, 0)], kind="journal_init") + frame(
        kind="correction", retracts=[1]
    )
    with union_case(tmp_path / "source", raw) as (case, task):
        begin(case.metadata, task)
        assert finish(case.metadata, task)[0].state == "complete"
        data = snapshot(case.metadata)
        assert len([a for a in data["arcs"] if a[0] == "0"]) == 2
        assert len([a for a in data["arcs"] if a[0] == "1"]) == 1
        exact_counts(data)


def test_high_degree_and_retired_history_use_only_bounded_active_carry_pages(tmp_path, monkeypatch):
    raw = frame([edge(n, 0, n + 1) for n in range(1025)], kind="journal_init")
    raw += frame(kind="correction", retracts=range(513))
    with union_case(tmp_path / "source", raw) as (case, task):
        begin(case.metadata, task)
        actual, selected = owner._active_seed, []

        def seed(*args):
            result = actual(*args)
            selected.extend(item.assertion_id for item in result)
            return result

        monkeypatch.setattr(owner, "_active_seed", seed)
        final, _ = finish(case.metadata, task)
        assert final.state == "complete" and final.active_count == 512
        assert len(selected) == len(set(selected)) == 512
        data = snapshot(case.metadata)
        assert len(data["arcs"]) == 1025 + 512
        exact_counts(data)


def test_interrupted_vertex_write_rolls_back_all_output_and_lost_reply_resumes(tmp_path):
    class Interrupted:
        def __init__(self, db):
            self.db = db

        def __getattr__(self, name):
            return getattr(self.db, name)

        def execute(self, sql, *args):
            if sql.startswith("INSERT INTO discovery_journal_union_vertices"):
                raise InterruptedError("endpoint publication interrupted")
            return self.db.execute(sql, *args)

    with union_case(tmp_path / "source", frame([edge(0), edge(1, 1, 2)], kind="journal_init")) as (
        case,
        task,
    ):
        begin(case.metadata, task)
        assert apply(case.metadata, task, prepare(case.metadata, task))
        page = prepare(case.metadata, task)
        before = progress(case.metadata, task), snapshot(case.metadata)
        with pytest.raises(InterruptedError):
            apply(case.metadata, task, page, wrapper=Interrupted)
        assert (progress(case.metadata, task), snapshot(case.metadata)) == before
        with pytest.raises(StaleArchiveTask, match="owned prepared"):
            apply(case.metadata, task, replace(page))
        with pytest.raises(TimeoutError):
            assert apply(case.metadata, task, page)
            raise TimeoutError("reply lost")
        resumed = owner.DiscoveryJournalUnionWork(task.topology)
        assert begin(case.metadata, resumed).consumed == 2
        assert finish(case.metadata, resumed)[0].state == "complete"
        exact_counts(snapshot(case.metadata))


@pytest.mark.parametrize("damage", ["topology", "cursor", "last-source", "last-arc"])
def test_missing_original_work_or_private_markers_fail_closed(tmp_path, damage):
    with union_case(tmp_path / "source", frame([edge(0)], kind="journal_init")) as (case, task):
        begin(case.metadata, task)
        finish(case.metadata, task)
        with case.metadata.transaction() as (db, _):
            if damage == "topology":
                db.execute("UPDATE discovery_journal_topology SET version='999'").close()
            elif damage == "cursor":
                db.execute("UPDATE discovery_journal_union SET consumed='00'").close()
            elif damage == "last-source":
                db.execute("DELETE FROM discovery_journal_topology_facts").close()
            else:
                db.execute("DELETE FROM discovery_journal_union_arcs").close()
        with pytest.raises(StaleArchiveTask):
            progress(case.metadata, task)


@pytest.mark.parametrize(
    "raw",
    [
        b"truncated",
        frame([edge(0), edge(0, 1, 2)], kind="journal_init"),
        frame(kind="assertion"),
    ],
)
def test_incomplete_or_speculatively_invalid_sources_defer_without_graph_acceptance(tmp_path, raw):
    with union_case(tmp_path / "source", raw) as (case, task):
        begin(case.metadata, task)
        assert finish(case.metadata, task)[0].state == "deferred"


def test_actual_worker_handoff_keeps_file_binding_and_rejects_old_delta(tmp_path):
    with owned(tmp_path / "owned") as case:
        with quantum(case) as (work, framing):
            original = epochs.DiscoveryJournalEpochWork(framing, work.journal_source(framing))
            begin_epochs(work.metadata, original)
            finish_epochs(work.metadata, original)
            facts = topology.DiscoveryJournalTopologyWork(original)
            begin_topology(work.metadata, facts)
            finish_topology(work.metadata, facts)
            task = owner.DiscoveryJournalUnionWork(facts)
            before = begin(work.metadata, task)
            old_task, old_page = task, prepare(work.metadata, task)
            path = work.metadata.custody.directory / "discovery.sqlite3"
            inode, binding = path.stat().st_ino, original.source.binding
        case.pool.close()
        case.pool = DiscoveryValidationOwner(case.factory, case.root)
        with quantum(case) as (work, framing):
            original = epochs.DiscoveryJournalEpochWork(framing, work.journal_source(framing))
            facts = topology.DiscoveryJournalTopologyWork(original)
            task = owner.DiscoveryJournalUnionWork(facts)
            assert original.source.binding == binding and path.stat().st_ino == inode
            assert begin(work.metadata, task) == before
            with pytest.raises(StaleArchiveTask):
                with work.metadata.transaction() as (db, key):
                    owner.apply_discovery_journal_union(db, key, old_task, old_page)
            assert finish(work.metadata, task)[0].state == "complete"
