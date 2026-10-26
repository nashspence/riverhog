from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from graphlib import CycleError, TopologicalSorter

import pytest
from riverhog_core import canonical_discovery_journal_epochs as epochs
from riverhog_core import canonical_discovery_journal_rank as owner
from riverhog_core import canonical_discovery_journal_topology as topology
from riverhog_core import canonical_discovery_journal_union as union
from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask
from riverhog_core.services.discovery_validation_owner import DiscoveryValidationOwner

from tests.unit.test_canonical_discovery_journal_epochs import begin as begin_epochs
from tests.unit.test_canonical_discovery_journal_epochs import finish as finish_epochs
from tests.unit.test_canonical_discovery_journal_topology import begin as begin_topology
from tests.unit.test_canonical_discovery_journal_topology import finish as finish_topology
from tests.unit.test_canonical_discovery_journal_union import (
    begin as begin_union,
)
from tests.unit.test_canonical_discovery_journal_union import (
    edge,
    frame,
    uid,
    union_case,
)
from tests.unit.test_canonical_discovery_journal_union import (
    finish as finish_union,
)
from tests.unit.test_canonical_discovery_journal_union import (
    snapshot as union_snapshot,
)
from tests.unit.test_discovery_journal_owner import prepared as owned
from tests.unit.test_discovery_journal_owner import quantum


@contextmanager
def rank_case(path, raw, *, retained=1):
    with union_case(path, raw, retained=retained) as (case, original):
        begin_union(case.metadata, original)
        finish_union(case.metadata, original)
        yield case, owner.DiscoveryJournalRankWork(original)


def begin(metadata, task):
    with metadata.transaction() as (db, key):
        return owner.begin_discovery_journal_rank(db, key, task)


def progress(metadata, task):
    with metadata.read() as (db, key):
        return owner.discovery_journal_rank_progress(db, key, task)


def prepare(metadata, task):
    with metadata.read() as (db, key):
        return owner.prepare_discovery_journal_rank(db, key, task)


def apply(metadata, task, page, *, wrapper=lambda db: db):
    with metadata.transaction() as (db, key):
        return owner.apply_discovery_journal_rank(wrapper(db), key, task, page)


def finish(metadata, task):
    while page := prepare(metadata, task):
        assert page.work_items <= 64
        assert len(page.changes) <= 64
        assert apply(metadata, task, page)
    return progress(metadata, task)


def snapshot(metadata):
    with metadata.read() as (db, _):
        return {
            "graphs": db.execute(
                "SELECT epoch,kind,state,vertices,arcs,settled,visited "
                "FROM discovery_journal_rank_graphs ORDER BY epoch,kind"
            ).fetchall(),
            "vertices": db.execute(
                "SELECT epoch,kind,vertex_id,degree,rank,ready "
                "FROM discovery_journal_rank_vertices ORDER BY epoch,kind,vertex_id"
            ).fetchall(),
        }


def exact_ranks(metadata):
    original, ranked = union_snapshot(metadata), snapshot(metadata)
    assert len(ranked["graphs"]) == len(original["graphs"])
    for epoch, kind, state, vertices, arcs, settled, visited in ranked["graphs"]:
        pairs = [(a, b) for e, k, a, b in original["arcs"] if (e, k) == (epoch, kind)]
        endpoints = {v for pair in pairs for v in pair}
        oracle = {v: {a for a, b in pairs if b == v} for v in endpoints}
        try:
            tuple(TopologicalSorter(oracle).static_order())
            cyclic = False
        except CycleError:
            cyclic = True
        assert (state == "cyclic") == cyclic
        assert (int(vertices), int(arcs)) == (len(endpoints), len(pairs))
        rows = {
            v: (int(d), None if r is None else int(r), ready)
            for e, k, v, d, r, ready in ranked["vertices"]
            if (e, k) == (epoch, kind)
        }
        assert set(rows) == endpoints
        assert all(ready == 0 for _, _, ready in rows.values())
        positions = [rank for _, rank, _ in rows.values() if rank is not None]
        assert sorted(positions) == list(range(int(settled)))
        if not cyclic:
            assert int(settled) == len(endpoints) and int(visited) == len(pairs)
            assert all(rows[a][1] < rows[b][1] for a, b in pairs)
            assert all(degree == 0 for degree, _, _ in rows.values())


@pytest.mark.parametrize(
    "pairs",
    [
        [],
        [(0, 1), (0, 1)],
        [(0, 1), (0, 2), (1, 3), (2, 3)],
        [(0, 1), (1, 0)],
        [(0, 0)],
    ],
)
def test_actual_union_rank_matches_independent_graph_oracle(tmp_path, pairs):
    raw = frame([edge(n, a, b) for n, (a, b) in enumerate(pairs)], kind="journal_init")
    with rank_case(tmp_path / "source", raw) as (case, task):
        begin(case.metadata, task)
        assert finish(case.metadata, task).state == "complete"
        exact_ranks(case.metadata)


def test_real_corrections_keep_earlier_cycles_and_separate_graph_kinds(tmp_path):
    raw = frame(
        [
            edge(0),
            edge(1, 1, 0),
            edge(2, 0, 1, kind="specialization"),
        ],
        kind="journal_init",
    )
    raw += frame([edge(3, 1, 2)], kind="correction", retracts=[1])
    with rank_case(tmp_path / "source", raw) as (case, task):
        begin(case.metadata, task)
        assert finish(case.metadata, task).epoch == 2
        exact_ranks(case.metadata)
        assert [(e, k, s) for e, k, s, *_ in snapshot(case.metadata)["graphs"]] == [
            ("0", "derivation", "cyclic"),
            ("0", "specialization", "acyclic"),
            ("1", "derivation", "acyclic"),
            ("1", "specialization", "acyclic"),
        ]


def test_high_degree_adjacency_resumes_without_revisiting_unique_targets(tmp_path, monkeypatch):
    raw = frame([edge(n, 0, n + 1) for n in range(1025)], kind="journal_init")
    with rank_case(tmp_path / "source", raw) as (case, task):
        begin(case.metadata, task)
        actual, selected, pages = owner.decrement_rank_degrees, [], []

        def decrement(targets, degrees):
            assert not case.metadata._db.in_transaction
            assert len(targets) <= 64
            selected.extend(targets)
            pages.append(len(targets))
            return actual(targets, degrees)

        monkeypatch.setattr(owner, "decrement_rank_degrees", decrement)
        assert finish(case.metadata, task).state == "complete"
        assert len(selected) == len(set(selected)) == 1025 and 64 in pages
        exact_ranks(case.metadata)


def test_interrupted_rank_publication_rolls_back_and_owned_retry_resumes(tmp_path):
    class Interrupted:
        def __init__(self, db):
            self.db, self.writes = db, 0

        def __getattr__(self, name):
            return getattr(self.db, name)

        def execute(self, sql, *args):
            if sql.startswith("INSERT INTO discovery_journal_rank_vertices"):
                self.writes += 1
                if self.writes == 2:
                    raise InterruptedError("rank seed publication interrupted")
            return self.db.execute(sql, *args)

    raw = frame([edge(n, n, n + 1) for n in range(17)], kind="journal_init")
    with rank_case(tmp_path / "source", raw) as (case, task):
        begin(case.metadata, task)
        first = prepare(case.metadata, task)
        assert apply(case.metadata, task, first)
        page = prepare(case.metadata, task)
        before, rows = progress(case.metadata, task), snapshot(case.metadata)
        with pytest.raises(InterruptedError):
            apply(case.metadata, task, page, wrapper=Interrupted)
        assert progress(case.metadata, task) == before and snapshot(case.metadata) == rows
        with pytest.raises(StaleArchiveTask):
            apply(case.metadata, task, replace(page))
        assert prepare(case.metadata, task) is page
        assert apply(case.metadata, task, page)
        restored = owner.DiscoveryJournalRankWork(task.union)
        assert progress(case.metadata, restored) == page.progress
        assert finish(case.metadata, restored).state == "complete"
        exact_ranks(case.metadata)


@pytest.mark.parametrize("damage", ["union", "counter", "vertex", "graph", "active"])
def test_missing_original_or_rank_markers_fail_closed(tmp_path, damage):
    raw = frame([edge(0)], kind="journal_init")
    with rank_case(tmp_path / "source", raw) as (case, task):
        begin(case.metadata, task)
        while progress(case.metadata, task).phase != "edges":
            page = prepare(case.metadata, task)
            assert apply(case.metadata, task, page)
        with case.metadata.transaction() as (db, _):
            if damage == "union":
                db.execute("UPDATE discovery_journal_union SET algorithm='changed'").close()
            elif damage == "counter":
                db.execute("UPDATE discovery_journal_rank SET settled='01'").close()
            elif damage == "vertex":
                db.execute("DELETE FROM discovery_journal_rank_vertices").close()
            elif damage == "graph":
                db.execute("UPDATE discovery_journal_union_graphs SET vertices='7'").close()
            else:
                db.execute(
                    "UPDATE discovery_journal_rank_vertices SET rank='7' WHERE vertex_id=?",
                    (uid(0),),
                ).close()
        with pytest.raises(StaleArchiveTask):
            progress(case.metadata, task)


@pytest.mark.parametrize("raw", [b"truncated", frame([edge(0), edge(0)], kind="journal_init")])
def test_deferred_original_union_never_claims_rank_acceptance(tmp_path, raw):
    with rank_case(tmp_path / "source", raw) as (case, task):
        assert begin(case.metadata, task).state == "deferred"
        assert prepare(case.metadata, task) is None
        assert not snapshot(case.metadata)["graphs"]


def test_actual_worker_handoff_keeps_rank_file_binding_and_rejects_old_delta(tmp_path):
    with owned(tmp_path / "owned") as case:
        with quantum(case) as (work, framing):
            original = epochs.DiscoveryJournalEpochWork(framing, work.journal_source(framing))
            begin_epochs(work.metadata, original)
            finish_epochs(work.metadata, original)
            facts = topology.DiscoveryJournalTopologyWork(original)
            begin_topology(work.metadata, facts)
            finish_topology(work.metadata, facts)
            graphs = union.DiscoveryJournalUnionWork(facts)
            begin_union(work.metadata, graphs)
            finish_union(work.metadata, graphs)
            task = owner.DiscoveryJournalRankWork(graphs)
            before = begin(work.metadata, task)
            old_task, old_page = task, prepare(work.metadata, task)
            path = work.metadata.custody.directory / "discovery.sqlite3"
            inode, binding = path.stat().st_ino, original.source.binding
        case.pool.close()
        case.pool = DiscoveryValidationOwner(case.factory, case.root)
        with quantum(case) as (work, framing):
            original = epochs.DiscoveryJournalEpochWork(framing, work.journal_source(framing))
            facts = topology.DiscoveryJournalTopologyWork(original)
            task = owner.DiscoveryJournalRankWork(union.DiscoveryJournalUnionWork(facts))
            assert original.source.binding == binding and path.stat().st_ino == inode
            assert begin(work.metadata, task) == before
            with pytest.raises(StaleArchiveTask):
                with work.metadata.transaction() as (db, key):
                    owner.apply_discovery_journal_rank(db, key, old_task, old_page)
            assert finish(work.metadata, task).state == "complete"
