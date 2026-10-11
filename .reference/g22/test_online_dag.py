from __future__ import annotations

import itertools
import random
import sqlite3
from collections import Counter
from contextlib import closing

import pytest

from riverhog_provenance import ProvenanceValidationError
from riverhog_provenance._online_dag import OnlineDag, install


def acyclic(edges):
    nodes = {v for edge in edges for v in edge}
    indegree = {v: 0 for v in nodes}
    outgoing = {v: [] for v in nodes}
    for a, b in edges:
        indegree[b] += 1
        outgoing[a].append(b)
    ready = [v for v in nodes if indegree[v] == 0]
    visited = 0
    while ready:
        v = ready.pop()
        visited += 1
        for w in outgoing[v]:
            indegree[w] -= 1
            if indegree[w] == 0:
                ready.append(w)
    return visited == len(nodes)


def database():
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE edges(kind TEXT,source TEXT,target TEXT,PRIMARY KEY(kind,source,target))")
    install(db)
    return db


def snapshot(db):
    return tuple(tuple(db.execute(f"SELECT * FROM {table} ORDER BY 1,2,3")) for table in (
        "edges", "dag_levels", "dag_incoming"
    ))


def check_invariants(db, kind):
    edges = {(a, b) for a, b in db.execute("SELECT source,target FROM edges WHERE kind=?", (kind,))}
    levels = {a: int(b) for a, b in db.execute("SELECT vertex,level FROM dag_levels WHERE kind=?", (kind,))}
    assert all(levels[a] <= levels[b] for a, b in edges)
    incoming = {(b, a) for a, b in db.execute("SELECT target,source FROM dag_incoming WHERE kind=?", (kind,))}
    assert incoming == {(a, b) for a, b in edges if levels[a] == levels[b]}
    assert not db.execute("SELECT 1 FROM dag_seen").fetchone()
    assert not db.execute("SELECT 1 FROM dag_queue").fetchone()


def exercise(sequence):
    with closing(database()) as db:
        dag = OnlineDag(db, "derivation")
        accepted = set()
        for edge in sequence:
            before = snapshot(db)
            if acyclic(accepted | {edge}):
                dag.add(*edge)
                accepted.add(edge)
            else:
                with pytest.raises(ProvenanceValidationError):
                    dag.add(*edge)
                assert snapshot(db) == before
            check_invariants(db, "derivation")
            assert {(a, b) for a, b in db.execute("SELECT source,target FROM edges")} == accepted


def test_every_three_vertex_edge_order_matches_independent_kahn():
    # Every ordering of all six non-self directed edges; invalid insertions
    # rollback and later valid insertions must still have the exact prior graph.
    edges = list(itertools.permutations(("a", "b", "c"), 2))
    for sequence in itertools.permutations(edges):
        exercise(sequence)


@pytest.mark.parametrize("seed", range(12))
def test_random_online_updates_match_independent_graph_and_rollback(seed):
    rng = random.Random(seed)
    nodes = [f"n{i}" for i in range(15)]
    exercise([(rng.choice(nodes), rng.choice(nodes)) for _ in range(100)])


def test_namespaces_and_duplicate_edges_remain_independent():
    with closing(database()) as db:
        a = OnlineDag(db, "derivation")
        b = OnlineDag(db, "specialization")
        a.add("a", "b")
        a.add("a", "b")
        b.add("b", "a")
        check_invariants(db, "derivation")
        check_invariants(db, "specialization")
        assert db.execute("SELECT count(*) FROM edges").fetchone()[0] == 2


def test_online_counts_are_not_disguised_as_a_linear_certificate():
    measurements = []
    for n in (32, 64, 128):
        counts = Counter()
        with closing(database()) as db:
            dag = OnlineDag(db, "derivation", lambda key, n=1: counts.update({key: n}))
            for i in reversed(range(n)):
                dag.add(f"v{i:05d}", f"v{i+1:05d}")
            assert counts["edge_requests"] == n
            # Loose conservative implementation budget for the selected online
            # algorithm, NOT a claim of O(n) for a sequence of adversarial inserts.
            assert sum(counts.values()) < 30 * (n + 1) ** 1.5
        measurements.append(sum(counts.values()))
    assert all(b > a for a, b in zip(measurements, measurements[1:]))
