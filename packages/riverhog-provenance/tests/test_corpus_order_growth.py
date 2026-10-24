from __future__ import annotations

import builtins
import hashlib
import itertools
import sqlite3
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path

import pytest
import riverhog_canonical_json as canonical
import riverhog_provenance._corpus_order as ordering
import riverhog_provenance.corpus as corpus
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    ProvenanceValidationError,
    append_assertions,
    assertion,
    create_journal,
    external_reference,
    reference,
    validate_journal,
)

ORDER = corpus.CanonicalCorpusValidator._acyclic


def octets(value):
    if isinstance(value, str):
        return len(value.encode())
    if isinstance(value, bytes):
        return len(value)
    if isinstance(value, int):
        return len(str(value))
    if isinstance(value, tuple):
        return sum(octets(item) for item in value)
    assert value is None
    return 0


class Cursor:
    def __init__(self, actual, recorder, query):
        self.actual, self.recorder, self.query = actual, recorder, query
        self.closed = False
        recorder.live += 1

    @property
    def rowcount(self):
        return self.actual.rowcount

    def fetchone(self):
        result = self.actual.fetchone()
        self.recorder.metrics["returned_rows"] += int(result is not None)
        self.recorder.metrics["returned_octets"] += octets(result)
        if result is not None:
            if self.query.startswith(
                (
                    "SELECT phase,",
                    "SELECT source, target",
                    "SELECT id, degree",
                    "SELECT id FROM ready",
                    "SELECT target FROM edges",
                    "SELECT degree FROM nodes",
                )
            ):
                self.recorder.metrics["projected_payload_octets"] += octets(result)
            for query, metric in (
                ("SELECT source, target FROM edges", "source_edge_rows"),
                ("SELECT target FROM edges", "outgoing_edge_rows"),
                ("SELECT id, degree FROM nodes", "seed_vertex_rows"),
                ("SELECT id FROM ready", "ready_rows"),
            ):
                if self.query.startswith(query):
                    self.recorder.metrics[metric] += 1
        return result

    def __iter__(self):
        return self

    def __next__(self):
        result = self.fetchone()
        if result is None:
            raise StopIteration
        return result

    def close(self):
        if not self.closed:
            self.actual.close()
            self.closed = True
            self.recorder.live -= 1


class Connection:
    def __init__(self, actual, recorder, *, owned=False):
        self.actual, self.recorder, self.owned = actual, recorder, owned

    def execute(self, query, parameters=()):
        recorder = self.recorder
        recorder.metrics["sql_statements"] += 1
        assert "assertion_payloads" not in query
        recorder.metrics["parameter_octets"] += octets(parameters)
        recorder.queries.add(query)
        if recorder.failure_query is not None and query.startswith(recorder.failure_query):
            raise recorder.failure
        actual = self.actual.execute(query, parameters)
        if actual.rowcount > 0:
            recorder.metrics["written_rows"] += actual.rowcount
            if query.startswith("INSERT OR IGNORE INTO nodes"):
                recorder.metrics["written_octets"] += octets(parameters) + 1
            elif query.startswith("INSERT INTO nodes"):
                recorder.metrics["written_octets"] += octets(parameters) + 1
            elif query.startswith("INSERT INTO checkpoint"):
                recorder.metrics["written_octets"] += 9
            else:
                assert query.startswith(
                    ("UPDATE nodes", "INSERT INTO ready", "DELETE FROM ready", "UPDATE checkpoint")
                )
                recorder.metrics["written_octets"] += octets(parameters)
        return Cursor(actual, recorder, query)

    def rollback(self):
        self.recorder.metrics["sql_statements"] += 1
        self.actual.rollback()

    def close(self):
        self.actual.close()
        if self.owned:
            self.recorder.closed += 1


class Recorder:
    def __init__(self):
        self.metrics = defaultdict(int)
        self.queries = set()
        self.live = self.opened = self.closed = 0
        self.failure_query = self.failure = None

    @contextmanager
    def observe(self, current, monkeypatch):
        connect = sqlite3.connect

        def stepped():
            self.metrics["sqlite_vm_instructions"] += 1
            return 0

        def connected(*args, **kwargs):
            actual = connect(*args, **kwargs)
            actual.set_progress_handler(stepped, 1)
            self.opened += 1
            return Connection(actual, self, owned=True)

        def decoded(value):
            self.metrics["degree_decoded_octets"] += octets(value)
            return builtins.int(value)

        def encoded(value):
            result = builtins.str(value)
            self.metrics["degree_encoded_octets"] += octets(result)
            return result

        def forbidden_json(*args, **kwargs):
            self.metrics["json_calls"] += 1
            pytest.fail("corpus ordering attempted source JSON decoding or encoding")

        def forbidden_hash(value=b"", *args, **kwargs):
            self.metrics["hashed_octets"] += len(value)
            pytest.fail("corpus ordering attempted source hashing")

        actual = current._db
        actual.set_progress_handler(stepped, 1)
        try:
            with monkeypatch.context() as patch:
                patch.setattr(current, "_db", Connection(actual, self))
                patch.setattr(corpus.sqlite3, "connect", connected)
                patch.setattr(ordering, "_decode_count", decoded)
                patch.setattr(ordering, "_encode_count", encoded)
                patch.setattr(corpus, "canonical_document", forbidden_json)
                patch.setattr(corpus.json, "loads", forbidden_json)
                patch.setattr(canonical, "canonical_json_bytes", forbidden_json)
                patch.setattr(hashlib, "sha256", forbidden_hash)
                yield
        finally:
            actual.set_progress_handler(None, 0)
            assert self.live == 0
            assert self.opened == self.closed == 1
            assert not list(Path(current._scratch.name).glob("order-*"))


def dimensions(edges, kind):
    endpoints = {value for edge in edges for value in edge}
    return {
        "edges": len(edges),
        "vertices": len(endpoints),
        "identity_octets": max((octets(value) for value in endpoints), default=0),
        "degree_octets": len(str(max(1, len(edges)))),
        "kind_octets": octets(kind),
    }


def insert(current, edges, kind):
    current._db.executemany(
        "INSERT OR IGNORE INTO edges VALUES (?, ?, ?)",
        ((kind, source, target) for source, target in edges),
    )


def measure(current, edges, kind, monkeypatch, *, cyclic=False):
    recorder = Recorder()
    with recorder.observe(current, monkeypatch):
        if cyclic:
            with pytest.raises(ProvenanceValidationError, match=f"cross-journal {kind}.*cycle"):
                ORDER(current, kind)
        else:
            ORDER(current, kind)
    binding = growth_binding(ORDER)
    projection = project_bindings((binding,))
    sample = {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical_json_bytes(projection["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": dimensions(edges, kind),
        "metrics": {
            bound.metric: recorder.metrics[bound.metric] for bound in binding.declaration.bounds
        },
        "delegates": {},
    }
    validate_execution_samples(projection, binding.declaration.id, [sample])
    assert recorder.metrics["source_edge_rows"] == len(edges)
    assert recorder.metrics["seed_vertex_rows"] == sample["dimensions"]["vertices"]
    if not cyclic:
        assert recorder.metrics["outgoing_edge_rows"] == len(edges)
        assert recorder.metrics["ready_rows"] == sample["dimensions"]["vertices"]
    assert not any("remaining" in query or "COUNT(" in query for query in recorder.queries)
    return sample


def identity(number):
    return f"urn:uuid:00000000-0000-4000-8000-{number:012x}"


def edges_for(shape, count):
    if shape == "chain":
        return [(identity(i), identity(i + 1)) for i in range(count)]
    if shape == "reverse":
        return [(identity(i + 1), identity(i)) for i in reversed(range(count))]
    if shape == "pairs":
        return [(identity(2 * i), identity(2 * i + 1)) for i in range(count)]
    if shape == "branch":
        return [(identity(0), identity(i + 1)) for i in range(count)]
    assert shape == "converge"
    return [(identity(i + 1), identity(0)) for i in range(count)]


@pytest.mark.growth_witness(
    "native-corpus-topological-order/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus.CanonicalCorpusValidator._acyclic",
)
def test_actual_corpus_ordering_visits_complete_sparse_and_dense_edges(
    monkeypatch, record_property
):
    records = []
    for kind in ("derivation", "specialization"):
        for shape in ("chain", "reverse", "pairs", "branch", "converge"):
            for count in (0, 1, 17, 129, 1025):
                edges = edges_for(shape, count)
                with corpus.CanonicalCorpusValidator() as current:
                    insert(current, edges, kind)
                    insert(
                        current, reversed(edges), kind
                    )  # Exact duplicates remain one obligation.
                    records.append(measure(current, edges, kind, monkeypatch))
        for width in (1, 4, 12, 24):
            edges = [
                (identity(layer * width + i), identity((layer + 1) * width + j))
                for layer in range(2)
                for i in range(width)
                for j in range(width)
            ]
            with corpus.CanonicalCorpusValidator() as current:
                insert(current, edges, kind)
                records.append(measure(current, edges, kind, monkeypatch))
        # Full, non-ASCII endpoint widths enter both input and observed byte counts.
        edges = [(left + "é" * 1024, right + "é" * 1024) for left, right in edges_for("chain", 17)]
        with corpus.CanonicalCorpusValidator() as current:
            insert(current, edges, kind)
            records.append(measure(current, edges, kind, monkeypatch))
        for count in (1, 17, 129, 1025):
            edges = edges_for("chain", count)
            edges.append((identity(count), identity(0)))
            with corpus.CanonicalCorpusValidator() as current:
                insert(current, edges, kind)
                records.append(measure(current, edges, kind, monkeypatch, cyclic=True))
        with corpus.CanonicalCorpusValidator() as current:
            edges = [(identity(0), identity(0))]
            insert(current, edges, kind)
            records.append(measure(current, edges, kind, monkeypatch, cyclic=True))
    record_property("growth_samples", records)


def reference_acyclic(edges):
    vertices = {value for edge in edges for value in edge}
    reachable = set(edges)
    for pivot in vertices:
        for source in vertices:
            for target in vertices:
                if (source, pivot) in reachable and (pivot, target) in reachable:
                    reachable.add((source, target))
    return all((vertex, vertex) not in reachable for vertex in vertices)


@pytest.mark.parametrize("kind", ["derivation", "specialization"])
def test_corpus_ordering_matches_all_three_vertex_directed_graphs(kind):
    possible = list(itertools.product(("a", "b", "c"), repeat=2))
    with corpus.CanonicalCorpusValidator() as current:
        for mask in range(1 << len(possible)):
            edges = [edge for bit, edge in enumerate(possible) if mask & (1 << bit)]
            current._db.execute("DELETE FROM edges")
            insert(current, edges, kind)
            if reference_acyclic(edges):
                current.validate()
                current.validate()
            else:
                for _ in range(2):
                    with pytest.raises(ProvenanceValidationError, match=f"{kind}.*cycle"):
                        current.validate()
            assert current._db.execute("SELECT count(*) FROM edges").fetchone() == (len(edges),)
            assert not list(Path(current._scratch.name).glob("order-*"))


@pytest.mark.parametrize("kind", ["derivation", "specialization"])
@pytest.mark.parametrize(
    "query",
    [
        "INSERT OR IGNORE INTO nodes",
        "SELECT id, degree FROM nodes",
        "SELECT id FROM ready",
        "SELECT target FROM edges",
        "UPDATE nodes",
    ],
)
def test_corpus_ordering_failure_releases_scratch_and_preserves_retry(kind, query, monkeypatch):
    with corpus.CanonicalCorpusValidator() as current:
        edges = edges_for("chain", 17)
        insert(current, edges, kind)
        recorder = Recorder()
        recorder.failure_query, recorder.failure = (
            query,
            sqlite3.OperationalError("ordering interrupted"),
        )
        with (
            pytest.raises(sqlite3.OperationalError) as failed,
            recorder.observe(current, monkeypatch),
        ):
            ORDER(current, kind)
        assert failed.value is recorder.failure
        assert current._db.execute("SELECT count(*) FROM edges").fetchone() == (len(edges),)
        current.validate()


@pytest.mark.parametrize("kind", ["derivation", "specialization"])
def test_actual_sqlite_cancellation_retains_corpus_and_allows_retry(kind):
    with corpus.CanonicalCorpusValidator() as current:
        insert(current, edges_for("chain", 129), kind)
        current._db.commit()
        current._db.set_progress_handler(lambda: 1, 1)
        try:
            with pytest.raises(sqlite3.OperationalError, match="interrupted"):
                ORDER(current, kind)
        finally:
            current._db.set_progress_handler(None, 0)
        assert not list(Path(current._scratch.name).glob("order-*"))
        assert current._db.execute("SELECT count(*) FROM edges").fetchone() == (129,)
        current.validate()


def test_corpus_cycle_check_keeps_exact_cross_journal_specialization(journal, who, catalog):
    with validate_journal(journal, catalog=catalog) as primary:
        other = BoundedSourceObserver(catalog=catalog).observe(BytesSource(b"other"))
        graph = other.graph_fragment()
        graph["relations"] = [
            assertion(
                "specialization",
                who,
                specific=external_reference(primary, primary.states[0]["id"]),
                general=reference(other.state_id, "state"),
            )
        ]
        with validate_journal(
            create_journal(graph, recorded_by_agent_id=who, catalog=catalog), catalog=catalog
        ) as child:
            amended = append_assertions(
                journal,
                {
                    "relations": [
                        assertion(
                            "specialization",
                            who,
                            specific=external_reference(child, other.state_id),
                            general=reference(primary.states[0]["id"], "state"),
                        )
                    ]
                },
                recorded_by_agent_id=who,
                catalog=catalog,
            )
            with validate_journal(amended, catalog=catalog) as later:
                with corpus.CanonicalCorpusValidator() as current:
                    current.add(primary)
                    current.add(child)
                    current.validate()
                    current.add(later)
                    with pytest.raises(ProvenanceValidationError, match="specialization.*cycle"):
                        current.validate()


def test_corpus_reference_errors_precede_cycle_errors():
    with corpus.CanonicalCorpusValidator() as current:
        insert(current, [("a", "a")], "derivation")
        missing = {
            "journal_id": "missing",
            "assertion_id": "missing",
            "object_id": "missing",
            "object_type": "state",
            "entry": {},
        }
        current._db.execute("INSERT INTO refs VALUES (?)", (corpus.canonical_document(missing),))
        with pytest.raises(ProvenanceValidationError, match="unresolved required external"):
            current.validate()


@pytest.mark.parametrize("kind", ["derivation", "specialization"])
def test_actual_corpus_ordering_uses_endpoint_and_pending_key_indexes(kind, monkeypatch):
    connect = sqlite3.connect
    work_plans = []

    class PlannedConnection:
        def __init__(self, actual):
            self.actual = actual

        def execute(self, *args):
            return self.actual.execute(*args)

        def close(self):
            try:
                for query, parameters in (
                    ("SELECT id FROM ready ORDER BY id LIMIT 1", ()),
                    ("SELECT degree FROM nodes WHERE id = ?", (identity(1),)),
                ):
                    work_plans.append(
                        self.actual.execute("EXPLAIN QUERY PLAN " + query, parameters).fetchall()
                    )
            finally:
                self.actual.close()

    with corpus.CanonicalCorpusValidator() as current:
        insert(current, edges_for("chain", 17), kind)
        source_plan = current._db.execute(
            "EXPLAIN QUERY PLAN SELECT target FROM edges "
            "WHERE kind = ? AND source = ? ORDER BY target",
            (kind, identity(1)),
        ).fetchall()
        with monkeypatch.context() as patch:
            patch.setattr(
                corpus.sqlite3, "connect", lambda *a, **k: PlannedConnection(connect(*a, **k))
            )
            ORDER(current, kind)
        for plan, index in zip(
            [source_plan, *work_plans],
            ["sqlite_autoindex_edges_1", "sqlite_autoindex_ready_1", "sqlite_autoindex_nodes_1"],
            strict=True,
        ):
            assert any(index in row[-1] for row in plan)
            assert not any("TEMP B-TREE" in row[-1] for row in plan)


@pytest.mark.parametrize("kind", ["derivation", "specialization"])
@pytest.mark.parametrize("phase", ["build", "pop"])
def test_real_ordering_database_interrupt_preserves_uncommitted_corpus(kind, phase, monkeypatch):
    connect = sqlite3.connect
    closed = []
    trigger = "INSERT OR IGNORE INTO nodes" if phase == "build" else "SELECT id FROM ready"

    class InterruptedConnection:
        def __init__(self, actual):
            self.actual = actual

        def execute(self, query, parameters=()):
            if query.startswith(trigger):
                self.actual.set_progress_handler(lambda: 1, 1)
            return self.actual.execute(query, parameters)

        def rollback(self):
            self.actual.rollback()

        def close(self):
            self.actual.close()
            closed.append(True)

    with corpus.CanonicalCorpusValidator() as current:
        edges = edges_for("chain", 17)
        insert(current, edges, kind)
        assert current._db.in_transaction
        with monkeypatch.context() as patch:
            patch.setattr(
                corpus.sqlite3, "connect", lambda *a, **k: InterruptedConnection(connect(*a, **k))
            )
            with pytest.raises(sqlite3.OperationalError, match="interrupted"):
                ORDER(current, kind)
        assert closed == [True]
        assert not list(Path(current._scratch.name).glob("order-*"))
        assert current._db.in_transaction
        assert current._db.execute(
            "SELECT source, target FROM edges WHERE kind = ? ORDER BY source, target", (kind,)
        ).fetchall() == sorted(edges)
        current.validate()
