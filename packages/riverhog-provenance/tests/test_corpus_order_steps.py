from __future__ import annotations

import hashlib
import sqlite3

import pytest
import riverhog_provenance._corpus_order as ordering
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import (
    CanonicalCorpusValidator,
    CorpusOrderProgress,
    JournalValidationCapacity,
    ProvenanceValidationError,
)
from test_corpus_order_growth import Recorder, edges_for, insert
from test_corpus_source_growth import source_reads


def ready(corpus):
    assert corpus.advance_semantics().complete
    while not corpus.advance_dependencies().complete:
        pass


def finish(corpus, kind, *, rows=7, octets=4096):
    total_edges = corpus._db.execute("SELECT count(*) FROM edges WHERE kind=?", (kind,)).fetchone()[
        0
    ]
    for _ in range(10 * (total_edges + 1)):
        state = corpus.advance_order(kind, row_budget=rows, byte_budget=octets)
        assert isinstance(state, CorpusOrderProgress)
        assert state.processed_atoms <= rows
        assert state.admitted_octets <= octets
        if state.complete:
            return state
    pytest.fail("bounded ordering did not reach a terminal")


@pytest.mark.parametrize("kind", ["derivation", "specialization"])
@pytest.mark.parametrize("shape", ["chain", "reverse", "branch", "converge", "pairs"])
@pytest.mark.parametrize("rows", [1, 7, 64])
def test_bounded_ordering_preserves_complete_exact_graph_result(kind, shape, rows):
    edges = edges_for(shape, 17)
    with CanonicalCorpusValidator() as corpus:
        insert(corpus, edges, kind)
        ready(corpus)
        result = finish(corpus, kind, rows=rows, octets=512)
        assert result.source_edges == len(edges)
        assert (
            result.completed_vertices
            == result.vertices
            == len({vertex for edge in edges for vertex in edge})
        )
        completed = corpus.advance_order(kind, row_budget=1, byte_budget=1)
        assert completed.complete and completed.processed_atoms == completed.admitted_octets == 0


@pytest.mark.parametrize("kind", ["derivation", "specialization"])
def test_bounded_ordering_rejects_cycles_without_accepting_another_kind(kind):
    with CanonicalCorpusValidator() as corpus:
        insert(corpus, [("a", "b"), ("b", "c"), ("c", "a")], kind)
        ready(corpus)
        for _ in range(2):
            with pytest.raises(ProvenanceValidationError, match=kind + ".*cycle"):
                finish(corpus, kind, rows=1)
        other = "specialization" if kind == "derivation" else "derivation"
        assert finish(corpus, other, rows=1).vertices == 0


@pytest.mark.parametrize("name", ["row_budget", "byte_budget"])
@pytest.mark.parametrize("invalid", [0, -1, True, 1.0, "1", None])
def test_invalid_order_admission_precedes_workspace_creation(name, invalid, monkeypatch):
    with CanonicalCorpusValidator() as corpus:
        ready(corpus)

        def forbidden(*_args, **_kwargs):
            pytest.fail("invalid admission opened ordering storage")

        with monkeypatch.context() as patch:
            patch.setattr(ordering.sqlite3, "connect", forbidden)
            with pytest.raises(ValueError):
                corpus.advance_order("derivation", **{name: invalid})
        assert corpus._orders == {}


def test_ordering_requires_complete_dependencies_and_an_open_owner():
    with CanonicalCorpusValidator() as corpus:
        with pytest.raises(RuntimeError, match="dependencies"):
            corpus.advance_order("derivation")
        assert corpus.advance_semantics().complete
        with pytest.raises(RuntimeError, match="dependencies"):
            corpus.advance_order("derivation")
        ready(corpus)
        with pytest.raises(ValueError, match="graph kind"):
            corpus.advance_order("other")
    with pytest.raises(RuntimeError, match="closed"):
        corpus.advance_order("derivation")


def snapshot(order):
    return (
        order.db.execute("SELECT * FROM checkpoint").fetchall(),
        order.db.execute("SELECT * FROM nodes ORDER BY id").fetchall(),
        order.db.execute("SELECT * FROM ready ORDER BY id").fetchall(),
    )


def test_capacity_checks_lengths_before_endpoint_payloads_and_preserves_retry(monkeypatch):
    with CanonicalCorpusValidator() as corpus:
        edge = ("a" + "é" * 4096, "b" + "é" * 4096)
        insert(corpus, [edge], "derivation")
        ready(corpus)
        read = ordering._CorpusOrder._read

        def observed(db, query, parameters=()):
            assert not query.startswith("SELECT source, target")
            return read(db, query, parameters)

        with monkeypatch.context() as patch:
            patch.setattr(ordering._CorpusOrder, "_read", staticmethod(observed))
            with pytest.raises(JournalValidationCapacity):
                corpus.advance_order("derivation", byte_budget=1024)
        order = corpus._orders["derivation"]
        assert order.db.execute("SELECT source_edges FROM checkpoint").fetchone() == ("0",)
        assert not order.db.in_transaction
        assert order.db.execute("SELECT count(*) FROM nodes").fetchone() == (0,)
        result = finish(corpus, "derivation", rows=1, octets=65536)
        assert result.source_edges == 1 and result.vertices == result.completed_vertices == 2


def test_interrupted_quantum_rolls_back_mutations_with_checkpoint_and_retains_prior_calls(
    monkeypatch,
):
    class Interrupted(BaseException):
        pass

    with CanonicalCorpusValidator() as corpus:
        insert(corpus, edges_for("chain", 17), "derivation")
        ready(corpus)
        first = corpus.advance_order("derivation", row_budget=7)
        assert first.source_edges == 7
        order = corpus._orders["derivation"]
        before = snapshot(order)
        write, inserted = order._write, []

        def interrupted(query, parameters=()):
            if query.startswith("INSERT INTO nodes"):
                inserted.append(parameters)
                if len(inserted) == 3:
                    raise Interrupted
            return write(query, parameters)

        with monkeypatch.context() as patch:
            patch.setattr(order, "_write", interrupted)
            with pytest.raises(Interrupted):
                corpus.advance_order("derivation", row_budget=7)
        assert snapshot(order) == before
        assert not order.db.in_transaction
        result = finish(corpus, "derivation", rows=1)
        assert result.source_edges == 17 and result.completed_vertices == 18


@pytest.mark.parametrize("phase", ["input", "ready"])
def test_actual_order_database_write_cancellation_preserves_committed_checkpoint(
    phase, monkeypatch
):
    with CanonicalCorpusValidator() as corpus:
        insert(corpus, edges_for("chain", 17), "derivation")
        ready(corpus)
        state = corpus.advance_order("derivation", row_budget=7)
        order = corpus._orders["derivation"]
        while state.phase != phase:
            state = corpus.advance_order("derivation", row_budget=1)
        before = snapshot(order)
        write = order._write
        trigger = "INSERT OR IGNORE INTO nodes" if phase == "input" else "DELETE FROM ready"

        def cancelled(query, parameters=()):
            if query.startswith(trigger):
                order.db.set_progress_handler(lambda: 1, 1)
            return write(query, parameters)

        try:
            with monkeypatch.context() as patch:
                patch.setattr(order, "_write", cancelled)
                with pytest.raises(sqlite3.OperationalError, match="interrupted"):
                    corpus.advance_order("derivation", row_budget=7)
        finally:
            order.db.set_progress_handler(None, 0)
        assert snapshot(order) == before
        assert not order.db.in_transaction
        assert finish(corpus, "derivation", rows=1).completed_vertices == 18


def test_actual_source_cancellation_cannot_skip_unread_ordering_edges():
    with CanonicalCorpusValidator() as corpus:
        insert(corpus, edges_for("chain", 17), "derivation")
        corpus._db.commit()
        ready(corpus)
        assert corpus.advance_order("derivation", row_budget=7).source_edges == 7
        order = corpus._orders["derivation"]
        before = snapshot(order)
        corpus._db.set_progress_handler(lambda: 1, 1)
        try:
            with pytest.raises(sqlite3.OperationalError, match="interrupted"):
                corpus.advance_order("derivation", row_budget=7)
        finally:
            corpus._db.set_progress_handler(None, 0)
        assert snapshot(order) == before
        assert finish(corpus, "derivation", rows=1).source_edges == 17


def test_lost_commit_return_reloads_committed_progress_without_repeating_edges(monkeypatch):
    with CanonicalCorpusValidator() as corpus:
        insert(corpus, edges_for("chain", 17), "derivation")
        ready(corpus)
        first = corpus.advance_order("derivation", row_budget=7)
        order = corpus._orders["derivation"]
        write = order._write

        def lost_return(query, parameters=()):
            result = write(query, parameters)
            if query == "COMMIT":
                raise InterruptedError("committed return lost")
            return result

        with monkeypatch.context() as patch:
            patch.setattr(order, "_write", lost_return)
            with pytest.raises(InterruptedError):
                corpus.advance_order("derivation", row_budget=3)
        assert order.db.execute("SELECT source_edges FROM checkpoint").fetchone() == (
            str(first.source_edges + 3),
        )
        result = finish(corpus, "derivation", rows=1)
        assert result.source_edges == 17 and result.vertices == result.completed_vertices == 18


def test_whole_validation_resumes_an_existing_partial_order_instead_of_restarting_it(monkeypatch):
    with CanonicalCorpusValidator() as corpus:
        insert(corpus, edges_for("chain", 17), "derivation")
        ready(corpus)
        assert corpus.advance_order("derivation", row_budget=7).source_edges == 7
        acyclic = corpus._acyclic

        def independent(kind):
            assert kind != "derivation"
            return acyclic(kind)

        with monkeypatch.context() as patch:
            patch.setattr(corpus, "_acyclic", independent)
            corpus.validate()
        assert corpus._orders == {}


@pytest.mark.growth_witness(
    "native-corpus-order-advance-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus.CanonicalCorpusValidator.advance_order",
)
def test_actual_ordering_quanta_bound_payload_and_sql_work(monkeypatch, record_property):
    binding = growth_binding(CanonicalCorpusValidator.advance_order)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    samples = []
    for count, rows, octets in ((0, 1, 512), (1, 1, 512), (17, 7, 512), (129, 64, 4096)):
        with CanonicalCorpusValidator() as corpus:
            insert(corpus, edges_for("branch", count), "derivation")
            ready(corpus)
            recorder = Recorder()
            with source_reads(monkeypatch) as frames, recorder.observe(corpus, monkeypatch):
                for _ in range(10 * (count + 1)):
                    recorder.metrics.clear()
                    state = corpus.advance_order("derivation", row_budget=rows, byte_budget=octets)
                    metrics = {
                        bound.metric: recorder.metrics[bound.metric]
                        for bound in binding.declaration.bounds
                    }
                    metrics["processed_rows"] = state.processed_atoms
                    metrics["source_frame_reads"] = len(frames)
                    assert metrics["projected_payload_octets"] == state.admitted_octets
                    samples.append(
                        {
                            "declaration": binding.declaration.id,
                            "declaration_sha256": digest,
                            "dimensions": {"row_budget": rows, "byte_budget": octets},
                            "metrics": metrics,
                            "delegates": {},
                        }
                    )
                    if state.complete:
                        recorder.metrics.clear()
                        assert (
                            corpus.advance_order("derivation", byte_budget=1).admitted_octets == 0
                        )
                        assert recorder.metrics["sql_statements"] == 0
                        break
                else:
                    pytest.fail("ordering continuation did not complete")
                corpus._close_orders()
    validate_execution_samples(projection, binding.declaration.id, samples)
    record_property("growth_samples", samples)
