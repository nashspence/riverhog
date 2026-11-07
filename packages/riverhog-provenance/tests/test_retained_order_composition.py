from __future__ import annotations

import hashlib
from collections import defaultdict
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
import riverhog_provenance._corpus_order as ordering
import riverhog_provenance.corpus as corpus_owner
from riverhog_growth_contracts import growth_binding
from riverhog_provenance import CanonicalCorpusValidator, RetainedCorpusValidator
from test_corpus_order_growth import Connection as BodyConnection
from test_corpus_order_growth import Recorder, dimensions, edges_for, identity, insert
from test_retained_corpus_faults import EmptySource
from test_retained_corpus_growth import Connection, Observer, octets
from test_retained_corpus_workspace import open_workspace
from test_source_anchor_growth import record

ROOT = RetainedCorpusValidator.advance_order
NATIVE = CanonicalCorpusValidator.advance_order
TRANSACTION = RetainedCorpusValidator._operation


class PhysicalConnection(Connection):
    def execute(self, query, parameters=()):
        cursor = super().execute(query, parameters)
        if cursor.rowcount > 0 and query.startswith(("INSERT", "UPDATE", "DELETE")):
            self.observer.metrics["written_rows"] += cursor.rowcount
            self.observer.metrics["written_parameter_octets"] += octets(parameters)
            if "_ready" in query:
                self.observer.ready_delta += cursor.rowcount * (
                    -1 if query.startswith("DELETE") else 1
                )
        return cursor


class PhysicalObserver(Observer):
    def __init__(self):
        super().__init__()
        self.body = None
        self.native_active = False
        self.ready_delta = 0
        self.transitions = defaultdict(int)
        self.result = None

    @contextmanager
    def observe(self, monkeypatch, source, workspace):
        db = workspace._db
        next_atom = ordering._CorpusOrder._next
        decode, encode = ordering._decode_count, ordering._encode_count

        def stepped():
            self.metrics["sqlite_vm_instructions"] += 1
            return 0

        def transitioned(order, before, admission):
            after = next_atom(order, before, admission)
            self.metrics["completed_phase_atoms"] += 1
            if before.phase == "input" and after.source_edges > before.source_edges:
                self.transitions["input"] += 1
            elif before.phase == "seed" and after.seed != before.seed:
                self.transitions["seed"] += 1
            elif before.phase == "ready" and after.phase == "outgoing":
                self.transitions["ready"] += 1
            elif before.phase == "outgoing" and after.phase == "outgoing":
                self.transitions["outgoing"] += 1
            return after

        def decoded(value):
            self.metrics["degree_decoded_octets"] += octets(value)
            return decode(value)

        def encoded(value):
            result = encode(value)
            self.metrics["degree_encoded_octets"] += octets(result)
            return result

        def native(corpus, kind, **budgets):
            self.metrics["native_calls"] += 1
            self.body = Recorder()
            order = corpus._orders[kind]
            order.db = BodyConnection(order.db, self.body)
            order.source = BodyConnection(order.source, self.body)
            self.native_active = True
            try:
                result = NATIVE(corpus, kind, **budgets)
            finally:
                self.native_active = False
            assert result.processed_atoms == self.metrics["completed_phase_atoms"]
            assert result.admitted_octets == self.body.metrics["projected_payload_octets"]
            self.result = result
            return result

        def forbidden(*_args, **_kwargs):
            raise AssertionError("retained ordering loaded source data or hashed content")

        def authorized(action, table, column, *_rest):
            if (
                action == corpus_owner.sqlite3.SQLITE_READ
                and table not in ("corpus_workspace", "edges")
                and not table.startswith("corpus_order_")
            ):
                raise AssertionError("retained ordering read unrelated source rows")
            return corpus_owner.sqlite3.SQLITE_OK

        db.set_progress_handler(stepped, 1)
        db.set_authorizer(authorized)
        try:
            with super().observe(monkeypatch, source), monkeypatch.context() as patch:
                patch.setattr(workspace, "_db", PhysicalConnection(db, self))
                patch.setattr(CanonicalCorpusValidator, "advance_order", native)
                patch.setattr(ordering._CorpusOrder, "_next", transitioned)
                patch.setattr(ordering, "_decode_count", decoded)
                patch.setattr(ordering, "_encode_count", encoded)
                patch.setattr(corpus_owner, "canonical_document", forbidden)
                patch.setattr(corpus_owner, "json", SimpleNamespace(loads=forbidden))
                patch.setattr(hashlib, "sha256", forbidden)
                yield
        finally:
            db.set_progress_handler(None, 0)
            db.set_authorizer(None)
        assert self.body is not None and self.body.live == 0

    def sample(self, source, edges, kind, *, rows, byte_budget, retained_rows, counter_octets):
        native_metrics = self.body.metrics.copy()
        native_metrics["processed_rows"] = self.metrics["completed_phase_atoms"]
        native = record(
            growth_binding(NATIVE), {"row_budget": rows, "byte_budget": byte_budget}, native_metrics
        )
        lifecycle = self.metrics.copy()
        for metric in ("sql_statements", "returned_rows", "returned_octets", "parameter_octets"):
            lifecycle[metric] -= native_metrics[metric]
            assert lifecycle[metric] >= 0
        owner_dimensions = {
            "binding_octets": len(source.binding.to_bytes()),
            "checkpoint_octets": self.checkpoint_octets,
            "counter_octets": counter_octets,
            "retained_rows": retained_rows,
        }
        transaction = record(growth_binding(TRANSACTION), owner_dimensions, lifecycle)
        metrics = self.metrics.copy()
        metrics["algorithm_sql_statements"] = native_metrics["sql_statements"]
        metrics["admitted_phase_atoms"] = self.metrics["completed_phase_atoms"]
        for metric in (
            "projected_payload_octets",
            "source_edge_rows",
            "outgoing_edge_rows",
            "seed_vertex_rows",
            "ready_rows",
        ):
            metrics[metric] = native_metrics[metric]
        return record(
            growth_binding(ROOT),
            {
                **owner_dimensions,
                **dimensions(edges, kind),
                "row_budget": rows,
                "byte_budget": byte_budget,
                "processed_atoms": self.metrics["completed_phase_atoms"],
                "native_calls": self.metrics["native_calls"],
            },
            metrics,
            {"native": [native], "transaction": [transaction]},
        )


def measure(workspace, source, edges, kind, monkeypatch, *, rows, byte_budget, retained_rows):
    epoch, version, sources, selections = workspace._db.execute(
        "SELECT worker_epoch,version,sources,selections FROM corpus_workspace"
    ).fetchone()
    cookie = workspace._db.execute("PRAGMA schema_version").fetchone()[0]
    counter_octets = max(
        len(item)
        for item in (epoch, version, str(int(version) + 1), sources, selections, str(cookie))
    )
    observed = PhysicalObserver()
    with observed.observe(monkeypatch, source, workspace):
        result = ROOT(workspace, kind, row_budget=rows, byte_budget=byte_budget)
    sample = observed.sample(
        source,
        edges,
        kind,
        rows=rows,
        byte_budget=byte_budget,
        retained_rows=retained_rows,
        counter_octets=counter_octets,
    )
    return result, sample, observed


@pytest.mark.growth_witness(
    "native-retained-corpus-order-quantum/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus_workspace.RetainedCorpusValidator.advance_order",
)
def test_actual_retained_ordering_composes_lifecycle_and_physical_progress(
    tmp_path, catalog, monkeypatch, record_property
):
    samples = []
    shapes = [
        (shape, edges_for(shape, count), 64, 4096)
        for shape in ("chain", "reverse", "pairs", "branch", "converge")
        for count in (0, 1, 17, 129)
    ]
    shapes.append(("long-chain", edges_for("chain", 1025), 64, 8192))
    shapes.append(
        (
            "dense",
            [
                (identity(layer * 12 + i), identity((layer + 1) * 12 + j))
                for layer in range(2)
                for i in range(12)
                for j in range(12)
            ],
            64,
            8192,
        )
    )
    shapes.append(
        (
            "wide",
            [(left + "é" * 1024, right + "é" * 1024) for left, right in edges_for("chain", 17)],
            64,
            32768,
        )
    )
    for kind in ("derivation", "specialization"):
        other_kind = "specialization" if kind == "derivation" else "derivation"
        for index, (shape, edges, rows, byte_budget) in enumerate(shapes):
            path = tmp_path / (kind + str(index))
            path.mkdir()
            source = EmptySource(catalog)
            workspace = open_workspace(path, source, create=True)
            unrelated = edges_for("chain", 129)
            with workspace._operation():
                insert(workspace._corpus, edges, kind)
                insert(workspace._corpus, reversed(edges), kind)
                insert(workspace._corpus, unrelated, other_kind)
            while not workspace.advance_semantics().complete:
                pass
            while not workspace.advance_dependencies().complete:
                pass
            vertices = len({key for edge in edges for key in edge})
            total = defaultdict(int)
            transitions = defaultdict(int)
            previous_vertices = ready_rows = calls = 0
            resumed = False
            try:
                for _ in range(2 * len(edges) + 3 * vertices + 4):
                    retained_rows = len(edges) + len(unrelated) + previous_vertices + ready_rows + 3
                    result, sample, observed = measure(
                        workspace,
                        source,
                        edges,
                        kind,
                        monkeypatch,
                        rows=rows,
                        byte_budget=byte_budget,
                        retained_rows=retained_rows,
                    )
                    samples.append(sample)
                    calls += 1
                    for metric, value in sample["metrics"].items():
                        total[metric] += value
                    for phase, value in observed.transitions.items():
                        transitions[phase] += value
                    ready_rows += observed.ready_delta
                    assert ready_rows >= 0
                    previous_vertices = result.vertices
                    if not resumed and shape in ("long-chain", "dense"):
                        workspace.close()
                        workspace = open_workspace(path, source, epoch=1)
                        resumed = True
                    if result.complete:
                        break
                    assert result.processed_atoms > 0
                else:
                    pytest.fail("the fixed retained graph did not reach its exact terminal")
                assert result.source_edges == len(edges)
                assert result.vertices == result.completed_vertices == vertices
                assert ready_rows == 0
                assert transitions["input"] == transitions["outgoing"] == len(edges)
                assert transitions["seed"] == transitions["ready"] == vertices
                atoms = 2 * len(edges) + 3 * vertices + 3
                assert total["completed_phase_atoms"] == atoms
                assert calls <= atoms
                # Only a not-yet-committed admission can repeat an endpoint read.
                assert total["source_edge_rows"] <= len(edges) + calls
                assert total["outgoing_edge_rows"] <= len(edges) + calls
                assert total["seed_vertex_rows"] == total["ready_rows"] == vertices
                assert total["algorithm_sql_statements"] <= 32 * (atoms + calls)
                retained_rows = len(edges) + len(unrelated) + vertices + 3
                warm, sample, _ = measure(
                    workspace,
                    source,
                    edges,
                    kind,
                    monkeypatch,
                    rows=1,
                    byte_budget=byte_budget,
                    retained_rows=retained_rows,
                )
                assert warm.complete and warm.processed_atoms == 0
                samples.append(sample)
            finally:
                workspace.close()
    record_property("growth_samples", samples)
