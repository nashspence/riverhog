from __future__ import annotations

import copy
import json
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
import riverhog_provenance.corpus as native_owner
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_growth_contracts import growth_binding
from riverhog_provenance import (
    CanonicalCorpusValidator,
    IncompleteSourceError,
    JournalValidationCapacity,
    RetainedCorpusValidator,
    assertion,
    create_journal,
    new_id,
    parse_journal,
    reference,
    validate_journal,
)
from test_corpus_selection import edge_history, semantic_source
from test_corpus_source_growth import source_reads
from test_retained_corpus_growth import Observer, octets
from test_retained_corpus_workspace import OwnedCorpusSource, open_workspace
from test_retained_dependency_composition import PhysicalConnection
from test_source_anchor_growth import record

ROOT = RetainedCorpusValidator.advance_semantics
NATIVE = CanonicalCorpusValidator.advance_semantics
TRANSACTION = RetainedCorpusValidator._operation
KINDS = {"derivation", "specialization", "content_comparison"}


class CapturedCursor:
    def __init__(self, cursor, observer, kind):
        self.cursor, self.observer, self.kind = cursor, observer, kind

    @property
    def rowcount(self):
        return self.cursor.rowcount

    def fetchone(self):
        row = self.cursor.fetchone()
        if row is not None:
            if self.kind == "input":
                self.observer.inputs.append(tuple(row[:2]))
            else:
                self.observer.metrics["input_count_returned_rows"] += 1
                self.observer.metrics["input_count_returned_octets"] += octets(row)
        return row

    def close(self):
        self.cursor.close()


class SemanticConnection(PhysicalConnection):
    def execute(self, query, parameters=()):
        cursor = super().execute(query, parameters)
        if query.startswith("SELECT a.journal,a.assertion,"):
            return CapturedCursor(cursor, self.observer, "input")
        if query == "SELECT sources,selections FROM corpus_workspace WHERE singleton=1":
            self.observer.metrics["input_count_checks"] += 1
            return CapturedCursor(cursor, self.observer, "counts")
        return cursor


class SemanticObserver(Observer):
    def __init__(self):
        super().__init__()
        self.inputs = []
        self.body = Observer()
        self.body.inputs = []
        self.confirmed = []

    @contextmanager
    def observe(self, monkeypatch, source, workspace):
        db = workspace._db

        def stepped():
            self.metrics["sqlite_vm_instructions"] += 1
            return 0

        def decoded(value):
            self.metrics["json_decodes"] += 1
            self.body.metrics["json_decodes"] += 1
            self.metrics["semantic_decoded_octets"] += octets(value)
            return json.loads(value)

        def forbidden(*_args, **_kwargs):
            raise AssertionError("semantic registration traversed original source data")

        def native(corpus, **budgets):
            self.metrics["native_calls"] += 1
            before = corpus._semantic_processed
            actual = corpus._db
            corpus._db = SemanticConnection(actual, self.body)
            try:
                result = NATIVE(corpus, **budgets)
            finally:
                corpus._db = actual
            delta = result.processed_assertions - before
            assert delta <= len(self.body.inputs) <= delta + 1
            self.confirmed = self.body.inputs[:delta]
            if delta:
                assert corpus._semantic_after == self.confirmed[-1]
            self.body.metrics["processed_rows"] = delta
            self.metrics["completed_semantic_inputs"] = len(self.confirmed)
            assert result.admitted_octets == self.body.metrics["projected_payload_octets"]
            return result

        def authorized(action, table, column, *_rest):
            if action == native_owner.sqlite3.SQLITE_READ and table not in {
                "corpus_workspace",
                "assertions",
                "semantic_inputs",
                "selected_prefixes",
                "edges",
                "comparisons",
            }:
                raise AssertionError("semantic union read unrelated or original source data")
            return native_owner.sqlite3.SQLITE_OK

        db.set_progress_handler(stepped, 1)
        db.set_authorizer(authorized)
        try:
            with super().observe(monkeypatch, source), monkeypatch.context() as patch:
                connection = SemanticConnection(db, self)
                patch.setattr(workspace, "_db", connection)
                patch.setattr(workspace._corpus, "_db", connection)
                patch.setattr(CanonicalCorpusValidator, "advance_semantics", native)
                patch.setattr(native_owner, "json", SimpleNamespace(loads=decoded))
                patch.setattr(native_owner, "canonical_document", forbidden)
                with source_reads(monkeypatch) as frames:
                    try:
                        yield
                    finally:
                        assert frames == []
        finally:
            db.set_progress_handler(None, 0)
            db.set_authorizer(None)
            assert self.live == 0 and self.body.live == 0

    def sample(self, source, *, rows, byte_budget, retained_rows, metadata_octets, counter_octets):
        native_metrics = self.body.metrics.copy()
        native = (
            record(
                growth_binding(NATIVE),
                {"row_budget": rows, "byte_budget": byte_budget},
                native_metrics,
            )
            if self.metrics["native_calls"]
            else None
        )
        lifecycle = self.metrics.copy()
        for metric in ("sql_statements", "returned_rows", "returned_octets", "parameter_octets"):
            lifecycle[metric] -= native_metrics[metric]
        lifecycle["sql_statements"] -= self.metrics["input_count_checks"]
        lifecycle["returned_rows"] -= self.metrics["input_count_returned_rows"]
        lifecycle["returned_octets"] -= self.metrics["input_count_returned_octets"]
        assert all(
            lifecycle[metric] >= 0
            for metric in (
                "sql_statements",
                "returned_rows",
                "returned_octets",
                "parameter_octets",
            )
        )
        owner_dimensions = {
            "binding_octets": len(source.binding.to_bytes()),
            "checkpoint_octets": self.checkpoint_octets,
            "counter_octets": counter_octets,
            "retained_rows": retained_rows,
        }
        transaction = record(growth_binding(TRANSACTION), owner_dimensions, lifecycle)
        metrics = self.metrics.copy()
        metrics["algorithm_sql_statements"] = native_metrics["sql_statements"]
        for metric in ("processed_rows", "projected_payload_octets"):
            metrics[metric] = native_metrics[metric]
        return record(
            growth_binding(ROOT),
            {
                **owner_dimensions,
                "row_budget": rows,
                "byte_budget": byte_budget,
                "native_calls": self.metrics["native_calls"],
                "processed_inputs": self.metrics["completed_semantic_inputs"],
                "metadata_octets": metadata_octets,
            },
            metrics,
            {"native": [] if native is None else [native], "transaction": [transaction]},
        )


def measure(
    workspace,
    source,
    monkeypatch,
    *,
    rows,
    byte_budget,
    retained_rows,
    metadata_octets,
    reject=None,
):
    scalars = workspace._db.execute(
        "SELECT worker_epoch,version,sources,selections FROM corpus_workspace"
    ).fetchone()
    cookie = workspace._db.execute("PRAGMA schema_version").fetchone()[0]
    counters = (
        *scalars,
        str(int(scalars[1]) + 1),
        str(cookie),
        str(source.binding.source_count),
        str(source.binding.selection_count),
    )
    counter_octets = max(map(len, counters))
    observed = SemanticObserver()
    with observed.observe(monkeypatch, source, workspace):
        if reject:
            with pytest.raises(reject):
                ROOT(workspace, row_budget=rows, byte_budget=byte_budget)
            progress = None
        else:
            progress = ROOT(workspace, row_budget=rows, byte_budget=byte_budget)
    sample = observed.sample(
        source,
        rows=rows,
        byte_budget=byte_budget,
        retained_rows=retained_rows,
        metadata_octets=metadata_octets,
        counter_octets=counter_octets,
    )
    return progress, sample, observed


def comparison_source(graph, who, catalog, count):
    graph = copy.deepcopy(graph)
    observation = next(row for row in graph["descriptions"] if row["type"] == "observation")
    for _ in range(count):
        reported = assertion(
            "reported_description",
            who,
            state=reference(graph["states"][0]["id"], "state"),
            content=copy.deepcopy(observation["content"]),
        )
        graph["descriptions"].append(reported)
        graph.setdefault("relations", []).append(
            assertion(
                "content_comparison",
                who,
                left_description=reference(observation["id"], "observation"),
                right_description=reference(reported["id"], "reported_description"),
                result="matching_fixity",
                algorithm="sha-256",
            )
        )
    return create_journal(graph, recorded_by_agent_id=who, catalog=catalog)


def specialization_source(graph, who, catalog, count):
    graph = copy.deepcopy(graph)
    general = graph["artifacts"][0]
    for _ in range(count):
        specific = copy.deepcopy(general)
        specific["id"], specific["assertion_id"] = new_id(), new_id()
        graph["artifacts"].append(specific)
        graph.setdefault("relations", []).append(
            assertion(
                "specialization",
                who,
                specific=reference(specific["id"], "artifact"),
                general=reference(general["id"], "artifact"),
            )
        )
    return create_journal(graph, recorded_by_agent_id=who, catalog=catalog)


def expected_facts(objects, edges, comparisons):
    for value in objects.values():
        if value["type"] in {"derivation", "specialization"}:
            left, right = (
                ("used_state", "generated_state")
                if value["type"] == "derivation"
                else ("specific", "general")
            )
            edges.add((value["type"], value[left]["object_id"], value[right]["object_id"]))
        elif value["type"] == "content_comparison":
            comparisons.add(
                (
                    value["assertion_id"],
                    value["left_description"]["object_id"],
                    value["right_description"]["object_id"],
                    value["result"],
                )
            )


@pytest.mark.growth_witness(
    "native-retained-corpus-semantic-quantum/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus_workspace.RetainedCorpusValidator.advance_semantics",
)
def test_actual_selected_union_composes_manifests_lifecycle_and_physical_progress(
    tmp_path, graph, who, journal, catalog, monkeypatch, record_property
):
    samples, equal_work = [], []
    retired, _ = edge_history(journal, who, catalog)
    cases = [
        ("chain", 0, 1, 64, 4096, None),
        ("chain", 1, 1, 1, 4096, None),
        ("chain", 17, 17, 7, 4096, None),
        ("chain", 17, 129, 7, 4096, None),
        ("chain", 129, 1, 64, 512, None),
        ("comparison", 17, 1, 7, 4096, None),
        ("specialization", 17, 1, 7, 4096, None),
        *[("retired", 1, 3, 1, 4096, selected) for selected in ((0,), (1,), (2,), (0, 2), (1, 2))],
    ]
    for case, count, prefixes, rows, byte_budget, selected in cases:
        if case == "retired":
            raw = retired
        elif case == "comparison":
            raw = comparison_source(graph, who, catalog, count)
        elif case == "specialization":
            raw = specialization_source(graph, who, catalog, count)
        else:
            unrelated = copy.deepcopy(graph)
            unrelated["agents"][0]["name"] = "unrelated opaque text" * 4096
            raw = (
                semantic_source(unrelated, who, catalog, count, prefixes)
                if count
                else create_journal(unrelated, recorded_by_agent_id=who, catalog=catalog)
            )
        frames = parse_journal(raw)
        original = [
            value
            for frame in frames
            for group in frame.document["body"].get("assertions", {}).values()
            for value in group
        ]
        with validate_journal(raw, catalog=catalog) as native:
            sequences = tuple(range(prefixes)) if selected is None else selected
            anchors = [HistoryJournalAnchor.from_mapping(native.anchor_at(s)) for s in sequences]
            source = OwnedCorpusSource(native, catalog, anchors)
            expected = sorted(
                (native.journal_id, v["assertion_id"]) for v in original if v["type"] in KINDS
            )
            expected_edges, expected_comparisons = set(), set()
            if case == "retired":
                for sequence in sequences:
                    prefix = b"".join(frame.encoded for frame in frames[: sequence + 1])
                    with validate_journal(prefix, catalog=catalog) as oracle:
                        expected_facts(
                            oracle.graph_validation.objects, expected_edges, expected_comparisons
                        )
            else:
                expected_facts(
                    native.graph_validation.objects, expected_edges, expected_comparisons
                )
            metadata_octets = max(
                len(native.journal_id.encode()),
                *(len(v[key].encode()) for v in original for key in ("assertion_id", "id", "type")),
                len(str(len(raw))),
                len(str(prefixes)),
            )
            # Each fixture assertion has at most ten indexed Native rows; frame and selected
            # prefix rows add separately. This input-derived upper extent includes unrelated data.
            retained_rows = 10 * (len(original) + len(frames) + len(anchors) + 1)
            path = tmp_path / str(len(samples))
            path.mkdir()
            workspace = open_workspace(path, source, create=True)
            try:
                while not workspace.advance_source(native, frame_budget=64).complete:
                    pass
                if len(anchors) > 1:
                    workspace.select_source(anchors[0])
                    _, sample, observed = measure(
                        workspace,
                        source,
                        monkeypatch,
                        rows=1,
                        byte_budget=byte_budget,
                        retained_rows=retained_rows,
                        metadata_octets=metadata_octets,
                        reject=IncompleteSourceError,
                    )
                    assert sample["metrics"]["native_calls"] == 0
                    assert sample["metrics"]["input_count_checks"] == 1
                    samples.append(sample)
                for anchor in anchors:
                    workspace.select_source(anchor)
                if case == "chain" and count == 1:
                    before = (path / "corpus.sqlite3").read_bytes()
                    _, sample, observed = measure(
                        workspace,
                        source,
                        monkeypatch,
                        rows=1,
                        byte_budget=1,
                        retained_rows=retained_rows,
                        metadata_octets=metadata_octets,
                        reject=JournalValidationCapacity,
                    )
                    assert (path / "corpus.sqlite3").read_bytes() == before
                    assert sample["metrics"]["projected_payload_octets"] == 0
                    assert sample["metrics"]["algorithm_sql_statements"] == 2
                    samples.append(sample)
                actual, native_sql, calls, restart = [], 0, 0, False
                for _ in range(len(expected) + 2):
                    progress, sample, observed = measure(
                        workspace,
                        source,
                        monkeypatch,
                        rows=rows,
                        byte_budget=byte_budget,
                        retained_rows=retained_rows,
                        metadata_octets=metadata_octets,
                    )
                    samples.append(sample)
                    actual.extend(observed.confirmed)
                    native_sql += sample["metrics"]["algorithm_sql_statements"]
                    calls += 1
                    if len(expected) >= 17 and actual and not restart:
                        workspace.close()
                        workspace = open_workspace(path, source, epoch=1)
                        restart = True
                    if progress.complete:
                        break
                else:
                    pytest.fail("retained selected-source union did not terminate")
                assert actual == expected
                assert progress.processed_assertions == len(expected)
                assert calls <= len(expected) + 1
                assert native_sql <= 6 * len(expected) + 2 * calls + 1
                if case == "chain" and count == 17:
                    equal_work.append(native_sql)
                assert (
                    set(workspace._db.execute("SELECT kind,source,target FROM edges"))
                    == expected_edges
                )
                assert (
                    set(
                        workspace._db.execute(
                            "SELECT assertion,left_object,right_object,result FROM comparisons"
                        )
                    )
                    == expected_comparisons
                )
                progress, sample, observed = measure(
                    workspace,
                    source,
                    monkeypatch,
                    rows=1,
                    byte_budget=byte_budget,
                    retained_rows=retained_rows,
                    metadata_octets=metadata_octets,
                )
                assert progress.complete and observed.confirmed == []
                assert sample["metrics"]["algorithm_sql_statements"] == 0
                samples.append(sample)
            finally:
                workspace.close()
    assert equal_work[0] == equal_work[1]
    record_property("growth_samples", samples)
