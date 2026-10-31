from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
import riverhog_provenance.corpus as native_owner
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_growth_contracts import growth_binding
from riverhog_provenance import (
    CanonicalCorpusValidator,
    IncompleteSourceError,
    JournalValidationCapacity,
    ProvenanceValidationError,
    RetainedCorpusValidator,
    external_reference,
    new_id,
    parse_journal,
    selected_delivery_occurrence,
    validate_journal,
)
from riverhog_provenance_contracts import canonical_document
from test_corpus_delivery_support import delivery_source
from test_corpus_historical_source import SelectionObserver, correction_history, replacement_ladder
from test_corpus_source_growth import source_reads
from test_journal_result import history
from test_retained_corpus_growth import Connection, Cursor, Observer, octets
from test_retained_corpus_workspace import OwnedCorpusSource, open_workspace
from test_source_anchor_growth import record

TRANSACTION = RetainedCorpusValidator._operation


class PointCursor(Cursor):
    def fetchone(self):
        row = self.cursor.fetchone()
        if row is not None:
            self.observer.metrics["returned_rows"] += 1
            self.observer.metrics["returned_octets"] += octets(row)
        return row


class PointConnection(Connection):
    def execute(self, query, parameters=()):
        self.observer.metrics["sql_statements"] += 1
        self.observer.metrics["parameter_octets"] += octets(parameters)
        actual = self.db.execute(query, parameters)
        if query.startswith(("INSERT", "UPDATE", "DELETE")) and actual.rowcount > 0:
            self.observer.metrics["written_rows"] += actual.rowcount
            self.observer.metrics["written_parameter_octets"] += octets(parameters)
        return PointCursor(actual, self.observer)


class PointObserver(Observer):
    def __init__(self, operation):
        super().__init__()
        self.operation = operation
        self.native = getattr(CanonicalCorpusValidator, operation)
        self.root = getattr(RetainedCorpusValidator, operation)
        self.body = None
        self.in_native = False

    @contextmanager
    def observe(self, monkeypatch, source, workspace):
        db = workspace._db

        def stepped():
            self.metrics["sqlite_vm_instructions"] += 1
            if self.in_native:
                self.body.metrics["sqlite_vm_instructions"] += 1
            return 0

        def decoded(value):
            self.metrics["json_decodes"] += 1
            self.metrics["decoded_octets"] += len(value)
            self.body.metrics["json_decodes"] += 1
            self.body.metrics["decoded_octets"] += len(value)
            return json.loads(value)

        def native(corpus, *args, **kwargs):
            self.metrics["native_calls"] += 1
            actual = corpus._db
            self.body = SelectionObserver(actual)
            corpus._db, self.in_native = self.body, True
            try:
                return self.native(corpus, *args, **kwargs)
            finally:
                corpus._db, self.in_native = actual, False

        allowed = {"corpus_workspace", "assertions", "entries"}
        allowed.add(
            "delivery_projections"
            if self.operation == "selected_delivery_support"
            else "assertion_payloads"
        )

        def authorized(action, table, _column, *_rest):
            if action == native_owner.sqlite3.SQLITE_READ and table not in allowed:
                raise AssertionError("point lookup read unrelated retained data")
            return native_owner.sqlite3.SQLITE_OK

        db.set_progress_handler(stepped, 1)
        db.set_authorizer(authorized)
        try:
            with super().observe(monkeypatch, source), monkeypatch.context() as patch:
                connection = PointConnection(db, self)
                patch.setattr(workspace, "_db", connection)
                patch.setattr(workspace._corpus, "_db", connection)
                patch.setattr(CanonicalCorpusValidator, self.operation, native)
                patch.setattr(native_owner, "json", SimpleNamespace(loads=decoded))
                with source_reads(monkeypatch) as frames:
                    try:
                        yield
                    finally:
                        assert frames == []
        finally:
            db.set_progress_handler(None, 0)
            db.set_authorizer(None)
        assert self.live == 0
        assert self.body is None or all(cursor.closed for cursor in self.body.cursors)

    def sample(self, source, dimensions):
        metrics = self.metrics.copy()
        lifecycle = self.metrics.copy()
        body_metrics = None if self.body is None else self.body.metrics
        if body_metrics is not None:
            for metric in ("sql_statements", "returned_rows", "parameter_octets"):
                lifecycle[metric] -= body_metrics[metric]
            lifecycle["returned_octets"] -= (
                body_metrics["metadata_octets"] + body_metrics["payload_octets"]
            )
            metrics["payload_octets"] = body_metrics["payload_octets"]
        assert all(
            lifecycle[metric] >= 0
            for metric in ("sql_statements", "returned_rows", "parameter_octets", "returned_octets")
        )
        owner_dimensions = {
            "binding_octets": len(source.binding.to_bytes()),
            "checkpoint_octets": self.checkpoint_octets,
            "counter_octets": dimensions["counter_octets"],
            "retained_rows": dimensions["retained_rows"],
        }
        native_dimensions = {
            item.name: dimensions[item.name]
            for item in growth_binding(self.native).declaration.dimensions
        }
        native = (
            []
            if body_metrics is None
            else [record(growth_binding(self.native), native_dimensions, body_metrics)]
        )
        transaction = record(growth_binding(TRANSACTION), owner_dimensions, lifecycle)
        return record(
            growth_binding(self.root),
            {**owner_dimensions, **native_dimensions, "native_calls": metrics["native_calls"]},
            metrics,
            {"native": native, "transaction": [transaction]},
        )


def measure(
    workspace,
    source,
    operation,
    anchor,
    selected_octets,
    monkeypatch,
    *args,
    rejection=None,
    **kwargs,
):
    # Independent retained extents and fixture-selected payload widths precede observation.
    db = workspace._db
    assertions = db.execute("SELECT count(*) FROM assertions").fetchone()[0]
    frames = db.execute("SELECT count(*) FROM entries").fetchone()[0]
    scalars = db.execute(
        "SELECT worker_epoch,version,sources,selections FROM corpus_workspace"
    ).fetchone()
    cookie = db.execute("PRAGMA schema_version").fetchone()[0]
    counter_octets = max(len(value) for value in (*scalars, str(int(scalars[1]) + 1), str(cookie)))
    dimensions = {
        "retained_rows": assertions + frames,
        "retained_assertions": assertions,
        "retained_frames": frames,
        "counter_octets": counter_octets,
        "anchor_octets": len(canonical_document(anchor.to_mapping())),
        "selected_octets": selected_octets,
    }
    observer = PointObserver(operation)
    with observer.observe(monkeypatch, source, workspace):
        if rejection is None:
            result = getattr(workspace, operation)(*args, **kwargs)
        else:
            with pytest.raises(rejection):
                getattr(workspace, operation)(*args, **kwargs)
            result = None
    return result, observer.sample(source, dimensions), observer


def populate(workspace, summary):
    while not workspace.advance_source(summary, frame_budget=17).complete:
        pass


def original_octets(oracle, expected):
    if expected is None:
        return 0
    return len(canonical_document(expected)) + len(
        canonical_document(oracle.assertion_entries[expected["assertion_id"]])
    )


@pytest.mark.growth_witness(
    "native-retained-corpus-historical-point/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus_workspace.RetainedCorpusValidator.historical_assertion",
)
def test_retained_historical_points_compose_exact_intervals_and_owner_work(
    tmp_path, journal, who, catalog, monkeypatch, record_property
):
    samples = []
    cases = [(history(journal, count, catalog), None, (0, count - 1)) for count in (1, 17, 129)]
    raw, rows = correction_history(journal, who, catalog)
    cases.append((raw, rows[0]["id"], tuple(range(6))))
    for count in (1, 17, 129):
        raw, first = replacement_ladder(journal, who, count, catalog)
        cases.append((raw, first["id"], (1, count, count + 1)))
    for index, (raw, identity, positions) in enumerate(cases):
        path = tmp_path / str(index)
        path.mkdir()
        with validate_journal(raw, catalog=catalog) as summary:
            source = OwnedCorpusSource(summary, catalog)
            workspace = open_workspace(path, source, create=True)
            try:
                populate(workspace, summary)
                identity = identity or summary.states[0]["id"]
                frames = parse_journal(raw)
                for sequence in positions:
                    prefix = b"".join(frame.encoded for frame in frames[: sequence + 1])
                    with validate_journal(prefix, catalog=catalog) as oracle:
                        anchor = HistoryJournalAnchor.from_mapping(oracle.anchor)
                        expected = oracle.graph_validation.objects.get(identity)
                        result, sample, observed = measure(
                            workspace,
                            source,
                            "historical_assertion",
                            anchor,
                            original_octets(oracle, expected),
                            monkeypatch,
                            anchor,
                            identity,
                            byte_budget=2**22,
                        )
                        samples.append(sample)
                        assert (result.value if result else None) == expected
                        if result:
                            assert result.reference == external_reference(oracle, identity)
                        assert observed.body.metrics["sql_statements"] == (3 if result else 2)
                        result, sample, _ = measure(
                            workspace,
                            source,
                            "historical_assertion",
                            anchor,
                            0,
                            monkeypatch,
                            anchor,
                            new_id(),
                            byte_budget=2**22,
                        )
                        assert result is None
                        samples.append(sample)
                workspace.close()
                workspace = open_workspace(path, source, epoch=1)
                anchor = HistoryJournalAnchor.from_mapping(summary.anchor)
                identity = summary.states[0]["id"]
                expected = summary.graph_validation.objects[identity]
                selected = original_octets(summary, expected)
                before = (path / "corpus.sqlite3").read_bytes()
                _, sample, observed = measure(
                    workspace,
                    source,
                    "historical_assertion",
                    anchor,
                    0,
                    monkeypatch,
                    anchor,
                    identity,
                    byte_budget=selected - 1,
                    rejection=JournalValidationCapacity,
                )
                assert (path / "corpus.sqlite3").read_bytes() == before
                assert sample["metrics"]["payload_octets"] == 0
                assert observed.body.metrics["sql_statements"] == 2
                samples.append(sample)
                result, sample, _ = measure(
                    workspace,
                    source,
                    "historical_assertion",
                    anchor,
                    selected,
                    monkeypatch,
                    anchor,
                    identity,
                    byte_budget=selected,
                )
                assert result.value == expected
                samples.append(sample)
                before = (path / "corpus.sqlite3").read_bytes()
                checks = iter((True, False))
                with monkeypatch.context() as patch:
                    patch.setattr(source, "verify_current", lambda checks=checks: next(checks))
                    _, sample, observed = measure(
                        workspace,
                        source,
                        "historical_assertion",
                        anchor,
                        selected,
                        monkeypatch,
                        anchor,
                        identity,
                        byte_budget=selected,
                        rejection=IncompleteSourceError,
                    )
                assert (path / "corpus.sqlite3").read_bytes() == before
                assert observed.body.metrics["sql_statements"] == 3
                assert sample["metrics"]["source_checks"] == 2
                samples.append(sample)
                wrong = replace(anchor, prefix_sha256="0" * 64)
                _, sample, observed = measure(
                    workspace,
                    source,
                    "historical_assertion",
                    wrong,
                    0,
                    monkeypatch,
                    wrong,
                    identity,
                    byte_budget=selected,
                    rejection=ProvenanceValidationError,
                )
                assert observed.body.metrics["sql_statements"] == 1
                samples.append(sample)
                source.current = False
                _, sample, observed = measure(
                    workspace,
                    source,
                    "historical_assertion",
                    anchor,
                    0,
                    monkeypatch,
                    anchor,
                    identity,
                    byte_budget=selected,
                    rejection=IncompleteSourceError,
                )
                assert observed.body is None
                assert sample["metrics"]["sql_statements"] == 0
                samples.append(sample)
            finally:
                workspace.close()
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "native-retained-corpus-delivery-point/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus_workspace.RetainedCorpusValidator.selected_delivery_support",
)
def test_retained_delivery_points_preserve_narrow_support_and_owner_work(
    tmp_path, graph, who, catalog, monkeypatch, record_property
):
    samples = []
    payload_sizes = []
    for index, (opaque, role) in enumerate(
        (
            (0, "urn:test:member"),
            (2048, "urn:test:member"),
            (65536, "urn:test:member"),
            (0, "urn:test:" + "r" * 65536),
        )
    ):
        path = tmp_path / str(index)
        path.mkdir()
        with delivery_source(graph, who, catalog, opaque_octets=opaque, role=role) as (
            summary,
            arguments,
        ):
            source = OwnedCorpusSource(summary, catalog)
            workspace = open_workspace(path, source, create=True)
            try:
                populate(workspace, summary)
                expected = selected_delivery_occurrence(summary, **arguments)
                references = tuple(external_reference(summary, row["id"]) for row in expected)
                anchor = HistoryJournalAnchor.from_mapping(summary.anchor)
                # This fixture contains exactly the four independently expected delivery inputs.
                count, selected = workspace._db.execute(
                    "SELECT count(*),sum(length(a.entry)+length(p.value)) "
                    "FROM assertions a JOIN delivery_projections p "
                    "ON p.journal=a.journal AND p.assertion=a.assertion"
                ).fetchone()
                assert count == 4
                before = (path / "corpus.sqlite3").read_bytes()
                _, sample, observed = measure(
                    workspace,
                    source,
                    "selected_delivery_support",
                    anchor,
                    0,
                    monkeypatch,
                    **arguments,
                    metadata_byte_budget=1,
                    rejection=JournalValidationCapacity,
                )
                assert (path / "corpus.sqlite3").read_bytes() == before
                assert observed.body.metrics["sql_statements"] == 2
                assert sample["metrics"]["payload_octets"] == 0
                samples.append(sample)
                for epoch in (0, 1, 2):
                    if epoch:
                        workspace.close()
                        workspace = open_workspace(path, source, epoch=epoch)
                    actual, sample, observed = measure(
                        workspace,
                        source,
                        "selected_delivery_support",
                        anchor,
                        selected,
                        monkeypatch,
                        **arguments,
                        metadata_byte_budget=131072,
                    )
                    assert (actual.state_reference, actual.occurrence_reference) == references
                    assert observed.body.metrics["sql_statements"] == 9
                    assert sample["metrics"]["json_decodes"] == 8
                    assert sample["metrics"]["full_assertion_reads"] == 0
                    assert sample["metrics"]["payload_octets"] == selected
                    if len(role) < 100:
                        payload_sizes.append(selected)
                    samples.append(sample)
                before = (path / "corpus.sqlite3").read_bytes()
                checks = iter((True, False))
                with monkeypatch.context() as patch:
                    patch.setattr(source, "verify_current", lambda checks=checks: next(checks))
                    _, sample, observed = measure(
                        workspace,
                        source,
                        "selected_delivery_support",
                        anchor,
                        selected,
                        monkeypatch,
                        **arguments,
                        metadata_byte_budget=131072,
                        rejection=IncompleteSourceError,
                    )
                assert (path / "corpus.sqlite3").read_bytes() == before
                assert observed.body.metrics["sql_statements"] == 9
                assert sample["metrics"]["source_checks"] == 2
                samples.append(sample)
                source.current = False
                _, sample, observed = measure(
                    workspace,
                    source,
                    "selected_delivery_support",
                    anchor,
                    0,
                    monkeypatch,
                    **arguments,
                    metadata_byte_budget=131072,
                    rejection=IncompleteSourceError,
                )
                assert observed.body is None and sample["metrics"]["sql_statements"] == 0
                samples.append(sample)
            finally:
                workspace.close()
    assert max(payload_sizes) - min(payload_sizes) < 32
    record_property("growth_samples", samples)
