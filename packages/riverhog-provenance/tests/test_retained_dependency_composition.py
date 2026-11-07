from __future__ import annotations

import json
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
import riverhog_provenance.corpus as native_owner
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding
from riverhog_provenance import (
    CanonicalCorpusValidator,
    JournalValidationCapacity,
    RetainedCorpusValidator,
)
from test_corpus_source_growth import source_reads
from test_retained_corpus_faults import EmptySource
from test_retained_corpus_growth import Connection, Cursor, Observer, octets
from test_retained_corpus_workspace import open_workspace
from test_source_anchor_growth import record

ROOT = RetainedCorpusValidator.advance_dependencies
NATIVE = CanonicalCorpusValidator.advance_dependencies
TRANSACTION = RetainedCorpusValidator._operation


def populate(workspace, count, unrelated, width):
    # Independently materialized qualified metadata at Native's dependency boundary.
    prefix = "ü" * width
    journal, left, right = prefix + "journal", prefix + "left", prefix + "right"
    widths = [len(value.encode()) for value in (journal, left, right, "matching_fixity")]
    expected = []
    with workspace._operation():
        corpus = workspace._corpus
        db = corpus._db
        for index in range(count):
            entry = canonical_json_bytes({"sequence": str(index)})
            identity, object_id = prefix + str(index), prefix + "object-" + str(index)
            value = {
                "journal_id": journal,
                "assertion_id": identity,
                "object_id": object_id,
                "object_type": "state",
                "entry": {"sequence": str(index)},
            }
            anchor = {
                "journal_id": journal,
                "through": value["entry"],
                "prefix_sha256": "0" * 64,
                "prefix_bytes": str(index + 1),
            }
            widths.extend((len(entry), len(identity.encode()), len(object_id.encode()), 64))
            db.execute(
                "INSERT INTO assertions VALUES (?, ?, ?, ?, ?, ?, NULL, ?)",
                (journal, identity, object_id, "state", str(index), len(str(index)), entry),
            )
            db.execute("INSERT INTO refs VALUES (?)", (canonical_json_bytes(value),))
            db.execute(
                "INSERT INTO entries VALUES (?, ?, ?, ?, ?)",
                (journal, str(index), entry, anchor["prefix_sha256"], anchor["prefix_bytes"]),
            )
            db.execute("INSERT INTO forks VALUES (?)", (canonical_json_bytes(anchor),))
            db.execute(
                "INSERT INTO comparisons VALUES (?, ?, ?, ?, ?)",
                (journal, identity, left, right, "matching_fixity"),
            )
        db.executemany(
            "INSERT INTO fixities VALUES (?, ?, ?)",
            ((left, "42", "a" * 64), (right, "42", "a" * 64)),
        )
        for index in range(unrelated):
            identity = prefix + "unrelated-" + str(index)
            db.execute(
                "INSERT INTO assertions VALUES (?, ?, ?, ?, ?, ?, NULL, ?)",
                ("other-journal", identity, identity, "state", "0", 1, b"{}"),
            )
            widths.append(len(identity.encode()))
        assert corpus.advance_semantics().complete
    for phase in ("references", "forks"):
        expected.extend((phase, (index + 1,)) for index in range(count))
    expected.extend(
        ("comparisons", (journal, prefix + str(index)))
        for index in sorted(range(count), key=lambda index: prefix + str(index))
    )
    return expected, max(widths), 5 * count + unrelated + 2


class PhysicalCursor(Cursor):
    def __init__(self, actual, observer, query):
        super().__init__(actual, observer)
        self.query = query

    def fetchone(self):
        row = self.cursor.fetchone()
        if row is not None:
            self.observer.metrics["returned_rows"] += 1
            self.observer.metrics["returned_octets"] += octets(row)
            if self.query.startswith("SELECT substr("):
                self.observer.metrics["projected_payload_octets"] += len(row[0])
            elif self.query.startswith("SELECT left_object,"):
                self.observer.metrics["projected_payload_octets"] += octets(row)
        return row


class PhysicalConnection(Connection):
    def execute(self, query, parameters=()):
        self.observer.metrics["sql_statements"] += 1
        self.observer.metrics["parameter_octets"] += octets(parameters)
        assert "assertion_payloads" not in query
        actual = self.db.execute(query, parameters)
        if query.startswith(("INSERT", "UPDATE", "DELETE")) and actual.rowcount > 0:
            self.observer.metrics["written_rows"] += actual.rowcount
            self.observer.metrics["written_parameter_octets"] += octets(parameters)
        return PhysicalCursor(actual, self.observer, query)


class PhysicalObserver(Observer):
    def __init__(self):
        super().__init__()
        self.body = Observer()
        self.checked = []

    @contextmanager
    def observe(self, monkeypatch, source, workspace):
        db = workspace._db
        check, canonical = (
            native_owner.CanonicalCorpusValidator._check_dependency,
            native_owner.canonical_document,
        )

        def stepped():
            self.metrics["sqlite_vm_instructions"] += 1
            return 0

        def decoded(value):
            self.metrics["json_decodes"] += 1
            self.body.metrics["json_decodes"] += 1
            self.metrics["dependency_decoded_octets"] += octets(value)
            return json.loads(value)

        def encoded(value):
            result = canonical(value)
            self.metrics["dependency_encoded_octets"] += len(result)
            return result

        def checked(corpus, pending, size):
            phase = corpus._dependency_phase
            result = check(corpus, pending, size)
            self.checked.append((phase, tuple(pending[:-1])))
            self.metrics["completed_dependency_checks"] += 1
            return result

        def native(corpus, **budgets):
            self.metrics["native_calls"] += 1
            before = corpus._dependency_checked
            actual = corpus._db
            corpus._db = PhysicalConnection(actual, self.body)
            try:
                result = NATIVE(corpus, **budgets)
            finally:
                corpus._db = actual
            delta = result.checked_obligations - before
            self.body.metrics["processed_rows"] = delta
            assert delta == self.metrics["completed_dependency_checks"]
            assert result.admitted_octets == self.body.metrics["projected_payload_octets"]
            return result

        def authorized(action, table, column, *_rest):
            if action == native_owner.sqlite3.SQLITE_READ and table not in {
                "corpus_workspace",
                "refs",
                "forks",
                "comparisons",
                "assertions",
                "entries",
                "fixities",
            }:
                raise AssertionError("dependency validation read unrelated source data")
            return native_owner.sqlite3.SQLITE_OK

        db.set_progress_handler(stepped, 1)
        db.set_authorizer(authorized)
        try:
            with super().observe(monkeypatch, source), monkeypatch.context() as patch:
                connection = PhysicalConnection(db, self)
                patch.setattr(workspace, "_db", connection)
                patch.setattr(workspace._corpus, "_db", connection)
                patch.setattr(CanonicalCorpusValidator, "advance_dependencies", native)
                patch.setattr(CanonicalCorpusValidator, "_check_dependency", checked)
                patch.setattr(native_owner, "json", SimpleNamespace(loads=decoded))
                patch.setattr(native_owner, "canonical_document", encoded)
                with source_reads(monkeypatch) as frames:
                    yield
                assert frames == []
        finally:
            db.set_progress_handler(None, 0)
            db.set_authorizer(None)
        assert self.live == 0 and self.body.live == 0

    def sample(self, source, *, rows, byte_budget, retained_rows, metadata_octets, counter_octets):
        native_metrics = self.body.metrics.copy()
        native = record(
            growth_binding(NATIVE),
            {"row_budget": rows, "byte_budget": byte_budget},
            native_metrics,
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
        for metric in ("processed_rows", "projected_payload_octets"):
            metrics[metric] = native_metrics[metric]
        return record(
            growth_binding(ROOT),
            {
                **owner_dimensions,
                "row_budget": rows,
                "byte_budget": byte_budget,
                "native_calls": self.metrics["native_calls"],
                "checked_obligations": self.metrics["completed_dependency_checks"],
                "metadata_octets": metadata_octets,
            },
            metrics,
            {"native": [native], "transaction": [transaction]},
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
    reject=False,
):
    scalars = workspace._db.execute(
        "SELECT worker_epoch,version,sources,selections FROM corpus_workspace"
    ).fetchone()
    cookie = workspace._db.execute("PRAGMA schema_version").fetchone()[0]
    counter_octets = max(len(value) for value in (*scalars, str(int(scalars[1]) + 1), str(cookie)))
    observed = PhysicalObserver()
    with observed.observe(monkeypatch, source, workspace):
        if reject:
            with pytest.raises(JournalValidationCapacity):
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


@pytest.mark.growth_witness(
    "native-retained-corpus-dependency-quantum/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus_workspace.RetainedCorpusValidator.advance_dependencies",
)
def test_retained_dependencies_compose_physical_work_and_confirmed_progress(
    tmp_path, catalog, monkeypatch, record_property
):
    samples = []
    for count, rows, byte_budget, width, unrelated in (
        (0, 1, 4096, 0, 0),
        (1, 1, 4096, 0, 0),
        (17, 7, 4096, 0, 129),
        (129, 17, 4096, 0, 129),
        (17, 1, 4096, 129, 129),
        (129, 64, 16384, 129, 129),
    ):
        path = tmp_path / str(len(samples))
        path.mkdir()
        source = EmptySource(catalog)
        workspace = open_workspace(path, source, create=True)
        try:
            expected, metadata_octets, retained_rows = populate(workspace, count, unrelated, width)
            metadata_octets = max(metadata_octets, len(str(byte_budget)))
            if count == 1:
                before = (path / "corpus.sqlite3").read_bytes()
                _, rejected, observed = measure(
                    workspace,
                    source,
                    monkeypatch,
                    rows=1,
                    byte_budget=1,
                    retained_rows=retained_rows,
                    metadata_octets=metadata_octets,
                    reject=True,
                )
                assert (path / "corpus.sqlite3").read_bytes() == before
                assert observed.checked == []
                assert rejected["metrics"]["algorithm_sql_statements"] == 1
                assert rejected["metrics"]["projected_payload_octets"] == 0
                assert rejected["metrics"]["completed_dependency_checks"] == 0
                samples.append(rejected)
            actual, native_sql, restart = [], 0, False
            for _ in range(3 * count + 5):
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
                actual.extend(observed.checked)
                native_sql += sample["metrics"]["algorithm_sql_statements"]
                if count >= 17 and actual and not restart:
                    workspace.close()
                    workspace = open_workspace(path, source, epoch=1)
                    restart = True
                if progress.complete:
                    break
            else:
                pytest.fail("retained dependency walk did not terminate")
            assert actual == expected
            assert progress.checked_obligations == 3 * count
            assert native_sql == 9 * count + 3
            progress, sample, observed = measure(
                workspace,
                source,
                monkeypatch,
                rows=1,
                byte_budget=byte_budget,
                retained_rows=retained_rows,
                metadata_octets=metadata_octets,
            )
            assert progress.complete and observed.checked == []
            assert sample["metrics"]["algorithm_sql_statements"] == 0
            samples.append(sample)
        finally:
            workspace.close()
    record_property("growth_samples", samples)
