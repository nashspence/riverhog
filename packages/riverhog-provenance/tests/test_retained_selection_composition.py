from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace

import pytest
import riverhog_provenance.corpus as native_owner
import riverhog_provenance.corpus_workspace as owner
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_growth_contracts import growth_binding
from riverhog_provenance import (
    CanonicalCorpusValidator,
    IncompleteSourceError,
    RetainedCorpusValidator,
    validate_journal,
)
from riverhog_provenance_contracts import canonical_document
from test_corpus_source_growth import source_reads
from test_journal_result import history
from test_retained_corpus_growth import Observer
from test_retained_corpus_workspace import OwnedCorpusSource, open_workspace
from test_retained_point_composition import PointConnection
from test_source_anchor_growth import record

ROOT = RetainedCorpusValidator.select_source
NATIVE = CanonicalCorpusValidator.select_source
TRANSACTION = RetainedCorpusValidator._operation


class SelectionObserver(Observer):
    def __init__(self):
        super().__init__()
        self.body, self.registration = Observer(), Observer()
        self.in_registration = False

    @contextmanager
    def observe(self, monkeypatch, source, workspace):
        db = workspace._db
        register = workspace._register
        admit = source.admit_selection
        canonical, encoded = native_owner.canonical_document, owner.canonical_json_bytes

        def stepped():
            self.metrics["sqlite_vm_instructions"] += 1
            return 0

        def native(corpus, anchor):
            self.metrics["native_calls"] += 1
            actual = corpus._db
            corpus._db = PointConnection(actual, self.body)
            try:
                return NATIVE(corpus, anchor)
            finally:
                corpus._db = actual

        def registered(anchor, *, selection):
            before = self.metrics.copy()
            self.in_registration = True
            try:
                return register(anchor, selection=selection)
            finally:
                self.in_registration = False
                for metric in (
                    "sql_statements",
                    "returned_rows",
                    "returned_octets",
                    "parameter_octets",
                ):
                    self.registration.metrics[metric] += self.metrics[metric] - before[metric]
                self.metrics["registration_statements"] = self.registration.metrics[
                    "sql_statements"
                ]

        def admitted(anchor):
            self.metrics["manifest_checks"] += 1
            return admit(anchor)

        def native_encoded(value):
            result = canonical(value)
            self.metrics["selection_encoded_octets"] += len(result)
            return result

        def parent_encoded(value):
            result = encoded(value)
            if self.in_registration:
                self.metrics["selection_encoded_octets"] += len(result)
            return result

        def authorized(action, table, _column, *_rest):
            if action == native_owner.sqlite3.SQLITE_READ and table not in {
                "corpus_workspace",
                "entries",
                "selected_prefixes",
                "corpus_selected_inputs",
            }:
                raise AssertionError("selection scanned unrelated retained input")
            return native_owner.sqlite3.SQLITE_OK

        db.set_progress_handler(stepped, 1)
        db.set_authorizer(authorized)
        try:
            with super().observe(monkeypatch, source), monkeypatch.context() as patch:
                connection = PointConnection(db, self)
                patch.setattr(workspace, "_db", connection)
                patch.setattr(workspace._corpus, "_db", connection)
                patch.setattr(workspace, "_register", registered)
                patch.setattr(CanonicalCorpusValidator, "select_source", native)
                patch.setattr(source, "admit_selection", admitted)
                patch.setattr(native_owner, "canonical_document", native_encoded)
                patch.setattr(owner, "canonical_json_bytes", parent_encoded)
                with source_reads(monkeypatch) as frames:
                    try:
                        yield
                    finally:
                        assert frames == []
        finally:
            db.set_progress_handler(None, 0)
            db.set_authorizer(None)
        assert self.live == self.body.live == 0

    def sample(self, source, dimensions):
        native = (
            []
            if not self.metrics["native_calls"]
            else [
                record(
                    growth_binding(NATIVE),
                    {"selected_prefixes": dimensions["selected_prefixes"]},
                    self.body.metrics,
                )
            ]
        )
        lifecycle = self.metrics.copy()
        for metric in ("sql_statements", "returned_rows", "returned_octets", "parameter_octets"):
            lifecycle[metric] -= self.body.metrics[metric] + self.registration.metrics[metric]
            assert lifecycle[metric] >= 0
        owner_dimensions = {
            "binding_octets": len(source.binding.to_bytes()),
            "checkpoint_octets": self.checkpoint_octets,
            "counter_octets": dimensions["counter_octets"],
            "retained_rows": dimensions["retained_rows"],
        }
        transaction = record(growth_binding(TRANSACTION), owner_dimensions, lifecycle)
        return record(
            growth_binding(ROOT),
            {
                **owner_dimensions,
                "anchor_octets": dimensions["anchor_octets"],
                "selected_prefixes": dimensions["selected_prefixes"],
                "native_calls": self.metrics["native_calls"],
            },
            self.metrics,
            {"native": native, "transaction": [transaction]},
        )


def measure(workspace, source, anchor, monkeypatch, *, expected_count, rejection=None):
    db = workspace._db
    scalars = db.execute(
        "SELECT worker_epoch,version,sources,selections FROM corpus_workspace"
    ).fetchone()
    assert int(scalars[-1]) == expected_count
    assert db.execute("SELECT count(*) FROM selected_prefixes").fetchone()[0] == expected_count
    assert db.execute("SELECT count(*) FROM corpus_selected_inputs").fetchone()[0] == expected_count
    cookie = db.execute("PRAGMA schema_version").fetchone()[0]
    dimensions = {
        "counter_octets": max(
            len(value)
            for value in (
                *scalars,
                str(int(scalars[1]) + 1),
                str(cookie),
                str(source.binding.selection_count),
                str(source.binding.source_count),
                str(expected_count + 1),
            )
        ),
        "selected_prefixes": expected_count,
        "anchor_octets": len(canonical_document(anchor.to_mapping())),
        "retained_rows": sum(
            db.execute("SELECT count(*) FROM " + table).fetchone()[0]
            for table in ("entries", "assertions", "identities")
        ),
    }
    observer = SelectionObserver()
    with observer.observe(monkeypatch, source, workspace):
        if rejection is None:
            ROOT(workspace, anchor)
        else:
            with pytest.raises(rejection):
                ROOT(workspace, anchor)
    return observer.sample(source, dimensions), observer


@pytest.mark.growth_witness(
    "native-retained-corpus-selected-prefix/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus_workspace.RetainedCorpusValidator.select_source",
)
def test_retained_selected_prefixes_compose_exact_membership_unique_counts_and_physical_work(
    tmp_path, journal, catalog, monkeypatch, record_property
):
    samples = []
    for index, (frames, count) in enumerate(((1, 0), (1, 1), (129, 1), (129, 17), (129, 129))):
        path = tmp_path / str(index)
        path.mkdir()
        with validate_journal(history(journal, frames, catalog), catalog=catalog) as summary:
            anchors = [
                HistoryJournalAnchor.from_mapping(summary.anchor_at(position))
                for position in range(count)
            ]
            source = OwnedCorpusSource(summary, catalog, anchors)
            workspace = open_workspace(path, source, create=True)
            try:
                while not workspace.advance_source(summary, frame_budget=17).complete:
                    pass
                head = HistoryJournalAnchor.from_mapping(summary.anchor)
                absent = replace(head, prefix_sha256="0" * 64)
                before = (path / "corpus.sqlite3").read_bytes()
                sample, observed = measure(
                    workspace,
                    source,
                    absent,
                    monkeypatch,
                    expected_count=0,
                    rejection=IncompleteSourceError,
                )
                assert observed.metrics["native_calls"] == 0
                assert sample["metrics"]["manifest_checks"] == 1
                assert (path / "corpus.sqlite3").read_bytes() == before
                samples.append(sample)
                if count < frames:
                    sample, observed = measure(
                        workspace,
                        source,
                        head,
                        monkeypatch,
                        expected_count=0,
                        rejection=IncompleteSourceError,
                    )
                    assert sample["metrics"]["native_calls"] == 0
                    samples.append(sample)
                if count:
                    checks = iter((True, False))
                    before = (path / "corpus.sqlite3").read_bytes()
                    with monkeypatch.context() as patch:
                        patch.setattr(source, "verify_current", lambda checks=checks: next(checks))
                        sample, observed = measure(
                            workspace,
                            source,
                            anchors[0],
                            monkeypatch,
                            expected_count=0,
                            rejection=IncompleteSourceError,
                        )
                    assert sample["metrics"]["source_checks"] == 2
                    assert sample["metrics"]["registration_statements"] == 3
                    assert sample["metrics"]["written_rows"] == 3
                    assert (path / "corpus.sqlite3").read_bytes() == before
                    samples.append(sample)
                completed, cold_statements = 0, 0
                for anchor in reversed(anchors):
                    sample, observed = measure(
                        workspace,
                        source,
                        anchor,
                        monkeypatch,
                        expected_count=completed,
                    )
                    assert observed.body.metrics["sql_statements"] == 2
                    assert sample["metrics"]["registration_statements"] == 3
                    assert sample["metrics"]["written_rows"] == 4
                    cold_statements += sample["metrics"]["sql_statements"]
                    completed += 1
                    samples.append(sample)
                    if completed == 1:
                        workspace.close()
                        workspace = open_workspace(path, source, epoch=1)
                    sample, observed = measure(
                        workspace,
                        source,
                        anchor,
                        monkeypatch,
                        expected_count=completed,
                    )
                    assert observed.body.metrics["sql_statements"] == 2
                    assert sample["metrics"]["registration_statements"] == 1
                    assert sample["metrics"]["written_rows"] == 1
                    samples.append(sample)
                assert cold_statements == 11 * count
                assert workspace._db.execute(
                    "SELECT sources,selections FROM corpus_workspace"
                ).fetchone() == ("1", str(count))
                assert (
                    workspace._db.execute("SELECT count(*) FROM selected_prefixes").fetchone()[0]
                    == count
                )
                if count:
                    checks = iter((True, False))
                    before = (path / "corpus.sqlite3").read_bytes()
                    with monkeypatch.context() as patch:
                        patch.setattr(source, "verify_current", lambda checks=checks: next(checks))
                        sample, observed = measure(
                            workspace,
                            source,
                            anchors[0],
                            monkeypatch,
                            expected_count=count,
                            rejection=IncompleteSourceError,
                        )
                    assert sample["metrics"]["source_checks"] == 2
                    assert observed.metrics["native_calls"] == 1
                    assert (path / "corpus.sqlite3").read_bytes() == before
                    samples.append(sample)
                source.current = False
                sample, observed = measure(
                    workspace,
                    source,
                    head,
                    monkeypatch,
                    expected_count=count,
                    rejection=IncompleteSourceError,
                )
                assert observed.metrics["native_calls"] == 0
                assert sample["metrics"]["manifest_checks"] == 0
                assert sample["metrics"]["sql_statements"] == 0
                samples.append(sample)
            finally:
                workspace.close()
    record_property("growth_samples", samples)
