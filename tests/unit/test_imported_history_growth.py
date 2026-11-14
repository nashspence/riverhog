from __future__ import annotations

import hashlib
import sqlite3
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import asdict
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.services import upload_imported_history as owner
from riverhog_core.services import upload_journal_result as admission_owner
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import (
    CanonicalCorpusValidator,
    JournalValidationSession,
    journal_store,
    validate_journal,
)
from riverhog_provenance import corpus as corpus_owner
from sqlalchemy import func, select

from tests.support.catalog_work import octets
from tests.unit.test_imported_history_finalization import _source_material
from tests.unit.test_upload_journal_result import JOURNAL, Job, Journal, sealed, source_owner
from tests.unit.test_upload_journal_result_admission_growth import (
    CURRENT,
    PREPARE,
    observe,
    seed,
)

ROOT = owner.selected_upload_imported_primary_state
ENDPOINT = CanonicalCorpusValidator.require_source_endpoint
DELIVERY = CanonicalCorpusValidator.selected_delivery_support


def _sample(operation, dimensions, metrics, delegates=None):
    binding = growth_binding(operation)
    projection = project_bindings((binding,))
    sample = {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical_json_bytes(projection["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": dimensions,
        "metrics": {bound.metric: metrics[bound.metric] for bound in binding.declaration.bounds},
        "delegates": delegates or {},
    }
    validate_execution_samples(projection, binding.declaration.id, [sample])
    return sample


class _Cursor:
    def __init__(self, actual, observer, payload):
        self.actual, self.observer, self.payload = actual, observer, payload
        self.closed = False

    def fetchone(self):
        row = self.actual.fetchone()
        if row is not None:
            self.observer.metrics["returned_rows"] += 1
            key = "payload_octets" if self.payload else "metadata_octets"
            self.observer.metrics[key] += octets(row)
        return row

    def close(self):
        self.actual.close()
        self.closed = True
        self.observer.metrics["closed_cursors"] += 1


class _Native:
    def __init__(self, actual):
        self.actual, self.metrics = actual, defaultdict(int)
        self.cursors, self.queries = [], []

    def execute(self, query, parameters=()):
        assert all(cursor.closed for cursor in self.cursors)
        self.metrics["sql_statements"] += 1
        self.metrics["parameter_octets"] += octets(parameters)
        self.queries.append((query, parameters))
        cursor = _Cursor(self.actual.execute(query, parameters), self, "substr(" in query)
        self.cursors.append(cursor)
        return cursor


@contextmanager
def _native(corpus, monkeypatch):
    observed = _Native(corpus._db)
    decode = corpus_owner.json.loads

    def decoded(raw):
        observed.metrics["json_decodes"] += 1
        observed.metrics["decoded_octets"] += octets(raw)
        return decode(raw)

    def tick():
        observed.metrics["sqlite_vm_instructions"] += 1
        return 0

    def authorize(action, table, _column, *_rest):
        if action == sqlite3.SQLITE_READ and table not in {
            "source_extents",
            "entries",
            "assertions",
            "delivery_projections",
        }:
            observed.metrics["full_assertion_reads"] += 1
            raise AssertionError("historical selection read full or unrelated assertion payloads")
        if action in {sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE}:
            raise AssertionError("historical selection mutated its indexed source")
        return sqlite3.SQLITE_OK

    observed.actual.set_progress_handler(tick, 1)
    observed.actual.set_authorizer(authorize)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(corpus, "_db", observed)
            patch.setattr(corpus_owner, "json", SimpleNamespace(loads=decoded))
            yield observed
    finally:
        observed.actual.set_authorizer(None)
        observed.actual.set_progress_handler(None, 0)
    assert all(cursor.closed for cursor in observed.cursors)
    observed.metrics["returned_metadata_octets"] = observed.metrics["metadata_octets"]
    for query, parameters in observed.queries:
        plan = " ".join(
            row[3] for row in observed.actual.execute("EXPLAIN QUERY PLAN " + query, parameters)
        )
        assert "SCAN " not in plan and "TEMP B-TREE" not in plan, plan


@contextmanager
def _catalog_guard(connection, metrics):
    admitted = {
        Journal.__tablename__: set(admission_owner._SOURCE_FIELDS),
        Job.__tablename__: set(admission_owner.jobs.ValidationJob.__dataclass_fields__),
    }

    def authorize(action, table, column, *_rest):
        if action == sqlite3.SQLITE_READ and column not in admitted.get(table, set()):
            metrics["catalog_payload_reads"] += 1
            raise AssertionError("import admission read payload or unrelated catalog fields")
        if action in {sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE}:
            raise AssertionError("import admission attempted a catalog mutation")
        return sqlite3.SQLITE_OK

    connection.set_authorizer(authorize)
    try:
        yield
    finally:
        connection.set_authorizer(None)


def _measure(session, corpus, source, catalog, monkeypatch):
    dimensions = {
        "catalog_journals": session.scalar(select(func.count()).select_from(Journal)),
        "catalog_jobs": session.scalar(select(func.count()).select_from(Job)),
    }
    retained = {
        "retained_sources": corpus._db.execute("SELECT count(*) FROM source_extents").fetchone()[0],
        "retained_frames": corpus._db.execute("SELECT count(*) FROM entries").fetchone()[0],
        "retained_assertions": corpus._db.execute("SELECT count(*) FROM assertions").fetchone()[0],
    }
    actual = session.connection().connection.driver_connection
    changes = actual.total_changes
    metrics, children, metadata = defaultdict(int), {}, []

    def prepared(*args, **kwargs):
        with observe(session, PREPARE, monkeypatch) as (measured, _queries):
            admitted = PREPARE(*args, **kwargs)
            measured["detached_controls"] += int(
                isinstance(admitted, admission_owner.SealedJournalResultAdmission)
            )
        source_width = octets(tuple(value for _name, value in admitted._source))
        job_width = octets(tuple(asdict(admitted.job).values()))
        dimensions["catalog_row_octets"] = max(source_width, job_width)
        controls = source_width + job_width + octets((1, JOURNAL, catalog.checkpoint_sha256()))
        metadata.append(
            _sample(
                PREPARE,
                {
                    "catalog_journals": dimensions["catalog_journals"],
                    "catalog_jobs": dimensions["catalog_jobs"],
                    "control_octets": controls,
                },
                measured,
            )
        )
        return admitted

    def current(admitted, selected_session):
        with observe(session, CURRENT, monkeypatch) as (measured, _queries):
            result = CURRENT(admitted, selected_session)
        metadata.append(_sample(CURRENT, metadata[0]["dimensions"], measured))
        return result

    def endpoint(selected_corpus, anchor):
        with _native(selected_corpus, monkeypatch) as measured:
            ENDPOINT(selected_corpus, anchor)
        children["endpoint"] = [
            _sample(
                ENDPOINT,
                {
                    "retained_sources": retained["retained_sources"],
                    "retained_frames": retained["retained_frames"],
                    "anchor_octets": len(canonical_json_bytes(anchor.to_mapping())),
                },
                measured.metrics,
            )
        ]

    def delivery(selected_corpus, **kwargs):
        with _native(selected_corpus, monkeypatch) as measured:
            result = DELIVERY(selected_corpus, **kwargs)
        anchor = HistoryJournalAnchor.from_mapping(kwargs["binding"]["journal"])
        children["delivery"] = [
            _sample(
                DELIVERY,
                {
                    "retained_assertions": retained["retained_assertions"],
                    "retained_frames": retained["retained_frames"],
                    "anchor_octets": len(canonical_json_bytes(anchor.to_mapping())),
                    "selected_octets": measured.metrics["payload_octets"],
                },
                measured.metrics,
            )
        ]
        return result

    def forbidden_frame(*_args, **_kwargs):
        metrics["source_payload_octets"] += 1
        pytest.fail("imported primary selection attempted to load original source bytes")

    def forbidden_advance(*_args, **_kwargs):
        metrics["native_validation_advances"] += 1
        pytest.fail("imported primary selection replayed Native validation")

    def forbidden_open(*_args, **_kwargs):
        metrics["native_result_openings"] += 1
        pytest.fail("imported primary selection acquired a Native reader or workspace lease")

    with _catalog_guard(actual, metrics), monkeypatch.context() as patch:
        patch.setattr(owner, "prepare_upload_journal_result", prepared)
        patch.setattr(admission_owner.SealedJournalResultAdmission, "verify_current", current)
        patch.setattr(CanonicalCorpusValidator, "require_source_endpoint", endpoint)
        patch.setattr(CanonicalCorpusValidator, "selected_delivery_support", delivery)
        patch.setattr(journal_store._Frames, "__getitem__", forbidden_frame)
        patch.setattr(journal_store._Frames, "__iter__", forbidden_frame)
        patch.setattr(source_owner, "_iter_upload_journal_chunks", forbidden_frame)
        patch.setattr(JournalValidationSession, "advance", forbidden_advance)
        patch.setattr(admission_owner, "open_retained_journal_result", forbidden_open)
        patch.setattr(admission_owner, "lease_journal_validation_workspace", forbidden_open)
        result = ROOT(session, 1, source, catalog=catalog, corpus=corpus)
    metrics["catalog_changed_rows"] = actual.total_changes - changes
    for child in metadata:
        for destination, origin in (
            ("catalog_statements", "sql_statements"),
            ("catalog_returned_rows", "returned_rows"),
            ("catalog_metadata_octets", "returned_octets"),
            ("catalog_parameter_octets", "parameter_octets"),
            ("catalog_vm_instructions", "sqlite_vm_instructions"),
            ("catalog_open_cursors", "open_cursors"),
        ):
            metrics[destination] += child["metrics"][origin]
    for name, values in children.items():
        dimensions.update(
            (name + "_" + key, value) for key, value in values[0]["dimensions"].items()
        )
        metrics.update((name + "_" + key, value) for key, value in values[0]["metrics"].items())
    return result, _sample(ROOT, dimensions, metrics, children), metadata


@pytest.mark.growth_witness(
    "riverhog-terminal-imported-primary-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_imported_history.selected_upload_imported_primary_state",
)
def test_current_imported_primary_composes_actual_catalog_and_historical_point_work(
    tmp_path, monkeypatch, record_property, journal_constructions
):
    material = _source_material(4096, journal_id=JOURNAL)
    samples, metadata_samples = [], []
    with sealed(tmp_path, raw=material.raw, journal_constructions=journal_constructions) as (
        _engine,
        factory,
        _root,
        _job,
        raw,
    ):
        for retained in (0, 17):
            if retained:
                seed(factory, retained)
            with (
                factory() as session,
                session.begin(),
                validate_journal(raw, catalog=material.catalog) as complete,
                CanonicalCorpusValidator() as corpus,
            ):
                corpus.add(complete)
                actual, measured, metadata = _measure(
                    session, corpus, material.descriptor, material.catalog, monkeypatch
                )
                assert actual == material.imported.input_state
                samples.append(measured)
                metadata_samples.extend(metadata)
    record_property("growth_samples", samples)
    record_property("catalog_admission_samples", metadata_samples)
