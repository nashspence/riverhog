from __future__ import annotations

import hashlib
import os
import sqlite3
from contextlib import closing, contextmanager
from dataclasses import asdict

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core import canonical_discovery_archive_work as directory
from riverhog_core.canonical_discovery_archive_source import ArchiveJournalSource
from riverhog_growth_contracts import (
    evaluate_bound,
    growth_binding,
    project_bindings,
    validate_execution_samples,
)

from tests.support.catalog_work import octets
from tests.support.provenance_archive import archive_reader_fixture, discovery_archive_admission
from tests.unit.test_canonical_discovery_archive_work import drive, open_work, root_for
from tests.unit.test_canonical_discovery_metadata_growth import Recorder, case_for
from tests.unit.test_canonical_discovery_selection import _observe

_SPECIFICATIONS = (
    (1, 4097, 1),
    (17, 4097, 17),
    (1025, 4097, 1025),
    (1, 65537, 17),
)
_OPERATIONS = {
    "header": directory.archive_journal_header,
    "segment-at": directory.archive_journal_segment_at,
    "__init__": ArchiveJournalSource.__init__,
    "verify_current": ArchiveJournalSource.verify_current,
    "read_window": ArchiveJournalSource.read_window,
    "read_exact": ArchiveJournalSource.read_exact,
}


@pytest.fixture(scope="module")
def templates(tmp_path_factory):
    root = tmp_path_factory.mktemp("archive-source-originals")
    result = {}
    for segments, size, retained in _SPECIFICATIONS:
        raw = b"x" * size
        ends = tuple(size * ordinal // segments for ordinal in range(1, segments))
        reader, objects, journal_id, _ = archive_reader_fixture(raw, segment_ends=ends)
        admission = discovery_archive_admission(root_for(objects))
        db, key = open_work(":memory:", admission)
        try:
            drive(db, key, admission, reader)
            path = root / f"{segments}-{size}-{retained}.sqlite"
            with closing(sqlite3.connect(path)) as saved:
                db.backup(saved)
            result[(segments, size, retained)] = (
                path,
                admission,
                reader,
                journal_id,
                raw,
            )
        finally:
            db.close()
    return result


def seed(case, path, retained):
    names = (
        "discovery_archive_tasks",
        "discovery_archive_binding_pages",
        "discovery_archive_journals",
        "discovery_archive_journal_segments",
    )
    with closing(sqlite3.connect(path)) as original, case.metadata.transaction() as (db, key):
        for table in names:
            columns = [row[1] for row in original.execute(f"PRAGMA table_info({table})")]
            db.execute(f"DELETE FROM {table}").close()
            for row in original.execute(f"SELECT * FROM {table}"):
                values = list(row)
                for name, value in (("build_id", key.build_id), ("worker_id", key.worker_id)):
                    if name in columns:
                        values[columns.index(name)] = value
                db.execute(
                    f"INSERT INTO {table} VALUES({','.join('?' for _ in values)})", values
                ).close()
        row = db.execute("SELECT * FROM discovery_archive_journal_segments LIMIT 1").fetchone()
        for index in range(retained):
            other = list(row)
            other[0] = "unrelated-" + str(index)
            db.execute(
                "INSERT INTO discovery_archive_journal_segments VALUES(?,?,?,?,?,?,?)", other
            ).close()


def dimensions(case, source, requested, windows, payload):
    db = case.metadata._db
    retained, width = (
        0,
        octets(
            (
                tuple(asdict(case.metadata.job).values()),
                case.admission.root_json,
                case.admission.binding_json,
                case.metadata.key,
                source.identity,
            )
        ),
    )
    for (table,) in db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'discovery_%'"
    ).fetchall():
        for row in db.execute(f"SELECT * FROM {table}"):
            retained += 1
            width = max(width, octets(row))
    from riverhog_core.catalog_db import Base
    from sqlalchemy import func, select

    with case.engine.connect() as catalog:
        catalog_rows = sum(
            catalog.scalar(select(func.count()).select_from(table))
            for table in Base.metadata.tables.values()
        )
    return {
        "retained_core_rows": retained,
        "retained_catalog_rows": catalog_rows,
        "metadata_octets": width,
        "path_octets": len(os.fsencode(case.path / "discovery.sqlite3")),
        "requested_octets": requested,
        "payload_octets": payload,
        "segment_windows": windows,
    }


@contextmanager
def observe(case, source, monkeypatch, cold):
    db = case.metadata._db
    storage = Recorder()
    db.observer = storage
    db.set_trace_callback(storage.trace)
    with storage.observe(case, cold=cold), _observe(db, monkeypatch, source.reader) as core:
        yield storage, core
    for sql, parameters in storage.statements:
        if sql.lstrip().upper().startswith("SELECT"):
            plan = " ".join(
                str(row[3]).upper() for row in db.execute("EXPLAIN QUERY PLAN " + sql, parameters)
            )
            assert "SCAN " not in plan and "TEMP B-TREE" not in plan, plan


def sample(mode, dims, storage, core, source, returned):
    binding = growth_binding(_OPERATIONS[mode])
    projection = project_bindings((binding,))
    metrics = dict(storage.metrics)
    metrics["core_vm_instructions"] = core.metrics["sqlite_vm_instructions"]
    for name in (
        "encoded_octets",
        "decoded_octets",
        "hashed_octets",
        "sha256_block_octets",
        "written_octets",
        "source_reads",
        "source_read_octets",
    ):
        metrics[name] = core.metrics[name]
    for name, target in {
        "sql_statements": "catalog_sql_statements",
        "driver_statements": "catalog_driver_statements",
        "returned_rows": "catalog_returned_rows",
        "returned_metadata_octets": "catalog_returned_octets",
        "bound_parameter_octets": "catalog_parameter_octets",
        "sqlite_vm_instructions": "catalog_vm_instructions",
        "changed_rows": "catalog_changed_rows",
        "uuid_values": "uuid_values",
    }.items():
        metrics[target] = storage.catalog.metrics[name]
    metrics["returned_source_octets"] = returned
    metrics["retained_source_octets"] = 0 if source._page is None else len(source._page[1])
    if mode in ("header", "segment-at"):
        metrics.update(
            sql_statements=metrics["core_sql_statements"],
            returned_rows=metrics["core_returned_rows"],
            returned_octets=metrics["core_returned_octets"],
            parameter_octets=metrics["core_parameter_octets"],
            sqlite_vm_instructions=metrics["core_vm_instructions"],
        )
        dims = {
            "registered_inputs": dims["retained_core_rows"],
            "metadata_octets": dims["metadata_octets"],
        }
    result = {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical_json_bytes(projection["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": dims,
        "metrics": {
            bound.metric: metrics.get(bound.metric, 0) for bound in binding.declaration.bounds
        },
        "delegates": {},
    }
    try:
        validate_execution_samples(projection, binding.declaration.id, [result])
    except ValueError as error:
        raise AssertionError(
            [
                {
                    "metric": bound.metric,
                    "actual": result["metrics"][bound.metric],
                    "bound": evaluate_bound(binding, bound.metric, dims),
                }
                for bound in binding.declaration.bounds
                if result["metrics"][bound.metric] > evaluate_bound(binding, bound.metric, dims)
            ]
        ) from error
    return result


def exercise(tmp_path, templates, monkeypatch, record_property, mode):
    samples = []
    for specification in _SPECIFICATIONS:
        segments, size, retained = specification
        path, admission, reader, journal_id, raw = templates[specification]
        with case_for(tmp_path / f"{segments}-{size}-{retained}", admission, retained, 17) as case:
            initial = Recorder()
            with initial.connect_only():
                case.metadata.open()
            seed(case, path, retained)
            source = ArchiveJournalSource(case.metadata, reader, journal_id)
            payload = max(
                right - left
                for left, right in zip(
                    (0, *(size * i // segments for i in range(1, segments))),
                    (*(size * i // segments for i in range(1, segments)), size),
                    strict=True,
                )
            )
            variants = (
                ("zero", "cold", "warm")
                if mode in ("read_window", "read_exact")
                else ("cold", "warm", "stale")
                if mode == "verify_current"
                else ("cold", "warm")
            )
            for variant in variants:
                requested = 0 if variant == "zero" else size
                windows = 0 if requested == 0 else segments
                dims = dimensions(case, source, requested, windows, payload)
                if variant != "warm":
                    source._page = None
                if variant == "stale":
                    case.metadata.custody._held = False
                try:
                    returned = 0
                    with observe(case, source, monkeypatch, cold=variant == "cold") as (
                        storage,
                        core,
                    ):
                        db, key = case.metadata._db, case.metadata.key
                        if mode == "__init__":
                            source = ArchiveJournalSource(case.metadata, reader, journal_id)
                        elif mode == "verify_current":
                            assert source.verify_current() is (variant != "stale")
                        elif mode == "header":
                            assert (
                                directory.archive_journal_header(db, key, journal_id)
                                == source.identity
                            )
                        elif mode == "segment-at":
                            selected = directory.archive_journal_segment_at(
                                db, key, journal_id, size // 2
                            )
                            assert (
                                selected.journal_offset
                                <= size // 2
                                < (selected.journal_offset + selected.payload.bytes)
                            )
                        else:
                            result = getattr(source, mode)(0, requested)
                            expected = raw if mode == "read_exact" else raw[: size // segments]
                            assert result == (b"" if variant == "zero" else expected)
                            returned = len(result)
                    samples.append(sample(mode, dims, storage, core, source, returned))
                finally:
                    case.metadata.custody._held = True
    record_property("growth_samples", samples)
    record_property(
        "archive_source_conditions",
        "Real root-authenticated original archive metadata and payload verification, "
        "actual retained Core/catalog/kernel custody, full SQL rows/parameters, codecs, "
        "SHA/block bytes, VM and filesystem fences. Independent segment counts1/17/1025 "
        "at fixed4097bytes, then65537bytes, with independent unrelated retained rows. "
        "Cold, warm and zero reads; stale currentness rejects. Every SELECT uses an "
        "indexed plan. Range bytes retain a linear bound plus boundary segments; this "
        "is raw-source evidence, not Native semantic or publication acceptance.",
    )


@pytest.mark.growth_witness(
    "canonical-discovery-archive-journal-header/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_archive_work.archive_journal_header",
)
def test_header_work(tmp_path, templates, monkeypatch, record_property):
    exercise(tmp_path, templates, monkeypatch, record_property, "header")


@pytest.mark.growth_witness(
    "canonical-discovery-archive-journal-segment-at/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_archive_work.archive_journal_segment_at",
)
def test_segment_at_work(tmp_path, templates, monkeypatch, record_property):
    exercise(tmp_path, templates, monkeypatch, record_property, "segment-at")


@pytest.mark.growth_witness(
    "canonical-discovery-archive-source-construction/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_archive_source.ArchiveJournalSource.__init__",
)
def test_construction_work(tmp_path, templates, monkeypatch, record_property):
    exercise(tmp_path, templates, monkeypatch, record_property, "__init__")


@pytest.mark.growth_witness(
    "canonical-discovery-archive-source-current/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_archive_source.ArchiveJournalSource.verify_current",
)
def test_current_work(tmp_path, templates, monkeypatch, record_property):
    exercise(tmp_path, templates, monkeypatch, record_property, "verify_current")


@pytest.mark.growth_witness(
    "canonical-discovery-archive-source-window/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_archive_source.ArchiveJournalSource.read_window",
)
def test_window_work(tmp_path, templates, monkeypatch, record_property):
    exercise(tmp_path, templates, monkeypatch, record_property, "read_window")


@pytest.mark.growth_witness(
    "canonical-discovery-archive-source-exact/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_archive_source.ArchiveJournalSource.read_exact",
)
def test_exact_work(tmp_path, templates, monkeypatch, record_property):
    exercise(tmp_path, templates, monkeypatch, record_property, "read_exact")
