from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.services import collection_uploads as source_owner
from riverhog_core.services import upload_journal_replay as replay
from riverhog_core.services.upload_journal_source import UploadJournalValidationSource
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import replay_binding as codec_owner
from riverhog_provenance_contracts import ContractCatalog
from sqlalchemy import create_engine, event, update

from tests.support.catalog_work import octets
from tests.unit.test_journal_capture import retained
from tests.unit.test_journal_replay_binding import Scan, pack, ready
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_staged_journal_range import JOURNAL, Journal


def source_metadata_octets(work):
    return octets(
        (
            work.collection_id,
            work.journal_id,
            work.attempt_id,
            work.directory_id,
            work.source_bytes,
            work.source_sha256,
            work.phase,
            work.version,
            work.capture_failure,
            work.cursor.byte_offset,
            work.cursor.frame_start,
            work.cursor.frame_count,
        )
    )


def sample(binding, digest, dimensions, metrics, delegates):
    return {
        "declaration": binding.declaration.id,
        "declaration_sha256": digest,
        "dimensions": dimensions,
        "metrics": metrics,
        "delegates": delegates,
    }


@contextmanager
def selected_source(root, count, position, payload, catalog, *, wide_version=False):
    raw = source(payload, journal_id=JOURNAL)
    with retained(root, raw) as (engine, factory):
        work = ready(factory)
        identities = sorted(
            [
                JOURNAL,
                *(
                    f"urn:uuid:{number:08x}-0000-4000-8000-000000000000"
                    for number in range(1, count)
                ),
            ]
        )
        selected = identities[position]
        with factory() as session, session.begin():
            journal = session.get(Journal, (1, JOURNAL))
            scan = session.get(Scan, (1, JOURNAL))
            journal_values = {
                column.key: getattr(journal, column.key) for column in Journal.__table__.columns
            }
            scan_values = {
                column.key: getattr(scan, column.key) for column in Scan.__table__.columns
            }
            scans = []
            for identity in identities:
                if identity != JOURNAL:
                    session.add(Journal(**{**journal_values, "journal_id": identity}))
                    scans.append(Scan(**{**scan_values, "journal_id": identity}))
            session.flush()
            session.add_all(scans)
        work = replace(work, journal_id=selected)
        if wide_version:
            work = replace(work, version=2**62)
            with factory() as session, session.begin():
                session.execute(
                    update(Journal)
                    .where(
                        Journal.collection_id == work.collection_id,
                        Journal.journal_id == work.journal_id,
                    )
                    .values(capture_version=work.version)
                ).close()
        current = UploadJournalValidationSource(factory, work, catalog=catalog)
        yield engine, factory, work, current


def metadata_snapshot(factory, work):
    with factory() as session:
        journal = session.get(Journal, (work.collection_id, work.journal_id))
        scan = session.get(Scan, (work.collection_id, work.journal_id))
        return (
            tuple(getattr(journal, column.key) for column in Journal.__table__.columns),
            tuple(getattr(scan, column.key) for column in Scan.__table__.columns),
        )


def observe_current(
    engine,
    factory,
    work,
    current,
    mode,
    count,
    dimensions,
    binding,
    probe_binding,
    projection,
    digests,
):
    before = metadata_snapshot(factory, work)
    metrics = {bound.metric: 0 for bound in binding.declaration.bounds}
    children, statements, results = [], [], []
    with engine.connect() as connection:
        native = connection.connection.driver_connection
        assert isinstance(native, sqlite3.Connection)
    changes = native.total_changes
    actual_probe, actual_snapshot = replay._probe, replay.read_snapshot
    actual_loads, actual_encode = codec_owner.json.loads, codec_owner.canonical_json_bytes

    class ProbeResult:
        def __init__(self, actual):
            self.actual = actual

        def __enter__(self):
            self.actual.__enter__()
            return self

        def __exit__(self, *args):
            try:
                return self.actual.__exit__(*args)
            finally:
                assert self.actual.closed

        def one_or_none(self):
            row = self.actual.one_or_none()
            metrics["returned_rows"] += int(row is not None)
            return row

    @contextmanager
    def observed_snapshot(selected_factory):
        with actual_snapshot(selected_factory) as session:
            execute = session.execute

            def observed_execute(*args, **kwargs):
                result = ProbeResult(execute(*args, **kwargs))
                results.append(result)
                return result

            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(session, "execute", observed_execute)
                yield session

    def sql(_connection, _cursor, statement, parameters, _context, many):
        assert not many and _connection.connection.driver_connection is native
        metrics["sql_statements"] += 1
        metrics["bound_parameter_octets"] += octets(parameters)
        statements.append((statement, parameters))

    def tick():
        metrics["sqlite_vm_instructions"] += 1
        return 0

    def authorize(action, _table, column, _database, _trigger):
        if action == sqlite3.SQLITE_READ and column == "content":
            raise AssertionError("source currentness requested payload")
        return sqlite3.SQLITE_OK

    def loads(value, *args, **kwargs):
        metrics["json_decodes"] += 1
        metrics["json_decoded_octets"] += octets(value)
        return actual_loads(value, *args, **kwargs)

    def encode(*args, **kwargs):
        metrics["json_encodes"] += 1
        value = actual_encode(*args, **kwargs)
        metrics["json_encoded_octets"] += len(value)
        return value

    def probe(*args, **kwargs):
        result = actual_probe(*args, **kwargs)
        metrics["binding_octets"] = octets(result[1])
        child_metrics = {
            bound.metric: metrics[bound.metric] for bound in probe_binding.declaration.bounds
        }
        children.append(
            sample(
                probe_binding,
                digests[probe_binding.declaration.id],
                {"retained_journals": count},
                child_metrics,
                {},
            )
        )
        return result

    event.listen(engine, "before_cursor_execute", sql)
    native.set_authorizer(authorize)
    native.set_progress_handler(tick, 1)
    try:
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(replay, "read_snapshot", observed_snapshot)
            patch.setattr(replay, "_probe", probe)
            patch.setattr(codec_owner, "json", SimpleNamespace(loads=loads))
            patch.setattr(codec_owner, "canonical_json_bytes", encode)
            if mode in {"changed", "malformed", "noncanonical"}:
                with pytest.raises(RuntimeError, match="retained"):
                    current.verify_current()
            else:
                assert current.verify_current() == (mode != "stale")
    finally:
        native.set_progress_handler(None, 0)
        native.set_authorizer(None)
        event.remove(engine, "before_cursor_execute", sql)
    metrics["changed_rows"] = native.total_changes - changes
    assert metrics["changed_rows"] == 0 and engine.pool.checkedout() == 0
    assert len(children) == len(results) == 1 and all(item.actual.closed for item in results)
    assert metadata_snapshot(factory, work) == before
    for statement, parameters in statements:
        with engine.connect() as connection:
            with connection.exec_driver_sql("EXPLAIN QUERY PLAN " + statement, parameters) as rows:
                plan = " ".join(row[3] for row in rows)
        assert "SEARCH" in plan and "SCAN" not in plan and "USE TEMP" not in plan
    measured = sample(
        binding, digests[binding.declaration.id], dimensions, metrics, {"probe": children}
    )
    validate_execution_samples(projection, binding.declaration.id, [measured])
    return measured


@pytest.mark.growth_witness(
    "riverhog-retained-source-current-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_source.UploadJournalValidationSource.verify_current",
)
def test_actual_source_currentness_composes_the_probe_and_binding_codec(tmp_path, record_property):
    operation = UploadJournalValidationSource.verify_current
    binding, probe_binding = growth_binding(operation), growth_binding(replay._probe)
    assert dict(binding.delegates) == {"probe": replay._probe}
    projection = project_bindings((binding,))
    digests = {
        item.declaration.id: hashlib.sha256(
            canonical_json_bytes(projection["declarations"][item.declaration.id])
        ).hexdigest()
        for item in (binding, probe_binding)
    }
    samples = []
    record_property("growth_samples", samples)
    specifications = (
        (1, 0, 0, "absent", False),
        (1, 0, 0, "matching", False),
        (17, 0, 0, "matching", False),
        (129, 64, 0, "matching", False),
        (1025, 0, 0, "matching", False),
        (1025, 512, 0, "matching", False),
        (1025, 1024, 0, "matching", False),
        (1, 0, 65537, "matching", False),
        (1, 0, 0, "matching", True),
        (129, 128, 0, "wide-version", False),
        (17, 16, 0, "stale", False),
        (17, 8, 0, "changed", False),
        (1, 0, 0, "malformed", False),
        (1, 0, 0, "noncanonical", False),
    )
    for index, (count, position, payload, mode, wide_catalog) in enumerate(specifications):
        catalog = (
            ContractCatalog((pack("wide", "x" * 65537),))
            if wide_catalog
            else source_owner.admission_provenance_catalog()
        )
        with selected_source(tmp_path / str(index), count, position, payload, catalog) as (
            engine,
            factory,
            work,
            current,
        ):
            raw = current.binding.to_json()
            if mode == "absent":
                raw = None
            elif mode == "changed":
                digest = current.binding.catalog_sha256
                changed_digest = digest[:-1] + ("0" if digest[-1] != "0" else "1")
                raw = replace(current.binding, catalog_sha256=changed_digest).to_json()
            elif mode == "malformed":
                raw = '{"broken":true}'
            elif mode == "noncanonical":
                raw = json.dumps(json.loads(raw), indent=1)
            with factory() as session, session.begin():
                session.execute(
                    update(Scan)
                    .where(
                        Scan.collection_id == work.collection_id, Scan.journal_id == work.journal_id
                    )
                    .values(replay_binding_json=raw)
                ).close()
                if mode == "stale":
                    session.execute(
                        update(Journal)
                        .where(
                            Journal.collection_id == work.collection_id,
                            Journal.journal_id == work.journal_id,
                        )
                        .values(capture_version=work.version + 1)
                    ).close()
            visible_raw = None if mode == "stale" else raw
            dimensions = {
                "retained_journals": count,
                "binding_present": int(visible_raw is not None),
                "binding_octets": octets(visible_raw),
                "source_metadata_octets": source_metadata_octets(work),
            }
            samples.append(
                observe_current(
                    engine,
                    factory,
                    work,
                    current,
                    mode,
                    count,
                    dimensions,
                    binding,
                    probe_binding,
                    projection,
                    digests,
                )
            )


def test_native_metadata_row_width_counts_complete_fields():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    try:
        value, epoch = "界" * 65537, 2**60
        with engine.connect() as connection:
            row = connection.exec_driver_sql("SELECT ?, ?, ?", (value, None, epoch)).one()
        assert octets(row) == len(value.encode()) + len(str(epoch).encode())
        assert tuple(row) == (value, None, epoch)
    finally:
        engine.dispose()
