from __future__ import annotations

import fcntl
import hashlib
import os
import sqlite3
from contextlib import closing
from dataclasses import asdict

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.services import upload_journal_result as owner
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import JournalFrame, encode_entry, iter_journal_frames, new_id
from sqlalchemy import event, func, insert, select

from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_staged_journal_range import JOURNAL, Journal
from tests.unit.test_upload_journal_result import open_result, sealed


def octets(value):
    if value is None:
        return 0
    return len(value) if isinstance(value, bytes) else len(str(value).encode())


class NativeCursor(sqlite3.Cursor):
    closed = False

    def record(self, row):
        if row is not None:
            self.connection.metrics["returned_rows"] += 1
            self.connection.metrics["returned_metadata_octets"] += sum(map(octets, row))
        return row

    def fetchone(self):
        return self.record(super().fetchone())

    def fetchall(self):
        return [self.record(row) for row in super().fetchall()]

    def close(self):
        self.closed = True
        return super().close()


class NativeConnection(sqlite3.Connection):
    def execute(self, statement, parameters=()):
        if not self.recording:
            return super().execute(statement, parameters)
        self.metrics["bound_parameter_octets"] += sum(map(octets, parameters))
        cursor = self.cursor(factory=NativeCursor)
        self.executions.append((statement, parameters, cursor))
        return cursor.execute(statement, parameters)


class CatalogResult:
    def __init__(self, actual, metrics):
        self.actual, self.metrics = actual, metrics

    def __enter__(self):
        self.actual.__enter__()
        return self

    def __exit__(self, *args):
        return self.actual.__exit__(*args)

    def mappings(self):
        return CatalogResult(self.actual.mappings(), self.metrics)

    def one_or_none(self):
        row = self.actual.one_or_none()
        if row is not None:
            self.metrics["catalog_returned_rows"] += 1
            values = row.values() if hasattr(row, "values") else row
            self.metrics["catalog_metadata_octets"] += sum(map(octets, values))
        return row


def history(raw, frames, catalog):
    first = next(iter_journal_frames((raw,)))
    previous, template = first.reference, first.document
    digest, size, pieces = hashlib.sha256(raw), len(raw), [raw]
    for sequence in range(1, frames):
        document = {
            **template,
            "id": new_id(),
            "sequence": str(sequence),
            "entry_kind": "checkpoint",
            "previous_entry": previous,
            "body": {
                "covered_through": previous,
                "prefix_sha256": digest.hexdigest(),
                "prefix_bytes": str(size),
                "purpose": "urn:test:sealed-result-work",
            },
        }
        encoded = encode_entry(document, catalog=catalog)
        pieces.append(encoded)
        digest.update(encoded)
        size += len(encoded)
        previous = JournalFrame(encoded[1:-1]).reference
    return b"".join(pieces)


def native_dimensions(connect, database, raw, frames):
    with closing(connect(database)) as db:
        headers = []
        for table in (
            "journal_workspace",
            "dag_algorithm",
            "dag_runs",
            "journal_prefix",
            "journal_validation",
            "journal_validation_input",
        ):
            headers.extend(db.execute(f"SELECT * FROM {table}").fetchall())
        headers.extend(
            db.execute(
                "SELECT reference FROM frames WHERE sequence=?", (str(frames - 1),)
            ).fetchall()
        )
        return {
            "journal_octets": len(raw),
            "frozen_frames": frames,
            "indexed_assertions": db.execute("SELECT count(*) FROM assertions").fetchone()[0],
            "header_octets": max(sum(map(octets, row)) for row in headers),
        }


def sample(binding, projection, dimensions, metrics, delegates):
    return {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical_json_bytes(projection["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": dimensions,
        "metrics": metrics,
        "delegates": delegates,
    }


@pytest.mark.growth_witness(
    "riverhog-sealed-result-composed-admission-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_result.open_upload_journal_result",
)
def test_sealed_result_admission_records_catalog_native_and_lease_work(
    tmp_path,
    monkeypatch,
    record_property,
):
    binding = growth_binding(owner.open_upload_journal_result)
    native_binding = growth_binding(owner.open_retained_journal_result)
    assert dict(binding.delegates)["native"] is owner.open_retained_journal_result
    projection = project_bindings((binding,))
    samples = []
    record_property("growth_samples", samples)
    connect = sqlite3.connect
    native_open = owner.open_retained_journal_result
    from riverhog_core.services.collection_uploads import admission_provenance_catalog

    catalog = admission_provenance_catalog()
    regimes = (
        (1, 1, 1, 0),
        (65, 1, 1, 0),
        (1, 65, 1, 0),
        (257, 257, 1, 0),
        (1, 1, 17, 0),
        (1, 1, 1, 65536),
    )
    for index, (journals, job_count, frames, payload) in enumerate(regimes):
        raw = history(source(payload, journal_id=JOURNAL), frames, catalog)
        with sealed(tmp_path / str(index), raw=raw) as (engine, factory, root, job, _):
            with factory() as session, session.begin():
                if journals > 1:
                    session.execute(
                        insert(Journal),
                        [
                            dict(
                                collection_id=1,
                                journal_id=f"urn:uuid:{number:08x}-0000-4000-8000-000000000000",
                                bytes=0,
                                sha256="0" * 64,
                                accepted_bytes=0,
                                next_chunk_ordinal=0,
                                content_hash_state="{}",
                                state="accepting",
                            )
                            for number in range(1, journals)
                        ],
                    )
                if job_count > 1:
                    session.execute(
                        insert(owner.Job),
                        [
                            {
                                **asdict(job),
                                "workspace_id": f"{number:032x}",
                                "collection_id": None,
                                "journal_id": None,
                            }
                            for number in range(1, job_count)
                        ],
                    )
            database = root / job.workspace_id / "snapshot.sqlite3"
            child_dimensions = native_dimensions(connect, database, raw, frames)
            with factory() as session, session.begin():
                source_row = session.execute(
                    select(*(getattr(Journal, key) for key in owner._SOURCE_FIELDS)).where(
                        Journal.journal_id == JOURNAL
                    )
                ).one()
                job_row = session.execute(
                    select(*(getattr(owner.Job, key) for key in asdict(job))).where(
                        owner.Job.workspace_id == job.workspace_id
                    )
                ).one()
                dimensions = {
                    **child_dimensions,
                    "catalog_journals": session.scalar(select(func.count()).select_from(Journal)),
                    "catalog_jobs": session.scalar(select(func.count()).select_from(owner.Job)),
                    "catalog_row_octets": max(
                        sum(map(octets, row)) for row in (source_row, job_row)
                    ),
                    "workspace_path_octets": len(os.fsencode(database)),
                    "workspace_path_components": len(database.parts),
                }
                assert dimensions["catalog_journals"] == journals
                assert dimensions["catalog_jobs"] == job_count
                metrics = {
                    key: 0
                    for key in (
                        "catalog_statements",
                        "catalog_returned_rows",
                        "catalog_metadata_octets",
                        "catalog_parameter_octets",
                        "catalog_vm_instructions",
                        "catalog_payload_reads",
                        "lease_opens",
                        "lease_fsyncs",
                        "lease_lock_calls",
                        "filesystem_metadata_calls",
                        "filesystem_path_octets",
                    )
                }
                native_metrics = {
                    key: 0
                    for key in (
                        "sql_statements",
                        "returned_rows",
                        "returned_metadata_octets",
                        "bound_parameter_octets",
                        "sqlite_vm_instructions",
                        "source_probes",
                        "opening_callbacks",
                        "historical_payload_reads",
                        "source_payload_octets",
                    )
                }
                connections, executions, results, children, locks = [], [], [], [], []
                catalog_db = session.connection().connection.driver_connection
                catalog_before = catalog_db.total_changes
                execute, verify = session.execute, owner._SealedSource.verify_current

                def tick_catalog(observations=metrics):
                    observations["catalog_vm_instructions"] += 1
                    return 0

                def authorize_catalog(action, table, column, *_rest, observations=metrics):
                    if action in {
                        sqlite3.SQLITE_INSERT,
                        sqlite3.SQLITE_UPDATE,
                        sqlite3.SQLITE_DELETE,
                    }:
                        return sqlite3.SQLITE_DENY
                    if action == sqlite3.SQLITE_READ and column == "content":
                        observations["catalog_payload_reads"] += 1
                        return sqlite3.SQLITE_DENY
                    return sqlite3.SQLITE_OK

                def statement(
                    _connection,
                    _cursor,
                    sql,
                    parameters,
                    _context,
                    many,
                    observations=metrics,
                    seen=executions,
                ):
                    assert not many
                    observations["catalog_statements"] += 1
                    observations["catalog_parameter_octets"] += sum(map(octets, parameters))
                    seen.append((sql, parameters))

                def observed_execute(
                    *args, actual=execute, observations=metrics, seen=results, **kwargs
                ):
                    result = actual(*args, **kwargs)
                    seen.append(result)
                    return CatalogResult(result, observations)

                def current(self, actual=verify, observations=native_metrics):
                    observations["source_probes"] += 1
                    return actual(self)

                def observed_connect(
                    database_path, *, observations=native_metrics, opened=connections, **kwargs
                ):
                    assert str(database_path).endswith("?mode=ro")
                    connection = connect(database_path, factory=NativeConnection, **kwargs)
                    connection.metrics, connection.executions, connection.recording = (
                        observations,
                        [],
                        True,
                    )
                    opened.append(connection)

                    def trace(_sql):
                        observations["sql_statements"] += 1

                    def tick():
                        observations["sqlite_vm_instructions"] += 1
                        return 0

                    def authorize(action, table, column, *_rest):
                        if action in {
                            sqlite3.SQLITE_INSERT,
                            sqlite3.SQLITE_UPDATE,
                            sqlite3.SQLITE_DELETE,
                        }:
                            return sqlite3.SQLITE_DENY
                        if (
                            action == sqlite3.SQLITE_READ
                            and table in {"frames", "assertions", "journal_validation_frame"}
                            and column == "value"
                        ):
                            observations["historical_payload_reads"] += 1
                            return sqlite3.SQLITE_DENY
                        return sqlite3.SQLITE_OK

                    connection.set_trace_callback(trace)
                    connection.set_progress_handler(tick, 1)
                    connection.set_authorizer(authorize)
                    return connection

                def observe_native(
                    *args,
                    observations=native_metrics,
                    opened=connections,
                    child_samples=children,
                    inputs=child_dimensions,
                    **kwargs,
                ):
                    callback = kwargs.get("on_storage_open")
                    if callback is not None:

                        def retained(opening):
                            observations["opening_callbacks"] += 1
                            return callback(opening)

                        kwargs["on_storage_open"] = retained
                    result = native_open(*args, **kwargs)
                    db = result._store.db
                    observations.update(
                        returned_handles=int(result._store._frozen),
                        opened_databases=len(opened),
                        changed_rows=db.total_changes,
                        open_cursors=sum(
                            not cursor.closed for _sql, _params, cursor in db.executions
                        ),
                    )
                    child_samples.append(
                        sample(native_binding, projection, inputs, dict(observations), {})
                    )
                    return result

                def payload_read(self, offset, maximum_octets, observations=native_metrics):
                    observations["source_payload_octets"] += maximum_octets
                    pytest.fail("sealed admission requested source payload")

                def observer(
                    actual, name, *, path=False, is_lock=False, observations=metrics, acquired=locks
                ):
                    def invoke(*args, **kwargs):
                        observations[name] += 1
                        if path and isinstance(args[0], (str, bytes, os.PathLike)):
                            observations["filesystem_path_octets"] += len(os.fsencode(args[0]))
                        if is_lock:
                            acquired.append(args[1])
                        return actual(*args, **kwargs)

                    return invoke

                event.listen(engine, "before_cursor_execute", statement)
                catalog_db.set_progress_handler(tick_catalog, 1)
                catalog_db.set_authorizer(authorize_catalog)
                try:
                    with monkeypatch.context() as patch:
                        patch.setattr(session, "execute", observed_execute)
                        patch.setattr(owner._SealedSource, "verify_current", current)
                        patch.setattr(owner._SealedSource, "read_window", payload_read)
                        patch.setattr(owner._SealedSource, "read_exact", payload_read)
                        patch.setattr(owner, "open_retained_journal_result", observe_native)
                        patch.setattr(sqlite3, "connect", observed_connect)
                        patch.setattr(os, "open", observer(os.open, "lease_opens", path=True))
                        patch.setattr(os, "fsync", observer(os.fsync, "lease_fsyncs"))
                        for operation in ("stat", "lstat", "fstat", "readlink"):
                            patch.setattr(
                                os,
                                operation,
                                observer(
                                    getattr(os, operation),
                                    "filesystem_metadata_calls",
                                    path=operation != "fstat",
                                ),
                            )
                        patch.setattr(
                            fcntl, "flock", observer(fcntl.flock, "lease_lock_calls", is_lock=True)
                        )
                        result = open_result(session, root)
                finally:
                    event.remove(engine, "before_cursor_execute", statement)
                    catalog_db.set_progress_handler(None, 0)
                    catalog_db.set_authorizer(None)
                assert locks == [
                    fcntl.LOCK_EX | fcntl.LOCK_NB,
                    fcntl.LOCK_SH | fcntl.LOCK_NB,
                    fcntl.LOCK_UN,
                ]
                db = result._store.db
                native_metrics["open_cursors"] = sum(
                    not cursor.closed for _sql, _params, cursor in db.executions
                )
                metrics.update(("native_" + key, value) for key, value in native_metrics.items())
                metrics.update(
                    catalog_changed_rows=catalog_db.total_changes - catalog_before,
                    catalog_open_cursors=sum(not row.closed for row in results),
                )
                db.recording = False
                db.set_progress_handler(None, 0)
                db.set_trace_callback(None)
                for sql, parameters in executions:
                    plan = " ".join(
                        row[3]
                        for row in catalog_db.execute("EXPLAIN QUERY PLAN " + sql, parameters)
                    )
                    assert "SCAN " not in plan and "TEMP B-TREE" not in plan, plan
                for sql, parameters, _cursor in db.executions:
                    if sql.startswith("SELECT"):
                        with closing(connect(database)) as oracle:
                            plan = " ".join(
                                row[3]
                                for row in oracle.execute("EXPLAIN QUERY PLAN " + sql, parameters)
                            )
                            assert "SCAN " not in plan and "TEMP B-TREE" not in plan, plan
                with result:
                    assert result.journal_sha256 == hashlib.sha256(raw).hexdigest()
                    assert result.anchor["through"]["sequence"] == str(frames - 1)
                measured = sample(binding, projection, dimensions, metrics, {"native": children})
                validate_execution_samples(projection, binding.declaration.id, [measured])
                samples.append(measured)
