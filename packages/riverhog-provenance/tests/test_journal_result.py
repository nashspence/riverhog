from __future__ import annotations

import hashlib
import sqlite3
from contextlib import closing
from dataclasses import replace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import (
    IncompleteSourceError,
    JournalFrame,
    assertion,
    create_journal,
    encode_entry,
    iter_journal_frames,
    new_id,
    open_retained_journal_result,
    reference,
)
from riverhog_provenance._session_checkpoint import StaleValidationCheckpoint
from riverhog_provenance.journal_session import JournalValidationSession
from riverhog_provenance.journal_store import InvalidJournalWorkspace, JournalStore
from test_journal_graph_certificates import Certificates
from test_retained_journal_session import ATTEMPT, OwnedSource, directory, finish


def completed(tmp_path, journal, catalog, *, certified=False):
    path = directory(tmp_path)
    source = OwnedSource(journal, catalog)
    certificates = Certificates(source) if certified else None
    with JournalValidationSession.open_retained(
        path,
        source,
        attempt_id=ATTEMPT,
        worker_epoch=7,
        create=True,
        catalog=catalog,
        certificates=certificates,
    ) as session:
        confirmed = finish(session)
        with session.result() as result:
            expected = result.materialize()
    source.calls.clear()
    return path, source, confirmed, certificates, expected


def logical_octets(value):
    if value is None:
        return 0
    return len(value) if isinstance(value, bytes) else len(str(value).encode())


class ResultCursor(sqlite3.Cursor):
    closed = False

    def record(self, row):
        if row is not None:
            self.connection.metrics["returned_rows"] += 1
            self.connection.metrics["returned_metadata_octets"] += sum(map(logical_octets, row))
        return row

    def fetchone(self):
        return self.record(super().fetchone())

    def fetchall(self):
        return [self.record(row) for row in super().fetchall()]

    def close(self):
        self.closed = True
        return super().close()


class ResultConnection(sqlite3.Connection):
    recording = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.metrics = {}
        self.executions = []

    def execute(self, statement, parameters=()):
        if not self.recording:
            return super().execute(statement, parameters)
        self.metrics["bound_parameter_octets"] += sum(map(logical_octets, parameters))
        cursor = self.cursor(factory=ResultCursor)
        self.executions.append((statement, parameters, cursor))
        return cursor.execute(statement, parameters)


def history(raw, frame_count, catalog):
    first = next(iter_journal_frames((raw,)))
    template, previous = first.document, first.reference
    digest, size, pieces = hashlib.sha256(raw), len(raw), [raw]
    for sequence in range(1, frame_count):
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
                "purpose": "urn:test:retained-result-work",
            },
        }
        encoded = encode_entry(document, catalog=catalog)
        pieces.append(encoded)
        digest.update(encoded)
        size += len(encoded)
        previous = JournalFrame(encoded[1:-1]).reference
    return b"".join(pieces)


@pytest.mark.growth_witness(
    "riverhog-retained-result-admission-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.journal_result.open_retained_journal_result",
)
def test_retained_result_admission_records_actual_work(
    tmp_path, journal, graph, who, catalog, monkeypatch, record_property
):
    binding = growth_binding(open_retained_journal_result)
    projection = project_bindings((binding,))
    declaration = projection["declarations"][binding.declaration.id]
    samples = []
    record_property("growth_samples", samples)
    connect = sqlite3.connect
    regimes = ((1, 0, 0), (17, 0, 0), (65, 0, 0), (1, 65536, 1), (1, 0, 32), (1, 0, 128))
    for index, (frame_count, payload, extensions) in enumerate(regimes):
        raw = journal
        if extensions:
            selected = {
                **graph,
                "extensions": [
                    assertion(
                        "extension",
                        who,
                        subject=reference(graph["artifacts"][0]["id"], "artifact"),
                        property="urn:test:retained-result-work",
                        value={"type": "text", "value": "x" * (payload or 1)},
                    )
                    for _ in range(extensions)
                ],
            }
            raw = create_journal(selected, recorded_by_agent_id=who, catalog=catalog)
        raw = history(raw, frame_count, catalog)
        fixture = tmp_path / str(index)
        fixture.mkdir()
        path, source, confirmed, _, expected = completed(fixture, raw, catalog)
        with closing(connect(path / "snapshot.sqlite3")) as db:
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
                    "SELECT reference FROM frames WHERE sequence=?", (str(frame_count - 1),)
                ).fetchall()
            )
            header_octets = max(sum(map(logical_octets, row)) for row in headers)
            indexed = db.execute("SELECT count(*) FROM assertions").fetchone()[0]
        metrics = {
            key: 0
            for key in (
                "sql_statements",
                "returned_rows",
                "returned_metadata_octets",
                "bound_parameter_octets",
                "sqlite_vm_instructions",
                "source_probes",
                "historical_payload_reads",
            )
        }
        connections = []

        def current(observations=metrics):
            observations["source_probes"] += 1
            return True

        def observed_connect(database_path, *, observations=metrics, opened=connections, **kwargs):
            assert str(database_path).endswith("?mode=ro")
            connection = connect(database_path, factory=ResultConnection, **kwargs)
            connection.metrics = observations
            opened.append(connection)

            def trace(_statement):
                observations["sql_statements"] += 1

            def tick():
                observations["sqlite_vm_instructions"] += 1
                return 0

            def authorize(action, selected_table, column, *_rest):
                if action in {sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE}:
                    return sqlite3.SQLITE_DENY
                if (
                    action == sqlite3.SQLITE_READ
                    and selected_table in {"frames", "assertions", "journal_validation_frame"}
                    and column == "value"
                ):
                    observations["historical_payload_reads"] += 1
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            connection.set_trace_callback(trace)
            connection.set_progress_handler(tick, 1)
            connection.set_authorizer(authorize)
            return connection

        source.verify_current = current
        with monkeypatch.context() as patch:
            patch.setattr(sqlite3, "connect", observed_connect)
            result = open_retained_journal_result(
                path, source, confirmed=confirmed, catalog=catalog
            )
        connection = result._store.db
        metrics.update(
            returned_handles=int(result._store._frozen),
            opened_databases=len(connections),
            changed_rows=connection.total_changes,
            source_payload_octets=sum(octets for _offset, octets in source.calls),
            open_cursors=sum(not cursor.closed for _sql, _params, cursor in connection.executions),
        )
        executions = list(connection.executions)
        connection.recording = False
        connection.set_trace_callback(None)
        connection.set_progress_handler(None, 0)
        for statement, parameters, _cursor in executions:
            if statement.startswith("SELECT"):
                with closing(connect(path / "snapshot.sqlite3")) as oracle:
                    plan = " ".join(
                        row[3]
                        for row in oracle.execute("EXPLAIN QUERY PLAN " + statement, parameters)
                    )
                    assert "SCAN " not in plan and "TEMP B-TREE" not in plan, plan
        with result:
            assert result.anchor == expected["journal"]
            assert len(result.frames) == frame_count
            assert result.journal_sha256 == hashlib.sha256(raw).hexdigest()
        sample = {
            "declaration": binding.declaration.id,
            "declaration_sha256": hashlib.sha256(canonical_json_bytes(declaration)).hexdigest(),
            "dimensions": {
                "journal_octets": len(raw),
                "frozen_frames": frame_count,
                "indexed_assertions": indexed,
                "header_octets": header_octets,
            },
            "metrics": metrics,
            "delegates": {},
        }
        validate_execution_samples(projection, binding.declaration.id, [sample])
        samples.append(sample)


def test_confirmed_result_reads_only_fixed_metadata_and_preserves_worker_checkpoint(
    tmp_path, journal, catalog, monkeypatch
):
    path, source, confirmed, _, expected = completed(tmp_path, journal, catalog)
    before = (path / "snapshot.sqlite3").read_bytes()
    connect = sqlite3.connect
    statements = []

    def forbidden(*_args, **_kwargs):
        pytest.fail("frozen result replayed source or advanced validation")

    def authorize(action, table, column, *_rest):
        return (
            sqlite3.SQLITE_DENY
            if action in {sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE}
            or action == sqlite3.SQLITE_READ
            and table in {"frames", "assertions", "journal_validation_frame"}
            and column == "value"
            else sqlite3.SQLITE_OK
        )

    def readonly(*args, **kwargs):
        assert args[0].endswith("?mode=ro") and kwargs["uri"] is True
        connection = connect(*args, **kwargs)
        connection.set_authorizer(authorize)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(sqlite3, "connect", readonly)
    monkeypatch.setattr(source, "read_window", forbidden)
    monkeypatch.setattr(source, "read_exact", forbidden)
    monkeypatch.setattr(JournalValidationSession, "advance", forbidden)
    with open_retained_journal_result(path, source, confirmed=confirmed, catalog=catalog) as result:
        assert result.anchor == expected["journal"]
        count = len(statements)
        with result.borrow() as independent:
            assert len(statements) == count
            assert independent.anchor == result.anchor
        result._store.db.set_authorizer(None)
        assert result.materialize() == expected
        assert result._store.db.total_changes == 0
    assert (path / "snapshot.sqlite3").read_bytes() == before
    assert source.calls == []


def test_frozen_result_sqlite_connection_cannot_write_even_if_query_only_is_disabled(
    tmp_path, journal, catalog
):
    path, source, confirmed, _, _ = completed(tmp_path, journal, catalog)
    with open_retained_journal_result(path, source, confirmed=confirmed, catalog=catalog) as result:
        db = result._store.db
        db.execute("PRAGMA query_only=OFF").close()
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            db.execute("UPDATE journal_workspace SET format='changed'")
        db.rollback()
    with pytest.raises(ValueError, match="cannot create"):
        JournalStore.open_retained(path, create=True, read_only=True)


@pytest.mark.parametrize(
    "fault",
    [
        {"complete": False},
        {"phase": "terminal"},
        {"version": 0},
        {"worker_epoch": 8},
        {"attempt_id": "b" * 32},
        {"checkpoint_sha256": "0" * 64},
        {"accepted_frames": 0},
        {"accepted_octets": 0},
        {"accepted_sha256": "0" * 64},
        {"source_octets": 0},
    ],
)
def test_frozen_result_requires_the_exact_confirmed_progress(tmp_path, journal, catalog, fault):
    path, source, confirmed, _, _ = completed(tmp_path, journal, catalog)
    before = (path / "snapshot.sqlite3").read_bytes()
    released = []
    with pytest.raises((InvalidJournalWorkspace, StaleValidationCheckpoint)):
        open_retained_journal_result(
            path,
            source,
            confirmed=replace(confirmed, **fault),
            catalog=catalog,
            on_storage_close=lambda: released.append("closed"),
        )
    assert not released
    assert (path / "snapshot.sqlite3").read_bytes() == before


@pytest.mark.parametrize("when", ["before", "during"])
def test_frozen_result_checks_live_source_authority_on_both_sides_of_admission(
    tmp_path, journal, catalog, when
):
    path, source, confirmed, _, _ = completed(tmp_path, journal, catalog)
    calls = []

    def current():
        calls.append(1)
        return when == "during" and len(calls) == 1

    source.verify_current = current
    with pytest.raises(IncompleteSourceError):
        open_retained_journal_result(path, source, confirmed=confirmed, catalog=catalog)
    assert len(calls) == (1 if when == "before" else 2)


@pytest.mark.parametrize("changed", [False, True])
def test_frozen_result_requires_its_original_certificate_binding(
    tmp_path, journal, catalog, changed
):
    path, source, confirmed, certificates, expected = completed(
        tmp_path, journal, catalog, certified=True
    )
    with open_retained_journal_result(
        path,
        source,
        confirmed=confirmed,
        catalog=catalog,
        certificates=certificates.binding,
    ) as result:
        assert result.anchor == expected["journal"]
    binding = replace(certificates.binding, manifest_sha256="e" * 64) if changed else None
    with pytest.raises(StaleValidationCheckpoint):
        open_retained_journal_result(
            path,
            source,
            confirmed=confirmed,
            catalog=catalog,
            certificates=binding,
        )


def test_frozen_result_releases_owner_only_after_last_borrow(tmp_path, journal, catalog):
    path, source, confirmed, _, expected = completed(tmp_path, journal, catalog)
    released = []
    first = open_retained_journal_result(
        path,
        source,
        confirmed=confirmed,
        catalog=catalog,
        on_storage_close=lambda: released.append("closed"),
    )
    last = first.borrow()
    first.close()
    assert not released
    with last:
        assert last.anchor == expected["journal"]
    first.close()
    last.close()
    assert released == ["closed"]


@pytest.mark.parametrize("fault", ["missing", "input", "unfinished"])
def test_frozen_result_never_recreates_or_repairs_lost_native_state(
    tmp_path, journal, catalog, fault
):
    path, source, confirmed, _, _ = completed(tmp_path, journal, catalog)
    if fault == "missing":
        (path / "snapshot.sqlite3").unlink()
    else:
        with closing(sqlite3.connect(path / "snapshot.sqlite3")) as db:
            if fault == "input":
                db.execute("DELETE FROM journal_validation_input")
            else:
                db.execute(
                    "INSERT INTO journal_validation_frame VALUES(1,?,?,?)",
                    (str(confirmed.accepted_frames), "0" * 64, b"unfinished fixture atom"),
                )
            db.commit()
    released = []
    with pytest.raises((InvalidJournalWorkspace, RuntimeError, sqlite3.DatabaseError)):
        open_retained_journal_result(
            path,
            source,
            confirmed=confirmed,
            catalog=catalog,
            on_storage_close=lambda: released.append("closed"),
        )
    assert not released
    assert (path / "snapshot.sqlite3").exists() is (fault != "missing")


@pytest.mark.parametrize(
    "fault",
    [
        {"source_version": "8"},
        {"attempt_id": "foreign-capture"},
        {"directory_id": "another-owned-directory"},
        {"source_sha256": "0" * 64},
        {"require_profiles": False},
        {"catalog_sha256": "0" * 64},
    ],
)
def test_frozen_result_cannot_adopt_another_source_or_mode(tmp_path, journal, catalog, fault):
    path, source, confirmed, _, _ = completed(tmp_path, journal, catalog)
    source.binding = replace(source.binding, **fault)
    with pytest.raises((InvalidJournalWorkspace, StaleValidationCheckpoint)):
        open_retained_journal_result(path, source, confirmed=confirmed, catalog=catalog)


def test_parallel_borrowed_readers_preserve_exact_retained_snapshot(
    tmp_path, journal, catalog, parallel_snapshot_reads
):
    path, source, confirmed, _, expected = completed(tmp_path, journal, catalog)
    with open_retained_journal_result(
        path, source, confirmed=confirmed, catalog=catalog
    ) as summary:
        parallel_snapshot_reads(summary)
        assert summary.materialize() == expected
