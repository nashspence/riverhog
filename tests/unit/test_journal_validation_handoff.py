from __future__ import annotations

import hashlib
import sqlite3
import subprocess
import sys
import threading
from contextlib import contextmanager

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.catalog_models import (
    CollectionUploadProvenanceFrameRecord as Frame,
)
from riverhog_core.catalog_models import (
    CollectionUploadRecord,
)
from riverhog_core.services import collection_uploads as owner
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import validate_journal
from sqlalchemy import delete, event, update
from sqlalchemy.orm import sessionmaker

from tests.unit.test_journal_capture import advance, checkpoint, retained
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_staged_journal_range import EXTENT, JOURNAL, Chunk, Journal, staged_transport


def complete_capture(factory):
    while (work := owner._read_upload_journal_capture(factory, 1, JOURNAL)) is not None:
        assert owner._commit_upload_journal_capture(factory, work, advance(work))
    work = owner._read_upload_journal_validation(factory, 1, JOURNAL)
    assert work is not None
    return work


def service_for(factory):
    service = object.__new__(owner.SqlAlchemyCollectionUploadService)
    service._session_factory = factory
    return service


def terminal(factory):
    with factory() as session:
        return owner._journal_payload(session.get(Journal, (1, JOURNAL)))


def record_state(factory):
    with factory() as session:
        record = session.get(Journal, (1, JOURNAL))
        return tuple(getattr(record, column.name) for column in Journal.__table__.columns)


def test_native_computation_receives_exact_chunks_after_each_reader_closes(tmp_path, monkeypatch):
    raw = source(2 * EXTENT + 37, journal_id=JOURNAL)
    with retained(tmp_path, raw) as (engine, factory):
        work = complete_capture(factory)
        assert engine.pool.checkedout() == 0
        expected_chunks = []
        compute = owner.validate_journal_chunks
        read = owner._read_upload_journal_validation_chunk

        def observed_read(*args, **kwargs):
            result = read(*args, **kwargs)
            assert engine.pool.checkedout() == 0
            expected_chunks.append(result)
            return result

        def observed_compute(chunks, **kwargs):
            assert engine.pool.checkedout() == 0

            def detached():
                for raw in chunks:
                    assert engine.pool.checkedout() == 0
                    yield raw
                    assert engine.pool.checkedout() == 0

            return compute(detached(), **kwargs)

        monkeypatch.setattr(owner, "_read_upload_journal_validation_chunk", observed_read)
        monkeypatch.setattr(owner, "validate_journal_chunks", observed_compute)
        outcome = owner._compute_upload_journal_validation(factory, work)
        assert b"".join(expected_chunks) == raw
        assert len(expected_chunks) == 3 and max(map(len, expected_chunks)) <= EXTENT
        assert outcome.failure is None and engine.pool.checkedout() == 0
        assert terminal(factory)["state"] == "validating"
        assert terminal(factory)["anchor"] is None
        assert owner._commit_upload_journal_validation(factory, work, outcome)
        with validate_journal(raw) as expected:
            assert terminal(factory)["anchor"] == expected.anchor


@pytest.mark.parametrize("failure", [False, True])
@pytest.mark.parametrize(
    "fence",
    [
        "capture_attempt_id",
        "capture_directory_id",
        "capture_version",
        "capture_algorithm",
        "capture_source_sha256",
        "capture_source_bytes",
        "capture_phase",
        "sha256",
        "bytes",
        "capture_byte_offset",
        "capture_frame_count",
        "state",
    ],
)
def test_stale_success_and_failure_cannot_publish_or_read_replacement(tmp_path, failure, fence):
    raw = source(journal_id=JOURNAL)
    with retained(tmp_path, raw) as (_, factory):
        work = complete_capture(factory)
        outcome = owner._compute_upload_journal_validation(factory, work)
        if failure:
            outcome = owner._UploadJournalValidationOutcome("rejected old attempt")
        replacements = {
            "capture_attempt_id": "replacement-attempt",
            "capture_directory_id": "replacement-directory",
            "capture_version": work.version + 1,
            "capture_algorithm": "replacement-algorithm",
            "capture_source_sha256": "d" * 64,
            "capture_source_bytes": len(raw) + 1,
            "capture_phase": "scanning",
            "sha256": "d" * 64,
            "bytes": len(raw) + 1,
            "capture_byte_offset": len(raw) - 1,
            "capture_frame_count": work.cursor.frame_count + 1,
            "state": "accepting",
        }
        with factory() as session, session.begin():
            session.execute(update(Journal).values({fence: replacements[fence]}))
        before = record_state(factory), checkpoint(factory)
        with pytest.raises(owner._StaleUploadJournalValidation):
            owner._read_upload_journal_validation_chunk(factory, work, ordinal=0, offset=0)
        assert not owner._commit_upload_journal_validation(factory, work, outcome)
        assert (record_state(factory), checkpoint(factory)) == before


@pytest.mark.parametrize("failure", [False, True])
def test_duplicate_terminal_publication_is_fenced(tmp_path, failure):
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        work = complete_capture(factory)
        outcome = (
            owner._UploadJournalValidationOutcome("native rejection")
            if failure
            else owner._compute_upload_journal_validation(factory, work)
        )
        assert owner._commit_upload_journal_validation(factory, work, outcome)
        published = terminal(factory), checkpoint(factory)
        assert checkpoint(factory)[0] == work.version + 1
        assert terminal(factory)["state"] == ("failed" if failure else "sealed")
        assert not owner._commit_upload_journal_validation(factory, work, outcome)
        assert (terminal(factory), checkpoint(factory)) == published


@pytest.mark.parametrize("replace_attempt", [False, True])
def test_unrelated_catalog_work_progresses_during_native_computation(
    tmp_path, monkeypatch, replace_attempt
):
    raw = source(journal_id=JOURNAL)
    with retained(tmp_path, raw) as (engine, factory):
        complete_capture(factory)
        paused, resume = threading.Event(), threading.Event()
        native = owner.validate_journal_chunks
        failures = []

        def waiting(chunks, **kwargs):
            iterator = iter(chunks)
            first = next(iterator)
            assert engine.pool.checkedout() == 0
            paused.set()
            if not resume.wait(30):
                raise TimeoutError("native computation was not resumed")

            def continued():
                yield first
                yield from iterator

            return native(continued(), **kwargs)

        monkeypatch.setattr(owner, "validate_journal_chunks", waiting)

        def worker():
            try:
                service_for(factory)._advance_provenance_journal_validation(1, JOURNAL)
            except BaseException as exc:
                failures.append(exc)

        thread = threading.Thread(target=worker)
        thread.start()
        try:
            assert paused.wait(10)
            assert engine.pool.checkedout() == 0
            with factory() as session, session.begin():
                session.execute(
                    update(CollectionUploadRecord).values(last_activity_at="2026-01-02T00:00:00Z")
                )
                if replace_attempt:
                    session.execute(update(Journal).values(capture_attempt_id="replacement"))
            with factory() as session:
                assert (
                    session.get(CollectionUploadRecord, 1).last_activity_at
                    == "2026-01-02T00:00:00Z"
                )
        finally:
            resume.set()
            thread.join(30)
        assert not thread.is_alive() and not failures
        assert engine.pool.checkedout() == 0
        assert terminal(factory)["state"] == ("validating" if replace_attempt else "sealed")
        if replace_attempt:
            assert terminal(factory)["anchor"] is None
            service_for(factory)._advance_provenance_journal_validation(1, JOURNAL)
            assert terminal(factory)["state"] == "sealed"


def test_lost_retained_chunk_never_publishes_an_anchor(tmp_path):
    with retained(tmp_path, source(journal_id=JOURNAL)) as (engine, factory):
        work = complete_capture(factory)
        with factory() as session, session.begin():
            session.execute(delete(Chunk))
        outcome = owner._compute_upload_journal_validation(factory, work)
        assert outcome.failure == "staged canonical journal chunks are discontinuous"
        assert engine.pool.checkedout() == 0
        assert owner._commit_upload_journal_validation(factory, work, outcome)
        assert terminal(factory)["state"] == "failed"
        assert terminal(factory)["anchor"] is None
        assert checkpoint(factory)[3] == 0


def test_lost_capture_manifest_link_before_publication_fails_closed(tmp_path):
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        work = complete_capture(factory)
        outcome = owner._compute_upload_journal_validation(factory, work)
        with factory() as session, session.begin():
            session.execute(delete(Frame))
        before = record_state(factory)
        with pytest.raises(RuntimeError, match="missing frame position"):
            owner._commit_upload_journal_validation(factory, work, outcome)
        assert record_state(factory) == before
        assert terminal(factory)["state"] == "validating"
        assert terminal(factory)["anchor"] is None


@pytest.mark.parametrize("commit", [False, True])
def test_process_exit_preserves_only_committed_terminal_metadata(tmp_path, commit):
    raw = source(journal_id=JOURNAL)
    with retained(tmp_path, raw) as (engine, factory):
        complete_capture(factory)
        before = terminal(factory), checkpoint(factory)
        code = """
import os, sys
from contextlib import contextmanager
from riverhog_core.catalog_db import make_session_factory
from riverhog_core.services import collection_uploads as owner
factory = make_session_factory(sys.argv[1])
work = owner._read_upload_journal_validation(factory, 1, sys.argv[2])
outcome = owner._compute_upload_journal_validation(factory, work)
assert outcome.failure is None
original = owner.session_scope
@contextmanager
def interrupted(factory):
    with original(factory) as session:
        yield session
        os._exit(73)
if sys.argv[3] == 'False':
    owner.session_scope = interrupted
assert owner._commit_upload_journal_validation(factory, work, outcome)
os._exit(73)
"""
        result = subprocess.run(
            [sys.executable, "-c", code, str(engine.url), JOURNAL, str(commit)],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 73, result.stderr
        engine.dispose()
        if commit:
            assert terminal(factory)["state"] == "sealed"
            assert checkpoint(factory)[0] == before[1][0] + 1
            with validate_journal(raw) as expected:
                assert terminal(factory)["anchor"] == expected.anchor
        else:
            assert (terminal(factory), checkpoint(factory)) == before
            service_for(factory)._advance_provenance_journal_validation(1, JOURNAL)
            assert terminal(factory)["state"] == "sealed"


class ObservedSourceResult:
    def __init__(self, result, counts):
        self.result, self.counts = result, counts

    def __enter__(self):
        self.result.__enter__()
        return self

    def __exit__(self, *args):
        try:
            return self.result.__exit__(*args)
        finally:
            self.counts["closed"] = self.result.closed

    def one_or_none(self):
        row = self.result.one_or_none()
        if row is not None:
            self.counts["source_rows"] += 1
            self.counts["loaded_chunk_octets"] += len(row.content)
        return row


@pytest.mark.growth_witness(
    "riverhog-staged-journal-validation-source-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.collection_uploads._read_upload_journal_validation_chunk",
)
def test_fenced_source_units_record_actual_indexed_work(tmp_path, monkeypatch, record_property):
    binding = growth_binding(owner._read_upload_journal_validation_chunk)
    projection = project_bindings((binding,))
    declaration = projection["declarations"][binding.declaration.id]
    samples = []
    for count, first_octets, last_octets in [
        *((j, n, 37) for j in (2, 17, 129) for n in (EXTENT, 37)),
        (17, 37, EXTENT),
    ]:
        with staged_transport(
            tmp_path / f"{count}-{first_octets}-{last_octets}",
            count,
            last_octets=last_octets,
            first_octets=first_octets,
        ) as (engine, total):
            factory = sessionmaker(engine)
            with factory() as session, session.begin():
                record = session.get(Journal, (1, JOURNAL))
                owner._begin_upload_journal_capture(session, record)
                record.capture_phase = "deferred"
                record.capture_failure = "transport fixture has no semantic acceptance"
            work = owner._read_upload_journal_validation(factory, 1, JOURNAL)
            counts = {"source_rows": 0, "loaded_chunk_octets": 0, "closed": False}
            original = owner.read_snapshot

            @contextmanager
            def observed_snapshot(factory, snapshot=original, observations=counts):
                with snapshot(factory) as session:
                    execute = session.execute

                    def observed_execute(*args, **kwargs):
                        return ObservedSourceResult(execute(*args, **kwargs), observations)

                    monkeypatch.setattr(session, "execute", observed_execute)
                    yield session

            with engine.connect() as connection:
                native = connection.connection.driver_connection
                assert isinstance(native, sqlite3.Connection)
            statements, instructions = [], [0]

            def observed_sql(
                _connection, _cursor, sql, parameters, _context, _many, recorder=statements
            ):
                recorder.append((sql, parameters))

            def progress(counter=instructions):
                counter[0] += 1
                return 0

            event.listen(engine, "before_cursor_execute", observed_sql)
            native.set_progress_handler(progress, 1)
            try:
                with monkeypatch.context() as patch:
                    patch.setattr(owner, "read_snapshot", observed_snapshot)
                    actual = owner._read_upload_journal_validation_chunk(
                        factory, work, ordinal=count - 1, offset=total - last_octets
                    )
            finally:
                native.set_progress_handler(None, 0)
                event.remove(engine, "before_cursor_execute", observed_sql)
            assert actual == bytes([(count - 1) % 251]) * last_octets
            assert engine.pool.checkedout() == 0 and counts["closed"]
            for sql, parameters in statements:
                with engine.connect() as connection:
                    rows = connection.exec_driver_sql("EXPLAIN QUERY PLAN " + sql, parameters)
                    try:
                        plan = " ".join(row[3] for row in rows)
                    finally:
                        rows.close()
                assert "SEARCH" in plan and "SCAN" not in plan and "USE TEMP" not in plan
            sample = {
                "declaration": binding.declaration.id,
                "declaration_sha256": hashlib.sha256(canonical_json_bytes(declaration)).hexdigest(),
                "dimensions": {"staged_chunks": count},
                "metrics": {
                    "sql_statements": len(statements),
                    "source_rows": counts["source_rows"],
                    "loaded_chunk_octets": counts["loaded_chunk_octets"],
                    "returned_octets": len(actual),
                    "sqlite_vm_instructions": instructions[0],
                },
                "delegates": {},
            }
            validate_execution_samples(projection, binding.declaration.id, [sample])
            samples.append(sample)
    record_property("growth_samples", samples)
