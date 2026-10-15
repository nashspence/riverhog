from __future__ import annotations

import hashlib
import sqlite3
import subprocess
import sys
from dataclasses import replace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.catalog_models import CollectionUploadProvenanceCaptureSegmentRecord as Segment
from riverhog_core.services import collection_uploads as owner
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from sqlalchemy import delete, event, select, update

from tests.unit.test_journal_capture import advance, checkpoint, retained
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_journal_validation_handoff import complete_capture, terminal
from tests.unit.test_staged_journal_range import JOURNAL, Chunk, Journal

PAGE = owner._JOURNAL_CAPTURE_SEGMENT_OCTETS


def pages(factory):
    with factory() as session:
        return [
            tuple(row)
            for row in session.execute(
                select(Segment.ordinal, Segment.content, Segment.sha256).order_by(Segment.ordinal)
            )
        ]


def assert_prefix(factory, raw):
    position = checkpoint(factory)[1]
    rows = pages(factory)
    assert [row[0] for row in rows] == list(range((position + PAGE - 1) // PAGE))
    assert b"".join(row[1] for row in rows) == raw[:position]
    for ordinal, content, digest in rows:
        assert len(content) == min(PAGE, position - ordinal * PAGE)
        assert hashlib.sha256(content).hexdigest() == digest


@pytest.mark.parametrize("size", [PAGE - 1, PAGE, PAGE + 1, 2 * PAGE + 1])
def test_capture_retains_exact_pages_and_never_rewrites_completed_pages(tmp_path, size):
    raw = b"\x1e" + b"x" * (size - 2) + b"\n"
    with retained(tmp_path, raw) as (_, factory):
        while (work := owner._read_upload_journal_capture(factory, 1, JOURNAL)) is not None:
            before = pages(factory)
            result = advance(work)
            prepared = owner._prepare_upload_journal_capture_segments(work, result)
            assert len(prepared) <= 2
            assert sum(len(row[1]) for row in prepared) <= (
                len(work.tail_content) + result.cursor.byte_offset - work.cursor.byte_offset
            )
            assert owner._commit_upload_journal_capture(factory, work, result)
            assert_prefix(factory, raw)
            assert (
                pages(factory)[: work.cursor.byte_offset // PAGE]
                == before[: work.cursor.byte_offset // PAGE]
            )
        assert checkpoint(factory)[5] == "complete"
        assert terminal(factory)["state"] == "validating"
        assert terminal(factory)["anchor"] is None


def test_one_octet_transport_rows_are_coalesced_without_inventing_a_total_limit(tmp_path):
    raw = b"\x1e" + b"x" * 203 + b"\n"
    with retained(tmp_path, raw) as (_, factory):
        with factory() as session, session.begin():
            session.execute(delete(Chunk))
            session.add_all(
                [
                    Chunk(
                        collection_id=1,
                        journal_id=JOURNAL,
                        ordinal=n,
                        byte_offset=n,
                        content=raw[n : n + 1],
                    )
                    for n in range(len(raw))
                ]
            )
            session.get(Journal, (1, JOURNAL)).next_chunk_ordinal = len(raw)
        steps = 0
        while (work := owner._read_upload_journal_capture(factory, 1, JOURNAL)) is not None:
            assert len(work.content) == 1
            assert owner._commit_upload_journal_capture(factory, work, advance(work))
            steps += 1
        assert steps == len(raw)
        assert len(pages(factory)) == 1
        assert_prefix(factory, raw)


def test_a_partial_source_tail_crosses_into_two_pages_with_one_bounded_unit(tmp_path):
    raw = b"\x1e" + b"x" * (2 * PAGE + 9) + b"\n"
    with retained(tmp_path, raw) as (_, factory):
        with factory() as session, session.begin():
            session.execute(delete(Chunk))
            starts = [0, 37, 37 + PAGE, len(raw)]
            session.add_all(
                [
                    Chunk(
                        collection_id=1,
                        journal_id=JOURNAL,
                        ordinal=n,
                        byte_offset=start,
                        content=raw[start:end],
                    )
                    for n, (start, end) in enumerate(zip(starts[:-1], starts[1:], strict=True))
                ]
            )
            session.get(Journal, (1, JOURNAL)).next_chunk_ordinal = len(starts) - 1
        crossing = False
        while (work := owner._read_upload_journal_capture(factory, 1, JOURNAL)) is not None:
            result = advance(work)
            prepared = owner._prepare_upload_journal_capture_segments(work, result)
            crossing |= len(prepared) == 2
            assert len(prepared) <= 2
            assert sum(len(row[1]) for row in prepared) <= len(work.content) + len(
                work.tail_content
            )
            assert owner._commit_upload_journal_capture(factory, work, result)
            assert_prefix(factory, raw)
        assert crossing


def test_stale_reader_checks_the_attempt_before_loading_an_existing_page(tmp_path):
    with retained(tmp_path, source(journal_id=JOURNAL)) as (engine, factory):
        work = complete_capture(factory)
        before = pages(factory)
        with factory() as session, session.begin():
            session.execute(update(Journal).values(capture_attempt_id="replacement"))
        statements = []

        def sql(_connection, _cursor, statement, _parameters, _context, _many):
            statements.append(statement)

        event.listen(engine, "before_cursor_execute", sql)
        try:
            assert owner._read_upload_journal_capture_segment(factory, work, ordinal=0) is None
        finally:
            event.remove(engine, "before_cursor_execute", sql)
        assert len(statements) == 1
        assert pages(factory) == before
        assert engine.pool.checkedout() == 0


def test_authenticated_prefix_extension_retains_full_pages_and_extends_only_the_tail(tmp_path):
    raw = source(2 * PAGE + 7, journal_id=JOURNAL)
    extra = b"\x1e{}\n"
    with retained(tmp_path, raw) as (_, factory):
        complete_capture(factory)
        before = pages(factory)
        with factory() as session, session.begin():
            record = session.get(Journal, (1, JOURNAL))
            directory = record.capture_directory_id
            session.add(
                Chunk(
                    collection_id=1,
                    journal_id=JOURNAL,
                    ordinal=record.next_chunk_ordinal,
                    byte_offset=len(raw),
                    content=extra,
                )
            )
            record.next_chunk_ordinal += 1
            record.bytes = record.accepted_bytes = len(raw) + len(extra)
            record.sha256 = hashlib.sha256(raw + extra).hexdigest()
            record.capture_phase = "extending"
            owner._begin_upload_journal_capture(session, record)
            assert record.capture_directory_id == directory
        complete_capture(factory)
        assert_prefix(factory, raw + extra)
        assert pages(factory)[: len(raw) // PAGE] == before[: len(raw) // PAGE]


@pytest.mark.parametrize("fault", ["missing", "content", "digest", "extent"])
def test_reopen_fails_closed_on_a_missing_or_changed_source_page(tmp_path, fault):
    raw = source(2 * PAGE, journal_id=JOURNAL)
    with retained(tmp_path, raw) as (engine, factory):
        work = owner._read_upload_journal_capture(factory, 1, JOURNAL)
        assert owner._commit_upload_journal_capture(factory, work, advance(work))
        before = checkpoint(factory)
        with factory() as session, session.begin():
            if fault == "missing":
                session.execute(delete(Segment))
            else:
                values = (
                    {"content": b"x" * PAGE}
                    if fault == "content"
                    else (
                        {"sha256": "0" * 64}
                        if fault == "digest"
                        else {"content": b"x", "sha256": hashlib.sha256(b"x").hexdigest()}
                    )
                )
                session.execute(update(Segment).values(**values))
        with pytest.raises(RuntimeError, match="source page"):
            owner._read_upload_journal_capture(factory, 1, JOURNAL)
        assert checkpoint(factory) == before
        assert engine.pool.checkedout() == 0


def test_failed_source_page_write_rolls_back_cursor_and_frame_directory(tmp_path):
    with retained(tmp_path, source(journal_id=JOURNAL)) as (engine, factory):
        work = owner._read_upload_journal_capture(factory, 1, JOURNAL)
        before = checkpoint(factory), pages(factory)

        def failed(_connection, _cursor, sql, _parameters, _context, _many):
            if sql.startswith("INSERT INTO collection_upload_provenance_capture_segments"):
                raise RuntimeError("source page write failed")

        event.listen(engine, "before_cursor_execute", failed)
        try:
            with pytest.raises(RuntimeError, match="source page write failed"):
                owner._commit_upload_journal_capture(factory, work, advance(work))
        finally:
            event.remove(engine, "before_cursor_execute", failed)
        assert (checkpoint(factory), pages(factory)) == before


def test_stale_prepared_tail_cannot_overwrite_a_replacement_attempt(tmp_path):
    raw = source(journal_id=JOURNAL)
    with retained(tmp_path, raw) as (_, factory):
        work = owner._read_upload_journal_capture(factory, 1, JOURNAL)
        result = advance(work)
        with factory() as session, session.begin():
            session.execute(update(Journal).values(capture_attempt_id="replacement"))
        before = checkpoint(factory), pages(factory)
        assert not owner._commit_upload_journal_capture(factory, work, result)
        assert (
            owner._read_upload_journal_capture_segment(
                factory, replace(work, cursor=result.cursor), ordinal=0
            )
            is None
        )
        assert (checkpoint(factory), pages(factory)) == before


@pytest.mark.parametrize("commit", [False, True])
def test_process_exit_preserves_only_pages_committed_with_the_cursor(tmp_path, commit):
    raw = source(3 * PAGE, journal_id=JOURNAL)
    with retained(tmp_path, raw) as (engine, factory):
        work = owner._read_upload_journal_capture(factory, 1, JOURNAL)
        assert owner._commit_upload_journal_capture(factory, work, advance(work))
        before = checkpoint(factory), pages(factory)
        code = """
import os,sys
from contextlib import contextmanager
from riverhog_core.catalog_db import make_session_factory
from riverhog_core.services import collection_uploads as owner
from tests.unit.test_journal_capture import advance
factory=make_session_factory(sys.argv[1])
work=owner._read_upload_journal_capture(factory,1,sys.argv[2])
original=owner.session_scope
@contextmanager
def interrupted(factory):
    with original(factory) as session:
        yield session
        session.flush()
        os._exit(73)
if sys.argv[3]=="False": owner.session_scope=interrupted
assert owner._commit_upload_journal_capture(factory,work,advance(work))
os._exit(73)
"""
        completed = subprocess.run(
            [sys.executable, "-c", code, str(engine.url), JOURNAL, str(commit)],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert completed.returncode == 73, completed.stderr
        engine.dispose()
        if not commit:
            assert (checkpoint(factory), pages(factory)) == before
        else:
            assert checkpoint(factory)[1] > before[0][1]
        assert_prefix(factory, raw)
        resumed = owner._read_upload_journal_capture(factory, 1, JOURNAL)
        assert resumed.cursor.byte_offset == checkpoint(factory)[1]


class ObservedResult:
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
            self.counts["loaded_segment_octets"] += len(row.content)
        return row


@pytest.mark.growth_witness(
    "riverhog-journal-capture-segment-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.collection_uploads._read_upload_journal_capture_segment",
)
def test_actual_fenced_source_page_work(tmp_path, monkeypatch, record_property):
    from contextlib import contextmanager

    binding = growth_binding(owner._read_upload_journal_capture_segment)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    samples = []
    for count, final in [(1, 37), (2, 37), (17, 37), (129, 37), (2, PAGE), (17, PAGE), (129, PAGE)]:
        raw = b"p" * ((count - 1) * PAGE + final)
        with retained(tmp_path / f"{count}-{final}", raw) as (engine, factory):
            with factory() as session, session.begin():
                record = session.get(Journal, (1, JOURNAL))
                record.capture_phase = "deferred"
                record.capture_failure = "source fixture has no semantic acceptance"
                record.capture_byte_offset = len(raw)
                session.add_all(
                    [
                        Segment(
                            collection_id=1,
                            journal_id=JOURNAL,
                            directory_id=record.capture_directory_id,
                            ordinal=n,
                            content=raw[n * PAGE : (n + 1) * PAGE],
                            sha256=hashlib.sha256(raw[n * PAGE : (n + 1) * PAGE]).hexdigest(),
                        )
                        for n in range(count)
                    ]
                )
            work = owner._read_upload_journal_validation(factory, 1, JOURNAL)
            counts = {
                "source_rows": 0,
                "loaded_segment_octets": 0,
                "hashed_octets": 0,
                "closed": False,
            }
            snapshot = owner.read_snapshot
            sha256 = owner.hashlib.sha256

            @contextmanager
            def observed_snapshot(factory, base=snapshot, observed=counts):
                with base(factory) as session:
                    execute = session.execute

                    def observed_execute(*args, **kwargs):
                        return ObservedResult(execute(*args, **kwargs), observed)

                    monkeypatch.setattr(session, "execute", observed_execute)
                    yield session

            def observed_hash(value, base=sha256, observed=counts):
                assert engine.pool.checkedout() == 0
                observed["hashed_octets"] += len(value)
                return base(value)

            with engine.connect() as connection:
                native = connection.connection.driver_connection
                assert isinstance(native, sqlite3.Connection)
            statements, instructions = [], [0]

            def sql(
                _connection, _cursor, statement, parameters, _context, _many, output=statements
            ):
                output.append((statement, parameters))

            def progress(counter=instructions):
                counter[0] += 1
                return 0

            event.listen(engine, "before_cursor_execute", sql)
            native.set_progress_handler(progress, 1)
            try:
                with monkeypatch.context() as patch:
                    patch.setattr(owner, "read_snapshot", observed_snapshot)
                    patch.setattr(owner.hashlib, "sha256", observed_hash)
                    actual = owner._read_upload_journal_capture_segment(
                        factory, work, ordinal=count - 1
                    )
            finally:
                native.set_progress_handler(None, 0)
                event.remove(engine, "before_cursor_execute", sql)
            assert actual == b"p" * final and counts["closed"] and engine.pool.checkedout() == 0
            for statement, parameters in statements:
                with engine.connect() as connection:
                    result = connection.exec_driver_sql(
                        "EXPLAIN QUERY PLAN " + statement, parameters
                    )
                    try:
                        plan = " ".join(row[3] for row in result)
                    finally:
                        result.close()
                assert "SEARCH" in plan and "SCAN" not in plan and "USE TEMP" not in plan
            sample = {
                "declaration": binding.declaration.id,
                "declaration_sha256": digest,
                "dimensions": {"captured_segments": count},
                "metrics": {
                    "sql_statements": len(statements),
                    **{key: value for key, value in counts.items() if key != "closed"},
                    "returned_octets": len(actual),
                    "sqlite_vm_instructions": instructions[0],
                },
                "delegates": {},
            }
            validate_execution_samples(projection, binding.declaration.id, [sample])
            samples.append(sample)
    record_property("growth_samples", samples)
