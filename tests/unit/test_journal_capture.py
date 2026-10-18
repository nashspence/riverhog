from __future__ import annotations

import hashlib
import subprocess
import sys
from contextlib import contextmanager
from dataclasses import fields

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.catalog_db import initialize_db, validate_db
from riverhog_core.catalog_models import CollectionUploadProvenanceFrameRecord as Frame
from riverhog_core.services import collection_uploads as owner
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import (
    JournalFramingCursor,
    ProvenanceValidationError,
    advance_journal_framing,
    framing,
    parse_journal,
    validate_journal,
)
from sqlalchemy import delete, event, func, select, update
from sqlalchemy.orm import Session, sessionmaker

from tests.unit.db_helpers import sqlite_url
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_staged_journal_range import (
    EXTENT,
    JOURNAL,
    Chunk,
    Journal,
    observe_execute,
    staged_transport,
)


def advance(work):
    return advance_journal_framing(
        work.cursor,
        work.content,
        byte_budget=owner._JOURNAL_CAPTURE_OCTET_BUDGET,
        frame_budget=owner._JOURNAL_CAPTURE_FRAME_BUDGET,
        at_end=work.cursor.byte_offset + len(work.content) == work.source_bytes,
    )


@contextmanager
def retained(root, raw):
    root.mkdir(parents=True, exist_ok=True)
    initialize_db(sqlite_url(root / "ranges.db"))
    count = (len(raw) + EXTENT - 1) // EXTENT
    with staged_transport(root, count, len(raw) - (count - 1) * EXTENT) as (engine, _):
        factory = sessionmaker(engine)
        with Session(engine) as session, session.begin():
            record = session.get(Journal, (1, JOURNAL))
            record.state = "validating"
            record.sha256 = hashlib.sha256(raw).hexdigest()
            owner._begin_upload_journal_capture(session, record)
            for ordinal in range(count):
                session.get(Chunk, (1, JOURNAL, ordinal)).content = raw[
                    ordinal * EXTENT : (ordinal + 1) * EXTENT
                ]
        yield engine, factory


def checkpoint(factory):
    with factory() as session:
        record = session.get(Journal, (1, JOURNAL))
        return (
            record.capture_version,
            record.capture_byte_offset,
            record.capture_frame_count,
            record.validation_byte_offset,
            record.state,
            record.capture_phase,
            session.scalar(select(func.count()).select_from(Frame)),
        )


@pytest.mark.parametrize("byte_budget", [1, 2, 7, 127, 4096])
@pytest.mark.parametrize("frame_budget", [1, 3, 64])
def test_native_framing_positions_survive_hostile_boundaries(byte_budget, frame_budget):
    raw = source(257)
    cursor = JournalFramingCursor()
    positions = []
    while True:
        result = advance_journal_framing(
            cursor,
            raw[cursor.byte_offset :],
            byte_budget=byte_budget,
            frame_budget=frame_budget,
            at_end=True,
        )
        assert result.failure is None
        assert 0 < result.cursor.byte_offset - cursor.byte_offset <= byte_budget
        assert len(result.positions) <= frame_budget
        positions.extend(result.positions)
        cursor = result.cursor
        if result.complete:
            break
    native = parse_journal(raw)
    assert cursor.frame_count == len(native)
    for position, expected in zip(positions, native, strict=True):
        assert (
            raw[position.byte_offset : position.byte_offset + position.json_bytes + 2]
            == expected.encoded
        )


@pytest.mark.parametrize(
    "raw,failure",
    [
        (b"", "journal is empty"),
        (b"x", "bytes outside RFC 7464 frames"),
        (b"\x1ex\x1e", "unframed record separator before line feed"),
        (b"\x1ex", "incomplete final journal frame"),
        (b"\x1e12345\n", "journal entry exceeds the admission byte limit"),
    ],
)
def test_speculative_framing_failure_is_not_semantic_acceptance(raw, failure):
    result = advance_journal_framing(
        JournalFramingCursor(),
        raw,
        byte_budget=99,
        frame_budget=9,
        at_end=True,
        max_entry_bytes=4,
    )
    assert result.failure == failure and not result.complete


def test_entry_limit_boundary_and_incomplete_frame_resume():
    result = advance_journal_framing(
        JournalFramingCursor(),
        b"\x1e1234\n",
        byte_budget=5,
        frame_budget=1,
        at_end=True,
        max_entry_bytes=4,
    )
    assert result.failure is None and not result.complete
    result = advance_journal_framing(
        result.cursor,
        b"\n",
        byte_budget=1,
        frame_budget=1,
        at_end=True,
        max_entry_bytes=4,
    )
    assert result.complete and result.positions[0].json_bytes == 4


@pytest.mark.parametrize(
    "kwargs",
    [
        {"byte_budget": 0},
        {"byte_budget": True},
        {"frame_budget": 0},
        {"max_entry_bytes": False},
        {"at_end": 1},
    ],
)
def test_invalid_framing_budgets_fail_before_scanning(kwargs, monkeypatch):
    def forbidden(_):
        raise AssertionError("invalid input was scanned")

    monkeypatch.setattr(framing, "_octets", forbidden)
    options = {"byte_budget": 1, "frame_budget": 1, "at_end": True, **kwargs}
    with pytest.raises((TypeError, ValueError)):
        advance_journal_framing(JournalFramingCursor(), b"x", **options)


def test_durable_capture_reopens_without_replaying_the_committed_prefix(tmp_path):
    raw = source(200000, journal_id=JOURNAL)
    with retained(tmp_path, raw) as (engine, factory):
        work = owner._read_upload_journal_capture(factory, 1, JOURNAL)
        assert owner._commit_upload_journal_capture(factory, work, advance(work))
        saved = checkpoint(factory)
        assert saved[1] == owner._JOURNAL_CAPTURE_OCTET_BUDGET and saved[3] == 0
        engine.dispose()
        assert validate_db(str(engine.url)).condition == "current"
        assert initialize_db(str(engine.url)).condition == "current"
        assert checkpoint(factory) == saved
        work = owner._read_upload_journal_capture(factory, 1, JOURNAL)
        assert work.cursor.byte_offset == saved[1]
        assert work.content == raw[saved[1] : saved[1] + owner._JOURNAL_CAPTURE_OCTET_BUDGET]
        while work is not None:
            assert owner._commit_upload_journal_capture(factory, work, advance(work))
            work = owner._read_upload_journal_capture(factory, 1, JOURNAL)
        saved = checkpoint(factory)
        assert saved[1] == len(raw) and saved[3:6] == (0, "validating", "complete")
        assert saved[2] == saved[6] == len(parse_journal(raw))
        service = object.__new__(owner.SqlAlchemyCollectionUploadService)
        service._session_factory = factory
        status = service.seal_provenance_journal(1, JOURNAL)
        with validate_journal(raw) as expected:
            assert status["state"] == "sealed" and status["anchor"] == expected.anchor


@pytest.mark.parametrize(
    "fence, replacement",
    [
        ("capture_attempt_id", "replacement-worker"),
        ("capture_algorithm", "unknown-algorithm"),
        ("capture_directory_id", "replacement-directory"),
        ("sha256", "d" * 64),
        ("capture_version", 2),
    ],
)
def test_stale_source_attempt_algorithm_and_version_cannot_publish(tmp_path, fence, replacement):
    raw = source(journal_id=JOURNAL)
    with retained(tmp_path, raw) as (_, factory):
        work = owner._read_upload_journal_capture(factory, 1, JOURNAL)
        with factory() as session, session.begin():
            session.execute(update(Journal).values({fence: replacement}))
        before = checkpoint(factory)
        assert not owner._commit_upload_journal_capture(factory, work, advance(work))
        assert checkpoint(factory) == before


def test_duplicate_computed_work_cannot_duplicate_directory_rows(tmp_path):
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        work = owner._read_upload_journal_capture(factory, 1, JOURNAL)
        result = advance(work)
        assert owner._commit_upload_journal_capture(factory, work, result)
        before = checkpoint(factory)
        assert not owner._commit_upload_journal_capture(factory, work, result)
        assert checkpoint(factory) == before


def test_repeated_initialization_retains_the_current_attempt_and_progress(tmp_path):
    with retained(tmp_path, source(90000, journal_id=JOURNAL)) as (_, factory):
        work = owner._read_upload_journal_capture(factory, 1, JOURNAL)
        assert owner._commit_upload_journal_capture(factory, work, advance(work))
        before = checkpoint(factory)
        with factory() as session, session.begin():
            record = session.get(Journal, (1, JOURNAL))
            owner._begin_upload_journal_capture(session, record)
            assert record.capture_attempt_id == work.attempt_id
        assert checkpoint(factory) == before


def test_directory_write_failure_rolls_back_the_cursor_together(tmp_path):
    with retained(tmp_path, source(journal_id=JOURNAL)) as (engine, factory):
        work = owner._read_upload_journal_capture(factory, 1, JOURNAL)
        before = checkpoint(factory)

        def failed(_connection, _cursor, statement, _parameters, _context, _many):
            if statement.startswith("INSERT INTO collection_upload_provenance_frames"):
                raise OSError("capture directory unavailable")

        event.listen(engine, "before_cursor_execute", failed)
        try:
            with pytest.raises(OSError, match="directory unavailable"):
                owner._commit_upload_journal_capture(factory, work, advance(work))
        finally:
            event.remove(engine, "before_cursor_execute", failed)
        assert checkpoint(factory) == before
        assert owner._commit_upload_journal_capture(factory, work, advance(work))


def test_missing_checkpoint_manifest_link_fails_closed_on_reopen(tmp_path):
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        work = owner._read_upload_journal_capture(factory, 1, JOURNAL)
        assert owner._commit_upload_journal_capture(factory, work, advance(work))
        assert checkpoint(factory)[6] > 0
        with factory() as session, session.begin():
            session.execute(delete(Frame))
        with pytest.raises(RuntimeError, match="missing frame position"):
            owner._read_upload_journal_capture(factory, 1, JOURNAL)
        assert checkpoint(factory)[3:5] == (0, "validating")


@pytest.mark.parametrize("commit", [False, True])
def test_real_process_exit_preserves_only_committed_cursor_and_directory(tmp_path, commit):
    with retained(tmp_path, source(90000, journal_id=JOURNAL)) as (engine, factory):
        work = owner._read_upload_journal_capture(factory, 1, JOURNAL)
        assert owner._commit_upload_journal_capture(factory, work, advance(work))
        before = checkpoint(factory)
        code = """
import os, sys
from contextlib import contextmanager
from dataclasses import fields
from riverhog_core.catalog_db import make_session_factory
from riverhog_core.services import collection_uploads as owner
from riverhog_provenance import advance_journal_framing
factory = make_session_factory(sys.argv[1])
work = owner._read_upload_journal_capture(factory, 1, sys.argv[2])
result = advance_journal_framing(work.cursor, work.content,
    byte_budget=owner._JOURNAL_CAPTURE_OCTET_BUDGET,
    frame_budget=owner._JOURNAL_CAPTURE_FRAME_BUDGET,
    at_end=work.cursor.byte_offset + len(work.content) == work.source_bytes)
original = owner.session_scope
@contextmanager
def interrupted(factory):
    with original(factory) as session:
        yield session
        os._exit(73)
if sys.argv[3] == "False":
    owner.session_scope = interrupted
assert owner._commit_upload_journal_capture(factory, work, result)
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
        after = checkpoint(factory)
        if commit:
            assert after[0] == before[0] + 1 and after[1] > before[1]
            assert after[2] == after[6] and after[3] == 0
        else:
            assert after == before
        resumed = owner._read_upload_journal_capture(factory, 1, JOURNAL)
        assert resumed is None or resumed.cursor.byte_offset == after[1]


def test_earlier_native_error_precedes_later_speculative_framing_failure(tmp_path):
    raw = source(100000, journal_id=JOURNAL)
    frames = parse_journal(raw)
    first = frames[0].document
    first["sequence"] = "1"
    raw = (
        b"\x1e"
        + canonical_json_bytes(first)
        + b"\n"
        + b"".join(f.encoded for f in frames[1:])
        + b"outside"
    )
    with pytest.raises(ProvenanceValidationError) as expected:
        validate_journal(raw)
    with retained(tmp_path, raw) as (_, factory):
        service = object.__new__(owner.SqlAlchemyCollectionUploadService)
        service._session_factory = factory
        while True:
            status = service.seal_provenance_journal(1, JOURNAL)
            if status["state"] != "validating":
                break
            assert status["anchor"] is None
        assert status["state"] == "failed"
        assert status["failure"] == str(expected.value)
        assert status["anchor"] is None
        assert checkpoint(factory)[3] == 0


def test_capture_loads_and_commits_only_admitted_rows_and_closes_readers(tmp_path, monkeypatch):
    raw = b"\x1e{}\n" * 20000
    with retained(tmp_path, raw) as (engine, factory):
        observations = []
        actual = owner._upload_journal_chunk_at

        def observed(session, *args, **kwargs):
            with monkeypatch.context() as patch:
                counts = observe_execute(session, patch)
                result = actual(session, *args, **kwargs)
            assert counts["closed"] is True
            observations.append(counts)
            return result

        monkeypatch.setattr(owner, "_upload_journal_chunk_at", observed)
        work = owner._read_upload_journal_capture(factory, 1, JOURNAL)
        assert engine.pool.checkedout() == 0
        assert len(work.content) <= owner._JOURNAL_CAPTURE_OCTET_BUDGET
        assert observations[0]["returned_rows"] == 1
        writes = []

        def sql(_connection, _cursor, statement, parameters, _context, many):
            if statement.startswith("INSERT INTO collection_upload_provenance_frames"):
                writes.append(len(parameters) if many else 1)

        event.listen(engine, "before_cursor_execute", sql)
        try:
            result = advance(work)
            assert len(result.positions) == owner._JOURNAL_CAPTURE_FRAME_BUDGET
            assert owner._commit_upload_journal_capture(factory, work, result)
        finally:
            event.remove(engine, "before_cursor_execute", sql)
        assert sum(writes) == owner._JOURNAL_CAPTURE_FRAME_BUDGET
        assert checkpoint(factory)[3] == 0


@pytest.mark.growth_witness(
    "riverhog-journal-framing-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.framing.advance_journal_framing",
)
def test_native_framing_records_actual_octet_and_position_work(monkeypatch, record_property):
    binding = growth_binding(advance_journal_framing)
    projection = project_bindings((binding,))
    declaration = projection["declarations"][binding.declaration.id]
    samples = []
    for content, byte_budget, frame_budget in [
        (source(0), 64, 1),
        (source(65536), 4096, 64),
        (b"\x1e{}\n" * 10000, 65536, 64),
        (b"\x1e" + b"x" * 65536, 65536, 64),
        (b"\x1e{}\nx", 64, 64),
    ]:
        visited = []
        original = framing._octets

        def observed(chunk, iterator=original, observations=visited):
            for position, octet in iterator(chunk):
                observations.append(position)
                yield position, octet

        with monkeypatch.context() as patch:
            patch.setattr(framing, "_octets", observed)
            result = advance_journal_framing(
                JournalFramingCursor(),
                content,
                byte_budget=byte_budget,
                frame_budget=frame_budget,
                at_end=True,
            )
        assert len(visited) == result.cursor.byte_offset
        retained_octets = sum(
            len(value)
            for field in fields(result.cursor)
            if isinstance(value := getattr(result.cursor, field.name), (bytes, bytearray))
        )
        sample = {
            "declaration": binding.declaration.id,
            "declaration_sha256": hashlib.sha256(canonical_json_bytes(declaration)).hexdigest(),
            "dimensions": {
                "consumed_octets": result.cursor.byte_offset,
                "byte_budget": byte_budget,
                "frame_budget": frame_budget,
            },
            "metrics": {
                "visited_octets": len(visited),
                "consumed_octets": result.cursor.byte_offset,
                "returned_positions": len(result.positions),
                "retained_content_octets": retained_octets,
            },
            "delegates": {},
        }
        validate_execution_samples(projection, binding.declaration.id, [sample])
        samples.append(sample)
    record_property("growth_samples", samples)
