from __future__ import annotations

import hashlib
import sqlite3
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.catalog_db import Base, create_catalog_engine
from riverhog_core.catalog_models import (
    CollectionUploadProvenanceJournalChunkRecord as Chunk,
)
from riverhog_core.catalog_models import (
    CollectionUploadProvenanceJournalRecord as Journal,
)
from riverhog_core.catalog_models import (
    CollectionUploadRecord,
    StorageIncarnationRecord,
)
from riverhog_core.services import collection_uploads as owner
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_protocol import COLLECTION_UPLOAD_PROVENANCE_APPEND_BYTES_MAX as EXTENT
from sqlalchemy import delete, event, update
from sqlalchemy.orm import Session

from tests.unit.db_helpers import sqlite_url

JOURNAL = "urn:uuid:11111111-1111-4111-8111-111111111111"
INCARNATION = "22222222-2222-4222-8222-222222222222"


def chunk_bytes(ordinal, size):
    return bytes([ordinal % 251]) * size


@contextmanager
def staged_transport(root, count, last_octets=EXTENT, *, first_octets=EXTENT):
    """Native contiguous transport bytes; semantic journal acceptance is separate."""
    engine = create_catalog_engine(sqlite_url(root / "ranges.db"))
    try:
        Base.metadata.create_all(engine)
        total = (count - 2) * EXTENT + first_octets + last_octets if count > 1 else last_octets
        with Session(engine) as session, session.begin():
            session.add(
                StorageIncarnationRecord(
                    id=INCARNATION,
                    kind="archive",
                    name="archive",
                    state="bound",
                    created_at="2026-01-01T00:00:00Z",
                )
            )
            session.flush()
            session.add(
                CollectionUploadRecord(
                    collection_id=1,
                    idempotency_key="ranges",
                    creation_identity_sha256="a" * 64,
                    initial_tag_set_identity="b" * 64,
                    encryption_format="age-v1-scrypt",
                    passphrase_id="test-key",
                    archive_store="archive",
                    archive_incarnation_id=INCARNATION,
                    opened_at="2026-01-01T00:00:00Z",
                    last_activity_at="2026-01-01T00:00:00Z",
                    archive_phase_updated_at="2026-01-01T00:00:00Z",
                    archive_storage_prefix="collections/1",
                    planner_checkpoint_json="{}",
                )
            )
            session.flush()
            journal = Journal(
                collection_id=1,
                journal_id=JOURNAL,
                bytes=total,
                sha256="c" * 64,
                accepted_bytes=total,
                next_chunk_ordinal=count,
                content_hash_state="{}",
            )
            session.add(journal)
            session.flush()
            digest = hashlib.sha256()
            offset = 0
            for ordinal in range(count):
                width = (
                    last_octets
                    if ordinal == count - 1
                    else first_octets
                    if ordinal == 0
                    else EXTENT
                )
                raw = chunk_bytes(ordinal, width)
                digest.update(raw)
                session.add(
                    Chunk(
                        collection_id=1,
                        journal_id=JOURNAL,
                        ordinal=ordinal,
                        byte_offset=offset,
                        content=raw,
                    )
                )
                session.flush()
                offset += len(raw)
            journal.sha256 = digest.hexdigest()
        yield engine, total
    finally:
        engine.dispose()


def expected_range(offset, size, *, first_octets=EXTENT):
    pieces = []
    while size:
        if offset < first_octets:
            ordinal, start, width = 0, offset, first_octets
        else:
            ordinal, start = divmod(offset - first_octets, EXTENT)
            ordinal, width = ordinal + 1, EXTENT
        take = min(size, width - start)
        pieces.append(chunk_bytes(ordinal, take))
        offset += take
        size -= take
    return b"".join(pieces)


class ObservedResult:
    def __init__(self, actual, counts):
        self.actual, self.counts = actual, counts

    def __enter__(self):
        self.actual.__enter__()
        return self

    def __exit__(self, *args):
        try:
            return self.actual.__exit__(*args)
        finally:
            self.counts["closed"] = self.actual.closed

    def __iter__(self):
        for row in self.actual:
            self.counts["returned_rows"] += 1
            self.counts["loaded_chunk_octets"] += len(row.content)
            yield row


def observe_execute(session, monkeypatch):
    counts = {"sql_statements": 0, "returned_rows": 0, "loaded_chunk_octets": 0, "closed": False}
    execute = session.execute

    def observed(*args, **kwargs):
        counts["sql_statements"] += 1
        return ObservedResult(execute(*args, **kwargs), counts)

    monkeypatch.setattr(session, "execute", observed)
    return counts


@pytest.mark.parametrize(
    "offset,size",
    [
        (0, 1),
        (0, EXTENT),
        (EXTENT - 1, 2),
        (EXTENT + 19, 37),
        (2 * EXTENT - 13, 50),
        (2 * EXTENT, 37),
        (2 * EXTENT + 36, 1),
    ],
)
def test_exact_ranges_cross_transport_boundaries_and_close(tmp_path, monkeypatch, offset, size):
    with staged_transport(tmp_path, 3, last_octets=37) as (engine, _), Session(engine) as session:
        counts = observe_execute(session, monkeypatch)
        actual = owner._upload_journal_range_bytes(session, 1, JOURNAL, offset=offset, size=size)
        assert actual == expected_range(offset, size)
        overlap = (offset + size - 1) // EXTENT - offset // EXTENT + 1
        assert counts["closed"] is True and counts["sql_statements"] == (1 if overlap == 1 else 2)
        assert counts["returned_rows"] == overlap


@pytest.mark.parametrize("alteration", ["missing", "offset", "oversized", "short", "ordinal"])
def test_corrupt_selected_range_fails_and_closes(tmp_path, monkeypatch, alteration):
    with staged_transport(tmp_path, 3) as (engine, _):
        with Session(engine) as session, session.begin():
            selected = (Chunk.collection_id == 1, Chunk.journal_id == JOURNAL, Chunk.ordinal == 1)
            if alteration == "missing":
                session.execute(delete(Chunk).where(*selected))
            elif alteration == "offset":
                session.execute(update(Chunk).where(*selected).values(byte_offset=EXTENT + 1))
            elif alteration == "oversized":
                session.execute(update(Chunk).where(*selected).values(content=b"x" * (EXTENT + 1)))
            elif alteration == "short":
                session.execute(update(Chunk).where(*selected).values(content=b"x"))
            else:
                session.execute(update(Chunk).where(*selected).values(ordinal=7))
        with Session(engine) as session:
            counts = observe_execute(session, monkeypatch)
            with pytest.raises(RuntimeError, match="not contiguous|unavailable"):
                owner._upload_journal_range_bytes(
                    session, 1, JOURNAL, offset=EXTENT - 1, size=EXTENT + 2
                )
            assert counts["closed"] is True


@pytest.mark.parametrize(
    "offset,size", [(-1, 1), (True, 1), (1.5, 1), (0, False), (0, 0), (0, 8 * EXTENT + 1)]
)
def test_invalid_range_never_opens_a_query(offset, size):
    def forbidden(*args, **kwargs):
        raise AssertionError("invalid request reached the database")

    with pytest.raises(RuntimeError, match="range is invalid"):
        owner._upload_journal_range_bytes(
            SimpleNamespace(execute=forbidden), 1, JOURNAL, offset=offset, size=size
        )


def test_tail_overrun_is_an_error_and_reopen_resumes_exact_range(tmp_path, monkeypatch):
    with staged_transport(tmp_path, 3, last_octets=37) as (engine, total):
        with Session(engine) as session:
            counts = observe_execute(session, monkeypatch)
            with pytest.raises(RuntimeError, match="unavailable"):
                owner._upload_journal_range_bytes(session, 1, JOURNAL, offset=total - 1, size=2)
            assert counts["closed"] is True
        engine.dispose()
        with Session(engine) as session:
            assert owner._upload_journal_range_bytes(
                session, 1, JOURNAL, offset=2 * EXTENT, size=37
            ) == expected_range(2 * EXTENT, 37)


@pytest.mark.growth_witness(
    "riverhog-staged-journal-range-indexed-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.collection_uploads._upload_journal_range_bytes",
)
def test_native_indexed_range_records_actual_work(tmp_path, monkeypatch, record_property):
    binding = growth_binding(owner._upload_journal_range_bytes)
    projection = project_bindings((binding,))
    declaration = projection["declarations"][binding.declaration.id]
    samples = []
    for count, first_octets, offset, size in (
        (2, EXTENT, EXTENT + 11, 37),
        (17, EXTENT, 16 * EXTENT + 11, 37),
        (129, EXTENT, 128 * EXTENT + 11, 37),
        (17, EXTENT, EXTENT - 13, 8 * EXTENT),
        (17, EXTENT, 7 * EXTENT, 8 * EXTENT),
        (2, 37, 37 + 11, 37),
        (17, 37, 37 + 15 * EXTENT + 11, 37),
        (129, 37, 37 + 127 * EXTENT + 11, 37),
    ):
        with (
            staged_transport(
                tmp_path / f"{count}-{first_octets}-{offset}-{size}",
                count,
                first_octets=first_octets,
            ) as (engine, _),
            Session(engine) as session,
        ):
            counts = observe_execute(session, monkeypatch)
            connection = session.connection().connection.driver_connection
            assert isinstance(connection, sqlite3.Connection)
            sql = []

            def statement(
                _connection, _cursor, text, parameters, _context, _many, *, observations=sql
            ):
                observations.append((text, parameters))

            event.listen(engine, "before_cursor_execute", statement)
            instructions = [0]

            def progress(counter=instructions):
                counter[0] += 1
                return 0

            buffers = []

            class Buffer(bytearray):
                def __init__(self, recorder=buffers):
                    super().__init__()
                    self.peak = 0
                    recorder.append(self)

                def extend(self, raw):
                    super().extend(raw)
                    self.peak = max(self.peak, len(self))

            connection.set_progress_handler(progress, 1)
            try:
                with monkeypatch.context() as patch:
                    patch.setattr(owner, "bytearray", Buffer, raising=False)
                    actual = owner._upload_journal_range_bytes(
                        session, 1, JOURNAL, offset=offset, size=size
                    )
            finally:
                connection.set_progress_handler(None, 0)
                event.remove(engine, "before_cursor_execute", statement)
            assert actual == expected_range(offset, size, first_octets=first_octets)
            assert counts["closed"] is True and len(buffers) == 1
            for text, parameters in sql:
                cursor = connection.execute("EXPLAIN QUERY PLAN " + text, parameters)
                try:
                    plan = " ".join(row[3] for row in cursor)
                finally:
                    cursor.close()
                assert "SEARCH" in plan and "byte_offset" in plan and "USE TEMP" not in plan

            def source_ordinal(position, first_octets=first_octets):
                return 0 if position < first_octets else 1 + (position - first_octets) // EXTENT

            overlap = source_ordinal(offset + size - 1) - source_ordinal(offset) + 1
            assert len(sql) == (1 if overlap == 1 else 2)
            sample = {
                "declaration": binding.declaration.id,
                "declaration_sha256": hashlib.sha256(canonical_json_bytes(declaration)).hexdigest(),
                "dimensions": {
                    "range_octets": size,
                    "overlap_chunks": overlap,
                    "staged_chunks": count,
                },
                "metrics": {
                    "sql_statements": counts["sql_statements"],
                    "returned_rows": counts["returned_rows"],
                    "loaded_chunk_octets": counts["loaded_chunk_octets"],
                    "range_buffer_octets": buffers[0].peak,
                    "returned_octets": len(actual),
                    "sqlite_vm_instructions": instructions[0],
                },
                "delegates": {},
            }
            validate_execution_samples(projection, binding.declaration.id, [sample])
            samples.append(sample)
    record_property("growth_samples", samples)
