from __future__ import annotations

import sqlite3
import weakref
from contextlib import contextmanager

import pytest
from riverhog_age import CHUNK_SIZE
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord, CollectionUploadRecord
from riverhog_core.services.collection_uploads import _record_payload_custody_progress
from sqlalchemy import event

from tests.unit.test_collection_payload_seal_accounting_work import (
    counters,
    seal_all_but,
    sealed_input,
    volume_ids,
)
from tests.unit.test_collection_upload_raw_readiness_work import raw_parts_fixture

_NOW = "2026-01-01T00:00:00Z"


@contextmanager
def coverage_cursors(engine):
    cursors = []

    def capture(_connection, cursor, statement, _parameters, _context, _many):
        if (
            statement.startswith("SELECT")
            and "JOIN collection_upload_artifact_volumes" in statement
        ):
            cursors.append(cursor)

    event.listen(engine, "after_cursor_execute", capture)
    try:
        yield cursors
    finally:
        event.remove(engine, "after_cursor_execute", capture)


def assert_cursors_closed(cursors):
    assert len(cursors) == 1
    for cursor in cursors:
        with pytest.raises(sqlite3.ProgrammingError):
            cursor.fetchone()


@pytest.mark.parametrize("parts", [1, 16, 17, 64, 65, 129, 1000])
def test_final_payload_verification_bounds_live_archive_records(tmp_path, parts, record_property):
    service, _config, collection_id, _policy, artifact_id, _digests = raw_parts_fixture(
        tmp_path, parts
    )
    factory = service._session_factory
    ids = volume_ids(factory, collection_id)
    seal_all_but(factory, collection_id, None)
    with session_scope(factory) as session:
        upload = session.get(CollectionUploadRecord, collection_id)
        current = session.get(CollectionArchiveObjectUploadRecord, (collection_id, ids[0]))
        live = weakref.WeakSet([current])
        work = {"peak_live_archive_records": 1, "archive_records_loaded": 0}

        def loaded(_session, row):
            if isinstance(row, CollectionArchiveObjectUploadRecord):
                live.add(row)
                work["archive_records_loaded"] += 1
                work["peak_live_archive_records"] = max(
                    work["peak_live_archive_records"], len(live)
                )

        event.listen(session, "loaded_as_persistent", loaded)
        try:
            with coverage_cursors(factory.kw["bind"]) as cursors:
                _record_payload_custody_progress(session, upload, current, now=_NOW)
                assert_cursors_closed(cursors)
        finally:
            event.remove(session, "loaded_as_persistent", loaded)
        assert upload.payload_sealed_artifact_count == 1
        assert upload.payload_sealed_artifact_bytes == parts * CHUNK_SIZE
        # Permit two fixed result windows plus the caller's retained input.
        assert work["peak_live_archive_records"] <= 130, (parts, work)
        assert len(live) <= 2
        assert work["archive_records_loaded"] <= parts + 16
    assert counters(factory, collection_id, artifact_id) == (1, parts * CHUNK_SIZE, _NOW)
    record_property("source_volumes", parts)
    record_property("peak_live_archive_records", work["peak_live_archive_records"])
    record_property("archive_records_loaded", work["archive_records_loaded"])


@pytest.mark.parametrize("position", [0, -1])
def test_rejected_final_payload_verification_closes_its_cursor(tmp_path, position):
    service, _config, collection_id, _policy, artifact_id, _digests = raw_parts_fixture(
        tmp_path, 129
    )
    factory = service._session_factory
    ids = volume_ids(factory, collection_id)
    bad_id = sorted(ids)[position]
    current_id = next(object_id for object_id in ids if object_id != bad_id)
    seal_all_but(factory, collection_id, bad_id)
    with session_scope(factory) as session:
        # Force the complete coverage authority to reject a damaged projection.
        row = session.get(CollectionArchiveObjectUploadRecord, (collection_id, bad_id))
        row.source_artifact_id = "b" * 64
    with session_scope(factory) as session:
        upload = session.get(CollectionUploadRecord, collection_id)
        current = session.get(CollectionArchiveObjectUploadRecord, (collection_id, current_id))
        with coverage_cursors(factory.kw["bind"]) as cursors:
            _record_payload_custody_progress(session, upload, current, now=_NOW)
            assert_cursors_closed(cursors)
    assert counters(factory, collection_id, artifact_id) == (0, 0, None)


def test_interrupted_final_payload_verification_rolls_back_and_closes_its_cursor(tmp_path):
    service, _config, collection_id, _policy, artifact_id, _digests = raw_parts_fixture(
        tmp_path, 129
    )
    factory = service._session_factory
    ids = volume_ids(factory, collection_id)
    seal_all_but(factory, collection_id, ids[-1])
    with factory() as session:
        upload = session.get(CollectionUploadRecord, collection_id)
        current = session.get(CollectionArchiveObjectUploadRecord, (collection_id, ids[-1]))
        sealed_input(current)
        loaded_count = 0

        def interrupt(_session, row):
            nonlocal loaded_count
            if isinstance(row, CollectionArchiveObjectUploadRecord):
                loaded_count += 1
                if loaded_count == 8:
                    raise RuntimeError("injected coverage verification interruption")

        event.listen(session, "loaded_as_persistent", interrupt)
        try:
            with coverage_cursors(factory.kw["bind"]) as cursors:
                with pytest.raises(RuntimeError, match="injected coverage verification"):
                    _record_payload_custody_progress(session, upload, current, now=_NOW)
                assert_cursors_closed(cursors)
        finally:
            event.remove(session, "loaded_as_persistent", interrupt)
            session.rollback()
    assert counters(factory, collection_id, artifact_id) == (0, 0, None)
    with session_scope(factory) as session:
        current = session.get(CollectionArchiveObjectUploadRecord, (collection_id, ids[-1]))
        assert current.state == "planned"
        assert current.sealed_receipt_json is None
        sealed_input(current)
        _record_payload_custody_progress(
            session, session.get(CollectionUploadRecord, collection_id), current, now=_NOW
        )
    assert counters(factory, collection_id, artifact_id) == (1, 129 * CHUNK_SIZE, _NOW)
