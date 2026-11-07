from __future__ import annotations

from contextlib import contextmanager

import pytest
from riverhog_age import CHUNK_SIZE
from riverhog_core.catalog_db import make_session_factory, session_scope
from riverhog_core.catalog_models import (
    CollectionArchiveObjectUploadRecord,
    CollectionUploadArtifactRecord,
    CollectionUploadRecord,
)
from riverhog_core.services.collection_uploads import _record_payload_custody_progress
from sqlalchemy import event, select

from tests.unit.test_collection_upload_raw_readiness_work import raw_parts_fixture
from tests.unit.test_collection_uploads import _CREATOR

_NOW = "2026-01-01T00:00:00Z"


@contextmanager
def accounting_work(session):
    work = {"sqlite_vm_steps": 0, "archive_records_loaded": 0}
    raw = session.connection().connection.driver_connection

    def tick():
        work["sqlite_vm_steps"] += 1
        return 0

    def loaded(_session, row):
        if isinstance(row, CollectionArchiveObjectUploadRecord):
            work["archive_records_loaded"] += 1

    event.listen(session, "loaded_as_persistent", loaded)
    raw.set_progress_handler(tick, 1)
    try:
        yield work
    finally:
        raw.set_progress_handler(None, 0)
        event.remove(session, "loaded_as_persistent", loaded)


def volume_ids(factory, collection_id):
    with session_scope(factory) as session:
        return list(
            session.scalars(
                select(CollectionArchiveObjectUploadRecord.object_id)
                .where(CollectionArchiveObjectUploadRecord.collection_id == collection_id)
                .order_by(CollectionArchiveObjectUploadRecord.sequence)
            )
        )


def sealed_input(row):
    # Supply the accounting boundary's catalog input only. These synthetic
    # rows do not establish valid archive bytes or a custody receipt.
    row.state = "sealed"
    row.sealed_receipt_json = "{}"


def seal_all_but(factory, collection_id, excluded):
    with session_scope(factory) as session:
        for row in session.scalars(
            select(CollectionArchiveObjectUploadRecord).where(
                CollectionArchiveObjectUploadRecord.collection_id == collection_id
            )
        ):
            if row.object_id != excluded:
                sealed_input(row)


def counters(factory, collection_id, artifact_id):
    with session_scope(factory) as session:
        upload = session.get(CollectionUploadRecord, collection_id)
        artifact = session.get(CollectionUploadArtifactRecord, (collection_id, artifact_id))
        return (
            upload.payload_sealed_artifact_count,
            upload.payload_sealed_artifact_bytes,
            artifact.payload_sealed_at,
        )


@pytest.mark.parametrize("parts", [1, 16, 17, 129, 1000])
def test_complete_raw_payload_accounting_has_linear_catalog_work(tmp_path, parts, record_property):
    service, config, collection_id, _policy, artifact_id, _digests = raw_parts_fixture(
        tmp_path, parts
    )
    factory = service._session_factory
    ids = volume_ids(factory, collection_id)
    assert len(ids) == parts
    steps = loaded = 0
    for offset, object_id in enumerate(ids):
        with session_scope(factory) as session:
            upload = session.get(CollectionUploadRecord, collection_id)
            row = session.get(CollectionArchiveObjectUploadRecord, (collection_id, object_id))
            sealed_input(row)
            with accounting_work(session) as work:
                _record_payload_custody_progress(session, upload, row, now=_NOW)
            steps += work["sqlite_vm_steps"]
            loaded += work["archive_records_loaded"]
            expected = int(offset == parts - 1)
            assert upload.payload_sealed_artifact_count == expected
            assert upload.payload_sealed_artifact_bytes == expected * parts * CHUNK_SIZE
        # Allow bounded per-callback work and one complete source verification.
        # Checking every prefix also fails promptly on a repeated source scan.
        assert loaded <= 64 + 32 * (offset + 1) + 2 * parts, (parts, offset, loaded)
        assert steps <= 2500 + 1500 * (offset + 1) + 80 * parts, (parts, offset, steps)

    assert counters(factory, collection_id, artifact_id) == (1, parts * CHUNK_SIZE, _NOW)
    factory.kw["bind"].dispose()
    reopened = make_session_factory(config.database_url)
    with session_scope(reopened) as session:
        upload = session.get(CollectionUploadRecord, collection_id)
        row = session.get(CollectionArchiveObjectUploadRecord, (collection_id, ids[-1]))
        with accounting_work(session) as retry:
            _record_payload_custody_progress(session, upload, row, now="2026-01-01T00:00:01Z")
        assert retry["archive_records_loaded"] <= 16
        assert retry["sqlite_vm_steps"] <= 1200
    assert counters(reopened, collection_id, artifact_id) == (1, parts * CHUNK_SIZE, _NOW)
    record_property("source_volumes", parts)
    record_property("seal_callbacks", parts)
    record_property("archive_records_loaded", loaded)
    record_property("sqlite_vm_steps", steps)


@pytest.mark.parametrize("missing", [0, -1])
def test_sealed_volume_without_a_receipt_blocks_member_accounting(tmp_path, missing):
    service, _config, collection_id, _policy, artifact_id, _digests = raw_parts_fixture(
        tmp_path, 17
    )
    factory = service._session_factory
    ids = volume_ids(factory, collection_id)
    with session_scope(factory) as session:
        for object_id in ids:
            row = session.get(CollectionArchiveObjectUploadRecord, (collection_id, object_id))
            sealed_input(row)
        session.get(
            CollectionArchiveObjectUploadRecord, (collection_id, ids[missing])
        ).sealed_receipt_json = None
    with session_scope(factory) as session:
        upload = session.get(CollectionUploadRecord, collection_id)
        row = session.get(CollectionArchiveObjectUploadRecord, (collection_id, ids[1]))
        _record_payload_custody_progress(session, upload, row, now=_NOW)
    assert counters(factory, collection_id, artifact_id) == (0, 0, None)
    with session_scope(factory) as session:
        upload = session.get(CollectionUploadRecord, collection_id)
        row = session.get(CollectionArchiveObjectUploadRecord, (collection_id, ids[missing]))
        row.sealed_receipt_json = "{}"
        _record_payload_custody_progress(session, upload, row, now=_NOW)
    assert counters(factory, collection_id, artifact_id) == (1, 17 * CHUNK_SIZE, _NOW)


def test_pending_source_projection_does_not_replace_coverage_authority(tmp_path):
    service, _config, collection_id, _policy, artifact_id, _digests = raw_parts_fixture(
        tmp_path, 17
    )
    factory = service._session_factory
    ids = volume_ids(factory, collection_id)
    seal_all_but(factory, collection_id, ids[0])
    with session_scope(factory) as session:
        # A damaged source projection must not hide this unsealed coverage edge.
        row = session.get(CollectionArchiveObjectUploadRecord, (collection_id, ids[0]))
        row.source_artifact_id = "b" * 64
    with session_scope(factory) as session:
        upload = session.get(CollectionUploadRecord, collection_id)
        row = session.get(CollectionArchiveObjectUploadRecord, (collection_id, ids[-1]))
        _record_payload_custody_progress(session, upload, row, now=_NOW)
    assert counters(factory, collection_id, artifact_id) == (0, 0, None)
    with session_scope(factory) as session:
        upload = session.get(CollectionUploadRecord, collection_id)
        row = session.get(CollectionArchiveObjectUploadRecord, (collection_id, ids[0]))
        row.source_artifact_id = artifact_id
        sealed_input(row)
        _record_payload_custody_progress(session, upload, row, now=_NOW)
    assert counters(factory, collection_id, artifact_id) == (1, 17 * CHUNK_SIZE, _NOW)


def test_member_accounting_failure_rolls_back_the_current_volume_receipt(tmp_path):
    service, _config, collection_id, _policy, artifact_id, _digests = raw_parts_fixture(
        tmp_path, 17
    )
    factory = service._session_factory
    ids = volume_ids(factory, collection_id)
    seal_all_but(factory, collection_id, ids[-1])
    engine = factory.kw["bind"]

    def fail(_connection, _cursor, statement, _parameters, _context, _many):
        if statement.startswith("UPDATE collection_upload_artifacts SET payload_sealed_at="):
            raise RuntimeError("injected member accounting failure")

    event.listen(engine, "before_cursor_execute", fail)
    try:
        with pytest.raises(RuntimeError, match="injected member accounting failure"):
            with session_scope(factory) as session:
                upload = session.get(CollectionUploadRecord, collection_id)
                row = session.get(CollectionArchiveObjectUploadRecord, (collection_id, ids[-1]))
                sealed_input(row)
                _record_payload_custody_progress(session, upload, row, now=_NOW)
    finally:
        event.remove(engine, "before_cursor_execute", fail)
    assert counters(factory, collection_id, artifact_id) == (0, 0, None)
    with session_scope(factory) as session:
        row = session.get(CollectionArchiveObjectUploadRecord, (collection_id, ids[-1]))
        assert row.state == "planned"
        assert row.sealed_receipt_json is None
        upload = session.get(CollectionUploadRecord, collection_id)
        sealed_input(row)
        _record_payload_custody_progress(session, upload, row, now=_NOW)
    assert counters(factory, collection_id, artifact_id) == (1, 17 * CHUNK_SIZE, _NOW)


def test_repeated_receipt_uses_durable_member_progress_with_a_stale_session(tmp_path):
    service, _config, collection_id, _policy, artifact_id, _digests = raw_parts_fixture(
        tmp_path, 129
    )
    factory = service._session_factory
    ids = volume_ids(factory, collection_id)
    seal_all_but(factory, collection_id, None)
    with factory() as stale:
        upload = stale.get(CollectionUploadRecord, collection_id)
        member = stale.get(CollectionUploadArtifactRecord, (collection_id, artifact_id))
        row = stale.get(CollectionArchiveObjectUploadRecord, (collection_id, ids[-1]))
        assert member.payload_sealed_at is None
        with session_scope(factory) as writer:
            _record_payload_custody_progress(
                writer,
                writer.get(CollectionUploadRecord, collection_id),
                writer.get(CollectionArchiveObjectUploadRecord, (collection_id, ids[-1])),
                now=_NOW,
            )
        assert member.payload_sealed_at is None
        with accounting_work(stale) as work:
            _record_payload_custody_progress(stale, upload, row, now="2026-01-01T00:00:01Z")
        stale.commit()
        assert work["archive_records_loaded"] <= 16
    assert counters(factory, collection_id, artifact_id) == (1, 129 * CHUNK_SIZE, _NOW)


def test_payload_accounting_scopes_pending_work_to_collection_and_artifact(tmp_path):
    service, _config, collection_id, _policy, artifact_id, _digests = raw_parts_fixture(
        tmp_path, 17
    )
    factory = service._session_factory
    ids = volume_ids(factory, collection_id)
    with session_scope(factory) as session:
        artifact = session.get(CollectionUploadArtifactRecord, (collection_id, artifact_id))
        copied = {
            "artifact_id": "b" * 64,
            "bytes": str(artifact.bytes),
            "sha256": artifact.sha256,
            "raw_parts": {
                "part_plaintext_bytes": str(artifact.raw_part_plaintext_bytes),
                "part_count": str(artifact.raw_part_count),
                "ordered_sha256": artifact.raw_part_ordered_sha256,
            },
        }
    service.register_artifacts(collection_id, [copied])
    foreign_id = int(
        service.create_or_resume(
            idempotency_key="foreign-payload-accounting",
            ingest_source="fixture",
            archive_store=None,
            initiator=_CREATOR,
            event_context=None,
        )["collection_id"]
    )
    service.register_artifacts(foreign_id, [{**copied, "artifact_id": artifact_id}])
    with session_scope(factory) as session:
        for object_id in ids:
            sealed_input(
                session.get(CollectionArchiveObjectUploadRecord, (collection_id, object_id))
            )
    with session_scope(factory) as session:
        _record_payload_custody_progress(
            session,
            session.get(CollectionUploadRecord, collection_id),
            session.get(CollectionArchiveObjectUploadRecord, (collection_id, ids[-1])),
            now=_NOW,
        )
    assert counters(factory, collection_id, artifact_id) == (1, 17 * CHUNK_SIZE, _NOW)
    assert counters(factory, collection_id, "b" * 64) == (1, 17 * CHUNK_SIZE, None)
    assert counters(factory, foreign_id, artifact_id) == (0, 0, None)
