from __future__ import annotations

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord as Volume
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services.collection_uploads import SqlAlchemyCollectionUploadService
from sqlalchemy import event

from tests.unit.test_collection_upload_preparation import _case
from tests.unit.test_collection_uploads import _CREATOR


def _seal(case):
    case.service.complete(case.collection_id)
    volumes = case.service.list_volumes(case.collection_id)["volumes"]
    for volume in volumes:
        case.service.upload_unit(
            case.collection_id,
            volume["volume_id"],
            0,
            principal=_CREATOR,
            plan_sha256=volume["plan_sha256"],
            content=case.content,
        )


def _fresh_coordinator(case):
    current = object.__new__(SqlAlchemyCollectionUploadService)
    current._session_factory = case.factory
    return current


@pytest.mark.parametrize("volumes", [1, 8, 32])
def test_claims_reuse_positive_readiness_across_fresh_owners(tmp_path, record_property, volumes):
    _assert_claims_reuse(tmp_path, record_property, volumes)


def _assert_claims_reuse(tmp_path, record_property, volumes, *, database_url=None):
    with _case(tmp_path, "segment", artifact_count=volumes, database_url=database_url) as case:
        engine = case.factory.kw["bind"]
        counts = []

        def observed(connection, cursor, statement, parameters, context, executemany):
            if statement.startswith("SELECT count(" + Volume.__tablename__ + ".object_id)"):
                counts.append(statement)

        event.listen(engine, "before_cursor_execute", observed)
        try:
            _seal(case)
            assert len(counts) == 1
            for _ in range(volumes):
                current = _fresh_coordinator(case)
                assert current.requeue_interrupted_finalizations_for_startup() == 1
                assert current._claim_due_finalization() == case.collection_id
                current._requeue_finalization_step(case.collection_id)
            assert len(counts) == 1
            current = _fresh_coordinator(case)
            assert current._claim_due_finalization() == case.collection_id
            current._schedule_finalization_if_ready(case.collection_id)
            with session_scope(case.factory) as session:
                upload = session.get(Upload, case.collection_id)
                assert upload.state == upload.archive_phase == "finalizing"
                assert upload.archive_next_attempt_at is None
                assert upload.lease_expires_at is None
            assert len(counts) == 1
            record_property(
                "readiness_work",
                {"sealed_volumes": volumes, "claims": volumes + 1, "volume_censuses": len(counts)},
            )
        finally:
            event.remove(engine, "before_cursor_execute", observed)


@pytest.mark.parametrize("state", ["orphaned", "discarding", "open", "closing", "uploading"])
@pytest.mark.parametrize("callback", ["retry", "requeue"])
def test_stale_finalizer_does_not_create_readiness(tmp_path, state, callback):
    with _case(tmp_path, "segment", artifact_count=1) as case:
        _seal(case)
        phase = {"open": "planning", "closing": "uploading"}.get(state, state)
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            upload.state, upload.archive_phase = state, phase
            upload.archive_next_attempt_at = None
            upload.archive_failure = "retained current owner"
        current = _fresh_coordinator(case)
        if callback == "retry":
            current._record_finalization_retry(case.collection_id, OSError("late report"))
        else:
            current._requeue_finalization_step(case.collection_id)
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            assert (upload.state, upload.archive_phase) == (state, phase)
            assert upload.archive_next_attempt_at is None
            assert upload.archive_failure == "retained current owner"


@pytest.mark.parametrize("state", ["orphaned", "discarding", "open"])
def test_readiness_callback_cannot_reopen_retired_or_open_construction(tmp_path, state):
    with _case(tmp_path, "segment", artifact_count=1) as case:
        _seal(case)
        phase = {"open": "planning", "closing": "uploading"}.get(state, state)
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            upload.state, upload.archive_phase = state, phase
            upload.archive_next_attempt_at = None
        _fresh_coordinator(case)._schedule_finalization_if_ready(case.collection_id)
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            assert (upload.state, upload.archive_phase) == (state, phase)
            assert upload.archive_next_attempt_at is None


@pytest.mark.parametrize("callback", ["retry", "requeue"])
def test_duplicate_callback_preserves_already_queued_work(tmp_path, callback):
    with _case(tmp_path, "segment", artifact_count=1) as case:
        _seal(case)
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            upload.archive_failure = "current queued owner"
            queued_at = upload.archive_next_attempt_at
        current = _fresh_coordinator(case)
        if callback == "retry":
            current._record_finalization_retry(case.collection_id, OSError("late report"))
        else:
            current._requeue_finalization_step(case.collection_id)
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            assert upload.state == "finalizing"
            assert upload.archive_phase == "finalization_queued"
            assert upload.archive_next_attempt_at == queued_at
            assert upload.archive_failure == "current queued owner"
