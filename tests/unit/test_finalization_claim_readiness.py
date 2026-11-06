from __future__ import annotations

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord as Volume
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services.collection_uploads import (
    SqlAlchemyCollectionUploadService,
    _FinalizationClaim,
)
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


def _current_claim(case):
    with session_scope(case.factory) as session:
        upload = session.get(Upload, case.collection_id)
        assert upload.archive_claim_id is not None
        return _FinalizationClaim(case.collection_id, upload.archive_claim_id)


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
                claim = current._claim_due_finalization()
                assert claim.collection_id == case.collection_id
                current._requeue_finalization_step(claim)
            assert len(counts) == 1
            current = _fresh_coordinator(case)
            assert current._claim_due_finalization().collection_id == case.collection_id
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
        claim = case.service._claim_due_finalization()
        assert claim is not None
        phase = {"open": "planning", "closing": "uploading"}.get(state, state)
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            upload.state, upload.archive_phase = state, phase
            upload.archive_next_attempt_at = None
            upload.archive_failure = "retained current owner"
        current = _fresh_coordinator(case)
        if callback == "retry":
            current._record_finalization_retry(claim, OSError("late report"))
        else:
            current._requeue_finalization_step(claim)
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
        claim = case.service._claim_due_finalization()
        assert claim is not None
        case.service._requeue_finalization_step(claim)
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            upload.archive_failure = "current queued owner"
            queued_at = upload.archive_next_attempt_at
        current = _fresh_coordinator(case)
        if callback == "retry":
            current._record_finalization_retry(claim, OSError("late report"))
        else:
            current._requeue_finalization_step(claim)
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            assert upload.state == "finalizing"
            assert upload.archive_phase == "finalization_queued"
            assert upload.archive_next_attempt_at == queued_at
            assert upload.archive_failure == "current queued owner"


@pytest.mark.parametrize("callback", ("retry", "requeue"))
def test_late_finalizer_feedback_preserves_the_selected_terminal_queue(tmp_path, callback):
    from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission

    with _case(tmp_path, "segment", artifact_count=1) as case:
        _seal(case)
        current = _fresh_coordinator(case)
        claim = current._claim_due_finalization()
        assert claim.collection_id == case.collection_id
        ArchiveWriteAdmission(case.factory).select_terminal(
            case.collection_id,
            kind="cancel",
            request={"fixture": "late-finalizer-feedback"},
            principal_id="uploader",
            grants=case.service._config.archive_write,
            guard=lambda session: None,
            validate_selection=lambda *args: None,
        )
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            before = (upload.archive_phase, upload.archive_next_attempt_at, upload.archive_failure)
        if callback == "retry":
            current._record_finalization_retry(claim, OSError("late result"))
        else:
            current._requeue_finalization_step(claim)
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            assert (
                upload.archive_phase,
                upload.archive_next_attempt_at,
                upload.archive_failure,
            ) == before


def _claim_snapshot(case):
    with session_scope(case.factory) as session:
        upload = session.get(Upload, case.collection_id)
        return (
            upload.state,
            upload.archive_phase,
            upload.archive_phase_updated_at,
            upload.archive_next_attempt_at,
            upload.archive_last_attempt_at,
            upload.archive_failure,
            upload.archive_claim_id,
            upload.archive_attempt_count,
        )


@pytest.mark.parametrize("callback", ("retry", "requeue"))
@pytest.mark.parametrize("release", ("restart", "acknowledged"))
def test_old_feedback_cannot_mutate_a_new_claim_at_the_same_timestamp(
    tmp_path, monkeypatch, callback, release
):
    from riverhog_core.services import collection_uploads as module

    with _case(tmp_path, "segment", artifact_count=1) as case:
        _seal(case)
        stamp = module.utc_timestamp_now()
        monkeypatch.setattr(module, "utc_timestamp_now", lambda: stamp)
        original = case.service._claim_due_finalization()
        assert original.collection_id == case.collection_id
        if release == "restart":
            assert _fresh_coordinator(case).requeue_interrupted_finalizations_for_startup() == 1
        else:
            case.service._requeue_finalization_step(original)
        replacement = _fresh_coordinator(case)._claim_due_finalization()
        assert replacement.collection_id == original.collection_id
        assert replacement.claim_id != original.claim_id
        with session_scope(case.factory) as session:
            session.get(Upload, case.collection_id).archive_failure = "new exact owner"
        before = _claim_snapshot(case)
        current = _fresh_coordinator(case)
        if callback == "retry":
            current._record_finalization_retry(original, OSError("old delayed result"))
        else:
            current._requeue_finalization_step(original)
        assert _claim_snapshot(case) == before
        current._requeue_finalization_step(replacement)
        after = _claim_snapshot(case)
        assert after[1] == "finalization_queued" and after[6] is None
        assert after[4] == before[4] == stamp and after[7] == before[7]


@pytest.mark.parametrize("callback", ("retry", "requeue"))
def test_received_claim_is_consumed_once_before_another_workflow_turn(tmp_path, callback):
    with _case(tmp_path, "segment", artifact_count=1) as case:
        _seal(case)
        claim = case.service._claim_due_finalization()
        assert claim is not None
        if callback == "retry":
            case.service._record_finalization_retry(claim, OSError("retry this turn"))
        else:
            case.service._requeue_finalization_step(claim)
        before = _claim_snapshot(case)
        assert before[1] == ("retry_wait" if callback == "retry" else "finalization_queued")
        assert before[6] is None
        case.service._record_finalization_retry(claim, OSError("duplicate"))
        case.service._requeue_finalization_step(claim)
        assert _claim_snapshot(case) == before


def test_lost_claim_commit_reply_requires_explicit_startup_recovery(tmp_path, monkeypatch):
    from contextlib import contextmanager

    from riverhog_core.services import collection_uploads as module

    with _case(tmp_path, "segment", artifact_count=1) as case:
        _seal(case)
        actual = module.session_scope

        @contextmanager
        def lose_reply(factory):
            with actual(factory) as session:
                yield session
            raise OSError("lost original claim reply")

        with monkeypatch.context() as patch:
            patch.setattr(module, "session_scope", lose_reply)
            with pytest.raises(OSError, match="lost original claim reply"):
                case.service._claim_due_finalization()
        original = _current_claim(case)
        assert _fresh_coordinator(case)._claim_due_finalization() is None
        assert _fresh_coordinator(case).requeue_interrupted_finalizations_for_startup() == 1
        replacement = _fresh_coordinator(case)._claim_due_finalization()
        assert replacement.collection_id == original.collection_id
        assert replacement.claim_id != original.claim_id
        before = _claim_snapshot(case)
        case.service._requeue_finalization_step(original)
        assert _claim_snapshot(case) == before
