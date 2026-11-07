from __future__ import annotations

from contextlib import contextmanager

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_protocol.errors import Conflict
from sqlalchemy import select
from time_formats import utc_timestamp_now

from tests.unit.test_collection_upload_preparation import _case
from tests.unit.test_finalization_claim_readiness import _claim_snapshot, _seal


@contextmanager
def _terminal_case(tmp_path, *, phase="queued", database_url=None):
    with _case(tmp_path, "segment", artifact_count=1, database_url=database_url) as case:
        _seal(case)
        if phase == "inflight":
            assert case.service._claim_due_finalization() is not None
        elif phase in {"ready", "retry"}:
            with session_scope(case.factory) as session:
                upload = session.get(Upload, case.collection_id)
                if phase == "ready":
                    upload.state, upload.archive_phase = "uploading", "uploading"
                    upload.archive_next_attempt_at = None
                else:
                    upload.archive_phase = "retry_wait"
                    upload.archive_next_attempt_at = utc_timestamp_now()
        operation = ArchiveWriteAdmission(case.factory).select_terminal(
            case.collection_id,
            kind="cancel",
            request={"fixture": "finalizer-admission"},
            principal_id="uploader",
            grants=case.service._config.archive_write,
            guard=lambda session: None,
            validate_selection=lambda session, upload: None,
        )
        case.terminal = operation
        yield case


@pytest.mark.parametrize("phase", ("queued", "retry"))
def test_selected_terminal_excludes_fresh_finalization_claims(tmp_path, phase):
    _closed_claim(tmp_path, phase)


def _closed_claim(tmp_path, phase, *, database_url=None):
    with _terminal_case(tmp_path, phase=phase, database_url=database_url) as case:
        before = _claim_snapshot(case)
        assert case.service._claim_due_finalization() is None
        assert _claim_snapshot(case) == before


@pytest.mark.parametrize("entry", ("batch", "point", "startup"))
def test_selected_terminal_prevents_readiness_and_startup_requeue(tmp_path, entry):
    _closed_readiness(tmp_path, entry)


def _closed_readiness(tmp_path, entry, *, database_url=None):
    phase = "inflight" if entry == "startup" else "ready"
    with _terminal_case(tmp_path, phase=phase, database_url=database_url) as case:
        before = _claim_snapshot(case)
        if entry == "batch":
            assert case.service._schedule_ready_finalizations(limit=1) == 0
        elif entry == "point":
            case.service._schedule_finalization_if_ready(case.collection_id)
        else:
            assert case.service.requeue_interrupted_finalizations_for_startup(limit=1) == 0
        assert _claim_snapshot(case) == before


@pytest.mark.parametrize(
    "stage",
    (
        "_stage_next_member_histories",
        "_advance_provenance_closure_validation",
        "_advance_archive_tree_checkpoint",
        "_advance_catalog_projection",
    ),
)
def test_selected_terminal_fences_new_catalog_finalization_steps(tmp_path, stage):
    _closed_catalog_stage(tmp_path, stage)


def _closed_catalog_stage(tmp_path, stage, *, database_url=None):
    with _terminal_case(tmp_path, phase="inflight", database_url=database_url) as case:
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            before = {
                column.name: getattr(upload, column.name) for column in Upload.__table__.columns
            }
        with pytest.raises(Conflict, match="archive write admission is closed"):
            getattr(case.service, stage)(case.collection_id)
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            after = {
                column.name: getattr(upload, column.name) for column in Upload.__table__.columns
            }
        assert after == before


def test_terminal_fence_precedes_published_root_and_copy_handoff(tmp_path):
    with _terminal_case(tmp_path, phase="inflight") as case:
        with session_scope(case.factory) as session:
            upload = session.scalar(
                select(Upload).where(Upload.collection_id == case.collection_id).with_for_update()
            )
            with pytest.raises(Conflict, match="archive write admission is closed"):
                case.service._publish_catalog_collection(session, upload)
