from __future__ import annotations

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_journal_validation_models import JournalValidationJobRecord as Job
from sqlalchemy import select
from sqlalchemy.exc import OperationalError

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_upload_catalog_cleanup import (
    _advance,
    _counts,
    _finish_children,
    _lost_children,
    _revoked_children,
    _seed_tags,
    _seed_validation,
    _validation_detachment,
)
from tests.unit.test_archive_upload_registration_cleanup import (
    _complete_registration,
    _prepared,
)

pytestmark = pytest.mark.integration


def test_postgres_bounded_upload_children_keep_original_terminal_and_independent_receipts(
    database_url, tmp_path, monkeypatch
):
    with _prepared(tmp_path, monkeypatch, database_url=database_url) as ready:
        _complete_registration(ready)
        _seed_tags(ready, 17)
        _finish_children(ready, expected_extra=17)


def test_postgres_catalog_windows_recheck_current_terminal_rights(
    database_url, tmp_path, monkeypatch
):
    with _prepared(tmp_path, monkeypatch, database_url=database_url) as ready:
        _complete_registration(ready)
        _revoked_children(ready)


@pytest.mark.parametrize("committed", (False, True))
def test_postgres_catalog_windows_retry_original_sql_interruption(
    database_url, tmp_path, monkeypatch, committed
):
    with _prepared(tmp_path, monkeypatch, database_url=database_url) as ready:
        _complete_registration(ready)
        _lost_children(ready, monkeypatch, committed)


def test_postgres_catalog_journal_retirement_keeps_native_workspace_receiving(
    database_url, tmp_path, monkeypatch
):
    with _prepared(tmp_path, monkeypatch, database_url=database_url) as ready:
        _complete_registration(ready)
        _validation_detachment(ready)


def test_postgres_busy_independent_validation_fence_defers_catalog_destruction(
    database_url, tmp_path, monkeypatch
):
    with _prepared(tmp_path, monkeypatch, database_url=database_url) as ready:
        _complete_registration(ready)
        _finish_children(ready)
        values = _seed_validation(ready)
        before = _counts(ready)
        with session_scope(ready.case.factory) as session:
            job = session.scalar(
                select(Job).where(Job.workspace_id == values["workspace_id"]).with_for_update()
            )
            assert job is not None
            with pytest.raises(OperationalError) as failure:
                _advance(ready)
            assert failure.value.orig.sqlstate == "55P03"
            assert _counts(ready) == before
        _finish_children(ready)
        with session_scope(ready.case.factory) as session:
            job = session.get(Job, values["workspace_id"])
            assert job is not None and job.state == "retiring" and job.collection_id is None
