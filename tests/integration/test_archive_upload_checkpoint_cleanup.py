from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_upload_checkpoint_cleanup import (
    _composed_checkpoints,
    _finish,
    _lost,
    _retained_effect,
    _revoked,
    _seed,
)
from tests.unit.test_archive_upload_registration_cleanup import (
    _complete_registration,
    _prepared,
)

pytestmark = pytest.mark.integration


def test_postgres_original_checkpoint_retirement_keeps_parent_and_terminal_proof(
    database_url, tmp_path, monkeypatch
):
    with _prepared(tmp_path, monkeypatch, database_url=database_url) as ready:
        _complete_registration(ready)
        _seed(ready)
        _finish(ready)


def test_postgres_original_checkpoint_retirement_rechecks_current_rights(
    database_url, tmp_path, monkeypatch
):
    with _prepared(tmp_path, monkeypatch, database_url=database_url) as ready:
        _complete_registration(ready)
        _seed(ready)
        _revoked(ready)


@pytest.mark.parametrize("committed", (False, True))
def test_postgres_original_checkpoint_retirement_retries_original_sql_interruption(
    database_url, tmp_path, monkeypatch, committed
):
    with _prepared(tmp_path, monkeypatch, database_url=database_url) as ready:
        _complete_registration(ready)
        _seed(ready)
        _lost(ready, monkeypatch, committed)


def test_postgres_closed_but_unreceived_original_effect_keeps_preparation(
    database_url, tmp_path, monkeypatch
):
    with _prepared(tmp_path, monkeypatch, database_url=database_url) as ready:
        _complete_registration(ready)
        _retained_effect(ready)


@pytest.mark.parametrize("revoked", (False, True))
def test_postgres_original_terminal_worker_composes_current_authorized_checkpoints(
    database_url, tmp_path, monkeypatch, revoked
):
    with _prepared(tmp_path, monkeypatch, database_url=database_url, provider_ready=False) as ready:
        _composed_checkpoints(ready, monkeypatch, revoked=revoked)


def test_postgres_original_checkpoint_resume_defers_actual_physical_startup_contention(
    database_url, tmp_path, monkeypatch
):
    with _prepared(tmp_path, monkeypatch, database_url=database_url, provider_ready=False) as ready:
        _composed_checkpoints(ready, monkeypatch, revoked=True, startup_contention=True)
