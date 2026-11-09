from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_upload_registration_cleanup import (
    _complete_registration,
    _composed_registration,
    _independent,
    _lost,
    _prepared,
    _revoked,
)

pytestmark = pytest.mark.integration


def test_postgres_native_registration_cleanup_uses_bounded_retained_windows(
    database_url, tmp_path, monkeypatch
):
    with _prepared(tmp_path, monkeypatch, database_url=database_url) as ready:
        _complete_registration(ready)


def test_postgres_native_registration_cleanup_requires_current_terminal_rights(
    database_url, tmp_path, monkeypatch
):
    with _prepared(tmp_path, monkeypatch, database_url=database_url) as ready:
        _revoked(ready)


def test_postgres_independent_registration_executor_cannot_advance_native_cleanup(
    database_url, tmp_path, monkeypatch
):
    with _prepared(tmp_path, monkeypatch, database_url=database_url) as ready:
        _independent(ready)


@pytest.mark.parametrize("committed", (False, True))
def test_postgres_native_registration_cleanup_retries_retained_cursor_after_lost_reply(
    database_url, tmp_path, monkeypatch, committed
):
    with _prepared(tmp_path, monkeypatch, database_url=database_url) as ready:
        _lost(ready, monkeypatch, committed)


@pytest.mark.parametrize("revoked", (False, True))
def test_postgres_original_terminal_worker_composes_registration_after_archive_proof(
    database_url, tmp_path, monkeypatch, revoked
):
    with _prepared(tmp_path, monkeypatch, database_url=database_url, provider_ready=False) as ready:
        _composed_registration(ready, monkeypatch, revoked=revoked)
