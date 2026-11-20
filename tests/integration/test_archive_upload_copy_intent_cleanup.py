from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_upload_copy_intent_cleanup import (
    DESTINATIONS,
    _composed_intents,
    _finish,
    _independent,
    _lost,
    _revoked,
)
from tests.unit.test_archive_upload_registration_cleanup import _complete_registration, _prepared

pytestmark = pytest.mark.integration


def test_postgres_original_copy_intents_keep_receipts_and_parent(
    database_url, tmp_path, monkeypatch
):
    with _prepared(tmp_path, monkeypatch, database_url=database_url, copy_to=DESTINATIONS) as ready:
        _complete_registration(ready)
        _finish(ready)


def test_postgres_original_copy_intent_windows_require_current_rights(
    database_url, tmp_path, monkeypatch
):
    with _prepared(tmp_path, monkeypatch, database_url=database_url, copy_to=DESTINATIONS) as ready:
        _complete_registration(ready)
        _revoked(ready)


@pytest.mark.parametrize("committed", (False, True))
def test_postgres_original_copy_intent_windows_retry_sql_interruption(
    database_url, tmp_path, monkeypatch, committed
):
    with _prepared(tmp_path, monkeypatch, database_url=database_url, copy_to=DESTINATIONS) as ready:
        _complete_registration(ready)
        _lost(ready, monkeypatch, committed)


@pytest.mark.parametrize("state", ("pending", "handed_off", "failed"))
def test_postgres_original_terminal_preserves_independent_copy_handoffs(
    database_url, tmp_path, monkeypatch, state
):
    with _prepared(tmp_path, monkeypatch, database_url=database_url, copy_to=DESTINATIONS) as ready:
        _complete_registration(ready)
        _independent(ready, state)


@pytest.mark.parametrize("revoked", (False, True))
def test_postgres_original_terminal_worker_composes_bounded_copy_intent_cancellation(
    database_url, tmp_path, monkeypatch, revoked
):
    with _prepared(
        tmp_path, monkeypatch, database_url=database_url, copy_to=DESTINATIONS, provider_ready=False
    ) as ready:
        _composed_intents(ready, monkeypatch, revoked=revoked)
