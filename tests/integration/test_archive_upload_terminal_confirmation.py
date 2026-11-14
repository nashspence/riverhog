from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_upload_projection_cleanup import _projection
from tests.unit.test_archive_upload_terminal_confirmation import (
    _clean,
    _finished,
    _lost,
    _receipts,
    _rights,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("indexed", (False, True))
def test_postgres_atomic_terminal_confirmation_preserves_original_receipts(
    database_url, tmp_path, indexed
):
    with _projection(tmp_path, database_url=database_url, indexed=indexed) as ready:
        _clean(ready)
        receipts = _receipts(ready)
        _finished(ready)
        assert _receipts(ready) == receipts


@pytest.mark.parametrize("committed", (False, True))
def test_postgres_terminal_confirmation_retries_rollback_or_lost_acknowledgement(
    database_url, tmp_path, monkeypatch, committed
):
    with _projection(tmp_path, database_url=database_url, indexed=True) as ready:
        _clean(ready)
        _lost(ready, monkeypatch, committed)


def test_postgres_current_terminal_result_rights_survive_parent_removal(database_url, tmp_path):
    with _projection(tmp_path, database_url=database_url, indexed=True) as ready:
        _clean(ready)
        _rights(ready)


def test_postgres_cancel_confirmation_preserves_registration_and_copy_receiving(
    database_url, tmp_path, monkeypatch
):
    from tests.unit.test_archive_upload_copy_intent_cleanup import DESTINATIONS
    from tests.unit.test_archive_upload_registration_cleanup import _prepared
    from tests.unit.test_archive_upload_terminal_confirmation import _history

    with _prepared(tmp_path, monkeypatch, database_url=database_url, copy_to=DESTINATIONS) as ready:
        _clean(ready)
        _history(ready)


def test_postgres_confirmed_replay_needs_no_new_disposal_decision(database_url, tmp_path):
    from tests.unit.test_archive_upload_terminal_confirmation import _replay_without_disposal

    with _projection(tmp_path, database_url=database_url, indexed=True) as ready:
        _clean(ready)
        _finished(ready)
        _replay_without_disposal(ready)
