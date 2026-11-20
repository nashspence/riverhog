from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_upload_projection_cleanup import _projection
from tests.unit.test_archive_upload_terminal_completion import (
    _actor,
    _blocked_after_commit,
    _drive,
    _lost_ack,
)
from tests.unit.test_archive_upload_terminal_confirmation import _confirmed

pytestmark = pytest.mark.integration


def test_postgres_original_actor_confirms_and_receives_its_actual_join(
    database_url, tmp_path, monkeypatch
):
    with _projection(tmp_path, database_url=database_url, indexed=True) as ready:
        _drive(_actor(ready, monkeypatch))
        assert _confirmed(ready) == ready.original


def test_postgres_original_actor_reconciles_lost_ack_without_read_or_disposal_borrowing(
    database_url, tmp_path, monkeypatch
):
    with _projection(tmp_path, database_url=database_url, indexed=True) as ready:
        _lost_ack(ready, monkeypatch)


def test_postgres_confirmed_state_keeps_running_original_worker_charged(
    database_url, tmp_path, monkeypatch
):
    with _projection(tmp_path, database_url=database_url, indexed=True) as ready:
        _blocked_after_commit(ready, monkeypatch)
