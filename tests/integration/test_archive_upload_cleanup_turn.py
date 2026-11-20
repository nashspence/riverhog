from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_upload_cleanup_turn import (
    _assert_complete,
    _binding_during_receiving,
    _composed,
    _failed_receiving,
    _filesystem,
    _ready,
    _unknown,
)
from tests.unit.test_archive_upload_terminal_turn import _turn_case

pytestmark = pytest.mark.integration


def test_postgres_bounded_archive_inventory_retains_original_terminal_result(
    database_url, tmp_path
):
    with _ready(tmp_path, database_url=database_url) as ready:
        _assert_complete(ready)


@pytest.mark.parametrize(
    "method,stage",
    (
        ("record_response", "dispose"),
        ("select_page", "inventory"),
        ("acknowledge_response", "dispose"),
    ),
)
def test_postgres_original_archive_cleanup_receives_lost_committed_reply(
    database_url, tmp_path, monkeypatch, method, stage
):
    with _ready(tmp_path, database_url=database_url) as ready:
        _failed_receiving(ready, monkeypatch, method, stage, True)


def test_postgres_unknown_archive_call_cannot_borrow_inventory_exhaustion(
    database_url, tmp_path, monkeypatch
):
    with _ready(tmp_path, database_url=database_url) as ready:
        _unknown(ready, monkeypatch)


def test_postgres_actual_original_terminal_worker_composes_archive_cleanup(
    database_url, tmp_path, monkeypatch
):
    with _turn_case(tmp_path, database_url=database_url, originals=2) as case:
        _composed(case, monkeypatch)


def test_postgres_actual_filesystem_cleanup_retains_independent_scope(
    database_url, tmp_path, monkeypatch
):
    with _ready(tmp_path, database_url=database_url) as ready:
        _filesystem(ready, tmp_path, monkeypatch)


@pytest.mark.parametrize("stage", ("inventory", "dispose"))
def test_postgres_current_binding_cannot_redirect_original_captured_receiving(
    database_url, tmp_path, monkeypatch, stage
):
    with _ready(tmp_path, database_url=database_url) as ready:
        _binding_during_receiving(ready, monkeypatch, stage, True)
