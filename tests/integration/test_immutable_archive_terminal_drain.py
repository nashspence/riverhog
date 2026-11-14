from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_initial_description_checkpoint import _description_source
from tests.unit.test_immutable_archive_terminal_drain import (
    _assert_stopped,
    _busy,
    _unknown,
    _unused,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_postgres_running_immutable_actor_drain_remains_nonblocking(
    database_url, tmp_path, monkeypatch, mode
):
    with _description_source(tmp_path, monkeypatch, mode=mode, database_url=database_url) as f:
        _busy(f, monkeypatch)


@pytest.mark.parametrize("mode", ("direct", "http"))
@pytest.mark.parametrize("returned", (False, True))
def test_postgres_terminal_immutable_drain_preserves_uncertain_execution(
    database_url, tmp_path, monkeypatch, mode, returned
):
    with _description_source(tmp_path, monkeypatch, mode=mode, database_url=database_url) as f:
        _unknown(f, monkeypatch, returned)


def test_postgres_terminal_immutable_drain_withdraws_an_unused_original(
    database_url, tmp_path, monkeypatch
):
    with _description_source(tmp_path, monkeypatch, database_url=database_url) as f:
        _assert_stopped(f, _unused(f, monkeypatch))
