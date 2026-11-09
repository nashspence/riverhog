from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_upload_projection_cleanup import (
    _finish,
    _independent,
    _linked_discovery,
    _lost,
    _projection,
    _revoked,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("indexed", (False, True))
def test_postgres_exact_unpublished_projection_retires_bounded_children(
    database_url, tmp_path, indexed
):
    with _projection(tmp_path, database_url=database_url, indexed=indexed) as ready:
        _finish(ready)


def test_postgres_unpublished_projection_requires_current_terminal_rights(database_url, tmp_path):
    with _projection(tmp_path, database_url=database_url, indexed=True) as ready:
        _revoked(ready)


@pytest.mark.parametrize("committed", (False, True))
def test_postgres_unpublished_projection_retries_rollback_and_lost_reply(
    database_url, tmp_path, monkeypatch, committed
):
    with _projection(tmp_path, database_url=database_url, indexed=True) as ready:
        _lost(ready, monkeypatch, committed)


@pytest.mark.parametrize("kind", ("cache", "processing"))
def test_postgres_unpublished_projection_preserves_independent_receiving(
    database_url, tmp_path, kind
):
    with _projection(tmp_path, database_url=database_url, indexed=True) as ready:
        _independent(ready, kind)


def test_postgres_linked_discovery_retains_exact_files_and_exclusive_lease(database_url, tmp_path):
    with _projection(tmp_path, database_url=database_url, indexed=True, linked=True) as ready:
        _linked_discovery(ready, tmp_path)
