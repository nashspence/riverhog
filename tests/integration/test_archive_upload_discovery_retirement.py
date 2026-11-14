from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_upload_discovery_retirement import _lost, _retire, _revoked
from tests.unit.test_archive_upload_projection_cleanup import _projection

pytestmark = pytest.mark.integration


def test_postgres_exact_discovery_detaches_only_after_exclusive_original_lease(
    database_url, tmp_path
):
    with _projection(tmp_path, database_url=database_url, indexed=True, linked=True) as ready:
        _retire(ready)


@pytest.mark.parametrize("committed", (False, True))
def test_postgres_discovery_detachment_retains_lease_through_commit_or_rollback(
    database_url, tmp_path, monkeypatch, committed
):
    with _projection(tmp_path, database_url=database_url, indexed=True, linked=True) as ready:
        _lost(ready, monkeypatch, committed=committed)


def test_postgres_discovery_detachment_requires_current_terminal_rights(database_url, tmp_path):
    with _projection(tmp_path, database_url=database_url, indexed=True, linked=True) as ready:
        _revoked(ready)


@pytest.mark.parametrize("committed", (None, False, True))
def test_postgres_same_original_actor_receives_completion_after_exact_discovery_detachment(
    database_url, tmp_path, monkeypatch, committed
):
    from tests.unit.test_archive_upload_discovery_retirement import _composed

    with _projection(tmp_path, database_url=database_url, indexed=True, linked=True) as ready:
        _composed(ready, monkeypatch, committed=committed)
