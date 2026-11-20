from __future__ import annotations

from dataclasses import replace

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_cache_population_cleanup import GRANTS, _abandoned
from tests.unit.test_cache_population_cleanup_turn import (
    _FAULTS,
    _assert_failed_receiving,
    _assert_turns,
    _assert_unknown,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
def test_postgres_cache_cleanup_turn_keeps_provider_execution_outside_catalog_writer(
    database_url, tmp_path, mode
):
    with _abandoned(tmp_path, mode=mode, database_url=database_url) as ready:
        _assert_turns(ready)


@pytest.mark.parametrize("method,stage", _FAULTS)
@pytest.mark.parametrize("committed", (False, True))
def test_postgres_cache_cleanup_turn_retries_only_original_database_receiving(
    database_url, tmp_path, monkeypatch, method, stage, committed
):
    with _abandoned(tmp_path, database_url=database_url) as ready:
        _assert_failed_receiving(ready, monkeypatch, method, stage, committed)


def test_postgres_cache_cleanup_turn_never_closes_unknown_original_from_later_progress(
    database_url, tmp_path
):
    with _abandoned(
        tmp_path, database_url=database_url, grants=replace(GRANTS, control_bytes=256 * 1024)
    ) as ready:
        _assert_unknown(ready)
