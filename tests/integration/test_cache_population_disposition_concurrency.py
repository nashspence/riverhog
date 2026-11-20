from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest
from riverhog_core.catalog_cache_write_models import CachePopulationDispositionRecord as Disposition
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import RetrievalCacheStoreAccountingRecord
from sqlalchemy import func, select, text

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_cache_population_cleanup import _abandoned, _guard
from tests.unit.test_cache_population_disposition import (
    _assert_disposition,
    _assert_receiving_blocks,
    _complete,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("retire_source", (False, True))
def test_postgres_cache_disposition_retains_original_outcome_and_releases_only_owned_data(
    database_url, tmp_path, mode, retire_source
):
    _assert_disposition(tmp_path, mode, database_url=database_url, retire_source=retire_source)


@pytest.mark.parametrize("recorded", (False, True))
def test_postgres_cache_disposition_retains_unreceived_original_calls(
    database_url, tmp_path, recorded
):
    with _abandoned(tmp_path, database_url=database_url) as ready:
        _assert_receiving_blocks(ready, recorded)


def test_postgres_duplicate_cache_disposition_uses_one_atomic_original_outcome(
    database_url, tmp_path
):
    with _abandoned(tmp_path, database_url=database_url) as ready:
        _complete(ready)
        barrier = Barrier(2)
        pids = []

        def guard(session):
            _guard(session)
            pids.append(session.scalar(text("SELECT pg_backend_pid()")))
            barrier.wait(timeout=10)

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(ready.owner.finish, guard=guard) for _ in range(2)]
            results = [future.result(timeout=20) for future in futures]
        assert len(set(pids)) == 2 and results[0] == results[1]
        with session_scope(ready.case.factory) as session:
            assert session.scalar(select(func.count()).select_from(Disposition)) == 1
            assert (
                session.get(
                    RetrievalCacheStoreAccountingRecord, ready.case.candidate.name
                ).reserved_bytes
                == 0
            )
