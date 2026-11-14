from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier

import pytest
from riverhog_core.catalog_archive_write_models import TerminalCleanupCapacityRecord as Capacity
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_object_cleanup_models import ObjectCleanupAttemptRecord as Attempt
from riverhog_protocol.errors import Conflict
from sqlalchemy import func, select, text

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_cache_population_cleanup import (
    GRANTS,
    _abandoned,
    _archive_scope,
    _assert_cleanup,
    _assert_lost_reply,
    _guard,
    _inventory_arm,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("retire_source", (False, True))
def test_postgres_cache_cleanup_keeps_original_scope_and_data_owned(
    database_url, tmp_path, monkeypatch, mode, retire_source
):
    _assert_cleanup(
        tmp_path, monkeypatch, mode, database_url=database_url, retire_source=retire_source
    )


def test_postgres_later_cache_progress_does_not_close_an_unreceived_original(
    database_url, tmp_path
):
    _assert_lost_reply(tmp_path, database_url=database_url)


def test_postgres_archive_and_cache_owners_compete_for_shared_terminal_capacity(
    database_url, tmp_path
):
    grants = replace(GRANTS, calls=1)
    with _abandoned(tmp_path, database_url=database_url, grants=grants) as ready:
        archive, request = _archive_scope(ready, grants)
        barrier = Barrier(2)
        pids = []

        def guard(session):
            _guard(session)
            pids.append(session.scalar(text("SELECT pg_backend_pid()")))
            barrier.wait(timeout=10)

        def arm(cache):
            try:
                return (
                    _inventory_arm(ready, guard=guard)
                    if cache
                    else archive.arm_inventory(
                        ready.case.collection_id,
                        ready.operation,
                        request=request,
                        after=None,
                        dispatch_owner="a" * 64,
                        guard=guard,
                    )
                )
            except Conflict:
                return None

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(arm, cache) for cache in (True, False)]
            results = [future.result(timeout=20) for future in futures]
        assert len(set(pids)) == 2 and sum(result is not None for result in results) == 1
        with session_scope(ready.case.factory) as session:
            assert session.scalar(select(func.count()).select_from(Attempt)) == 1
            assert session.get(Capacity, ready.operation).calls == "1"
