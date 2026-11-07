from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier

import pytest
from riverhog_core.catalog_archive_write_models import ArchiveCleanupAttemptRecord as Attempt
from riverhog_core.catalog_db import session_scope
from riverhog_protocol.errors import Conflict
from sqlalchemy import func, select, text

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_upload_cleanup import (
    GRANTS,
    _capacity,
    _cleanup_arm,
    _inventory_arm,
    _partial_retry,
    _ready,
    _receiving_after_retirement,
    _select,
)
from tests.unit.test_archive_write_admission import _case, _guard

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("flow", ("partial", "retirement", "capacity"))
def test_postgres_cleanup_original_receiving_and_independent_charges(database_url, tmp_path, flow):
    with _case(tmp_path, database_url=database_url) as case:
        ready = _ready(case, grants=replace(GRANTS, calls=1) if flow == "capacity" else GRANTS)
        {
            "partial": _partial_retry,
            "retirement": _receiving_after_retirement,
            "capacity": _capacity,
        }[flow](ready)


@pytest.mark.parametrize("kind", ("dispose", "inventory"))
def test_postgres_cleanup_capacity_is_serialized_across_independent_connections(
    database_url, tmp_path, kind
):
    with _case(tmp_path, database_url=database_url) as case:
        ready = _ready(case, grants=replace(GRANTS, calls=1))
        page = _select(ready) if kind == "dispose" else None
        simultaneous = Barrier(2)
        pids = []

        def guard(session):
            _guard(session)
            pids.append(session.scalar(text("SELECT pg_backend_pid()")))
            simultaneous.wait(timeout=10)

        def arm():
            try:
                return (
                    _cleanup_arm(ready, page, guard=guard)
                    if kind == "dispose"
                    else _inventory_arm(ready, guard=guard)
                )
            except Conflict:
                return None

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(arm) for _ in range(2)]
            results = [future.result(timeout=20) for future in futures]
        assert len(set(pids)) == 2
        assert sum(result is not None for result in results) == 1
        with session_scope(case.factory) as session:
            assert session.scalar(select(func.count()).select_from(Attempt)) == 1
            assert session.scalar(select(Attempt)).unresolved


@pytest.mark.parametrize("flow", ("lost", "retirement"))
def test_postgres_original_inventory_receiving_and_independent_charges(
    database_url, tmp_path, flow
):
    from tests.unit.test_archive_cleanup_inventory import (
        _inventory_receiving_after_retirement,
        _lost_inventory_reply,
    )

    with _case(tmp_path, database_url=database_url) as case:
        ready = _ready(case)
        {"lost": _lost_inventory_reply, "retirement": _inventory_receiving_after_retirement}[flow](
            ready
        )
