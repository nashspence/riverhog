from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest
from riverhog_core.catalog_cache_write_models import CachePopulationBeginRecord
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import RetrievalCacheStoreAccountingRecord
from sqlalchemy import text

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_cache_population_abandonment import _assert_handoff, _begun
from tests.unit.test_cache_population_begin import _case, _terminal

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("withdrawn", (False, True))
def test_postgres_cache_abandonment_retains_original_receiving_and_data_reservations(
    database_url, tmp_path, monkeypatch, mode, withdrawn
):
    _assert_handoff(tmp_path, monkeypatch, mode, database_url=database_url, withdrawn=withdrawn)


def test_postgres_duplicate_cache_abandonment_has_one_durable_receiver(database_url, tmp_path):
    with _case(tmp_path, database_url=database_url) as case:
        ticket = _begun(case)
        operation = _terminal(case)
        barrier = Barrier(2)
        pids = []

        def guard(session):
            pids.append(session.scalar(text("SELECT pg_backend_pid()")))
            barrier.wait(timeout=10)
            case.guard(session)

        def accept():
            return case.cache.abandonment.accept_report(
                case.collection_id,
                case.object_id,
                ticket.call_id,
                operation_id=operation,
                guard=guard,
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(accept) for _ in range(2)]
            received = [future.result(timeout=20) for future in futures]
        assert len(set(pids)) == 2 and received[0] == received[1]
        case.cache.abandonment.acknowledge(received[0])
        case.cache.abandonment.acknowledge(received[1])
        with session_scope(case.factory) as session:
            assert session.get(CachePopulationBeginRecord, ticket.call_id) is None
            assert (
                session.get(RetrievalCacheStoreAccountingRecord, case.candidate.name).reserved_bytes
                == 3
            )
