from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_cache_population_cleanup import _abandoned
from tests.unit.test_cache_population_cleanup_maintenance import (
    _assert_join_before_retirement,
    _assert_lost_finish,
    _assert_overlap,
    _assert_service_cleanup,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("paginated", (False, True))
@pytest.mark.parametrize("disabled", (False, True))
def test_postgres_service_maintenance_advances_actual_native_cache_pages(
    database_url, tmp_path, monkeypatch, mode, paginated, disabled
):
    with _abandoned(tmp_path, mode=mode, database_url=database_url) as ready:
        _assert_service_cleanup(ready, monkeypatch, paginated=paginated, disabled=disabled)


@pytest.mark.parametrize("method", ("finish", "acknowledge_response"))
def test_postgres_original_cleanup_receiving_resumes_after_population_row_teardown(
    database_url, tmp_path, monkeypatch, method
):
    with _abandoned(tmp_path, database_url=database_url) as ready:
        _assert_lost_finish(ready, monkeypatch, method=method)


@pytest.mark.parametrize("lost_start_reply", (False, True))
def test_postgres_cleanup_retirement_requires_actual_original_worker_join(
    database_url, tmp_path, monkeypatch, lost_start_reply
):
    with _abandoned(tmp_path, database_url=database_url) as ready:
        _assert_join_before_retirement(ready, monkeypatch, lost_start_reply=lost_start_reply)


def test_postgres_competing_original_inventory_receiving_does_not_strand_cleanup(
    database_url, tmp_path, monkeypatch
):
    with _abandoned(tmp_path, database_url=database_url) as ready:
        _assert_overlap(ready, monkeypatch)


@pytest.mark.parametrize("committed", (False, True))
def test_postgres_cache_due_schedule_loss_never_speculatively_dispatches(
    database_url, tmp_path, monkeypatch, committed
):
    from tests.unit.test_cache_population_cleanup_maintenance import _assert_lost_due

    with _abandoned(tmp_path, database_url=database_url) as ready:
        _assert_lost_due(ready, monkeypatch, committed)


def test_postgres_native_cleanup_due_selector_skips_an_actual_held_population_lock(
    database_url, tmp_path, monkeypatch
):
    from concurrent.futures import ThreadPoolExecutor

    from riverhog_core.catalog_db import session_scope
    from riverhog_core.catalog_models import RetrievalCachePopulationRecord as Population
    from sqlalchemy import select

    from tests.unit.test_cache_population_cleanup_maintenance import _clock, _drain, _service

    with _abandoned(tmp_path, database_url=database_url) as ready:
        advance = _clock(monkeypatch)
        with _service(ready, monkeypatch) as (service, calls, actors):
            with ThreadPoolExecutor(max_workers=1) as pool:
                with session_scope(ready.case.factory) as session:
                    population = session.scalar(select(Population).with_for_update())
                    original = population.updated_at
                    advance()
                    assert pool.submit(service.process_due_archive_uploads).result(timeout=2) == 0
                    assert population.updated_at == original
                    assert calls == actors == []
            _drain(ready, service, advance)
            assert len(calls) == 2


@pytest.mark.parametrize("idle_polls", (1, 2, 3))
def test_postgres_waiting_cache_scope_keeps_its_next_actual_shared_slot_turn(
    database_url, tmp_path, monkeypatch, idle_polls
):
    from tests.unit.test_cache_population_cleanup_maintenance import _assert_shared_slot_fairness

    with _abandoned(tmp_path, database_url=database_url) as ready:
        _assert_shared_slot_fairness(ready, monkeypatch, idle_polls)
