from __future__ import annotations

import os
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from pathlib import Path
from uuid import uuid4

import pytest
from riverhog_core.catalog_db import create_catalog_engine, session_scope
from riverhog_core.catalog_models import RetrievalPlanObjectRecord
from riverhog_core.services.archive_copy_retirements import (
    SqlAlchemyArchiveCopyRetirementService,
)
from riverhog_protocol.errors import Conflict
from sqlalchemy import event, select, text
from sqlalchemy.engine import make_url

from tests.unit.test_retrieval_selection import ARTIFACT, _add_mirror, _warm

pytestmark = pytest.mark.integration


@pytest.fixture
def database_url() -> Iterator[str]:
    value = os.environ.get("RIVERHOG_TEST_POSTGRES_URL", "").strip()
    if not value:
        pytest.skip("RIVERHOG_TEST_POSTGRES_URL is required")
    schema = "riverhog_retrieval_selection_" + uuid4().hex
    admin = create_catalog_engine(value)
    with admin.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    scoped = (
        make_url(value)
        .update_query_dict({"options": f"-csearch_path={schema},public"})
        .render_as_string(hide_password=False)
    )
    try:
        yield scoped
    finally:
        with admin.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()


def test_postgres_cache_selection_serializes_with_eviction(
    database_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, collection_id, cache = _warm(tmp_path, database_url=database_url)
    service, _ranges, _mirror = _add_mirror(service, collection_id)
    selected = threading.Event()
    release = threading.Event()
    usable = cache.is_usable_store

    def pause(**kwargs: str) -> bool:
        selected.set()
        assert release.wait(10)
        return usable(**kwargs)

    monkeypatch.setattr(cache, "is_usable_store", pause)
    with ThreadPoolExecutor(max_workers=2) as executor:
        planning = executor.submit(
            service.plan, ((collection_id, ARTIFACT),), source_store="mirror"
        )
        try:
            assert selected.wait(10)
            assert executor.submit(service.sweep).result(timeout=10) == 0
        finally:
            release.set()
        plan = planning.result(timeout=10)
    assert plan["state"] == "ready"
    assert service.sweep() == 0 and cache.deleted == []
    with session_scope(service._session_factory) as session:
        obj = session.scalar(
            select(RetrievalPlanObjectRecord).where(
                RetrievalPlanObjectRecord.plan_id == plan["id"],
            )
        )
        assert obj is not None and obj.cache_source_store == "archive"


def test_postgres_source_retirement_waits_for_a_cross_source_cache_plan(
    database_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, collection_id, cache = _warm(tmp_path, database_url=database_url)
    service, _ranges, origin_mirror = _add_mirror(service, collection_id)
    retirement = SqlAlchemyArchiveCopyRetirementService(
        service._config,
        service._archive_stores,
        session_factory=service._session_factory,
    )
    challenge = str(retirement.plan(collection_id, store="archive")["challenge"])
    assert challenge != "None"
    selected = threading.Event()
    release = threading.Event()
    retire_started = threading.Event()
    usable = cache.is_usable_store

    def pause(**kwargs: str) -> bool:
        selected.set()
        assert release.wait(10)
        return usable(**kwargs)

    def before_query(_conn: object, _cursor: object, statement: str, *_args: object) -> None:
        if "FROM collections" in statement and "FOR UPDATE" in statement:
            retire_started.set()

    monkeypatch.setattr(cache, "is_usable_store", pause)
    engine = service._session_factory.kw["bind"]
    event.listen(engine, "before_cursor_execute", before_query)
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            planning = executor.submit(
                service.plan, ((collection_id, ARTIFACT),), source_store="mirror"
            )
            try:
                assert selected.wait(10)
                retire_started.clear()
                retiring = executor.submit(
                    retirement.retire, collection_id, store="archive", challenge=challenge
                )
                assert retire_started.wait(10)
                with pytest.raises(TimeoutError):
                    retiring.result(timeout=0.25)
            finally:
                release.set()
            assert planning.result(timeout=10)["state"] == "ready"
            with pytest.raises(Conflict, match="plan changed|blocked"):
                retiring.result(timeout=10)
    finally:
        event.remove(engine, "before_cursor_execute", before_query)
    assert origin_mirror.deleted == []
    assert retirement.plan(collection_id, store="archive")["challenge"] is None
