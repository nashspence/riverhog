from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from pathlib import Path

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import (
    ArchiveCopyRetirementRecord,
    CollectionArchiveCopyRecord,
    RetrievalCacheObjectRecord,
    RetrievalCachePopulationRecord,
    RetrievalCacheStoreAccountingRecord,
)
from riverhog_protocol.errors import Conflict
from sqlalchemy import event
from sqlalchemy.exc import IntegrityError

from tests.integration.test_retrieval_selection_concurrency import database_url as database_url
from tests.unit.archive_object_fixtures import COLLECTION_ID
from tests.unit.test_archive_copy_retirement_cache import setup

pytestmark = pytest.mark.integration


def test_postgres_retirement_waits_for_cache_population_admission(
    database_url: str, tmp_path: Path
) -> None:
    service, cache, _candidate, factory, deep, _b2, _key, _size = setup(
        tmp_path, placements=(), database_url=database_url
    )
    challenge = str(service.plan(COLLECTION_ID, store="deep")["challenge"])
    anchored, release = threading.Event(), threading.Event()

    def pause(_connection, _cursor, statement, _parameters, _context, _many):
        if threading.current_thread().name.startswith("admitting") and (
            "FROM collections" in statement and "FOR UPDATE" in statement
        ):
            anchored.set()
            assert release.wait(10)

    engine = factory.kw["bind"]
    event.listen(engine, "after_cursor_execute", pause)
    try:
        with ThreadPoolExecutor(max_workers=1, thread_name_prefix="admitting") as writer:
            admission = writer.submit(
                cache.admit,
                owner="writer",
                source_store="deep",
                collection_id=COLLECTION_ID,
                object_id="volume",
                expected_bytes=60,
            )
            assert anchored.wait(10)
            with ThreadPoolExecutor(max_workers=1) as retiring:
                retirement = retiring.submit(
                    service.retire, COLLECTION_ID, store="deep", challenge=challenge
                )
                try:
                    with pytest.raises(TimeoutError):
                        retirement.result(timeout=0.25)
                finally:
                    release.set()
                assert admission.result(timeout=10) is not None
                with pytest.raises(Conflict, match="plan changed|blocked"):
                    retirement.result(timeout=10)
    finally:
        release.set()
        event.remove(engine, "after_cursor_execute", pause)
    assert deep.deleted == []
    with session_scope(factory) as session:
        assert (
            session.get(RetrievalCachePopulationRecord, ("deep", COLLECTION_ID, "volume"))
            is not None
        )
        assert session.get(ArchiveCopyRetirementRecord, (COLLECTION_ID, "deep")) is None


def test_postgres_archive_ownership_requires_physical_cache_settlement(
    database_url: str, tmp_path: Path
) -> None:
    service, _cache, candidate, factory, _deep, _b2, key, size = setup(
        tmp_path, database_url=database_url
    )
    with pytest.raises(IntegrityError):
        with session_scope(factory) as session:
            session.delete(session.get(CollectionArchiveCopyRecord, (COLLECTION_ID, "deep")))
    with session_scope(factory) as session:
        assert session.get(RetrievalCacheObjectRecord, key) is not None
        assert session.get(RetrievalCacheStoreAccountingRecord, "local").committed_bytes == size
    challenge = str(service.plan(COLLECTION_ID, store="deep")["challenge"])
    assert service.retire(COLLECTION_ID, store="deep", challenge=challenge)["status"] == "deleted"
    assert candidate.objects == set()
    with session_scope(factory) as session:
        assert session.get(RetrievalCacheObjectRecord, key) is None
        assert session.get(RetrievalCacheStoreAccountingRecord, "local").committed_bytes == 0


def test_postgres_overlapping_cache_cleanup_decrements_accounting_once(
    database_url: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, cache, candidate, factory, _deep, _b2, key, size = setup(
        tmp_path, database_url=database_url
    )
    challenge = str(service.plan(COLLECTION_ID, store="deep")["challenge"])
    candidate.fail_delete = True
    with pytest.raises(RuntimeError, match="lost cache deletion response"):
        service.retire(COLLECTION_ID, store="deep", challenge=challenge)
    effects = threading.Barrier(2, timeout=10)
    deleting = threading.Event()
    delete = candidate.delete

    def overlap(**kwargs):
        deleting.set()
        effects.wait()
        delete(**kwargs)

    monkeypatch.setattr(candidate, "delete", overlap)
    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(
            cache.settle_source, source_store="deep", collection_id=COLLECTION_ID
        )
        assert deleting.wait(10)
        second = executor.submit(
            cache.settle_source, source_store="deep", collection_id=COLLECTION_ID
        )
        cleanups = [first, second]
        for cleanup in cleanups:
            cleanup.result(timeout=20)
    with session_scope(factory) as session:
        assert session.get(RetrievalCacheObjectRecord, key) is None
        accounting = session.get(RetrievalCacheStoreAccountingRecord, "local")
        assert accounting.committed_bytes == 0 and accounting.generation == 1
        assert session.get(CollectionArchiveCopyRecord, (COLLECTION_ID, "deep")) is not None
    assert len(candidate.delete_calls) == 3
    assert size > 0 and candidate.objects == set()
    assert service.retire(COLLECTION_ID, store="deep", challenge=challenge)["status"] == "deleted"
