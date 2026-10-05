from __future__ import annotations

from pathlib import Path

import pytest
from riverhog_core.catalog_db import make_session_factory, session_scope
from riverhog_core.catalog_models import (
    ArchiveCopyRetirementRecord,
    CollectionArchiveCopyRecord,
    CollectionArchiveObjectRecord,
    RetrievalCacheLeaseRecord,
    RetrievalCacheObjectRecord,
    RetrievalCachePopulationClaimRecord,
    RetrievalCachePopulationRecord,
    RetrievalCacheStoreAccountingRecord,
)
from riverhog_core.domain.retrieval_cache import RetrievalCacheReceipt
from riverhog_core.ports.archive_objects import CompletedObjectReceipt, WriteSession
from riverhog_core.services.archive_copy_retirements import SqlAlchemyArchiveCopyRetirementService
from riverhog_core.services.retrieval_cache import SqlAlchemyRetrievalCache, register_cache_ready
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from tests.unit.archive_object_fixtures import COLLECTION_ID
from tests.unit.storage_incarnation_fixtures import seed_storage_incarnation
from tests.unit.test_archive_copy_retirements import _service
from tests.unit.test_retrieval_cache_coordinator import _Candidate, _registration

STAMP = "2026-08-08T00:00:00.000000000Z"


class Cache(_Candidate):
    def __init__(self) -> None:
        super().__init__("local")
        self.objects: set[str] = set()
        self.fail_delete = False
        self.fail_abort = False
        self.available = True

    def is_current_incarnation(self, incarnation_id: str) -> bool:
        return self.available and super().is_current_incarnation(incarnation_id)

    def delete(self, *, object_path: str, revision: str | None) -> None:
        super().delete(object_path=object_path, revision=revision)
        self.objects.discard(object_path)
        if self.fail_delete:
            self.fail_delete = False
            raise RuntimeError("lost cache deletion response")

    def abort_write(self, *, session: WriteSession) -> None:
        if self.fail_abort:
            self.fail_abort = False
            raise RuntimeError("cache abort unavailable")
        super().abort_write(session=session)


def setup(
    tmp_path: Path, *, placements: tuple[str, ...] = ("deep",), database_url: str | None = None
):
    config, deep, b2, original = _service(tmp_path / "catalog.sqlite3", database_url=database_url)
    factory = make_session_factory(config.database_url)
    with session_scope(factory) as session:
        incarnation = seed_storage_incarnation(session, "cache", "local")
    candidate = Cache()
    cache = SqlAlchemyRetrievalCache(
        {"local": candidate},  # type: ignore[arg-type]
        {"local": _registration("local")},
        session_factory=factory,
    )
    service = SqlAlchemyArchiveCopyRetirementService(
        config, original._archive_stores, retrieval_cache=cache, session_factory=factory
    )
    identity = None
    size = 0
    with session_scope(factory) as session:
        for source in placements:
            obj = session.scalar(
                select(CollectionArchiveObjectRecord)
                .where(
                    CollectionArchiveObjectRecord.collection_id == COLLECTION_ID,
                    CollectionArchiveObjectRecord.store == source,
                    CollectionArchiveObjectRecord.kind.in_({"pack", "segment"}),
                )
                .limit(1)
            )
            assert obj is not None
            path = f"cache/{source}/{obj.object_id}"
            session.add(
                RetrievalCacheObjectRecord(
                    source_store=source,
                    source_incarnation_id=seed_storage_incarnation(session, "archive", source),
                    collection_id=COLLECTION_ID,
                    object_id=obj.object_id,
                    cache_store="local",
                    cache_incarnation_id=incarnation,
                    object_path=path,
                    revision="cache-revision",
                    stored_bytes=obj.stored_bytes,
                    stored_sha256=obj.stored_sha256,
                    cached_at=STAMP,
                    verified_at=STAMP,
                    state="ready",
                )
            )
            candidate.objects.add(path)
            accounting = session.get(RetrievalCacheStoreAccountingRecord, "local")
            assert accounting is not None
            accounting.committed_bytes += obj.stored_bytes
            if source == "deep":
                identity = (source, COLLECTION_ID, obj.object_id)
                size = obj.stored_bytes
    return service, cache, candidate, factory, deep, b2, identity, size


@pytest.mark.parametrize("lease_count", [1, 101])
def test_retirement_preserves_live_lease_and_deletes_only_its_owned_cache(
    tmp_path: Path, lease_count: int
) -> None:
    service, cache, candidate, factory, deep, _b2, key, size = setup(
        tmp_path, placements=("deep", "b2")
    )
    assert key is not None
    with session_scope(factory) as session:
        for number in range(lease_count):
            session.add(
                RetrievalCacheLeaseRecord(
                    owner=f"new-archive-{number}",
                    source_store=key[0],
                    collection_id=key[1],
                    object_id=key[2],
                    expires_at="2099-01-01T00:00:00.000000000Z",
                )
            )
    plan = service.plan(COLLECTION_ID, store="deep")
    assert "retrieval cache lease is active on the selected copy" in plan["blockers"]
    assert plan["challenge"] is None and candidate.delete_calls == [] and deep.deleted == []
    with session_scope(factory) as session:
        for lease in session.scalars(select(RetrievalCacheLeaseRecord)):
            lease.expires_at = STAMP
    challenge = str(service.plan(COLLECTION_ID, store="deep")["challenge"])
    assert service.retire(COLLECTION_ID, store="deep", challenge=challenge)["status"] == "deleted"
    with session_scope(factory) as session:
        assert session.get(RetrievalCacheObjectRecord, key) is None
        assert session.scalar(select(RetrievalCacheLeaseRecord)) is None
        remaining = session.scalar(select(RetrievalCacheObjectRecord))
        accounting = session.get(RetrievalCacheStoreAccountingRecord, "local")
        assert remaining is not None and remaining.source_store == "b2"
        assert accounting is not None and accounting.committed_bytes == remaining.stored_bytes
        generation = accounting.generation
    assert len(candidate.objects) == 1
    assert (
        service.retire(COLLECTION_ID, store="deep", challenge=challenge)["status"]
        == "already_absent"
    )
    with session_scope(factory) as session:
        assert session.get(RetrievalCacheStoreAccountingRecord, "local").generation == generation


@pytest.mark.parametrize("unavailable", [False, True])
def test_retirement_retains_cache_truth_and_accounting_until_verified_deletion(
    tmp_path: Path, unavailable: bool
) -> None:
    service, cache, candidate, factory, deep, _b2, key, size = setup(tmp_path)
    challenge = str(service.plan(COLLECTION_ID, store="deep")["challenge"])
    candidate.available = not unavailable
    candidate.fail_delete = not unavailable
    with pytest.raises(
        RuntimeError, match="incarnation is unavailable|lost cache deletion response"
    ):
        service.retire(COLLECTION_ID, store="deep", challenge=challenge)
    assert deep.deleted == []
    assert (candidate.delete_calls == []) == unavailable
    with session_scope(factory) as session:
        row = session.get(RetrievalCacheObjectRecord, key)
        assert row is not None and row.state == "delete_pending"
        assert session.get(RetrievalCacheStoreAccountingRecord, "local").committed_bytes == size
        assert session.get(CollectionArchiveCopyRecord, (COLLECTION_ID, "deep")) is not None
        assert session.get(ArchiveCopyRetirementRecord, (COLLECTION_ID, "deep")) is not None
    candidate.available = True
    restarted = SqlAlchemyArchiveCopyRetirementService(
        service._config, service._archive_stores, retrieval_cache=cache, session_factory=factory
    )
    assert restarted.retire(COLLECTION_ID, store="deep", challenge=challenge)["status"] == "deleted"
    with session_scope(factory) as session:
        assert session.get(RetrievalCacheStoreAccountingRecord, "local").committed_bytes == 0
        assert session.get(RetrievalCacheObjectRecord, key) is None
    assert candidate.objects == set()


def test_retirement_settles_abandoned_population_and_fences_late_writers(tmp_path: Path) -> None:
    service, cache, candidate, factory, deep, _b2, _key, _size = setup(tmp_path, placements=())
    admission = cache.admit(
        owner="writer",
        source_store="deep",
        collection_id=COLLECTION_ID,
        object_id="staged-volume",
        expected_bytes=60,
    )
    assert admission is not None
    plan = service.plan(COLLECTION_ID, store="deep")
    assert "retrieval cache population is active on the selected copy" in plan["blockers"]
    assert plan["challenge"] is None
    candidate.fail_abort = True
    with pytest.raises(RuntimeError, match="cache abort unavailable"):
        cache.release(owner="writer")
    challenge = str(service.plan(COLLECTION_ID, store="deep")["challenge"])
    candidate.fail_abort = True
    with pytest.raises(RuntimeError, match="cache abort unavailable"):
        service.retire(COLLECTION_ID, store="deep", challenge=challenge)
    assert deep.deleted == []
    with session_scope(factory) as session:
        population = session.get(
            RetrievalCachePopulationRecord, ("deep", COLLECTION_ID, "staged-volume")
        )
        assert population is not None and population.state == "abandoning"
        assert session.get(RetrievalCacheStoreAccountingRecord, "local").reserved_bytes == 60
    assert (
        cache.admit(
            owner="late",
            source_store="deep",
            collection_id=COLLECTION_ID,
            object_id="late-volume",
            expected_bytes=60,
        )
        is None
    )
    with pytest.raises(RuntimeError, match="source copy is retiring"):
        with session_scope(factory) as session:
            register_cache_ready(
                session,
                source_store="deep",
                collection_id=COLLECTION_ID,
                object_id="staged-volume",
                receipt=RetrievalCacheReceipt(
                    cache_store="local",
                    object_path=admission.object_path,
                    revision=None,
                    stored_bytes=60,
                    stored_sha256=None,
                    cached_at=STAMP,
                    verified_at=STAMP,
                ),
            )
    assert service.retire(COLLECTION_ID, store="deep", challenge=challenge)["status"] == "deleted"
    with session_scope(factory) as session:
        assert session.scalar(select(RetrievalCachePopulationRecord)) is None
        assert session.scalar(select(RetrievalCachePopulationClaimRecord)) is None
        assert session.get(RetrievalCacheStoreAccountingRecord, "local").reserved_bytes == 0
    assert (
        cache.admit(
            owner="after",
            source_store="deep",
            collection_id=COLLECTION_ID,
            object_id="after-volume",
            expected_bytes=60,
        )
        is None
    )


@pytest.mark.parametrize("unavailable", [False, True])
def test_retirement_cleans_completed_unpublished_population_before_removing_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, unavailable: bool
) -> None:
    service, cache, candidate, factory, deep, _b2, _key, _size = setup(tmp_path, placements=())
    admission = cache.admit(
        owner="writer",
        source_store="deep",
        collection_id=COLLECTION_ID,
        object_id="completed-volume",
        expected_bytes=60,
    )
    assert admission is not None
    completed = CompletedObjectReceipt(admission.object_path, "completed-revision", None, 60, STAMP)
    candidate.objects.add(completed.object_path)
    monkeypatch.setattr(
        candidate,
        "find_completed_population",
        lambda **_: completed if completed.object_path in candidate.objects else None,
    )
    with session_scope(factory) as session:
        session.delete(
            session.get(
                RetrievalCachePopulationClaimRecord,
                ("writer", "deep", COLLECTION_ID, "completed-volume"),
            )
        )
    challenge = str(service.plan(COLLECTION_ID, store="deep")["challenge"])
    candidate.available = not unavailable
    candidate.fail_delete = not unavailable
    with pytest.raises(
        RuntimeError, match="incarnation is unavailable|lost cache deletion response"
    ):
        service.retire(COLLECTION_ID, store="deep", challenge=challenge)
    assert deep.deleted == []
    assert (candidate.delete_calls == []) == unavailable
    with session_scope(factory) as session:
        population = session.get(
            RetrievalCachePopulationRecord, ("deep", COLLECTION_ID, "completed-volume")
        )
        assert population is not None and population.state == "abandoning"
        accounting = session.get(RetrievalCacheStoreAccountingRecord, "local")
        assert accounting.reserved_bytes == 60 and accounting.committed_bytes == 0
    candidate.available = True
    assert service.retire(COLLECTION_ID, store="deep", challenge=challenge)["status"] == "deleted"
    with session_scope(factory) as session:
        assert session.scalar(select(RetrievalCachePopulationRecord)) is None
        assert session.get(RetrievalCacheStoreAccountingRecord, "local").reserved_bytes == 0
    assert candidate.objects == set()


def test_archive_copy_catalog_cannot_cascade_away_cache_ownership(tmp_path: Path) -> None:
    _service_, _cache, candidate, factory, _deep, _b2, key, _size = setup(tmp_path)
    with pytest.raises(IntegrityError):
        with session_scope(factory) as session:
            session.delete(session.get(CollectionArchiveCopyRecord, (COLLECTION_ID, "deep")))
    with session_scope(factory) as session:
        assert session.get(RetrievalCacheObjectRecord, key) is not None
    assert len(candidate.objects) == 1
