from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from riverhog_age import CHUNK_SIZE, UploadState
from riverhog_api.deps import get_container
from riverhog_api.routers.retrieval import router
from riverhog_api.schemas.retrieval import RetrievalPlanArtifactPageOut, RetrievalPlanOut
from riverhog_core.app_permissions import RETRIEVAL_MANAGE, ApplicationAccess, Principal
from riverhog_core.archive_store_registry import ArchiveStoreBinding, ArchiveStoreRegistry
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import (
    ArchiveCopyRetirementRecord,
    CollectionArchiveArtifactObjectRecord,
    CollectionArchiveCopyRecord,
    CollectionArchiveObjectRecord,
    CollectionTagPublicationRecord,
    RetrievalCacheObjectRecord,
    RetrievalCacheStoreAccountingRecord,
    RetrievalPlanArtifactRecord,
    RetrievalPlanObjectRecord,
)
from riverhog_core.pack_retrieval import PackRangeRetrievalPolicy
from riverhog_core.services.archive_copy_retirements import (
    SqlAlchemyArchiveCopyRetirementService,
)
from riverhog_core.services.retrieval import SqlAlchemyRetrievalService
from riverhog_core.services.retrieval_cache import SqlAlchemyRetrievalCache
from riverhog_core.services.retrieval_selection import _payload_identity
from riverhog_protocol.errors import BadRequest, Conflict, InvalidState
from sqlalchemy import select

from tests.unit.storage_incarnation_fixtures import (
    fixture_storage_incarnation_id,
    seed_storage_incarnation,
)
from tests.unit.test_retrieval_cache_coordinator import _Candidate, _registration
from tests.unit.test_retrieval_service import (
    DirectArchiveStore,
    MemoryArchiveRangeStore,
    MemoryRetrievalCache,
    RecordingDownloadAllowance,
    _drive_requested,
    _ready_job,
    _seed_collection,
)

ARTIFACT = "1" * 64
OTHER = "2" * 64
PAYLOAD = b"cache reuse across exact archive copies"


class _CacheCandidate(_Candidate):
    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.incarnation_checks: list[str] = []

    def is_current_incarnation(self, incarnation_id: str) -> bool:
        self.incarnation_checks.append(incarnation_id)
        return incarnation_id == fixture_storage_incarnation_id("cache", self.name)


def _values(record: Any) -> dict[str, Any]:
    return {
        column.name: getattr(record, column.name)
        for column in record.__table__.columns
        if column.computed is None
    }


def _add_mirror(
    service: SqlAlchemyRetrievalService,
    collection_id: int,
    *,
    read_mode: str = "immediate",
    old_source_unreachable: bool = False,
    origin_probes: list[str] | None = None,
) -> tuple[SqlAlchemyRetrievalService, MemoryArchiveRangeStore, DirectArchiveStore]:
    original = service._archive_stores.require("archive")
    with session_scope(service._session_factory) as session:
        incarnation = seed_storage_incarnation(session, "archive", "mirror")
        copy = session.get(CollectionArchiveCopyRecord, (collection_id, "archive"))
        assert copy is not None
        session.add(
            CollectionArchiveCopyRecord(
                **{**_values(copy), "store": "mirror", "incarnation_id": incarnation}
            )
        )
        session.flush()
        objects = session.scalars(
            select(CollectionArchiveObjectRecord).where(
                CollectionArchiveObjectRecord.collection_id == collection_id,
                CollectionArchiveObjectRecord.store == "archive",
            )
        ).all()
        for obj in objects:
            session.add(CollectionArchiveObjectRecord(**{**_values(obj), "store": "mirror"}))
        session.flush()
        placements = session.scalars(
            select(CollectionArchiveArtifactObjectRecord).where(
                CollectionArchiveArtifactObjectRecord.collection_id == collection_id,
                CollectionArchiveArtifactObjectRecord.store == "archive",
            )
        ).all()
        for placement in placements:
            session.add(
                CollectionArchiveArtifactObjectRecord(**{**_values(placement), "store": "mirror"})
            )
        tags = session.get(CollectionTagPublicationRecord, (collection_id, "archive"))
        if tags is not None:
            session.add(
                CollectionTagPublicationRecord(
                    **{
                        **_values(tags),
                        "store": "mirror",
                        "incarnation_id": incarnation,
                    }
                )
            )
    registration = replace(
        service._config.archive_store("archive"),
        name="mirror",
        base_url="https://mirror.example.test",
    )
    config = replace(
        service._config,
        archive_stores={**service._config.archive_stores, "mirror": registration},
        archive_read_order=("mirror", "archive"),
    )
    ranges = MemoryArchiveRangeStore(original.resumable_objects)  # type: ignore[arg-type]
    store = DirectArchiveStore(original.resumable_objects, read_mode=read_mode)  # type: ignore[arg-type]

    def unreachable() -> None:
        if origin_probes is not None:
            origin_probes.append("archive")
        raise AssertionError("historical archive adapter must not be probed for a cache hit")

    registry = ArchiveStoreRegistry(
        {
            "archive": original,
            "mirror": ArchiveStoreBinding(
                incarnation_id=incarnation,
                store=store,
                resumable_objects=original.resumable_objects,
                immutable_objects=original.immutable_objects,
                object_ranges=ranges,
            ),
        },
        probes={"archive": unreachable} if old_source_unreachable else None,
    )
    return (
        SqlAlchemyRetrievalService(
            config,
            registry,
            service._cache,
            service._download_allowance,
            session_factory=service._session_factory,
        ),
        ranges,
        store,
    )


def _warm(
    tmp_path: Path,
    *,
    raw: bool = False,
    files: dict[str, bytes] | None = None,
    allowance: RecordingDownloadAllowance | None = None,
    database_url: str | None = None,
) -> tuple[SqlAlchemyRetrievalService, int, MemoryRetrievalCache]:
    cache = MemoryRetrievalCache()
    service, collection_id, _ranges, _store = _seed_collection(
        tmp_path,
        files or {ARTIFACT: PAYLOAD},
        cache=cache,
        read_mode="restore_required",
        raw=raw,
        allowance=allowance,
        database_url=database_url,
    )
    job = _drive_requested(service, _ready_job(service, collection_id, ARTIFACT))
    assert job["state"] == "ready"
    service.acknowledge(principal_id="reader", job_id=str(job["id"]))
    return service, collection_id, cache


def _create(service: SqlAlchemyRetrievalService, plan: dict[str, object]) -> dict[str, object]:
    return service.create(
        principal_id="reader", plan_id=str(plan["id"]), plan_etag=str(plan["etag"])
    )


def _content(
    service: SqlAlchemyRetrievalService, job: dict[str, object], collection_id: int, artifact: str
) -> bytes:
    chunks, _bytes, _sha256 = service.content(
        principal_id="reader",
        job_id=str(job["id"]),
        collection_id=collection_id,
        artifact_id=artifact,
    )
    return b"".join(chunks)


@pytest.mark.parametrize("explicit", [False, True])
@pytest.mark.parametrize("raw", [False, True])
def test_equivalent_cache_beats_selected_archive_without_probing_its_historical_source(
    tmp_path: Path, explicit: bool, raw: bool
) -> None:
    service, collection_id, cache = _warm(tmp_path, raw=raw)
    probes: list[str] = []
    service, ranges, mirror = _add_mirror(
        service,
        collection_id,
        old_source_unreachable=True,
        origin_probes=probes,
    )
    plan = service.plan(
        ((collection_id, ARTIFACT),),
        source_store="mirror" if explicit else None,
        restore_policy="never",
    )
    assert plan["state"] == "ready" and plan["requires_restore"] is False
    assert plan["source_store"] == ("mirror" if explicit else None)
    RetrievalPlanOut.model_validate(plan)
    page = service.list_plan_artifacts(
        principal_id="",
        plan_id=str(plan["id"]),
        etag=str(plan["etag"]),
        start_ordinal=0,
        page_size=100,
    )
    RetrievalPlanArtifactPageOut.model_validate(page)
    assert page["artifacts"][0]["source_store"] == "mirror"
    with session_scope(service._session_factory) as session:
        planned = session.scalar(
            select(RetrievalPlanObjectRecord).where(RetrievalPlanObjectRecord.plan_id == plan["id"])
        )
        assert planned is not None
        assert (planned.source_store, planned.cache_source_store, planned.cache_store) == (
            "mirror",
            "archive",
            "memory",
        )
    assert _content(service, _create(service, plan), collection_id, ARTIFACT) == PAYLOAD
    assert cache.range_requests
    assert ranges.requests == [] and mirror.prepare_calls == 0
    assert probes == []


@pytest.mark.parametrize(
    "failure", ["unknown", "missing", "retiring", "unavailable", "incarnation"]
)
def test_explicit_source_fails_strictly_even_when_another_archive_and_cache_are_usable(
    tmp_path: Path, failure: str
) -> None:
    service, collection_id, _cache = _warm(tmp_path)
    service, ranges, mirror = _add_mirror(service, collection_id)
    if failure == "unknown":
        with pytest.raises(BadRequest, match="not configured"):
            service.plan(((collection_id, ARTIFACT),), source_store="absent")
        return
    with session_scope(service._session_factory) as session:
        copy = session.get(CollectionArchiveCopyRecord, (collection_id, "mirror"))
        assert copy is not None
        if failure == "missing":
            session.delete(copy)
        elif failure == "retiring":
            session.add(
                ArchiveCopyRetirementRecord(
                    collection_id=collection_id,
                    store="mirror",
                    incarnation_id=copy.incarnation_id,
                    challenge="retiring",
                    plan_json="{}",
                    started_at=copy.last_uploaded_at,
                )
            )
    if failure in {"unavailable", "incarnation"}:
        original = service._archive_stores.require("archive")
        binding = service._archive_stores.require("mirror")
        service._archive_stores = ArchiveStoreRegistry(
            {"archive": original, "mirror": replace(binding, incarnation_id="different")}
            if failure == "incarnation"
            else {"archive": original},
            unavailable={"mirror": "temporarily unavailable"},
        )
    plan = service.plan(((collection_id, ARTIFACT),), source_store="mirror")
    assert plan["state"] == "failed"
    assert "no readable archive copy" in str(plan["failure"])
    assert ranges.requests == [] and mirror.prepare_calls == 0


@pytest.mark.parametrize("raw", [False, True])
def test_cache_equivalence_excludes_provider_placement_and_optional_stored_hash(
    tmp_path: Path, raw: bool
) -> None:
    service, collection_id, _cache = _warm(tmp_path, raw=raw)
    service, ranges, _mirror = _add_mirror(service, collection_id)
    with session_scope(service._session_factory) as session:
        for obj in session.scalars(
            select(CollectionArchiveObjectRecord).where(
                CollectionArchiveObjectRecord.collection_id == collection_id,
                CollectionArchiveObjectRecord.store == "mirror",
                CollectionArchiveObjectRecord.kind.in_({"pack", "segment"}),
            )
        ):
            obj.object_path = "provider/local/changed/path"
            obj.revision = "provider-local-revision"
            if not raw:
                obj.archive_parts_json = "[]"
            obj.stored_sha256 = None
    plan = service.plan(((collection_id, ARTIFACT),), source_store="mirror")
    assert plan["state"] == "ready" and plan["requires_restore"] is False
    assert _content(service, _create(service, plan), collection_id, ARTIFACT) == PAYLOAD
    assert ranges.requests == []


@pytest.mark.parametrize("state", ["incomplete", "unverified", "retiring"])
def test_cache_reuse_requires_valid_nonretiring_durable_source_provenance(
    tmp_path: Path,
    state: str,
) -> None:
    service, collection_id, cache = _warm(tmp_path)
    service, ranges, _mirror = _add_mirror(service, collection_id)
    with session_scope(service._session_factory) as session:
        source = session.get(CollectionArchiveCopyRecord, (collection_id, "archive"))
        assert source is not None
        if state == "incomplete":
            source.state = "retrying"
        elif state == "unverified":
            source.last_verified_at = None
        else:
            session.add(
                ArchiveCopyRetirementRecord(
                    collection_id=collection_id,
                    store="archive",
                    incarnation_id=source.incarnation_id,
                    challenge="retiring",
                    plan_json="{}",
                    started_at=source.last_uploaded_at,
                )
            )
    plan = service.plan(((collection_id, ARTIFACT),), source_store="mirror")
    assert plan["state"] == "ready"
    assert _content(service, _create(service, plan), collection_id, ARTIFACT) == PAYLOAD
    assert len(ranges.requests) == 1 and cache.range_requests == []


def test_full_cache_cannot_substitute_for_selectable_archive_authority(tmp_path: Path) -> None:
    service, collection_id, _cache = _warm(tmp_path)
    service._archive_stores = ArchiveStoreRegistry({}, unavailable={"archive": "unavailable"})
    plan = service.plan(((collection_id, ARTIFACT),))
    assert plan["state"] == "failed" and plan["etag"] is None
    assert "no readable archive copy" in str(plan["failure"])


@pytest.mark.parametrize(
    "field",
    [
        "kind",
        "plaintext_bytes",
        "stored_bytes",
        "sha256",
        "age_state_json",
        "plan_sha256",
        "index_sha256",
        "stored_sha256",
        "cache_stored_bytes",
        "cache_stored_sha256",
    ],
)
def test_disagreement_between_sealed_complete_copies_fails_closed(
    tmp_path: Path, field: str
) -> None:
    service, collection_id, _cache = _warm(tmp_path)
    service, _ranges, _mirror = _add_mirror(service, collection_id)
    with session_scope(service._session_factory) as session:
        obj = session.scalar(
            select(CollectionArchiveObjectRecord).where(
                CollectionArchiveObjectRecord.collection_id == collection_id,
                CollectionArchiveObjectRecord.store == "mirror",
                CollectionArchiveObjectRecord.kind == "pack",
            )
        )
        assert obj is not None
        if field == "kind":
            obj.kind = "segment"
        elif field in {"plaintext_bytes", "stored_bytes"}:
            setattr(obj, field, getattr(obj, field) + 1)
        elif field == "age_state_json":
            obj.age_state_json = '{"different":"sealed-state"}'
        elif field == "cache_stored_bytes":
            cached = session.get(
                RetrievalCacheObjectRecord, ("archive", collection_id, obj.object_id)
            )
            assert cached is not None
            cached.stored_bytes += 1
        elif field == "cache_stored_sha256":
            cached = session.get(
                RetrievalCacheObjectRecord, ("archive", collection_id, obj.object_id)
            )
            source = session.get(
                CollectionArchiveObjectRecord, (collection_id, "archive", obj.object_id)
            )
            assert cached is not None and source is not None
            source.stored_sha256 = obj.stored_sha256 = cached.stored_sha256
            cached.stored_sha256 = "b" * 64
        else:
            if field == "stored_sha256":
                source = session.get(
                    CollectionArchiveObjectRecord, (collection_id, "archive", obj.object_id)
                )
                assert source is not None
                source.stored_sha256 = "a" * 64
            setattr(obj, field, "b" * 64)
    plan = service.plan(((collection_id, ARTIFACT),), source_store="mirror")
    assert plan["state"] == "failed" and plan["etag"] is None
    assert "sealed" in str(plan["failure"])


@pytest.mark.parametrize(
    "field", ["artifact_offset", "object_offset", "bytes", "missing", "duplicate"]
)
def test_raw_cache_equivalence_checks_the_exact_unique_artifact_placement(
    tmp_path: Path, field: str
) -> None:
    service, collection_id, _cache = _warm(tmp_path, raw=True)
    service, _ranges, _mirror = _add_mirror(service, collection_id)
    with session_scope(service._session_factory) as session:
        placement = session.scalar(
            select(CollectionArchiveArtifactObjectRecord).where(
                CollectionArchiveArtifactObjectRecord.collection_id == collection_id,
                CollectionArchiveArtifactObjectRecord.store == "archive",
            )
        )
        assert placement is not None
        if field == "missing":
            session.delete(placement)
        elif field == "duplicate":
            session.add(
                CollectionArchiveArtifactObjectRecord(**{**_values(placement), "sequence": 1})
            )
        else:
            setattr(placement, field, getattr(placement, field) + 1)
    plan = service.plan(((collection_id, ARTIFACT),), source_store="mirror")
    assert plan["state"] == "failed"
    assert "sealed" in str(plan["failure"])


@pytest.mark.parametrize("read_mode", ["immediate", "restore_required"])
def test_mixed_cache_coverage_reads_only_misses_from_the_explicit_archive(
    tmp_path: Path,
    read_mode: str,
) -> None:
    allowance = RecordingDownloadAllowance()
    service, collection_id, cache = _warm(
        tmp_path,
        raw=True,
        files={ARTIFACT: PAYLOAD, OTHER: b"cold payload"},
        allowance=allowance,
    )
    service, ranges, mirror = _add_mirror(service, collection_id, read_mode=read_mode)
    plan = service.plan(((collection_id, ARTIFACT), (collection_id, OTHER)), source_store="mirror")
    job = _drive_requested(service, _create(service, plan))
    assert _content(service, job, collection_id, ARTIFACT) == PAYLOAD
    assert ranges.requests == []
    assert _content(service, job, collection_id, OTHER) == b"cold payload"
    assert cache.range_requests
    if read_mode == "immediate":
        assert len(ranges.requests) == 1 and mirror.prepare_calls == 0
        assert [source for source, _bytes, _attribution in allowance.tracked] == [
            "retrieval-cache",
            "mirror",
        ]
    else:
        assert ranges.requests == [] and mirror.prepare_calls == 1
        with session_scope(service._session_factory) as session:
            assert set(session.scalars(select(RetrievalCacheObjectRecord.source_store))) == {
                "archive",
                "mirror",
            }


def test_restore_never_uses_full_cache_and_rejects_the_cold_selected_source(tmp_path: Path) -> None:
    service, collection_id, _cache = _warm(
        tmp_path, raw=True, files={ARTIFACT: PAYLOAD, OTHER: b"cold"}
    )
    service, ranges, mirror = _add_mirror(service, collection_id, read_mode="restore_required")
    hit = service.plan(((collection_id, ARTIFACT),), source_store="mirror", restore_policy="never")
    assert _create(service, hit)["state"] == "ready"
    cold = service.plan(((collection_id, OTHER),), source_store="mirror", restore_policy="never")
    assert cold["requires_restore"] is True
    with pytest.raises(Conflict, match="restore"):
        _create(service, cold)
    assert mirror.prepare_calls == 0 and ranges.requests == []


def test_sealed_sources_survive_restart_replay_and_read_order_drift(tmp_path: Path) -> None:
    service, collection_id, cache = _warm(tmp_path)
    service, ranges, _mirror = _add_mirror(service, collection_id)
    plan = service.plan(((collection_id, ARTIFACT),), source_store="mirror", idempotency_key="same")
    with pytest.raises(Conflict, match="idempotency identity changed"):
        service.plan(((collection_id, ARTIFACT),), source_store="archive", idempotency_key="same")
    restarted = SqlAlchemyRetrievalService(
        replace(service._config, archive_read_order=("archive", "mirror")),
        service._archive_stores,
        cache,
        session_factory=service._session_factory,
    )
    assert (
        restarted.plan(((collection_id, ARTIFACT),), source_store="mirror", idempotency_key="same")
        == plan
    )
    assert _content(restarted, _create(restarted, plan), collection_id, ARTIFACT) == PAYLOAD
    assert ranges.requests == []


def test_cross_source_cache_is_protected_for_plan_job_renewal_and_retirement(
    tmp_path: Path,
) -> None:
    service, collection_id, cache = _warm(tmp_path)
    service, _ranges, _mirror = _add_mirror(service, collection_id)
    plan = service.plan(((collection_id, ARTIFACT),), source_store="mirror")
    retirement = SqlAlchemyArchiveCopyRetirementService(service._config, service._archive_stores)
    assert service.cache_status()["protected_objects"] == 1
    assert service.sweep() == 0
    assert retirement.plan(collection_id, store="archive")["blockers"]
    job = _create(service, plan)
    renewed = service.renew(principal_id="reader", job_id=str(job["id"]), lease=timedelta(hours=36))
    assert renewed["state"] == "ready" and renewed["lease_seconds"] == 36 * 3600
    with session_scope(service._session_factory) as session:
        obj = session.scalar(
            select(RetrievalPlanObjectRecord).where(RetrievalPlanObjectRecord.plan_id == plan["id"])
        )
        assert obj is not None
        inspection = service.get_cache_object(
            collection_id=collection_id, source_store="archive", object_id=obj.object_id
        )
        assert inspection["retrieval_job_leases"] == 1
    assert service.sweep() == 0 and cache.deleted == []
    service.acknowledge(principal_id="reader", job_id=str(job["id"]))
    assert retirement.plan(collection_id, store="archive")["blockers"] == []
    assert service.sweep() == 1 and len(cache.deleted) == 1


def test_pinned_cache_placement_cannot_be_replaced_after_sealing(tmp_path: Path) -> None:
    service, collection_id, _cache = _warm(tmp_path)
    service, _ranges, _mirror = _add_mirror(service, collection_id)
    plan = service.plan(((collection_id, ARTIFACT),), source_store="mirror")
    job = _create(service, plan)
    with session_scope(service._session_factory) as session:
        incarnation = seed_storage_incarnation(session, "cache", "replacement")
        cached = session.scalar(select(RetrievalCacheObjectRecord))
        assert cached is not None
        cached.cache_store = "replacement"
        cached.cache_incarnation_id = incarnation
    with pytest.raises(InvalidState, match="incarnation changed"):
        _content(service, job, collection_id, ARTIFACT)
    with pytest.raises(InvalidState, match="cache"):
        service.renew(principal_id="reader", job_id=str(job["id"]), lease=timedelta(hours=36))


def test_cache_admission_cannot_evict_a_cross_source_plan_or_job(tmp_path: Path) -> None:
    service, collection_id, _cache = _warm(tmp_path)
    service, _ranges, _mirror = _add_mirror(service, collection_id)
    candidate = _Candidate("memory")
    coordinator = SqlAlchemyRetrievalCache(
        {"memory": candidate},
        {"memory": _registration("memory", budget=65536)},
        session_factory=service._session_factory,
    )
    with session_scope(service._session_factory) as session:
        cached = session.scalar(select(RetrievalCacheObjectRecord))
        accounting = session.get(RetrievalCacheStoreAccountingRecord, "memory")
        assert cached is not None and accounting is not None
        accounting.committed_bytes = cached.stored_bytes
    plan = service.plan(((collection_id, ARTIFACT),), source_store="mirror")
    assert coordinator._evict_one(cache_store="memory") is False
    job = _create(service, plan)
    assert coordinator._evict_one(cache_store="memory") is False
    assert _content(service, job, collection_id, ARTIFACT) == PAYLOAD
    service.cancel(principal_id="reader", job_id=str(job["id"]))
    assert coordinator._evict_one(cache_store="memory") is True
    assert len(candidate.delete_calls) == 1
    with session_scope(service._session_factory) as session:
        accounting = session.get(RetrievalCacheStoreAccountingRecord, "memory")
        assert accounting is not None and accounting.committed_bytes == 0


def test_cache_store_order_precedes_historical_archive_source_order(tmp_path: Path) -> None:
    service, collection_id, cache = _warm(tmp_path)
    service, _ranges, _mirror = _add_mirror(service, collection_id)

    class PrioritizedCache(MemoryRetrievalCache):
        @property
        def store_names(self) -> tuple[str, ...]:
            return ("first", "memory")

        def is_usable_store(self, *, cache_store: str, incarnation_id: str) -> bool:
            return (
                cache_store in self.store_names
                and incarnation_id == fixture_storage_incarnation_id("cache", cache_store)
            )

    ordered = PrioritizedCache()
    ordered.objects = cache.objects
    service._cache = ordered
    with session_scope(service._session_factory) as session:
        incarnation = seed_storage_incarnation(session, "cache", "first")
        cached = session.scalar(select(RetrievalCacheObjectRecord))
        assert cached is not None
        session.add(
            RetrievalCacheObjectRecord(
                **{
                    **_values(cached),
                    "source_store": "mirror",
                    "source_incarnation_id": fixture_storage_incarnation_id("archive", "mirror"),
                    "cache_store": "first",
                    "cache_incarnation_id": incarnation,
                }
            )
        )
    plan = service.plan(((collection_id, ARTIFACT),))
    assert plan["state"] == "ready"
    with session_scope(service._session_factory) as session:
        obj = session.scalar(
            select(RetrievalPlanObjectRecord).where(RetrievalPlanObjectRecord.plan_id == plan["id"])
        )
        assert obj is not None
        assert (obj.cache_store, obj.cache_source_store) == ("first", "mirror")


def test_late_cache_recovery_preserves_status_admission_and_cross_source_read_priority(
    tmp_path: Path,
) -> None:
    service, collection_id, _cache = _warm(tmp_path)
    service, _ranges, _mirror = _add_mirror(service, collection_id)
    registrations = {name: _registration(name) for name in ("local", "cloud")}
    local, cloud = _CacheCandidate("local"), _CacheCandidate("cloud")
    with session_scope(service._session_factory) as session:
        for name in registrations:
            seed_storage_incarnation(session, "cache", name)
        cached = session.scalar(select(RetrievalCacheObjectRecord))
        assert cached is not None
        cached.cache_store = "cloud"
        cached.cache_incarnation_id = fixture_storage_incarnation_id("cache", "cloud")
        values = _values(cached)
    coordinator = SqlAlchemyRetrievalCache(
        {"cloud": cloud},  # type: ignore[arg-type]
        registrations,
        session_factory=service._session_factory,
    )
    service._cache = coordinator
    service._config = replace(service._config, retrieval_cache_stores=registrations)
    recovered = False
    attempts = 0

    def recover() -> None:
        nonlocal attempts
        attempts += 1
        if recovered and "local" not in coordinator._stores:
            coordinator.admit_store("local", local)  # type: ignore[arg-type]

    coordinator.set_recovery(recover)
    assert coordinator.store_names == ("local", "cloud")
    assert attempts == 0
    initial = service.plan(((collection_id, ARTIFACT),), source_store="mirror")
    with session_scope(service._session_factory) as session:
        selected = session.scalar(
            select(RetrievalPlanObjectRecord).where(
                RetrievalPlanObjectRecord.plan_id == initial["id"]
            )
        )
        assert selected is not None and selected.cache_store == "cloud"
    recovered = True
    admission = coordinator.admit(
        owner="late-recovery",
        source_store="mirror",
        collection_id=collection_id,
        object_id=str(values["object_id"]),
        expected_bytes=int(values["stored_bytes"]),
    )
    assert admission is not None and admission.cache_store == "local"
    assert len(local.begin_calls) == 1 and cloud.begin_calls == []
    assert attempts == 1
    assert tuple(coordinator._stores) == ("cloud", "local")
    assert coordinator.store_names == ("local", "cloud")
    assert [
        (store["cache_store"], store["priority"]) for store in service.cache_status()["stores"]
    ] == [("local", 1), ("cloud", 2)]
    with session_scope(service._session_factory) as session:
        session.add(
            RetrievalCacheObjectRecord(
                **{
                    **values,
                    "source_store": "mirror",
                    "source_incarnation_id": fixture_storage_incarnation_id("archive", "mirror"),
                    "cache_store": "local",
                    "cache_incarnation_id": fixture_storage_incarnation_id("cache", "local"),
                }
            )
        )
    plan = service.plan(((collection_id, ARTIFACT),), source_store="archive")
    assert plan["state"] == "ready"
    with session_scope(service._session_factory) as session:
        selected = session.scalar(
            select(RetrievalPlanObjectRecord).where(RetrievalPlanObjectRecord.plan_id == plan["id"])
        )
        assert selected is not None
        assert (selected.source_store, selected.cache_store, selected.cache_source_store) == (
            "archive",
            "local",
            "mirror",
        )
    assert attempts == 1


@pytest.mark.parametrize("warm,admitted", [(False, True), (True, True), (True, False)])
def test_cache_selection_does_not_recover_optional_stores_under_catalog_locks(
    tmp_path: Path, warm: bool, admitted: bool
) -> None:
    files = {ARTIFACT: PAYLOAD, OTHER: b"second independent raw object"}
    if warm:
        service, collection_id, _cache = _warm(tmp_path, raw=True, files=files)
        job = _drive_requested(service, _ready_job(service, collection_id, OTHER))
        service.acknowledge(principal_id="reader", job_id=str(job["id"]))
        service, _ranges, _mirror = _add_mirror(service, collection_id)
    else:
        service, collection_id, _ranges, _store = _seed_collection(
            tmp_path, files, raw=True, cache=MemoryRetrievalCache()
        )
    candidate = _CacheCandidate("memory")

    def recover() -> None:
        raise AssertionError("planning must not discover unavailable optional cache adapters")

    coordinator = SqlAlchemyRetrievalCache(
        {"memory": candidate} if admitted else {},  # type: ignore[arg-type]
        {name: _registration(name) for name in ("offline", "memory")},
        session_factory=service._session_factory,
        recover=recover,
    )
    service._cache = coordinator
    plan = service.plan(tuple((collection_id, artifact) for artifact in files))
    assert plan["state"] == "ready"
    with session_scope(service._session_factory) as session:
        objects = session.scalars(
            select(RetrievalPlanObjectRecord).where(RetrievalPlanObjectRecord.plan_id == plan["id"])
        ).all()
        assert len(objects) == 2
        assert {obj.read_mode for obj in objects} == (
            {"cache"} if warm and admitted else {"immediate"}
        )
    assert candidate.incarnation_checks == (
        [fixture_storage_incarnation_id("cache", "memory")] if warm and admitted else []
    )


@pytest.mark.parametrize("initially_warm", [False, True])
def test_cached_pack_reads_ignore_archive_fallback_range_policy(
    tmp_path: Path, initially_warm: bool
) -> None:
    payload = bytes(range(256)) * 1024
    if initially_warm:
        service, collection_id, cache = _warm(tmp_path, files={ARTIFACT: payload})
    else:
        cache = MemoryRetrievalCache()
        service, collection_id, _ranges, _store = _seed_collection(
            tmp_path, {ARTIFACT: payload}, read_mode="restore_required", cache=cache
        )
    service, ranges, _mirror = _add_mirror(service, collection_id, read_mode="restore_required")
    service._config = replace(
        service._config,
        range_policy_by_store={
            "archive": PackRangeRetrievalPolicy(
                max_request_ciphertext_bytes=CHUNK_SIZE + 16,
                billing_mode="whole_object",
            ),
            "mirror": PackRangeRetrievalPolicy(
                merge_gap_ciphertext_bytes=CHUNK_SIZE,
                max_request_ciphertext_bytes=2 * (CHUNK_SIZE + 16),
            ),
        },
    )
    requests: list[list[tuple[str, int, int]]] = []
    for source in ("archive", "mirror"):
        plan = service.plan(((collection_id, ARTIFACT),), source_store=source)
        job = _drive_requested(service, _create(service, plan))
        assert job["state"] == "ready"
        cache.range_requests.clear()
        assert _content(service, job, collection_id, ARTIFACT) == payload
        requests.append(list(cache.range_requests))
        service.acknowledge(principal_id="reader", job_id=str(job["id"]))
    assert requests[0] == requests[1] and requests[0]
    assert ranges.requests == []


def test_all_artifacts_in_one_collection_share_the_pinned_archive_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("riverhog_core.services.retrieval._RETRIEVAL_PLAN_SEGMENT_BATCH", 1)
    files = {ARTIFACT: PAYLOAD, OTHER: b"second"}
    service, collection_id, _ranges, _store = _seed_collection(tmp_path, files)
    service, _ranges, _mirror = _add_mirror(service, collection_id)
    plan = service.plan(tuple((collection_id, artifact) for artifact in files))
    assert plan["state"] == "planning"
    restarted = SqlAlchemyRetrievalService(
        replace(service._config, archive_read_order=("archive", "mirror")),
        service._archive_stores,
        service._cache,
        session_factory=service._session_factory,
    )
    ready = restarted.advance_plan(principal_id="", plan_id=str(plan["id"]))
    assert ready["state"] == "ready"
    with session_scope(service._session_factory) as session:
        assert set(
            session.scalars(
                select(RetrievalPlanArtifactRecord.source_store).where(
                    RetrievalPlanArtifactRecord.plan_id == plan["id"],
                )
            )
        ) == {"mirror"}


def test_copy_equivalence_preserves_exact_counts_above_json_safe_integer_range(
    tmp_path: Path,
) -> None:
    service, collection_id, _cache = _warm(tmp_path)
    service, _ranges, _mirror = _add_mirror(service, collection_id)
    with session_scope(service._session_factory) as session:
        source = session.scalar(
            select(CollectionArchiveObjectRecord).where(
                CollectionArchiveObjectRecord.collection_id == collection_id,
                CollectionArchiveObjectRecord.store == "archive",
                CollectionArchiveObjectRecord.kind == "pack",
            )
        )
        assert source is not None and source.age_state_json is not None
        mirror = session.get(
            CollectionArchiveObjectRecord, (collection_id, "mirror", source.object_id)
        )
        assert mirror is not None
        for obj in (source, mirror):
            obj.plaintext_bytes = 2**53 + 1
            obj.stored_bytes = 2**53 + 4097
            obj.age_state_json = (
                replace(
                    UploadState.from_json_bytes(source.age_state_json),
                    plaintext_size=obj.plaintext_bytes,
                )
                .to_json_bytes()
                .decode()
            )
        assert _payload_identity(session, source) == _payload_identity(session, mirror)
        mirror.stored_bytes += 1
        assert _payload_identity(session, source) != _payload_identity(session, mirror)


def test_http_and_openapi_expose_requested_and_resolved_archive_source(tmp_path: Path) -> None:
    service, collection_id, _cache = _warm(tmp_path)
    service, _ranges, _mirror = _add_mirror(service, collection_id)
    principal = Principal(
        id="reader",
        key_id="key",
        access=frozenset({ApplicationAccess(RETRIEVAL_MANAGE)}),
    )
    container = SimpleNamespace(
        retrieval=service,
        app_keys=SimpleNamespace(authenticate=lambda _token: principal),
    )
    app = FastAPI()
    app.include_router(router, prefix="/v1")
    app.dependency_overrides[get_container] = lambda: container
    with TestClient(app, headers={"Authorization": "Bearer fixture"}) as client:
        response = client.post(
            "/v1/retrieval-plans",
            json={
                "artifacts": [{"collection_id": str(collection_id), "artifact_id": ARTIFACT}],
                "source_store": "mirror",
                "restore_policy": "never",
                "idempotency_key": "http-plan",
            },
        )
        assert response.status_code == 200, response.text
        plan = response.json()
        assert plan["source_store"] == "mirror" and plan["requires_restore"] is False
        page = client.get(
            f"/v1/retrieval-plans/{plan['id']}/artifacts",
            headers={"If-Match": '"' + plan["etag"] + '"'},
        )
        assert page.status_code == 200
        assert page.json()["artifacts"][0]["source_store"] == "mirror"
        invalid = client.post(
            "/v1/retrieval-plans",
            json={
                "artifacts": [{"collection_id": str(collection_id), "artifact_id": ARTIFACT}],
                "source_store": "../invalid",
                "idempotency_key": "invalid-plan",
            },
        )
        assert invalid.status_code == 422
        for name in ("RetrievalPlanRequest", "RetrievalPlanOut", "RetrievalPlanArtifactOut"):
            assert (
                "source_store"
                in client.get("/openapi.json").json()["components"]["schemas"][name]["properties"]
            )
