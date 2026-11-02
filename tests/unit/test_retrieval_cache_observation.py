from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import (
    RetrievalCacheObjectRecord,
    RetrievalCachePopulationClaimRecord,
    RetrievalCachePopulationRecord,
    RetrievalCacheStoreAccountingRecord,
)
from riverhog_core.domain.retrieval_cache import RetrievalCacheReceipt
from riverhog_core.ports.archive_objects import CompletedObjectReceipt
from riverhog_core.services.retrieval_cache import SqlAlchemyRetrievalCache
from sqlalchemy import event, select

from tests.unit.test_retrieval_cache_coordinator import (
    _Candidate,
    _coordinator,
    _registration,
    _seed_ready_object,
)

_OWNER = "collection-upload:17:volume"
_KEY = ("deep", 17, "volume")
_NOW = "2026-08-08T00:00:00.000000000Z"


def _snapshot(factory: Any) -> tuple[tuple[tuple[object, ...], ...], ...]:
    with session_scope(factory) as session:
        return tuple(
            tuple(tuple(row) for row in session.execute(select(*model.__table__.columns)))
            for model in (
                RetrievalCachePopulationRecord,
                RetrievalCachePopulationClaimRecord,
                RetrievalCacheStoreAccountingRecord,
                RetrievalCacheObjectRecord,
            )
        )


def _observe(coordinator: SqlAlchemyRetrievalCache, *, owner: str = _OWNER, size: int = 3):
    return coordinator.observe_completed_population(
        owner=owner,
        source_store=_KEY[0],
        collection_id=_KEY[1],
        object_id=_KEY[2],
        expected_bytes=size,
    )


def _receipt(candidate: _Candidate) -> RetrievalCacheReceipt:
    return RetrievalCacheReceipt(
        cache_store=candidate.name,
        object_path=candidate.object_path(*_KEY),
        revision="original-cache-revision",
        stored_bytes=3,
        stored_sha256=None,
        cached_at=_NOW,
        verified_at=_NOW,
    )


def _sealed(candidate: _Candidate) -> CompletedObjectReceipt:
    return CompletedObjectReceipt(
        object_path=candidate.object_path(*_KEY),
        revision="original-cache-revision",
        entity_token="original-entity",
        bytes=3,
        completed_at=_NOW,
    )


def test_cache_observation_has_no_creation_or_recovery_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = _Candidate("local")
    coordinator, factory = _coordinator(tmp_path, (candidate,), (_registration("local"),))
    coordinator.set_recovery(lambda: pytest.fail("observation cannot start recovery"))
    monkeypatch.setattr(coordinator, "admit", lambda **kwargs: pytest.fail("new admission"))
    monkeypatch.setattr(coordinator, "release", lambda **kwargs: pytest.fail("cleanup"))
    before = _snapshot(factory)
    assert _observe(coordinator) is None
    assert _snapshot(factory) == before
    assert candidate.begin_calls == candidate.abort_calls == candidate.delete_calls == []


@pytest.mark.parametrize("state", ["admitting", "admitted", "writing"])
@pytest.mark.parametrize("sealed", [False, True])
def test_cold_cache_observation_preserves_exact_claim_and_reservation_without_dispatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    state: str,
    sealed: bool,
) -> None:
    candidate = _Candidate("local")
    coordinator, factory = _coordinator(tmp_path, (candidate,), (_registration("local"),))
    admission = coordinator.admit(
        owner=_OWNER,
        source_store=_KEY[0],
        collection_id=_KEY[1],
        object_id=_KEY[2],
        expected_bytes=3,
    )
    assert admission is not None
    with session_scope(factory) as session:
        population = session.get(RetrievalCachePopulationRecord, _KEY)
        population.state = state
        if state == "admitting":
            population.write_token = None
    cold = SqlAlchemyRetrievalCache(
        {"local": candidate},
        {"local": _registration("local")},
        session_factory=factory,
        recover=lambda: pytest.fail("observation cannot start recovery"),
    )
    queries: list[tuple[Any, str]] = []
    engine = factory.kw["bind"]

    def record(connection, cursor, statement, parameters, context, executemany):
        queries.append((connection, statement))

    observations = []

    def completed(**kwargs):
        assert kwargs == {
            "source_store": _KEY[0],
            "collection_id": _KEY[1],
            "object_id": _KEY[2],
            "expected_bytes": 3,
        }
        assert queries and all(connection.closed for connection, _ in queries)
        observations.append(kwargs)
        return _sealed(candidate) if sealed else None

    monkeypatch.setattr(candidate, "find_completed_population", completed)
    monkeypatch.setattr(candidate, "receipt", lambda **kwargs: _receipt(candidate), raising=False)
    before = _snapshot(factory)
    event.listen(engine, "before_cursor_execute", record)
    try:
        observed = _observe(cold)
    finally:
        event.remove(engine, "before_cursor_execute", record)
    assert observed == (_receipt(candidate) if sealed else None)
    assert len(observations) == 1
    assert len([sql for _, sql in queries if sql.lstrip().upper().startswith("SELECT")]) <= 5
    assert not any(
        sql.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE", "BEGIN IMMEDIATE"))
        or "FOR UPDATE" in sql.upper()
        for _, sql in queries
    )
    assert all(
        "write_token" not in sql and "failure" not in sql
        for _, sql in queries
        if "FROM retrieval_cache_populations" in sql
    )
    assert _snapshot(factory) == before
    assert len(candidate.begin_calls) == 1
    assert candidate.abort_calls == candidate.delete_calls == []


@pytest.mark.parametrize(
    "refusal", ["different-owner", "abandoning", "changed-incarnation", "missing-store"]
)
def test_cache_observation_does_not_adopt_or_replace_an_unavailable_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    refusal: str,
) -> None:
    candidate = _Candidate("local")
    coordinator, factory = _coordinator(tmp_path, (candidate,), (_registration("local"),))
    coordinator.admit(
        owner=_OWNER,
        source_store=_KEY[0],
        collection_id=_KEY[1],
        object_id=_KEY[2],
        expected_bytes=3,
    )
    if refusal == "abandoning":
        with session_scope(factory) as session:
            session.get(RetrievalCachePopulationRecord, _KEY).state = "abandoning"
    elif refusal == "changed-incarnation":
        monkeypatch.setattr(candidate, "is_current_incarnation", lambda incarnation: False)
    elif refusal == "missing-store":
        coordinator = SqlAlchemyRetrievalCache(
            {},
            {"local": _registration("local")},
            session_factory=factory,
            recover=lambda: pytest.fail("observation cannot recover a removed store"),
        )
    monkeypatch.setattr(
        candidate,
        "find_completed_population",
        lambda **kwargs: pytest.fail("unexpected provider read"),
    )
    before = _snapshot(factory)
    assert _observe(coordinator, owner="other" if refusal == "different-owner" else _OWNER) is None
    assert _snapshot(factory) == before
    assert len(candidate.begin_calls) == 1
    assert candidate.abort_calls == candidate.delete_calls == []


@pytest.mark.parametrize("ready", [False, True])
def test_observation_refuses_changed_byte_identity_before_provider_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ready: bool,
) -> None:
    candidate = _Candidate("local")
    coordinator, factory = _coordinator(tmp_path, (candidate,), (_registration("local"),))
    if ready:
        _seed_ready_object(factory, cache_store="local", object_id="volume", stored_bytes=3)
        collection_id = 1
    else:
        coordinator.admit(
            owner=_OWNER,
            source_store=_KEY[0],
            collection_id=_KEY[1],
            object_id=_KEY[2],
            expected_bytes=3,
        )
        collection_id = 17
    monkeypatch.setattr(
        candidate,
        "find_completed_population",
        lambda **kwargs: pytest.fail("unexpected provider read"),
    )
    before = _snapshot(factory)
    with pytest.raises(RuntimeError, match="byte identity changed"):
        coordinator.observe_completed_population(
            owner=_OWNER,
            source_store="deep",
            collection_id=collection_id,
            object_id="volume",
            expected_bytes=4,
        )
    assert _snapshot(factory) == before
    assert candidate.abort_calls == candidate.delete_calls == []


def test_observation_does_not_acquire_published_placement_for_a_new_population_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = _Candidate("local")
    coordinator, factory = _coordinator(tmp_path, (candidate,), (_registration("local"),))
    _seed_ready_object(factory, cache_store="local", object_id="volume", stored_bytes=3)
    monkeypatch.setattr(
        candidate, "find_completed_population", lambda **kwargs: pytest.fail("new provider lookup")
    )
    before = _snapshot(factory)
    receipt = coordinator.observe_completed_population(
        owner=_OWNER,
        source_store="deep",
        collection_id=1,
        object_id="volume",
        expected_bytes=3,
    )
    assert receipt is None
    assert _snapshot(factory) == before
    assert candidate.begin_calls == candidate.abort_calls == candidate.delete_calls == []


def test_observation_never_probes_a_changed_retained_population_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = _Candidate("local")
    coordinator, factory = _coordinator(tmp_path, (candidate,), (_registration("local"),))
    coordinator.admit(
        owner=_OWNER,
        source_store=_KEY[0],
        collection_id=_KEY[1],
        object_id=_KEY[2],
        expected_bytes=3,
    )
    with session_scope(factory) as session:
        session.get(RetrievalCachePopulationRecord, _KEY).object_path = "objects/different"
    monkeypatch.setattr(
        candidate,
        "find_completed_population",
        lambda **kwargs: pytest.fail("changed provider target"),
    )
    before = _snapshot(factory)
    with pytest.raises(RuntimeError, match="object path changed"):
        _observe(coordinator)
    assert _snapshot(factory) == before
    assert candidate.abort_calls == candidate.delete_calls == []


def test_real_filesystem_seal_recovery_observes_without_restarting_or_disposing_population(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from riverhog_core.catalog_db import initialize_db, make_session_factory
    from riverhog_core.catalog_models import StorageIncarnationRecord
    from riverhog_core.stores.storage_adapter_retrieval_cache import StorageAdapterRetrievalCache
    from riverhog_core.throughput import ArchiveThroughputTuning, ArchiveTransferResources

    from tests.unit.db_helpers import sqlite_url
    from tests.unit.storage_incarnation_fixtures import seed_storage_incarnation
    from tests.unit.test_archive_write_execution import _filesystem

    database_url = sqlite_url(tmp_path / "catalog.sqlite3")
    initialize_db(database_url)
    factory = make_session_factory(database_url)
    with _filesystem(tmp_path / "cache") as adapter:
        with session_scope(factory) as session:
            seed_storage_incarnation(session, "archive", "deep")
            session.add(
                StorageIncarnationRecord(
                    id=adapter.descriptor().storage_incarnation_id,
                    kind="cache",
                    name="local",
                    state="bound",
                    binding_generation=1,
                    created_at=_NOW,
                    last_bound_at=_NOW,
                    last_read_mode="immediate",
                )
            )
        tuning = ArchiveThroughputTuning(
            write_concurrency=1, upload_request_concurrency=1, upload_max_inflight_bytes=2 * 1024**2
        )
        candidate = StorageAdapterRetrievalCache(
            "local",
            adapter,
            write_segment_bytes=64 * 1024,
            throughput_tuning=tuning,
            transfer_resources=ArchiveTransferResources.from_tuning(tuning),
        )
        coordinator = SqlAlchemyRetrievalCache(
            {"local": candidate}, {"local": _registration("local")}, session_factory=factory
        )
        admission = coordinator.admit(
            owner=_OWNER,
            source_store=_KEY[0],
            collection_id=_KEY[1],
            object_id=_KEY[2],
            expected_bytes=3,
        )
        assert admission is not None
        sealed = coordinator.put(admission=admission, content=(b"abc",))
        cold = SqlAlchemyRetrievalCache(
            {"local": candidate},
            {"local": _registration("local")},
            session_factory=factory,
            recover=lambda: pytest.fail("observation cannot run recovery"),
        )

        def no_mutation(*args: Any, **kwargs: Any) -> None:
            pytest.fail("completion observation cannot mutate the provider")

        for method in (
            "begin_write",
            "write_segment",
            "complete_write",
            "abort_write",
            "put_small_object",
            "delete_object",
            "delete_prefix",
        ):
            monkeypatch.setattr(adapter, method, no_mutation)
        before = _snapshot(factory)
        observed = _observe(cold)
        assert observed is not None
        assert observed.cache_store == sealed.cache_store
        assert observed.object_path == sealed.object_path
        assert observed.revision == sealed.revision
        assert observed.stored_bytes == sealed.stored_bytes == 3
        assert _snapshot(factory) == before
