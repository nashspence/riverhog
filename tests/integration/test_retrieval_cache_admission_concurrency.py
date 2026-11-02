from __future__ import annotations

import os
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from riverhog_core.catalog_db import (
    STATE_VERSION_TABLE,
    Base,
    create_catalog_engine,
    initialize_db,
    make_session_factory,
    session_scope,
)
from riverhog_core.catalog_models import RetrievalCacheStoreAccountingRecord
from riverhog_core.ports.archive_objects import WriteSession
from riverhog_core.runtime_config import (
    RetrievalCacheStoreRegistration,
    StorageAdapterRegistration,
)
from riverhog_core.services.retrieval_cache import SqlAlchemyRetrievalCache
from sqlalchemy import text

from tests.unit.storage_incarnation_fixtures import seed_storage_incarnation

pytestmark = pytest.mark.integration


@pytest.fixture
def database_url() -> Iterator[str]:
    value = os.getenv("RIVERHOG_TEST_POSTGRES_URL", "").strip()
    if not value:
        pytest.skip("RIVERHOG_TEST_POSTGRES_URL is required")
    engine = create_catalog_engine(value)
    Base.metadata.drop_all(engine)
    with engine.begin() as connection:
        connection.execute(text(f'DROP TABLE IF EXISTS "{STATE_VERSION_TABLE}"'))
    engine.dispose()
    initialize_db(value)
    with session_scope(make_session_factory(value)) as session:
        seed_storage_incarnation(session, "archive", "deep")
        seed_storage_incarnation(session, "cache", "local")
    try:
        yield value
    finally:
        engine = create_catalog_engine(value)
        Base.metadata.drop_all(engine)
        with engine.begin() as connection:
            connection.execute(text(f'DROP TABLE IF EXISTS "{STATE_VERSION_TABLE}"'))
        engine.dispose()


class _Candidate:
    name = "local"

    @staticmethod
    def object_path(source_store: str, collection_id: int, object_id: str) -> str:
        return f"objects/{source_store}/{collection_id}/{object_id}"

    def find_completed_population(self, **_: object) -> None:
        return None

    def begin_population(
        self,
        *,
        source_store: str,
        collection_id: int,
        object_id: str,
        expected_bytes: int,
    ) -> WriteSession:
        return WriteSession(
            self.object_path(source_store, collection_id, object_id),
            f"write-{object_id}",
            expected_bytes,
        )


def _cache(database_url: str) -> SqlAlchemyRetrievalCache:
    registration = RetrievalCacheStoreRegistration(
        name="local",
        adapter=StorageAdapterRegistration(
            name="local",
            base_url="https://local.example.test",
            token_file=Path("/run/secrets/local.token"),
        ),
        admission_budget_bytes=100,
    )
    return SqlAlchemyRetrievalCache(
        {"local": _Candidate()},  # type: ignore[arg-type]
        {"local": registration},
        session_factory=make_session_factory(database_url),
    )


def test_postgres_serializes_exact_cache_admission_across_service_instances(
    database_url: str,
) -> None:
    caches = (_cache(database_url), _cache(database_url))
    barrier = threading.Barrier(2)

    def admit(item: tuple[int, SqlAlchemyRetrievalCache]) -> object | None:
        index, cache = item
        barrier.wait()
        return cache.admit(
            owner=f"worker:{item[0]}",
            source_store="deep",
            collection_id=1,
            object_id=f"volume-{index}",
            expected_bytes=60,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        admissions = list(executor.map(admit, enumerate(caches)))

    assert sum(current is not None for current in admissions) == 1
    with session_scope(make_session_factory(database_url)) as session:
        accounting = session.get(RetrievalCacheStoreAccountingRecord, "local")
        assert accounting is not None
        assert accounting.reserved_bytes == 60
        assert accounting.committed_bytes == 0


def test_postgres_cache_begin_reports_after_parent_terminal_and_population_claim_loss(
    database_url, tmp_path, monkeypatch
):
    from riverhog_core.catalog_cache_write_models import CachePopulationBeginRecord

    from tests.unit import test_cache_population_begin as witness

    with witness._case(tmp_path, database_url=database_url) as case:
        ticket = witness._arm(case)
        entered, release = threading.Event(), threading.Event()
        original = case.adapter.begin_write

        def delayed(request):
            entered.set()
            assert release.wait(timeout=10)
            return original(request)

        monkeypatch.setattr(case.adapter, "begin_write", delayed)
        with ThreadPoolExecutor(max_workers=1) as pool:
            result = pool.submit(witness._execute, case, ticket)
            try:
                assert entered.wait(timeout=5)
                assert witness._terminal(case)
                assert case.cache.release(owner="upload-cache") == 0
                assert case.cache.reap_abandoned_populations() == 0
                assert witness._stored(case, ticket)[0:2] == ("armed", True)
            finally:
                release.set()
            response = result.result(timeout=10)
        assert response.outcome == "session"
        assert witness._stored(case, ticket)[0:2] == ("settled", False)
        assert witness._stored(case, ticket)[-1] == 3
        with session_scope(case.factory) as session:
            assert session.get(CachePopulationBeginRecord, ticket.call_id).result_json is not None


def test_postgres_cache_begin_waits_for_selected_parent_terminal(
    database_url, tmp_path, monkeypatch
):
    import time

    from riverhog_core.catalog_cache_write_models import CachePopulationBeginRecord
    from riverhog_core.services import archive_write_admission
    from riverhog_protocol.errors import Conflict
    from sqlalchemy import event, select

    from tests.unit import test_cache_population_begin as witness

    with witness._case(tmp_path, database_url=database_url) as case:
        entered, release, waiting = threading.Event(), threading.Event(), threading.Event()
        selected_pid, admission_pid = [], []
        execution = threading.local()
        original = archive_write_admission.freeze_registration_execution
        engine = case.factory.kw["bind"]

        def paused(session, *args, **kwargs):
            original(session, *args, **kwargs)
            selected_pid.append(session.scalar(text("SELECT pg_backend_pid()")))
            entered.set()
            assert release.wait(timeout=10)

        def capture(connection, cursor, statement, parameters, context, executemany):
            if (
                getattr(execution, "admitting", False)
                and "FROM collection_uploads" in statement
                and "FOR UPDATE" in statement
            ):
                cursor.execute("SELECT pg_backend_pid()")
                admission_pid.append(cursor.fetchone()[0])
                waiting.set()

        def admit():
            execution.admitting = True
            return witness._arm(case)

        monkeypatch.setattr(archive_write_admission, "freeze_registration_execution", paused)
        event.listen(engine, "before_cursor_execute", capture)
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                selected = pool.submit(witness._terminal, case)
                try:
                    assert entered.wait(timeout=10)
                    admitted = pool.submit(admit)
                    assert waiting.wait(timeout=10)
                    deadline, blocked = time.monotonic() + 10, False
                    with engine.connect() as observer:
                        while time.monotonic() < deadline:
                            if selected_pid[0] in observer.scalar(
                                text("SELECT pg_blocking_pids(:pid)"), {"pid": admission_pid[0]}
                            ):
                                blocked = True
                                break
                            time.sleep(0.01)
                    assert blocked
                finally:
                    release.set()
                assert selected.result(timeout=10)
                with pytest.raises(Conflict, match="admission is closed"):
                    admitted.result(timeout=10)
        finally:
            release.set()
            event.remove(engine, "before_cursor_execute", capture)
        with session_scope(case.factory) as session:
            assert session.scalar(select(CachePopulationBeginRecord.id)) is None
            assert (
                session.get(RetrievalCacheStoreAccountingRecord, case.candidate.name).reserved_bytes
                == 0
            )


@pytest.mark.parametrize("data_available", (True, False))
def test_postgres_cache_begin_rechecks_custody_expiry_after_accounting_wait(
    database_url, tmp_path, monkeypatch, data_available
):
    import time
    from dataclasses import replace

    from riverhog_core.catalog_cache_write_models import CachePopulationBeginRecord
    from riverhog_core.catalog_models import CollectionUploadRecord
    from riverhog_core.services import cache_population_begin, collection_uploads
    from riverhog_protocol.errors import Conflict
    from sqlalchemy import event, select

    from tests.unit import test_cache_population_begin as witness

    clock = ["2026-11-02T17:00:00Z"]
    monkeypatch.setattr(collection_uploads, "utc_timestamp_now", lambda: clock[0])
    monkeypatch.setattr(cache_population_begin, "utc_timestamp_now", lambda: clock[0])
    with witness._case(
        tmp_path, database_url=database_url, custody_mode="custody-transfer"
    ) as case:
        with session_scope(case.factory) as session:
            session.get(
                CollectionUploadRecord, case.collection_id
            ).lease_expires_at = "2026-11-02T18:00:00Z"
        waiting, execution, pids = threading.Event(), threading.local(), []
        engine = case.factory.kw["bind"]

        def capture(connection, cursor, statement, parameters, context, executemany):
            if (
                getattr(execution, "admitting", False)
                and "FROM retrieval_cache_store_accounting" in statement
                and "FOR UPDATE" in statement
            ):
                cursor.execute("SELECT pg_backend_pid()")
                pids.append(cursor.fetchone()[0])
                waiting.set()

        def admit():
            execution.admitting = True
            return witness._arm(
                case,
                registration=replace(
                    case.registration, admission_budget_bytes=100 if data_available else 2
                ),
            )

        blocker = case.factory()
        event.listen(engine, "before_cursor_execute", capture)
        try:
            blocker.scalar(
                select(RetrievalCacheStoreAccountingRecord)
                .where(RetrievalCacheStoreAccountingRecord.cache_store == case.candidate.name)
                .with_for_update()
            )
            blocker_pid = blocker.scalar(text("SELECT pg_backend_pid()"))
            with ThreadPoolExecutor(max_workers=1) as pool:
                admitted = pool.submit(admit)
                try:
                    assert waiting.wait(timeout=10)
                    deadline, blocked = time.monotonic() + 10, False
                    with engine.connect() as observer:
                        while time.monotonic() < deadline:
                            if blocker_pid in observer.scalar(
                                text("SELECT pg_blocking_pids(:pid)"), {"pid": pids[0]}
                            ):
                                blocked = True
                                break
                            time.sleep(0.01)
                    assert blocked
                    clock[0] = "2026-11-02T19:00:00Z"
                finally:
                    blocker.rollback()
                with pytest.raises(Conflict, match="custody lease") as refused:
                    admitted.result(timeout=10)
                assert not isinstance(
                    refused.value, cache_population_begin.CacheDataReservationUnavailable
                )
        finally:
            blocker.close()
            event.remove(engine, "before_cursor_execute", capture)
        with session_scope(case.factory) as session:
            assert session.scalar(select(CachePopulationBeginRecord.id)) is None
            assert (
                session.get(RetrievalCacheStoreAccountingRecord, case.candidate.name).reserved_bytes
                == 0
            )


def test_postgres_cache_begin_concurrent_reports_keep_closed_original_after_population_retirement(
    database_url, tmp_path
):
    from riverhog_core.catalog_models import RetrievalCachePopulationRecord
    from sqlalchemy import delete, event

    from tests.unit import test_cache_population_begin as witness

    with witness._case(tmp_path, database_url=database_url) as case:
        ticket = witness._arm(case)
        response = case.owner.execute(ticket, candidate=case.candidate)
        with session_scope(case.factory) as session:
            session.execute(delete(RetrievalCachePopulationRecord))
        barrier, execution = threading.Barrier(2), threading.local()
        engine = case.factory.kw["bind"]

        def capture(connection, cursor, statement, parameters, context, executemany):
            if (
                getattr(execution, "reporting", False)
                and not getattr(execution, "observed", False)
                and "FROM cache_population_begins" in statement
                and "FOR UPDATE" not in statement
            ):
                execution.observed = True
                barrier.wait(timeout=10)

        def report(value):
            execution.reporting = True
            case.owner.record_response(ticket, value)

        event.listen(engine, "after_cursor_execute", capture)
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                reports = [
                    pool.submit(report, response),
                    pool.submit(report, response.model_copy(update={"execution": "unknown"})),
                ]
                for result in reports:
                    result.result(timeout=10)
        finally:
            event.remove(engine, "after_cursor_execute", capture)
        assert witness._stored(case, ticket)[0:2] == ("settled", False)
        assert witness._stored(case, ticket)[-1] == 3


@pytest.mark.parametrize(
    "witness",
    ["_assert_withdrawal_survives_owner_retirement", "_assert_withdrawal_commit_and_ack_failures"],
)
def test_postgres_original_cache_withdrawal_preserves_owned_reservation(
    database_url, tmp_path, witness
):
    from tests.unit import test_cache_population_begin as cache_begin

    with cache_begin._case(tmp_path, database_url=database_url) as case:
        getattr(cache_begin, witness)(case, tmp_path)


def test_postgres_original_cache_session_parts_and_completion(database_url, tmp_path, monkeypatch):
    from tests.unit import test_cache_population_writes as witness

    with witness._writing(tmp_path, database_url=database_url) as writing:
        witness._assert_actual_session_parts_and_completion(writing, monkeypatch, "direct")


@pytest.mark.parametrize("kind", ["part", "complete"])
def test_postgres_late_http_cache_session_effect_survives_retirement(
    database_url, tmp_path, monkeypatch, kind
):
    from tests.unit import test_cache_population_writes as witness

    with witness._writing(tmp_path, database_url=database_url, mode="owned-http") as writing:
        witness._assert_late_http_effect_survives_retirement(writing, tmp_path, monkeypatch, kind)


def test_postgres_cache_session_write_rechecks_custody_after_original_begin_wait(
    database_url, tmp_path, monkeypatch
):
    import time

    from riverhog_core.catalog_cache_write_models import (
        CachePopulationBeginRecord,
        CachePopulationWriteEffectRecord,
    )
    from riverhog_core.catalog_models import CollectionUploadRecord
    from riverhog_core.services import (
        cache_population_begin,
        cache_population_writes,
        collection_uploads,
    )
    from riverhog_protocol.errors import Conflict
    from sqlalchemy import event, select

    from tests.unit import test_cache_population_writes as witness

    clock = ["2026-11-02T21:00:00Z"]
    for module in (cache_population_begin, cache_population_writes, collection_uploads):
        monkeypatch.setattr(module, "utc_timestamp_now", lambda: clock[0])
    with witness._writing(
        tmp_path, database_url=database_url, custody_mode="custody-transfer"
    ) as writing:
        case = writing.case
        with session_scope(case.factory) as session:
            session.get(
                CollectionUploadRecord, case.collection_id
            ).lease_expires_at = "2026-11-02T22:00:00Z"
        waiting, execution, pids = threading.Event(), threading.local(), []
        engine = case.factory.kw["bind"]

        def capture(connection, cursor, statement, parameters, context, executemany):
            if (
                getattr(execution, "admitting", False)
                and "FROM cache_population_begins" in statement
                and "FOR UPDATE" in statement
            ):
                cursor.execute("SELECT pg_backend_pid()")
                pids.append(cursor.fetchone()[0])
                waiting.set()

        def admit():
            execution.admitting = True
            return witness._arm(writing, witness._part(writing))

        blocker = case.factory()
        event.listen(engine, "before_cursor_execute", capture)
        try:
            blocker.scalar(
                select(CachePopulationBeginRecord)
                .where(CachePopulationBeginRecord.id == writing.begin.call_id)
                .with_for_update()
            )
            blocker_pid = blocker.scalar(text("SELECT pg_backend_pid()"))
            with ThreadPoolExecutor(max_workers=1) as pool:
                admitted = pool.submit(admit)
                try:
                    assert waiting.wait(timeout=10)
                    deadline, blocked = time.monotonic() + 10, False
                    with engine.connect() as observer:
                        while time.monotonic() < deadline:
                            if blocker_pid in observer.scalar(
                                text("SELECT pg_blocking_pids(:pid)"), {"pid": pids[0]}
                            ):
                                blocked = True
                                break
                            time.sleep(0.01)
                    assert blocked
                    clock[0] = "2026-11-02T23:00:00Z"
                finally:
                    blocker.rollback()
                with pytest.raises(Conflict, match="custody lease"):
                    admitted.result(timeout=10)
        finally:
            blocker.close()
            event.remove(engine, "before_cursor_execute", capture)
        with session_scope(case.factory) as session:
            assert session.scalar(select(CachePopulationWriteEffectRecord.id)) is None
            assert (
                session.get(CachePopulationBeginRecord, writing.begin.call_id).completion_call_id
                is None
            )
            assert (
                session.get(RetrievalCacheStoreAccountingRecord, case.candidate.name).reserved_bytes
                == 3
            )
