from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager

import pytest
from riverhog_core.app_permissions import CATALOG_READ, ApplicationAccess
from riverhog_core.catalog_db import SessionFactory, dispose_session_factory, session_scope
from riverhog_core.catalog_models import AppKeyRecord
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services.app_keys import SqlAlchemyAppKeyService
from sqlalchemy import select, text
from sqlalchemy.exc import OperationalError
from state_schema import read_snapshot

from tests.integration.test_operation_lifecycle_postgres import database_url as database_url
from tests.unit.test_app_keys import create_key

pytestmark = pytest.mark.integration


@contextmanager
def _held_usage_lock(factory: SessionFactory, key_id: str, kind: str) -> Iterator[threading.Thread]:
    ready = threading.Event()
    release = threading.Event()
    failures: list[BaseException] = []

    def hold() -> None:
        try:
            with session_scope(factory) as session:
                if kind == "row":
                    assert (
                        session.scalar(
                            select(AppKeyRecord.id)
                            .where(AppKeyRecord.id == key_id)
                            .with_for_update()
                        )
                        == key_id
                    )
                else:
                    session.execute(text("LOCK TABLE app_keys IN SHARE MODE"))
                ready.set()
                assert release.wait(30), "usage lock holder exceeded its resource deadline"
        except BaseException as error:
            failures.append(error)
            ready.set()

    original = threading.Thread(target=hold)
    original.start()
    try:
        assert ready.wait(10), "original usage lock holder did not enter"
        if failures:
            raise failures[0]
        yield original
    finally:
        release.set()
        original.join(10)
        assert not original.is_alive(), "original usage lock holder was not received"
        if failures:
            raise failures[0]


@pytest.mark.parametrize("kind", ["row", "relation"])
def test_usage_observation_defers_while_original_postgres_lock_remains_held(
    database_url: str, kind: str, record_property
) -> None:
    service = SqlAlchemyAppKeyService(RuntimeConfig.for_testing(database_url=database_url))
    factory = service._session_factory
    try:
        created = create_key(service, app="usage-contention")
        key_id = str(created["id"])
        with read_snapshot(factory) as session:
            key = session.get(AppKeyRecord, key_id)
            assert key is not None and key.last_used_at is None
            generation = key.access_generation

        with _held_usage_lock(factory, key_id, kind) as original:
            started = time.monotonic()
            principal = service.authenticate(str(created["token"]))
            elapsed = time.monotonic() - started
            assert principal is not None
            assert principal.key_id == key_id
            assert principal.access == frozenset({ApplicationAccess(CATALOG_READ)})
            assert original.is_alive()
            assert service.authenticate("invalid-application-token") is None
            with read_snapshot(factory) as session:
                key = session.get(AppKeyRecord, key_id)
                assert key is not None and key.last_used_at is None
                assert key.access_generation == generation

            with pytest.raises(OperationalError) as held:
                with session_scope(factory) as session:
                    if kind == "row":
                        session.scalar(
                            select(AppKeyRecord.id)
                            .where(AppKeyRecord.id == key_id)
                            .with_for_update(nowait=True)
                        )
                    else:
                        session.execute(text("LOCK TABLE app_keys IN ROW EXCLUSIVE MODE NOWAIT"))
            assert getattr(held.value.orig, "sqlstate", None) == "55P03"
            assert original.is_alive()

        assert not original.is_alive()
        after = service.authenticate(str(created["token"]))
        assert after is not None
        assert after.authorization_view_identity == principal.authorization_view_identity
        with read_snapshot(factory) as session:
            key = session.get(AppKeyRecord, key_id)
            assert key is not None and key.last_used_at is not None
            assert key.access_generation == generation
        record_property(
            "usage_lock_observation",
            {
                "lock": kind,
                "authentication_seconds": elapsed,
                "independent_lock_probe_sqlstate": "55P03",
                "original_lock_held_after_authentication": True,
                "original_lock_holder_received": True,
                "usage_omitted_while_held": True,
                "usage_recorded_after_release": True,
            },
        )
    finally:
        dispose_session_factory(factory)
