from __future__ import annotations

import sqlite3
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import pytest
from riverhog_core.app_permissions import CATALOG_READ, RETRIEVAL_MANAGE, ApplicationAccess
from riverhog_core.catalog_db import dispose_session_factory, session_scope
from riverhog_core.catalog_models import AppKeyRecord
from riverhog_core.services import app_keys as app_keys_module
from riverhog_core.services.app_keys import require_current_principal
from riverhog_protocol.errors import NotFound
from sqlalchemy import event
from sqlalchemy.exc import OperationalError
from state_schema import read_snapshot

from tests.unit.test_app_keys import BOOTSTRAP, app_keys, create_key


@pytest.fixture
def usage_service(tmp_path: Path):
    service, _config = app_keys(tmp_path)
    try:
        yield service, service._session_factory, tmp_path / "catalog.sqlite3"
    finally:
        dispose_session_factory(service._session_factory)


@contextmanager
def _held_writer(path: Path) -> Iterator[threading.Thread]:
    ready = threading.Event()
    release = threading.Event()
    failures: list[BaseException] = []

    def hold() -> None:
        try:
            connection = sqlite3.connect(path)
            try:
                connection.execute("BEGIN IMMEDIATE")
                ready.set()
                release.wait()
                connection.rollback()
            finally:
                connection.close()
        except BaseException as error:
            failures.append(error)
            ready.set()

    original = threading.Thread(target=hold)
    original.start()
    try:
        assert ready.wait(10), "original writer did not reach its admission rendezvous"
        if failures:
            raise failures[0]
        yield original
    finally:
        release.set()
        original.join(10)
        assert not original.is_alive(), "original writer was not received"
        if failures:
            raise failures[0]


def _usage(factory, key_id):
    with read_snapshot(factory) as session:
        record = session.get(AppKeyRecord, key_id)
        return None if record is None else record.last_used_at


def test_authentication_omits_busy_usage_and_receives_original_writer(
    usage_service, record_property
) -> None:
    service, factory, path = usage_service
    created = create_key(service, app="observed")
    key_id = str(created["id"])
    attempts = []
    engine = factory.kw["bind"]

    def capture(connection, cursor, statement, parameters, context, executemany):
        if statement == "BEGIN IMMEDIATE":
            probe = connection.connection.driver_connection.cursor()
            try:
                attempts.append(probe.execute("PRAGMA busy_timeout").fetchone()[0])
            finally:
                probe.close()

    with factory() as session:
        session.connection().exec_driver_sql("PRAGMA busy_timeout=1379")
    event.listen(engine, "before_cursor_execute", capture)
    try:
        with _held_writer(path) as original:
            started = time.perf_counter()
            for _ in range(4):
                principal = service.authenticate(str(created["token"]))
                assert principal is not None and principal.key_id == key_id
                assert principal.allows(CATALOG_READ)
                assert original.is_alive()
                assert _usage(factory, key_id) is None
            elapsed = time.perf_counter() - started
            assert attempts == [0] * 4
            with factory() as session:
                restored = session.connection().exec_driver_sql("PRAGMA busy_timeout").scalar_one()
            assert restored == 1379
            record_property("busy_authentication", {"calls": 4, "elapsed_seconds": elapsed})
        assert not original.is_alive()
        assert service.authenticate(str(created["token"])) is not None
        assert _usage(factory, key_id) is not None
    finally:
        event.remove(engine, "before_cursor_execute", capture)


@pytest.mark.parametrize("state", ["empty", "invalid", "revoked", "expired", "deleted", "rotated"])
def test_busy_usage_never_authenticates_invalid_credentials(usage_service, state) -> None:
    service, factory, path = usage_service
    created = create_key(service, app="denied")
    token = str(created["token"])
    if state in {"empty", "invalid"}:
        token = "" if state == "empty" else "invalid"
    elif state == "revoked":
        service.revoke(app="denied", key_id=str(created["id"]))
    elif state == "rotated":
        service.rotate(app="denied", key_id=str(created["id"]), grantor=BOOTSTRAP)
    else:
        with session_scope(factory) as session:
            key = session.get(AppKeyRecord, str(created["id"]))
            if state == "expired":
                key.expires_at = "2000-01-01T00:00:00.000000000Z"
            else:
                session.delete(key)
    with _held_writer(path) as original:
        assert service.authenticate(token) is None
        assert original.is_alive()
        assert _usage(factory, str(created["id"])) is None


def test_busy_authentication_reads_current_grants_and_invalidates_old_view(usage_service) -> None:
    service, factory, path = usage_service
    created = create_key(service, app="fresh")
    old = service.authenticate(str(created["token"]))
    service.replace_access(
        app="fresh",
        key_id=str(created["id"]),
        access=(ApplicationAccess(RETRIEVAL_MANAGE),),
        grantor=BOOTSTRAP,
    )
    previous_usage = _usage(factory, str(created["id"]))
    with _held_writer(path):
        current = service.authenticate(str(created["token"]))
        assert current is not None and current.allows(RETRIEVAL_MANAGE)
        assert not current.allows(CATALOG_READ)
        assert current.authorization_view_identity != old.authorization_view_identity
        assert _usage(factory, str(created["id"])) == previous_usage
        with read_snapshot(factory) as session:
            with pytest.raises(NotFound, match="authorization changed"):
                require_current_principal(session, old)


def test_key_and_grants_share_one_snapshot_and_later_mutation_is_fenced(
    usage_service, monkeypatch
) -> None:
    service, factory, _path = usage_service
    created = create_key(service, app="snapshot")
    key_id = str(created["id"])
    original = app_keys_module._record_access
    changed = False

    def mutate_before_grants(session, selected_id):
        nonlocal changed
        if not changed:
            changed = True
            service.replace_access(
                app="snapshot",
                key_id=key_id,
                access=(ApplicationAccess(RETRIEVAL_MANAGE),),
                grantor=BOOTSTRAP,
            )
        return original(session, selected_id)

    with monkeypatch.context() as patch:
        patch.setattr(app_keys_module, "_record_access", mutate_before_grants)
        principal = service.authenticate(str(created["token"]))
    assert changed and principal is not None
    assert principal.allows(CATALOG_READ) and not principal.allows(RETRIEVAL_MANAGE)
    assert _usage(factory, key_id) is None
    with read_snapshot(factory) as session:
        with pytest.raises(NotFound, match="authorization changed"):
            require_current_principal(session, principal)
    fresh = service.authenticate(str(created["token"]))
    assert fresh is not None and fresh.allows(RETRIEVAL_MANAGE)
    assert not fresh.allows(CATALOG_READ)


def test_key_expiring_during_grant_read_is_denied(usage_service, monkeypatch) -> None:
    service, factory, _path = usage_service
    created = create_key(service, app="expires")
    key_id = str(created["id"])
    with session_scope(factory) as session:
        session.get(AppKeyRecord, key_id).expires_at = "2030-01-02T00:00:00.000000000Z"
    clock = [datetime(2030, 1, 1, tzinfo=UTC)]
    original = app_keys_module._record_access

    def advance_during_grants(session, selected_id):
        access = original(session, selected_id)
        clock[0] = datetime(2030, 1, 3, tzinfo=UTC)
        return access

    monkeypatch.setattr(app_keys_module, "utc_now", lambda: clock[0])
    monkeypatch.setattr(app_keys_module, "_record_access", advance_during_grants)
    assert service.authenticate(str(created["token"])) is None
    assert _usage(factory, key_id) is None


@pytest.mark.parametrize("mutation", ["revoke", "expire", "delete", "rotate", "grants"])
def test_observation_cannot_overwrite_authority_changed_after_authentication(
    usage_service, monkeypatch, mutation
) -> None:
    service, factory, _path = usage_service
    created = create_key(service, app="racing")
    key_id = str(created["id"])
    original = service._record_last_used

    def mutate_then_record(**kwargs):
        if mutation == "revoke":
            service.revoke(app="racing", key_id=key_id)
        elif mutation == "rotate":
            service.rotate(app="racing", key_id=key_id, grantor=BOOTSTRAP)
        elif mutation == "grants":
            service.replace_access(
                app="racing",
                key_id=key_id,
                access=(ApplicationAccess(RETRIEVAL_MANAGE),),
                grantor=BOOTSTRAP,
            )
        else:
            with session_scope(factory) as session:
                key = session.get(AppKeyRecord, key_id)
                if mutation == "expire":
                    key.expires_at = "2000-01-01T00:00:00.000000000Z"
                else:
                    session.delete(key)
        original(**kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(service, "_record_last_used", mutate_then_record)
        principal = service.authenticate(str(created["token"]))
    assert principal is not None
    assert _usage(factory, key_id) is None
    if mutation != "rotate":
        with read_snapshot(factory) as session:
            with pytest.raises(NotFound):
                require_current_principal(session, principal)
    if mutation != "grants":
        assert service.authenticate(str(created["token"])) is None


def test_usage_is_monotonic_and_does_not_change_authorization_generation(
    usage_service, monkeypatch
) -> None:
    service, factory, _path = usage_service
    created = create_key(service, app="monotonic")
    with read_snapshot(factory) as session:
        generation = session.get(AppKeyRecord, str(created["id"])).access_generation
    monkeypatch.setattr(app_keys_module, "utc_now", lambda: datetime(2030, 1, 1, tzinfo=UTC))
    first = service.authenticate(str(created["token"]))
    recorded = _usage(factory, str(created["id"]))
    monkeypatch.setattr(app_keys_module, "utc_now", lambda: datetime(2029, 1, 1, tzinfo=UTC))
    second = service.authenticate(str(created["token"]))
    assert _usage(factory, str(created["id"])) == recorded
    assert first.authorization_view_identity == second.authorization_view_identity
    with read_snapshot(factory) as session:
        assert session.get(AppKeyRecord, str(created["id"])).access_generation == generation


@pytest.mark.parametrize("phase", ["read", "observation"])
@pytest.mark.parametrize("code", [sqlite3.SQLITE_BUSY, sqlite3.SQLITE_IOERR])
def test_only_observation_contention_is_omitted(usage_service, phase, code) -> None:
    service, factory, _path = usage_service
    created = create_key(service, app="errors")
    driver_error = sqlite3.OperationalError("injected database failure")
    driver_error.sqlite_errorcode = code
    failure = OperationalError("injected statement", {}, driver_error)
    engine = factory.kw["bind"]

    def inject(connection, cursor, statement, parameters, context, executemany):
        selected = statement.lstrip().upper()
        if (phase == "read" and selected.startswith("SELECT")) or (
            phase == "observation" and selected.startswith("UPDATE APP_KEYS")
        ):
            raise failure

    event.listen(engine, "before_cursor_execute", inject)
    try:
        if phase == "observation" and code == sqlite3.SQLITE_BUSY:
            assert service.authenticate(str(created["token"])) is not None
        else:
            with pytest.raises(OperationalError) as raised:
                service.authenticate(str(created["token"]))
            assert raised.value is failure
    finally:
        event.remove(engine, "before_cursor_execute", inject)
    assert _usage(factory, str(created["id"])) is None


@pytest.mark.parametrize("extent", [1, 64])
def test_usage_observation_is_one_point_update_as_keys_and_grants_grow(
    usage_service, extent, record_property
) -> None:
    service, factory, _path = usage_service
    for index in range(extent):
        create_key(service, app=f"unrelated-{index}")
    created = create_key(
        service,
        app="measured",
        access=tuple(ApplicationAccess(CATALOG_READ, f"tag:grant-{n}") for n in range(extent)),
    )
    engine = factory.kw["bind"]
    statements = []

    def capture(connection, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    event.listen(engine, "before_cursor_execute", capture)
    try:
        principal = service.authenticate(str(created["token"]))
    finally:
        event.remove(engine, "before_cursor_execute", capture)
    assert principal is not None and len(principal.access) == extent
    assert statements.count("BEGIN IMMEDIATE") == 1
    observer = statements[statements.index("BEGIN IMMEDIATE") + 1 :]
    updates = [sql for sql in observer if sql.startswith("UPDATE app_keys")]
    assert len(updates) == 1 and "WHERE app_keys.id =" in updates[0]
    assert not any(sql.startswith("SELECT") for sql in observer)
    record_property(
        "usage_observation_work",
        {"unrelated_keys": extent, "current_grants": extent, "writer_attempts": 1, "updates": 1},
    )


def test_interrupted_observation_rolls_back_and_releases_the_original_transaction(
    usage_service,
) -> None:
    service, factory, _path = usage_service
    created = create_key(service, app="interrupted")
    engine = factory.kw["bind"]

    def interrupt(connection, cursor, statement, parameters, context, executemany):
        if statement.startswith("UPDATE app_keys"):
            raise KeyboardInterrupt("interrupted after actual usage update")

    event.listen(engine, "after_cursor_execute", interrupt)
    try:
        with pytest.raises(KeyboardInterrupt, match="after actual usage update"):
            service.authenticate(str(created["token"]))
    finally:
        event.remove(engine, "after_cursor_execute", interrupt)
    assert _usage(factory, str(created["id"])) is None
    assert service.authenticate(str(created["token"])) is not None
    assert _usage(factory, str(created["id"])) is not None
