from __future__ import annotations

from dataclasses import replace

import pytest
from riverhog_core.app_permissions import (
    CATALOG_READ,
    COLLECTIONS_CREATE,
    ApplicationAccess,
    Principal,
)
from riverhog_core.catalog_db import dispose_session_factory, make_session_factory, session_scope
from riverhog_core.catalog_models import AppKeyRecord
from riverhog_core.services.app_keys import (
    _begin_key_mutation,
    require_key_authorization,
    retain_key_authorization,
)
from riverhog_protocol.errors import BadRequest, Conflict, NotFound
from sqlalchemy import event
from sqlalchemy.exc import OperationalError

from tests.unit.test_app_keys import BOOTSTRAP, app_keys, create_key


def _authorized(tmp_path, *, access=None):
    service, config = app_keys(tmp_path)
    created = create_key(
        service,
        app="registration-writer",
        access=access
        or (
            ApplicationAccess(COLLECTIONS_CREATE),
            ApplicationAccess(CATALOG_READ),
        ),
    )
    principal = service.authenticate(str(created["token"]))
    assert principal is not None
    factory = make_session_factory(config.database_url)
    with session_scope(factory) as session:
        _begin_key_mutation(session)
        fence = retain_key_authorization(session, principal)
    assert fence is not None
    return service, config, created, principal, factory, fence


@pytest.mark.parametrize("mutation", ["replace", "add", "remove"])
def test_every_grant_mutation_invalidates_retained_work_even_after_permissions_are_restored(
    tmp_path, mutation
):
    service, config, created, principal, factory, fence = _authorized(tmp_path)
    args = {"app": principal.id, "key_id": fence.key_id}
    try:
        if mutation == "replace":
            service.replace_access(
                **args, access=(ApplicationAccess(CATALOG_READ),), grantor=BOOTSTRAP
            )
        elif mutation == "add":
            service.add_access(
                **args, access=ApplicationAccess(CATALOG_READ, "tag:extra"), grantor=BOOTSTRAP
            )
        else:
            service.remove_access(**args, access=ApplicationAccess(COLLECTIONS_CREATE))
        with session_scope(factory) as session:
            with pytest.raises(NotFound, match="authorization changed"):
                require_key_authorization(session, fence)
        service.replace_access(**args, access=tuple(principal.access), grantor=BOOTSTRAP)
        with session_scope(factory) as session:
            with pytest.raises(NotFound, match="authorization changed"):
                require_key_authorization(session, fence)
        current = service.authenticate(str(created["token"]))
        assert current is not None
        with session_scope(factory) as session:
            _begin_key_mutation(session)
            replacement = retain_key_authorization(session, current)
            assert replacement is not None and replacement != fence
            require_key_authorization(session, replacement)
    finally:
        dispose_session_factory(factory)


@pytest.mark.parametrize("disposition", ["revoke", "expire", "delete"])
def test_retained_work_rechecks_current_key_liveness(tmp_path, disposition):
    service, config, created, principal, factory, fence = _authorized(tmp_path)
    try:
        if disposition == "revoke":
            service.revoke(app=principal.id, key_id=fence.key_id)
        else:
            with session_scope(factory) as session:
                key = session.get(AppKeyRecord, fence.key_id)
                if disposition == "expire":
                    key.expires_at = "2000-01-01T00:00:00.000000000Z"
                else:
                    session.delete(key)
        with session_scope(factory) as session:
            with pytest.raises(NotFound, match="no longer active"):
                require_key_authorization(session, fence)
    finally:
        dispose_session_factory(factory)


def test_credential_rotation_preserves_grant_fence_without_retaining_a_token(tmp_path):
    service, config, created, principal, factory, fence = _authorized(tmp_path)
    try:
        service.rotate(app=principal.id, key_id=fence.key_id, grantor=BOOTSTRAP)
        with session_scope(factory) as session:
            _begin_key_mutation(session)
            require_key_authorization(session, fence)
        assert set(fence.__dataclass_fields__) == {"key_id", "app", "generation"}
    finally:
        dispose_session_factory(factory)


@pytest.mark.parametrize("mutation", ["duplicate", "missing", "final"])
def test_rejected_grant_mutation_does_not_change_retained_authorization(tmp_path, mutation):
    access = (ApplicationAccess(COLLECTIONS_CREATE),) if mutation == "final" else None
    service, config, created, principal, factory, fence = _authorized(tmp_path, access=access)
    args = {"app": principal.id, "key_id": fence.key_id}
    try:
        with pytest.raises((BadRequest, Conflict, NotFound)):
            if mutation == "duplicate":
                service.add_access(
                    **args, access=ApplicationAccess(COLLECTIONS_CREATE), grantor=BOOTSTRAP
                )
            elif mutation == "missing":
                service.remove_access(**args, access=ApplicationAccess(CATALOG_READ, "tag:absent"))
            else:
                service.remove_access(**args, access=ApplicationAccess(COLLECTIONS_CREATE))
        with session_scope(factory) as session:
            _begin_key_mutation(session)
            require_key_authorization(session, fence)
    finally:
        dispose_session_factory(factory)


@pytest.mark.parametrize("grants", [1, 16, 129, 1000])
def test_background_key_fence_is_one_point_read_independent_of_grant_count(tmp_path, grants):
    service, config, created, principal, factory, fence = _authorized(
        tmp_path,
        access=tuple(ApplicationAccess(CATALOG_READ, f"tag:member-{n}") for n in range(grants)),
    )
    engine = factory.kw["bind"]
    statements = []

    def capture(connection, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    try:
        event.listen(engine, "before_cursor_execute", capture)
        with session_scope(factory) as session:
            _begin_key_mutation(session)
            require_key_authorization(session, fence)
        selects = [s for s in statements if s.lstrip().upper().startswith("SELECT")]
        assert len(selects) == 1
        assert "app_keys.id =" in selects[0]
        assert "app_key_access_grants" not in selects[0]
    finally:
        event.remove(engine, "before_cursor_execute", capture)
        dispose_session_factory(factory)


def test_fence_reloads_a_key_already_present_in_the_session_identity_map(tmp_path):
    service, config, created, principal, factory, fence = _authorized(tmp_path)
    try:
        with session_scope(factory) as stale:
            key = stale.get(AppKeyRecord, fence.key_id)
            assert key.access_generation == fence.generation
            service.replace_access(
                app=principal.id,
                key_id=fence.key_id,
                access=(ApplicationAccess(CATALOG_READ),),
                grantor=BOOTSTRAP,
            )
            with pytest.raises(NotFound, match="authorization changed"):
                require_key_authorization(stale, fence)
    finally:
        dispose_session_factory(factory)


def test_sqlite_key_mutation_acquires_writer_before_reading_the_grant_set(tmp_path):
    service, config, created, principal, factory, fence = _authorized(tmp_path)
    try:
        with factory() as first, factory() as second:
            second.connection().exec_driver_sql("PRAGMA busy_timeout=0")
            second.commit()
            _begin_key_mutation(first)
            require_key_authorization(first, fence)
            with pytest.raises(OperationalError) as blocked:
                _begin_key_mutation(second)
            assert blocked.value.orig.sqlite_errorcode == 5
            second.rollback()
            first.rollback()
            _begin_key_mutation(second)
            require_key_authorization(second, fence)
    finally:
        dispose_session_factory(factory)


def test_key_fence_is_bound_to_both_key_owner_and_generation(tmp_path):
    service, config, created, principal, factory, fence = _authorized(tmp_path)
    try:
        for mismatch in (
            replace(fence, app="different-writer"),
            replace(fence, generation="0" * 64),
        ):
            with session_scope(factory) as session:
                with pytest.raises(NotFound, match="authorization changed"):
                    require_key_authorization(session, mismatch)
        with session_scope(factory) as session:
            assert retain_key_authorization(session, BOOTSTRAP) is None
            with pytest.raises(NotFound, match="authorization view is required"):
                retain_key_authorization(
                    session,
                    Principal(id=principal.id, key_id=fence.key_id, access=principal.access),
                )
    finally:
        dispose_session_factory(factory)
