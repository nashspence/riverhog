from __future__ import annotations

from datetime import timedelta

import pytest
from riverhog_core.app_permissions import (
    COLLECTIONS_CREATE,
    COLLECTIONS_DELETE,
    ApplicationAccess,
    Principal,
    collection_resource,
)
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import AppKeyRecord, CollectionUploadRecord
from riverhog_core.catalog_workflow_models import CollectionProcessingClaimRecord
from riverhog_core.services import app_keys
from riverhog_core.services.app_keys import SqlAlchemyAppKeyService
from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy import event

from tests.unit.test_app_keys import BOOTSTRAP, create_key, seed_tag
from tests.unit.test_archive_write_admission import _case
from tests.unit.test_upload_terminal_claim_authority import _output


def _operator(case, *, access=None, expires_in=None):
    keys = SqlAlchemyAppKeyService(case.service._config, session_factory=case.factory)
    created = create_key(
        keys,
        app="deletion-operator",
        access=access if access is not None else (ApplicationAccess(COLLECTIONS_DELETE),),
        expires_in=expires_in,
    )
    principal = keys.authenticate(str(created["token"]))
    assert principal is not None
    return keys, principal


def _select(case, principal, guard):
    return case.owner.select_terminal(
        case.collection_id,
        kind="discard",
        request={"challenge": "original-discard-scope"},
        principal_id=principal.id,
        grants=case.service._config.archive_write,
        guard=guard,
        validate_selection=lambda session, upload: None,
    )


def _observe(case, operation, guard):
    with session_scope(case.factory) as session:
        return case.service._archive_terminal_observation.observe(
            session, case.collection_id, operation, guard=guard
        )


def _control(case):
    with session_scope(case.factory) as session:
        row = session.get(ArchiveWriteControlRecord, case.collection_id)
        return row.construction_json, row.gate, row.epoch, row.terminal_id


@pytest.mark.parametrize("scope", ("another-collection", "create-only"))
def test_discard_authority_binds_current_deletion_scope_before_any_parent_query(tmp_path, scope):
    with _case(tmp_path) as case:
        if scope == "another-collection":
            seed_tag(
                case.service._config,
                "unrelated-collection",
                collection_id=case.collection_id + 1,
            )
        access = (
            ApplicationAccess(COLLECTIONS_DELETE, collection_resource(case.collection_id + 1))
            if scope == "another-collection"
            else ApplicationAccess(COLLECTIONS_CREATE)
        )
        _, principal = _operator(case, access=(access,))
        guard = case.service._discard_write_guard(case.collection_id, principal)
        before = _control(case)

        def capture(connection, cursor, statement, parameters, context, executemany):
            assert "FROM collection_uploads" not in statement
            assert "FROM archive_upload_terminals" not in statement

        engine = case.factory.kw["bind"]
        event.listen(engine, "before_cursor_execute", capture)
        try:
            with pytest.raises(NotFound, match="upload not found"):
                _select(case, principal, guard)
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        assert _control(case) == before


@pytest.mark.parametrize("change", ("revoke", "expire", "delete", "replace-access"))
def test_retained_discard_guard_rechecks_current_key_before_terminal_resources(
    tmp_path, monkeypatch, change
):
    with _case(tmp_path) as case:
        keys, principal = _operator(case)
        guard = case.service._discard_write_guard(case.collection_id, principal)
        operation = _select(case, principal, guard)
        before = _control(case)
        assert _observe(case, operation, guard).records.operation_id == operation
        if change == "revoke":
            keys.revoke(app=principal.id, key_id=principal.key_id)
        elif change == "replace-access":
            keys.replace_access(
                app=principal.id,
                key_id=principal.key_id,
                access=(ApplicationAccess(COLLECTIONS_CREATE),),
                grantor=BOOTSTRAP,
            )
        else:
            with session_scope(case.factory) as session:
                key = session.get(AppKeyRecord, principal.key_id)
                if change == "expire":
                    key.expires_at = "2000-01-01T00:00:00.000000000Z"
                else:
                    session.delete(key)

        def forbidden(*args, **kwargs):
            pytest.fail("invalid current deletion authority reached local resource probes")

        def capture(connection, cursor, statement, parameters, context, executemany):
            assert "FROM collection_uploads" not in statement
            assert "FROM archive_upload_terminals" not in statement

        monkeypatch.setattr(case.service._archive_finalizers, "local_terminal_status", forbidden)
        engine = case.factory.kw["bind"]
        event.listen(engine, "before_cursor_execute", capture)
        try:
            with pytest.raises(NotFound, match="application key"):
                _observe(case, operation, guard)
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        assert _control(case) == before


def test_discard_guard_rechecks_expiry_after_acquiring_the_later_parent(tmp_path, monkeypatch):
    with _case(tmp_path) as case:
        clock = [app_keys.utc_now()]
        monkeypatch.setattr(app_keys, "utc_now", lambda: clock[0])
        _, principal = _operator(case, expires_in=timedelta(minutes=10))
        guard = case.service._discard_write_guard(case.collection_id, principal)
        before = _control(case)
        advanced = []

        def capture(connection, cursor, statement, parameters, context, executemany):
            if (
                "FROM collection_uploads" in statement
                and "collection_uploads.idempotency_key" in statement
            ):
                clock[0] += timedelta(minutes=11)
                advanced.append(True)

        engine = case.factory.kw["bind"]
        event.listen(engine, "after_cursor_execute", capture)
        try:
            with pytest.raises(NotFound, match="no longer active"):
                _select(case, principal, guard)
        finally:
            event.remove(engine, "after_cursor_execute", capture)
        assert advanced == [True] and _control(case) == before


def test_discard_guard_never_caches_original_output_claim_inactivity(tmp_path):
    with _output(tmp_path) as case:
        _, principal = _operator(case)
        guard = case.service._discard_write_guard(case.collection_id, principal)
        before = _control(case)
        with pytest.raises(Conflict, match="claim remains active"):
            _select(case, principal, guard)
        assert _control(case) == before
        with session_scope(case.factory) as session:
            session.get(
                CollectionProcessingClaimRecord, case.claim_id
            ).expires_at = "2000-01-01T00:00:00Z"
        operation = _select(case, principal, guard)
        assert _observe(case, operation, guard).records.operation_id == operation
        selected = _control(case)
        with session_scope(case.factory) as session:
            session.get(
                CollectionProcessingClaimRecord, case.claim_id
            ).expires_at = "2999-01-01T00:00:00Z"
        with pytest.raises(Conflict, match="claim remains active"):
            _observe(case, operation, guard)
        assert _control(case) == selected


def test_missing_original_output_claim_cannot_select_discard(tmp_path):
    with _output(tmp_path) as case:
        _, principal = _operator(case)
        guard = case.service._discard_write_guard(case.collection_id, principal)
        with session_scope(case.factory) as session:
            session.get(CollectionProcessingClaimRecord, case.claim_id).execution_id = "f" * 64
        before = _control(case)
        with pytest.raises(NotFound, match="output claim is unavailable"):
            _select(case, principal, guard)
        assert _control(case) == before


def test_discard_guard_validates_original_construction_instead_of_borrowing_new_creator(tmp_path):
    with _output(tmp_path) as case:
        _, principal = _operator(case)
        guard = case.service._discard_write_guard(case.collection_id, principal)
        before = _control(case)
        with session_scope(case.factory) as session:
            session.get(
                CollectionUploadRecord, case.collection_id
            ).initiated_by_principal_id = "replacement-producer"
        with pytest.raises(Conflict, match="construction identity changed"):
            _select(case, principal, guard)
        assert _control(case) == before


def test_real_output_capability_does_not_supply_discard_permission(tmp_path):
    with _output(tmp_path) as case:
        guard = case.service._discard_write_guard(case.collection_id, case.principal)
        before = _control(case)
        with pytest.raises(NotFound, match="upload not found"):
            _select(case, case.principal, guard)
        assert _control(case) == before


def test_discard_permission_evaluation_binds_the_original_collection_identity(tmp_path):
    with _case(tmp_path) as case:
        principal = Principal(
            id="deletion-operator",
            key_id=None,
            access=frozenset(
                {ApplicationAccess(COLLECTIONS_DELETE, collection_resource(case.collection_id))}
            ),
        )
        guard = case.service._discard_write_guard(case.collection_id, principal)
        operation = _select(case, principal, guard)
        assert _observe(case, operation, guard).records.operation_id == operation
