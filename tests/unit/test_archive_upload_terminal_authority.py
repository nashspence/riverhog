from __future__ import annotations

from contextlib import contextmanager

import pytest
from riverhog_core.app_permissions import (
    CATALOG_READ,
    COLLECTIONS_CREATE,
    COLLECTIONS_DELETE,
    ApplicationAccess,
    tag_resource,
)
from riverhog_core.catalog_archive_write_models import (
    ArchiveUploadTerminalRecord,
    ArchiveWriteControlRecord,
)
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import AppKeyRecord
from riverhog_core.catalog_workflow_models import CollectionProcessingClaimRecord
from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy import event

from tests.unit.test_app_keys import BOOTSTRAP
from tests.unit.test_archive_upload_terminal_tags import _tagged, _teardown_upload
from tests.unit.test_archive_write_admission import _case, _selected
from tests.unit.test_upload_cancel_authority import _select as _select_cancel
from tests.unit.test_upload_discard_authority import _operator, _select
from tests.unit.test_upload_terminal_claim_authority import _output
from tests.unit.test_upload_terminal_read_authority import _creator_key, _shrink_live_tags


@contextmanager
def _discard_case(tmp_path, *, database_url=None):
    with _case(tmp_path, database_url=database_url) as case:
        case.keys, case.principal = _operator(case)
        case.operation = _select(
            case,
            case.principal,
            case.service._discard_write_guard(case.collection_id, case.principal),
        )
        yield case


def _observe(case, guard):
    with session_scope(case.factory) as session:
        return case.service._archive_terminal_observation.observe(
            session, case.collection_id, case.operation, guard=guard
        )


def _unchanged(case):
    with session_scope(case.factory) as session:
        row = session.get(ArchiveWriteControlRecord, case.collection_id)
        terminal = session.get(ArchiveUploadTerminalRecord, case.operation)
        assert row.gate == "closed" and row.terminal_id == case.operation
        assert terminal.phase == "draining" and terminal.result_json is None


def test_cold_discard_reloads_current_grants_without_token_authentication(tmp_path, monkeypatch):
    from riverhog_core.services.app_keys import SqlAlchemyAppKeyService
    from riverhog_core.services.collection_workflows import SqlAlchemyCollectionWorkflowService

    with _discard_case(tmp_path) as case:

        def forbidden(*args, **kwargs):
            pytest.fail("continuation must not reconstruct or authenticate a bearer")

        monkeypatch.setattr(SqlAlchemyAppKeyService, "authenticate", forbidden)
        monkeypatch.setattr(
            SqlAlchemyCollectionWorkflowService, "authenticate_capability", forbidden
        )
        guard = case.service._terminal_write_guard(case.collection_id, case.operation)
        assert _observe(case, guard).records.operation_id == case.operation
        case.keys.rotate(app=case.principal.id, key_id=case.principal.key_id, grantor=BOOTSTRAP)
        cold = case.service._terminal_write_guard(case.collection_id, case.operation)
        assert _observe(case, cold).records.operation_id == case.operation
        _unchanged(case)


@pytest.mark.parametrize("warm", (False, True))
@pytest.mark.parametrize("change", ("revoke", "expire", "delete", "app", "permission"))
def test_current_authority_failure_cannot_reach_terminal_resources(
    tmp_path, monkeypatch, warm, change
):
    with _discard_case(tmp_path) as case:
        guard = case.service._terminal_write_guard(case.collection_id, case.operation)
        if warm:
            assert _observe(case, guard).records.operation_id == case.operation
        if change == "permission":
            case.keys.replace_access(
                app=case.principal.id,
                key_id=case.principal.key_id,
                access=(ApplicationAccess(CATALOG_READ),),
                grantor=BOOTSTRAP,
            )
        else:
            with session_scope(case.factory) as session:
                key = session.get(AppKeyRecord, case.principal.key_id)
                if change == "revoke":
                    key.revoked_at = "2000-01-01T00:00:00Z"
                elif change == "expire":
                    key.expires_at = "2000-01-01T00:00:00Z"
                elif change == "delete":
                    session.delete(key)
                else:
                    key.app = "another-application"

        def forbidden(*args, **kwargs):
            pytest.fail("invalid current authority reached local resources")

        def capture(connection, cursor, statement, parameters, context, executemany):
            assert "FROM collection_uploads" not in statement
            assert "FROM archive_upload_terminals" not in statement

        monkeypatch.setattr(case.service._archive_finalizers, "local_terminal_status", forbidden)
        engine = case.factory.kw["bind"]
        event.listen(engine, "before_cursor_execute", capture)
        try:
            with pytest.raises(NotFound):
                _observe(case, guard)
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        _unchanged(case)


def test_original_request_resumes_after_current_permission_is_restored(tmp_path):
    with _discard_case(tmp_path) as case:
        guard = case.service._terminal_write_guard(case.collection_id, case.operation)
        assert _observe(case, guard).records.operation_id == case.operation
        for access, allowed in (
            ((ApplicationAccess(COLLECTIONS_CREATE),), False),
            ((ApplicationAccess(COLLECTIONS_DELETE), ApplicationAccess(CATALOG_READ)), True),
            ((ApplicationAccess(CATALOG_READ),), False),
            ((ApplicationAccess(COLLECTIONS_DELETE),), True),
        ):
            case.keys.replace_access(
                app=case.principal.id,
                key_id=case.principal.key_id,
                access=access,
                grantor=BOOTSTRAP,
            )
            if allowed:
                assert _observe(case, guard).records.operation_id == case.operation
            else:
                with pytest.raises(NotFound):
                    _observe(case, guard)
            _unchanged(case)


def test_unchanged_key_generation_reuses_only_the_current_permission_proof(tmp_path):
    with _discard_case(tmp_path) as case:
        guard = case.service._terminal_write_guard(case.collection_id, case.operation)
        assert _observe(case, guard).records.operation_id == case.operation
        queries = []

        def capture(connection, cursor, statement, parameters, context, executemany):
            assert "FROM app_key_access_grants" not in statement
            queries.append(statement)

        engine = case.factory.kw["bind"]
        event.listen(engine, "before_cursor_execute", capture)
        try:
            assert _observe(case, guard).records.operation_id == case.operation
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        assert any("FROM app_keys" in query for query in queries)
        assert any("FROM collection_uploads" in query for query in queries)


def test_reloaded_creator_scope_keeps_original_tags_after_partial_row_cleanup(tmp_path):
    with _tagged(tmp_path, count=5) as case:
        keys, principal, _ = _creator_key(case)
        case.operation = _select_cancel(
            case, principal, case.service._cancel_write_guard(case.collection_id, principal)
        )
        guard = case.service._terminal_write_guard(case.collection_id, case.operation)
        assert _observe(case, guard).records.operation_id == case.operation
        _shrink_live_tags(case)
        keys.replace_access(
            app=principal.id,
            key_id=principal.key_id,
            access=(ApplicationAccess(COLLECTIONS_CREATE, tag_resource(case.tags[0])),),
            grantor=BOOTSTRAP,
        )
        with pytest.raises(NotFound, match="collection creation"):
            _observe(case, guard)
        keys.replace_access(
            app=principal.id,
            key_id=principal.key_id,
            access=tuple(principal.access),
            grantor=BOOTSTRAP,
        )
        assert _observe(case, guard).records.operation_id == case.operation
        _unchanged(case)


def test_keyless_attribution_cannot_invent_cold_execution_permission(tmp_path):
    with _case(tmp_path) as case:
        case.operation = _selected(case)
        guard = case.service._terminal_write_guard(case.collection_id, case.operation)
        with pytest.raises(NotFound, match="current key authority"):
            _observe(case, guard)
        _unchanged(case)


def test_continuation_does_not_replace_read_authority_after_parent_retirement(tmp_path):
    with _discard_case(tmp_path) as case:
        guard = case.service._terminal_write_guard(case.collection_id, case.operation)
        _teardown_upload(case)
        with pytest.raises(NotFound, match="upload not found"):
            _observe(case, guard)
        case.service.require_terminal_read_access(
            case.collection_id, case.operation, case.principal
        )
        _unchanged(case)


@pytest.mark.parametrize("change", ("settle", "expire", "unseal", "key", "app", "result"))
def test_processing_cancel_rechecks_live_original_claim_on_every_turn(tmp_path, change):
    with _output(tmp_path) as case:
        case.operation = _select_cancel(
            case,
            case.principal,
            case.service._cancel_write_guard(case.collection_id, case.principal),
        )
        guard = case.service._terminal_write_guard(case.collection_id, case.operation)
        assert _observe(case, guard).records.operation_id == case.operation
        with session_scope(case.factory) as session:
            claim = session.get(CollectionProcessingClaimRecord, case.claim_id)
            if change == "settle":
                claim.state = "settled"
            elif change == "expire":
                claim.expires_at = "2000-01-01T00:00:00Z"
            elif change == "unseal":
                claim.plan_sealed_at = None
            elif change == "key":
                claim.consumer_key_id = "another-original-key"
            elif change == "app":
                claim.consumer_app = "another-application"
            else:
                claim.result_kind = "external-effect"
        with pytest.raises(NotFound, match="execution authority"):
            _observe(case, guard)
        _unchanged(case)


def test_processing_continuation_does_not_inherit_application_deletion_grants(tmp_path):
    with _output(tmp_path) as case:
        case.operation = case.owner.select_terminal(
            case.collection_id,
            kind="discard",
            request={"selected": "fixture"},
            principal_id=case.principal.id,
            key_id=case.principal.key_id,
            grants=case.service._config.archive_write,
            guard=case.service._payload_write_guard(case.collection_id, case.principal),
            validate_selection=lambda *args: None,
        )
        guard = case.service._terminal_write_guard(case.collection_id, case.operation)
        with pytest.raises(NotFound, match="delegated deletion"):
            _observe(case, guard)
        _unchanged(case)


def test_retained_attribution_drift_is_rejected_before_a_continuation_is_built(tmp_path):
    with _discard_case(tmp_path) as case:
        with session_scope(case.factory) as session:
            session.get(
                ArchiveUploadTerminalRecord, case.operation
            ).principal_id = "another-application"
        with pytest.raises(Conflict, match="original selection changed"):
            case.service._terminal_write_guard(case.collection_id, case.operation)
