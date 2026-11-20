from __future__ import annotations

from datetime import timedelta

import pytest
from riverhog_core.app_permissions import (
    COLLECTION_TAGS_MANAGE,
    COLLECTIONS_CREATE,
    ApplicationAccess,
)
from riverhog_core.catalog_archive_write_models import (
    ArchiveUploadTerminalTagRecord,
    ArchiveWriteControlRecord,
)
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import AppKeyRecord, CollectionUploadRecord
from riverhog_core.catalog_workflow_models import CollectionProcessingClaimRecord
from riverhog_core.services import app_keys, collection_upload_authority, collection_uploads
from riverhog_core.services.archive_write_admission import _writer
from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy import event

from tests.unit.test_app_keys import BOOTSTRAP, create_key
from tests.unit.test_archive_upload_terminal_tags import _tagged
from tests.unit.test_collection_uploads import _CREATOR
from tests.unit.test_upload_terminal_claim_authority import _output
from tests.unit.test_upload_terminal_read_authority import _creator_key, _scoped, _shrink_live_tags


def _select(case, principal, guard):
    return case.owner.select_terminal(
        case.collection_id,
        kind="cancel",
        request={"selected": "original-creator-cancel"},
        principal_id=principal.id,
        key_id=principal.key_id,
        grants=case.service._config.archive_write,
        guard=guard,
        validate_selection=lambda *args: None,
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


@pytest.mark.parametrize("change", ("revoke", "expire", "delete", "replace-access"))
def test_cancel_guard_rechecks_current_key_before_parent_or_local_resources(
    tmp_path, monkeypatch, change
):
    with _tagged(tmp_path, count=3) as case:
        keys, principal, _ = _creator_key(case)
        guard = case.service._cancel_write_guard(case.collection_id, principal)
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
                    key.expires_at = "2000-01-01T00:00:00Z"
                else:
                    session.delete(key)

        def forbidden(*args, **kwargs):
            pytest.fail("invalid current creator authority reached local probes")

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


def test_fresh_cancel_guard_keeps_original_tag_scope_after_partial_cleanup(tmp_path):
    with _tagged(tmp_path, count=8) as case:
        guard = case.service._cancel_write_guard(case.collection_id, _CREATOR)
        operation = _select(case, _CREATOR, guard)
        _shrink_live_tags(case)
        narrowed = _scoped(case.tags[:1])
        with pytest.raises(NotFound, match="collection creation"):
            _observe(
                case,
                operation,
                case.service._cancel_write_guard(case.collection_id, narrowed),
            )
        assert _observe(case, operation, guard).records.operation_id == operation


def test_cancel_reuses_only_the_same_key_and_original_tag_permission_proof(tmp_path, monkeypatch):
    with _tagged(tmp_path, count=8) as case:
        _, principal, _ = _creator_key(case)
        guard = case.service._cancel_write_guard(case.collection_id, principal)
        operation = _select(case, principal, guard)

        def forbidden(*args, **kwargs):
            pytest.fail("unchanged current scope was traversed again")

        monkeypatch.setattr(collection_uploads, "require_terminal_creator_scope", forbidden)
        engine = case.factory.kw["bind"]

        def capture(connection, cursor, statement, parameters, context, executemany):
            assert "FROM collection_tag_nodes" not in statement
            assert "FROM collection_upload_tags" not in statement

        event.listen(engine, "before_cursor_execute", capture)
        try:
            assert _observe(case, operation, guard).records.operation_id == operation
        finally:
            event.remove(engine, "before_cursor_execute", capture)


def test_open_preselection_tag_changes_invalidate_the_cached_creator_scope(tmp_path):
    with _tagged(tmp_path, count=3) as case:
        _, principal, _ = _creator_key(case)
        guard = case.service._cancel_write_guard(case.collection_id, principal)
        with session_scope(case.factory) as session:
            _writer(session)
            guard(session)
        from dataclasses import replace

        manager = replace(
            _CREATOR,
            access=_CREATOR.access | {ApplicationAccess(COLLECTION_TAGS_MANAGE)},
        )
        case.service.add_tags(case.collection_id, ("another-staged-tag",), principal=manager)
        before = _control(case)
        with pytest.raises(NotFound, match="collection creation"):
            _select(case, principal, guard)
        assert _control(case) == before


def test_another_read_authorized_key_cannot_adopt_the_selected_cancel(tmp_path):
    with _tagged(tmp_path, count=3) as case:
        _, original, _ = _creator_key(case)
        _, reader, _ = _creator_key(case, seed=False)
        operation = _select(
            case, original, case.service._cancel_write_guard(case.collection_id, original)
        )
        case.service.require_terminal_read_access(case.collection_id, operation, reader)
        before = _control(case)
        with pytest.raises(Conflict, match="different archive upload terminal request"):
            _observe(
                case,
                operation,
                case.service._cancel_write_guard(case.collection_id, reader),
            )
        assert _control(case) == before


def test_creator_rights_do_not_reacquire_or_extend_an_expired_writer_lease(tmp_path):
    with _tagged(tmp_path, count=3) as case:
        with session_scope(case.factory) as session:
            upload = session.get(CollectionUploadRecord, case.collection_id)
            upload.state = "orphaned"
            upload.orphaned_at = "2000-01-01T00:00:00Z"
            upload.lease_expires_at = "2000-01-01T00:00:00Z"
        guard = case.service._cancel_write_guard(case.collection_id, _CREATOR)
        operation = _select(case, _CREATOR, guard)
        assert _observe(case, operation, guard).records.operation_id == operation
        with session_scope(case.factory) as session:
            upload = session.get(CollectionUploadRecord, case.collection_id)
            assert upload.state == "orphaned"
            assert upload.lease_expires_at == "2000-01-01T00:00:00Z"


def test_cancel_key_expiry_is_rechecked_after_scope_traversal(tmp_path, monkeypatch):
    with _tagged(tmp_path, count=3) as case:
        keys, _, _ = _creator_key(case)
        clock = [app_keys.utc_now()]
        monkeypatch.setattr(app_keys, "utc_now", lambda: clock[0])
        created = create_key(
            keys,
            app=_CREATOR.id,
            access=(ApplicationAccess(COLLECTIONS_CREATE),),
            expires_in=timedelta(minutes=10),
        )
        principal = keys.authenticate(str(created["token"]))
        assert principal is not None
        original = collection_uploads.require_terminal_creator_scope

        def advance(*args):
            original(*args)
            clock[0] += timedelta(minutes=11)

        monkeypatch.setattr(collection_uploads, "require_terminal_creator_scope", advance)
        before = _control(case)
        with pytest.raises(NotFound, match="no longer active"):
            _select(
                case, principal, case.service._cancel_write_guard(case.collection_id, principal)
            )
        assert _control(case) == before


def test_cancel_claim_expiry_is_rechecked_after_scope_traversal(tmp_path, monkeypatch):
    with _output(tmp_path) as case:
        clock = [collection_upload_authority.utc_timestamp_now()]
        monkeypatch.setattr(collection_upload_authority, "utc_timestamp_now", lambda: clock[0])
        original = collection_uploads.require_terminal_creator_scope

        def advance(*args):
            original(*args)
            clock[0] = "2999-01-01T00:00:00Z"

        monkeypatch.setattr(collection_uploads, "require_terminal_creator_scope", advance)
        before = _control(case)
        with pytest.raises(NotFound, match="execution authority"):
            _select(
                case,
                case.principal,
                case.service._cancel_write_guard(case.collection_id, case.principal),
            )
        assert _control(case) == before


@pytest.mark.parametrize("change", ("expire", "missing"))
def test_current_cancel_claim_is_checked_before_the_upload_parent(tmp_path, change):
    with _output(tmp_path) as case:
        with session_scope(case.factory) as session:
            claim = session.get(CollectionProcessingClaimRecord, case.claim_id)
            if change == "expire":
                claim.expires_at = "2000-01-01T00:00:00Z"
            else:
                claim.execution_id = "f" * 64
        engine = case.factory.kw["bind"]

        def capture(connection, cursor, statement, parameters, context, executemany):
            assert "FROM collection_uploads" not in statement

        event.listen(engine, "before_cursor_execute", capture)
        try:
            with pytest.raises(NotFound, match="execution authority"):
                _select(
                    case,
                    case.principal,
                    case.service._cancel_write_guard(case.collection_id, case.principal),
                )
        finally:
            event.remove(engine, "before_cursor_execute", capture)


def test_missing_original_cancel_selector_refuses_before_local_probes(tmp_path, monkeypatch):
    with _tagged(tmp_path, count=3) as case:
        guard = case.service._cancel_write_guard(case.collection_id, _CREATOR)
        operation = _select(case, _CREATOR, guard)
        with session_scope(case.factory) as session:
            session.delete(session.get(ArchiveUploadTerminalTagRecord, operation))

        def forbidden(*args, **kwargs):
            pytest.fail("missing original cancel context reached local probes")

        monkeypatch.setattr(case.service._archive_finalizers, "local_terminal_status", forbidden)
        with pytest.raises(NotFound, match="original tag selector"):
            _observe(case, operation, guard)
