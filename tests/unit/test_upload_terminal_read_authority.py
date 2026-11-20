from __future__ import annotations

import json
from dataclasses import replace

import pytest
from riverhog_core.app_permissions import (
    COLLECTIONS_CREATE,
    COLLECTIONS_DELETE,
    ApplicationAccess,
    Principal,
    collection_resource,
    tag_resource,
)
from riverhog_core.catalog_archive_write_models import (
    ArchiveUploadTerminalRecord,
    ArchiveUploadTerminalTagRecord,
    ArchiveWriteControlRecord,
)
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import (
    AppKeyRecord,
    CollectionUploadRecord,
    CollectionUploadTagRecord,
)
from riverhog_core.services.app_keys import SqlAlchemyAppKeyService
from riverhog_core.services.archive_upload_drain import lock_terminal_selection
from riverhog_core.services.archive_write_admission import _writer
from riverhog_core.services.collection_tags import advance_collection_tag_set, collection_tag_sha256
from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy import delete, event

from tests.unit.test_app_keys import BOOTSTRAP, create_key, seed_tag
from tests.unit.test_archive_upload_terminal_tags import _reap, _select, _tagged, _teardown_upload
from tests.unit.test_collection_uploads import _CREATOR
from tests.unit.test_upload_discard_authority import _operator
from tests.unit.test_upload_terminal_claim_authority import _output


def _scoped(tags):
    return replace(
        _CREATOR,
        access=frozenset(ApplicationAccess(COLLECTIONS_CREATE, tag_resource(tag)) for tag in tags),
    )


def _shrink_live_tags(case):
    first = case.tags[0]
    with session_scope(case.factory) as session:
        session.execute(
            delete(CollectionUploadTagRecord).where(
                CollectionUploadTagRecord.collection_id == case.collection_id,
                CollectionUploadTagRecord.tag_sha256 != collection_tag_sha256(first),
            )
        )
        root = advance_collection_tag_set(
            session,
            root_sha256=None,
            tags=(first,),
            retain_for_collection_id=case.collection_id,
        )
        upload = session.get(CollectionUploadRecord, case.collection_id)
        upload.tag_staging_root_sha256 = root.root.root_sha256
        upload.tag_staging_set_identity = root.identity


def _creator_key(case, *, seed=True):
    if seed:
        for tag in case.tags:
            seed_tag(case.service._config, tag)
    keys = SqlAlchemyAppKeyService(case.service._config, session_factory=case.factory)
    created = create_key(
        keys,
        app=_CREATOR.id,
        access=tuple(ApplicationAccess(COLLECTIONS_CREATE, tag_resource(tag)) for tag in case.tags),
    )
    principal = keys.authenticate(str(created["token"]))
    assert principal is not None
    return keys, principal, created


def _select_creator(case, principal):
    return case.owner.select_terminal(
        case.collection_id,
        kind="cancel",
        request={"selected": "original-read-scope"},
        principal_id=principal.id,
        key_id=principal.key_id,
        grants=case.service._config.archive_write,
        guard=case.service._payload_write_guard(case.collection_id, principal),
        validate_selection=lambda *args: None,
    )


@pytest.mark.parametrize("teardown", (False, True))
def test_current_creator_read_uses_the_original_set_after_partial_tag_cleanup(tmp_path, teardown):
    with _tagged(tmp_path, count=8) as case:
        operation = _select(case)
        _shrink_live_tags(case)
        if teardown:
            _teardown_upload(case)
            _reap(case)
        allowed = _scoped(case.tags)
        partial = _scoped(case.tags[:1])
        case.service.require_terminal_read_access(case.collection_id, operation, allowed)
        with pytest.raises(NotFound, match="collection creation"):
            case.service.require_terminal_read_access(case.collection_id, operation, partial)
        if not teardown:
            case.service.require_read_access(case.collection_id, allowed)
            with pytest.raises(NotFound, match="collection creation"):
                case.service.require_read_access(case.collection_id, partial)


def test_original_empty_set_requires_current_wildcard_creation_rights(tmp_path):
    with _tagged(tmp_path, count=0) as case:
        operation = _select(case)
        case.service.require_terminal_read_access(case.collection_id, operation, _CREATOR)
        with pytest.raises(NotFound, match="collection creation"):
            case.service.require_terminal_read_access(
                case.collection_id, operation, _scoped(("some-other-tag",))
            )


def test_confirmed_readback_does_not_require_a_new_orphan_disposal_grant(tmp_path):
    with _output(tmp_path) as case:
        operation = _select_creator(case, case.principal)
        with session_scope(case.factory) as session:
            terminal = session.get(ArchiveUploadTerminalRecord, operation)
            terminal.phase = "done"
            terminal.result_json = '{"confirmed":"original"}'
            upload = session.get(CollectionUploadRecord, case.collection_id)
            upload.lease_expires_at = "2000-01-01T00:00:00Z"
        _, reader = _operator(case)
        case.service.require_terminal_read_access(case.collection_id, operation, reader)
        case.service.require_read_access(case.collection_id, reader)
        with session_scope(case.factory) as session:
            _writer(session)
            with pytest.raises(Conflict, match="claim remains active"):
                case.service._discard_write_guard(case.collection_id, reader)(session)
        with session_scope(case.factory) as session:
            terminal = session.get(ArchiveUploadTerminalRecord, operation)
            assert terminal.phase == "done" and json.loads(terminal.result_json) == {
                "confirmed": "original"
            }


@pytest.mark.parametrize("change", ("revoke", "delete"))
def test_current_creator_key_is_rechecked_after_selected_rows_are_cleaned(tmp_path, change):
    with _tagged(tmp_path, count=3) as case:
        keys, principal, _ = _creator_key(case)
        operation = _select_creator(case, principal)
        _teardown_upload(case)
        case.service.require_terminal_read_access(case.collection_id, operation, principal)
        if change == "revoke":
            keys.revoke(app=principal.id, key_id=principal.key_id)
        else:
            with session_scope(case.factory) as session:
                session.delete(session.get(AppKeyRecord, principal.key_id))
        with pytest.raises(NotFound, match="application key"):
            case.service.require_terminal_read_access(case.collection_id, operation, principal)


def test_fresh_narrowed_key_cannot_borrow_a_partially_cleaned_tag_set(tmp_path):
    with _tagged(tmp_path, count=3) as case:
        keys, principal, created = _creator_key(case)
        operation = _select_creator(case, principal)
        _shrink_live_tags(case)
        keys.replace_access(
            app=principal.id,
            key_id=principal.key_id,
            access=(ApplicationAccess(COLLECTIONS_CREATE, tag_resource(case.tags[0])),),
            grantor=BOOTSTRAP,
        )
        current = keys.authenticate(str(created["token"]))
        assert current is not None
        with pytest.raises(NotFound, match="authorization changed"):
            case.service.require_terminal_read_access(case.collection_id, operation, principal)
        with pytest.raises(NotFound, match="collection creation"):
            case.service.require_terminal_read_access(case.collection_id, operation, current)
        with pytest.raises(NotFound, match="collection creation"):
            case.service.require_read_access(case.collection_id, current)


def test_another_currently_authorized_key_can_read_the_original_confirmed_context(tmp_path):
    with _tagged(tmp_path, count=3) as case:
        keys, original, _ = _creator_key(case)
        _, current, _ = _creator_key(case, seed=False)
        operation = _select_creator(case, original)
        _teardown_upload(case)
        keys.revoke(app=original.id, key_id=original.key_id)
        case.service.require_terminal_read_access(case.collection_id, operation, current)
        with pytest.raises(NotFound, match="application key"):
            case.service.require_terminal_read_access(case.collection_id, operation, original)


@pytest.mark.parametrize("role", ("wrong-deletion-target", "unrelated-creator"))
def test_terminal_read_scope_is_bound_to_original_creator_or_exact_collection(tmp_path, role):
    with _tagged(tmp_path, count=3) as case:
        operation = _select(case)
        access = (
            ApplicationAccess(COLLECTIONS_DELETE, collection_resource(case.collection_id + 1))
            if role == "wrong-deletion-target"
            else ApplicationAccess(COLLECTIONS_CREATE)
        )
        reader = Principal(id="other-reader", key_id=None, access=frozenset({access}))
        with pytest.raises(NotFound, match="terminal not found"):
            case.service.require_terminal_read_access(case.collection_id, operation, reader)


@pytest.mark.parametrize("count", (0, 3))
def test_missing_original_selector_is_not_readable_as_an_empty_current_set(tmp_path, count):
    with _tagged(tmp_path, count=count) as case:
        operation = _select(case)
        with session_scope(case.factory) as session:
            session.delete(session.get(ArchiveUploadTerminalTagRecord, operation))
        with pytest.raises(NotFound, match="original tag selector"):
            case.service.require_terminal_read_access(case.collection_id, operation, _CREATOR)


def test_original_outcome_read_does_not_borrow_the_mutable_current_admission_frontier(tmp_path):
    with _tagged(tmp_path, count=3) as case:
        operation = _select(case)
        with session_scope(case.factory) as session:
            control = session.get(ArchiveWriteControlRecord, case.collection_id)
            control.gate = "open"
            control.terminal_id = None
            control.epoch = "f" * 64
            control.next_call = "5"
        case.service.require_terminal_read_access(case.collection_id, operation, _scoped(case.tags))
        with session_scope(case.factory) as session:
            with pytest.raises(Conflict, match="selection is not current"):
                lock_terminal_selection(session, case.collection_id, operation)


@pytest.mark.parametrize("count", (8, 32, 128))
def test_fixed_small_grant_refuses_without_counting_or_scanning_the_original_set(tmp_path, count):
    with _tagged(tmp_path, count=count) as case:
        operation = _select(case)
        first = min(case.tags, key=collection_tag_sha256)
        reader = _scoped((first,))
        reads = []

        def capture(connection, cursor, statement, parameters, context, executemany):
            if statement.lstrip().upper().startswith("SELECT"):
                reads.append(statement)
                assert "FROM collection_upload_tags" not in statement
                assert "count(" not in statement.lower()

        engine = case.factory.kw["bind"]
        event.listen(engine, "before_cursor_execute", capture)
        try:
            with pytest.raises(NotFound, match="collection creation"):
                case.service.require_terminal_read_access(case.collection_id, operation, reader)
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        # SHA-256 radix paths have at most 33 nodes for each of two visited tags.
        assert len(reads) <= 70


@pytest.mark.parametrize("role", ("creator", "deleter"))
def test_broad_current_read_uses_only_point_facts_and_no_writes_or_tag_walk(tmp_path, role):
    with _tagged(tmp_path, count=32) as case:
        operation = _select(case)
        reader = (
            _CREATOR
            if role == "creator"
            else Principal(
                id="deletion-reader",
                key_id=None,
                access=frozenset(
                    {ApplicationAccess(COLLECTIONS_DELETE, collection_resource(case.collection_id))}
                ),
            )
        )
        statements = []

        def capture(connection, cursor, statement, parameters, context, executemany):
            statements.append(statement)
            assert not statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            assert "FOR UPDATE" not in statement
            assert "FROM collection_upload_tags" not in statement
            assert "FROM collection_tag_nodes" not in statement

        engine = case.factory.kw["bind"]
        event.listen(engine, "before_cursor_execute", capture)
        try:
            case.service.require_terminal_read_access(case.collection_id, operation, reader)
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        assert len(statements) <= 5
