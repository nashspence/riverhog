from __future__ import annotations

from dataclasses import replace

import pytest
from riverhog_core.app_permissions import (
    CATALOG_READ,
    COLLECTION_TAGS_MANAGE,
    COLLECTIONS_CREATE,
    ApplicationAccess,
    tag_resource,
)
from riverhog_core.catalog_db import dispose_session_factory, session_scope
from riverhog_core.catalog_models import (
    AppKeyAccessGrantRecord,
    AppKeyRecord,
    CollectionUploadRecord,
)
from riverhog_core.catalog_workflow_models import CollectionProcessingClaimRecord
from riverhog_core.services import collection_uploads as uploads
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy import event, select, update

from tests.unit.test_app_keys import BOOTSTRAP
from tests.unit.test_collection_registration_delegation import _delegated
from tests.unit.test_collection_registration_operations import _close, _member, _open


def _check(service, guard):
    with session_scope(service._session_factory) as session:
        if session.get_bind().dialect.name == "sqlite":
            session.connection(execution_options={"sqlite_immediate_writer": True})
        guard(session)


@pytest.mark.parametrize(
    "invalid", ["revoked", "expired", "grants", "owner", "permission", "lease", "orphaned"]
)
def test_actual_unit_entry_rechecks_authority_before_provider_work(tmp_path, monkeypatch, invalid):
    service, keys, principal, created, collection_id = _open(
        tmp_path, custody_mode="custody-transfer"
    )
    try:
        service.register_artifacts(collection_id, [_member("1", 1)])
        volume = service.list_volumes(collection_id)["volumes"][0]
        if invalid in {"revoked", "expired", "grants", "lease", "orphaned"}:
            with session_scope(service._session_factory) as session:
                key = session.get(AppKeyRecord, principal.key_id)
                upload = session.get(CollectionUploadRecord, collection_id)
                if invalid == "revoked":
                    key.revoked_at = "2020-01-01T00:00:00.000000000Z"
                elif invalid == "expired":
                    key.expires_at = "2020-01-01T00:00:00.000000000Z"
                elif invalid == "grants":
                    key.access_generation = "a" * 64
                    session.execute(
                        update(AppKeyAccessGrantRecord)
                        .where(AppKeyAccessGrantRecord.key_id == principal.key_id)
                        .values(permission=CATALOG_READ)
                    )
                elif invalid == "lease":
                    upload.lease_expires_at = "2020-01-01T00:00:00.000000000Z"
                else:
                    upload.state = "orphaned"
        if invalid == "owner":
            principal = BOOTSTRAP
        elif invalid == "permission":
            principal = replace(principal, access=frozenset())
        provider_calls = []

        def provider(**arguments):
            provider_calls.append(arguments)
            pytest.fail("an unauthorized unit reached provider dispatch")

        monkeypatch.setattr(
            service._archive_stores.require("archive").resumable_objects, "begin_write", provider
        )
        expected = Conflict if invalid in {"lease", "orphaned"} else NotFound
        with pytest.raises(expected):
            service.upload_unit(
                collection_id,
                volume["volume_id"],
                0,
                principal=principal,
                plan_sha256=volume["plan_sha256"],
                content=b"",
            )
        assert provider_calls == []
    finally:
        _close(service, keys)


def test_payload_permission_proof_is_caller_scoped_and_rechecks_changed_tags(
    tmp_path, monkeypatch, record_property
):
    service, keys, original, created, collection_id = _open(tmp_path)
    try:
        broad_created = keys.create(
            app=original.id,
            access=(
                ApplicationAccess(COLLECTIONS_CREATE),
                ApplicationAccess(COLLECTION_TAGS_MANAGE),
            ),
            grantor=BOOTSTRAP,
        )
        broad = keys.authenticate(str(broad_created["token"]))
        assert broad is not None
        service.add_tags(collection_id, ["allowed"], principal=broad)
        narrow_created = keys.create(
            app=original.id,
            access=(ApplicationAccess(COLLECTIONS_CREATE, tag_resource("allowed")),),
            grantor=BOOTSTRAP,
        )
        narrow = keys.authenticate(str(narrow_created["token"]))
        assert narrow is not None
        checked = []
        original_check = uploads._require_upload_create_access

        def check(session, current, principal, **arguments):
            checked.append(principal)
            return original_check(session, current, principal, **arguments)

        monkeypatch.setattr(uploads, "_require_upload_create_access", check)
        guard = service._payload_write_guard(collection_id, narrow)
        statements = []
        engine = service._session_factory.kw["bind"]

        def capture(connection, cursor, statement, parameters, context, executemany):
            statements.append(statement)

        event.listen(engine, "before_cursor_execute", capture)
        try:
            for _ in range(4):
                _check(service, guard)
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        assert checked == [narrow]
        assert not any(
            forbidden in statement
            for statement in statements
            for forbidden in ("planner_checkpoint_json", "checkpoint_json", "plan_json")
        )
        record_property("repeated_guard_calls", 4)
        record_property("unchanged_tag_permission_walks", len(checked))
        service.add_tags(collection_id, ["denied"], principal=broad)
        with pytest.raises(NotFound, match="creation is not available"):
            _check(service, guard)
        # A fresh narrow actor cannot borrow the broad actor's existing proof.
        _check(service, service._payload_write_guard(collection_id, broad))
        with pytest.raises(NotFound, match="creation is not available"):
            _check(service, service._payload_write_guard(collection_id, narrow))
    finally:
        _close(service, keys)


@pytest.mark.parametrize("state", ["open", "closing", "uploading"])
def test_payload_guard_accepts_the_live_payload_state_domain(tmp_path, state):
    service, keys, principal, created, collection_id = _open(tmp_path)
    try:
        with session_scope(service._session_factory) as session:
            session.get(CollectionUploadRecord, collection_id).state = state
        _check(service, service._payload_write_guard(collection_id, principal))
    finally:
        _close(service, keys)


@pytest.mark.parametrize("combined", [False, True])
def test_payload_guard_retains_the_real_sealed_claim_and_refuses_ended_authority(
    tmp_path, combined
):
    service, keys, workflows, principal, collection_id = _delegated(tmp_path, combined=combined)
    try:
        guard = service._payload_write_guard(collection_id, principal)
        owner = ArchiveWriteAdmission(service._session_factory)
        assert len(owner.bind_epoch(collection_id, guard=guard)) == 64
        with session_scope(service._session_factory) as session:
            claim = session.scalar(
                select(CollectionProcessingClaimRecord).where(
                    CollectionProcessingClaimRecord.execution_id
                    == principal.id.removeprefix("processing:")
                )
            )
            claim.state = "settled"
        with pytest.raises(NotFound, match="execution authority"):
            owner.bind_epoch(collection_id, guard=guard)
    finally:
        dispose_session_factory(workflows._session_factory)
        _close(service, keys)


@pytest.mark.parametrize("invalid", ["revoked", "expired", "generation"])
def test_retained_payload_permission_proof_keeps_a_live_key_fence(tmp_path, invalid):
    service, keys, principal, created, collection_id = _open(tmp_path)
    try:
        guard = service._payload_write_guard(collection_id, principal)
        _check(service, guard)
        with session_scope(service._session_factory) as session:
            key = session.get(AppKeyRecord, principal.key_id)
            if invalid == "revoked":
                key.revoked_at = "2020-01-01T00:00:00.000000000Z"
            elif invalid == "expired":
                key.expires_at = "2020-01-01T00:00:00.000000000Z"
            else:
                key.access_generation = "a" * 64
        with pytest.raises(NotFound):
            _check(service, guard)
    finally:
        _close(service, keys)
