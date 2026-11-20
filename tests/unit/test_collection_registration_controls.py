from __future__ import annotations

from dataclasses import replace

import pytest
from riverhog_core.app_permissions import CATALOG_READ, ApplicationAccess
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_registration_models import CollectionRegistrationVolumeRecord
from riverhog_core.registration_tuning import RegistrationTuning
from riverhog_core.services import collection_uploads as uploads
from riverhog_core.services.app_keys import SqlAlchemyAppKeyService
from riverhog_core.services.collection_registration import CollectionRegistrationOwner
from riverhog_protocol.errors import BadRequest, Conflict, NotFound
from riverhog_protocol.pack_ingress import canonical_json_bytes
from sqlalchemy import delete, event

from tests.unit.test_app_keys import BOOTSTRAP
from tests.unit.test_collection_registration_operations import (
    LIMITS,
    _close,
    _member,
    _open,
    _read,
    _until,
)


def _published(service, collection_id, principal, member=None):
    owner = CollectionRegistrationOwner(service)
    pending = owner.admit(collection_id, [member or _member()], principal=principal)
    operation = pending["operation_id"]
    _until(service, collection_id, operation, "ready")
    return owner.advance(collection_id, operation, limits=LIMITS)


def test_pending_control_does_not_execute_or_expose_a_staged_summary(tmp_path):
    service, keys, principal, created, collection_id = _open(tmp_path)
    try:
        owner = CollectionRegistrationOwner(service)
        pending = owner.admit(collection_id, [_member()], principal=principal)
        owner.advance(collection_id, pending["operation_id"], limits=LIMITS)
        before = _read(service, pending["operation_id"])
        for _ in range(3):
            assert (
                owner.status(collection_id, pending["operation_id"], principal=principal) == pending
            )
            with pytest.raises(Conflict, match="not published"):
                owner.list_volumes(collection_id, pending["operation_id"], principal=principal)
        assert _read(service, pending["operation_id"]) == before
    finally:
        _close(service, keys)


def test_summary_pages_are_pinned_across_append_and_cold_reopen(tmp_path):
    service, keys, principal, created, collection_id = _open(tmp_path)
    acknowledgement = _published(service, collection_id, principal)
    operation = acknowledgement["operation_id"]
    owner = CollectionRegistrationOwner(service)
    page = owner.list_volumes(collection_id, operation, principal=principal, page_size=3)
    summaries = page["volumes"]
    assert len(summaries) == 3
    assert not page["exhausted"]
    token = page["next_page_token"]
    _published(service, collection_id, principal, _member("3", 1))
    config, stores, policy = service._config, service._archive_stores, service._policy
    _close(service, keys)
    service = uploads.SqlAlchemyCollectionUploadService(config, stores, policy=policy)
    keys = SqlAlchemyAppKeyService(config)
    principal = keys.authenticate(str(created["token"]))
    try:
        owner = CollectionRegistrationOwner(service)
        assert owner.status(collection_id, operation, principal=principal) == acknowledgement
        while token is not None:
            page = owner.list_volumes(
                collection_id, operation, principal=principal, page_size=3, page_token=token
            )
            assert page["emitted"] == acknowledgement["emitted"]
            summaries.extend(page["volumes"])
            token = page["next_page_token"]
        assert page["exhausted"]
        assert [int(row["sequence"], 16) for row in summaries] == list(range(17))
        assert all(row["kind"] == "segment" for row in summaries)
    finally:
        _close(service, keys)


def test_summary_cursor_rejects_other_operation_request_principal_and_tampering(tmp_path):
    service, keys, principal, created, collection_id = _open(tmp_path)
    try:
        first = _published(service, collection_id, principal)
        second = _published(service, collection_id, principal, _member("3", 4))
        owner = CollectionRegistrationOwner(service)
        page = owner.list_volumes(
            collection_id, first["operation_id"], principal=principal, page_size=2
        )
        token = page["next_page_token"]
        other = keys.create(app=principal.id, access=tuple(principal.access), grantor=BOOTSTRAP)
        other_principal = keys.authenticate(str(other["token"]))
        for op, actor, size, supplied in (
            (second["operation_id"], principal, 2, token),
            (first["operation_id"], other_principal, 2, token),
            (first["operation_id"], principal, 3, token),
            (first["operation_id"], principal, 2, "x" + token),
        ):
            with pytest.raises(BadRequest, match="token"):
                owner.list_volumes(
                    collection_id, op, principal=actor, page_size=size, page_token=supplied
                )
    finally:
        _close(service, keys)


def test_summary_bytes_stop_with_an_exact_continuation_and_capacity_refusal(tmp_path):
    service, keys, principal, created, collection_id = _open(tmp_path)
    try:
        ack = _published(service, collection_id, principal)
        owner = CollectionRegistrationOwner(service)
        original = owner.list_volumes(collection_id, ack["operation_id"], principal=principal)
        one = len(canonical_json_bytes(original["volumes"][0]))
        service._config = replace(
            service._config, registration=RegistrationTuning(summary_page_bytes=one)
        )
        observed = []
        token = None
        while True:
            page = owner.list_volumes(
                collection_id, ack["operation_id"], principal=principal, page_token=token
            )
            assert sum(len(canonical_json_bytes(row)) for row in page["volumes"]) <= one
            observed.extend(page["volumes"])
            token = page["next_page_token"]
            if token is None:
                assert page["exhausted"]
                break
        assert observed == original["volumes"]
        service._config = replace(
            service._config, registration=RegistrationTuning(summary_page_bytes=one - 1)
        )
        from riverhog_core.incremental_emission import VolumeEmissionCapacity

        with pytest.raises(VolumeEmissionCapacity, match="summary_page_bytes"):
            owner.list_volumes(collection_id, ack["operation_id"], principal=principal)
    finally:
        _close(service, keys)


@pytest.mark.parametrize("published", [False, True])
def test_control_rejects_revoked_or_replaced_authorization(tmp_path, published):
    service, keys, principal, created, collection_id = _open(tmp_path)
    try:
        owner = CollectionRegistrationOwner(service)
        result = (
            _published(service, collection_id, principal)
            if published
            else owner.admit(collection_id, [_member()], principal=principal)
        )
        keys.replace_access(
            app=principal.id,
            key_id=principal.key_id,
            access=(ApplicationAccess(CATALOG_READ),),
            grantor=BOOTSTRAP,
        )
        for actor in (principal, keys.authenticate(str(created["token"]))):
            with pytest.raises(NotFound):
                owner.status(collection_id, result["operation_id"], principal=actor)
            with pytest.raises(NotFound):
                owner.list_volumes(collection_id, result["operation_id"], principal=actor)
        keys.revoke(app=principal.id, key_id=principal.key_id)
        with pytest.raises(NotFound, match="no longer active"):
            owner.status(collection_id, result["operation_id"], principal=principal)
    finally:
        _close(service, keys)


def test_status_uses_one_snapshot_and_reuses_the_unchanged_tag_proof(tmp_path):
    service, keys, principal, created, collection_id = _open(tmp_path)
    try:
        owner = CollectionRegistrationOwner(service)
        pending = owner.admit(collection_id, [_member()], principal=principal)
        observed = []
        engine = service._session_factory.kw["bind"]

        def statement(_connection, _cursor, sql, _parameters, _context, _many):
            observed.append(sql.lower())

        event.listen(engine, "before_cursor_execute", statement)
        try:
            assert owner.status(collection_id, pending["operation_id"], principal=principal)
        finally:
            event.remove(engine, "before_cursor_execute", statement)
        assert any(sql.strip() == "begin" for sql in observed)
        assert not any("collection_upload_tags" in sql for sql in observed)
        assert not any("collection_archive_object_uploads" in sql for sql in observed)
        assert not any("collection_upload_artifacts" in sql for sql in observed)
        assert not any("for update" in sql for sql in observed)
    finally:
        _close(service, keys)


def test_summary_missing_suffix_is_an_integrity_failure_not_exhaustion(tmp_path):
    service, keys, principal, created, collection_id = _open(tmp_path)
    try:
        ack = _published(service, collection_id, principal)
        with session_scope(service._session_factory) as session:
            session.execute(
                delete(CollectionRegistrationVolumeRecord).where(
                    CollectionRegistrationVolumeRecord.registration_id == ack["operation_id"],
                    CollectionRegistrationVolumeRecord.sequence == 16,
                )
            )
        with pytest.raises(RuntimeError, match="incomplete"):
            CollectionRegistrationOwner(service).list_volumes(
                collection_id, ack["operation_id"], principal=principal
            )
    finally:
        _close(service, keys)


def test_empty_interval_is_explicitly_exhausted(tmp_path):
    service, keys, principal, created, collection_id = _open(tmp_path)
    try:
        service.register_artifacts(collection_id, [_member()])
        ack = _published(service, collection_id, principal)
        page = CollectionRegistrationOwner(service).list_volumes(
            collection_id, ack["operation_id"], principal=principal
        )
        assert page["volumes"] == []
        assert page["emitted"]["first"] == page["emitted"]["next"]
        assert page["exhausted"] and page["next_page_token"] is None
    finally:
        _close(service, keys)
