from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest
from riverhog_core.app_permissions import CATALOG_READ, ApplicationAccess
from riverhog_core.catalog_db import dispose_session_factory, session_scope
from riverhog_core.catalog_workflow_models import (
    CollectionProcessingCapabilityRecord,
    CollectionProcessingClaimRecord,
)
from riverhog_core.services.collection_registration import CollectionRegistrationOwner
from riverhog_core.services.collection_workflows import SqlAlchemyCollectionWorkflowService
from riverhog_protocol.errors import NotFound
from riverhog_protocol.output_collection_policy import OutputCollectionPolicy
from sqlalchemy import select
from time_formats import utc_timestamp_now

from tests.unit.artifact_scope_fixtures import persisted_artifact_scope
from tests.unit.test_collection_registration_operations import (
    LIMITS,
    _close,
    _member,
    _open,
    _read,
    _until,
)


def _delegated(tmp_path, *, combined=False):
    service, keys, original, created, source_id = _open(tmp_path)
    scope = persisted_artifact_scope(
        service._config.database_url,
        access=(),
        artifacts=((source_id, "a" * 64, 4, "b" * 64),),
    )
    execution = hashlib.sha256(b"registration sealed execution").hexdigest()
    token = "rhc_registration-capability-fixture"
    with session_scope(service._session_factory) as session:
        capability = session.get(
            CollectionProcessingCapabilityRecord, scope.artifact_scope_capability_id
        )
        claim = session.get(CollectionProcessingClaimRecord, capability.claim_id)
        claim.execution_id = execution
        claim.plan_sealed_at = utc_timestamp_now()
        claim.result_kind = "collection"
        claim.output_policy_json = OutputCollectionPolicy().model_dump_json()
        capability.token_sha256 = hashlib.sha256(token.encode()).hexdigest()
        capability.actions_json = (
            '["read-inputs","write-output"]' if combined else '["write-output"]'
        )
    workflows = SqlAlchemyCollectionWorkflowService(service._config)
    principal = workflows.authenticate_capability(token)
    assert principal is not None
    assert principal.id == "processing:" + execution
    assert principal.has_artifact_scope is combined
    opened = service.create_or_resume(
        idempotency_key=execution,
        ingest_source="processing:" + execution,
        archive_store=None,
        initiator=principal,
        event_context=None,
    )
    return service, keys, workflows, principal, int(opened["collection_id"])


@pytest.mark.parametrize("combined", [False, True])
def test_registration_accepts_the_real_write_capability_and_retains_its_live_claim(
    tmp_path, combined
):
    service, keys, workflows, principal, collection_id = _delegated(tmp_path, combined=combined)
    try:
        owner = CollectionRegistrationOwner(service)
        pending = owner.admit(collection_id, [_member()], principal=principal)
        operation = pending["operation_id"]
        assert owner.status(collection_id, operation, principal=principal) == pending
        _until(service, collection_id, operation, "ready")
        ack = owner.advance(collection_id, operation, limits=LIMITS)
        assert ack["phase"] == "published"
        assert owner.status(collection_id, operation, principal=principal) == ack
        page = owner.list_volumes(collection_id, operation, principal=principal, page_size=3)
        assert len(page["volumes"]) == 3 and not page["exhausted"]
        before = _read(service, operation)
        with session_scope(service._session_factory) as session:
            claim = session.scalar(
                select(CollectionProcessingClaimRecord).where(
                    CollectionProcessingClaimRecord.execution_id
                    == principal.id.removeprefix("processing:")
                )
            )
            claim.state = "settled"
        with pytest.raises(NotFound, match="execution authority"):
            owner.status(collection_id, operation, principal=principal)
        assert _read(service, operation) == before
    finally:
        dispose_session_factory(workflows._session_factory)
        _close(service, keys)


def test_delegated_readback_does_not_borrow_the_admission_permission_set(tmp_path):
    service, keys, workflows, principal, collection_id = _delegated(tmp_path, combined=True)
    try:
        owner = CollectionRegistrationOwner(service)
        operation = owner.admit(collection_id, [_member()], principal=principal)["operation_id"]
        narrowed = replace(
            principal,
            access=frozenset({ApplicationAccess(CATALOG_READ)}),
        )
        with pytest.raises(NotFound):
            owner.status(collection_id, operation, principal=narrowed)
        with pytest.raises(NotFound):
            owner.admit(collection_id, [_member()], principal=narrowed)
    finally:
        dispose_session_factory(workflows._session_factory)
        _close(service, keys)
