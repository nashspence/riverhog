from __future__ import annotations

import hashlib

from riverhog_core.app_permissions import (
    CATALOG_READ,
    PROVENANCE_EXPORT,
    PROVENANCE_READ,
    RETRIEVAL_MANAGE,
)
from riverhog_core.catalog_db import initialize_db, make_session_factory, session_scope
from riverhog_core.catalog_workflow_models import CollectionProcessingCapabilityRecord
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services.collection_workflows import SqlAlchemyCollectionWorkflowService

from tests.unit.artifact_scope_fixtures import persisted_artifact_scope
from tests.unit.db_helpers import sqlite_url


def test_processing_payload_and_provenance_read_scopes_are_distinct(tmp_path) -> None:
    database_url = sqlite_url(tmp_path / "catalog.sqlite3")
    initialize_db(database_url)
    fixture = persisted_artifact_scope(
        database_url,
        access=(),
        artifacts=((1, "a" * 64, 4, "b" * 64), (2, "c" * 64, 5, "d" * 64)),
    )
    token = "rhc_" + fixture.id.removeprefix("claim:")
    with session_scope(make_session_factory(database_url)) as session:
        capability = session.get(
            CollectionProcessingCapabilityRecord, fixture.artifact_scope_capability_id
        )
        assert capability is not None
        capability.token_sha256 = hashlib.sha256(token.encode()).hexdigest()
    service = SqlAlchemyCollectionWorkflowService(
        RuntimeConfig.for_testing(database_url=database_url)
    )

    payload = service.authenticate_capability(token)
    assert payload is not None
    assert payload.artifact_scope_capability_id == fixture.artifact_scope_capability_id
    for collection_id in (1, 2):
        assert payload.allows_collection(CATALOG_READ, collection_id)
        assert payload.allows_collection(RETRIEVAL_MANAGE, collection_id)
        assert not payload.allows_collection(PROVENANCE_READ, collection_id)
        assert not payload.allows_collection(PROVENANCE_EXPORT, collection_id)

    with session_scope(make_session_factory(database_url)) as session:
        capability = session.get(
            CollectionProcessingCapabilityRecord, fixture.artifact_scope_capability_id
        )
        assert capability is not None
        capability.actions_json = '["read-provenance"]'
    provenance = service.authenticate_capability(token)
    assert provenance is not None
    assert provenance.artifact_scope_capability_id == fixture.artifact_scope_capability_id
    for collection_id in (1, 2):
        assert provenance.allows_collection(CATALOG_READ, collection_id)
        assert provenance.allows_collection(PROVENANCE_READ, collection_id)
        assert provenance.allows_collection(PROVENANCE_EXPORT, collection_id)
        assert not provenance.allows_collection(RETRIEVAL_MANAGE, collection_id)

    with session_scope(make_session_factory(database_url)) as session:
        capability = session.get(
            CollectionProcessingCapabilityRecord, fixture.artifact_scope_capability_id
        )
        assert capability is not None
        capability.actions_json = '["read-root"]'
    root = service.authenticate_capability(token)
    assert root is not None
    assert root.artifact_scope_capability_id == fixture.artifact_scope_capability_id
    for collection_id in (1, 2):
        assert root.allows_collection(CATALOG_READ, collection_id)
        assert not root.allows_collection(RETRIEVAL_MANAGE, collection_id)
        assert not root.allows_collection(PROVENANCE_READ, collection_id)
        assert not root.allows_collection(PROVENANCE_EXPORT, collection_id)
