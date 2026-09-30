from __future__ import annotations

from riverhog_core.app_permissions import Principal
from riverhog_core.artifact_access import artifact_scope_filter
from riverhog_core.catalog_models import CollectionArtifactRecord
from sqlalchemy import select


def test_persisted_artifact_scope_compiles_to_one_correlated_authority_lookup() -> None:
    principal = Principal(
        id=f"claim:{'a' * 64}",
        key_id="fixture-key",
        access=frozenset(),
        artifact_scope_capability_id="a" * 32,
    )
    statement = select(CollectionArtifactRecord.artifact_id).where(
        artifact_scope_filter(
            CollectionArtifactRecord.collection_id,
            CollectionArtifactRecord.artifact_id,
            principal,
        )
    )
    compiled = statement.compile()

    assert set(compiled.params.values()) == {"a" * 32}
    assert "EXISTS" in str(compiled)
    assert "collection_processing_capability_artifacts" in str(compiled)


def test_unscoped_principal_does_not_add_an_artifact_predicate() -> None:
    predicate = artifact_scope_filter(
        CollectionArtifactRecord.collection_id,
        CollectionArtifactRecord.artifact_id,
        Principal(id="reader", key_id="reader", access=frozenset()),
    )

    assert str(predicate.compile()).lower() == "true"
