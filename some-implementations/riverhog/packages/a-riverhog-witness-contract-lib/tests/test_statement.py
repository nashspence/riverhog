from __future__ import annotations

import pytest
from a_riverhog_witness_contract_lib import CollectionWitnessStatement
from riverhog_protocol import CatalogSyncDescriptor


def _descriptor(*, description: str | None = None, revision: str = "1") -> CatalogSyncDescriptor:
    return CatalogSyncDescriptor(
        collection_id="42",
        archive_root_sha256="a" * 64,
        content_identity="b" * 64,
        description=description,
        description_revision=0 if description is None else 1,
        description_identity="c" * 64,
        tag_revision=1,
        tag_set_identity="d" * 64,
        revision=revision,
    )


def test_statement_is_canonical_and_ignores_mutable_catalog_fields() -> None:
    first = CollectionWitnessStatement.from_catalog("e" * 64, _descriptor())
    changed = CollectionWitnessStatement.from_catalog(
        "e" * 64, _descriptor(description="new description", revision="9")
    )
    assert first == changed
    raw = first.serialize()
    assert raw == (
        b"a-riverhog-collection-witness/v1\n"
        b'{"archive_root_sha256":"'
        + b"a" * 64
        + b'","collection_id":"42","content_identity":"'
        + b"b" * 64
        + b'","format":"a-riverhog-collection-witness/v1","source_identity":"'
        + b"e" * 64
        + b'"}\n'
    )
    assert CollectionWitnessStatement.parse(raw) == first
    assert (
        first.sha256().hex() == "d6801d31ae6d035a3af71df2671b0b681f4c82be47b89bdbcce49d3fdf84c8e3"
    )


def test_statement_rejects_wrong_authority_and_noncanonical_bytes() -> None:
    statement = CollectionWitnessStatement.from_catalog("e" * 64, _descriptor())
    raw = statement.serialize()
    with pytest.raises(ValueError):
        CollectionWitnessStatement(
            source_identity="E" * 64,
            collection_id=42,
            archive_root_sha256="a" * 64,
            content_identity="b" * 64,
        )
    with pytest.raises(ValueError):
        CollectionWitnessStatement(
            source_identity="e" * 64,
            collection_id=0,
            archive_root_sha256="a" * 64,
            content_identity="b" * 64,
        )
    with pytest.raises(ValueError):
        CollectionWitnessStatement.parse(raw.replace(b'"42"', b'"042"'))
    with pytest.raises(ValueError):
        CollectionWitnessStatement.parse(raw + b"\n")
    with pytest.raises(ValueError):
        CollectionWitnessStatement.parse(raw.replace(b'"format"', b'"other"'))
