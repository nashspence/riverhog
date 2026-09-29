from __future__ import annotations

import pytest
from pydantic import ValidationError
from riverhog_protocol import (
    PortableCollectionArtifact,
    PortableCollectionError,
    PortableCollectionHeader,
    PortableCollectionIdentityBuilder,
    PortableCollectionInventoryAuthority,
    PortableCollectionInventoryPage,
)


def test_portable_inventory_commits_to_opaque_members_and_mandatory_provenance() -> None:
    header = PortableCollectionHeader(
        collection="7",
        artifact_set_identity="a" * 64,
        encryption_format="age-v1-scrypt",
        passphrase_id="collection-test-key-v1",
        provenance_identity="b" * 64,
    )
    artifacts = (
        PortableCollectionArtifact(artifact_id="0" * 63 + "1", bytes=1, sha256="c" * 64),
        PortableCollectionArtifact(artifact_id="0" * 63 + "2", bytes=2, sha256="d" * 64),
    )
    builder = PortableCollectionIdentityBuilder(header)
    for artifact in artifacts:
        builder.add(artifact)
    page = PortableCollectionInventoryPage(
        authority=PortableCollectionInventoryAuthority(
            header=header,
            inventory_identity=builder.identity,
            artifact_count="2",
            artifact_bytes="3",
        ),
        artifacts=[artifact.to_mapping() for artifact in artifacts],
        complete=True,
    )

    assert [item.artifact_id for item in page.artifacts] == [
        "0" * 63 + "1",
        "0" * 63 + "2",
    ]
    assert len(page.authority.inventory_identity) == 64


def test_portable_inventory_rejects_path_and_missing_provenance() -> None:
    with pytest.raises(PortableCollectionError, match="fields are invalid"):
        PortableCollectionArtifact.from_mapping(
            {"path": "old-name", "bytes": "1", "sha256": "b" * 64}
        )
    with pytest.raises(PortableCollectionError, match="artifact bytes"):
        PortableCollectionArtifact(artifact_id="0" * 64, bytes=-1, sha256="b" * 64)
    with pytest.raises(ValidationError, match="provenance_identity"):
        PortableCollectionHeader.model_validate(
            {
                "collection": "7",
                "artifact_set_identity": "a" * 64,
                "encryption_format": "age-v1-scrypt",
                "passphrase_id": "collection-test-key-v1",
            }
        )
