from __future__ import annotations

import pytest
from pydantic import ValidationError
from riverhog_protocol.artifact_identity import (
    ArtifactId,
    ArtifactMemberIdentityDocument,
    derive_artifact_id,
)
from riverhog_protocol.manifest import (
    ArtifactSetIdentityBuilder,
    artifact_set_identity,
    artifact_set_identity_ordered,
)


def member(artifact_id: str, byte_count: str, sha256: str) -> ArtifactMemberIdentityDocument:
    return ArtifactMemberIdentityDocument.model_validate(
        {"artifact_id": artifact_id, "bytes": byte_count, "sha256": sha256}
    )


def test_reference_artifact_set_vectors() -> None:
    empty_payload = member(
        "0" * 64, "0", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )
    first = member(
        "0" * 63 + "1", "3", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    )
    second = member("0" * 63 + "2", "3", first.sha256)
    maximum = member("f" * 64, str((1 << 63) - 1), "a" * 64)

    assert (
        artifact_set_identity([empty_payload])
        == "32b815357b8e4eb6b58ac3c4b7e075fcba8672a390a68f4dc4ede66ca3e32394"
    )
    assert (
        artifact_set_identity([second, first])
        == "002a96446085a78b9aab4f1924ced0c95f43bb277657be774b6b239a20a9644e"
    )
    assert (
        artifact_set_identity([maximum])
        == "aa2f3143ffda0139a704b997f01f8f638c290176a3eee554b6e10856407088de"
    )


def test_set_identity_distinguishes_instances_and_rejects_repeated_ids() -> None:
    first = member("0" * 63 + "1", "3", "a" * 64)
    second = member("0" * 63 + "2", "3", "a" * 64)
    assert artifact_set_identity([first]) != artifact_set_identity([second])
    assert artifact_set_identity([first, second]) == artifact_set_identity([second, first])
    with pytest.raises(ValueError, match="strictly increasing"):
        artifact_set_identity_ordered([second, first])
    with pytest.raises(ValueError, match="strictly increasing"):
        artifact_set_identity([first, first])
    with pytest.raises(ValueError, match="nonempty"):
        ArtifactSetIdentityBuilder().finish()


def test_member_contract_is_pathless_and_size_bounded() -> None:
    with pytest.raises(ValidationError):
        ArtifactMemberIdentityDocument.model_validate(
            {"artifact_id": "0" * 64, "bytes": "1", "sha256": "a" * 64, "path": "x"}
        )
    with pytest.raises(ValidationError):
        member("0" * 64, str(1 << 63), "a" * 64)
    with pytest.raises(ValueError, match="artifact ID"):
        ArtifactId("A" * 64)


def test_allocation_vectors_keep_key_boundaries_distinct() -> None:
    assert derive_artifact_id(b"construction", b"output-1") == ArtifactId(
        "ba175f8dc1eef90a979164375ca25e44ad5179d659ff67cb41ae22f86e7aa472"
    )
    assert derive_artifact_id(b"ab", b"c") != derive_artifact_id(b"a", b"bc")
