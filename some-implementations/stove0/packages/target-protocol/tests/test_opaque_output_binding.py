from __future__ import annotations

import pytest
from stove0_target_protocol import (
    OutputArtifact,
    OutputArtifactSetIdentity,
    TargetOutputBinding,
)


def test_equal_byte_products_keep_independent_member_bindings() -> None:
    first = OutputArtifact(
        id="product-a", role="archive-media", artifact_id="a" * 64, bytes="3", sha256="c" * 64
    )
    second = OutputArtifact(
        id="product-b", role="archive-media", artifact_id="b" * 64, bytes="3", sha256="c" * 64
    )
    sealed = OutputArtifactSetIdentity.seal((first, second))
    assert sealed.artifact_count == 2
    assert sealed.total_bytes == 6
    assert sealed.sha256 != OutputArtifactSetIdentity.seal((first,)).sha256
    assert "path" not in first.model_dump(mode="json")

    collection = {
        "collection_id": "7",
        "archive_root_sha256": "d" * 64,
        "artifact_set_identity": "e" * 64,
        "derivation_sha256": "f" * 64,
    }
    binding = TargetOutputBinding(
        output_id=first.id,
        role=first.role,
        collection=collection,
        artifact_id=first.artifact_id,
        bytes=str(first.bytes),
        sha256=first.sha256,
    )
    assert binding.artifact_id == first.artifact_id
    with pytest.raises(ValueError):
        TargetOutputBinding.model_validate({**binding.model_dump(mode="json"), "path": "clip.mkv"})
