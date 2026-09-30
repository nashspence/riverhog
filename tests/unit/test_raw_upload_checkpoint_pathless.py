from __future__ import annotations

import json

from riverhog_core.raw_upload import RawUploadCheckpoint
from riverhog_protocol import ArtifactId


def test_raw_checkpoint_serializes_opaque_artifact_id_as_json_string() -> None:
    artifact_id = ArtifactId("ab" * 32)
    checkpoint = RawUploadCheckpoint(
        collection_id=1,
        volume_id="segment-" + "0" * 64,
        object_path="archives/test/segment.age",
        relative_path="segments/segment.age",
        artifact_id=artifact_id,
        artifact_offset=0,
        plaintext_bytes=65536,
        artifact_bytes=65536,
        artifact_sha256="cd" * 32,
        target_part_plaintext_bytes=65536,
        expected_part_sha256s=("ef" * 32,),
        write_token="test-token",
        age_state_json="{}",
        next_part=0,
        archive_parts=(),
    )

    assert json.loads(checkpoint.to_json())["artifact_id"] == str(artifact_id)
