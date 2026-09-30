from __future__ import annotations

import hashlib

from riverhog_core.collection_plan import (
    CollectionVolumePolicy,
    plan_collection_volumes,
)
from riverhog_core.domain.archive import ArchiveArtifact

TEST_ARCHIVE_PART_BYTES = 5 * 1024 * 1024


def _artifact(artifact_id: str, byte_count: int, marker: bytes) -> ArchiveArtifact:
    digest = hashlib.sha256(marker * byte_count).hexdigest()
    return ArchiveArtifact(artifact_id=artifact_id, bytes=byte_count, sha256=digest)


def test_collection_planner_assigns_canonical_pack_then_segment_sequences() -> None:
    policy = CollectionVolumePolicy(
        pack_source_bytes=10,
        pack_artifacts=2,
        pack_member_bytes=8,
        pack_part_plaintext_bytes=TEST_ARCHIVE_PART_BYTES,
        raw_volume_plaintext_bytes=TEST_ARCHIVE_PART_BYTES,
        raw_part_plaintext_bytes=TEST_ARCHIVE_PART_BYTES,
    )
    plan = plan_collection_volumes(
        (
            _artifact("2" * 64, 4, b"b"),
            _artifact("3" * 64, 2 * TEST_ARCHIVE_PART_BYTES + 2, b"l"),
            _artifact("1" * 64, 3, b"a"),
        ),
        policy=policy,
    )

    assert [current.volume_id for current in plan.packs] == [f"pack-{0:064x}"]
    assert [current.sequence for current in plan.raw_volumes] == [1, 2, 3]
    assert [current.artifact_offset for current in plan.raw_volumes] == [
        0,
        TEST_ARCHIVE_PART_BYTES,
        2 * TEST_ARCHIVE_PART_BYTES,
    ]
    assert plan.volume_count == 4


def test_default_policy_preserves_retrieval_economics_boundary() -> None:
    policy = CollectionVolumePolicy()
    below = _artifact("1" * 64, policy.pack_member_bytes - 1, b"b")
    at = _artifact("2" * 64, policy.pack_member_bytes, b"a")
    plan = plan_collection_volumes((below, at))

    assert policy.pack_member_bytes == 16 * 1024 * 1024
    assert policy.pack_source_bytes == 32 * 1024 * 1024
    assert [current.artifact_id for current in plan.packs[0].members] == [below.artifact_id]
    assert [current.artifact_id for current in plan.raw_volumes] == [at.artifact_id]


def test_collection_policy_exposes_persisted_layout_knobs() -> None:
    policy = CollectionVolumePolicy(
        pack_source_bytes=48 * 1024**2,
        pack_artifacts=12_000,
        pack_member_bytes=12 * 1024**2,
        pack_part_plaintext_bytes=96 * 1024**2,
        raw_volume_plaintext_bytes=24 * 1024**3,
        raw_part_plaintext_bytes=96 * 1024**2,
    )

    assert policy.pack_source_bytes == 48 * 1024**2
    assert policy.pack_artifacts == 12_000
    assert policy.pack_member_bytes == 12 * 1024**2
    assert policy.pack_part_plaintext_bytes == 96 * 1024**2
    assert policy.raw_volume_plaintext_bytes == 24 * 1024**3
    assert policy.raw_part_plaintext_bytes == 96 * 1024**2
