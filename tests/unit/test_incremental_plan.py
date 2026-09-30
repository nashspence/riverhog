from __future__ import annotations

import hashlib
import json

import pytest
from riverhog_archive_contracts import format_archive_sequence
from riverhog_core.collection_plan import CollectionVolumePolicy
from riverhog_core.domain.archive import ArchiveArtifact
from riverhog_core.incremental_plan import (
    OrderedArchiveArtifact,
    advance_incremental_volume_plan,
    incremental_volume_planner_checkpoint_bytes,
    new_incremental_volume_planner,
    parse_incremental_volume_planner_checkpoint,
)

PART = 5 * 1024 * 1024


def _artifact(order: int, digit: str, size: int) -> OrderedArchiveArtifact:
    return OrderedArchiveArtifact(
        order,
        ArchiveArtifact(digit * 64, size, hashlib.sha256(f"{digit}:{size}".encode()).hexdigest()),
    )


def _policy() -> CollectionVolumePolicy:
    return CollectionVolumePolicy(
        pack_source_bytes=10,
        pack_artifacts=2,
        pack_member_bytes=8,
        pack_part_plaintext_bytes=PART,
        raw_volume_plaintext_bytes=PART,
        raw_part_plaintext_bytes=PART,
    )


@pytest.mark.parametrize(
    "policy",
    (
        _policy(),
        CollectionVolumePolicy(
            pack_source_bytes=18,
            pack_artifacts=3,
            pack_member_bytes=12,
            pack_part_plaintext_bytes=2 * PART,
            raw_volume_plaintext_bytes=2 * PART,
            raw_part_plaintext_bytes=PART,
        ),
    ),
)
def test_incremental_restart_matches_one_shot_physical_volumes(
    policy: CollectionVolumePolicy,
) -> None:
    artifacts = (
        _artifact(0, "1", 3),
        _artifact(1, "2", 4),
        _artifact(2, "3", 0),
        _artifact(3, "4", 2 * PART + 7),
        _artifact(4, "5", 2),
    )
    initial = new_incremental_volume_planner(policy=policy)
    one_shot = advance_incremental_volume_plan(initial, artifacts, final=True)
    checkpoint = initial
    emitted = []
    for artifact in artifacts:
        batch = advance_incremental_volume_plan(checkpoint, (artifact,))
        emitted.extend(batch.volumes)
        checkpoint = parse_incremental_volume_planner_checkpoint(
            incremental_volume_planner_checkpoint_bytes(batch.checkpoint)
        )
    sealed = advance_incremental_volume_plan(checkpoint, (), final=True)
    emitted.extend(sealed.volumes)
    assert sealed.checkpoint == one_shot.checkpoint
    assert tuple(emitted) == one_shot.volumes
    assert [volume.sequence for volume in emitted] == list(range(len(emitted)))
    assert sealed.checkpoint.artifacts_seen == len(artifacts)
    assert sealed.checkpoint.bytes_seen == sum(item.artifact.bytes for item in artifacts)


def test_large_artifact_flushes_pack_before_raw_segments() -> None:
    batch = advance_incremental_volume_plan(
        new_incremental_volume_planner(policy=_policy()),
        (_artifact(0, "1", 3), _artifact(1, "2", 2 * PART + 4)),
        final=True,
    )
    assert [volume.sequence for volume in batch.packs] == [0]
    assert [volume.sequence for volume in batch.raw_volumes] == [1, 2, 3]
    assert batch.checkpoint.pending_pack_artifacts == ()


def test_checkpoint_rejects_noncontiguous_registration_and_has_no_member_set_identity() -> None:
    with pytest.raises(ValueError, match="order"):
        advance_incremental_volume_plan(
            new_incremental_volume_planner(policy=_policy()),
            (_artifact(1, "1", 1),),
        )
    checkpoint = advance_incremental_volume_plan(
        new_incremental_volume_planner(policy=_policy()),
        (_artifact(0, "1", 2),),
    ).checkpoint
    payload = json.loads(incremental_volume_planner_checkpoint_bytes(checkpoint))
    assert payload["pending_pack_artifacts"][0]["artifact_id"] == "1" * 64
    assert "artifact_set_identity" not in payload


def test_checkpoint_preserves_full_sequence_domain() -> None:
    payload = json.loads(
        incremental_volume_planner_checkpoint_bytes(
            new_incremental_volume_planner(policy=_policy())
        )
    )
    payload["next_sequence"] = format_archive_sequence((1 << 256) - 1)
    restored = parse_incremental_volume_planner_checkpoint(
        json.dumps(payload, sort_keys=True, separators=(",", ":"))
    )
    assert restored.next_sequence == (1 << 256) - 1
    payload["next_sequence"] = "1" + "0" * 64
    with pytest.raises(ValueError, match="invalid canonical integer string"):
        parse_incremental_volume_planner_checkpoint(
            json.dumps(payload, sort_keys=True, separators=(",", ":"))
        )
