from __future__ import annotations

import hashlib
import io
import json
import tarfile

from riverhog_core.archive_manifest import collection_artifact_set_identity
from riverhog_core.domain.archive import ArchiveArtifact
from riverhog_core.incremental_plan import (
    OrderedArchiveArtifact,
    advance_incremental_volume_plan,
    incremental_volume_planner_checkpoint_bytes,
    new_incremental_volume_planner,
    parse_incremental_volume_planner_checkpoint,
)
from riverhog_core.pack_volume import iter_render_pack_upload_unit, plan_pack_volume


def test_pack_placement_uses_opaque_members_and_arrival_order_does_not_change_identity() -> None:
    payloads = {"1" * 64: b"same", "2" * 64: b"same"}
    artifacts = tuple(
        ArchiveArtifact(artifact_id, len(content), hashlib.sha256(content).hexdigest())
        for artifact_id, content in reversed(tuple(payloads.items()))
    )
    plan = plan_pack_volume(artifacts, sequence=0)
    assert plan == plan_pack_volume(tuple(reversed(artifacts)), sequence=0)
    assert collection_artifact_set_identity(artifacts) == collection_artifact_set_identity(
        tuple(reversed(artifacts))
    )

    content = b"".join(
        iter_render_pack_upload_unit(plan, 0, lambda artifact_id: (payloads[artifact_id],))
    )
    with tarfile.open(fileobj=io.BytesIO(content), mode="r:") as archive:
        assert set(archive.getnames()) == {
            "artifacts/" + artifact_id for artifact_id in payloads
        } | {".riverhog/pack-index.json"}
        index = json.load(archive.extractfile(".riverhog/pack-index.json"))
    assert {item["artifact_id"] for item in index["artifacts"]} == set(payloads)
    assert all("path" not in item for item in index["artifacts"])


def test_incremental_volume_checkpoint_is_physical_and_has_no_member_identity() -> None:
    artifacts = (
        ArchiveArtifact("2" * 64, 1, "a" * 64),
        ArchiveArtifact("1" * 64, 1, "b" * 64),
    )
    accepted = tuple(OrderedArchiveArtifact(i, row) for i, row in enumerate(artifacts))
    batch = advance_incremental_volume_plan(new_incremental_volume_planner(), accepted, final=True)
    checkpoint = batch.checkpoint
    assert checkpoint.closed and checkpoint.artifacts_seen == 2
    encoded = incremental_volume_planner_checkpoint_bytes(checkpoint)
    assert parse_incremental_volume_planner_checkpoint(encoded) == checkpoint
    assert b"content_identity" not in encoded and b"path" not in encoded
    assert collection_artifact_set_identity(artifacts) == collection_artifact_set_identity(
        tuple(reversed(artifacts))
    )
