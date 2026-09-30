from __future__ import annotations

import hashlib
import json

import pytest
from riverhog_archive_contracts import CollectionArchiveManifest
from riverhog_core.archive_manifest import build_collection_archive_authority
from riverhog_core.domain.archive import (
    ArchiveArtifact,
    SealedPackVolume,
    SealedProvenanceObject,
    SealedRawVolume,
    StoredArchivePart,
    VerifiedRawArtifact,
)
from riverhog_core.pack_volume import iter_render_pack_upload_unit, plan_pack_volume
from riverhog_core.raw_verification import raw_artifact_ordered_volume_commitment

from tests.fixtures.archive import age_state_json


def _artifact(digit: str, content: bytes) -> ArchiveArtifact:
    return ArchiveArtifact(digit * 64, len(content), hashlib.sha256(content).hexdigest())


def _provenance_root() -> SealedProvenanceObject:
    return SealedProvenanceObject(
        object_id="provenance-root",
        kind="provenance-root",
        relative_path="provenance/root.json.age",
        plaintext_bytes=1,
        plaintext_sha256="d" * 64,
        stored_bytes=2,
        stored_sha256="e" * 64,
        revision=None,
        completed_at="2026-08-03T00:00:00Z",
    )


def _pack(artifacts: tuple[ArchiveArtifact, ...], payloads: dict[str, bytes]):
    plan = plan_pack_volume(artifacts, sequence=0)
    parts = []
    for unit in plan.units:
        plaintext = b"".join(
            iter_render_pack_upload_unit(
                plan, unit.unit, lambda artifact_id: (payloads[artifact_id],)
            )
        )
        parts.append(
            StoredArchivePart(
                number=unit.unit + 1,
                plaintext_start=unit.plaintext_start,
                plaintext_bytes=len(plaintext),
                plaintext_sha256=hashlib.sha256(plaintext).hexdigest(),
                stored_bytes=len(plaintext) + 100,
                stored_sha256=hashlib.sha256(b"stored" + plaintext).hexdigest(),
            )
        )
    receipt = SealedPackVolume(
        volume_id=plan.volume_id,
        sequence=plan.sequence,
        relative_path=f"volumes/{plan.volume_id}.tar.age",
        artifacts=len(plan.members),
        source_bytes=sum(member.bytes for member in plan.members),
        plaintext_bytes=plan.plaintext_bytes,
        age_state_json=age_state_json(plan.plaintext_bytes),
        index_sha256=plan.index_sha256,
        plan_sha256=plan.plan_sha256,
        parts=tuple(parts),
        revision="v1",
        completed_at="2026-08-03T00:00:00Z",
    )
    return plan, receipt


def _raw(
    artifact: ArchiveArtifact, content: bytes, *, offset: int, sequence: int
) -> SealedRawVolume:
    volume_id = f"segment-{sequence:064x}"
    part = StoredArchivePart(
        number=1,
        plaintext_start=0,
        plaintext_bytes=len(content),
        plaintext_sha256=hashlib.sha256(content).hexdigest(),
        stored_bytes=len(content) + 1,
        stored_sha256=hashlib.sha256(b"stored" + content).hexdigest(),
    )
    return SealedRawVolume(
        volume_id=volume_id,
        sequence=sequence,
        relative_path=f"volumes/{volume_id}.bin.age",
        artifact_id=artifact.artifact_id,
        artifact_offset=offset,
        plaintext_bytes=len(content),
        artifact_bytes=artifact.bytes,
        artifact_sha256=artifact.sha256,
        age_state_json=age_state_json(len(content)),
        parts=(part,),
        revision=None,
        completed_at="2026-08-03T00:00:00Z",
    )


def test_root_manifest_commits_small_opaque_member_and_provenance_authorities() -> None:
    payloads = {"1" * 64: b"alpha", "2" * 64: b"beta"}
    artifacts = tuple(
        ArchiveArtifact(key, len(value), hashlib.sha256(value).hexdigest())
        for key, value in payloads.items()
    )
    manifest, volumes = build_collection_archive_authority(
        archive_generation="a" * 64,
        artifacts=artifacts,
        packs=(_pack(artifacts, payloads),),
        provenance_identity="d" * 64,
        provenance_objects=(_provenance_root(),),
    )
    payload = json.loads(manifest)
    assert payload["format"] == "collection-archive-manifest/v1"
    assert payload["artifact_set"]["count"] == "2"
    assert payload["provenance"]["identity"] == "d" * 64
    assert set(payload["volume_sequence"]) == {"sha256"}
    assert len(volumes) == 1
    assert CollectionArchiveManifest.from_json_bytes(manifest).to_mapping() == payload
    assert "files" not in payload


def test_root_manifest_requires_contiguous_raw_artifact_coverage() -> None:
    content = b"abcdefgh"
    artifact = _artifact("1", content)
    first = _raw(artifact, content[:4], offset=0, sequence=0)
    second = _raw(artifact, content[4:], offset=4, sequence=1)
    verified = VerifiedRawArtifact(
        artifact_id=artifact.artifact_id,
        bytes=artifact.bytes,
        sha256=artifact.sha256,
        ordered_volume_sha256=raw_artifact_ordered_volume_commitment(
            artifact=artifact, volumes=(first, second)
        ),
        verified_at="2026-08-03T00:00:00Z",
    )
    manifest, volumes = build_collection_archive_authority(
        archive_generation="a" * 64,
        artifacts=(artifact,),
        packs=(),
        raw_volumes=(first, second),
        verified_raw_artifacts=(verified,),
        provenance_identity="d" * 64,
        provenance_objects=(_provenance_root(),),
    )
    assert len(volumes) == 2
    assert (
        CollectionArchiveManifest.from_json_bytes(manifest).to_mapping()["artifact_set"]["count"]
        == "1"
    )

    gapped = _raw(artifact, content[5:], offset=5, sequence=1)
    with pytest.raises(ValueError, match="(contiguous|do not form)"):
        build_collection_archive_authority(
            archive_generation="a" * 64,
            artifacts=(artifact,),
            packs=(),
            raw_volumes=(first, gapped),
            verified_raw_artifacts=(verified,),
            provenance_identity="d" * 64,
            provenance_objects=(_provenance_root(),),
        )
