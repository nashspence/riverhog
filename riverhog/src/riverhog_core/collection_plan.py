from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from riverhog_age import CHUNK_SIZE
from riverhog_archive_contracts import ARCHIVE_PACK_ARTIFACTS_MAX, ARCHIVE_VOLUME_PARTS_MAX
from riverhog_protocol.artifact_identity import ArtifactId
from riverhog_protocol.pack_ingress import canonical_json_bytes

from riverhog_core.archive_manifest import collection_artifact_set_identity
from riverhog_core.domain.archive import ArchiveArtifact, PackVolumePlan, RawVolumePlan
from riverhog_core.pack_volume import (
    DEFAULT_PACK_MEMBER_BYTES,
    DEFAULT_PACK_SOURCE_BYTES,
    DEFAULT_PART_PLAINTEXT_BYTES,
    plan_pack_volumes,
)
from riverhog_core.raw_volume import (
    DEFAULT_RAW_PART_PLAINTEXT_BYTES,
    DEFAULT_RAW_VOLUME_PLAINTEXT_BYTES,
    plan_raw_volumes,
)

COLLECTION_VOLUME_PLAN_FORMAT = "collection-volume-plan/v1"
_SHA256_RE = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True, slots=True)
class CollectionVolumePolicy:
    pack_source_bytes: int = DEFAULT_PACK_SOURCE_BYTES
    pack_artifacts: int = ARCHIVE_PACK_ARTIFACTS_MAX
    pack_member_bytes: int = DEFAULT_PACK_MEMBER_BYTES
    pack_part_plaintext_bytes: int = DEFAULT_PART_PLAINTEXT_BYTES
    raw_volume_plaintext_bytes: int = DEFAULT_RAW_VOLUME_PLAINTEXT_BYTES
    raw_part_plaintext_bytes: int = DEFAULT_RAW_PART_PLAINTEXT_BYTES

    def __post_init__(self) -> None:
        positive = (
            self.pack_source_bytes,
            self.pack_artifacts,
            self.pack_member_bytes,
            self.pack_part_plaintext_bytes,
            self.raw_volume_plaintext_bytes,
            self.raw_part_plaintext_bytes,
        )
        if any(current < 1 for current in positive):
            raise ValueError("collection volume policy values must be positive")
        if self.pack_part_plaintext_bytes % CHUNK_SIZE:
            raise ValueError("pack part plaintext target must align to age chunks")
        if self.raw_part_plaintext_bytes % CHUNK_SIZE:
            raise ValueError("raw part plaintext target must align to age chunks")
        if self.raw_volume_plaintext_bytes < self.raw_part_plaintext_bytes:
            raise ValueError("raw volume must contain at least one configured raw part")
        if self.raw_volume_plaintext_bytes % self.raw_part_plaintext_bytes:
            raise ValueError("raw volume size must be a multiple of the raw part size")
        if self.raw_volume_plaintext_bytes // self.raw_part_plaintext_bytes > (
            ARCHIVE_VOLUME_PARTS_MAX
        ):
            raise ValueError("raw volume contains too many construction parts")
        if (
            self.pack_source_bytes + self.pack_part_plaintext_bytes - 1
        ) // self.pack_part_plaintext_bytes + 2 > ARCHIVE_VOLUME_PARTS_MAX:
            raise ValueError("pack volume contains too many construction parts")
        if self.pack_artifacts > ARCHIVE_PACK_ARTIFACTS_MAX:
            raise ValueError("pack member target exceeds the v1 construction limit")


@dataclass(frozen=True, slots=True)
class CollectionVolumePlan:
    artifacts: tuple[ArchiveArtifact, ...]
    policy: CollectionVolumePolicy
    packs: tuple[PackVolumePlan, ...]
    raw_volumes: tuple[RawVolumePlan, ...]
    artifact_set_sha256: str
    plan_sha256: str

    @property
    def volume_count(self) -> int:
        return len(self.packs) + len(self.raw_volumes)


def plan_collection_volumes(
    artifacts: Sequence[ArchiveArtifact],
    *,
    policy: CollectionVolumePolicy | None = None,
) -> CollectionVolumePlan:
    effective_policy = policy or CollectionVolumePolicy()
    normalized = _normalized_artifacts(artifacts)
    packed_artifacts = tuple(
        current for current in normalized if current.bytes < effective_policy.pack_member_bytes
    )
    raw_artifacts = tuple(
        current for current in normalized if current.bytes >= effective_policy.pack_member_bytes
    )
    packs = (
        plan_pack_volumes(
            packed_artifacts,
            source_bytes_per_volume=effective_policy.pack_source_bytes,
            artifacts_per_volume=effective_policy.pack_artifacts,
            max_member_bytes=effective_policy.pack_member_bytes,
            part_plaintext_bytes=effective_policy.pack_part_plaintext_bytes,
        )
        if packed_artifacts
        else ()
    )
    raw_volumes = (
        plan_raw_volumes(
            raw_artifacts,
            starting_sequence=len(packs),
            max_plaintext_bytes=effective_policy.raw_volume_plaintext_bytes,
        )
        if raw_artifacts
        else ()
    )
    sequences = [current.sequence for current in packs] + [
        current.sequence for current in raw_volumes
    ]
    if sequences != list(range(len(sequences))):
        raise RuntimeError("collection volume planner produced non-canonical sequences")
    artifact_set = collection_artifact_set_identity(normalized)
    base = _base_payload(
        artifacts=normalized,
        policy=effective_policy,
        packs=packs,
        raw_volumes=raw_volumes,
        artifact_set=artifact_set,
    )
    plan_sha256 = hashlib.sha256(canonical_json_bytes(base)).hexdigest()
    return CollectionVolumePlan(
        artifacts=normalized,
        policy=effective_policy,
        packs=tuple(packs),
        raw_volumes=tuple(raw_volumes),
        artifact_set_sha256=str(artifact_set["sha256"]),
        plan_sha256=plan_sha256,
    )


def _base_payload(
    *,
    artifacts: Sequence[ArchiveArtifact],
    policy: CollectionVolumePolicy,
    packs: Sequence[PackVolumePlan],
    raw_volumes: Sequence[RawVolumePlan],
    artifact_set: Mapping[str, object],
) -> dict[str, object]:
    return {
        "format": COLLECTION_VOLUME_PLAN_FORMAT,
        "policy": {
            "pack_source_bytes": policy.pack_source_bytes,
            "pack_artifacts": policy.pack_artifacts,
            "pack_member_bytes": policy.pack_member_bytes,
            "pack_part_plaintext_bytes": policy.pack_part_plaintext_bytes,
            "raw_volume_plaintext_bytes": policy.raw_volume_plaintext_bytes,
            "raw_part_plaintext_bytes": policy.raw_part_plaintext_bytes,
        },
        "artifact_set": dict(artifact_set),
        "artifacts": [
            {"artifact_id": current.artifact_id, "bytes": current.bytes, "sha256": current.sha256}
            for current in artifacts
        ],
        "volumes": [
            *(
                {
                    "id": current.volume_id,
                    "sequence": current.sequence,
                    "kind": "pack",
                    "plaintext_bytes": current.plaintext_bytes,
                    "artifacts": len(current.members),
                    "source_bytes": sum(member.bytes for member in current.members),
                    "index_sha256": current.index_sha256,
                    "plan_sha256": current.plan_sha256,
                }
                for current in packs
            ),
            *(
                {
                    "id": current.volume_id,
                    "sequence": current.sequence,
                    "kind": "segment",
                    "artifact_id": current.artifact_id,
                    "artifact_offset": current.artifact_offset,
                    "plaintext_bytes": current.plaintext_bytes,
                    "artifact_bytes": current.artifact_bytes,
                    "artifact_sha256": current.artifact_sha256,
                }
                for current in raw_volumes
            ),
        ],
    }


def _normalized_artifacts(artifacts: Sequence[ArchiveArtifact]) -> tuple[ArchiveArtifact, ...]:
    normalized: list[ArchiveArtifact] = []
    seen: set[str] = set()
    for current in artifacts:
        artifact_id = str(ArtifactId(current.artifact_id))
        if artifact_id in seen:
            raise ValueError(f"duplicate collection artifact ID: {artifact_id}")
        if (
            current.bytes < 0
            or current.bytes >= 1 << 63
            or _SHA256_RE.fullmatch(current.sha256) is None
        ):
            raise ValueError(f"collection volume plan artifact identity is invalid: {artifact_id}")
        seen.add(artifact_id)
        normalized.append(
            ArchiveArtifact(artifact_id=artifact_id, bytes=current.bytes, sha256=current.sha256)
        )
    if not normalized:
        raise ValueError("collection volume plan requires at least one artifact")
    return tuple(sorted(normalized, key=lambda current: current.artifact_id))
