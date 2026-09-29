from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass

from riverhog_canonical_json import format_scalar, parse_scalar, require_canonical_json
from riverhog_protocol.artifact_identity import ArtifactId
from riverhog_protocol.pack_ingress import canonical_json_bytes

from riverhog_core.collection_plan import CollectionVolumePolicy
from riverhog_core.domain.archive import ArchiveArtifact, PackVolumePlan, RawVolumePlan
from riverhog_core.pack_volume import plan_pack_volume
from riverhog_core.raw_volume import plan_raw_volumes

INCREMENTAL_VOLUME_PLANNER_CHECKPOINT_FORMAT = "incremental-volume-planner-checkpoint/v1"
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")


@dataclass(frozen=True, slots=True)
class OrderedArchiveArtifact:
    order: int
    artifact: ArchiveArtifact


@dataclass(frozen=True, slots=True)
class IncrementalVolumePlannerCheckpoint:
    policy: CollectionVolumePolicy
    next_artifact_order: int = 0
    next_sequence: int = 0
    artifacts_seen: int = 0
    bytes_seen: int = 0
    pending_pack_artifacts: tuple[ArchiveArtifact, ...] = ()
    closed: bool = False

    def __post_init__(self) -> None:
        if (
            min(
                self.next_artifact_order,
                self.next_sequence,
                self.artifacts_seen,
                self.bytes_seen,
            )
            < 0
        ):
            raise ValueError("incremental planner counters must be non-negative")
        if self.next_artifact_order != self.artifacts_seen:
            raise ValueError("incremental planner order differs from accepted count")
        format_scalar("sequence256", self.next_sequence)
        if len(self.pending_pack_artifacts) > self.policy.pack_artifacts:
            raise ValueError("incremental planner pending pack exceeds its artifact limit")
        pending_bytes = 0
        seen: set[str] = set()
        for current in self.pending_pack_artifacts:
            normalized = _normalized_artifact(current)
            if normalized.artifact_id in seen:
                raise ValueError("incremental planner repeats a pending artifact")
            if normalized.bytes >= self.policy.pack_member_bytes:
                raise ValueError("incremental planner pending artifact is outside pack policy")
            seen.add(normalized.artifact_id)
            pending_bytes += normalized.bytes
        if pending_bytes > self.policy.pack_source_bytes:
            raise ValueError("incremental planner pending pack exceeds its byte limit")
        if self.closed and self.pending_pack_artifacts:
            raise ValueError("closed incremental planner cannot retain pending artifacts")


@dataclass(frozen=True, slots=True)
class IncrementalVolumePlanBatch:
    checkpoint: IncrementalVolumePlannerCheckpoint
    packs: tuple[PackVolumePlan, ...]
    raw_volumes: tuple[RawVolumePlan, ...]

    @property
    def volumes(self) -> tuple[PackVolumePlan | RawVolumePlan, ...]:
        return tuple(sorted((*self.packs, *self.raw_volumes), key=lambda current: current.sequence))


def new_incremental_volume_planner(
    *,
    policy: CollectionVolumePolicy | None = None,
) -> IncrementalVolumePlannerCheckpoint:
    return IncrementalVolumePlannerCheckpoint(policy=policy or CollectionVolumePolicy())


def normalize_ordered_archive_artifact(
    value: OrderedArchiveArtifact,
) -> OrderedArchiveArtifact:
    if value.order < 0:
        raise ValueError("incremental planner artifact order must be non-negative")
    return OrderedArchiveArtifact(order=value.order, artifact=_normalized_artifact(value.artifact))


def advance_incremental_volume_plan(
    checkpoint: IncrementalVolumePlannerCheckpoint,
    artifacts: Sequence[OrderedArchiveArtifact],
    *,
    final: bool = False,
) -> IncrementalVolumePlanBatch:
    """Advance bounded physical volume planning without retaining all members.

    The caller persists the checkpoint and emitted plans with accepted registration rows.
    This cursor does not commit member identity: a separate ID-ordered catalog scan does.
    """

    if checkpoint.closed:
        raise ValueError("incremental volume planner is already closed")
    pending = list(checkpoint.pending_pack_artifacts)
    pending_bytes = sum(current.bytes for current in pending)
    next_order = checkpoint.next_artifact_order
    next_sequence = checkpoint.next_sequence
    artifacts_seen = checkpoint.artifacts_seen
    bytes_seen = checkpoint.bytes_seen
    packs: list[PackVolumePlan] = []
    raw_volumes: list[RawVolumePlan] = []

    def flush_pack() -> None:
        nonlocal pending, pending_bytes, next_sequence
        if not pending:
            return
        packs.append(
            plan_pack_volume(
                pending,
                sequence=next_sequence,
                max_member_bytes=checkpoint.policy.pack_member_bytes,
                part_plaintext_bytes=checkpoint.policy.pack_part_plaintext_bytes,
            )
        )
        next_sequence += 1
        pending = []
        pending_bytes = 0

    for raw_ordered in artifacts:
        ordered = normalize_ordered_archive_artifact(raw_ordered)
        if ordered.order != next_order:
            raise ValueError("incremental planner artifact order is not contiguous")
        current = _normalized_artifact(ordered.artifact)
        if current.bytes < checkpoint.policy.pack_member_bytes:
            if pending and (
                len(pending) >= checkpoint.policy.pack_artifacts
                or pending_bytes + current.bytes > checkpoint.policy.pack_source_bytes
            ):
                flush_pack()
            pending.append(current)
            pending_bytes += current.bytes
        else:
            flush_pack()
            planned = plan_raw_volumes(
                (current,),
                starting_sequence=next_sequence,
                max_plaintext_bytes=checkpoint.policy.raw_volume_plaintext_bytes,
            )
            raw_volumes.extend(planned)
            next_sequence += len(planned)
        next_order += 1
        artifacts_seen += 1
        bytes_seen += current.bytes

    if final:
        flush_pack()
    next_checkpoint = IncrementalVolumePlannerCheckpoint(
        policy=checkpoint.policy,
        next_artifact_order=next_order,
        next_sequence=next_sequence,
        artifacts_seen=artifacts_seen,
        bytes_seen=bytes_seen,
        pending_pack_artifacts=tuple(pending),
        closed=final,
    )
    emitted: list[PackVolumePlan | RawVolumePlan] = [*packs, *raw_volumes]
    emitted.sort(key=lambda current: current.sequence)
    if [current.sequence for current in emitted] != list(
        range(checkpoint.next_sequence, next_sequence)
    ):
        raise RuntimeError("incremental planner emitted non-canonical volume sequences")
    packs_by_sequence = tuple(current for current in emitted if isinstance(current, PackVolumePlan))
    raw_by_sequence = tuple(current for current in emitted if isinstance(current, RawVolumePlan))
    return IncrementalVolumePlanBatch(
        checkpoint=next_checkpoint,
        packs=packs_by_sequence,
        raw_volumes=raw_by_sequence,
    )


def incremental_volume_planner_checkpoint_payload(
    checkpoint: IncrementalVolumePlannerCheckpoint,
) -> dict[str, object]:
    IncrementalVolumePlannerCheckpoint(
        policy=checkpoint.policy,
        next_artifact_order=checkpoint.next_artifact_order,
        next_sequence=checkpoint.next_sequence,
        artifacts_seen=checkpoint.artifacts_seen,
        bytes_seen=checkpoint.bytes_seen,
        pending_pack_artifacts=checkpoint.pending_pack_artifacts,
        closed=checkpoint.closed,
    )
    return {
        "format": INCREMENTAL_VOLUME_PLANNER_CHECKPOINT_FORMAT,
        "policy": _policy_payload(checkpoint.policy),
        "next_artifact_order": format_scalar("nonnegative", checkpoint.next_artifact_order),
        "next_sequence": format_scalar("sequence256", checkpoint.next_sequence),
        "artifacts_seen": format_scalar("nonnegative", checkpoint.artifacts_seen),
        "bytes_seen": format_scalar("nonnegative", checkpoint.bytes_seen),
        "pending_pack_artifacts": [
            {
                "artifact_id": current.artifact_id,
                "bytes": format_scalar("nonnegative", current.bytes),
                "sha256": current.sha256,
            }
            for current in checkpoint.pending_pack_artifacts
        ],
        "closed": checkpoint.closed,
    }


def incremental_volume_planner_checkpoint_bytes(
    checkpoint: IncrementalVolumePlannerCheckpoint,
) -> bytes:
    return canonical_json_bytes(incremental_volume_planner_checkpoint_payload(checkpoint))


def parse_incremental_volume_planner_checkpoint(
    content: bytes | str,
) -> IncrementalVolumePlannerCheckpoint:
    try:
        payload = require_canonical_json(
            content if isinstance(content, bytes) else content.encode()
        )
    except (UnicodeError, ValueError) as exc:
        raise ValueError("incremental volume planner checkpoint is not canonical JSON") from exc
    expected = {
        "format",
        "policy",
        "next_artifact_order",
        "next_sequence",
        "artifacts_seen",
        "bytes_seen",
        "pending_pack_artifacts",
        "closed",
    }
    if (
        not isinstance(payload, dict)
        or payload.get("format") != INCREMENTAL_VOLUME_PLANNER_CHECKPOINT_FORMAT
        or set(payload) != expected
    ):
        raise ValueError("incremental volume planner checkpoint format mismatch")
    policy = _parse_policy(payload.get("policy"))
    raw_pending = payload.get("pending_pack_artifacts")
    if not isinstance(raw_pending, list):
        raise ValueError("incremental planner pending artifacts must be a list")
    pending: list[ArchiveArtifact] = []
    for raw in raw_pending:
        if not isinstance(raw, dict) or set(raw) != {"artifact_id", "bytes", "sha256"}:
            raise ValueError("incremental planner pending artifact is invalid")
        pending.append(
            _normalized_artifact(
                ArchiveArtifact(
                    artifact_id=str(raw.get("artifact_id", "")),
                    bytes=parse_scalar("nonnegative", raw.get("bytes")),
                    sha256=str(raw.get("sha256", "")),
                )
            )
        )
    closed = payload.get("closed")
    if not isinstance(closed, bool):
        raise ValueError("incremental planner closed flag must be boolean")
    checkpoint = IncrementalVolumePlannerCheckpoint(
        policy=policy,
        next_artifact_order=parse_scalar("nonnegative", payload.get("next_artifact_order")),
        next_sequence=parse_scalar("sequence256", payload.get("next_sequence")),
        artifacts_seen=parse_scalar("nonnegative", payload.get("artifacts_seen")),
        bytes_seen=parse_scalar("nonnegative", payload.get("bytes_seen")),
        pending_pack_artifacts=tuple(pending),
        closed=closed,
    )
    if incremental_volume_planner_checkpoint_bytes(checkpoint) != canonical_json_bytes(payload):
        raise ValueError("incremental planner checkpoint is not canonical")
    return checkpoint


def _normalized_artifact(artifact: ArchiveArtifact) -> ArchiveArtifact:
    artifact_id = str(ArtifactId(artifact.artifact_id))
    if (
        artifact.bytes < 0
        or artifact.bytes >= 1 << 63
        or _SHA256_RE.fullmatch(artifact.sha256) is None
    ):
        raise ValueError("incremental planner artifact identity is invalid")
    return ArchiveArtifact(artifact_id=artifact_id, bytes=artifact.bytes, sha256=artifact.sha256)


def _policy_payload(policy: CollectionVolumePolicy) -> dict[str, int | str]:
    return {
        "pack_source_bytes": format_scalar("nonnegative", policy.pack_source_bytes),
        "pack_artifacts": policy.pack_artifacts,
        "pack_member_bytes": format_scalar("nonnegative", policy.pack_member_bytes),
        "pack_part_plaintext_bytes": format_scalar("nonnegative", policy.pack_part_plaintext_bytes),
        "raw_volume_plaintext_bytes": format_scalar(
            "nonnegative", policy.raw_volume_plaintext_bytes
        ),
        "raw_part_plaintext_bytes": format_scalar("nonnegative", policy.raw_part_plaintext_bytes),
    }


def _parse_policy(value: object) -> CollectionVolumePolicy:
    expected = {
        "pack_source_bytes",
        "pack_artifacts",
        "pack_member_bytes",
        "pack_part_plaintext_bytes",
        "raw_volume_plaintext_bytes",
        "raw_part_plaintext_bytes",
    }
    if not isinstance(value, dict) or set(value) != expected:
        raise ValueError("incremental planner policy is invalid")
    return CollectionVolumePolicy(
        pack_source_bytes=_positive_scalar(value.get("pack_source_bytes"), "pack source bytes"),
        pack_artifacts=_positive(value.get("pack_artifacts"), "pack artifacts"),
        pack_member_bytes=_positive_scalar(value.get("pack_member_bytes"), "pack member bytes"),
        pack_part_plaintext_bytes=_positive_scalar(
            value.get("pack_part_plaintext_bytes"), "pack part plaintext bytes"
        ),
        raw_volume_plaintext_bytes=_positive_scalar(
            value.get("raw_volume_plaintext_bytes"), "raw volume plaintext bytes"
        ),
        raw_part_plaintext_bytes=_positive_scalar(
            value.get("raw_part_plaintext_bytes"), "raw part plaintext bytes"
        ),
    )


def _positive(value: object, label: str) -> int:
    parsed = _uint(value, label=label)
    if parsed < 1:
        raise ValueError(f"{label} must be positive")
    return parsed


def _positive_scalar(value: object, label: str) -> int:
    parsed = parse_scalar("nonnegative", value)
    if parsed < 1:
        raise ValueError(f"{label} must be positive")
    return parsed


def _uint(value: object, *, label: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be a non-negative integer")
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a non-negative integer") from exc
    if parsed < 0 or str(parsed) != str(value):
        raise ValueError(f"{label} must be a canonical non-negative integer")
    return parsed
