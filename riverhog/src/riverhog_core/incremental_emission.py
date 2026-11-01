from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace

from riverhog_canonical_json import format_scalar, parse_scalar, require_canonical_json
from riverhog_protocol.pack_ingress import canonical_json_bytes

from riverhog_core.domain.archive import PackVolumePlan, RawVolumePlan
from riverhog_core.incremental_plan import (
    IncrementalVolumePlannerCheckpoint,
    OrderedArchiveArtifact,
    incremental_volume_planner_checkpoint_payload,
    normalize_ordered_archive_artifact,
    parse_incremental_volume_planner_checkpoint,
)
from riverhog_core.pack_volume import pack_volume_plan_bytes, plan_pack_volume
from riverhog_core.raw_volume import raw_volume_plan_bytes

_FRONTIER_FORMAT = "incremental-volume-emission-frontier/v1"


@dataclass(frozen=True, slots=True)
class VolumeEmissionFrontier:
    """Candidate progress inside one retained, ordered registration request.

    The checkpoint counts a raw member only after its entire extent is emitted.
    This candidate is not the accepted registration checkpoint.
    """

    checkpoint: IncrementalVolumePlannerCheckpoint
    input_index: int = 0
    raw_offset: int = 0
    done: bool = False

    def __post_init__(self) -> None:
        if any(
            type(value) is not int or value < 0 for value in (self.input_index, self.raw_offset)
        ):
            raise ValueError("emission frontier counters must be non-negative integers")
        if type(self.done) is not bool:
            raise ValueError("emission frontier completion must be boolean")
        if self.raw_offset and (
            self.done
            or self.checkpoint.closed
            or self.checkpoint.pending_pack_artifacts
            or self.raw_offset % self.checkpoint.policy.raw_volume_plaintext_bytes
        ):
            raise ValueError("emission frontier has an invalid retained raw offset")
        if self.checkpoint.closed and not self.done:
            raise ValueError("closed emission frontier must be complete")


@dataclass(frozen=True, slots=True)
class VolumeEmissionLimits:
    plans: int
    coverage_rows: int
    metadata_bytes: int
    input_transitions: int

    def __post_init__(self) -> None:
        if any(
            type(value) is not int or value <= 0
            for value in (
                self.plans,
                self.coverage_rows,
                self.metadata_bytes,
                self.input_transitions,
            )
        ):
            raise ValueError("emission grants must be positive integers")


@dataclass(frozen=True, slots=True)
class VolumeEmissionQuantum:
    frontier: VolumeEmissionFrontier
    volumes: tuple[PackVolumePlan | RawVolumePlan, ...]
    coverage_rows: int
    metadata_bytes: int
    input_transitions: int


class VolumeEmissionCapacity(ValueError):
    """One existing-format plan cannot fit the admitted quantum."""

    def __init__(self, *, dimension: str, required: int, admitted: int) -> None:
        self.dimension = dimension
        self.required = required
        self.admitted = admitted
        super().__init__(f"one volume requires {required} {dimension}; quantum admits {admitted}")


def volume_emission_frontier_bytes(frontier: VolumeEmissionFrontier) -> bytes:
    return canonical_json_bytes(
        {
            "format": _FRONTIER_FORMAT,
            "checkpoint": incremental_volume_planner_checkpoint_payload(frontier.checkpoint),
            "input_index": format_scalar("nonnegative", frontier.input_index),
            "raw_offset": format_scalar("nonnegative", frontier.raw_offset),
            "done": frontier.done,
        }
    )


def parse_volume_emission_frontier(content: bytes | str) -> VolumeEmissionFrontier:
    payload = require_canonical_json(content.encode() if isinstance(content, str) else content)
    if (
        not isinstance(payload, dict)
        or set(payload) != {"format", "checkpoint", "input_index", "raw_offset", "done"}
        or payload["format"] != _FRONTIER_FORMAT
    ):
        raise ValueError("emission frontier format mismatch")
    done = payload["done"]
    if not isinstance(done, bool):
        raise ValueError("emission frontier completion must be boolean")
    frontier = VolumeEmissionFrontier(
        checkpoint=parse_incremental_volume_planner_checkpoint(
            canonical_json_bytes(payload["checkpoint"])
        ),
        input_index=parse_scalar("nonnegative", payload["input_index"]),
        raw_offset=parse_scalar("nonnegative", payload["raw_offset"]),
        done=done,
    )
    if volume_emission_frontier_bytes(frontier) != canonical_json_bytes(payload):
        raise ValueError("emission frontier is not canonical")
    return frontier


def volume_emission_metadata(
    plan: PackVolumePlan | RawVolumePlan,
) -> tuple[bytes, tuple[str, ...], bytes]:
    """Exact plan and coverage metadata charged to an emission quantum.

    Database/index overhead and the existing single-pack author's workspace are
    separate costs. Coverage order follows the immutable plan, not query order.
    """

    if isinstance(plan, PackVolumePlan):
        body = pack_volume_plan_bytes(plan)
        members = tuple(member.artifact_id for member in plan.members)
    else:
        body = raw_volume_plan_bytes(plan)
        members = (plan.artifact_id,)
    edges = canonical_json_bytes({"object_id": plan.volume_id, "artifact_ids": list(members)})
    return body, members, edges


def advance_volume_emission(
    frontier: VolumeEmissionFrontier,
    artifacts: Sequence[OrderedArchiveArtifact],
    *,
    limits: VolumeEmissionLimits,
    final: bool = False,
) -> VolumeEmissionQuantum:
    """Emit one bounded window, resuming raw construction at its retained offset.

    The owner pins the exact bounded request and commits the returned candidate
    with its immutable plan/coverage rows. It publishes acceptance separately.
    """

    if frontier.input_index > len(artifacts):
        raise ValueError("emission frontier is beyond its retained request")
    if frontier.done:
        if frontier.input_index != len(artifacts) or frontier.checkpoint.closed != final:
            raise ValueError("completed emission frontier differs from its request")
        return VolumeEmissionQuantum(frontier, (), 0, 0, 0)
    if frontier.checkpoint.closed:
        raise ValueError("volume planner is already closed")
    if frontier.raw_offset and frontier.input_index == len(artifacts):
        raise ValueError("raw emission frontier has no retained member")

    state = frontier
    emitted: list[PackVolumePlan | RawVolumePlan] = []
    used_bytes = used_edges = transitions = 0
    while transitions < limits.input_transitions and len(emitted) < limits.plans:
        checkpoint = state.checkpoint
        policy = checkpoint.policy
        candidate = state
        plan: PackVolumePlan | RawVolumePlan | None = None
        if state.input_index == len(artifacts):
            if final and checkpoint.pending_pack_artifacts:
                plan = plan_pack_volume(
                    checkpoint.pending_pack_artifacts,
                    sequence=checkpoint.next_sequence,
                    max_member_bytes=policy.pack_member_bytes,
                    part_plaintext_bytes=policy.pack_part_plaintext_bytes,
                )
                checkpoint = replace(
                    checkpoint,
                    next_sequence=checkpoint.next_sequence + 1,
                    pending_pack_artifacts=(),
                )
            else:
                state = replace(state, checkpoint=replace(checkpoint, closed=final), done=True)
                break
        else:
            ordered = normalize_ordered_archive_artifact(artifacts[state.input_index])
            if ordered.order != checkpoint.next_artifact_order:
                raise ValueError("emission request artifact order is not contiguous")
            artifact = ordered.artifact
            pending = checkpoint.pending_pack_artifacts
            if pending and (
                artifact.bytes >= policy.pack_member_bytes
                or len(pending) >= policy.pack_artifacts
                or sum(member.bytes for member in pending) + artifact.bytes
                > policy.pack_source_bytes
            ):
                plan = plan_pack_volume(
                    pending,
                    sequence=checkpoint.next_sequence,
                    max_member_bytes=policy.pack_member_bytes,
                    part_plaintext_bytes=policy.pack_part_plaintext_bytes,
                )
                checkpoint = replace(
                    checkpoint,
                    next_sequence=checkpoint.next_sequence + 1,
                    pending_pack_artifacts=(),
                )
            elif artifact.bytes < policy.pack_member_bytes:
                if state.raw_offset:
                    raise ValueError("raw emission frontier names a pack member")
                checkpoint = replace(
                    checkpoint,
                    next_artifact_order=checkpoint.next_artifact_order + 1,
                    artifacts_seen=checkpoint.artifacts_seen + 1,
                    bytes_seen=checkpoint.bytes_seen + artifact.bytes,
                    pending_pack_artifacts=(*pending, artifact),
                )
                candidate = replace(state, input_index=state.input_index + 1)
            else:
                offset = state.raw_offset
                if not 0 <= offset < artifact.bytes:
                    raise ValueError("raw emission offset is outside its retained member")
                length = min(policy.raw_volume_plaintext_bytes, artifact.bytes - offset)
                plan = RawVolumePlan(
                    volume_id=f"segment-{checkpoint.next_sequence:064x}",
                    sequence=checkpoint.next_sequence,
                    artifact_id=artifact.artifact_id,
                    artifact_offset=offset,
                    plaintext_bytes=length,
                    artifact_bytes=artifact.bytes,
                    artifact_sha256=artifact.sha256,
                )
                checkpoint = replace(checkpoint, next_sequence=checkpoint.next_sequence + 1)
                if offset + length == artifact.bytes:
                    checkpoint = replace(
                        checkpoint,
                        next_artifact_order=checkpoint.next_artifact_order + 1,
                        artifacts_seen=checkpoint.artifacts_seen + 1,
                        bytes_seen=checkpoint.bytes_seen + artifact.bytes,
                    )
                    candidate = replace(state, input_index=state.input_index + 1, raw_offset=0)
                else:
                    candidate = replace(state, raw_offset=offset + length)
        if plan is not None:
            body, members, edges = volume_emission_metadata(plan)
            size = len(body) + len(edges)
            for dimension, required, admitted in (
                ("coverage_rows", len(members), limits.coverage_rows),
                ("metadata_bytes", size, limits.metadata_bytes),
            ):
                if required > admitted:
                    raise VolumeEmissionCapacity(
                        dimension=dimension, required=required, admitted=admitted
                    )
            if (
                used_bytes + size > limits.metadata_bytes
                or used_edges + len(members) > limits.coverage_rows
            ):
                break
            emitted.append(plan)
            used_bytes += size
            used_edges += len(members)
        state = replace(candidate, checkpoint=checkpoint)
        transitions += 1
    return VolumeEmissionQuantum(
        frontier=state,
        volumes=tuple(emitted),
        coverage_rows=used_edges,
        metadata_bytes=used_bytes,
        input_transitions=transitions,
    )
