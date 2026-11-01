"""Reference-only bounded planner; never imported by product code.

Uses maintained pack/raw plan types and serializers. Persistence, HTTP and
custody are modelled separately, not claimed as production implementations.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Sequence

from riverhog_core.domain.archive import PackVolumePlan, RawVolumePlan
from riverhog_core.incremental_plan import (
    IncrementalVolumePlannerCheckpoint,
    OrderedArchiveArtifact,
    normalize_ordered_archive_artifact,
)
from riverhog_core.pack_volume import pack_volume_plan_bytes, plan_pack_volume
from riverhog_core.raw_volume import raw_volume_plan_bytes


@dataclass(frozen=True)
class Frontier:
    checkpoint: IncrementalVolumePlannerCheckpoint
    member: int = 0
    raw_offset: int = 0
    done: bool = False


def plan_bytes(plan: PackVolumePlan | RawVolumePlan) -> bytes:
    return (pack_volume_plan_bytes(plan) if isinstance(plan, PackVolumePlan)
            else raw_volume_plan_bytes(plan))


def coverage(plan: PackVolumePlan | RawVolumePlan) -> tuple[str, ...]:
    return (tuple(item.artifact_id for item in plan.members)
            if isinstance(plan, PackVolumePlan) else (plan.artifact_id,))


def advance(
    state: Frontier,
    inputs: Sequence[OrderedArchiveArtifact],
    *,
    max_plans: int,
    max_steps: int,
    max_bytes: int,
    max_edges: int,
    final: bool = False,
) -> tuple[Frontier, tuple[PackVolumePlan | RawVolumePlan, ...]]:
    """One bounded quantum; input is one admitted bounded registration batch.

    max_bytes bounds emitted serialized plans, not total Python/Native memory.
    A single pack plan still has its existing policy-bounded authoring cost.
    Cursor selection and emitted rows must be committed in the same transaction.
    """
    if min(max_plans, max_steps, max_bytes, max_edges) <= 0:
        raise ValueError("quantum grants must be positive")
    if state.done:
        return state, ()
    if state.checkpoint.closed:
        raise ValueError("planner is already closed")
    emitted = []
    used = 0
    edge_count = 0
    steps = 0
    while steps < max_steps and len(emitted) < max_plans:
        before = state
        cp = state.checkpoint
        policy = cp.policy
        plan = None
        if state.member == len(inputs):
            if final and cp.pending_pack_artifacts:
                plan = plan_pack_volume(cp.pending_pack_artifacts,
                    sequence=cp.next_sequence, max_member_bytes=policy.pack_member_bytes,
                    part_plaintext_bytes=policy.pack_part_plaintext_bytes)
                cp = replace(cp, pending_pack_artifacts=(), next_sequence=cp.next_sequence + 1)
            else:
                return replace(state, checkpoint=replace(cp, closed=final), done=True), tuple(emitted)
        else:
            item = normalize_ordered_archive_artifact(inputs[state.member])
            if item.order != cp.next_artifact_order:
                raise ValueError("noncontiguous registration")
            artifact = item.artifact
            pending = cp.pending_pack_artifacts
            flush = bool(pending) and (
                artifact.bytes >= policy.pack_member_bytes
                or len(pending) >= policy.pack_artifacts
                or sum(x.bytes for x in pending) + artifact.bytes > policy.pack_source_bytes)
            if flush:
                plan = plan_pack_volume(pending, sequence=cp.next_sequence,
                    max_member_bytes=policy.pack_member_bytes,
                    part_plaintext_bytes=policy.pack_part_plaintext_bytes)
                cp = replace(cp, pending_pack_artifacts=(), next_sequence=cp.next_sequence + 1)
            elif artifact.bytes < policy.pack_member_bytes:
                cp = replace(cp, pending_pack_artifacts=(*pending, artifact),
                    next_artifact_order=cp.next_artifact_order + 1,
                    artifacts_seen=cp.artifacts_seen + 1, bytes_seen=cp.bytes_seen + artifact.bytes)
                state = replace(state, member=state.member + 1)
            else:
                offset = state.raw_offset
                if not 0 <= offset < artifact.bytes:
                    raise ValueError("invalid retained raw offset")
                if cp.next_sequence >= 1 << 256:
                    raise ValueError("sequence representation exhausted")
                length = min(policy.raw_volume_plaintext_bytes, artifact.bytes - offset)
                plan = RawVolumePlan(
                    volume_id=f"segment-{cp.next_sequence:064x}", sequence=cp.next_sequence,
                    artifact_id=artifact.artifact_id, artifact_offset=offset,
                    plaintext_bytes=length, artifact_bytes=artifact.bytes,
                    artifact_sha256=artifact.sha256)
                cp = replace(cp, next_sequence=cp.next_sequence + 1)
                offset += length
                if offset == artifact.bytes:
                    cp = replace(cp, next_artifact_order=cp.next_artifact_order + 1,
                        artifacts_seen=cp.artifacts_seen + 1, bytes_seen=cp.bytes_seen + artifact.bytes)
                    state = replace(state, member=state.member + 1, raw_offset=0)
                else:
                    state = replace(state, raw_offset=offset)
        if plan is not None:
            size = len(plan_bytes(plan))
            edges = len(coverage(plan))
            if edges > max_edges:
                raise ValueError("one existing-format plan exceeds the coverage-row grant")
            if size > max_bytes:
                raise ValueError("one existing-format plan exceeds the quantum byte grant")
            if used + size > max_bytes or edge_count + edges > max_edges:
                return before, tuple(emitted)
            emitted.append(plan)
            used += size
            edge_count += edges
        state = replace(state, checkpoint=cp)
        steps += 1
    return state, tuple(emitted)
