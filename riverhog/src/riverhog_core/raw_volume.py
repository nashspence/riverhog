from __future__ import annotations

import json
import re
from collections.abc import Sequence

from riverhog_age import CHUNK_SIZE, AgeAlignedUnitPlan, ResumableAgeScryptSession
from riverhog_protocol.artifact_identity import ArtifactId
from riverhog_protocol.pack_ingress import canonical_json_bytes

from riverhog_core.domain.archive import ArchiveArtifact, RawVolumePlan

RAW_VOLUME_PLAN_FORMAT = "raw-volume-plan/v1"
DEFAULT_RAW_VOLUME_PLAINTEXT_BYTES = 16 * 1024 * 1024 * 1024
DEFAULT_RAW_PART_PLAINTEXT_BYTES = 64 * 1024 * 1024
_SHA256_RE = re.compile(r"[0-9a-f]{64}")


def plan_raw_volumes(
    artifacts: Sequence[ArchiveArtifact],
    *,
    starting_sequence: int,
    max_plaintext_bytes: int = DEFAULT_RAW_VOLUME_PLAINTEXT_BYTES,
) -> tuple[RawVolumePlan, ...]:
    if starting_sequence < 0:
        raise ValueError("raw volume starting sequence must be non-negative")
    if starting_sequence >= 1 << 256:
        raise ValueError("raw volume starting sequence exceeds its v1 representation")
    if max_plaintext_bytes <= 0:
        raise ValueError("raw volume plaintext limit must be positive")
    plans: list[RawVolumePlan] = []
    seen: set[str] = set()
    for current in sorted(artifacts, key=lambda value: value.artifact_id):
        artifact_id = str(ArtifactId(current.artifact_id))
        if artifact_id in seen:
            raise ValueError(f"duplicate collection artifact ID: {artifact_id}")
        if (
            current.bytes < 0
            or current.bytes >= 1 << 63
            or _SHA256_RE.fullmatch(current.sha256) is None
        ):
            raise ValueError(f"collection archive artifact identity is invalid: {artifact_id}")
        seen.add(artifact_id)
        offset = 0
        if current.bytes == 0:
            plans.append(
                RawVolumePlan(
                    volume_id=f"segment-{starting_sequence + len(plans):064x}",
                    sequence=starting_sequence + len(plans),
                    artifact_id=artifact_id,
                    artifact_offset=0,
                    plaintext_bytes=0,
                    artifact_bytes=0,
                    artifact_sha256=current.sha256,
                )
            )
            continue
        while offset < current.bytes:
            length = min(max_plaintext_bytes, current.bytes - offset)
            sequence = starting_sequence + len(plans)
            if sequence >= 1 << 256:
                raise ValueError("raw volume sequence exceeds its v1 representation")
            plans.append(
                RawVolumePlan(
                    volume_id=f"segment-{sequence:064x}",
                    sequence=sequence,
                    artifact_id=artifact_id,
                    artifact_offset=offset,
                    plaintext_bytes=length,
                    artifact_bytes=current.bytes,
                    artifact_sha256=current.sha256,
                )
            )
            offset += length
    return tuple(plans)


def raw_volume_plan_payload(plan: RawVolumePlan) -> dict[str, object]:
    return {
        "format": RAW_VOLUME_PLAN_FORMAT,
        "volume_id": plan.volume_id,
        "sequence": plan.sequence,
        "artifact_id": plan.artifact_id,
        "artifact_offset": plan.artifact_offset,
        "plaintext_bytes": plan.plaintext_bytes,
        "artifact_bytes": plan.artifact_bytes,
        "artifact_sha256": plan.artifact_sha256,
    }


def raw_volume_plan_bytes(plan: RawVolumePlan) -> bytes:
    return canonical_json_bytes(raw_volume_plan_payload(plan))


def parse_raw_volume_plan(content: bytes | str) -> RawVolumePlan:
    if isinstance(content, bytes):
        content = content.decode("utf-8")
    try:
        payload = json.loads(content)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("raw volume plan is not valid JSON") from exc
    if not isinstance(payload, dict) or payload.get("format") != RAW_VOLUME_PLAN_FORMAT:
        raise ValueError("raw volume plan format mismatch")
    expected = {
        "format",
        "volume_id",
        "sequence",
        "artifact_id",
        "artifact_offset",
        "plaintext_bytes",
        "artifact_bytes",
        "artifact_sha256",
    }
    if set(payload) != expected:
        raise ValueError("raw volume plan fields are invalid")
    sequence = _canonical_nonnegative_int(payload.get("sequence"), label="raw sequence")
    volume_id = str(payload.get("volume_id", ""))
    if sequence >= 1 << 256 or volume_id != f"segment-{sequence:064x}":
        raise ValueError("raw volume plan identity is invalid")
    artifact_id = str(ArtifactId(str(payload.get("artifact_id", ""))))
    artifact_offset = _canonical_nonnegative_int(
        payload.get("artifact_offset"), label="artifact offset"
    )
    plaintext_bytes = _canonical_nonnegative_int(
        payload.get("plaintext_bytes"), label="plaintext bytes"
    )
    artifact_bytes = _canonical_nonnegative_int(
        payload.get("artifact_bytes"), label="artifact bytes"
    )
    artifact_sha256 = str(payload.get("artifact_sha256", ""))
    if (
        artifact_bytes >= 1 << 63
        or artifact_offset + plaintext_bytes > artifact_bytes
        or _SHA256_RE.fullmatch(artifact_sha256) is None
    ):
        raise ValueError("raw volume artifact identity is invalid")
    return RawVolumePlan(
        volume_id=volume_id,
        sequence=sequence,
        artifact_id=artifact_id,
        artifact_offset=artifact_offset,
        plaintext_bytes=plaintext_bytes,
        artifact_bytes=artifact_bytes,
        artifact_sha256=artifact_sha256,
    )


def raw_age_aligned_unit_plans(
    plan: RawVolumePlan,
    session: ResumableAgeScryptSession,
    *,
    target_plaintext_bytes: int = DEFAULT_RAW_PART_PLAINTEXT_BYTES,
) -> tuple[AgeAlignedUnitPlan, ...]:
    if target_plaintext_bytes <= 0 or target_plaintext_bytes % CHUNK_SIZE:
        raise ValueError("raw part target must be a positive age-chunk multiple")
    chunks_per_unit = max(1, target_plaintext_bytes // CHUNK_SIZE)
    return tuple(
        session.age_aligned_unit_plans(
            plan.plaintext_bytes,
            chunks_per_unit=chunks_per_unit,
        )
    )


def _canonical_nonnegative_int(value: object, *, label: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be a non-negative integer")
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a non-negative integer") from exc
    if parsed < 0 or str(parsed) != str(value):
        raise ValueError(f"{label} must be a canonical non-negative integer")
    return parsed
