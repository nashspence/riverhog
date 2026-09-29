from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from typing import TypedDict

from riverhog_age import UploadState
from riverhog_archive_contracts import (
    ARCHIVE_ENCRYPTION_FORMAT,
    COLLECTION_ARCHIVE_MANIFEST_FORMAT,
    COLLECTION_ARCHIVE_TERMINAL_FORMAT,
    COLLECTION_ARCHIVE_VOLUME_FORMAT,
    PACK_INDEX_FORMAT,
    SELECTIVE_READ_FORMAT,
    CollectionArchiveManifest,
    CollectionArchiveTerminalDocument,
    CollectionArchiveVolumeDocument,
    format_archive_sequence,
    ordered_archive_volume_commitment,
)
from riverhog_canonical_json import format_scalar
from riverhog_protocol.artifact_identity import ArtifactId, ArtifactMemberIdentityDocument
from riverhog_protocol.manifest import artifact_set_identity
from riverhog_protocol.paths import validate_canonical_relpath

from riverhog_core.domain.archive import (
    ArchiveArtifact,
    PackVolumePlan,
    RawVolumePlan,
    SealedPackVolume,
    SealedProvenanceObject,
    SealedRawVolume,
    StoredArchivePart,
    VerifiedRawArtifact,
)
from riverhog_core.raw_verification import raw_artifact_ordered_volume_commitment

_SHA256_RE = re.compile(r"[0-9a-f]{64}")


class CollectionArtifactSetIdentity(TypedDict):
    count: int
    bytes: int
    sha256: str


def validate_collection_archive_plan(
    *,
    artifacts: Sequence[ArchiveArtifact],
    packs: Sequence[PackVolumePlan],
    raw_volumes: Sequence[RawVolumePlan | SealedRawVolume] = (),
) -> tuple[ArchiveArtifact, ...]:
    """Require volume plans to cover the exact opaque member set once."""

    normalized_artifacts = _normalized_artifacts(artifacts)
    expected_by_id = {current.artifact_id: current for current in normalized_artifacts}
    coverage: dict[str, list[tuple[int, int, str]]] = {
        current.artifact_id: [] for current in normalized_artifacts
    }
    plans = sorted((*packs, *raw_volumes), key=lambda current: current.sequence)
    if [current.sequence for current in plans] != list(range(len(plans))):
        raise ValueError("archive volume plan sequence must be canonical and contiguous")
    if len({current.volume_id for current in plans}) != len(plans):
        raise ValueError("archive volume plan ids must be unique")

    for pack_plan in packs:
        for member in pack_plan.members:
            expected = expected_by_id.get(member.artifact_id)
            if (
                expected is None
                or expected.bytes != member.bytes
                or expected.sha256 != member.sha256
            ):
                raise ValueError(
                    f"pack plan does not match collection artifact: {member.artifact_id}"
                )
            coverage[member.artifact_id].append((0, member.bytes, pack_plan.volume_id))

    for raw_plan in raw_volumes:
        artifact_id = str(ArtifactId(raw_plan.artifact_id))
        expected = expected_by_id.get(artifact_id)
        if expected is None:
            raise ValueError(f"raw volume references an unknown artifact: {artifact_id}")
        if raw_plan.artifact_offset < 0 or raw_plan.plaintext_bytes < 0:
            raise ValueError("raw volume placement is invalid")
        if raw_plan.artifact_bytes != expected.bytes or raw_plan.artifact_sha256 != expected.sha256:
            raise ValueError(f"raw volume artifact identity mismatch: {artifact_id}")
        if raw_plan.artifact_offset + raw_plan.plaintext_bytes > expected.bytes:
            raise ValueError(f"raw volume exceeds its artifact: {artifact_id}")
        coverage[artifact_id].append(
            (raw_plan.artifact_offset, raw_plan.plaintext_bytes, raw_plan.volume_id)
        )

    _validate_artifact_coverage(normalized_artifacts, coverage)
    return normalized_artifacts


def build_collection_archive_authority(
    *,
    archive_generation: str,
    artifacts: Sequence[ArchiveArtifact],
    packs: Sequence[tuple[PackVolumePlan, SealedPackVolume]],
    raw_volumes: Sequence[SealedRawVolume] = (),
    verified_raw_artifacts: Sequence[VerifiedRawArtifact] = (),
    provenance_identity: str,
    provenance_objects: Sequence[SealedProvenanceObject] = (),
) -> tuple[bytes, tuple[CollectionArchiveVolumeDocument, ...]]:
    normalized_artifacts = validate_collection_archive_plan(
        artifacts=artifacts,
        packs=tuple(plan for plan, _receipt in packs),
        raw_volumes=raw_volumes,
    )
    expected_by_id = {current.artifact_id: current for current in normalized_artifacts}
    volume_rows: list[dict[str, object]] = []

    for plan, pack_receipt in packs:
        _validate_pack_receipt(plan, pack_receipt)
        volume_rows.append(_pack_volume_row(plan, pack_receipt))

    verified_by_id = _verified_raw_artifacts(verified_raw_artifacts)
    raw_by_id: dict[str, list[SealedRawVolume]] = {}
    for current in raw_volumes:
        raw_by_id.setdefault(str(ArtifactId(current.artifact_id)), []).append(current)
    if set(raw_by_id) != set(verified_by_id):
        raise ValueError("every raw artifact must be verified before root publication")
    for artifact_id, verified in verified_by_id.items():
        expected = expected_by_id.get(artifact_id)
        if (
            expected is None
            or verified.bytes != expected.bytes
            or verified.sha256 != expected.sha256
            or verified.ordered_volume_sha256
            != raw_artifact_ordered_volume_commitment(
                artifact=expected, volumes=raw_by_id[artifact_id]
            )
        ):
            raise ValueError(f"raw artifact verification differs: {artifact_id}")

    for raw_receipt in raw_volumes:
        artifact_id = str(ArtifactId(raw_receipt.artifact_id))
        expected = expected_by_id.get(artifact_id)
        if expected is None:
            raise ValueError(f"raw volume references an unknown artifact: {artifact_id}")
        if raw_receipt.artifact_offset < 0 or raw_receipt.plaintext_bytes < 0:
            raise ValueError("raw volume placement is invalid")
        if (
            raw_receipt.artifact_bytes != expected.bytes
            or raw_receipt.artifact_sha256 != expected.sha256
        ):
            raise ValueError(f"raw volume artifact identity mismatch: {artifact_id}")
        if raw_receipt.artifact_offset + raw_receipt.plaintext_bytes > expected.bytes:
            raise ValueError(f"raw volume exceeds its artifact: {artifact_id}")
        _validate_part_receipts(
            raw_receipt.parts,
            plaintext_bytes=raw_receipt.plaintext_bytes,
        )
        volume_rows.append(_raw_volume_row(raw_receipt))

    volume_rows.sort(key=lambda row: _stored_int(row["sequence"], "volume sequence"))
    if [_stored_int(row["sequence"], "volume sequence") for row in volume_rows] != list(
        range(len(volume_rows))
    ):
        raise ValueError("archive volume sequence must be canonical and contiguous")
    if len({str(row["id"]) for row in volume_rows}) != len(volume_rows):
        raise ValueError("archive volume ids must be unique")
    if len({str(row["path"]) for row in volume_rows}) != len(volume_rows):
        raise ValueError("archive volume paths must be unique")

    artifact_set = collection_artifact_set_identity(normalized_artifacts)
    documents = tuple(
        _archive_volume_document(
            archive_generation=archive_generation,
            artifact_set_sha256=artifact_set["sha256"],
            row=row,
        )
        for row in volume_rows
    )
    terminal = build_collection_archive_terminal_document(
        archive_generation=archive_generation,
        artifact_set_sha256=artifact_set["sha256"],
        sequence=len(documents),
    )
    manifest = build_collection_archive_root_manifest(
        archive_generation=archive_generation,
        artifact_set=artifact_set,
        ordered_volume_sha256=ordered_archive_volume_commitment((*documents, terminal)),
        provenance_identity=provenance_identity,
        provenance_objects=provenance_objects,
    )
    return manifest, documents


def build_collection_archive_terminal_document(
    *,
    archive_generation: str,
    artifact_set_sha256: str,
    sequence: int,
) -> CollectionArchiveTerminalDocument:
    return CollectionArchiveTerminalDocument.from_mapping(
        {
            "format": COLLECTION_ARCHIVE_TERMINAL_FORMAT,
            "archive_generation": archive_generation,
            "artifact_set_sha256": artifact_set_sha256,
            "sequence": format_archive_sequence(sequence),
            "kind": "terminal",
        }
    )


def build_collection_archive_volume_document(
    *,
    archive_generation: str,
    artifact_set_sha256: str,
    plan: PackVolumePlan | None,
    receipt: SealedPackVolume | SealedRawVolume,
) -> CollectionArchiveVolumeDocument:
    """Build one independently bounded archive-volume authority document."""

    if _SHA256_RE.fullmatch(artifact_set_sha256) is None:
        raise ValueError("archive artifact-set identity is invalid")
    if isinstance(receipt, SealedPackVolume):
        if plan is None:
            raise ValueError("sealed pack volume requires its canonical plan")
        _validate_pack_receipt(plan, receipt)
        row = _pack_volume_row(plan, receipt)
    else:
        if plan is not None:
            raise ValueError("sealed raw volume does not accept a pack plan")
        _validate_part_receipts(receipt.parts, plaintext_bytes=receipt.plaintext_bytes)
        row = _raw_volume_row(receipt)
    return _archive_volume_document(
        archive_generation=archive_generation,
        artifact_set_sha256=artifact_set_sha256,
        row=row,
    )


def build_collection_archive_root_manifest(
    *,
    archive_generation: str,
    artifact_set: CollectionArtifactSetIdentity,
    ordered_volume_sha256: str,
    provenance_identity: str,
    provenance_objects: Sequence[SealedProvenanceObject] = (),
) -> bytes:
    """Build the small immutable root after every referenced object is durable."""

    if _SHA256_RE.fullmatch(archive_generation) is None:
        raise ValueError("archive generation is invalid")
    if (
        artifact_set["count"] < 1
        or artifact_set["bytes"] < 0
        or _SHA256_RE.fullmatch(artifact_set["sha256"]) is None
    ):
        raise ValueError("archive artifact-set identity is invalid")
    if _SHA256_RE.fullmatch(ordered_volume_sha256) is None:
        raise ValueError("archive ordered volume commitment is invalid")
    payload: dict[str, object] = {
        "format": COLLECTION_ARCHIVE_MANIFEST_FORMAT,
        "archive_generation": archive_generation,
        "storage_profile": {
            "encryption": ARCHIVE_ENCRYPTION_FORMAT,
            "pack_index": PACK_INDEX_FORMAT,
            "part_digest": "sha256",
            "selective_read": SELECTIVE_READ_FORMAT,
        },
        "artifact_set": {
            "count": format_scalar("nonnegative", artifact_set["count"]),
            "bytes": format_scalar("nonnegative", artifact_set["bytes"]),
            "sha256": artifact_set["sha256"],
        },
        "volume_sequence": {
            "sha256": ordered_volume_sha256,
        },
    }
    if _SHA256_RE.fullmatch(provenance_identity) is None:
        raise ValueError("archive provenance identity is invalid")
    roots = [item for item in provenance_objects if item.kind == "provenance-root"]
    if len(provenance_objects) != 1 or len(roots) != 1:
        raise ValueError("archive provenance requires exactly one small root")
    payload["provenance"] = {
        "identity": provenance_identity,
        "root": _provenance_object_row(roots[0]),
    }
    return CollectionArchiveManifest.from_mapping(payload).to_json_bytes()


def _archive_volume_document(
    *,
    archive_generation: str,
    artifact_set_sha256: str,
    row: Mapping[str, object],
) -> CollectionArchiveVolumeDocument:
    volume = dict(row)
    volume["sequence"] = format_archive_sequence(
        _stored_int(volume["sequence"], "archive volume sequence")
    )
    return CollectionArchiveVolumeDocument.from_mapping(
        {
            "format": COLLECTION_ARCHIVE_VOLUME_FORMAT,
            "archive_generation": archive_generation,
            "artifact_set_sha256": artifact_set_sha256,
            "volume": volume,
        }
    )


def build_collection_archive_manifest(
    *,
    archive_generation: str,
    artifacts: Sequence[ArchiveArtifact],
    packs: Sequence[tuple[PackVolumePlan, SealedPackVolume]],
    raw_volumes: Sequence[SealedRawVolume] = (),
    verified_raw_artifacts: Sequence[VerifiedRawArtifact] = (),
    provenance_identity: str,
    provenance_objects: Sequence[SealedProvenanceObject] = (),
) -> bytes:
    manifest, _documents = build_collection_archive_authority(
        archive_generation=archive_generation,
        artifacts=artifacts,
        packs=packs,
        raw_volumes=raw_volumes,
        verified_raw_artifacts=verified_raw_artifacts,
        provenance_identity=provenance_identity,
        provenance_objects=provenance_objects,
    )
    return manifest


def _provenance_object_row(item: SealedProvenanceObject) -> dict[str, object]:
    return {
        "id": item.object_id,
        "kind": item.kind,
        "path": validate_canonical_relpath(item.relative_path),
        "plaintext_bytes": format_scalar("nonnegative", item.plaintext_bytes),
        "sha256": item.plaintext_sha256,
        "stored_bytes": format_scalar("nonnegative", item.stored_bytes),
        "stored_sha256": item.stored_sha256,
    }


def collection_artifact_set_identity(
    artifacts: Sequence[ArchiveArtifact],
) -> CollectionArtifactSetIdentity:
    normalized = _normalized_artifacts(artifacts)
    identity = artifact_set_identity(
        ArtifactMemberIdentityDocument.model_validate(
            {
                "artifact_id": current.artifact_id,
                "bytes": format_scalar("nonnegative", current.bytes),
                "sha256": current.sha256,
            }
        )
        for current in normalized
    )
    return {
        "count": len(normalized),
        "bytes": sum(current.bytes for current in normalized),
        "sha256": identity,
    }


def _pack_volume_row(plan: PackVolumePlan, receipt: SealedPackVolume) -> dict[str, object]:
    expected_path = f"volumes/{receipt.volume_id}.tar.age"
    if validate_canonical_relpath(receipt.relative_path) != expected_path:
        raise ValueError("sealed pack receipt path is not canonical")
    return {
        "id": receipt.volume_id,
        "sequence": receipt.sequence,
        "kind": "pack",
        "path": expected_path,
        "artifacts": receipt.artifacts,
        "source_bytes": format_scalar("nonnegative", receipt.source_bytes),
        "plaintext_bytes": format_scalar("nonnegative", receipt.plaintext_bytes),
        "age_state": _age_state_row(
            receipt.age_state_json, plaintext_bytes=receipt.plaintext_bytes
        ),
        "index_sha256": receipt.index_sha256,
        "plan_sha256": receipt.plan_sha256,
        "parts": [_part_row(current) for current in receipt.parts],
    }


def _raw_volume_row(receipt: SealedRawVolume) -> dict[str, object]:
    artifact_id = str(ArtifactId(receipt.artifact_id))
    expected_path = f"volumes/{receipt.volume_id}.bin.age"
    if validate_canonical_relpath(receipt.relative_path) != expected_path:
        raise ValueError("sealed raw receipt path is not canonical")
    if receipt.sequence >= 1 << 256 or receipt.volume_id != f"segment-{receipt.sequence:064x}":
        raise ValueError("sealed raw receipt identity is not canonical")
    return {
        "id": receipt.volume_id,
        "sequence": receipt.sequence,
        "kind": "segment",
        "path": expected_path,
        "plaintext_bytes": format_scalar("nonnegative", receipt.plaintext_bytes),
        "age_state": _age_state_row(
            receipt.age_state_json, plaintext_bytes=receipt.plaintext_bytes
        ),
        "artifact": {
            "artifact_id": artifact_id,
            "offset": format_scalar("nonnegative", receipt.artifact_offset),
            "bytes": format_scalar("nonnegative", receipt.plaintext_bytes),
            "artifact_bytes": format_scalar("nonnegative", receipt.artifact_bytes),
            "sha256": receipt.artifact_sha256,
        },
        "parts": [_part_row(current) for current in receipt.parts],
    }


def _age_state_row(age_state_json: str, *, plaintext_bytes: int) -> dict[str, object]:
    try:
        state = UploadState.from_json_bytes(age_state_json)
    except (TypeError, ValueError) as exc:
        raise ValueError("collection archive volume age state is invalid") from exc
    if state.plaintext_size != plaintext_bytes:
        raise ValueError("collection archive volume age state size mismatch")
    normalized = json.loads(state.to_json_bytes())
    if not isinstance(normalized, dict):  # pragma: no cover - UploadState owns this shape
        raise ValueError("collection archive volume age state is invalid")
    return dict(normalized)


def _stored_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} must be a non-negative integer")
    return value


def _part_row(current: StoredArchivePart) -> dict[str, object]:
    return {
        "number": current.number,
        "plaintext_start": format_scalar("nonnegative", current.plaintext_start),
        "plaintext_bytes": format_scalar("nonnegative", current.plaintext_bytes),
        "plaintext_sha256": current.plaintext_sha256,
        "stored_bytes": format_scalar("nonnegative", current.stored_bytes),
        "stored_sha256": current.stored_sha256,
    }


def _validate_pack_receipt(plan: PackVolumePlan, receipt: SealedPackVolume) -> None:
    if (
        receipt.volume_id != plan.volume_id
        or receipt.sequence != plan.sequence
        or receipt.sequence >= 1 << 256
        or receipt.volume_id != f"pack-{receipt.sequence:064x}"
    ):
        raise ValueError("sealed pack receipt does not match its plan")
    if receipt.artifacts != len(plan.members):
        raise ValueError("sealed pack receipt file count mismatch")
    if receipt.source_bytes != sum(current.bytes for current in plan.members):
        raise ValueError("sealed pack receipt source byte count mismatch")
    if receipt.plaintext_bytes != plan.plaintext_bytes:
        raise ValueError("sealed pack receipt plaintext byte count mismatch")
    if receipt.index_sha256 != plan.index_sha256 or receipt.plan_sha256 != plan.plan_sha256:
        raise ValueError("sealed pack receipt identity mismatch")
    _validate_part_receipts(receipt.parts, plaintext_bytes=plan.plaintext_bytes)


def _validate_part_receipts(
    parts: Sequence[StoredArchivePart],
    *,
    plaintext_bytes: int,
) -> None:
    if not parts:
        raise ValueError("sealed archive volume requires at least one part")
    expected_plaintext_start = 0
    for index, current in enumerate(parts, start=1):
        if current.number != index:
            raise ValueError("sealed archive volume part order is invalid")
        if current.plaintext_start != expected_plaintext_start or current.plaintext_bytes < 0:
            raise ValueError("sealed archive volume plaintext ranges are not contiguous")
        if current.stored_bytes <= 0:
            raise ValueError("sealed archive volume stored part must not be empty")
        if _SHA256_RE.fullmatch(current.plaintext_sha256) is None:
            raise ValueError("sealed archive volume plaintext part sha256 is invalid")
        if _SHA256_RE.fullmatch(current.stored_sha256) is None:
            raise ValueError("sealed archive volume stored part sha256 is invalid")
        expected_plaintext_start += current.plaintext_bytes
    if expected_plaintext_start != plaintext_bytes:
        raise ValueError("sealed archive volume parts do not cover its plaintext")


def _validate_artifact_coverage(
    artifacts: Sequence[ArchiveArtifact],
    coverage: Mapping[str, list[tuple[int, int, str]]],
) -> None:
    for current in artifacts:
        ranges = sorted(coverage[current.artifact_id])
        if not ranges:
            raise ValueError(f"collection artifact has no volume placement: {current.artifact_id}")
        expected_offset = 0
        for offset, byte_count, _volume_id in ranges:
            if offset != expected_offset or byte_count < 0:
                raise ValueError(
                    f"collection artifact placements are not contiguous: {current.artifact_id}"
                )
            expected_offset += byte_count
        if expected_offset != current.bytes:
            raise ValueError(
                f"collection artifact placements do not cover it: {current.artifact_id}"
            )


def _verified_raw_artifacts(
    artifacts: Sequence[VerifiedRawArtifact],
) -> dict[str, VerifiedRawArtifact]:
    out: dict[str, VerifiedRawArtifact] = {}
    for current in artifacts:
        artifact_id = str(ArtifactId(current.artifact_id))
        if artifact_id in out:
            raise ValueError("duplicate raw artifact verification")
        if (
            current.bytes < 0
            or _SHA256_RE.fullmatch(current.sha256) is None
            or _SHA256_RE.fullmatch(current.ordered_volume_sha256) is None
            or not current.verified_at
        ):
            raise ValueError("raw artifact verification identity is invalid")
        out[artifact_id] = VerifiedRawArtifact(
            artifact_id=artifact_id,
            bytes=current.bytes,
            sha256=current.sha256,
            ordered_volume_sha256=current.ordered_volume_sha256,
            verified_at=current.verified_at,
        )
    return out


def _normalized_artifacts(artifacts: Sequence[ArchiveArtifact]) -> tuple[ArchiveArtifact, ...]:
    out: list[ArchiveArtifact] = []
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
            raise ValueError(f"collection archive artifact identity is invalid: {artifact_id}")
        seen.add(artifact_id)
        out.append(
            ArchiveArtifact(artifact_id=artifact_id, bytes=current.bytes, sha256=current.sha256)
        )
    if not out:
        raise ValueError("collection archive requires at least one artifact")
    return tuple(sorted(out, key=lambda current: current.artifact_id))
