"""Independent, ID-keyed extraction of encrypted archive payload volumes."""

from __future__ import annotations

import hashlib
import re
import shutil
import sqlite3
import tarfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from riverhog_archive_contracts import (
    PACK_INDEX_FORMAT,
    ArchiveArtifactIdentity,
    CollectionArchiveManifest,
    CollectionArchiveTerminalDocument,
    CollectionArchiveVolumeDocument,
    PackArchiveVolume,
    SegmentArchiveVolume,
    StoredPartIdentity,
    format_archive_sequence,
    update_archive_sequence_commitment,
)
from riverhog_canonical_json import require_canonical_json
from riverhog_protocol import ArtifactMemberIdentityDocument
from riverhog_protocol.manifest import ArtifactSetIdentityBuilder

from ._archive_io import EncryptedArchive, archive_file, sha256_file

_PACK_INDEX_PATH = ".riverhog/pack-index.json"
_PADDING = re.compile(r"\.riverhog/padding/pack-[0-9a-f]{64}-[0-9]{6}\Z")
_ARTIFACT_TAR_PATH = re.compile(r"artifacts/([0-9a-f]{64})\Z")


@dataclass(frozen=True, slots=True)
class PayloadSummary:
    artifacts: int
    bytes: int
    volumes: int
    artifact_set_sha256: str


def _verify_parts(path: Path, parts: tuple[StoredPartIdentity, ...], *, stored: bool) -> None:
    expected_total = sum(part.stored_bytes if stored else part.plaintext_bytes for part in parts)
    if path.stat().st_size != expected_total:
        raise ValueError("archive volume byte count differs from its part sequence")
    with path.open("rb") as source:
        for part in parts:
            remaining = part.stored_bytes if stored else part.plaintext_bytes
            expected = part.stored_sha256 if stored else part.plaintext_sha256
            digest = hashlib.sha256()
            while remaining:
                chunk = source.read(min(8 * 1024 * 1024, remaining))
                if not chunk:
                    raise ValueError("archive volume part is incomplete")
                remaining -= len(chunk)
                digest.update(chunk)
            if digest.hexdigest() != expected:
                raise ValueError("archive volume part digest changed")
        if source.read(1):
            raise ValueError("archive volume has trailing bytes")


def _row(identity: ArchiveArtifactIdentity) -> ArtifactMemberIdentityDocument:
    return ArtifactMemberIdentityDocument.model_validate(
        {
            "artifact_id": identity.artifact_id,
            "bytes": str(identity.bytes),
            "sha256": identity.sha256,
        }
    )


def _insert_member(
    state: sqlite3.Connection,
    identity: ArchiveArtifactIdentity,
    *,
    kind: str,
) -> int:
    prior = state.execute(
        "SELECT bytes, sha256, received, kind FROM artifacts WHERE artifact_id = ?",
        (identity.artifact_id,),
    ).fetchone()
    if prior is None:
        state.execute(
            "INSERT INTO artifacts VALUES (?, ?, ?, 0, ?)",
            (identity.artifact_id, identity.bytes, identity.sha256, kind),
        )
        return 0
    if (int(prior[0]), str(prior[1]), str(prior[3])) != (
        identity.bytes,
        identity.sha256,
        kind,
    ):
        raise ValueError("archive volumes disagree about an artifact identity")
    return int(prior[2])


def _copy_member(source: Any, destination: Path, *, identity: ArchiveArtifactIdentity) -> None:
    digest = hashlib.sha256()
    remaining = identity.bytes
    with destination.open("xb") as output:
        while remaining:
            chunk = source.read(min(8 * 1024 * 1024, remaining))
            if not chunk:
                raise ValueError("pack artifact is truncated")
            remaining -= len(chunk)
            digest.update(chunk)
            output.write(chunk)
    if digest.hexdigest() != identity.sha256:
        raise ValueError("pack artifact digest changed")


def _pack_rows(content: bytes, volume: PackArchiveVolume) -> list[dict[str, Any]]:
    if len(content) > 4 * 1024 * 1024:
        raise ValueError("pack index exceeds its bounded contract")
    payload = require_canonical_json(content)
    if (
        not isinstance(payload, dict)
        or set(payload) != {"format", "volume", "artifact_set", "artifacts"}
        or payload["format"] != PACK_INDEX_FORMAT
    ):
        raise ValueError("pack index format is invalid")
    if payload["volume"] != {"id": volume.id, "sequence": volume.sequence}:
        raise ValueError("pack index names another volume")
    rows = payload["artifacts"]
    if not isinstance(rows, list) or len(rows) != volume.artifacts:
        raise ValueError("pack index artifact count differs")
    builder = ArtifactSetIdentityBuilder()
    expected_bytes = 0
    previous_offset = -1
    previous_unit = -1
    for current in rows:
        if not isinstance(current, dict) or set(current) != {
            "artifact_id",
            "bytes",
            "sha256",
            "unit",
            "header_offset",
            "data_offset",
        }:
            raise ValueError("pack index artifact row is invalid")
        if (
            type(current["artifact_id"]) is not str
            or type(current["bytes"]) is not int
            or type(current["sha256"]) is not str
        ):
            raise ValueError("pack index artifact identity is invalid")
        identity = ArchiveArtifactIdentity(
            current["artifact_id"], current["bytes"], current["sha256"]
        )
        if (
            type(current["unit"]) is not int
            or type(current["header_offset"]) is not int
            or type(current["data_offset"]) is not int
            or current["unit"] < previous_unit
            or current["unit"] > previous_unit + 1
            or previous_unit == -1
            and current["unit"] != 0
            or current["header_offset"] <= previous_offset
            or current["data_offset"] <= current["header_offset"]
        ):
            raise ValueError("pack index offsets or units are invalid")
        previous_offset = current["data_offset"]
        previous_unit = current["unit"]
        expected_bytes += identity.bytes
        builder.add(_row(identity))
    if (
        payload["artifact_set"]
        != {
            "count": len(rows),
            "bytes": expected_bytes,
            "sha256": builder.finish(),
        }
        or expected_bytes != volume.source_bytes
    ):
        raise ValueError("pack index artifact set differs")
    return cast(list[dict[str, Any]], rows)


def _extract_pack(
    plaintext: Path, volume: PackArchiveVolume, state: sqlite3.Connection, spool: Path
) -> None:
    with tarfile.open(plaintext, mode="r:") as archive:
        members = archive.getmembers()
        by_name = {member.name: member for member in members}
        if len(by_name) != len(members) or not members or members[-1].name != _PACK_INDEX_PATH:
            raise ValueError("pack members or final index are invalid")
        index = by_name[_PACK_INDEX_PATH]
        if not index.isfile() or index.size > 4 * 1024 * 1024:
            raise ValueError("pack index is invalid")
        index_source = archive.extractfile(index)
        if index_source is None:
            raise ValueError("pack index cannot be read")
        with index_source:
            index_content = index_source.read(index.size + 1)
        if (
            len(index_content) != index.size
            or hashlib.sha256(index_content).hexdigest() != volume.index_sha256
        ):
            raise ValueError("pack index digest differs")
        rows = _pack_rows(index_content, volume)
        expected = {f"artifacts/{row['artifact_id']}" for row in rows}
        padding = set(by_name) - expected - {_PACK_INDEX_PATH}
        if any(_PADDING.fullmatch(name) is None for name in padding):
            raise ValueError("pack contains an unindexed artifact")
        if set(by_name) != expected | padding | {_PACK_INDEX_PATH}:
            raise ValueError("pack is missing an indexed artifact")
        for name in sorted(padding):
            info = by_name[name]
            if not info.isfile() or info.size >= 64 * 1024:
                raise ValueError("pack padding has an invalid shape")
            source = archive.extractfile(info)
            if source is None:
                raise ValueError("pack padding cannot be read")
            with source:
                while chunk := source.read(8 * 1024):
                    if any(chunk):
                        raise ValueError("pack padding is not zero-filled")
        for row in rows:
            artifact_id = row["artifact_id"]
            name = f"artifacts/{artifact_id}"
            if _ARTIFACT_TAR_PATH.fullmatch(name) is None:
                raise ValueError("pack artifact path is invalid")
            info = by_name[name]
            identity = ArchiveArtifactIdentity(artifact_id, row["bytes"], row["sha256"])
            if (
                not info.isfile()
                or info.size != identity.bytes
                or info.offset != row["header_offset"]
                or info.offset_data != row["data_offset"]
            ):
                raise ValueError("pack artifact placement differs from index")
            if _insert_member(state, identity, kind="pack") != 0:
                raise ValueError("pack repeats an artifact")
            source = archive.extractfile(info)
            if source is None:
                raise ValueError("pack artifact cannot be read")
            with source:
                _copy_member(source, spool / artifact_id, identity=identity)
            state.execute(
                "UPDATE artifacts SET received = ? WHERE artifact_id = ?",
                (identity.bytes, artifact_id),
            )


def _extract_segment(
    plaintext: Path, volume: SegmentArchiveVolume, state: sqlite3.Connection, spool: Path
) -> None:
    identity = volume.source_artifact
    received = _insert_member(state, identity, kind="segment")
    if received != volume.artifact.offset:
        raise ValueError("artifact segments are not contiguous")
    destination = spool / identity.artifact_id
    if received == 0 and destination.exists():
        raise ValueError("artifact segment destination already exists")
    with (
        plaintext.open("rb") as source,
        destination.open("xb" if received == 0 else "ab") as output,
    ):
        shutil.copyfileobj(source, output, length=8 * 1024 * 1024)
    state.execute(
        "UPDATE artifacts SET received = ? WHERE artifact_id = ?",
        (received + volume.plaintext_bytes, identity.artifact_id),
    )


def stage_payloads(
    encrypted: EncryptedArchive,
    *,
    manifest: CollectionArchiveManifest,
    scratch: Path,
) -> tuple[PayloadSummary, sqlite3.Connection]:
    """Verify every volume and spool each artifact exactly once under its opaque ID."""

    spool = scratch / "artifact-spool"
    if spool.exists():
        shutil.rmtree(spool)
    spool.mkdir(mode=0o700)
    state = sqlite3.connect(scratch / "artifacts.sqlite3")
    state.execute("DROP TABLE IF EXISTS artifacts")
    state.execute(
        "CREATE TABLE artifacts (artifact_id TEXT PRIMARY KEY, bytes INTEGER NOT NULL, "
        "sha256 TEXT NOT NULL, received INTEGER NOT NULL, kind TEXT NOT NULL)"
    )
    sequence = 0
    commitment = hashlib.sha256()
    while True:
        token = format_archive_sequence(sequence)
        raw = encrypted.read_bounded(f"metadata/volume-{token}.json.age", 64 * 1024)
        value = require_canonical_json(raw)
        if not isinstance(value, dict):
            raise ValueError("archive volume descriptor is not an object")
        if value.get("format") == "collection-archive-terminal/v1":
            terminal = CollectionArchiveTerminalDocument.from_json_bytes(raw)
            if (
                terminal.sequence != sequence
                or terminal.archive_generation != manifest.archive_generation
                or terminal.artifact_set_sha256 != manifest.artifact_set_sha256
            ):
                raise ValueError("archive terminal differs from root")
            update_archive_sequence_commitment(commitment, terminal)
            break
        document = CollectionArchiveVolumeDocument.from_json_bytes(raw)
        volume = document.volume
        if (
            document.archive_generation != manifest.archive_generation
            or document.artifact_set_sha256 != manifest.artifact_set_sha256
            or volume.sequence != sequence
        ):
            raise ValueError("archive volume differs from root")
        update_archive_sequence_commitment(commitment, document)
        ciphertext = archive_file(encrypted.root, volume.path)
        _verify_parts(ciphertext, volume.parts, stored=True)
        plaintext = encrypted.decrypt_to(volume.path, scratch / f"volume-{token}.plaintext")
        try:
            _verify_parts(plaintext, volume.parts, stored=False)
            if isinstance(volume, PackArchiveVolume):
                _extract_pack(plaintext, volume, state, spool)
            elif isinstance(volume, SegmentArchiveVolume):
                _extract_segment(plaintext, volume, state, spool)
            else:
                raise ValueError("archive volume kind is unsupported")
            state.commit()
        finally:
            plaintext.unlink(missing_ok=True)
        sequence += 1
    if commitment.hexdigest() != manifest.ordered_volume_sha256:
        raise ValueError("archive volume sequence differs from root")
    builder = ArtifactSetIdentityBuilder()
    for artifact_id, byte_count, sha256, received, _kind in state.execute(
        "SELECT artifact_id, bytes, sha256, received, kind FROM artifacts ORDER BY artifact_id"
    ):
        identity = ArchiveArtifactIdentity(str(artifact_id), int(byte_count), str(sha256))
        if received != identity.bytes or sha256_file(spool / identity.artifact_id) != (
            identity.bytes,
            identity.sha256,
        ):
            raise ValueError("recovered artifact differs from its archive identity")
        builder.add(_row(identity))
    set_sha256 = builder.finish()
    if (
        builder.count != manifest.artifacts
        or builder.bytes != manifest.bytes
        or set_sha256 != manifest.artifact_set_sha256
    ):
        raise ValueError("artifact set differs from selected archive root")
    return PayloadSummary(builder.count, builder.bytes, sequence, set_sha256), state


__all__ = ["PayloadSummary", "stage_payloads"]
