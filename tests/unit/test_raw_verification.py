from __future__ import annotations

import hashlib

import pytest
from riverhog_core import raw_verification
from riverhog_core.domain.archive import (
    ArchiveArtifact,
    SealedRawVolume,
    StoredArchivePart,
)

from tests.fixtures.archive import age_state_json


def _sealed_segment(
    *,
    sequence: int,
    artifact_id: str,
    whole: bytes,
    offset: int,
    content: bytes,
) -> SealedRawVolume:
    digest = hashlib.sha256(content).hexdigest()
    volume_id = f"segment-{sequence:064x}"
    return SealedRawVolume(
        volume_id=volume_id,
        sequence=sequence,
        relative_path=f"volumes/{volume_id}.bin.age",
        artifact_id=artifact_id,
        artifact_offset=offset,
        plaintext_bytes=len(content),
        age_state_json=age_state_json(len(content)),
        artifact_bytes=len(whole),
        artifact_sha256=hashlib.sha256(whole).hexdigest(),
        parts=(
            StoredArchivePart(
                number=1,
                plaintext_start=0,
                plaintext_bytes=len(content),
                plaintext_sha256=digest,
                stored_bytes=len(content),
                stored_sha256=digest,
            ),
        ),
        revision=f"v-{sequence}",
        completed_at="2026-08-03T00:00:00Z",
    )


def test_raw_artifact_is_reassembled_and_verified_before_root_publication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        raw_verification,
        "iter_decrypt_age_scrypt",
        lambda chunks, _passphrase: chunks,
    )
    whole = b"abcdefghij"
    artifact_id = "1" * 64
    first = _sealed_segment(sequence=0, artifact_id=artifact_id, whole=whole, offset=0, content=whole[:6])
    second = _sealed_segment(sequence=1, artifact_id=artifact_id, whole=whole, offset=6, content=whole[6:])
    stored = {
        first.relative_path: whole[:6],
        second.relative_path: whole[6:],
    }

    verified = raw_verification.verify_raw_artifact(
        artifact=ArchiveArtifact(
            artifact_id=artifact_id,
            bytes=len(whole),
            sha256=hashlib.sha256(whole).hexdigest(),
        ),
        volumes=(first, second),
        passphrase="archive passphrase",
        read_ciphertext_chunks=lambda path: (stored[path],),
        verified_at="2026-08-03T00:00:01Z",
    )

    assert verified.artifact_id == artifact_id
    assert verified.sha256 == hashlib.sha256(whole).hexdigest()
    assert verified.ordered_volume_sha256 == raw_verification.raw_artifact_ordered_volume_commitment(
        artifact=ArchiveArtifact(
            artifact_id=artifact_id,
            bytes=len(whole),
            sha256=hashlib.sha256(whole).hexdigest(),
        ),
        volumes=(first, second),
    )
    payload = raw_verification.raw_artifact_verification_payload(verified)
    assert payload["format"] == "raw-artifact-verification/v1"


def test_raw_verification_rejects_stored_part_corruption(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        raw_verification,
        "iter_decrypt_age_scrypt",
        lambda chunks, _passphrase: chunks,
    )
    whole = b"abcdefghij"
    volume = _sealed_segment(sequence=0, artifact_id="1" * 64, whole=whole, offset=0, content=whole)

    with pytest.raises(ValueError, match="raw (stored|plaintext) part sha256 mismatch"):
        raw_verification.verify_raw_artifact(
            artifact=ArchiveArtifact(
                artifact_id="1" * 64,
                bytes=len(whole),
                sha256=hashlib.sha256(whole).hexdigest(),
            ),
            volumes=(volume,),
            passphrase="archive passphrase",
            read_ciphertext_chunks=lambda _path: (b"abcdefghik",),
            verified_at="2026-08-03T00:00:01Z",
        )


def test_raw_volume_set_digest_changes_with_immutable_object_identity() -> None:
    whole = b"abcdefghij"
    artifact = ArchiveArtifact(
        artifact_id="1" * 64,
        bytes=len(whole),
        sha256=hashlib.sha256(whole).hexdigest(),
    )
    first = _sealed_segment(sequence=0, artifact_id=artifact.artifact_id, whole=whole, offset=0, content=whole)
    changed_part = StoredArchivePart(
        number=1,
        plaintext_start=0,
        plaintext_bytes=first.parts[0].plaintext_bytes,
        plaintext_sha256=first.parts[0].plaintext_sha256,
        stored_bytes=first.parts[0].stored_bytes,
        stored_sha256=hashlib.sha256(b"different ciphertext").hexdigest(),
    )
    changed = SealedRawVolume(
        volume_id=first.volume_id,
        sequence=first.sequence,
        relative_path=first.relative_path,
        artifact_id=first.artifact_id,
        artifact_offset=first.artifact_offset,
        plaintext_bytes=first.plaintext_bytes,
        age_state_json=age_state_json(first.plaintext_bytes),
        artifact_bytes=first.artifact_bytes,
        artifact_sha256=first.artifact_sha256,
        parts=(changed_part,),
        revision="new-version",
        completed_at="2026-08-03T00:00:02Z",
    )

    assert raw_verification.raw_artifact_ordered_volume_commitment(
        artifact=artifact, volumes=(first,)
    ) != raw_verification.raw_artifact_ordered_volume_commitment(artifact=artifact, volumes=(changed,))


def test_part_manifest_verification_avoids_remote_read_after_write() -> None:
    from riverhog_client.source_hashing import hash_raw_source_chunks

    whole = b"abcdefghij"
    artifact = ArchiveArtifact(
        artifact_id="1" * 64,
        bytes=len(whole),
        sha256=hashlib.sha256(whole).hexdigest(),
    )
    volume = _sealed_segment(
        sequence=0,
        artifact_id=artifact.artifact_id,
        whole=whole,
        offset=0,
        content=whole,
    )
    source_hash = hash_raw_source_chunks(
        artifact_id=artifact.artifact_id,
        chunks=(whole,),
        expected_bytes=len(whole),
        part_plaintext_bytes=65536,
    )

    try:
        verified = raw_verification.verify_raw_artifact_from_digest_summary(
            artifact=artifact,
            volumes=(volume,),
            summary=source_hash.summary,
            verified_at="2026-08-03T00:00:01Z",
        )
    finally:
        source_hash.close()

    assert verified.sha256 == artifact.sha256
    assert verified.ordered_volume_sha256
