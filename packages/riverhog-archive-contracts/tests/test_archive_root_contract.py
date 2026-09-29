from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from riverhog_archive_contracts import (
    ArchiveManifestError,
    CollectionArchiveManifest,
    CollectionArchiveTerminalDocument,
    CollectionArchiveVolumeDocument,
    CollectionArtifactSetIdentity,
    PackArchiveVolume,
    StoredPartIdentity,
    format_archive_sequence,
    ordered_archive_volume_commitment,
)

ZERO = "0" * 64
ONE = "1" * 64


def _volume_mapping() -> dict[str, object]:
    return {
        "format": "collection-archive-volume/v1",
        "archive_generation": ONE,
        "artifact_set_sha256": ZERO,
        "volume": {
            "id": f"pack-{format_archive_sequence(0)}",
            "sequence": format_archive_sequence(0),
            "kind": "pack",
            "path": f"volumes/pack-{format_archive_sequence(0)}.tar.age",
            "artifacts": 1,
            "source_bytes": "0",
            "plaintext_bytes": "0",
            "age_state": {
                "format": "age-v1-scrypt-resumable",
                "header_b64": "YQ",
                "payload_nonce_b64": "MDAwMDAwMDAwMDAwMDAwMA",
                "plaintext_size": "0",
            },
            "index_sha256": ZERO,
            "plan_sha256": ONE,
            "parts": [
                {
                    "number": 1,
                    "plaintext_start": "0",
                    "plaintext_bytes": "0",
                    "plaintext_sha256": ZERO,
                    "stored_bytes": "1",
                    "stored_sha256": ONE,
                }
            ],
        },
    }


def _manifest_mapping() -> dict[str, object]:
    volume = CollectionArchiveVolumeDocument.from_mapping(_volume_mapping())
    terminal = CollectionArchiveTerminalDocument(
        archive_generation=ONE,
        artifact_set_sha256=ZERO,
        sequence=1,
    )
    return {
        "format": "collection-archive-manifest/v1",
        "archive_generation": ONE,
        "storage_profile": {
            "encryption": "age-v1-scrypt",
            "pack_index": "riverhog-pack-index/v1",
            "part_digest": "sha256",
            "selective_read": "age-chunk-range/v1",
        },
        "artifact_set": {"count": "1", "bytes": "0", "sha256": ZERO},
        "volume_sequence": {
            "sha256": ordered_archive_volume_commitment((volume, terminal)),
        },
        "provenance": {
            "identity": ZERO,
            "root": {
                "id": "provenance-root",
                "kind": "provenance-root",
                "path": "provenance/root.json.age",
                "plaintext_bytes": "1",
                "sha256": ZERO,
                "stored_bytes": "1",
                "stored_sha256": ONE,
            },
        },
    }


def test_archive_root_has_one_canonical_public_model() -> None:
    source = _manifest_mapping()
    manifest = CollectionArchiveManifest.from_mapping(source)
    reparsed = CollectionArchiveManifest.from_json_bytes(manifest.to_json_bytes())

    assert reparsed == manifest
    assert reparsed.to_mapping() == source
    assert reparsed.ordered_volume_sha256 == source["volume_sequence"]["sha256"]


def test_archive_root_keeps_large_exact_artifact_totals() -> None:
    source = _manifest_mapping()
    source["artifact_set"] = {
        "count": str(2**63 - 1),
        "bytes": str(2**100 + 1),
        "sha256": ZERO,
    }
    manifest = CollectionArchiveManifest.from_mapping(source)
    encoded = manifest.to_json_bytes()

    assert CollectionArchiveManifest.from_json_bytes(encoded) == manifest
    assert json.loads(encoded)["artifact_set"] == source["artifact_set"]


def test_checked_schema_names_the_same_archive_root_contract() -> None:
    path = Path(__file__).parents[1] / "schemas" / "collection-archive-manifest-v1.schema.json"
    schema = json.loads(path.read_text())

    assert schema["properties"]["format"]["const"] == "collection-archive-manifest/v1"
    assert schema["additionalProperties"] is False
    assert "CollectionArchiveManifest" in schema["$comment"]
    Draft202012Validator(schema).validate(_manifest_mapping())


def test_checked_volume_schema_names_the_same_bounded_volume_contract() -> None:
    path = Path(__file__).parents[1] / "schemas" / "collection-archive-volume-v1.schema.json"
    schema = json.loads(path.read_text())

    assert schema["properties"]["format"]["const"] == "collection-archive-volume/v1"
    assert schema["additionalProperties"] is False
    assert "CollectionArchiveVolumeDocument" in schema["$comment"]
    Draft202012Validator(schema).validate(_volume_mapping())


def test_public_archive_root_constructors_share_the_parser_validity_domain() -> None:
    manifest = CollectionArchiveManifest.from_mapping(_manifest_mapping())
    volume = CollectionArchiveVolumeDocument.from_mapping(_volume_mapping()).volume
    assert isinstance(volume, PackArchiveVolume)

    with pytest.raises(ArchiveManifestError, match="artifact count"):
        CollectionArtifactSetIdentity(count=0, bytes=0, sha256=ZERO)
    with pytest.raises(ArchiveManifestError, match="part number"):
        StoredPartIdentity(
            number=0,
            plaintext_start=0,
            plaintext_bytes=0,
            plaintext_sha256=ZERO,
            stored_bytes=1,
            stored_sha256=ONE,
        )
    with pytest.raises(ArchiveManifestError, match="path is not canonical"):
        replace(volume, path="volumes/wrong.tar.age")
    with pytest.raises(ArchiveManifestError, match="part order"):
        replace(volume, parts=(replace(volume.parts[0], number=2),))

    assert CollectionArchiveManifest.from_json_bytes(manifest.to_json_bytes()) == manifest


def test_archive_root_schema_projects_expressible_semantic_constraints() -> None:
    path = Path(__file__).parents[1] / "schemas" / "collection-archive-manifest-v1.schema.json"
    validator = Draft202012Validator(json.loads(path.read_text()))
    invalid = _volume_mapping()
    volume = CollectionArchiveVolumeDocument.from_mapping(invalid)
    invalid_root = _manifest_mapping()
    volume_sequence = invalid_root["volume_sequence"]
    assert isinstance(volume_sequence, dict)
    volume_sequence["sha256"] = "invalid"

    assert volume.volume.sequence == 0
    assert list(validator.iter_errors(invalid_root))


def test_segment_placement_selects_opaque_member_not_workspace_name() -> None:
    row = _volume_mapping()
    row["volume"] = {
        "id": f"segment-{format_archive_sequence(0)}",
        "sequence": format_archive_sequence(0),
        "kind": "segment",
        "path": f"volumes/segment-{format_archive_sequence(0)}.bin.age",
        "plaintext_bytes": "1",
        "age_state": {
            "format": "age-v1-scrypt-resumable",
            "header_b64": "YQ",
            "payload_nonce_b64": "MDAwMDAwMDAwMDAwMDAwMA",
            "plaintext_size": "1",
        },
        "artifact": {
            "artifact_id": "a" * 64,
            "offset": "0",
            "bytes": "1",
            "artifact_bytes": "1",
            "sha256": "b" * 64,
        },
        "parts": [
            {
                "number": 1,
                "plaintext_start": "0",
                "plaintext_bytes": "1",
                "plaintext_sha256": ZERO,
                "stored_bytes": "1",
                "stored_sha256": ONE,
            }
        ],
    }
    document = CollectionArchiveVolumeDocument.from_mapping(row)
    assert document.volume.source_artifact.artifact_id == "a" * 64
    schema = json.loads(
        (
            Path(__file__).parents[1] / "schemas" / "collection-archive-volume-v1.schema.json"
        ).read_text()
    )
    Draft202012Validator(schema).validate(row)
    legacy = json.loads(json.dumps(row))
    legacy["volume"]["artifact"]["path"] = "camera/source.mov"
    with pytest.raises(ArchiveManifestError):
        CollectionArchiveVolumeDocument.from_mapping(legacy)
