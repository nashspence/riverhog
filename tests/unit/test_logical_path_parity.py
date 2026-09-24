from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from pydantic import TypeAdapter, ValidationError
from riverhog_archive_contracts import ArchiveFileIdentity, ArchiveManifestError
from riverhog_client.producer import ProducerFile
from riverhog_protocol.collection_upload_transport import CollectionUploadRawDigestProgressDocument
from riverhog_protocol.paths import CanonicalRelPath, normalize_relpath
from riverhog_storage_adapter_protocol import validate_object_path

_LOGICAL_PATH = TypeAdapter(CanonicalRelPath)
_SHA256 = "0" * 64


@pytest.mark.parametrize(
    "path",
    (
        "file.txt",
        "folder/caf\N{LATIN SMALL LETTER E WITH ACUTE}.txt",
        "folder/ name.txt",
    ),
)
def test_archive_and_protocol_accept_the_same_canonical_logical_paths(path: str) -> None:
    assert _LOGICAL_PATH.validate_python(path, strict=True) == path
    assert ArchiveFileIdentity(path=path, bytes=0, sha256=_SHA256).path == path


@pytest.mark.parametrize(
    "path",
    (
        "cafe\N{COMBINING ACUTE ACCENT}.txt",
        " leading.txt",
        "trailing.txt ",
        "nul\x00file",
        "surrogate\ud800file",
        "folder//file",
        "folder/./file",
        "folder/../file",
        "folder\\file",
        "x" * 4097,
    ),
)
def test_archive_and_protocol_reject_the_same_noncanonical_logical_paths(path: str) -> None:
    with pytest.raises(ValidationError):
        _LOGICAL_PATH.validate_python(path, strict=True)
    with pytest.raises(ArchiveManifestError):
        ArchiveFileIdentity(path=path, bytes=0, sha256=_SHA256)


def test_logical_normalization_and_storage_object_validation_have_distinct_roles() -> None:
    assert normalize_relpath(" folder\\file ") == "folder/file"
    with pytest.raises(ValueError):
        normalize_relpath("nul\x00file")

    decomposed = "cafe\N{COMBINING ACUTE ACCENT}.txt"
    assert validate_object_path(decomposed) == decomposed
    with pytest.raises(ValueError):
        validate_object_path(" folder\\file ")


def test_wire_and_client_boundaries_do_not_rewrite_logical_file_identity(tmp_path) -> None:
    with pytest.raises(ValidationError):
        CollectionUploadRawDigestProgressDocument.model_validate(
            {
                "path": " folder\\file ",
                "accepted_parts": "0",
                "expected_parts": "1",
                "complete": False,
            }
        )

    source = tmp_path / "file"
    source.write_bytes(b"content")
    with pytest.raises(ValueError):
        ProducerFile(source=source, path=" folder\\file ")
    canonical = normalize_relpath(" folder\\file ")
    assert ProducerFile(source=source, path=canonical).path == "folder/file"


def test_protocol_and_archive_schemas_exclude_unrepresentable_logical_paths() -> None:
    archive_schema = json.loads(
        (
            Path(__file__).parents[2]
            / "packages/riverhog-archive-contracts/schemas/collection-archive-volume-v1.schema.json"
        ).read_text(encoding="utf-8")
    )["$defs"]["segment_file"]["properties"]["path"]
    protocol_schema = _LOGICAL_PATH.json_schema()

    for schema in (archive_schema, protocol_schema):
        validator = Draft202012Validator(schema)
        assert validator.is_valid("folder/caf\N{LATIN SMALL LETTER E WITH ACUTE}.txt")
        for value in ("nul\x00file", "surrogate\ud800file", " leading", "x" * 4097):
            assert not validator.is_valid(value)
