"""Backward interpretation probes, not migration ownership or live-provider proof."""
from __future__ import annotations

import base64
import hashlib
import json
import lzma
import os
from datetime import datetime
from io import BytesIO
from pathlib import Path

import pytest
from a_riverhog_filesystem_store import FilesystemStorageAdapter, FilesystemStorageAdapterConfig
from a_riverhog_filesystem_store.incarnation import read_storage_incarnation
from a_riverhog_filesystem_store.materialize import (
    MaterializationError,
    MaterializationSelection,
    materialize_committed_objects,
)
from a_riverhog_s3_store_lib.adapter import S3StorageAdapter, S3StorageAdapterConfig
from a_riverhog_s3_store_lib.incarnation import marker_document, marker_key
from riverhog_storage_adapter_protocol import ObjectHeadRequest, ObjectLocator, ObjectReadRequest

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"


@pytest.fixture
def backing(tmp_path):
    envelope = json.loads((FIXTURES / "filesystem-backing.json").read_bytes())
    raw = lzma.decompress(base64.b64decode(envelope["xz_base64"], validate=True))
    assert hashlib.sha256(raw).hexdigest() == envelope["decoded_sha256"]
    captured = json.loads(raw)
    root = tmp_path / "backing"
    root.mkdir(mode=0o700)
    for relative in captured["directories"]:
        (root / relative).mkdir(mode=0o700, parents=True, exist_ok=True)
    for relative, encoded in captured["files"].items():
        target = root / relative
        target.write_bytes(base64.b64decode(encoded))
        target.chmod(0o600)
    return root, captured


def _snapshot(root):
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in root.rglob("*") if path.is_file()
    }


def test_materializer_reads_historical_committed_bytes_without_adapter_startup(backing, tmp_path):
    root, captured = backing
    before = _snapshot(root)
    assert read_storage_incarnation(root) == captured["incarnation_id"]
    result = materialize_committed_objects(
        source=root, destination=tmp_path / "logical",
        selection=MaterializationSelection(all_objects=True),
    )
    assert result.selected_objects == len(captured["logical"])
    for path, encoded in captured["logical"].items():
        assert (tmp_path / "logical" / path).read_bytes() == base64.b64decode(encoded)
    assert _snapshot(root) == before  # Exact source-file contents and membership are unchanged.


def test_completed_segment_ledger_is_required_preservation_state(backing, tmp_path):
    root, _ = backing
    ledgers = list(root.rglob("segments.sqlite3"))
    assert len(ledgers) == 1
    ledgers[0].unlink()
    with pytest.raises(MaterializationError):
        materialize_committed_objects(
            source=root, destination=tmp_path / "incomplete",
            selection=MaterializationSelection(all_objects=True),
        )


@pytest.mark.skipif(os.name != "posix", reason="supplied filesystem adapter is Linux-specific")
def test_upgraded_adapter_can_read_captured_small_and_segmented_objects(backing):
    root, captured = backing
    config = FilesystemStorageAdapterConfig(
        root=root, segment_bytes=65536, read_chunk_bytes=65536, minimum_free_bytes=0,
    )
    with FilesystemStorageAdapter(config) as adapter:
        assert adapter.descriptor().storage_incarnation_id == captured["incarnation_id"]
        for path, encoded in captured["logical"].items():
            raw = base64.b64decode(encoded)
            with adapter.read_object(ObjectReadRequest(
                object=ObjectLocator(object_path=path), expected_bytes=len(raw),
            )) as stream:
                assert b"".join(stream.content) == raw


def test_s3_mapping_incarnation_and_persisted_assertions_read_the_fixed_envelope():
    data = json.loads((FIXTURES / "s3-backing.json").read_bytes())
    assert marker_key(data["root_prefix"]) == data["marker_key"]
    assert marker_document(data["incarnation_id"]) == data["marker_utf8"].encode()
    assert not data["marker_key"].startswith(data["root_prefix"] + "/")

    class ReadOnlyProvider:
        def get_object(self, *, Bucket, Key):
            assert Bucket == "reference" and Key == data["marker_key"]
            return {"Body": BytesIO(data["marker_utf8"].encode())}

        def head_object(self, *, Bucket, Key):
            assert Bucket == "reference" and Key == data["provider_key"]
            return {
                **data["provider_head"],
                "LastModified": datetime.fromisoformat(data["provider_head"]["LastModified"]),
            }

    adapter = S3StorageAdapter(ReadOnlyProvider(), S3StorageAdapterConfig(
        implementation_id="reference/v1", implementation_version="0.1.0",
        bucket="reference", root_prefix=data["root_prefix"],
    ))
    head = adapter.head_object(ObjectHeadRequest(
        object=ObjectLocator(object_path=data["logical_path"]),
        expected_placement_policy="archive_default",
    ))
    assert head is not None
    assert head.observed_identity_assertions == data["expected_assertions"]
    assert head.stored_sha256 == data["payload_sha256"]
    assert head.revision == data["provider_head"]["VersionId"]
