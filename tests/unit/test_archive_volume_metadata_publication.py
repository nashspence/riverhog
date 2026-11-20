from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest
from riverhog_age import decrypt_age_scrypt
from riverhog_core.archive_formats import VOLUME_METADATA_STORAGE_FORMAT
from riverhog_core.archive_manifest import (
    build_collection_archive_terminal_document,
    build_collection_archive_volume_document,
)
from riverhog_core.archive_root import (
    VOLUME_METADATA_CONTENT_TYPE,
    ArchiveRootPublisher,
    ArchiveVolumeMetadataPublication,
)
from riverhog_core.archive_volume_receipts import parse_sealed_pack, parse_sealed_raw
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord as Volume
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.pack_volume import parse_pack_volume_plan

from tests.unit.test_archive_root import MemoryImmutableStore
from tests.unit.test_collection_upload_preparation import _case
from tests.unit.test_finalization_claim_readiness import _seal


def _terminal():
    document = build_collection_archive_terminal_document(
        archive_generation="a" * 64, artifact_set_sha256="b" * 64, sequence=1
    )
    return ArchiveVolumeMetadataPublication.from_document(
        archive_storage_prefix="/archive/fixture/", document=document
    ), document


@pytest.mark.parametrize("kind", ["pack", "segment"])
def test_actual_volume_document_preserves_publisher_bytes_identity_and_receipt(tmp_path, kind):
    with _case(tmp_path, kind, artifact_count=2) as case:
        _seal(case)
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            volume = session.get(Volume, (case.collection_id, case.object_id))
            if kind == "pack":
                plan = parse_pack_volume_plan(volume.plan_json)
                receipt = parse_sealed_pack(volume.sealed_receipt_json)
            else:
                plan = None
                receipt = parse_sealed_raw(volume.sealed_receipt_json)
            prefix = upload.archive_storage_prefix
            document = build_collection_archive_volume_document(
                archive_generation=upload.archive_generation,
                artifact_set_sha256="b" * 64,
                plan=plan,
                receipt=receipt,
            )
        publication = ArchiveVolumeMetadataPublication.from_document(
            archive_storage_prefix=prefix, document=document
        )
        store = MemoryImmutableStore()
        publisher = ArchiveRootPublisher(object_store=store, passphrase="fixture", scrypt_log_n=1)
        first = publisher.publish_volume_metadata(archive_storage_prefix=prefix, document=document)
        stored = store.objects[first.object_path]
        assert decrypt_age_scrypt(stored.content, "fixture") == document.to_json_bytes()
        assert stored.identity == publication.required_identity_assertions
        assert publication.sealed_receipt(stored.receipt) == first
        assert (
            publisher.publish_volume_metadata(archive_storage_prefix=prefix, document=document)
            == first
        )


def test_terminal_document_preserves_existing_format_and_receipt():
    publication, document = _terminal()
    store = MemoryImmutableStore()
    receipt = ArchiveRootPublisher(
        object_store=store, passphrase="fixture", scrypt_log_n=1
    ).publish_terminal_metadata(archive_storage_prefix="/archive/fixture/", document=document)
    stored = store.objects[receipt.object_path]
    assert publication.archive_storage_prefix == "archive/fixture"
    assert publication.relative_path == "metadata/volume-" + "0" * 63 + "1.json.age"
    assert decrypt_age_scrypt(stored.content, "fixture") == publication.plaintext
    assert publication.plaintext == document.to_json_bytes()
    assert stored.identity == {
        "riverhog-format": VOLUME_METADATA_STORAGE_FORMAT,
        "riverhog-plaintext-bytes": str(len(document.to_json_bytes())),
        "riverhog-plaintext-sha256": hashlib.sha256(document.to_json_bytes()).hexdigest(),
        "riverhog-volume-sequence": "0" * 63 + "1",
    }
    assert publication.sealed_receipt(stored.receipt) == receipt


def test_request_identity_keeps_original_ciphertext_independent():
    publication, _ = _terminal()
    first = publication.request(b"first opaque ciphertext")
    second = publication.request(b"different original opaque ciphertext")
    assert first.stored_sha256 != second.stored_sha256
    assert first.content_type == VOLUME_METADATA_CONTENT_TYPE
    publication.require_request(first)
    publication.require_request(second)
    assertions = publication.required_identity_assertions
    assertions["riverhog-plaintext-sha256"] = "c" * 64
    assert publication.required_identity_assertions != assertions


@pytest.mark.parametrize(
    "field",
    [
        "object_path",
        "content_type",
        "mode",
        "precondition",
        "placement",
        "format",
        "bytes",
        "sha256",
        "sequence",
    ],
)
def test_request_cannot_change_original_metadata_source(field):
    publication, _ = _terminal()
    request = publication.request(b"ciphertext")
    assertions = dict(request.required_identity_assertions)
    changes = {
        "object_path": {"object_path": "archive/other"},
        "content_type": {"content_type": "application/octet-stream"},
        "mode": {"mode": "replace_current"},
        "precondition": {"expected_current_stored_sha256": "d" * 64},
        "placement": {"placement_policy": "changed"},
    }
    assertion_key = {
        "format": "riverhog-format",
        "bytes": "riverhog-plaintext-bytes",
        "sha256": "riverhog-plaintext-sha256",
        "sequence": "riverhog-volume-sequence",
    }
    if field in assertion_key:
        assertions[assertion_key[field]] = "changed"
        changed = request.model_copy(update={"required_identity_assertions": assertions})
    else:
        changed = request.model_copy(update=changes[field])
    with pytest.raises(ValueError, match="differs from its source"):
        publication.require_request(changed)


@pytest.mark.parametrize("change", ["path", "empty"])
def test_inconsistent_physical_receipt_cannot_become_volume_metadata(change, monkeypatch):
    publication, document = _terminal()
    store = MemoryImmutableStore()
    ArchiveRootPublisher(
        object_store=store, passphrase="fixture", scrypt_log_n=1
    ).publish_terminal_metadata(archive_storage_prefix="archive/fixture", document=document)
    receipt = store.objects[publication.object_path].receipt
    changed = replace(
        receipt, **({"object_path": "archive/other"} if change == "path" else {"stored_bytes": 0})
    )
    with pytest.raises(RuntimeError, match="inconsistent receipt"):
        publication.sealed_receipt(changed)
    monkeypatch.setattr(store, "put_immutable_object", lambda **kwargs: changed)
    with pytest.raises(RuntimeError, match="inconsistent receipt"):
        ArchiveRootPublisher(
            object_store=store, passphrase="fixture", scrypt_log_n=1
        ).publish_terminal_metadata(archive_storage_prefix="archive/fixture", document=document)


@pytest.mark.parametrize("prefix", ["", "/"])
def test_empty_metadata_prefix_is_rejected(prefix):
    _, document = _terminal()
    with pytest.raises(ValueError, match="prefix"):
        ArchiveVolumeMetadataPublication.from_document(
            archive_storage_prefix=prefix, document=document
        )
