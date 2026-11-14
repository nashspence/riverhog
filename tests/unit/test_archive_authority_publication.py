from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest
from riverhog_age import decrypt_age_scrypt
from riverhog_archive_contracts import (
    ARCHIVE_ENCRYPTION_FORMAT,
    CollectionEncryptionBinding,
    RecoveryDescriptor,
)
from riverhog_core.archive_manifest import build_collection_archive_root_manifest
from riverhog_core.archive_recovery_descriptor import (
    ArchiveRecoveryDescriptorPublication,
    ArchiveRecoveryDescriptorPublisher,
    build_recovery_descriptor,
)
from riverhog_core.archive_root import ArchiveRootPublication, ArchiveRootPublisher
from riverhog_core.domain.archive import SealedProvenanceObject

from tests.unit.test_archive_root import MemoryImmutableStore


def _publication():
    provenance = SealedProvenanceObject(
        object_id="provenance-root",
        kind="provenance-root",
        relative_path="provenance/root.json.age",
        plaintext_bytes=32,
        plaintext_sha256="b" * 64,
        stored_bytes=64,
        stored_sha256="c" * 64,
        revision="provenance-v1",
        completed_at="2026-08-03T00:00:00Z",
    )
    manifest = build_collection_archive_root_manifest(
        archive_generation="a" * 64,
        artifact_set={"count": 129, "bytes": 8192, "sha256": "d" * 64},
        ordered_volume_sha256="e" * 64,
        provenance_identity=provenance.plaintext_sha256,
        provenance_objects=(provenance,),
    )
    return ArchiveRootPublication("/archive/fixture/", manifest)


def _published_root():
    publication = _publication()
    store = MemoryImmutableStore()
    root = ArchiveRootPublisher(
        object_store=store, passphrase="fixture", scrypt_log_n=1
    ).publish_root_manifest(
        archive_storage_prefix=publication.archive_storage_prefix,
        manifest=publication.plaintext,
    )
    return publication, store, root


def _recovery(root):
    return ArchiveRecoveryDescriptorPublication.from_root(
        archive_storage_prefix="/archive/fixture/",
        root=root,
        encryption=CollectionEncryptionBinding(
            format=ARCHIVE_ENCRYPTION_FORMAT, passphrase_id="fixture-passphrase-id"
        ),
    )


def test_actual_root_and_recovery_preserve_formats_and_commit_original_stored_ciphertext(
    monkeypatch,
):
    publication = _publication()
    store = MemoryImmutableStore()
    calls = []
    original_put = store.put_immutable_object

    def capture(**kwargs):
        calls.append(kwargs)
        return original_put(**kwargs)

    monkeypatch.setattr(store, "put_immutable_object", capture)
    publisher = ArchiveRootPublisher(object_store=store, passphrase="fixture", scrypt_log_n=1)
    first = publisher.publish_root_manifest(
        archive_storage_prefix="/archive/fixture/", manifest=publication.plaintext
    )
    second = publisher.publish_root_manifest(
        archive_storage_prefix="archive/fixture", manifest=publication.plaintext
    )
    assert first == second
    assert calls[0]["content"] != calls[1]["content"]
    assert decrypt_age_scrypt(calls[0]["content"], "fixture") == publication.plaintext
    assert publication.sealed_receipt(store.objects[first.object_path].receipt) == first
    assert (first.artifact_count, first.artifact_bytes) == (129, 8192)
    assert first.artifact_set_sha256 == "d" * 64
    assert first.manifest_bytes == publication.plaintext and first.volume_metadata == ()
    for call in calls:
        request = publication.request(call["content"])
        publication.require_request(request)
        assert call["object_path"] == request.object_path
        assert call["content_type"] == request.content_type
        assert call["required_identity_assertions"] == request.required_identity_assertions
        assert call["placement_policy"] == request.placement_policy

    recovery = _recovery(second)
    assert recovery.content == build_recovery_descriptor(
        encryption=recovery.descriptor.encryption,
        root_relative_path=first.relative_path,
        root_stored_bytes=first.stored_bytes,
        root_stored_sha256=first.stored_sha256,
    )
    published = ArchiveRecoveryDescriptorPublisher(object_store=store).publish(
        archive_storage_prefix="/archive/fixture/",
        root=second,
        encryption=recovery.descriptor.encryption,
    )
    descriptor = RecoveryDescriptor.from_json_bytes(published.content)
    assert descriptor.root.stored_sha256 == hashlib.sha256(calls[0]["content"]).hexdigest()
    assert descriptor.root.stored_sha256 != hashlib.sha256(calls[1]["content"]).hexdigest()
    assert descriptor.root.stored_bytes == len(calls[0]["content"])
    assert recovery.sealed_receipt(store.objects[published.object_path].receipt) == published
    assert published.content == calls[2]["content"] == recovery.content
    assert calls[2]["required_identity_assertions"] == recovery.required_identity_assertions
    assert (
        ArchiveRecoveryDescriptorPublisher(object_store=store).publish(
            archive_storage_prefix="archive/fixture",
            root=second,
            encryption=recovery.descriptor.encryption,
        )
        == published
    )
    assert len(store.objects) == 2


def test_root_randomized_encoding_and_recovery_exact_encoding_have_distinct_identities():
    publication, _, root = _published_root()
    first = publication.request(b"first opaque ciphertext")
    second = publication.request(b"different opaque ciphertext")
    assert first.stored_sha256 != second.stored_sha256
    publication.require_request(first)
    publication.require_request(second)
    assertions = publication.required_identity_assertions
    assertions["riverhog-plaintext-sha256"] = "f" * 64
    assert publication.required_identity_assertions != assertions

    recovery = _recovery(root)
    recovery.require_request(recovery.request())
    assert recovery.request().stored_sha256 == hashlib.sha256(recovery.content).hexdigest()
    with pytest.raises(ValueError, match="recovery request differs"):
        recovery.require_request(
            recovery.request().model_copy(update={"stored_sha256": second.stored_sha256})
        )
    changed = _recovery(replace(root, stored_sha256="f" * 64))
    assert changed.content != recovery.content
    assert changed.required_identity_assertions != recovery.required_identity_assertions


@pytest.mark.parametrize("kind", ["root", "recovery"])
@pytest.mark.parametrize(
    "field",
    [
        "path",
        "content_type",
        "mode",
        "precondition",
        "placement",
        "format",
        "bytes",
        "digest",
        "extra",
    ],
)
def test_authority_request_cannot_change_original_format_source(kind, field):
    root_publication, _, root = _published_root()
    publication = root_publication if kind == "root" else _recovery(root)
    request = publication.request(b"ciphertext") if kind == "root" else publication.request()
    assertions = dict(request.required_identity_assertions)
    changes = {
        "path": {"object_path": "archive/other"},
        "content_type": {"content_type": "application/octet-stream"},
        "mode": {"mode": "replace_current"},
        "precondition": {"expected_current_stored_sha256": "f" * 64},
        "placement": {"placement_policy": "changed"},
    }
    assertion_keys = {
        "format": "riverhog-format",
        "bytes": "riverhog-plaintext-bytes" if kind == "root" else "riverhog-sha256",
        "digest": (
            "riverhog-plaintext-sha256" if kind == "root" else "riverhog-root-stored-sha256"
        ),
        "extra": "unexpected-assertion",
    }
    if field in assertion_keys:
        assertions[assertion_keys[field]] = "changed"
        changes[field] = {"required_identity_assertions": assertions}
    with pytest.raises(ValueError, match="request differs from its source"):
        publication.require_request(request.model_copy(update=changes[field]))


@pytest.mark.parametrize("field", ["artifact_set", "stored_bytes", "stored_sha256"])
def test_authority_specific_commitments_cannot_be_substituted(field):
    publication, _, root = _published_root()
    if field == "artifact_set":
        request = publication.request(b"ciphertext")
        assertions = dict(request.required_identity_assertions)
        assertions["riverhog-artifact-set-sha256"] = "f" * 64
        changed = request.model_copy(update={"required_identity_assertions": assertions})
    else:
        publication = _recovery(root)
        request = publication.request()
        changed = request.model_copy(
            update={field: request.stored_bytes + 1 if field == "stored_bytes" else "f" * 64}
        )
    with pytest.raises(ValueError, match="request differs from its source"):
        publication.require_request(changed)


@pytest.mark.parametrize(
    "kind,field",
    [
        ("root", "path"),
        ("root", "empty"),
        ("recovery", "path"),
        ("recovery", "bytes"),
        ("recovery", "sha256"),
    ],
)
def test_inconsistent_physical_receipt_cannot_become_authority(kind, field, monkeypatch):
    publication, store, root = _published_root()
    if kind == "recovery":
        publication = _recovery(root)
        ArchiveRecoveryDescriptorPublisher(object_store=store).publish(
            archive_storage_prefix=publication.archive_storage_prefix,
            root=root,
            encryption=publication.descriptor.encryption,
        )
    receipt = store.objects[publication.object_path].receipt
    changes = {
        "path": {"object_path": "archive/other"},
        "empty": {"stored_bytes": 0},
        "bytes": {"stored_bytes": receipt.stored_bytes + 1},
        "sha256": {"stored_sha256": "f" * 64},
    }
    changed = replace(receipt, **changes[field])
    with pytest.raises(RuntimeError, match="inconsistent receipt|changed the recovery"):
        publication.sealed_receipt(changed)
    monkeypatch.setattr(store, "put_immutable_object", lambda **kwargs: changed)
    with pytest.raises(RuntimeError, match="inconsistent receipt|changed the recovery"):
        if kind == "root":
            ArchiveRootPublisher(
                object_store=store, passphrase="fixture", scrypt_log_n=1
            ).publish_root_manifest(
                archive_storage_prefix=publication.archive_storage_prefix,
                manifest=publication.plaintext,
            )
        else:
            ArchiveRecoveryDescriptorPublisher(object_store=store).publish(
                archive_storage_prefix=publication.archive_storage_prefix,
                root=root,
                encryption=publication.descriptor.encryption,
            )


@pytest.mark.parametrize("kind", ["root", "recovery"])
@pytest.mark.parametrize("invalid", ["empty-prefix", "mutable", "noncanonical"])
def test_authority_publication_rejects_missing_identity_or_mutable_bytes(kind, invalid):
    root_publication, _, root = _published_root()
    original = root_publication.plaintext if kind == "root" else _recovery(root).content
    constructor = ArchiveRootPublication if kind == "root" else ArchiveRecoveryDescriptorPublication
    prefix = "" if invalid == "empty-prefix" else "archive/fixture"
    content = bytearray(original) if invalid == "mutable" else original
    if invalid == "noncanonical":
        content += b"\n"
    with pytest.raises((TypeError, ValueError)):
        constructor(prefix, content)
