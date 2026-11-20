from __future__ import annotations

import hashlib
from dataclasses import FrozenInstanceError, replace

import pytest
from riverhog_age import decrypt_age_scrypt
from riverhog_core import archive_collection_metadata as publication_module
from riverhog_core.archive_collection_metadata import (
    ArchiveCollectionMetadataPublication as Publication,
)
from riverhog_core.archive_collection_metadata import ArchiveCollectionMetadataPublisher
from riverhog_core.immutable_archive_identity import immutable_archive_identity
from riverhog_core.ports.archive_store import (
    CollectionDescriptionReceipt,
    CollectionTagObjectReceipt,
)
from riverhog_core.runtime_config import TEST_ARCHIVE_PASSPHRASE_ID
from riverhog_protocol import (
    COLLECTION_DESCRIPTION_RELATIVE_PATH,
    COLLECTION_TAG_HEAD_RELATIVE_PATH,
    CollectionDescriptionDocument,
    CollectionTagHeadDocument,
    CollectionTagNode,
    collection_tag_node_digest,
    collection_tag_node_path,
    encode_collection_tag_node,
)
from riverhog_storage_adapter_protocol import StorageAdapterRejection

from tests.unit.test_archive_root import MemoryImmutableStore
from tests.unit.test_storage_adapter_archive_store import _MemoryAdapter, _store

_ROLES = ("description", "tag-head", "tag-node")


def _publication(role, *, revision=1, suffix=""):
    extra = {}
    if role == "description":
        document = CollectionDescriptionDocument.seal(
            archive_root_sha256="a" * 64, revision=revision, description="caption" + suffix
        )
        plaintext = document.to_json_bytes()
        path = COLLECTION_DESCRIPTION_RELATIVE_PATH
        storage_format = "riverhog-collection-description/v1"
        content_type = "application/vnd.riverhog.collection-description.v1+age"
        extra = {
            "riverhog-archive-root-sha256": "a" * 64,
            "riverhog-description-identity": document.description_identity,
            "riverhog-description-revision": str(revision),
        }
    elif role == "tag-head":
        document = CollectionTagHeadDocument.seal(
            archive_root_sha256="a" * 64,
            revision=revision,
            root_sha256=None if not suffix else "b" * 64,
        )
        plaintext = document.to_json_bytes()
        path = COLLECTION_TAG_HEAD_RELATIVE_PATH
        storage_format = "riverhog-collection-tag-head/v1"
        content_type = "application/vnd.riverhog.collection-tag-head.v1+age"
        extra = {
            "riverhog-archive-root-sha256": "a" * 64,
            "riverhog-tag-head-identity": document.head_identity,
            "riverhog-tag-set-identity": document.tag_set_identity,
            "riverhog-tag-revision": str(revision),
        }
    else:
        plaintext = encode_collection_tag_node(
            CollectionTagNode(prefix=b"fixture", tag=("fixture" + suffix).encode())
        )
        digest = collection_tag_node_digest(plaintext)
        assert digest != hashlib.sha256(plaintext).hexdigest()
        path = collection_tag_node_path(digest)
        storage_format = "riverhog-collection-tag-node/v1"
        content_type = "application/vnd.riverhog.collection-tag-node.v1+age"
        extra = {"riverhog-tag-node-sha256": digest}
    publication = Publication("/archives/fixture/", role, plaintext, TEST_ARCHIVE_PASSPHRASE_ID)
    identity = {
        "riverhog-format": storage_format,
        **extra,
        "riverhog-encryption": "age-v1-scrypt",
        "riverhog-passphrase-id": TEST_ARCHIVE_PASSPHRASE_ID,
        "riverhog-plaintext-bytes": str(len(plaintext)),
        "riverhog-plaintext-sha256": hashlib.sha256(plaintext).hexdigest(),
    }
    return publication, path, content_type, identity


def _store_publish(store, p, *, expected=None, digest=None):
    kwargs = dict(
        collection_id=17,
        archive_storage_prefix=p.archive_storage_prefix,
        passphrase_id=p.passphrase_id,
    )
    if p.role == "tag-node":
        return store.publish_collection_tag_node(
            **kwargs,
            encoded=p.plaintext,
            digest=digest or p.required_identity_assertions["riverhog-tag-node-sha256"],
        )
    method = (
        store.publish_collection_description
        if p.role == "description"
        else store.publish_collection_tag_head
    )
    return method(
        **kwargs,
        document=p.plaintext,
        expected_current_stored_sha256=expected,
    )


@pytest.mark.parametrize("role", _ROLES)
def test_complete_format_identity_and_original_ciphertext_request(role):
    publication, path, content_type, identity = _publication(role)
    assert publication.object_path == "archives/fixture/" + path
    assert publication.required_identity_assertions == identity
    request = publication.request(b"first randomized ciphertext")
    assert request.content_type == content_type
    assert request.required_identity_assertions == identity
    assert request.mode == "create_only" and request.expected_current_stored_sha256 is None
    assert request.stored_bytes == len(b"first randomized ciphertext")
    assert request.stored_sha256 == hashlib.sha256(b"first randomized ciphertext").hexdigest()
    publication.require_request(request)
    alternate = publication.request(b"earlier randomized ciphertext with different size")
    publication.require_request(alternate)
    assert immutable_archive_identity(request) == immutable_archive_identity(alternate)
    with pytest.raises(FrozenInstanceError):
        publication.plaintext = b"changed"
    identity.clear()
    request.required_identity_assertions.clear()
    assert publication.required_identity_assertions["riverhog-format"]


@pytest.mark.parametrize("role", _ROLES)
def test_actual_immutable_publisher_encrypts_preserves_prior_encoding_and_projects_receipt(role):
    publication, _, content_type, identity = _publication(role)
    physical = MemoryImmutableStore()
    publisher = ArchiveCollectionMetadataPublisher(
        object_store=physical,
        passphrase="fixture key",
        scrypt_log_n=1,
    )
    receipt = publisher.publish_object(publication)
    stored = physical.objects[publication.object_path]
    assert decrypt_age_scrypt(stored.content, "fixture key") == publication.plaintext
    assert stored.identity == identity
    assert receipt.stored_sha256 == hashlib.sha256(stored.content).hexdigest()
    assert receipt.object_path == publication.object_path
    assert isinstance(
        receipt,
        CollectionDescriptionReceipt if role == "description" else CollectionTagObjectReceipt,
    )
    repeated = publisher.publish_object(publication)
    assert repeated == receipt and physical.objects[publication.object_path] is stored
    assert publication.request(stored.content).content_type == content_type


@pytest.mark.parametrize("role", _ROLES)
def test_existing_adapter_uses_same_format_and_observes_prior_before_key_lookup(role):
    publication, _, content_type, identity = _publication(role)
    adapter = _MemoryAdapter()
    store = _store(adapter)
    receipt = _store_publish(store, publication)
    stored = adapter.objects[publication.object_path]
    assert stored.identity == identity and stored.content_type == content_type
    passphrase = store._config.archive_passphrase_for(publication.passphrase_id)
    assert decrypt_age_scrypt(stored.content, passphrase) == publication.plaintext
    store._config = None
    assert _store_publish(store, publication) == receipt
    assert adapter.objects[publication.object_path] is stored and adapter.reads == 0


@pytest.mark.parametrize("role", ("description", "tag-head"))
def test_existing_mutable_updates_preserve_exact_cas_request(role):
    first, *_ = _publication(role)
    second, *_ = _publication(role, revision=2, suffix="changed")
    adapter = _MemoryAdapter()
    store = _store(adapter)
    prior = _store_publish(store, first)
    stored = adapter.objects[first.object_path]
    for wrong in (None, "0" * 64):
        with pytest.raises(StorageAdapterRejection):
            _store_publish(store, second, expected=wrong)
        assert adapter.objects[first.object_path] is stored
    receipt = _store_publish(store, second, expected=prior.stored_sha256)
    assert receipt.stored_sha256 != prior.stored_sha256
    assert adapter.objects[second.object_path].identity == second.required_identity_assertions
    request = second.request(b"ciphertext", expected_current_stored_sha256=prior.stored_sha256)
    assert request.mode == "replace_current"
    assert request.expected_current_stored_sha256 == prior.stored_sha256
    second.require_request(request, expected_current_stored_sha256=prior.stored_sha256)
    with pytest.raises(ValueError, match="differs"):
        second.require_request(request)


@pytest.mark.parametrize("role", _ROLES)
@pytest.mark.parametrize("changed", ("path", "content-type", "assertions", "placement", "cas"))
def test_original_format_request_refuses_changed_forward_identity(role, changed):
    publication, *_ = _publication(role)
    request = publication.request(b"ciphertext")
    changes = {
        "path": {"object_path": "archives/other/object"},
        "content-type": {"content_type": "application/octet-stream"},
        "assertions": {"required_identity_assertions": {"riverhog-format": "other"}},
        "placement": {"placement_policy": "immediate_container"},
        "cas": {"mode": "replace_current", "expected_current_stored_sha256": "a" * 64},
    }[changed]
    changed_request = request.model_copy(update=changes)
    with pytest.raises(ValueError, match="differs"):
        publication.require_request(changed_request)


def test_tag_node_digest_is_format_owned_and_never_accepts_replacement():
    publication, *_ = _publication("tag-node")
    adapter = _MemoryAdapter()
    with pytest.raises(ValueError, match="digest differs"):
        _store_publish(_store(adapter), publication, digest=publication.plaintext_sha256)
    assert not adapter.objects
    with pytest.raises(ValueError, match="immutable"):
        publication.request(b"ciphertext", expected_current_stored_sha256="a" * 64)
    with pytest.raises(ValueError, match="immutable"):
        publication.require_request(
            publication.request(b"ciphertext"),
            expected_current_stored_sha256="a" * 64,
        )


@pytest.mark.parametrize("role", _ROLES)
def test_invalid_original_documents_and_receipts_never_succeed(role):
    publication, *_ = _publication(role)
    with pytest.raises((ValueError, UnicodeError)):
        replace(publication, plaintext=b"invalid")
    with pytest.raises(TypeError, match="immutable bytes"):
        replace(publication, plaintext=bytearray(publication.plaintext))
    with pytest.raises(ValueError, match="canonical"):
        replace(publication, archive_storage_prefix="archives/../nested")
    physical = MemoryImmutableStore()
    publisher = ArchiveCollectionMetadataPublisher(
        object_store=physical,
        passphrase="fixture key",
        scrypt_log_n=1,
    )
    publisher.publish_object(publication)
    receipt = physical.objects[publication.object_path].receipt
    for changes in (
        {"object_path": "archives/other/description.json.age"},
        {"stored_bytes": 0},
        {"stored_sha256": ""},
    ):
        with pytest.raises(RuntimeError, match="inconsistent receipt"):
            publication.receipt(replace(receipt, **changes))


def test_publisher_key_and_supported_role_are_explicit(monkeypatch):
    publication, *_ = _publication("description")
    with pytest.raises(ValueError, match="unsupported"):
        replace(publication, role="other")
    with pytest.raises(ValueError, match="empty"):
        ArchiveCollectionMetadataPublisher(
            object_store=MemoryImmutableStore(),
            passphrase="",
            scrypt_log_n=1,
        )

    def interrupted(*args, **kwargs):
        raise OSError("injected original metadata encryption interruption")

    monkeypatch.setattr(publication_module, "encrypt_age_scrypt", interrupted)
    physical = MemoryImmutableStore()
    publisher = ArchiveCollectionMetadataPublisher(
        object_store=physical,
        passphrase="fixture key",
        scrypt_log_n=1,
    )
    with pytest.raises(OSError, match="injected"):
        publisher.publish_object(publication)
    assert not physical.objects


@pytest.mark.parametrize("role", _ROLES)
def test_format_owner_preserves_canonical_nested_prefix_and_existing_store_placement(role):
    publication, *_ = _publication(role)
    nested = replace(publication, archive_storage_prefix="archives/fixture/nested")
    physical = MemoryImmutableStore()
    publisher = ArchiveCollectionMetadataPublisher(
        object_store=physical,
        passphrase="fixture key",
        scrypt_log_n=1,
    )
    receipt = publisher.publish_object(nested)
    assert receipt.object_path == "archives/fixture/nested/" + publication.relative_path
    assert (
        decrypt_age_scrypt(physical.objects[receipt.object_path].content, "fixture key")
        == publication.plaintext
    )
    adapter = _MemoryAdapter()
    with pytest.raises(ValueError, match="canonical"):
        _store_publish(_store(adapter), nested)
    assert not adapter.objects
