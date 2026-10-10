from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import pytest
from riverhog_age import encrypt_age_scrypt
from riverhog_core.archive_provenance import ArchiveProvenancePublisher
from riverhog_core.ports.archive_objects import ArchiveObjectIdentityConflict
from riverhog_core.stores.storage_adapter_archive_objects import (
    StorageAdapterImmutableArchiveObjectStore,
)
from riverhog_storage_adapter_protocol import (
    ObjectHeadRequest,
    ObjectLocator,
    SmallObjectWriteRequest,
)


@pytest.mark.skipif(sys.platform == "win32", reason="native filesystem adapter requires POSIX")
def test_randomized_archive_retry_recovers_lost_receipt_without_replacing_ciphertext(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from a_riverhog_filesystem_store import (
        FilesystemStorageAdapter,
        FilesystemStorageAdapterConfig,
    )
    from a_riverhog_filesystem_store.incarnation import provision_storage_root

    root = tmp_path / "store"
    root.mkdir()
    provision_storage_root(root)
    config = FilesystemStorageAdapterConfig(root=root, minimum_free_bytes=0)
    plaintext = b"exact canonical archive evidence"
    ciphertext = encrypt_age_scrypt(plaintext, "fixture", log_n=1)
    replacement = encrypt_age_scrypt(plaintext, "fixture", log_n=1)
    assert ciphertext != replacement
    assertions = {
        "riverhog-format": "riverhog-provenance-journal/v1+age",
        "riverhog-plaintext-bytes": str(len(plaintext)),
        "riverhog-plaintext-sha256": hashlib.sha256(plaintext).hexdigest(),
    }
    options = {
        "object_path": "archive/provenance/journal.age",
        "content_type": "application/vnd.riverhog-provenance-journal.v1.age",
        "required_identity_assertions": assertions,
        "placement_policy": "immediate_default",
    }
    with FilesystemStorageAdapter(config) as adapter:
        put = adapter.put_small_object

        def lose_response(request: SmallObjectWriteRequest, content: bytes):
            put(request, content)
            raise ConnectionError("lost completed response")

        with monkeypatch.context() as scoped:
            scoped.setattr(adapter, "put_small_object", lose_response)
            with pytest.raises(ConnectionError, match="lost completed"):
                StorageAdapterImmutableArchiveObjectStore(adapter).put_immutable_object(
                    content=ciphertext, **options
                )

    with FilesystemStorageAdapter(config) as restarted:
        store = StorageAdapterImmutableArchiveObjectStore(restarted)
        receipt = store.put_immutable_object(content=replacement, **options)
        assert receipt.stored_bytes == len(ciphertext)
        assert receipt.stored_sha256 == hashlib.sha256(ciphertext).hexdigest()
        assert store.put_immutable_object(content=replacement, **options) == receipt
        for change in (
            {"content_type": "application/octet-stream"},
            {"required_identity_assertions": {**assertions, "riverhog-plaintext-sha256": "f" * 64}},
            {"required_identity_assertions": {**assertions, "riverhog-plaintext-bytes": "1"}},
        ):
            with pytest.raises(ArchiveObjectIdentityConflict):
                store.put_immutable_object(content=replacement, **{**options, **change})


@pytest.mark.skipif(sys.platform == "win32", reason="native filesystem adapter requires POSIX")
def test_repeated_provenance_publication_reuses_ciphertext_before_encryption(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from a_riverhog_filesystem_store import (
        FilesystemStorageAdapter,
        FilesystemStorageAdapterConfig,
    )
    from a_riverhog_filesystem_store.incarnation import provision_storage_root

    root = tmp_path / "store"
    root.mkdir()
    provision_storage_root(root)
    config = FilesystemStorageAdapterConfig(root=root, minimum_free_bytes=0)
    plaintext = b"exact journal segment"
    with FilesystemStorageAdapter(config) as adapter:
        publisher = ArchiveProvenancePublisher(
            object_store=StorageAdapterImmutableArchiveObjectStore(adapter),
            passphrase="fixture",
            scrypt_log_n=1,
        )
        receipt = publisher.publish_journal_segment(
            archive_storage_prefix="archive", content=plaintext
        )

    def forbid_encryption(*args, **kwargs):
        raise AssertionError("matching durable object must not be encrypted again")

    monkeypatch.setattr("riverhog_core.archive_provenance.encrypt_age_scrypt", forbid_encryption)
    with FilesystemStorageAdapter(config) as adapter:
        publisher = ArchiveProvenancePublisher(
            object_store=StorageAdapterImmutableArchiveObjectStore(adapter),
            passphrase="fixture",
            scrypt_log_n=1,
        )
        assert (
            publisher.publish_journal_segment(archive_storage_prefix="archive", content=plaintext)
            == receipt
        )
        store = StorageAdapterImmutableArchiveObjectStore(adapter)
        head = adapter.head_object(
            ObjectHeadRequest(
                object=ObjectLocator(object_path="archive/" + receipt.relative_path),
                expected_placement_policy="immediate_default",
            )
        )
        assert head is not None
        options = {
            "object_path": head.object_path,
            "content_type": head.content_type,
            "required_identity_assertions": head.observed_identity_assertions,
            "placement_policy": "immediate_default",
        }
        for change in (
            {"content_type": "application/octet-stream"},
            {
                "required_identity_assertions": {
                    **head.observed_identity_assertions,
                    "riverhog-plaintext-sha256": "f" * 64,
                }
            },
            {
                "required_identity_assertions": {
                    **head.observed_identity_assertions,
                    "riverhog-plaintext-bytes": "1",
                }
            },
        ):
            with pytest.raises(ArchiveObjectIdentityConflict):
                store.put_immutable_object(
                    content=lambda: forbid_encryption(), **{**options, **change}
                )
        head_calls = 0
        actual_head = adapter.head_object

        def hide_first_head(request):
            nonlocal head_calls
            head_calls += 1
            return None if head_calls == 1 else actual_head(request)

        monkeypatch.setattr(adapter, "head_object", hide_first_head)
        content_calls = 0

        def construct_ciphertext():
            nonlocal content_calls
            content_calls += 1
            return encrypt_age_scrypt(plaintext, "fixture", log_n=1)

        raced = store.put_immutable_object(content=construct_ciphertext, **options)
        assert content_calls == 1 and head_calls == 2
        assert raced.stored_sha256 == receipt.stored_sha256
        assert raced.stored_bytes == receipt.stored_bytes
        assert raced.revision == receipt.revision
