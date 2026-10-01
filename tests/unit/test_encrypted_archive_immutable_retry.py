from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import pytest
from riverhog_age import encrypt_age_scrypt
from riverhog_core.ports.archive_objects import ArchiveObjectIdentityConflict
from riverhog_core.stores.storage_adapter_archive_objects import (
    StorageAdapterImmutableArchiveObjectStore,
)
from riverhog_storage_adapter_protocol import SmallObjectWriteRequest


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
