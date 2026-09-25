from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
from a_riverhog_filesystem_store import FilesystemStorageAdapter, FilesystemStorageAdapterConfig
from a_riverhog_filesystem_store.incarnation import (
    StorageIncarnationError,
    provision_storage_root,
    read_storage_incarnation,
)
from riverhog_storage_adapter_protocol import DeletePrefixRequest, SmallObjectWriteRequest


def test_explicit_provisioning_survives_restart_and_refuses_unmarked_content(
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    with pytest.raises(StorageIncarnationError, match="no incarnation marker"):
        FilesystemStorageAdapter(FilesystemStorageAdapterConfig(root=root))
    (root / "existing").write_bytes(b"owned elsewhere")
    with pytest.raises(StorageIncarnationError, match="unmarked nonempty"):
        provision_storage_root(root)
    (root / "existing").unlink()

    identity = provision_storage_root(root)
    assert read_storage_incarnation(root) == identity
    assert provision_storage_root(root) == identity
    with FilesystemStorageAdapter(FilesystemStorageAdapterConfig(root=root)) as adapter:
        assert adapter.descriptor().storage_incarnation_id == identity
    with FilesystemStorageAdapter(FilesystemStorageAdapterConfig(root=root)) as restarted:
        assert restarted.descriptor().storage_incarnation_id == identity


def test_marker_corruption_never_reprovisions_or_admits_effects(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    provision_storage_root(root)
    with FilesystemStorageAdapter(FilesystemStorageAdapterConfig(root=root)) as adapter:
        (root / ".riverhog-incarnation").write_bytes(b"truncated")
        with pytest.raises(StorageIncarnationError, match="marker is invalid"):
            adapter.descriptor()
        with pytest.raises(StorageIncarnationError, match="marker is invalid"):
            adapter.delete_prefix(DeletePrefixRequest(object_prefix="archive/"))
    with pytest.raises(StorageIncarnationError, match="marker is invalid"):
        provision_storage_root(root)


def test_repointing_the_root_path_does_not_redirect_an_admitted_effect(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    original_id = provision_storage_root(root)
    with FilesystemStorageAdapter(FilesystemStorageAdapterConfig(root=root)) as adapter:
        old_root = tmp_path / "old-root"
        root.rename(old_root)
        root.mkdir()
        replacement_id = provision_storage_root(root)
        assert replacement_id != original_id
        payload = b"pinned to the original root"
        adapter.put_small_object(
            SmallObjectWriteRequest(
                object_path="archive/pinned",
                content_type="application/octet-stream",
                required_identity_assertions={},
                placement_policy="immediate_default",
                mode="create_only",
                stored_bytes=len(payload),
                stored_sha256=hashlib.sha256(payload).hexdigest(),
            ),
            payload,
        )
        assert adapter.descriptor().storage_incarnation_id == original_id
        assert any((old_root / "objects").rglob("payload.data"))
        assert not (root / "objects").exists()
