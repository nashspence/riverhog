"""Test-owned immutable bytes through the maintained adapter invocation contract."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from threading import Lock

from riverhog_core.ports.archive_objects import (
    ImmutableObjectReceipt as ArchiveImmutableObjectReceipt,
)
from riverhog_core.stores.storage_adapter_archive_objects import (
    StorageAdapterImmutableArchiveObjectStore,
)
from riverhog_storage_adapter_protocol import (
    AdapterDescriptor,
    BinaryContent,
    ImmutableObjectReceipt,
    ObjectHeadRequest,
    ObjectMetadataReceipt,
    SmallObjectWriteRequest,
    StorageAdapterRejection,
    WriteInvocation,
    execute_write_invocation,
)

from tests.unit.archive_object_fixtures import MemoryArchiveStore


@dataclass(frozen=True)
class StoredImmutableObject:
    content: bytes
    identity: dict[str, str]
    receipt: ArchiveImmutableObjectReceipt


class _StoredObjects(Mapping[str, StoredImmutableObject]):
    def __init__(self, store):
        self.store = store

    def __getitem__(self, path):
        content = self.store.objects[path]
        return StoredImmutableObject(
            content,
            self.store.object_metadata[path],
            self.store._immutable_receipt(path, content),
        )

    def __contains__(self, path):
        return path in self.store.objects

    def __iter__(self):
        return iter(self.store.objects)

    def __len__(self):
        return len(self.store.objects)


class _MemorySmallObjects:
    """A synchronous single-attempt owner over the same bytes as mutable ports."""

    def __init__(self, incarnation_id, store):
        self.incarnation_id = incarnation_id
        self.store = store
        self.lock = Lock()

    def descriptor(self):
        return AdapterDescriptor(
            storage_incarnation_id=self.incarnation_id,
            implementation_id="fixture.immutable-objects/v1",
            implementation_version="1",
            read_mode="immediate",
            minimum_nonfinal_segment_bytes=1,
        )

    def execute_write(self, invocation: WriteInvocation, *, content: BinaryContent = b""):
        return execute_write_invocation(
            self, invocation, content=content, profile="joined-single-attempt"
        )

    def put_small_object(self, request: SmallObjectWriteRequest, content: BinaryContent):
        data = content if isinstance(content, bytes) else b"".join(content)
        if (
            len(data) != request.stored_bytes
            or hashlib.sha256(data).hexdigest() != request.stored_sha256
        ):
            raise ValueError("test immutable content differs from its exact request")
        with self.lock:
            existing = self.store.objects.get(request.object_path)
            if existing is not None and (
                len(existing) != request.stored_bytes
                or hashlib.sha256(existing).hexdigest() != request.stored_sha256
                or self.store.object_content_types[request.object_path] != request.content_type
                or self.store.object_metadata[request.object_path]
                != request.required_identity_assertions
            ):
                raise StorageAdapterRejection(
                    "identity_conflict", "existing immutable object differs from its exact request"
                )
            receipt = self.store.put_immutable_object(
                object_path=request.object_path,
                content=data,
                content_type=request.content_type,
                required_identity_assertions=request.required_identity_assertions,
                placement_policy=request.placement_policy,
            )
            return ImmutableObjectReceipt(
                object_path=receipt.object_path,
                revision=receipt.revision,
                entity_token=receipt.entity_token,
                stored_bytes=receipt.stored_bytes,
                stored_sha256=receipt.stored_sha256,
                verified_content_type=self.store.object_content_types[receipt.object_path],
                verified_identity_assertions=self.store.object_metadata[receipt.object_path],
                verified_placement_policy=request.placement_policy,
                completed_at=receipt.completed_at,
            )

    def head_object(self, request: ObjectHeadRequest):
        with self.lock:
            data = self.store.objects.get(request.object.object_path)
            if data is None:
                return None
            receipt = self.store._immutable_receipt(request.object.object_path, data)
            return ObjectMetadataReceipt(
                object_path=receipt.object_path,
                revision=receipt.revision,
                entity_token=receipt.entity_token,
                stored_bytes=receipt.stored_bytes,
                stored_sha256=receipt.stored_sha256,
                content_type=self.store.object_content_types[receipt.object_path],
                observed_identity_assertions=self.store.object_metadata[receipt.object_path],
                verified_placement_policy="immediate_default",
                completed_at=receipt.completed_at,
            )


class MemoryImmutableArchiveStore(StorageAdapterImmutableArchiveObjectStore):
    def __init__(self, incarnation_id, *, backing=None):
        store = backing if backing is not None else MemoryArchiveStore()
        self.objects = _StoredObjects(store)
        super().__init__(_MemorySmallObjects(incarnation_id, store))  # type: ignore[arg-type]
