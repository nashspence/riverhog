from __future__ import annotations

import builtins
import hashlib
from dataclasses import dataclass, field

from riverhog_archive_contracts import (
    RECOVERY_DESCRIPTOR_PATH,
    ArchiveRootCiphertextIdentity,
    CollectionEncryptionBinding,
    RecoveryDescriptor,
)
from riverhog_storage_adapter_protocol import SmallObjectWriteRequest

from riverhog_core.archive_formats import RECOVERY_DESCRIPTOR_STORAGE_FORMAT
from riverhog_core.archive_root import SealedArchiveRoot
from riverhog_core.ports.archive_objects import ImmutableArchiveObjectStore, ImmutableObjectReceipt

RECOVERY_DESCRIPTOR_CONTENT_TYPE = "application/vnd.riverhog.recovery-descriptor+json"


def build_recovery_descriptor(
    *,
    encryption: CollectionEncryptionBinding,
    root_relative_path: str,
    root_stored_bytes: int,
    root_stored_sha256: str,
) -> bytes:
    """Return the canonical plaintext recovery descriptor for an encrypted root."""

    return RecoveryDescriptor(
        encryption=encryption,
        root=ArchiveRootCiphertextIdentity(
            path=root_relative_path,
            stored_bytes=root_stored_bytes,
            stored_sha256=root_stored_sha256,
        ),
    ).to_json_bytes()


@dataclass(frozen=True, slots=True)
class SealedRecoveryDescriptor:
    object_path: str
    relative_path: str
    revision: str | None
    bytes: int
    sha256: str
    completed_at: str
    content: builtins.bytes


@dataclass(frozen=True, slots=True)
class ArchiveRecoveryDescriptorPublication:
    """Exact plaintext recovery bytes bound to the original stored root."""

    archive_storage_prefix: str
    content: bytes = field(repr=False)
    descriptor: RecoveryDescriptor = field(init=False, repr=False)
    sha256: str = field(init=False)

    def __post_init__(self) -> None:
        prefix = self.archive_storage_prefix.strip("/")
        if not prefix:
            raise ValueError("archive storage prefix must not be empty")
        if not isinstance(self.content, bytes):
            raise TypeError("archive recovery content must be immutable bytes")
        object.__setattr__(self, "archive_storage_prefix", prefix)
        object.__setattr__(self, "descriptor", RecoveryDescriptor.from_json_bytes(self.content))
        object.__setattr__(self, "sha256", hashlib.sha256(self.content).hexdigest())

    @classmethod
    def from_root(
        cls,
        *,
        archive_storage_prefix: str,
        root: SealedArchiveRoot,
        encryption: CollectionEncryptionBinding,
    ) -> ArchiveRecoveryDescriptorPublication:
        return cls(
            archive_storage_prefix,
            build_recovery_descriptor(
                encryption=encryption,
                root_relative_path=root.relative_path,
                root_stored_bytes=root.stored_bytes,
                root_stored_sha256=root.stored_sha256,
            ),
        )

    @property
    def object_path(self) -> str:
        return f"{self.archive_storage_prefix}/{RECOVERY_DESCRIPTOR_PATH}"

    @property
    def required_identity_assertions(self) -> dict[str, str]:
        return {
            "riverhog-format": RECOVERY_DESCRIPTOR_STORAGE_FORMAT,
            "riverhog-sha256": self.sha256,
            "riverhog-root-stored-sha256": self.descriptor.root.stored_sha256,
        }

    def request(self) -> SmallObjectWriteRequest:
        return SmallObjectWriteRequest(
            object_path=self.object_path,
            content_type=RECOVERY_DESCRIPTOR_CONTENT_TYPE,
            required_identity_assertions=self.required_identity_assertions,
            placement_policy="immediate_default",
            mode="create_only",
            stored_bytes=len(self.content),
            stored_sha256=self.sha256,
        )

    def require_request(self, request: SmallObjectWriteRequest) -> None:
        if request != self.request():
            raise ValueError("archive recovery request differs from its source")

    def sealed_receipt(self, receipt: ImmutableObjectReceipt) -> SealedRecoveryDescriptor:
        if receipt.object_path != self.object_path or receipt.stored_bytes != len(self.content):
            raise RuntimeError("immutable descriptor store returned an inconsistent receipt")
        if receipt.stored_sha256 != self.sha256:
            raise RuntimeError("immutable descriptor store changed the recovery descriptor")
        return SealedRecoveryDescriptor(
            object_path=receipt.object_path,
            relative_path=RECOVERY_DESCRIPTOR_PATH,
            revision=receipt.revision,
            bytes=len(self.content),
            sha256=self.sha256,
            completed_at=receipt.completed_at,
            content=self.content,
        )


class ArchiveRecoveryDescriptorPublisher:
    """Publish the plaintext key-selection contract for one immutable root."""

    def __init__(self, *, object_store: ImmutableArchiveObjectStore) -> None:
        self._object_store = object_store

    def publish(
        self,
        *,
        archive_storage_prefix: str,
        root: SealedArchiveRoot,
        encryption: CollectionEncryptionBinding,
    ) -> SealedRecoveryDescriptor:
        publication = ArchiveRecoveryDescriptorPublication.from_root(
            archive_storage_prefix=archive_storage_prefix, root=root, encryption=encryption
        )
        request = publication.request()
        receipt = self._object_store.put_immutable_object(
            object_path=request.object_path,
            content=publication.content,
            content_type=request.content_type,
            required_identity_assertions=request.required_identity_assertions,
            placement_policy=request.placement_policy,
        )
        return publication.sealed_receipt(receipt)
