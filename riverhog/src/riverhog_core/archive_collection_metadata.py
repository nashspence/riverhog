"""Format-owned collection description and tag object publication."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Literal

from riverhog_age import encrypt_age_scrypt
from riverhog_protocol import (
    COLLECTION_DESCRIPTION_DOCUMENT_FORMAT,
    COLLECTION_DESCRIPTION_RELATIVE_PATH,
    COLLECTION_TAG_HEAD_FORMAT,
    COLLECTION_TAG_HEAD_RELATIVE_PATH,
    COLLECTION_TAG_NODE_FORMAT,
    CollectionDescriptionDocument,
    CollectionTagHeadDocument,
    collection_tag_node_digest,
    collection_tag_node_path,
    decode_collection_tag_node,
)
from riverhog_protocol.paths import validate_canonical_relpath
from riverhog_storage_adapter_protocol import (
    ImmutableObjectReceipt as AdapterReceipt,
)
from riverhog_storage_adapter_protocol import (
    ObjectMetadataReceipt,
    SmallObjectWriteRequest,
)

from riverhog_core.ports.archive_objects import ImmutableArchiveObjectStore, ImmutableObjectReceipt
from riverhog_core.ports.archive_store import (
    CollectionDescriptionReceipt,
    CollectionTagObjectReceipt,
)

Role = Literal["description", "tag-head", "tag-node"]


@dataclass(frozen=True, slots=True)
class ArchiveCollectionMetadataPublication:
    """One format's plaintext, protected path and complete storage assertions."""

    archive_storage_prefix: str
    role: Role
    plaintext: bytes = field(repr=False)
    passphrase_id: str
    relative_path: str = field(init=False)
    storage_format: str = field(init=False)
    content_type: str = field(init=False)
    plaintext_sha256: str = field(init=False)
    _identity_items: tuple[tuple[str, str], ...] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.plaintext, bytes):
            raise TypeError("collection metadata plaintext must be immutable bytes")
        prefix = self.archive_storage_prefix.strip("/")
        try:
            validate_canonical_relpath(prefix)
        except ValueError as exc:
            raise ValueError("archive storage prefix is not canonical") from exc
        if self.role == "description":
            document = CollectionDescriptionDocument.from_json_bytes(self.plaintext)
            relative_path = COLLECTION_DESCRIPTION_RELATIVE_PATH
            storage_format: str = COLLECTION_DESCRIPTION_DOCUMENT_FORMAT
            content_type = "application/vnd.riverhog.collection-description.v1+age"
            identity = {
                "riverhog-archive-root-sha256": document.archive_root_sha256,
                "riverhog-description-identity": document.description_identity,
                "riverhog-description-revision": str(document.revision),
            }
        elif self.role == "tag-head":
            head = CollectionTagHeadDocument.from_json_bytes(self.plaintext)
            relative_path = COLLECTION_TAG_HEAD_RELATIVE_PATH
            storage_format = COLLECTION_TAG_HEAD_FORMAT
            content_type = "application/vnd.riverhog.collection-tag-head.v1+age"
            identity = {
                "riverhog-archive-root-sha256": head.archive_root_sha256,
                "riverhog-tag-head-identity": head.head_identity,
                "riverhog-tag-set-identity": head.tag_set_identity,
                "riverhog-tag-revision": str(head.revision),
            }
        elif self.role == "tag-node":
            decode_collection_tag_node(self.plaintext)
            digest = collection_tag_node_digest(self.plaintext)
            relative_path = collection_tag_node_path(digest)
            storage_format = COLLECTION_TAG_NODE_FORMAT
            content_type = "application/vnd.riverhog.collection-tag-node.v1+age"
            identity = {"riverhog-tag-node-sha256": digest}
        else:
            raise ValueError("unsupported collection metadata role")
        plaintext_sha256 = hashlib.sha256(self.plaintext).hexdigest()
        identity.update(
            {
                "riverhog-format": storage_format,
                "riverhog-encryption": "age-v1-scrypt",
                "riverhog-passphrase-id": self.passphrase_id,
                "riverhog-plaintext-bytes": str(len(self.plaintext)),
                "riverhog-plaintext-sha256": plaintext_sha256,
            }
        )
        object.__setattr__(self, "archive_storage_prefix", prefix)
        object.__setattr__(self, "relative_path", relative_path)
        object.__setattr__(self, "storage_format", storage_format)
        object.__setattr__(self, "content_type", content_type)
        object.__setattr__(self, "plaintext_sha256", plaintext_sha256)
        object.__setattr__(self, "_identity_items", tuple(sorted(identity.items())))

    @property
    def object_path(self) -> str:
        return f"{self.archive_storage_prefix}/{self.relative_path}"

    @property
    def required_identity_assertions(self) -> dict[str, str]:
        return dict(self._identity_items)

    def request(
        self, content: bytes, *, expected_current_stored_sha256: str | None = None
    ) -> SmallObjectWriteRequest:
        if self.role == "tag-node" and expected_current_stored_sha256 is not None:
            raise ValueError("collection tag nodes are immutable")
        return SmallObjectWriteRequest(
            object_path=self.object_path,
            content_type=self.content_type,
            required_identity_assertions=self.required_identity_assertions,
            placement_policy="immediate_default",
            mode="create_only" if expected_current_stored_sha256 is None else "replace_current",
            expected_current_stored_sha256=expected_current_stored_sha256,
            stored_bytes=len(content),
            stored_sha256=hashlib.sha256(content).hexdigest(),
        )

    def require_request(
        self,
        request: SmallObjectWriteRequest,
        *,
        expected_current_stored_sha256: str | None = None,
    ) -> None:
        if self.role == "tag-node" and expected_current_stored_sha256 is not None:
            raise ValueError("collection tag nodes are immutable")
        if (
            request.object_path != self.object_path
            or request.content_type != self.content_type
            or request.required_identity_assertions != self.required_identity_assertions
            or request.placement_policy != "immediate_default"
            or request.mode
            != ("create_only" if expected_current_stored_sha256 is None else "replace_current")
            or request.expected_current_stored_sha256 != expected_current_stored_sha256
        ):
            raise ValueError("collection metadata request differs from its source")

    def receipt(
        self, receipt: ImmutableObjectReceipt | AdapterReceipt | ObjectMetadataReceipt
    ) -> CollectionDescriptionReceipt | CollectionTagObjectReceipt:
        if (
            receipt.object_path != self.object_path
            or receipt.stored_bytes <= 0
            or not receipt.stored_sha256
        ):
            raise RuntimeError("collection metadata store returned an inconsistent receipt")
        receipt_type = (
            CollectionDescriptionReceipt
            if self.role == "description"
            else CollectionTagObjectReceipt
        )
        return receipt_type(
            object_path=receipt.object_path,
            revision=receipt.revision,
            stored_bytes=receipt.stored_bytes,
            stored_sha256=receipt.stored_sha256,
            published_at=receipt.completed_at,
        )


class ArchiveCollectionMetadataPublisher:
    """Publish one create-only object through its selected immutable owner."""

    def __init__(
        self, *, object_store: ImmutableArchiveObjectStore, passphrase: str, scrypt_log_n: int
    ) -> None:
        if not passphrase:
            raise ValueError("archive passphrase must not be empty")
        self._object_store = object_store
        self._passphrase = passphrase
        self._scrypt_log_n = scrypt_log_n

    def publish_object(
        self, publication: ArchiveCollectionMetadataPublication
    ) -> CollectionDescriptionReceipt | CollectionTagObjectReceipt:
        ciphertext = encrypt_age_scrypt(
            publication.plaintext, self._passphrase, log_n=self._scrypt_log_n
        )
        request = publication.request(ciphertext)
        return publication.receipt(
            self._object_store.put_immutable_object(
                object_path=request.object_path,
                content=ciphertext,
                content_type=request.content_type,
                required_identity_assertions=dict(request.required_identity_assertions),
                placement_policy=request.placement_policy,
            )
        )
