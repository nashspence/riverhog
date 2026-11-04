from __future__ import annotations

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass, field

from riverhog_age import encrypt_age_scrypt
from riverhog_archive_contracts import (
    CollectionArchiveManifest,
    CollectionArchiveTerminalDocument,
    CollectionArchiveVolumeDocument,
    format_archive_sequence,
)
from riverhog_protocol.paths import validate_canonical_relpath
from riverhog_storage_adapter_protocol import SmallObjectWriteRequest

from riverhog_core.archive_formats import (
    ROOT_MANIFEST_STORAGE_FORMAT,
    VOLUME_METADATA_STORAGE_FORMAT,
)
from riverhog_core.archive_manifest import (
    build_collection_archive_authority,
    build_collection_archive_terminal_document,
    collection_artifact_set_identity,
)
from riverhog_core.domain.archive import (
    ArchiveArtifact,
    PackVolumePlan,
    SealedPackVolume,
    SealedProvenanceObject,
    SealedRawVolume,
    VerifiedRawArtifact,
)
from riverhog_core.ports.archive_objects import ImmutableArchiveObjectStore, ImmutableObjectReceipt

ROOT_MANIFEST_RELATIVE_PATH = "manifest.json.age"
ROOT_MANIFEST_CONTENT_TYPE = "application/vnd.riverhog.collection-manifest+age"
VOLUME_METADATA_CONTENT_TYPE = "application/vnd.riverhog.collection-volume+age"


@dataclass(frozen=True, slots=True)
class SealedArchiveVolumeMetadata:
    sequence: int
    object_path: str
    relative_path: str
    revision: str | None
    plaintext_bytes: int
    plaintext_sha256: str
    stored_bytes: int
    stored_sha256: str
    completed_at: str


@dataclass(frozen=True, slots=True)
class ArchiveVolumeMetadataPublication:
    """Format-owned plaintext identity, write request and sealed receipt projection."""

    archive_storage_prefix: str
    sequence: int
    plaintext: bytes = field(repr=False)
    plaintext_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        prefix = self.archive_storage_prefix.strip("/")
        if not prefix:
            raise ValueError("archive storage prefix must not be empty")
        format_archive_sequence(self.sequence)
        if not isinstance(self.plaintext, bytes):
            raise TypeError("archive volume metadata plaintext must be immutable bytes")
        object.__setattr__(self, "archive_storage_prefix", prefix)
        object.__setattr__(self, "plaintext_sha256", hashlib.sha256(self.plaintext).hexdigest())

    @classmethod
    def from_document(
        cls,
        *,
        archive_storage_prefix: str,
        document: CollectionArchiveVolumeDocument | CollectionArchiveTerminalDocument,
    ) -> ArchiveVolumeMetadataPublication:
        sequence = (
            document.volume.sequence
            if isinstance(document, CollectionArchiveVolumeDocument)
            else document.sequence
        )
        return cls(archive_storage_prefix, sequence, document.to_json_bytes())

    @property
    def relative_path(self) -> str:
        return f"metadata/volume-{format_archive_sequence(self.sequence)}.json.age"

    @property
    def object_path(self) -> str:
        return f"{self.archive_storage_prefix}/{self.relative_path}"

    @property
    def required_identity_assertions(self) -> dict[str, str]:
        return {
            "riverhog-format": VOLUME_METADATA_STORAGE_FORMAT,
            "riverhog-plaintext-bytes": str(len(self.plaintext)),
            "riverhog-plaintext-sha256": self.plaintext_sha256,
            "riverhog-volume-sequence": format_archive_sequence(self.sequence),
        }

    def request(self, content: bytes) -> SmallObjectWriteRequest:
        return SmallObjectWriteRequest(
            object_path=self.object_path,
            content_type=VOLUME_METADATA_CONTENT_TYPE,
            required_identity_assertions=self.required_identity_assertions,
            placement_policy="immediate_default",
            mode="create_only",
            stored_bytes=len(content),
            stored_sha256=hashlib.sha256(content).hexdigest(),
        )

    def require_request(self, request: SmallObjectWriteRequest) -> None:
        """Check plaintext format identity; preparation owns exact ciphertext."""
        if (
            request.object_path != self.object_path
            or request.content_type != VOLUME_METADATA_CONTENT_TYPE
            or request.required_identity_assertions != self.required_identity_assertions
            or request.placement_policy != "immediate_default"
            or request.mode != "create_only"
            or request.expected_current_stored_sha256 is not None
        ):
            raise ValueError("archive volume metadata request differs from its source")

    def sealed_receipt(self, receipt: ImmutableObjectReceipt) -> SealedArchiveVolumeMetadata:
        if receipt.object_path != self.object_path or receipt.stored_bytes <= 0:
            raise RuntimeError("immutable volume metadata store returned an inconsistent receipt")
        return SealedArchiveVolumeMetadata(
            sequence=self.sequence,
            object_path=receipt.object_path,
            relative_path=self.relative_path,
            revision=receipt.revision,
            plaintext_bytes=len(self.plaintext),
            plaintext_sha256=self.plaintext_sha256,
            stored_bytes=receipt.stored_bytes,
            stored_sha256=receipt.stored_sha256,
            completed_at=receipt.completed_at,
        )


@dataclass(frozen=True, slots=True)
class SealedArchiveRoot:
    object_path: str
    relative_path: str
    revision: str | None
    plaintext_bytes: int
    plaintext_sha256: str
    stored_bytes: int
    stored_sha256: str
    artifact_set_sha256: str
    artifact_count: int
    artifact_bytes: int
    completed_at: str
    manifest_bytes: bytes
    volume_metadata: tuple[SealedArchiveVolumeMetadata, ...]


class ArchiveRootPublisher:
    """Write the immutable root once after all referenced volumes are sealed.

    Idempotency is based on the plaintext manifest digest, not randomized age ciphertext.
    The object-store adapter may therefore recover a successful earlier write after a lost
    response without replacing the object.
    """

    def __init__(
        self,
        *,
        object_store: ImmutableArchiveObjectStore,
        passphrase: str,
        scrypt_log_n: int,
    ) -> None:
        if not passphrase:
            raise ValueError("archive passphrase must not be empty")
        self._object_store = object_store
        self._passphrase = passphrase
        self._scrypt_log_n = scrypt_log_n

    def publish(
        self,
        *,
        archive_generation: str,
        archive_storage_prefix: str,
        artifacts: Sequence[ArchiveArtifact],
        packs: Sequence[tuple[PackVolumePlan, SealedPackVolume]],
        provenance_identity: str,
        raw_volumes: Sequence[SealedRawVolume] = (),
        verified_raw_artifacts: Sequence[VerifiedRawArtifact] = (),
        provenance_objects: Sequence[SealedProvenanceObject] = (),
    ) -> SealedArchiveRoot:
        prefix = archive_storage_prefix.strip("/")
        if not prefix:
            raise ValueError("archive storage prefix must not be empty")
        manifest, volume_documents = build_collection_archive_authority(
            archive_generation=archive_generation,
            artifacts=artifacts,
            packs=packs,
            raw_volumes=raw_volumes,
            verified_raw_artifacts=verified_raw_artifacts,
            provenance_identity=provenance_identity,
            provenance_objects=provenance_objects,
        )
        plaintext_sha256 = hashlib.sha256(manifest).hexdigest()
        artifact_set = collection_artifact_set_identity(artifacts)
        sealed_metadata: list[SealedArchiveVolumeMetadata] = []
        for document in volume_documents:
            sealed_metadata.append(
                self.publish_volume_metadata(archive_storage_prefix=prefix, document=document)
            )
        terminal = build_collection_archive_terminal_document(
            archive_generation=archive_generation,
            artifact_set_sha256=str(artifact_set["sha256"]),
            sequence=len(volume_documents),
        )
        sealed_metadata.append(
            self.publish_terminal_metadata(
                archive_storage_prefix=prefix,
                document=terminal,
            )
        )
        ciphertext = encrypt_age_scrypt(
            manifest,
            self._passphrase,
            log_n=self._scrypt_log_n,
        )
        object_path = f"{prefix}/{ROOT_MANIFEST_RELATIVE_PATH}"
        receipt = self._object_store.put_immutable_object(
            object_path=object_path,
            content=ciphertext,
            content_type=ROOT_MANIFEST_CONTENT_TYPE,
            required_identity_assertions={
                "riverhog-format": ROOT_MANIFEST_STORAGE_FORMAT,
                "riverhog-plaintext-bytes": str(len(manifest)),
                "riverhog-plaintext-sha256": plaintext_sha256,
                "riverhog-artifact-set-sha256": str(artifact_set["sha256"]),
            },
            placement_policy="immediate_default",
        )
        if receipt.object_path != object_path or receipt.stored_bytes <= 0:
            raise RuntimeError("immutable root store returned an inconsistent receipt")
        return SealedArchiveRoot(
            object_path=receipt.object_path,
            relative_path=validate_canonical_relpath(ROOT_MANIFEST_RELATIVE_PATH),
            revision=receipt.revision,
            plaintext_bytes=len(manifest),
            plaintext_sha256=plaintext_sha256,
            stored_bytes=receipt.stored_bytes,
            stored_sha256=receipt.stored_sha256,
            artifact_set_sha256=str(artifact_set["sha256"]),
            artifact_count=artifact_set["count"],
            artifact_bytes=artifact_set["bytes"],
            completed_at=receipt.completed_at,
            manifest_bytes=manifest,
            volume_metadata=tuple(sealed_metadata),
        )

    def publish_volume_metadata(
        self,
        *,
        archive_storage_prefix: str,
        document: CollectionArchiveVolumeDocument,
    ) -> SealedArchiveVolumeMetadata:
        """Publish one bounded volume document before the immutable root exists."""

        return self._publish_volume_metadata_document(
            ArchiveVolumeMetadataPublication.from_document(
                archive_storage_prefix=archive_storage_prefix, document=document
            )
        )

    def publish_terminal_metadata(
        self,
        *,
        archive_storage_prefix: str,
        document: CollectionArchiveTerminalDocument,
    ) -> SealedArchiveVolumeMetadata:
        """Publish the bounded authenticated terminator before the root."""
        return self._publish_volume_metadata_document(
            ArchiveVolumeMetadataPublication.from_document(
                archive_storage_prefix=archive_storage_prefix, document=document
            )
        )

    def _publish_volume_metadata_document(
        self, publication: ArchiveVolumeMetadataPublication
    ) -> SealedArchiveVolumeMetadata:
        content = encrypt_age_scrypt(
            publication.plaintext, self._passphrase, log_n=self._scrypt_log_n
        )
        request = publication.request(content)
        receipt = self._object_store.put_immutable_object(
            object_path=request.object_path,
            content=content,
            content_type=request.content_type,
            required_identity_assertions=request.required_identity_assertions,
            placement_policy=request.placement_policy,
        )
        return publication.sealed_receipt(receipt)

    def publish_root_manifest(
        self,
        *,
        archive_storage_prefix: str,
        manifest: bytes,
    ) -> SealedArchiveRoot:
        """Publish only the small final authority after its exact sequence is durable."""

        prefix = archive_storage_prefix.strip("/")
        if not prefix:
            raise ValueError("archive storage prefix must not be empty")
        authority = CollectionArchiveManifest.from_json_bytes(manifest)
        plaintext_sha256 = hashlib.sha256(manifest).hexdigest()
        object_path = f"{prefix}/{ROOT_MANIFEST_RELATIVE_PATH}"
        receipt = self._object_store.put_immutable_object(
            object_path=object_path,
            content=encrypt_age_scrypt(
                manifest,
                self._passphrase,
                log_n=self._scrypt_log_n,
            ),
            content_type=ROOT_MANIFEST_CONTENT_TYPE,
            required_identity_assertions={
                "riverhog-format": ROOT_MANIFEST_STORAGE_FORMAT,
                "riverhog-plaintext-bytes": str(len(manifest)),
                "riverhog-plaintext-sha256": plaintext_sha256,
                "riverhog-artifact-set-sha256": authority.artifact_set.sha256,
            },
            placement_policy="immediate_default",
        )
        if receipt.object_path != object_path or receipt.stored_bytes <= 0:
            raise RuntimeError("immutable root store returned an inconsistent receipt")
        return SealedArchiveRoot(
            object_path=receipt.object_path,
            relative_path=validate_canonical_relpath(ROOT_MANIFEST_RELATIVE_PATH),
            revision=receipt.revision,
            plaintext_bytes=len(manifest),
            plaintext_sha256=plaintext_sha256,
            stored_bytes=receipt.stored_bytes,
            stored_sha256=receipt.stored_sha256,
            artifact_set_sha256=authority.artifact_set.sha256,
            artifact_count=authority.artifact_set.count,
            artifact_bytes=authority.artifact_set.bytes,
            completed_at=receipt.completed_at,
            manifest_bytes=manifest,
            volume_metadata=(),
        )
