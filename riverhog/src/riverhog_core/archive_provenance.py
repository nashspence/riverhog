from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

from riverhog_age import encrypt_age_scrypt
from riverhog_archive_contracts import (
    PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX,
    MemberHistoryBinding,
    ProvenanceRootDocument,
    ProvenanceTerminalDocument,
    ProvenanceVolumeDocument,
    RecordPage,
    SourceMemberHistoryBindingProof,
    format_archive_sequence,
    history_record_page_object_path,
    member_history_object_path,
    provenance_payload_object_path,
    source_binding_proof_object_path,
)
from riverhog_protocol.paths import validate_canonical_relpath
from riverhog_storage_adapter_protocol import SmallObjectWriteRequest

from riverhog_core.archive_formats import (
    PROVENANCE_BINDING_SEGMENT_STORAGE_FORMAT,
    PROVENANCE_HISTORY_STORAGE_FORMAT,
    PROVENANCE_JOURNAL_SEGMENT_STORAGE_FORMAT,
    PROVENANCE_RECORD_PAGE_STORAGE_FORMAT,
    PROVENANCE_ROOT_STORAGE_FORMAT,
    PROVENANCE_SOURCE_PROOF_STORAGE_FORMAT,
    PROVENANCE_TERMINAL_STORAGE_FORMAT,
    PROVENANCE_VOLUME_METADATA_STORAGE_FORMAT,
)
from riverhog_core.domain.archive import SealedProvenanceObject
from riverhog_core.ports.archive_objects import ImmutableArchiveObjectStore, ImmutableObjectReceipt


@dataclass(frozen=True, slots=True)
class SealedArchiveProvenanceVolume:
    sequence: int
    payload: SealedProvenanceObject
    metadata: SealedProvenanceObject


@dataclass(frozen=True, slots=True)
class SealedArchiveProvenance:
    identity: str
    root: SealedProvenanceObject


@dataclass(frozen=True, slots=True)
class ArchiveProvenancePublication:
    """One format-owned logical object, independent of randomized encryption."""

    archive_storage_prefix: str
    object_id: str
    kind: str
    relative_path: str
    plaintext: bytes = field(repr=False)
    storage_format: str
    plaintext_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.plaintext, bytes):
            raise TypeError("archive provenance plaintext must be immutable bytes")
        object.__setattr__(self, "archive_storage_prefix", _prefix(self.archive_storage_prefix))
        validate_canonical_relpath(self.relative_path)
        object.__setattr__(self, "plaintext_sha256", hashlib.sha256(self.plaintext).hexdigest())

    @classmethod
    def from_volume_payload(
        cls, *, archive_storage_prefix: str, document: ProvenanceVolumeDocument, payload: bytes
    ) -> ArchiveProvenancePublication:
        if (
            len(payload) != document.payload.bytes
            or hashlib.sha256(payload).hexdigest() != document.payload.sha256
        ):
            raise ValueError("provenance payload identity changed before publication")
        bindings = document.payload.kind == "bindings"
        return cls(
            archive_storage_prefix,
            f"provenance-payload-{document.payload.sha256}",
            "provenance-bindings" if bindings else "provenance-journal-segment",
            document.payload.path,
            payload,
            PROVENANCE_BINDING_SEGMENT_STORAGE_FORMAT
            if bindings
            else PROVENANCE_JOURNAL_SEGMENT_STORAGE_FORMAT,
        )

    @classmethod
    def from_volume_metadata(
        cls, *, archive_storage_prefix: str, document: ProvenanceVolumeDocument
    ) -> ArchiveProvenancePublication:
        return cls(
            archive_storage_prefix,
            f"provenance-volume-{format_archive_sequence(document.sequence)}",
            "provenance-volume-metadata",
            document.metadata_path,
            document.to_json_bytes(),
            PROVENANCE_VOLUME_METADATA_STORAGE_FORMAT,
        )

    @classmethod
    def from_journal_segment(
        cls, *, archive_storage_prefix: str, content: bytes
    ) -> ArchiveProvenancePublication:
        if not 1 <= len(content) <= PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX:
            raise ValueError("canonical custody segment exceeds its bounded byte contract")
        digest = hashlib.sha256(content).hexdigest()
        return cls(
            archive_storage_prefix,
            "provenance-payload-" + digest,
            "provenance-journal-segment",
            provenance_payload_object_path(digest),
            content,
            PROVENANCE_JOURNAL_SEGMENT_STORAGE_FORMAT,
        )

    @classmethod
    def from_member_history(
        cls, *, archive_storage_prefix: str, binding: MemberHistoryBinding, content: bytes
    ) -> ArchiveProvenancePublication:
        binding.verify_descriptor(content)
        return cls(
            archive_storage_prefix,
            "provenance-history-" + binding.history_sha256,
            "provenance-history",
            member_history_object_path(binding.history_sha256),
            content,
            PROVENANCE_HISTORY_STORAGE_FORMAT,
        )

    @classmethod
    def from_record_page(
        cls, *, archive_storage_prefix: str, page: RecordPage
    ) -> ArchiveProvenancePublication:
        return cls(
            archive_storage_prefix,
            "provenance-record-page-"
            + page.authority.records_sha256
            + "-"
            + format_archive_sequence(page.ordinal),
            "provenance-record-page",
            history_record_page_object_path(page.authority.records_sha256, page.ordinal),
            page.to_json_bytes(),
            PROVENANCE_RECORD_PAGE_STORAGE_FORMAT,
        )

    @classmethod
    def from_source_binding_proof(
        cls, *, archive_storage_prefix: str, proof: SourceMemberHistoryBindingProof
    ) -> ArchiveProvenancePublication:
        return cls(
            archive_storage_prefix,
            "provenance-source-proof-" + proof.identity,
            "provenance-source-proof",
            source_binding_proof_object_path(proof.identity),
            proof.to_json_bytes(),
            PROVENANCE_SOURCE_PROOF_STORAGE_FORMAT,
        )

    @classmethod
    def from_root(
        cls, *, archive_storage_prefix: str, root: ProvenanceRootDocument
    ) -> ArchiveProvenancePublication:
        return cls(
            archive_storage_prefix,
            "provenance-root",
            "provenance-root",
            "provenance/root.json.age",
            root.to_json_bytes(),
            PROVENANCE_ROOT_STORAGE_FORMAT,
        )

    @classmethod
    def from_terminal(
        cls, *, archive_storage_prefix: str, terminal: ProvenanceTerminalDocument
    ) -> ArchiveProvenancePublication:
        return cls(
            archive_storage_prefix,
            f"provenance-terminal-{format_archive_sequence(terminal.sequence)}",
            "provenance-terminal",
            terminal.metadata_path,
            terminal.to_json_bytes(),
            PROVENANCE_TERMINAL_STORAGE_FORMAT,
        )

    @property
    def object_path(self) -> str:
        return f"{self.archive_storage_prefix}/{self.relative_path}"

    @property
    def content_type(self) -> str:
        return f"application/vnd.{self.storage_format.replace('/', '.').replace('+', '.')}"

    @property
    def required_identity_assertions(self) -> dict[str, str]:
        return {
            "riverhog-format": self.storage_format,
            "riverhog-plaintext-bytes": str(len(self.plaintext)),
            "riverhog-plaintext-sha256": self.plaintext_sha256,
        }

    def request(self, content: bytes) -> SmallObjectWriteRequest:
        return SmallObjectWriteRequest(
            object_path=self.object_path,
            content_type=self.content_type,
            required_identity_assertions=self.required_identity_assertions,
            placement_policy="immediate_default",
            mode="create_only",
            stored_bytes=len(content),
            stored_sha256=hashlib.sha256(content).hexdigest(),
        )

    def require_request(self, request: SmallObjectWriteRequest) -> None:
        # Preparation retains exact original ciphertext; compatible observations
        # may identify an earlier randomized encoding of the same plaintext.
        if (
            request.object_path != self.object_path
            or request.content_type != self.content_type
            or request.required_identity_assertions != self.required_identity_assertions
            or request.placement_policy != "immediate_default"
            or request.mode != "create_only"
            or request.expected_current_stored_sha256 is not None
        ):
            raise ValueError("archive provenance request differs from its source")

    def sealed_receipt(self, receipt: ImmutableObjectReceipt) -> SealedProvenanceObject:
        if receipt.object_path != self.object_path or receipt.stored_bytes <= 0:
            raise RuntimeError("immutable provenance store returned an inconsistent receipt")
        return SealedProvenanceObject(
            object_id=self.object_id,
            kind=self.kind,
            relative_path=self.relative_path,
            plaintext_bytes=len(self.plaintext),
            plaintext_sha256=self.plaintext_sha256,
            stored_bytes=receipt.stored_bytes,
            stored_sha256=receipt.stored_sha256,
            revision=receipt.revision,
            completed_at=receipt.completed_at,
        )


class ArchiveProvenancePublisher:
    """Publish bounded provenance objects before their immutable root."""

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

    def publish_volume(
        self,
        *,
        archive_storage_prefix: str,
        document: ProvenanceVolumeDocument,
        payload: bytes,
    ) -> SealedArchiveProvenanceVolume:
        payload_publication = ArchiveProvenancePublication.from_volume_payload(
            archive_storage_prefix=archive_storage_prefix, document=document, payload=payload
        )
        metadata_publication = ArchiveProvenancePublication.from_volume_metadata(
            archive_storage_prefix=archive_storage_prefix, document=document
        )
        return SealedArchiveProvenanceVolume(
            sequence=document.sequence,
            payload=self.publish_object(payload_publication),
            metadata=self.publish_object(metadata_publication),
        )

    def publish_journal_segment(
        self, *, archive_storage_prefix: str, content: bytes
    ) -> SealedProvenanceObject:
        """Retain one bounded segment before an early primary custody receipt."""
        return self.publish_object(
            ArchiveProvenancePublication.from_journal_segment(
                archive_storage_prefix=archive_storage_prefix, content=content
            )
        )

    def publish_member_history(
        self,
        *,
        archive_storage_prefix: str,
        binding: MemberHistoryBinding,
        content: bytes,
    ) -> SealedProvenanceObject:
        return self.publish_object(
            ArchiveProvenancePublication.from_member_history(
                archive_storage_prefix=archive_storage_prefix, binding=binding, content=content
            )
        )

    def publish_record_page(
        self, *, archive_storage_prefix: str, page: RecordPage
    ) -> SealedProvenanceObject:
        return self.publish_object(
            ArchiveProvenancePublication.from_record_page(
                archive_storage_prefix=archive_storage_prefix, page=page
            )
        )

    def publish_source_binding_proof(
        self, *, archive_storage_prefix: str, proof: SourceMemberHistoryBindingProof
    ) -> SealedProvenanceObject:
        return self.publish_object(
            ArchiveProvenancePublication.from_source_binding_proof(
                archive_storage_prefix=archive_storage_prefix, proof=proof
            )
        )

    def publish_root(
        self, *, archive_storage_prefix: str, root: ProvenanceRootDocument
    ) -> SealedArchiveProvenance:
        sealed = self.publish_object(
            ArchiveProvenancePublication.from_root(
                archive_storage_prefix=archive_storage_prefix, root=root
            )
        )
        if sealed.plaintext_sha256 != root.identity:
            raise RuntimeError("published provenance root identity changed")
        return SealedArchiveProvenance(identity=root.identity, root=sealed)

    def publish_terminal(
        self, *, archive_storage_prefix: str, terminal: ProvenanceTerminalDocument
    ) -> SealedProvenanceObject:
        return self.publish_object(
            ArchiveProvenancePublication.from_terminal(
                archive_storage_prefix=archive_storage_prefix, terminal=terminal
            )
        )

    def publish_object(self, publication: ArchiveProvenancePublication) -> SealedProvenanceObject:
        """Publish exactly one physical object from its format-owned identity."""
        ciphertext = encrypt_age_scrypt(
            publication.plaintext, self._passphrase, log_n=self._scrypt_log_n
        )
        request = publication.request(ciphertext)
        receipt = self._object_store.put_immutable_object(
            object_path=request.object_path,
            content=ciphertext,
            content_type=request.content_type,
            required_identity_assertions=dict(request.required_identity_assertions),
            placement_policy=request.placement_policy,
        )
        return publication.sealed_receipt(receipt)


def _prefix(value: str) -> str:
    prefix = value.strip("/")
    if not prefix:
        raise ValueError("archive storage prefix must not be empty")
    return prefix


__all__ = [
    "ArchiveProvenancePublication",
    "ArchiveProvenancePublisher",
    "SealedArchiveProvenance",
    "SealedArchiveProvenanceVolume",
]
