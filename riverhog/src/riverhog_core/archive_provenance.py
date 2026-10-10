from __future__ import annotations

import hashlib
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from dataclasses import dataclass

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
from riverhog_core.ports.archive_objects import ImmutableArchiveObjectStore
from riverhog_core.throughput import ArchiveTransferResources


@dataclass(frozen=True, slots=True)
class SealedArchiveProvenanceVolume:
    sequence: int
    payload: SealedProvenanceObject
    metadata: SealedProvenanceObject


@dataclass(frozen=True, slots=True)
class SealedArchiveProvenance:
    identity: str
    root: SealedProvenanceObject


class ArchiveProvenancePublisher:
    """Publish one bounded provenance object at a time before its immutable root."""

    def __init__(
        self,
        *,
        object_store: ImmutableArchiveObjectStore,
        passphrase: str,
        scrypt_log_n: int,
        transfer_resources: ArchiveTransferResources | None = None,
    ) -> None:
        if not passphrase:
            raise ValueError("archive passphrase must not be empty")
        self._object_store = object_store
        self._passphrase = passphrase
        self._scrypt_log_n = scrypt_log_n
        self._resources = transfer_resources

    def publish_volume(
        self,
        *,
        archive_storage_prefix: str,
        document: ProvenanceVolumeDocument,
        payload: bytes,
    ) -> SealedArchiveProvenanceVolume:
        prefix = _prefix(archive_storage_prefix)
        if (
            len(payload) != document.payload.bytes
            or hashlib.sha256(payload).hexdigest() != document.payload.sha256
        ):
            raise ValueError("provenance payload identity changed before publication")
        with ThreadPoolExecutor(max_workers=2) as executor:
            payload_future = executor.submit(
                self._put,
                prefix=prefix,
                object_id=f"provenance-payload-{document.payload.sha256}",
                kind=(
                    "provenance-bindings"
                    if document.payload.kind == "bindings"
                    else "provenance-journal-segment"
                ),
                relative_path=document.payload.path,
                content=payload,
                storage_format=(
                    PROVENANCE_BINDING_SEGMENT_STORAGE_FORMAT
                    if document.payload.kind == "bindings"
                    else PROVENANCE_JOURNAL_SEGMENT_STORAGE_FORMAT
                ),
            )
            metadata_future = executor.submit(
                self._put,
                prefix=prefix,
                object_id=f"provenance-volume-{format_archive_sequence(document.sequence)}",
                kind="provenance-volume-metadata",
                relative_path=document.metadata_path,
                content=document.to_json_bytes(),
                storage_format=PROVENANCE_VOLUME_METADATA_STORAGE_FORMAT,
            )
            payload_object = payload_future.result()
            metadata_object = metadata_future.result()
        return SealedArchiveProvenanceVolume(
            sequence=document.sequence,
            payload=payload_object,
            metadata=metadata_object,
        )

    def publish_journal_segment(
        self, *, archive_storage_prefix: str, content: bytes
    ) -> SealedProvenanceObject:
        """Durably retain a bounded segment before an early primary custody receipt.

        Final corpus metadata refers to these same encrypted octets by digest;
        physical custody does not require a future collection or provenance root.
        """
        if not 1 <= len(content) <= PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX:
            raise ValueError("canonical custody segment exceeds its bounded byte contract")
        digest = hashlib.sha256(content).hexdigest()
        return self._put(
            prefix=_prefix(archive_storage_prefix),
            object_id="provenance-payload-" + digest,
            kind="provenance-journal-segment",
            relative_path=provenance_payload_object_path(digest),
            content=content,
            storage_format=PROVENANCE_JOURNAL_SEGMENT_STORAGE_FORMAT,
        )

    def publish_member_history(
        self,
        *,
        archive_storage_prefix: str,
        binding: MemberHistoryBinding,
        content: bytes,
    ) -> SealedProvenanceObject:
        """Seal the exact descriptor selected by a final member binding."""

        binding.verify_descriptor(content)
        return self._put(
            prefix=_prefix(archive_storage_prefix),
            object_id="provenance-history-" + binding.history_sha256,
            kind="provenance-history",
            relative_path=member_history_object_path(binding.history_sha256),
            content=content,
            storage_format=PROVENANCE_HISTORY_STORAGE_FORMAT,
        )

    def publish_record_page(
        self,
        *,
        archive_storage_prefix: str,
        page: RecordPage,
    ) -> SealedProvenanceObject:
        """Seal one bounded page; the caller verifies the complete set and terminal."""

        return self._put(
            prefix=_prefix(archive_storage_prefix),
            object_id=(
                "provenance-record-page-"
                + page.authority.records_sha256
                + "-"
                + format_archive_sequence(page.ordinal)
            ),
            kind="provenance-record-page",
            relative_path=history_record_page_object_path(
                page.authority.records_sha256, page.ordinal
            ),
            content=page.to_json_bytes(),
            storage_format=PROVENANCE_RECORD_PAGE_STORAGE_FORMAT,
        )

    def publish_source_binding_proof(
        self,
        *,
        archive_storage_prefix: str,
        proof: SourceMemberHistoryBindingProof,
    ) -> SealedProvenanceObject:
        return self._put(
            prefix=_prefix(archive_storage_prefix),
            object_id="provenance-source-proof-" + proof.identity,
            kind="provenance-source-proof",
            relative_path=source_binding_proof_object_path(proof.identity),
            content=proof.to_json_bytes(),
            storage_format=PROVENANCE_SOURCE_PROOF_STORAGE_FORMAT,
        )

    def publish_root(
        self,
        *,
        archive_storage_prefix: str,
        root: ProvenanceRootDocument,
    ) -> SealedArchiveProvenance:
        sealed = self._put(
            prefix=_prefix(archive_storage_prefix),
            object_id="provenance-root",
            kind="provenance-root",
            relative_path="provenance/root.json.age",
            content=root.to_json_bytes(),
            storage_format=PROVENANCE_ROOT_STORAGE_FORMAT,
        )
        if sealed.plaintext_sha256 != root.identity:
            raise RuntimeError("published provenance root identity changed")
        return SealedArchiveProvenance(identity=root.identity, root=sealed)

    def publish_terminal(
        self,
        *,
        archive_storage_prefix: str,
        terminal: ProvenanceTerminalDocument,
    ) -> SealedProvenanceObject:
        """Publish the authenticated sequence terminator before the root."""

        return self._put(
            prefix=_prefix(archive_storage_prefix),
            object_id=f"provenance-terminal-{format_archive_sequence(terminal.sequence)}",
            kind="provenance-terminal",
            relative_path=terminal.metadata_path,
            content=terminal.to_json_bytes(),
            storage_format=PROVENANCE_TERMINAL_STORAGE_FORMAT,
        )

    def _put(
        self,
        *,
        prefix: str,
        object_id: str,
        kind: str,
        relative_path: str,
        content: bytes,
        storage_format: str,
    ) -> SealedProvenanceObject:
        plaintext_sha256 = hashlib.sha256(content).hexdigest()

        def encrypt() -> bytes:
            with (
                nullcontext()
                if self._resources is None
                else self._resources.age_derivations.reserve()
            ):
                return encrypt_age_scrypt(content, self._passphrase, log_n=self._scrypt_log_n)

        media_type = storage_format.replace("/", ".").replace("+", ".")
        with (
            nullcontext() if self._resources is None else self._resources.upload_requests.reserve()
        ):
            receipt = self._object_store.put_immutable_object(
                object_path=f"{prefix}/{relative_path}",
                content=encrypt,
                content_type=f"application/vnd.{media_type}",
                required_identity_assertions={
                    "riverhog-format": storage_format,
                    "riverhog-plaintext-bytes": str(len(content)),
                    "riverhog-plaintext-sha256": plaintext_sha256,
                },
                placement_policy="immediate_default",
            )
        return SealedProvenanceObject(
            object_id=object_id,
            kind=kind,
            relative_path=relative_path,
            plaintext_bytes=len(content),
            plaintext_sha256=plaintext_sha256,
            stored_bytes=receipt.stored_bytes,
            stored_sha256=receipt.stored_sha256,
            revision=receipt.revision,
            completed_at=receipt.completed_at,
        )


def _prefix(value: str) -> str:
    prefix = value.strip("/")
    if not prefix:
        raise ValueError("archive storage prefix must not be empty")
    return prefix


__all__ = [
    "ArchiveProvenancePublisher",
    "SealedArchiveProvenance",
    "SealedArchiveProvenanceVolume",
]
