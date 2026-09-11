from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from pathlib import Path

from riverhog_age import encrypt_age_scrypt
from riverhog_archive_contracts import (
    ARCHIVE_ENCRYPTION_FORMAT,
    ArchiveRootCiphertextIdentity,
    CollectionEncryptionBinding,
    RecoveryDescriptor,
    format_archive_sequence,
)
from riverhog_core.archive_manifest import (
    build_collection_archive_authority,
    build_collection_archive_terminal_document,
)
from riverhog_core.domain.archive import (
    ArchiveFile,
    SealedPackVolume,
    SealedProvenanceObject,
    SealedRawVolume,
    StoredArchivePart,
    VerifiedRawFile,
)
from riverhog_core.pack_volume import iter_render_pack_upload_unit, plan_pack_volume
from riverhog_core.raw_verification import raw_file_ordered_volume_commitment
from riverhog_protocol import (
    COLLECTION_DESCRIPTION_RELATIVE_PATH,
    COLLECTION_TAG_HEAD_RELATIVE_PATH,
    CollectionDescriptionDocument,
    CollectionTagHeadDocument,
    CollectionTagSet,
    MemoryCollectionTagNodeStore,
    collection_tag_node_path,
)
from riverhog_provenance import (
    PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX,
    FileProvenanceBinding,
    ProvenancePayloadIdentity,
    ProvenanceRootDocument,
    ProvenanceTerminalDocument,
    ProvenanceVolumeDocument,
    binding_segment_bytes,
    create_observation_journal,
    format_provenance_sequence,
    update_ordered_volume_commitment,
    validate_journal,
)

from tests.fixtures.archive import age_state_json
from tests.provenance_observer import native_provenance_observer

PASSPHRASE = "correct horse battery archive"
PASSPHRASE_ID = "recovery-test-key-v1"


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _file(path: str, content: bytes) -> ArchiveFile:
    return ArchiveFile(path=path, bytes=len(content), sha256=_sha256(content))


def _part(plaintext: bytes, ciphertext: bytes) -> tuple[StoredArchivePart, ...]:
    return (
        StoredArchivePart(
            number=1,
            plaintext_start=0,
            plaintext_bytes=len(plaintext),
            plaintext_sha256=_sha256(plaintext),
            stored_bytes=len(ciphertext),
            stored_sha256=_sha256(ciphertext),
        ),
    )


def write_archive(
    root: Path,
    *,
    passphrase: str = PASSPHRASE,
    passphrase_id: str = PASSPHRASE_ID,
    with_provenance: bool = False,
    provenance_journal: bytes | None = None,
    provenance_journals: Mapping[str, bytes] | None = None,
    description: str | None = None,
    tags: Sequence[str] = (),
) -> tuple[dict[str, bytes], bytes | None]:
    expected = {
        "notes/alpha.txt": b"alpha\n",
        "notes/beta.txt": b"beta\n",
        "video.bin": b"first-second",
    }
    files = tuple(_file(path, content) for path, content in sorted(expected.items()))

    pack_files = tuple(current for current in files if current.path.startswith("notes/"))
    pack_plan = plan_pack_volume(pack_files, sequence=0)
    pack_plaintext = b"".join(
        iter_render_pack_upload_unit(
            pack_plan,
            0,
            lambda path: (expected[path],),
        )
    )
    pack_ciphertext = encrypt_age_scrypt(pack_plaintext, passphrase, log_n=1)
    sealed_pack = SealedPackVolume(
        volume_id=pack_plan.volume_id,
        sequence=0,
        relative_path=f"volumes/{pack_plan.volume_id}.tar.age",
        files=len(pack_files),
        source_bytes=sum(current.bytes for current in pack_files),
        plaintext_bytes=len(pack_plaintext),
        age_state_json=age_state_json(len(pack_plaintext)),
        index_sha256=pack_plan.index_sha256,
        plan_sha256=pack_plan.plan_sha256,
        parts=_part(pack_plaintext, pack_ciphertext),
        revision="pack-version",
        completed_at="2026-08-08T00:00:00Z",
    )

    raw_file = next(current for current in files if current.path == "video.bin")
    raw_volumes: list[SealedRawVolume] = []
    raw_ciphertexts: dict[str, bytes] = {}
    offset = 0
    for sequence, plaintext in enumerate((b"first-", b"second"), start=1):
        volume_id = f"segment-{format_archive_sequence(sequence)}"
        relative_path = f"volumes/{volume_id}.bin.age"
        raw_ciphertext = encrypt_age_scrypt(plaintext, passphrase, log_n=1)
        raw_ciphertexts[relative_path] = raw_ciphertext
        raw_volumes.append(
            SealedRawVolume(
                volume_id=volume_id,
                sequence=sequence,
                relative_path=relative_path,
                source_path=raw_file.path,
                file_offset=offset,
                plaintext_bytes=len(plaintext),
                file_bytes=raw_file.bytes,
                file_sha256=raw_file.sha256,
                age_state_json=age_state_json(len(plaintext)),
                parts=_part(plaintext, raw_ciphertext),
                revision=f"segment-version-{sequence}",
                completed_at="2026-08-08T00:00:00Z",
            )
        )
        offset += len(plaintext)
    verified_raw = VerifiedRawFile(
        path=raw_file.path,
        bytes=raw_file.bytes,
        sha256=raw_file.sha256,
        ordered_volume_sha256=raw_file_ordered_volume_commitment(
            file=raw_file,
            volumes=raw_volumes,
        ),
        verified_at="2026-08-08T00:00:00Z",
    )
    provenance_identity: str | None = None
    provenance_objects: tuple[SealedProvenanceObject, ...] = ()
    provenance_ciphertexts: dict[str, bytes] = {}
    exact_journal: bytes | None = None
    if with_provenance:
        exact_journal = provenance_journal
        if exact_journal is None:
            observed = root.parent / "observed-alpha.txt"
            observed.write_bytes(expected["notes/alpha.txt"])
            exact_journal = create_observation_journal(
                observed,
                relative_path="notes/alpha.txt",
                host_id="urn:uuid:00000000-0000-4000-8000-000000000001",
                agent_name="recovery-fixture",
                agent_version="1.0.0",
                observer=native_provenance_observer(),
            )
            observed.unlink()
        summary = validate_journal(exact_journal)
        bindings = tuple(
            FileProvenanceBinding(
                path=current.path,
                bytes=current.bytes,
                sha256=current.sha256,
                status="captured" if current.path == "notes/alpha.txt" else "omitted",
                journal_id=(summary.journal_id if current.path == "notes/alpha.txt" else None),
                current_state_id=(
                    summary.current_state_id if current.path == "notes/alpha.txt" else None
                ),
                omission_reason=(
                    None
                    if current.path == "notes/alpha.txt"
                    else "fixture explicitly omitted source provenance"
                ),
            )
            for current in files
        )
        journal_set = dict(provenance_journals or {summary.journal_id: exact_journal})
        if journal_set.get(summary.journal_id) != exact_journal:
            raise ValueError("recovery fixture current journal is missing from its exact set")
        tree_digest = hashlib.sha256()
        for current in files:
            tree_digest.update(f"{current.path}\t{current.bytes}\t{current.sha256}\n".encode())
        tree_sha256 = tree_digest.hexdigest()
        binding_payload = binding_segment_bytes(
            first_file_order=0,
            files=[
                {
                    "path": binding.path,
                    "bytes": binding.bytes,
                    "sha256": binding.sha256,
                    "status": binding.status,
                    **(
                        {
                            "journal_id": binding.journal_id,
                            "current_state_id": binding.current_state_id,
                        }
                        if binding.status == "captured"
                        else {"omission_reason": binding.omission_reason}
                    ),
                }
                for binding in bindings
            ],
        )
        provenance_volume_documents: list[ProvenanceVolumeDocument] = []
        volume_payloads: list[bytes] = []
        sequence = 0
        provenance_volume_documents.append(
            ProvenanceVolumeDocument(
                archive_generation="a" * 64,
                archive_tree_sha256=tree_sha256,
                sequence=sequence,
                payload=ProvenancePayloadIdentity(
                    kind="bindings",
                    path=f"provenance/payloads/volume-{format_provenance_sequence(sequence)}.bin.age",
                    bytes=len(binding_payload),
                    sha256=_sha256(binding_payload),
                ),
                first_file_order=0,
                file_count=len(bindings),
            )
        )
        volume_payloads.append(binding_payload)
        sequence += 1
        for journal_id, content in sorted(journal_set.items()):
            for offset in range(0, len(content), PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX):
                payload = content[offset : offset + PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX]
                provenance_volume_documents.append(
                    ProvenanceVolumeDocument(
                        archive_generation="a" * 64,
                        archive_tree_sha256=tree_sha256,
                        sequence=sequence,
                        payload=ProvenancePayloadIdentity(
                            kind="journal",
                            path=(
                                "provenance/payloads/volume-"
                                f"{format_provenance_sequence(sequence)}.bin.age"
                            ),
                            bytes=len(payload),
                            sha256=_sha256(payload),
                        ),
                        journal_id=journal_id,
                        journal_offset=offset,
                        journal_bytes=len(content),
                        journal_sha256=_sha256(content),
                    )
                )
                volume_payloads.append(payload)
                sequence += 1
        ordered = hashlib.sha256()
        for document, payload in zip(provenance_volume_documents, volume_payloads, strict=True):
            metadata_bytes = document.to_json_bytes()
            update_ordered_volume_commitment(ordered, document)
            provenance_ciphertexts[document.metadata_path] = encrypt_age_scrypt(
                metadata_bytes, passphrase, log_n=1
            )
            provenance_ciphertexts[document.payload.path] = encrypt_age_scrypt(
                payload, passphrase, log_n=1
            )
        provenance_terminal = ProvenanceTerminalDocument(
            archive_generation="a" * 64,
            archive_tree_sha256=tree_sha256,
            sequence=len(provenance_volume_documents),
        )
        update_ordered_volume_commitment(ordered, provenance_terminal)
        provenance_ciphertexts[provenance_terminal.metadata_path] = encrypt_age_scrypt(
            provenance_terminal.to_json_bytes(), passphrase, log_n=1
        )
        provenance_root = ProvenanceRootDocument(
            archive_generation="a" * 64,
            archive_tree_sha256=tree_sha256,
            ordered_volume_sha256=ordered.hexdigest(),
        )
        root_plaintext = provenance_root.to_json_bytes()
        root_ciphertext = encrypt_age_scrypt(root_plaintext, passphrase, log_n=1)
        provenance_ciphertexts["provenance/root.json.age"] = root_ciphertext
        provenance_identity = provenance_root.identity
        provenance_objects = (
            SealedProvenanceObject(
                object_id="provenance-root",
                kind="provenance-root",
                relative_path="provenance/root.json.age",
                plaintext_bytes=len(root_plaintext),
                plaintext_sha256=provenance_root.identity,
                stored_bytes=len(root_ciphertext),
                stored_sha256=_sha256(root_ciphertext),
                revision="provenance-root-version",
                completed_at="2026-08-08T00:00:00Z",
            ),
        )

    manifest, archive_volume_documents = build_collection_archive_authority(
        archive_generation="a" * 64,
        files=files,
        packs=((pack_plan, sealed_pack),),
        raw_volumes=raw_volumes,
        verified_raw_files=(verified_raw,),
        provenance_identity=provenance_identity,
        provenance_objects=provenance_objects,
    )
    encrypted_manifest = encrypt_age_scrypt(manifest, passphrase, log_n=1)
    descriptor = RecoveryDescriptor(
        encryption=CollectionEncryptionBinding(
            format=ARCHIVE_ENCRYPTION_FORMAT,
            passphrase_id=passphrase_id,
        ),
        root=ArchiveRootCiphertextIdentity(
            path="manifest.json.age",
            stored_bytes=len(encrypted_manifest),
            stored_sha256=_sha256(encrypted_manifest),
        ),
    ).to_json_bytes()
    archive_ciphertexts: dict[str, bytes] = {
        "manifest.json.age": encrypted_manifest,
        "recovery.json": descriptor,
        sealed_pack.relative_path: pack_ciphertext,
        **raw_ciphertexts,
        **provenance_ciphertexts,
    }
    if description is not None:
        description_document = CollectionDescriptionDocument.seal(
            archive_root_sha256=_sha256(manifest),
            revision=1,
            description=description,
        )
        archive_ciphertexts[COLLECTION_DESCRIPTION_RELATIVE_PATH] = encrypt_age_scrypt(
            description_document.to_json_bytes(),
            passphrase,
            log_n=1,
        )
    tag_store = MemoryCollectionTagNodeStore()
    tag_set = CollectionTagSet(tag_store)
    for tag in tags:
        tag_set = tag_set.insert(tag)
    tag_head = CollectionTagHeadDocument.seal(
        archive_root_sha256=_sha256(manifest),
        revision=1,
        root_sha256=tag_set.root.root_sha256,
    )
    archive_ciphertexts[COLLECTION_TAG_HEAD_RELATIVE_PATH] = encrypt_age_scrypt(
        tag_head.to_json_bytes(), passphrase, log_n=1
    )
    for digest, encoded in tag_store.nodes.items():
        archive_ciphertexts[collection_tag_node_path(digest)] = encrypt_age_scrypt(
            encoded, passphrase, log_n=1
        )
    for archive_document in archive_volume_documents:
        relative_path = (
            f"metadata/volume-{format_archive_sequence(archive_document.volume.sequence)}.json.age"
        )
        archive_ciphertexts[relative_path] = encrypt_age_scrypt(
            archive_document.to_json_bytes(),
            passphrase,
            log_n=1,
        )
    tree_sha256 = str(__import__("json").loads(manifest)["tree"]["sha256"])
    archive_terminal = build_collection_archive_terminal_document(
        archive_generation="a" * 64,
        tree_sha256=tree_sha256,
        sequence=len(archive_volume_documents),
    )
    terminal_path = f"metadata/volume-{format_archive_sequence(archive_terminal.sequence)}.json.age"
    archive_ciphertexts[terminal_path] = encrypt_age_scrypt(
        archive_terminal.to_json_bytes(), passphrase, log_n=1
    )
    for relative, content in archive_ciphertexts.items():
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
    return expected, exact_journal


__all__ = ["PASSPHRASE", "PASSPHRASE_ID", "write_archive"]
