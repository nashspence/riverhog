"""Exact root-selected provenance metadata/object fixtures."""

from __future__ import annotations

import hashlib

from riverhog_archive_contracts import (
    PROVENANCE_BINDINGS_FORMAT,
    HistoryJournalAnchor,
    MemberHistoryBinding,
    MemberHistoryBuilder,
    MemberHistoryPrimary,
    ProvenancePayload,
    ProvenanceRootDocument,
    ProvenanceTerminalDocument,
    ProvenanceVolumeDocument,
    binding_tree_commitment,
    ordered_provenance_commitment,
    provenance_structure_identity,
)
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.provenance_archive_read import CanonicalProvenanceArchiveReader


def archive_reader_fixture(
    journal_bytes: bytes = b"first-frame\nsecond-frame\n",
    journal_id: str = "urn:uuid:11111111-1111-4111-8111-111111111111",
) -> tuple[CanonicalProvenanceArchiveReader, dict[str, bytes], str, str]:
    generation = "a" * 64
    artifact_set = "b" * 64
    primary = MemberHistoryPrimary(
        HistoryJournalAnchor(
            journal_id,
            "urn:uuid:33333333-3333-4333-8333-333333333333",
            0,
            "d" * 64,
            hashlib.sha256(journal_bytes).hexdigest(),
            len(journal_bytes),
        ),
        "urn:uuid:22222222-2222-4222-8222-222222222222",
    )
    with MemberHistoryBuilder(
        artifact_id="c" * 64, bytes=7, sha256="a" * 64, primary=primary
    ) as builder:
        final, _history = builder.seal()
        binding = final.to_mapping()
        structures = {
            provenance_structure_identity(raw).relative_path: raw for raw in builder.objects()
        }
    binding_bytes = canonical_json_bytes(
        {"format": PROVENANCE_BINDINGS_FORMAT, "bindings": [binding]}
    )
    descriptors = (
        ProvenanceVolumeDocument(
            archive_generation=generation,
            artifact_set_sha256=artifact_set,
            sequence=0,
            payload=ProvenancePayload(
                kind="bindings",
                sequence=0,
                bytes=len(binding_bytes),
                sha256=hashlib.sha256(binding_bytes).hexdigest(),
            ),
            first_artifact_id="c" * 64,
            last_artifact_id="c" * 64,
            binding_count=1,
        ),
        ProvenanceVolumeDocument(
            archive_generation=generation,
            artifact_set_sha256=artifact_set,
            sequence=1,
            payload=ProvenancePayload(
                kind="journal",
                sequence=1,
                bytes=len(journal_bytes[:12]),
                sha256=hashlib.sha256(journal_bytes[:12]).hexdigest(),
            ),
            journal_id=journal_id,
            journal_offset=0,
            journal_bytes=len(journal_bytes),
            journal_sha256=hashlib.sha256(journal_bytes).hexdigest(),
        ),
        ProvenanceVolumeDocument(
            archive_generation=generation,
            artifact_set_sha256=artifact_set,
            sequence=2,
            payload=ProvenancePayload(
                kind="journal",
                sequence=2,
                bytes=len(journal_bytes[12:]),
                sha256=hashlib.sha256(journal_bytes[12:]).hexdigest(),
            ),
            journal_id=journal_id,
            journal_offset=12,
            journal_bytes=len(journal_bytes),
            journal_sha256=hashlib.sha256(journal_bytes).hexdigest(),
        ),
    )
    terminal = ProvenanceTerminalDocument(
        archive_generation=generation,
        artifact_set_sha256=artifact_set,
        sequence=3,
    )
    root = ProvenanceRootDocument(
        archive_generation=generation,
        artifact_set_sha256=artifact_set,
        delivery_context_id="urn:uuid:44444444-4444-4444-8444-444444444444",
        binding_count=1,
        binding_tree_sha256=binding_tree_commitment(
            (MemberHistoryBinding.from_mapping(binding),)
        ).root_sha256,
        journal_count=1,
        ordered_volume_sha256=ordered_provenance_commitment((*descriptors, terminal)),
    )
    objects = {"provenance/root.json.age": root.to_json_bytes()}
    objects.update(structures)
    for document in (*descriptors, terminal):
        objects[document.metadata_path] = document.to_json_bytes()
    objects[descriptors[0].payload.path] = binding_bytes
    objects[descriptors[1].payload.path] = journal_bytes[:12]
    objects[descriptors[2].payload.path] = journal_bytes[12:]

    def read_object(path: str):
        yield objects[path]

    reader = CanonicalProvenanceArchiveReader(
        read_object,
        expected_root_sha256=root.identity,
        archive_generation=generation,
        artifact_set_sha256=artifact_set,
    )
    return reader, objects, journal_id, journal_bytes.decode()
