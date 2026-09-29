from __future__ import annotations

import hashlib

import pytest
from riverhog_archive_contracts import (
    PROVENANCE_BINDINGS_FORMAT,
    ProvenancePayload,
    ProvenanceRootDocument,
    ProvenanceTerminalDocument,
    ProvenanceVolumeDocument,
    ordered_provenance_commitment,
)
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.provenance_archive_read import (
    CanonicalProvenanceArchiveReader,
    ProvenanceArchiveReadError,
)


def _archive() -> tuple[CanonicalProvenanceArchiveReader, dict[str, bytes], str, str]:
    generation = "a" * 64
    artifact_set = "b" * 64
    journal_id = "urn:uuid:11111111-1111-4111-8111-111111111111"
    association_id = "urn:uuid:22222222-2222-4222-8222-222222222222"
    entry_id = "urn:uuid:33333333-3333-4333-8333-333333333333"
    binding = {
        "artifact_id": "c" * 64,
        "journal": {
            "journal_id": journal_id,
            "through": {"entry_id": entry_id, "sequence": "0", "json_sha256": "d" * 64},
            "prefix_sha256": "e" * 64,
            "prefix_bytes": "9",
        },
        "delivery_association_id": association_id,
    }
    binding_bytes = canonical_json_bytes(
        {"format": PROVENANCE_BINDINGS_FORMAT, "bindings": [binding]}
    )
    journal_bytes = b"first-frame\nsecond-frame\n"
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
        journal_count=1,
        ordered_volume_sha256=ordered_provenance_commitment((*descriptors, terminal)),
    )
    objects = {"provenance/root.json.age": root.to_json_bytes()}
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


def test_root_bound_reader_streams_exact_journal_and_member_bindings() -> None:
    reader, _, journal_id, journal_text = _archive()
    assert reader.scan().volume_count == 3
    assert b"".join(reader.iter_journal_range(journal_id)) == journal_text.encode()
    assert (
        b"".join(reader.iter_journal_range(journal_id, offset=5, size=12))
        == (journal_text.encode()[5:17])
    )
    assert [row["artifact_id"] for row in reader.iter_bindings()] == ["c" * 64]


def test_root_bound_reader_rejects_changed_sequence_and_payload() -> None:
    reader, objects, journal_id, _ = _archive()
    payload_path = next(path for path in objects if path.startswith("provenance/payloads/volume-"))
    objects[payload_path] = b"changed"
    with pytest.raises(ProvenanceArchiveReadError):
        list(reader.iter_bindings())
    reader, objects, journal_id, _ = _archive()
    objects["provenance/metadata/volume-" + "0" * 63 + "1.json.age"] = b"{}"
    with pytest.raises((ProvenanceArchiveReadError, ValueError)):
        list(reader.iter_journal_range(journal_id))
