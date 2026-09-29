from __future__ import annotations

import hashlib

import pytest
from riverhog_archive_contracts import (
    ProvenanceArchiveError,
    ProvenancePayload,
    ProvenanceRootDocument,
    ProvenanceTerminalDocument,
    ProvenanceVolumeDocument,
    ordered_provenance_commitment,
)

_GENERATION = "a" * 64
_ARTIFACT_SET = "b" * 64
_FIRST = "1" * 64
_LAST = "2" * 64
_JOURNAL = "urn:uuid:12345678-1234-4234-9234-123456789abc"
_CONTEXT = "urn:uuid:abcdefab-1234-4234-9234-123456789abc"


def _binding_volume() -> ProvenanceVolumeDocument:
    return ProvenanceVolumeDocument(
        archive_generation=_GENERATION,
        artifact_set_sha256=_ARTIFACT_SET,
        sequence=0,
        payload=ProvenancePayload("bindings", 0, 8, hashlib.sha256(b"bindings").hexdigest()),
        first_artifact_id=_FIRST,
        last_artifact_id=_LAST,
        binding_count=2,
    )


def _journal_volume() -> ProvenanceVolumeDocument:
    return ProvenanceVolumeDocument(
        archive_generation=_GENERATION,
        artifact_set_sha256=_ARTIFACT_SET,
        sequence=1,
        payload=ProvenancePayload("journal", 1, 8, hashlib.sha256(b"journals").hexdigest()),
        journal_id=_JOURNAL,
        journal_offset=0,
        journal_bytes=8,
        journal_sha256=hashlib.sha256(b"journals").hexdigest(),
    )


def test_bounded_structural_corpus_root_round_trip() -> None:
    bindings, journal = _binding_volume(), _journal_volume()
    terminal = ProvenanceTerminalDocument(_GENERATION, _ARTIFACT_SET, 2)
    assert ProvenanceVolumeDocument.from_json_bytes(bindings.to_json_bytes()) == bindings
    assert ProvenanceVolumeDocument.from_json_bytes(journal.to_json_bytes()) == journal
    assert ProvenanceTerminalDocument.from_json_bytes(terminal.to_json_bytes()) == terminal
    root = ProvenanceRootDocument(
        _GENERATION,
        _ARTIFACT_SET,
        _CONTEXT,
        binding_count=2,
        journal_count=1,
        ordered_volume_sha256=ordered_provenance_commitment((bindings, journal, terminal)),
    )
    assert ProvenanceRootDocument.from_json_bytes(root.to_json_bytes()) == root
    assert root.identity == hashlib.sha256(root.to_json_bytes()).hexdigest()
    assert b"path" not in root.to_json_bytes()


def test_sequence_and_authority_must_be_complete_and_contiguous() -> None:
    bindings, journal = _binding_volume(), _journal_volume()
    terminal = ProvenanceTerminalDocument(_GENERATION, _ARTIFACT_SET, 2)
    for documents in (
        (bindings, journal),
        (bindings, terminal),
        (journal, terminal),
        (bindings, terminal, journal),
    ):
        with pytest.raises(ProvenanceArchiveError):
            ordered_provenance_commitment(documents)
    changed = ProvenanceTerminalDocument(_GENERATION, "c" * 64, 2)
    with pytest.raises(ProvenanceArchiveError, match="authority"):
        ordered_provenance_commitment((bindings, journal, changed))


def test_noncanonical_metadata_and_wrong_range_are_rejected() -> None:
    volume = _binding_volume()
    with pytest.raises(ProvenanceArchiveError, match="JCS"):
        ProvenanceVolumeDocument.from_json_bytes(volume.to_json_bytes() + b"\n")
    with pytest.raises(ProvenanceArchiveError, match="reversed"):
        ProvenanceVolumeDocument(
            _GENERATION,
            _ARTIFACT_SET,
            0,
            volume.payload,
            first_artifact_id=_LAST,
            last_artifact_id=_FIRST,
            binding_count=2,
        )
    with pytest.raises(ProvenanceArchiveError, match="segment bound"):
        ProvenancePayload("bindings", 0, 4 * 1024 * 1024 + 1, "c" * 64)
