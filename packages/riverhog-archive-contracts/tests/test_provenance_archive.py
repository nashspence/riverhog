from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
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
    operation_root = ProvenanceRootDocument(
        _GENERATION,
        _ARTIFACT_SET,
        _CONTEXT,
        binding_count=2,
        journal_count=1,
        ordered_volume_sha256=root.ordered_volume_sha256,
        operation_journal_id=_JOURNAL,
    )
    assert ProvenanceRootDocument.from_json_bytes(operation_root.to_json_bytes()) == operation_root
    assert operation_root.identity != root.identity


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


def test_archive_custody_schemas_match_their_distinct_wire_formats() -> None:
    bindings, journal = _binding_volume(), _journal_volume()
    terminal = ProvenanceTerminalDocument(_GENERATION, _ARTIFACT_SET, 2)
    root = ProvenanceRootDocument(
        _GENERATION,
        _ARTIFACT_SET,
        _CONTEXT,
        binding_count=2,
        journal_count=1,
        ordered_volume_sha256=ordered_provenance_commitment((bindings, journal, terminal)),
        operation_journal_id=_JOURNAL,
    )
    binding_page = {
        "format": "riverhog-archive-provenance-bindings/v1",
        "bindings": [
            {
                "artifact_id": _FIRST,
                "journal": {
                    "journal_id": _JOURNAL,
                    "through": {
                        "entry_id": _CONTEXT,
                        "sequence": "0",
                        "json_sha256": _ARTIFACT_SET,
                    },
                    "prefix_sha256": _GENERATION,
                    "prefix_bytes": "8",
                },
                "delivery_association_id": _CONTEXT,
            }
        ],
    }
    documents = {
        "volume": (bindings.to_mapping(), journal.to_mapping()),
        "terminal": (terminal.to_mapping(),),
        "root": (root.to_mapping(),),
        "bindings": (binding_page,),
    }
    schema_dir = Path(__file__).resolve().parents[1] / "schemas"
    for kind, values in documents.items():
        schema = json.loads(
            (schema_dir / f"riverhog-archive-provenance-{kind}-v1.schema.json").read_text()
        )
        validator = Draft202012Validator(schema)
        for value in values:
            validator.validate(value)
