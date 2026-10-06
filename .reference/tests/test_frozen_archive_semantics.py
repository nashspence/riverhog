"""Candidate readers consume fixed decoded bytes; no candidate writer is an oracle.

The capture is intentionally pre-v1 and reference-only. It is not an encrypted
archive export, a final v1 baseline, or cryptographic interoperability evidence.
"""
from __future__ import annotations

import base64
import hashlib
import json
import lzma
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest
from a_riverhog_recovery_tool._metadata import stage_metadata
from a_riverhog_recovery_tool._payloads import _extract_pack, _extract_segment
from a_riverhog_recovery_tool._provenance_reader import (
    CanonicalProvenanceArchiveReader as RecoveryReader,
)
from riverhog_archive_contracts import (
    CollectionArchiveManifest,
    CollectionArchiveTerminalDocument,
    CollectionArchiveVolumeDocument,
    MemberHistoryBinding,
    PackArchiveVolume,
    ProvenanceRootDocument,
    ProvenanceTerminalDocument,
    ProvenanceVolumeDocument,
    binding_tree_commitment,
    ordered_archive_volume_commitment,
    ordered_provenance_commitment,
)
from riverhog_core.provenance_archive_read import CanonicalProvenanceArchiveReader as ServiceReader

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "archive-semantics.json"


@pytest.fixture
def capture():
    envelope = json.loads(FIXTURE.read_bytes())
    raw = lzma.decompress(base64.b64decode(envelope["xz_base64"], validate=True))
    assert hashlib.sha256(raw).hexdigest() == envelope["decoded_sha256"]
    value = json.loads(raw)
    objects = {
        path: row["utf8"].encode("utf-8") if "utf8" in row else base64.b64decode(row["base64"])
        for path, row in value["plaintext_objects"].items()
    }
    assert set(objects) == set(value["plaintext_sha256"])
    for path, content in objects.items():
        assert hashlib.sha256(content).hexdigest() == value["plaintext_sha256"][path]
    return value, objects


def _manifest(objects):
    return CollectionArchiveManifest.from_json_bytes(objects["manifest.json.age"])


def _main_sequence(objects):
    for path in sorted(path for path in objects if path.startswith("metadata/volume-")):
        raw = objects[path]
        if json.loads(raw)["format"] == "collection-archive-terminal/v1":
            yield CollectionArchiveTerminalDocument.from_json_bytes(raw)
        else:
            yield CollectionArchiveVolumeDocument.from_json_bytes(raw)


def test_fixed_root_and_ordered_commitment_preimages(capture):
    expected, objects = capture
    manifest = _manifest(objects)
    assert hashlib.sha256(manifest.to_json_bytes()).hexdigest() == expected["archive_root_sha256"]
    assert manifest.to_json_bytes() == objects["manifest.json.age"]
    observed = ordered_archive_volume_commitment(_main_sequence(objects))
    assert observed == manifest.ordered_volume_sha256
    root = ProvenanceRootDocument.from_json_bytes(objects["provenance/root.json.age"])
    assert root.identity == manifest.provenance.identity
    sequence = []
    for path in sorted(path for path in objects if path.startswith("provenance/metadata/volume-")):
        raw = objects[path]
        model = (
            ProvenanceTerminalDocument
            if json.loads(raw)["format"] == "riverhog-archive-provenance-terminal/v1"
            else ProvenanceVolumeDocument
        )
        document = model.from_json_bytes(raw)
        assert document.to_json_bytes() == raw
        sequence.append(document)
    assert ordered_provenance_commitment(sequence) == root.ordered_volume_sha256


@pytest.mark.parametrize("reader_type", [ServiceReader, RecoveryReader])
def test_service_and_recovery_read_the_same_frozen_provenance(capture, reader_type):
    expected, objects = capture
    manifest = _manifest(objects)
    reader = reader_type(
        lambda path: (objects[path],), expected_root_sha256=manifest.provenance.identity,
        archive_generation=manifest.archive_generation,
        artifact_set_sha256=manifest.artifact_set_sha256,
    )
    summary = reader.scan()
    bindings = [MemberHistoryBinding.from_mapping(row) for row in reader.iter_bindings()]
    assert binding_tree_commitment(bindings).root_sha256 == summary.root.binding_tree_sha256
    journals = list(reader.iter_journal_headers())
    assert len(journals) == expected["journal_count"]
    for journal_id, size, digest in journals:
        raw = b"".join(reader.iter_journal_range(journal_id, offset=0, size=size))
        assert len(raw) == size and hashlib.sha256(raw).hexdigest() == digest
    assert {binding.artifact_id for binding in bindings} == set(expected["members"])


def test_embedded_source_roots_and_selected_record_sets_remain_verifiable(capture):
    _, objects = capture
    manifest = _manifest(objects)
    reader = ServiceReader(
        lambda path: (objects[path],), expected_root_sha256=manifest.provenance.identity,
        archive_generation=manifest.archive_generation,
        artifact_set_sha256=manifest.artifact_set_sha256,
    )
    imports = []
    for row in reader.iter_bindings():
        binding = MemberHistoryBinding.from_mapping(row)
        reader.member_history(binding)  # Validates both complete record-set selections.
        imports.extend(reader.iter_history_imports(binding))
    assert imports
    for imported in imports:
        proof = reader.source_binding_proof(imported)
        proof.verify_import(imported)
        assert hashlib.sha256(proof.archive_root).hexdigest() == imported.source_archive_root_sha256


def test_fixed_pack_grammar_and_segment_bytes(capture, tmp_path):
    expected, objects = capture
    spool = tmp_path / "spool"
    spool.mkdir()
    with closing(sqlite3.connect(":memory:")) as state:
        state.execute(
            "CREATE TABLE artifacts(artifact_id TEXT PRIMARY KEY, bytes INTEGER, "
            "sha256 TEXT, received INTEGER, kind TEXT)"
        )
        for document in _main_sequence(objects):
            if isinstance(document, CollectionArchiveTerminalDocument):
                continue
            volume = document.volume
            path = tmp_path / volume.id
            path.write_bytes(objects[volume.path])
            if isinstance(volume, PackArchiveVolume):
                _extract_pack(path, volume, state, spool)
            else:
                _extract_segment(path, volume, state, spool)
        rows = state.execute(
            "SELECT artifact_id, bytes, sha256, received FROM artifacts"
        ).fetchall()
        assert len(rows) == len(expected["members"])
        for artifact_id, size, digest, received in rows:
            raw = (spool / artifact_id).read_bytes()
            assert raw == base64.b64decode(expected["members"][artifact_id])
            assert len(raw) == size == received and hashlib.sha256(raw).hexdigest() == digest


def test_fixed_copy_adjacent_metadata_and_binary_tag_nodes(capture, tmp_path):
    expected, objects = capture

    def read(path, maximum):
        raw = objects[path]
        assert len(raw) <= maximum
        return raw

    metadata = stage_metadata(
        read_plaintext=read, description_exists=True,
        archive_root_sha256=expected["archive_root_sha256"], staging=tmp_path,
    )
    assert metadata.description_sha256 == expected["description_sha256"]
    assert metadata.head_sha256 == expected["tag_head_sha256"]
    lines = (tmp_path / "metadata/tags.jsonseq").read_bytes().splitlines()
    records = [json.loads(line) for line in lines]
    assert [row["tag"] for row in records if row["record"] == "tag"] == expected["tags"]
    assert (tmp_path / "metadata/description.txt").read_text() == expected["description"]
