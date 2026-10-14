#!/usr/bin/env python3
"""Measure source-reader primitives in disposable local state; never contacts providers.

Run once against the audited checkout and once against the patched checkout.
The fixture establishes structural commitments, not semantic provenance or crypto
interoperability. Numeric-domain probes allocate one empty artifact, not enormous data.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import sqlite3
import sys
import tempfile
from collections import Counter
from contextlib import nullcontext
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--source', required=True, type=Path)
parser.add_argument('--output', required=True, type=Path)
args = parser.parse_args()
root_path = args.source.resolve(strict=True)
sys.path[:0] = [str(root_path)] + [str(p) for p in sorted(root_path.rglob('src'))
    if (p.parent / 'pyproject.toml').is_file() and '.git' not in p.parts and '.venv' not in p.parts]
from a_riverhog_recovery_tool._provenance_reader import CanonicalProvenanceArchiveReader
from a_riverhog_filesystem_store.materialize import _cleanup_inactive_generations, _initialize_state
from riverhog_archive_contracts import (
    MemberHistoryBinding, ProvenancePayload, ProvenanceRootDocument,
    ProvenanceTerminalDocument, ProvenanceVolumeDocument, format_archive_sequence,
    binding_tree_commitment, ordered_provenance_commitment,
)
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.domain.archive import ArchiveArtifact
from riverhog_core.pack_volume import plan_pack_volume

def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()

def corpus(journals: int, segments: int):
    """Exact structural custody; synthetic journal bytes are not semantic journals."""
    generation, artifacts = "a" * 64, "b" * 64
    member = MemberHistoryBinding("c" * 64, 0, digest(b""), "d" * 64, 1)
    bindings = canonical_json_bytes({
        "format": "riverhog-archive-member-history-bindings/v1",
        "bindings": [member.to_mapping()],
    })
    first = ProvenancePayload("bindings", 0, len(bindings), digest(bindings))
    documents = [ProvenanceVolumeDocument(
        generation, artifacts, 0, first, first_artifact_id=member.artifact_id,
        last_artifact_id=member.artifact_id, binding_count=1,
    )]
    objects = {first.path: bindings}
    expected = {}
    for ordinal in range(journals):
        journal_id = f"urn:uuid:00000000-0000-4000-8000-{ordinal:012x}"
        chunks = [f"journal {ordinal} segment {i}\n".encode() for i in range(segments)]
        raw = b"".join(chunks)
        expected[journal_id] = raw
        offset = 0
        for chunk in chunks:
            sequence = len(documents)
            payload = ProvenancePayload("journal", sequence, len(chunk), digest(chunk))
            documents.append(ProvenanceVolumeDocument(
                generation, artifacts, sequence, payload, journal_id=journal_id,
                journal_offset=offset, journal_bytes=len(raw), journal_sha256=digest(raw),
            ))
            objects[payload.path] = chunk
            offset += len(chunk)
    terminal = ProvenanceTerminalDocument(generation, artifacts, len(documents))
    root = ProvenanceRootDocument(
        generation, artifacts, "urn:uuid:00000000-0000-4000-8000-000000000fff",
        1, binding_tree_commitment((member,)).root_sha256, journals,
        ordered_provenance_commitment((*documents, terminal)),
    )
    objects["provenance/root.json.age"] = root.to_json_bytes()
    for document in (*documents, terminal):
        objects[document.metadata_path] = document.to_json_bytes()
    return root, objects, expected

def reader_for(root, objects, reads):
    def read(path):
        reads[path] += 1
        yield objects[path]
    return CanonicalProvenanceArchiveReader(
        read, expected_root_sha256=root.identity,
        archive_generation=root.archive_generation, artifact_set_sha256=root.artifact_set_sha256,
    )

result = {
    'scope': 'source primitives; synthetic structural journals; not whole-operation qualification',
    'python': sys.version.split()[0], 'sqlite': sqlite3.sqlite_version,
    'provenance': [], 'checkpoint_cleanup': [], 'sequence_domain': [],
}
with tempfile.TemporaryDirectory(prefix='riverhog-growth-probe-') as scratch:
    for journals in (1, 4, 16, 64):
        segments = 3
        root, objects, expected = corpus(journals, segments)
        reads = Counter()
        reader = reader_for(root, objects, reads)
        context = reader.prepared(scratch=Path(scratch)) if hasattr(reader, 'prepared') else nullcontext(reader)
        with context:
            reader.scan()
            for journal_id, byte_count, sha in reader.iter_journal_headers():
                actual = b''.join(reader.iter_journal_range(journal_id))
                assert actual == expected[journal_id] and len(actual) == byte_count and digest(actual) == sha
        result['provenance'].append({
            'journals': journals, 'segments_per_journal': segments,
            'nonterminal_descriptors': 1 + journals * segments,
            'root_reads': reads['provenance/root.json.age'],
            'descriptor_reads': sum(v for k,v in reads.items() if '/metadata/' in k),
            'payload_reads': sum(v for k,v in reads.items() if '/payloads/' in k),
        })
for size in (1, 512, 2048, 8192):
    db = sqlite3.connect(':memory:')
    try:
        _initialize_state(db)
        db.execute("INSERT INTO authority VALUES ('active_generation','active')")
        for generation in ('active','z-abandoned'):
            db.execute("INSERT INTO projection_generation(generation,status) VALUES (?, 'complete')", (generation,))
            db.executemany("INSERT INTO projection VALUES (?, ?, ?, '{}', 2, 0)",
                ((generation,str(i),str(i)) for i in range(size)))
            db.executemany("INSERT INTO projection_part VALUES (?, ?, 1, 0, 0, ?)",
                ((generation,str(i),'a'*64) for i in range(size)))
        db.commit()
        steps = [0]
        def observe():
            steps[0] += 1
            return 0
        db.set_progress_handler(observe, 1)
        _cleanup_inactive_generations(db)
        db.set_progress_handler(None, 0)
        assert db.execute('SELECT COUNT(*) FROM projection').fetchone()[0] == size
        assert db.execute('SELECT COUNT(*) FROM projection_part').fetchone()[0] == size
        result['checkpoint_cleanup'].append({'rows_per_generation_per_table': size,
            'sqlite_vm_instructions': steps[0], 'active_rows_preserved': True})
    finally:
        db.close()
for ordinal in (0, 2**53-1, 2**53, 2**53+1, 2**256-2):
    row = {'ordinal': str(ordinal), 'sequence_encoding': format_archive_sequence(ordinal)}
    try:
        plan = plan_pack_volume((ArchiveArtifact('c'*64, 0, digest(b'')),), sequence=ordinal)
        row.update(writer='accepted', encoded_index_bytes=len(plan.index_bytes))
    except (ValueError, OverflowError) as error:
        row.update(writer='rejected', exception=type(error).__name__, reason=str(error))
    result['sequence_domain'].append(row)
result['source_sha256'] = {}
for name in (
    'some-implementations/riverhog/recovery/src/a_riverhog_recovery_tool/_provenance_reader.py',
    'some-implementations/riverhog/recovery/src/a_riverhog_recovery_tool/_archive_io.py',
    'some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/materialize.py',
    'riverhog/src/riverhog_core/pack_volume.py',
):
    result['source_sha256'][name] = digest((root_path/name).read_bytes())
args.output.write_text(json.dumps(result, indent=2, sort_keys=True)+'\n')
print(json.dumps({k:v for k,v in result.items() if k != 'source_sha256'}, indent=2))
