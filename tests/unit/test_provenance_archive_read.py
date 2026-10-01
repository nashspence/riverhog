from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest
from riverhog_archive_contracts import (
    BOUND_HISTORY_EXTENT,
    MEMBER_HISTORY_IMPORTS_SCHEMA,
    MEMBER_HISTORY_ROOTS_SCHEMA,
    PROVENANCE_BINDINGS_FORMAT,
    RETAINED_HISTORY_EXTENT,
    HistoryJournalAnchor,
    MemberHistoryBinding,
    MemberHistoryBuilder,
    MemberHistoryDocument,
    MemberHistoryPrimary,
    MemberHistoryRoot,
    ProvenancePayload,
    ProvenanceRootDocument,
    ProvenanceTerminalDocument,
    ProvenanceVolumeDocument,
    RecordPage,
    RecordSetCommitment,
    binding_tree_commitment,
    history_record_page_object_path,
    member_history_object_path,
    ordered_provenance_commitment,
    provenance_structure_identity,
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
    journal_bytes = b"first-frame\nsecond-frame\n"
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


def test_root_bound_reader_streams_exact_journal_and_member_bindings() -> None:
    reader, _, journal_id, journal_text = _archive()
    assert reader.scan().volume_count == 3
    assert list(reader.iter_journal_ids()) == [journal_id]
    assert list(reader.iter_journal_headers()) == [
        (journal_id, len(journal_text.encode()), hashlib.sha256(journal_text.encode()).hexdigest())
    ]
    assert reader.journal_metadata(journal_id) == (
        len(journal_text.encode()),
        hashlib.sha256(journal_text.encode()).hexdigest(),
    )
    assert b"".join(reader.iter_journal_range(journal_id)) == journal_text.encode()
    assert (
        b"".join(reader.iter_journal_range(journal_id, offset=5, size=12))
        == (journal_text.encode()[5:17])
    )
    assert [row["artifact_id"] for row in reader.iter_bindings()] == ["c" * 64]


def test_root_bound_reader_rejects_changed_sequence_and_payload() -> None:
    reader, objects, journal_id, _ = _archive()
    payload_path = next(path for path in objects if path.startswith("provenance/payloads/"))
    objects[payload_path] = b"changed"
    with pytest.raises(ProvenanceArchiveReadError):
        list(reader.iter_bindings())
    reader, objects, journal_id, _ = _archive()
    objects["provenance/metadata/volume-" + "0" * 63 + "1.json.age"] = b"{}"
    with pytest.raises((ProvenanceArchiveReadError, ValueError)):
        list(reader.iter_journal_range(journal_id))


def test_member_binding_tree_must_match_the_archived_binding_pages() -> None:
    reader, objects, _, _ = _archive()
    root = reader.scan().root
    for selected, valid in ((root.binding_tree_sha256, True), ("0" * 64, False)):
        amended = replace(root, binding_tree_sha256=selected)
        objects["provenance/root.json.age"] = amended.to_json_bytes()

        def read_object(path: str):
            yield objects[path]

        selected_reader = CanonicalProvenanceArchiveReader(
            read_object,
            expected_root_sha256=amended.identity,
            archive_generation=root.archive_generation,
            artifact_set_sha256=root.artifact_set_sha256,
        )
        if valid:
            assert selected_reader.binding_inclusion("c" * 64).target_index == 0
        else:
            with pytest.raises(ProvenanceArchiveReadError, match="differs from the root"):
                selected_reader.binding_inclusion("c" * 64)


def test_binding_pages_progress_across_the_bounded_archive_extent() -> None:
    generation = "a" * 64
    artifact_set = "b" * 64
    journal_id = "urn:uuid:11111111-1111-4111-8111-111111111111"
    bindings = [
        {
            "artifact_id": f"{index:064x}",
            "bytes": "7",
            "sha256": "a" * 64,
            "history_sha256": "f" * 64,
            "history_bytes": "512",
        }
        for index in range(513)
    ]
    pages = [bindings[:512], bindings[512:]]
    objects: dict[str, bytes] = {}
    descriptors: list[ProvenanceVolumeDocument] = []
    for sequence, page in enumerate(pages):
        payload = canonical_json_bytes({"format": PROVENANCE_BINDINGS_FORMAT, "bindings": page})
        descriptor = ProvenanceVolumeDocument(
            archive_generation=generation,
            artifact_set_sha256=artifact_set,
            sequence=sequence,
            payload=ProvenancePayload(
                "bindings", sequence, len(payload), hashlib.sha256(payload).hexdigest()
            ),
            first_artifact_id=page[0]["artifact_id"],
            last_artifact_id=page[-1]["artifact_id"],
            binding_count=len(page),
        )
        descriptors.append(descriptor)
        objects[descriptor.payload.path] = payload
        objects[descriptor.metadata_path] = descriptor.to_json_bytes()
    raw_journal = b"journal"
    journal = ProvenanceVolumeDocument(
        archive_generation=generation,
        artifact_set_sha256=artifact_set,
        sequence=2,
        payload=ProvenancePayload(
            "journal", 2, len(raw_journal), hashlib.sha256(raw_journal).hexdigest()
        ),
        journal_id=journal_id,
        journal_offset=0,
        journal_bytes=len(raw_journal),
        journal_sha256=hashlib.sha256(raw_journal).hexdigest(),
    )
    descriptors.append(journal)
    objects[journal.payload.path] = raw_journal
    objects[journal.metadata_path] = journal.to_json_bytes()
    terminal = ProvenanceTerminalDocument(generation, artifact_set, 3)
    objects[terminal.metadata_path] = terminal.to_json_bytes()
    root = ProvenanceRootDocument(
        generation,
        artifact_set,
        "urn:uuid:44444444-4444-4444-8444-444444444444",
        binding_count=513,
        binding_tree_sha256=binding_tree_commitment(
            MemberHistoryBinding.from_mapping(row) for row in bindings
        ).root_sha256,
        journal_count=1,
        ordered_volume_sha256=ordered_provenance_commitment((*descriptors, terminal)),
    )
    objects["provenance/root.json.age"] = root.to_json_bytes()

    def read_object(path: str):
        yield objects[path]

    reader = CanonicalProvenanceArchiveReader(
        read_object,
        expected_root_sha256=root.identity,
        archive_generation=generation,
        artifact_set_sha256=artifact_set,
    )
    assert [row["artifact_id"] for row in reader.iter_bindings()] == [
        row["artifact_id"] for row in bindings
    ]


def test_member_history_requires_exact_root_and_terminal_pages() -> None:
    reader, objects, journal_id, _ = _archive()
    primary = HistoryJournalAnchor(
        journal_id=journal_id,
        through_entry_id="urn:uuid:33333333-3333-4333-8333-333333333333",
        through_sequence=0,
        through_json_sha256="d" * 64,
        prefix_sha256="e" * 64,
        prefix_bytes=9,
    )
    completion = replace(
        primary,
        journal_id="urn:uuid:55555555-5555-4555-8555-555555555555",
        prefix_sha256="f" * 64,
    )
    selected = (MemberHistoryRoot(primary, "bound"), MemberHistoryRoot(completion, "retained"))
    rows = tuple(
        {"key": root.key, "value": root.to_mapping()}
        for root in sorted(selected, key=lambda root: root.key)
    )
    commitment = RecordSetCommitment(MEMBER_HISTORY_ROOTS_SCHEMA)
    for row in rows:
        commitment.update(row["key"], row["value"])
    roots = commitment.ref()
    imports = RecordSetCommitment(MEMBER_HISTORY_IMPORTS_SCHEMA).ref()
    history = MemberHistoryDocument(
        artifact_id="c" * 64,
        bytes=7,
        sha256="a" * 64,
        primary=MemberHistoryPrimary(primary, "urn:uuid:22222222-2222-4222-8222-222222222222"),
        roots=roots,
        imports=imports,
    )
    raw = history.to_json_bytes()
    binding = MemberHistoryBinding(
        history.artifact_id,
        history.bytes,
        history.sha256,
        history.identity,
        len(raw),
    )
    objects[member_history_object_path(history.identity)] = raw
    objects[history_record_page_object_path(roots.records_sha256, 0)] = RecordPage(
        roots, 0, rows, False
    ).to_json_bytes()
    root_terminal = history_record_page_object_path(roots.records_sha256, 1)
    objects[root_terminal] = RecordPage(roots, 1, (), True).to_json_bytes()
    objects[history_record_page_object_path(imports.records_sha256, 0)] = RecordPage(
        imports, 0, (), True
    ).to_json_bytes()

    assert reader.member_history(binding) == history
    assert list(reader.iter_selected_history_roots(binding, extent=BOUND_HISTORY_EXTENT)) == [
        selected[0]
    ]
    assert list(
        reader.iter_selected_history_roots(binding, extent=RETAINED_HISTORY_EXTENT)
    ) == sorted(selected, key=lambda root: root.key)
    del objects[root_terminal]
    with pytest.raises(KeyError):
        reader.member_history(binding)
