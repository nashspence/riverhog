from __future__ import annotations

import hashlib

import pytest
from riverhog_archive_contracts import (
    MEMBER_HISTORY_IMPORTS_SCHEMA,
    MEMBER_HISTORY_ROOTS_SCHEMA,
    HistoryJournalAnchor,
    MemberHistoryBinding,
    MemberHistoryDocument,
    MemberHistoryImport,
    MemberHistoryPrimary,
    MemberHistoryRoot,
    RecordPage,
    RecordSetCommitment,
    format_archive_sequence,
    history_record_page_object_path,
    member_history_object_path,
    source_binding_proof_object_path,
    verify_member_history_sets,
    verify_record_pages,
)
from riverhog_canonical_json import canonical_json_bytes


def _anchor(seed: str) -> HistoryJournalAnchor:
    return HistoryJournalAnchor(
        journal_id=f"urn:uuid:{seed * 8}-{seed * 4}-{seed * 4}-{seed * 4}-{seed * 12}",
        through_entry_id="urn:uuid:11111111-1111-4111-8111-111111111111",
        through_sequence=0,
        through_json_sha256="a" * 64,
        prefix_sha256=seed * 64,
        prefix_bytes=123,
    )


def test_member_history_commits_exact_primary_and_bounded_sets() -> None:
    primary = _anchor("1")
    completion = _anchor("2")
    rows = tuple(
        {"key": row.key, "value": row.to_mapping()}
        for row in sorted(
            (MemberHistoryRoot(primary, "bound"), MemberHistoryRoot(completion, "bound")),
            key=lambda item: item.key,
        )
    )
    root_commitment = RecordSetCommitment(MEMBER_HISTORY_ROOTS_SCHEMA)
    for row in rows:
        root_commitment.update(row["key"], row["value"])
    roots = root_commitment.ref()
    imports = RecordSetCommitment(MEMBER_HISTORY_IMPORTS_SCHEMA).ref()
    descriptor = MemberHistoryDocument(
        artifact_id="c" * 64,
        bytes=7,
        sha256=hashlib.sha256(b"derived").hexdigest(),
        primary=MemberHistoryPrimary(primary, "urn:uuid:33333333-3333-4333-8333-333333333333"),
        roots=roots,
        imports=imports,
    )
    assert MemberHistoryDocument.from_json_bytes(descriptor.to_json_bytes()) == descriptor
    assert descriptor.identity == hashlib.sha256(descriptor.to_json_bytes()).hexdigest()
    binding = MemberHistoryBinding(
        artifact_id=descriptor.artifact_id,
        bytes=descriptor.bytes,
        sha256=descriptor.sha256,
        history_sha256=descriptor.identity,
        history_bytes=len(descriptor.to_json_bytes()),
    )
    assert MemberHistoryBinding.from_mapping(binding.to_mapping()) == binding
    assert binding.verify_descriptor(descriptor.to_json_bytes()) == descriptor
    assert member_history_object_path(binding.history_sha256).endswith(
        f"/{binding.history_sha256}.json.age"
    )
    assert history_record_page_object_path(roots.records_sha256, 2).endswith(
        "/" + format_archive_sequence(2) + ".json.age"
    )
    with pytest.raises(ValueError, match="root binding"):
        binding.verify_descriptor(descriptor.to_json_bytes() + b" ")
    verify_record_pages(
        roots,
        (
            RecordPage(roots, 0, rows[:1], False),
            RecordPage(roots, 1, rows[1:], False),
            RecordPage(roots, 2, (), True),
        ),
    )
    verify_member_history_sets(
        descriptor,
        root_pages=(
            RecordPage(roots, 0, rows, False),
            RecordPage(roots, 1, (), True),
        ),
        import_pages=(RecordPage(imports, 0, (), True),),
    )
    with pytest.raises(ValueError, match="incomplete"):
        verify_record_pages(roots, (RecordPage(roots, 0, rows, False),))
    with pytest.raises(ValueError, match="strictly ordered"):
        duplicate = RecordSetCommitment(MEMBER_HISTORY_ROOTS_SCHEMA)
        duplicate.update(rows[0]["key"], rows[0]["value"])
        duplicate.update(rows[0]["key"], rows[0]["value"])
    with pytest.raises(ValueError, match="RFC 8785"):
        MemberHistoryDocument.from_json_bytes(
            b'{"sha256":"' + b"d" * 64 + b'","artifact_id":"' + b"c" * 64 + b'"}'
        )
    assert canonical_json_bytes(
        MemberHistoryRoot(primary, "bound").to_mapping()
    ) != canonical_json_bytes(MemberHistoryRoot(primary, "retained").to_mapping())


def test_member_history_rejects_missing_primary_and_conflicting_heads() -> None:
    primary = _anchor("1")
    other = _anchor("2")

    def selected(roots: tuple[MemberHistoryRoot, ...]) -> None:
        rows = tuple(
            {"key": root.key, "value": root.to_mapping()}
            for root in sorted(roots, key=lambda root: root.key)
        )
        commitment = RecordSetCommitment(MEMBER_HISTORY_ROOTS_SCHEMA)
        for row in rows:
            commitment.update(row["key"], row["value"])
        ref = commitment.ref()
        descriptor = MemberHistoryDocument(
            artifact_id="c" * 64,
            bytes=7,
            sha256="d" * 64,
            primary=MemberHistoryPrimary(primary, "urn:uuid:33333333-3333-4333-8333-333333333333"),
            roots=ref,
            imports=RecordSetCommitment(MEMBER_HISTORY_IMPORTS_SCHEMA).ref(),
        )
        verify_member_history_sets(
            descriptor,
            root_pages=(RecordPage(ref, 0, rows, False), RecordPage(ref, 1, (), True)),
            import_pages=(RecordPage(descriptor.imports, 0, (), True),),
        )

    with pytest.raises(ValueError, match="omits its exact primary"):
        selected((MemberHistoryRoot(other, "bound"),))
    with pytest.raises(ValueError, match="primary history root must be bound"):
        selected((MemberHistoryRoot(primary, "retained"),))
    replacement = HistoryJournalAnchor(
        journal_id=primary.journal_id,
        through_entry_id=primary.through_entry_id,
        through_sequence=primary.through_sequence,
        through_json_sha256=primary.through_json_sha256,
        prefix_sha256="f" * 64,
        prefix_bytes=primary.prefix_bytes,
    )
    with pytest.raises(ValueError, match="conflicting bound heads"):
        selected((MemberHistoryRoot(primary, "bound"), MemberHistoryRoot(replacement, "bound")))


def test_inherited_history_retains_source_scope_proof_and_selected_extent() -> None:
    selected = MemberHistoryImport(
        source_collection_id=17,
        source_archive_root_sha256="a" * 64,
        source_artifact_set_sha256="b" * 64,
        source_artifact_id="c" * 64,
        source_history_sha256="d" * 64,
        source_binding_proof_sha256="e" * 64,
        extent="bound-and-required-history",
        input_state={
            "scope": "external",
            "journal_id": "urn:uuid:11111111-1111-4111-8111-111111111111",
            "entry": {
                "entry_id": "urn:uuid:22222222-2222-4222-8222-222222222222",
                "sequence": "0",
                "json_sha256": "f" * 64,
            },
            "assertion_id": "urn:uuid:33333333-3333-4333-8333-333333333333",
            "object_id": "urn:uuid:44444444-4444-4444-8444-444444444444",
            "object_type": "state",
        },
    )
    assert MemberHistoryImport.from_mapping(selected.to_mapping()) == selected
    assert source_binding_proof_object_path(selected.source_binding_proof_sha256).endswith(
        "/" + selected.source_binding_proof_sha256 + ".json.age"
    )
    widened = {**selected.to_mapping(), "extent": "complete-retained-history"}
    assert MemberHistoryImport.from_mapping(widened).key == selected.key
    assert MemberHistoryImport.from_mapping(widened).extent != selected.extent
    wrong = {
        **selected.to_mapping(),
        "input_state": {**selected.input_state, "object_type": "artifact"},
    }
    with pytest.raises(ValueError, match="exact external State"):
        MemberHistoryImport.from_mapping(wrong)
