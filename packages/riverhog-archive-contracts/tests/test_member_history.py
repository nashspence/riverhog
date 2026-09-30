from __future__ import annotations

import hashlib

import pytest
from riverhog_archive_contracts import (
    MEMBER_HISTORY_IMPORTS_SCHEMA,
    MEMBER_HISTORY_ROOTS_SCHEMA,
    HistoryJournalAnchor,
    MemberHistoryBinding,
    MemberHistoryDocument,
    MemberHistoryPrimary,
    MemberHistoryRoot,
    RecordPage,
    RecordSetCommitment,
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
