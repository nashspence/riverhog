from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import patch

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor, MemberHistoryStore
from riverhog_provenance import (
    MemberHistoryClosure,
    ProvenanceValidationError,
    append_assertion_batches,
    assertion,
    external_reference,
    history,
    new_id,
    reference,
    validate_journal,
)
from riverhog_provenance_contracts import ContractCatalog


def test_history_reuses_validation_with_fresh_reads_and_isolated_views(
    journal: bytes, catalog: ContractCatalog
) -> None:
    anchor = HistoryJournalAnchor.from_mapping(validate_journal(journal, catalog=catalog).anchor)
    reads = 0
    closed = 0

    def read_journal(_identity: str, _end: int | None) -> Iterator[bytes]:
        nonlocal reads, closed
        reads += 1
        try:
            yield journal
        finally:
            closed += 1

    with (
        MemberHistoryClosure(
            MemberHistoryStore(lambda _path: ()),
            read_journal,
            member_role="member",
            catalog=catalog,
        ) as closure,
        patch.object(
            history, "validate_journal_chunks", wraps=history.validate_journal_chunks
        ) as parse,
    ):
        first = closure.summary_at(anchor)
        first.frames[0].document["recorded_at"] = "changed"
        view = first.graph
        view["states"][0]["id"] = "changed"
        second = closure.summary_at(anchor)
        assert second.anchor == anchor.to_mapping()
        assert second.frames[0].document["recorded_at"] != "changed"
        assert second.states[0]["id"] != "changed"
        assert reads == closed == 2
        assert parse.call_count == 1
    # Returned immutable summaries keep their own validation snapshots alive.
    assert first.states == second.states


@pytest.mark.parametrize("change", ["bytes", "short", "read_fence", "anchor"])
def test_history_validation_reuse_rejects_changed_bytes_fences_and_anchors(
    journal: bytes, catalog: ContractCatalog, change: str
) -> None:
    anchor = HistoryJournalAnchor.from_mapping(validate_journal(journal, catalog=catalog).anchor)
    changed = False

    def read_journal(_identity: str, _end: int | None) -> Iterator[bytes]:
        try:
            if changed and change == "bytes":
                yield b"\x1f" + journal[1:]
            elif changed and change == "short":
                yield journal[:-1]
            else:
                yield journal
        finally:
            if changed and change == "read_fence":
                raise PermissionError("current read fence revoked")

    with MemberHistoryClosure(
        MemberHistoryStore(lambda _path: ()), read_journal, member_role="member", catalog=catalog
    ) as closure:
        closure.summary_at(anchor)
        changed = True
        if change == "anchor":
            altered = anchor.to_mapping()
            altered["through"] = {**altered["through"], "entry_id": new_id()}
            anchor = HistoryJournalAnchor.from_mapping(altered)
        error = PermissionError if change == "read_fence" else ProvenanceValidationError
        with pytest.raises(error):
            closure.summary_at(anchor)


def test_journals_larger_than_history_validation_cache_remain_accepted(
    journal: bytes, who: str, catalog: ContractCatalog
) -> None:
    initial = validate_journal(journal, catalog=catalog)
    artifact_id = initial.graph["artifacts"][0]["id"]
    batches = (
        {
            "extensions": [
                assertion(
                    "extension",
                    who,
                    subject=reference(artifact_id, "artifact"),
                    property="urn:test:large-documentary-fact",
                    value={"type": "text", "value": "x" * (32 * 1024)},
                )
            ]
        }
        for _ in range(128)
    )
    large = append_assertion_batches(journal, batches, recorded_by_agent_id=who, catalog=catalog)
    assert len(large) > 4 * 1024 * 1024
    anchor = HistoryJournalAnchor.from_mapping(validate_journal(large, catalog=catalog).anchor)
    with (
        MemberHistoryClosure(
            MemberHistoryStore(lambda _path: ()),
            lambda _identity, _end: (large,),
            member_role="member",
            catalog=catalog,
        ) as closure,
        patch.object(
            history, "validate_journal_chunks", wraps=history.validate_journal_chunks
        ) as parse,
    ):
        assert closure.summary_at(anchor).anchor == anchor.to_mapping()
        assert closure.summary_at(anchor).anchor == anchor.to_mapping()
        assert parse.call_count == 2


def test_documentary_reference_index_reuses_parsing_with_complete_fenced_reads(
    journal: bytes, catalog: ContractCatalog
) -> None:
    summary = validate_journal(journal, catalog=catalog)
    references = [external_reference(summary, row["id"]) for row in summary.states]
    reads = closed = 0

    def read_journal(identity, _end):
        nonlocal reads, closed
        assert identity == summary.journal_id
        reads += 1
        try:
            # Preserve framing across hostile transport boundaries.
            for offset in range(0, len(journal), 137):
                yield journal[offset : offset + 137]
        finally:
            closed += 1

    with (
        MemberHistoryClosure(
            MemberHistoryStore(lambda _path: ()),
            read_journal,
            member_role="member",
            catalog=catalog,
        ) as closure,
        patch.object(history, "iter_journal_frames", wraps=history.iter_journal_frames) as parse,
    ):
        anchors = [closure._reference_anchor(row) for row in references]
        assert [closure._reference_anchor(row) for row in references] == anchors
        assert all(
            anchor.through_json_sha256 == row["entry"]["json_sha256"]
            for anchor, row in zip(anchors, references, strict=True)
        )
        assert reads == closed == 2 * len(references)
        assert parse.call_count == 1


def test_invalid_cold_reference_closes_its_provider_and_does_not_poison_retry(
    journal: bytes, catalog: ContractCatalog
) -> None:
    summary = validate_journal(journal, catalog=catalog)
    selected = external_reference(summary, summary.states[0]["id"])
    invalid, closed = True, 0

    def read_journal(_identity, _end):
        nonlocal closed
        try:
            if invalid:
                yield b"invalid framing"
            yield journal
        finally:
            closed += 1

    with MemberHistoryClosure(
        MemberHistoryStore(lambda _path: ()), read_journal, member_role="member", catalog=catalog
    ) as closure:
        with pytest.raises(ProvenanceValidationError):
            closure._reference_anchor(selected)
        assert closed == 1
        invalid = False
        assert (
            closure._reference_anchor(selected).through_json_sha256
            == selected["entry"]["json_sha256"]
        )
        assert closed == 2


@pytest.mark.parametrize("change", ["bytes", "short", "read_fence", "entry", "object", "assertion"])
def test_documentary_reference_reuse_preserves_exact_identity_and_current_read_fences(
    journal: bytes, catalog: ContractCatalog, change: str
) -> None:
    summary = validate_journal(journal, catalog=catalog)
    selected = external_reference(summary, summary.states[0]["id"])
    changed = False

    def read_journal(_identity, _end):
        try:
            if changed and change == "bytes":
                yield b"\x1f" + journal[1:]
            elif changed and change == "short":
                yield journal[:-1]
            else:
                yield journal
        finally:
            if changed and change == "read_fence":
                raise PermissionError("read authority revoked")

    with MemberHistoryClosure(
        MemberHistoryStore(lambda _path: ()), read_journal, member_role="member", catalog=catalog
    ) as closure:
        closure._reference_anchor(selected)
        changed = True
        if change == "entry":
            selected = {**selected, "entry": {**selected["entry"], "json_sha256": "0" * 64}}
        elif change == "object":
            selected = {**selected, "object_id": new_id()}
        elif change == "assertion":
            selected = {**selected, "assertion_id": new_id()}
        error = PermissionError if change == "read_fence" else ProvenanceValidationError
        with pytest.raises(error):
            closure._reference_anchor(selected)
