from __future__ import annotations

import gc
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest
from riverhog_archive_contracts import MemberHistoryStore
from riverhog_provenance import (
    JournalReferenceCache,
    MemberHistoryClosure,
    ProvenanceValidationError,
    append_checkpoint,
    external_reference,
    journal_reference_cache,
    new_id,
    validate_journal,
)
from riverhog_provenance_contracts import ContractCatalog


def test_separate_histories_reuse_documentary_parsing_without_sharing_selection(
    journal: bytes,
    catalog: ContractCatalog,
) -> None:
    summary = validate_journal(journal, catalog=catalog)
    references = [external_reference(summary, row["id"]) for row in summary.states]
    cache = JournalReferenceCache()
    reads = closed = 0

    def read(_identity, _end):
        nonlocal reads, closed
        reads += 1
        try:
            for offset in range(0, len(journal), 137):
                yield journal[offset : offset + 137]
        finally:
            closed += 1

    with patch.object(
        journal_reference_cache,
        "iter_journal_frames",
        wraps=journal_reference_cache.iter_journal_frames,
    ) as parse:
        anchors = []
        for selected in references * 2:
            with MemberHistoryClosure(
                MemberHistoryStore(lambda _path: ()),
                read,
                member_role="member",
                catalog=catalog,
                reference_cache=cache,
            ) as closure:
                anchors.append(closure._reference_anchor(selected))
                assert tuple(closure.snapshots()) == ()
        assert parse.call_count == 1
    assert reads == closed == len(anchors)
    assert anchors[: len(references)] == anchors[len(references) :]
    assert all(anchor.journal_id == summary.journal_id for anchor in anchors)


def test_shared_documentary_coordinates_keep_exact_fields_and_late_read_fences(
    journal: bytes,
    catalog: ContractCatalog,
) -> None:
    summary = validate_journal(journal, catalog=catalog)
    selected = external_reference(summary, summary.states[0]["id"])
    cache = JournalReferenceCache()
    expected, identity = cache.reference_anchor((journal,), selected)
    for altered in (
        {**selected, "journal_id": new_id()},
        {**selected, "object_id": new_id()},
        {**selected, "assertion_id": new_id()},
        {**selected, "entry": {**selected["entry"], "json_sha256": "f" * 64}},
    ):
        with pytest.raises(ProvenanceValidationError):
            cache.reference_anchor((journal,), altered)

    def revoked():
        yield journal
        raise PermissionError("fresh provider read fence")

    with pytest.raises(PermissionError, match="fresh provider"):
        cache.reference_anchor(revoked(), selected)
    with pytest.raises(ProvenanceValidationError):
        cache.reference_anchor((journal[:-1],), selected)
    assert cache.reference_anchor((journal,), selected, expected_identity=identity)[0] == expected


def test_documentary_lookup_does_not_validate_unselected_later_effective_graph(
    journal: bytes,
    who: str,
    catalog: ContractCatalog,
) -> None:
    summary = validate_journal(journal, catalog=catalog)
    selected = external_reference(summary, summary.states[0]["id"])
    cache = JournalReferenceCache()
    expected, identity = cache.reference_anchor((journal,), selected)
    extended = append_checkpoint(
        journal,
        recorded_by_agent_id=who,
        purpose="urn:test:documentary-checkpoint",
        catalog=catalog,
    )
    assert cache.reference_anchor((extended,), selected)[0] == expected
    with pytest.raises(ProvenanceValidationError, match="identity changed"):
        cache.reference_anchor((extended,), selected, expected_identity=identity)
    # Documentary addresses preserve a selected earlier prefix. Effective graph
    # validation of that prefix remains a separate obligation of its owner.
    from riverhog_canonical_json import canonical_json_bytes

    tail = validate_journal(extended, catalog=catalog).tail.document
    del tail["recorded_by_agent_id"]
    lexical = journal + b"\x1e" + canonical_json_bytes(tail) + b"\n"
    assert cache.reference_anchor((lexical,), selected)[0] == expected
    with pytest.raises(ProvenanceValidationError):
        validate_journal(lexical, catalog=catalog)


def test_capacity_bypass_eviction_and_concurrent_documentary_views_preserve_coordinates(
    journal: bytes,
    catalog: ContractCatalog,
) -> None:
    summary = validate_journal(journal, catalog=catalog)
    selected = external_reference(summary, summary.states[0]["id"])
    uncached = JournalReferenceCache(byte_budget=0)
    expected, identity = uncached.reference_anchor((journal,), selected)
    assert uncached.reference_anchor((journal,), selected)[0] == expected
    assert not uncached._entries and uncached._bytes == 0
    cache = JournalReferenceCache(entry_budget=1)
    cache.reference_anchor((journal,), selected)

    def read(_):
        return cache.reference_anchor((journal,), selected, expected_identity=identity)[0]

    with ThreadPoolExecutor(max_workers=4) as workers:
        assert list(workers.map(read, range(12))) == [expected] * 12
    assert len(cache._entries) == 1 and cache._bytes <= cache.byte_budget
    snapshot = next(iter(cache._entries.values())).path
    other = append_checkpoint(
        journal,
        recorded_by_agent_id=summary.tail.document["recorded_by_agent_id"],
        purpose="urn:test:other-documentary-bytes",
        catalog=catalog,
    )
    assert cache.reference_anchor((other,), selected)[0] == expected
    gc.collect()
    assert len(cache._entries) == 1 and not snapshot.exists()
