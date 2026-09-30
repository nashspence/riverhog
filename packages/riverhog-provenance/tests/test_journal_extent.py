from __future__ import annotations

import tracemalloc
from tempfile import TemporaryFile

import pytest
from riverhog_provenance import (
    ProvenanceValidationError,
    assertion,
    encode_entry,
    new_id,
    reference,
    validate_graph,
    validate_journal,
    validate_journal_chunks,
    write_assertion_batches,
)
from riverhog_provenance.graph import GraphValidation
from riverhog_provenance.journal import _entry


def _entries(journal, who, catalog, count):
    seed = validate_journal(journal, catalog=catalog)
    artifact = next(iter(seed.graph_validation.view["artifacts"]))["id"]
    previous = seed.tail.reference
    yield journal
    for sequence in range(len(seed.frames), len(seed.frames) + count):
        document = _entry(
            journal_id=seed.journal_id,
            recorder_id=who,
            kind="assertion",
            body={
                "assertions": {
                    "extensions": [
                        assertion(
                            "extension",
                            who,
                            subject=reference(artifact, "artifact"),
                            property="urn:test:bounded-documentary-evidence",
                            value={"type": "text", "value": "a" * (64 * 1024)},
                        )
                    ]
                }
            },
            sequence=sequence,
            previous=previous,
            recorded_at="2026-01-01T00:00:00Z",
        )
        encoded = encode_entry(document, catalog=catalog)
        yield encoded
        from riverhog_provenance import JournalFrame

        previous = JournalFrame(encoded[1:-1]).reference


def test_streamed_snapshot_memory_does_not_grow_with_retained_documentary_bytes(
    journal, who, catalog, monkeypatch
):
    def materialization_forbidden(_self):
        raise AssertionError("stream validation must not materialize an effective graph")

    monkeypatch.setattr(GraphValidation, "graph", property(materialization_forbidden))

    def measure(count):
        tracemalloc.start()
        try:
            summary = validate_journal_chunks(
                _entries(journal, who, catalog, count), catalog=catalog
            )
            peak = tracemalloc.get_traced_memory()[1]
        finally:
            tracemalloc.stop()
        assert len(summary.frames) == count + 1
        assert len(summary.graph_validation.objects) >= count
        assert sum(1 for _ in summary.graph_validation.view["extensions"]) == count
        return peak

    small, larger = measure(4), measure(48)
    assert larger < small + 1024 * 1024


def test_every_prefix_is_validated_even_when_the_last_graph_would_be_valid(journal, who, catalog):
    seed = validate_journal(journal, catalog=catalog)
    missing = new_id()
    early = assertion(
        "extension",
        who,
        subject=reference(missing, "artifact"),
        property="urn:test:future-reference",
        value={"type": "text", "value": "future"},
    )
    later = assertion(
        "artifact", who, object_id=missing, continuity_policy_uri="urn:test:continuity"
    )
    with (
        TemporaryFile("w+b") as spool,
        pytest.raises(ProvenanceValidationError, match="unresolved local"),
    ):
        write_assertion_batches(
            spool,
            journal,
            ({"extensions": [early]}, {"artifacts": [later]}),
            recorded_by_agent_id=who,
            catalog=catalog,
        )
    final_graph = seed.graph
    final_graph["artifacts"].append(later)
    final_graph.setdefault("extensions", []).append(early)
    validate_graph(final_graph, catalog=catalog)


def test_streamed_snapshot_keeps_canonical_graph_validation_results(journal, who, catalog):
    summary = validate_journal_chunks(_entries(journal, who, catalog, 5), catalog=catalog)
    ordinary = validate_graph(summary.graph, catalog=catalog, journal_id=summary.journal_id)
    assert summary.graph_validation.objects == ordinary.objects
    assert set(map(str, summary.graph_validation.external_references)) == set(
        map(str, ordinary.external_references)
    )
    assert set(summary.findings) == set(ordinary.findings)
