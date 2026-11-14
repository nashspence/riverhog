from __future__ import annotations

import sqlite3
from dataclasses import replace

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_growth_contracts import growth_binding
from riverhog_provenance import (
    CanonicalCorpusValidator,
    ProvenanceValidationError,
    create_journal,
    new_id,
    validate_journal,
)
from riverhog_provenance_contracts import canonical_document
from test_corpus_historical_source import SelectionObserver
from test_corpus_source_growth import source_reads
from test_journal_result import history
from test_source_anchor_growth import record


def test_source_endpoint_rejects_earlier_prefix_and_every_changed_coordinate(journal, catalog):
    raw = history(journal, 3, catalog)
    with validate_journal(raw, catalog=catalog) as current, CanonicalCorpusValidator() as corpus:
        corpus.add(current)
        endpoint = HistoryJournalAnchor.from_mapping(current.anchor)
        earlier = HistoryJournalAnchor.from_mapping(current.anchor_at(0))
        corpus.require_source_endpoint(endpoint)
        # Being a valid selectable prefix does not make this the complete source.
        corpus.select_source(earlier)
        for changed in (
            earlier,
            replace(endpoint, journal_id=new_id()),
            replace(endpoint, through_entry_id=new_id()),
            replace(endpoint, through_sequence=endpoint.through_sequence + 1),
            replace(endpoint, through_json_sha256="0" * 64),
            replace(endpoint, prefix_sha256="0" * 64),
            replace(endpoint, prefix_bytes=endpoint.prefix_bytes + 1),
        ):
            with pytest.raises(ProvenanceValidationError, match="indexed endpoint"):
                corpus.require_source_endpoint(changed)
        corpus.require_source_endpoint(endpoint)
        with pytest.raises(TypeError, match="exact history journal anchor"):
            corpus.require_source_endpoint(endpoint.to_mapping())


def test_source_endpoint_requires_complete_indexing_before_current_source_acceptance(
    journal, catalog
):
    raw = history(journal, 3, catalog)
    with validate_journal(raw, catalog=catalog) as current, CanonicalCorpusValidator() as corpus:
        endpoint = HistoryJournalAnchor.from_mapping(current.anchor)
        with pytest.raises(ProvenanceValidationError, match="indexed endpoint"):
            corpus.require_source_endpoint(endpoint)
        progress = corpus.advance_source(current, frame_budget=1)
        assert not progress.complete
        with pytest.raises(ProvenanceValidationError, match="indexed endpoint"):
            corpus.require_source_endpoint(endpoint)
        while not corpus.advance_source(current, frame_budget=1).complete:
            pass
        corpus.require_source_endpoint(endpoint)
    with pytest.raises(RuntimeError, match="closed"):
        corpus.require_source_endpoint(endpoint)


def _measure(corpus, endpoint, monkeypatch):
    actual = corpus._db
    observer = SelectionObserver(actual)
    dimensions = {
        "retained_sources": actual.execute("SELECT count(*) FROM source_extents").fetchone()[0],
        "retained_frames": actual.execute("SELECT count(*) FROM entries").fetchone()[0],
        "anchor_octets": len(canonical_document(endpoint.to_mapping())),
    }

    def stepped():
        observer.metrics["sqlite_vm_instructions"] += 1
        return 0

    def authorized(action, table, _column, *_rest):
        if action == sqlite3.SQLITE_READ and table not in {"source_extents", "entries"}:
            raise AssertionError("endpoint check read unrelated source or assertion data")
        return sqlite3.SQLITE_OK

    actual.set_progress_handler(stepped, 1)
    actual.set_authorizer(authorized)
    try:
        with source_reads(monkeypatch) as reads, monkeypatch.context() as patch:
            patch.setattr(corpus, "_db", observer)
            corpus.require_source_endpoint(endpoint)
        observer.metrics["source_frame_reads"] = len(reads)
    finally:
        actual.set_authorizer(None)
        actual.set_progress_handler(None, 0)
    assert all(cursor.closed for cursor in observer.cursors)
    observer.metrics["returned_metadata_octets"] = observer.metrics["metadata_octets"]
    return record(
        growth_binding(CanonicalCorpusValidator.require_source_endpoint),
        dimensions,
        observer.metrics,
    )


@pytest.mark.growth_witness(
    "native-corpus-source-endpoint-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus.CanonicalCorpusValidator.require_source_endpoint",
)
def test_source_endpoint_work_depends_on_index_seeks_not_source_extent(
    graph, who, catalog, monkeypatch, record_property
):
    samples = []
    for sources, frames in ((1, 1), (4, 1), (1, 17), (4, 17)):
        with CanonicalCorpusValidator() as corpus:
            for _ in range(sources):
                raw = history(
                    create_journal(graph, recorded_by_agent_id=who, catalog=catalog),
                    frames,
                    catalog,
                )
                with validate_journal(raw, catalog=catalog) as current:
                    corpus.add(current)
                    endpoint = HistoryJournalAnchor.from_mapping(current.anchor)
            samples.append(_measure(corpus, endpoint, monkeypatch))
    record_property("growth_samples", samples)
