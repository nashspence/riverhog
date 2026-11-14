from __future__ import annotations

import copy
import json
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_growth_contracts import growth_binding
from riverhog_provenance import (
    CanonicalCorpusValidator,
    JournalValidationCapacity,
    assertion,
    create_journal,
    external_reference,
    reference,
    selected_delivery_occurrence,
    validate_journal,
)
from riverhog_provenance import corpus as owner
from riverhog_provenance_contracts import canonical_document
from test_corpus_historical_source import SelectionObserver
from test_corpus_source_growth import source_reads
from test_source_anchor_growth import record


@contextmanager
def delivery_source(graph, who, catalog, *, opaque_octets=0, role="urn:test:member"):
    graph = copy.deepcopy(graph)
    occurrence = graph["occurrences"][0]
    occurrence["identifiers"] = [
        {
            "scheme": "urn:test:opaque",
            "value": {"kind": "text", "text": "x" * opaque_octets or "small"},
            "scope": "global",
        }
    ]
    observation = next(value for value in graph["descriptions"] if value["type"] == "observation")
    observation["note"] = "x" * opaque_octets or "small"
    context = assertion("context", who, kind="delivery")
    association = assertion(
        "delivery_association",
        who,
        delivery_context_id=context["id"],
        slot={"kind": "text", "text": "member"},
        role=role,
        state=reference(graph["states"][0]["id"], "state"),
        verification_observation_id=observation["id"],
    )
    graph["contexts"] = [*graph.get("contexts", ()), context]
    graph["delivery_associations"] = [association]
    raw = create_journal(graph, recorded_by_agent_id=who, catalog=catalog)
    with validate_journal(raw, catalog=catalog) as summary:
        arguments = {
            "binding": {
                "artifact_id": "member",
                "journal": summary.anchor,
                "delivery_association_id": association["id"],
            },
            "artifact_id": "member",
            "byte_count": int(observation["content"]["size_bytes"]),
            "sha256": next(
                d["value"] for d in observation["content"]["digests"] if d["algorithm"] == "sha-256"
            ),
            "member_role": role,
        }
        yield summary, arguments


def measure_support(corpus, arguments, selected_octets, monkeypatch):
    observer = SelectionObserver(corpus._db)
    dimensions = {
        "retained_assertions": corpus._db.execute("SELECT count(*) FROM assertions").fetchone()[0],
        "retained_frames": corpus._db.execute("SELECT count(*) FROM entries").fetchone()[0],
        "anchor_octets": len(canonical_document(arguments["binding"]["journal"])),
        "selected_octets": selected_octets,
    }

    def decoded(value):
        observer.metrics["json_decodes"] += 1
        observer.metrics["decoded_octets"] += len(value)
        return json.loads(value)

    def stepped():
        observer.metrics["sqlite_vm_instructions"] += 1
        return 0

    def read(query, parameters):
        if "assertion_payloads" in query:
            observer.metrics["full_assertion_reads"] += 1
            raise AssertionError("delivery support read a full assertion")
        return original_read(query, parameters)

    original_read = corpus._source_read
    corpus._db.set_progress_handler(stepped, 1)
    try:
        with source_reads(monkeypatch) as reads, monkeypatch.context() as patch:
            patch.setattr(corpus, "_db", observer)
            patch.setattr(corpus, "_source_read", read)
            patch.setattr(owner, "json", SimpleNamespace(loads=decoded))
            result = corpus.selected_delivery_support(**arguments, metadata_byte_budget=4096)
        observer.metrics["source_frame_reads"] = len(reads)
    finally:
        observer.actual.set_progress_handler(None, 0)
    assert all(cursor.closed for cursor in observer.cursors)
    sample = record(
        growth_binding(owner.CanonicalCorpusValidator.selected_delivery_support),
        dimensions,
        observer.metrics,
    )
    return result, sample, observer.queries


@pytest.mark.growth_witness(
    "native-corpus-selected-delivery-support-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus.CanonicalCorpusValidator.selected_delivery_support",
)
def test_delivery_support_reads_narrow_metadata_across_opaque_payload_growth(
    graph, who, catalog, monkeypatch, record_property
):
    samples, selected_sizes = [], []
    for opaque_octets in (0, 2048, 65536):
        with (
            delivery_source(graph, who, catalog, opaque_octets=opaque_octets) as (
                summary,
                arguments,
            ),
            CanonicalCorpusValidator() as corpus,
        ):
            progress = corpus.advance_source(summary, frame_budget=1)
            assert progress.complete
            metadata_columns = {
                row[1] for row in corpus._db.execute("PRAGMA table_info(assertions)")
            }
            assert "value" not in metadata_columns
            assert (
                corpus._db.execute("SELECT max(length(value)) FROM assertion_payloads").fetchone()[
                    0
                ]
                >= opaque_octets
            )
            expected = selected_delivery_occurrence(summary, **arguments)
            expected_references = (
                external_reference(summary, expected[0]["id"]),
                external_reference(summary, expected[1]["id"]),
            )
            if opaque_octets == 65536:
                with pytest.raises(JournalValidationCapacity):
                    corpus.historical_assertion(
                        HistoryJournalAnchor.from_mapping(summary.anchor),
                        expected[1]["id"],
                        byte_budget=4096,
                    )
            count, selected_octets = corpus._db.execute(
                "SELECT count(*),sum(length(a.entry)+length(p.value)) "
                "FROM assertions a JOIN delivery_projections p "
                "ON p.journal=a.journal AND p.assertion=a.assertion"
            ).fetchone()
            assert count == 4
            for _ in range(3):
                result, sample, queries = measure_support(
                    corpus, arguments, selected_octets, monkeypatch
                )
                assert (result.state_reference, result.occurrence_reference) == expected_references
                assert len(queries) == 9
                assert sample["metrics"]["json_decodes"] == 8
                assert sample["metrics"]["source_frame_reads"] == 0
                assert sample["metrics"]["full_assertion_reads"] == 0
                assert sample["metrics"]["payload_octets"] < 4096
                selected_sizes.append(sample["metrics"]["payload_octets"])
                samples.append(sample)
    assert max(selected_sizes) - min(selected_sizes) < 32
    record_property("growth_samples", samples)


@pytest.mark.parametrize("budget", [0, -1, True, "4096", None])
def test_delivery_support_checks_admission_before_any_sql(graph, who, catalog, monkeypatch, budget):
    with (
        delivery_source(graph, who, catalog) as (summary, arguments),
        CanonicalCorpusValidator() as corpus,
    ):
        corpus.advance_source(summary)

        def no_read(*_):
            raise AssertionError("invalid admission performed SQL")

        monkeypatch.setattr(corpus, "_source_read", no_read)
        with pytest.raises(ValueError, match="byte budget"):
            corpus.selected_delivery_support(**arguments, metadata_byte_budget=budget)


def test_large_matching_role_needs_explicit_metadata_admission_before_payload_read(
    graph, who, catalog, monkeypatch
):
    role = "urn:test:" + "r" * 65536
    with (
        delivery_source(graph, who, catalog, role=role) as (summary, arguments),
        CanonicalCorpusValidator() as corpus,
    ):
        corpus.advance_source(summary)
        original_read, queries = corpus._source_read, []

        def read(query, parameters):
            queries.append(query)
            return original_read(query, parameters)

        monkeypatch.setattr(corpus, "_source_read", read)
        with pytest.raises(JournalValidationCapacity, match="admitted byte budget"):
            corpus.selected_delivery_support(**arguments, metadata_byte_budget=4096)
        assert len(queries) == 2
        assert all("substr(" not in query for query in queries)
        expected = selected_delivery_occurrence(summary, **arguments)
        actual = corpus.selected_delivery_support(**arguments, metadata_byte_budget=131072)
        assert actual.state_reference == external_reference(summary, expected[0]["id"])
        assert actual.occurrence_reference == external_reference(summary, expected[1]["id"])
