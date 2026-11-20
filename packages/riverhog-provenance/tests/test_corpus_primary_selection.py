from __future__ import annotations

import copy
import json
from collections import defaultdict
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_growth_contracts import growth_binding
from riverhog_provenance import (
    CanonicalCorpusValidator,
    JournalValidationCapacity,
    ProvenanceValidationError,
    append_assertions,
    append_checkpoint,
    append_correction,
    assertion,
    assertion_reference,
    new_id,
    parse_journal,
    reference,
    validate_journal,
)
from riverhog_provenance import corpus_selection as owner
from riverhog_provenance_contracts import canonical_document
from sha256_continuation import CheckpointSHA256
from test_corpus_delivery_support import delivery_source
from test_corpus_source_growth import source_reads
from test_source_anchor_growth import record


def source_extensions(journal, who, catalog, *, unrelated=0):
    with validate_journal(journal, catalog=catalog) as summary:
        subject = reference(summary.graph["artifacts"][0]["id"], "artifact")
    rows = [
        assertion(
            "extension",
            who,
            subject=subject,
            property="urn:test:selected",
            value={"type": "text", "value": str(index)},
        )
        for index in range(4)
    ]
    extra = [
        assertion(
            "extension",
            who,
            subject=subject,
            property="urn:test:unrelated",
            value={"type": "text", "value": "x" * 1000},
        )
        for _ in range(unrelated)
    ]
    raw = append_assertions(
        journal, {"extensions": rows + extra}, recorded_by_agent_id=who, catalog=catalog
    )
    return raw, subject, rows


def test_index_is_an_explicit_empty_source_capability(journal, catalog):
    with (
        validate_journal(journal, catalog=catalog) as summary,
        CanonicalCorpusValidator() as corpus,
    ):
        corpus.add(summary)
        with pytest.raises(ValueError, match="before source indexing"):
            owner.enable_selection(corpus)
        assert (
            corpus._db.execute(
                "SELECT name FROM sqlite_master WHERE name='primary_selection_payloads'"
            ).fetchone()
            is None
        )


def test_every_selected_prefix_matches_raw_oracle_through_retirement_gap_and_replacement(
    journal, who, catalog
):
    raw, subject, rows = source_extensions(journal, who, catalog)
    with validate_journal(raw, catalog=catalog) as summary:
        target = assertion_reference(summary, rows[0]["assertion_id"])
    raw = append_correction(
        raw, [target], reason="withdrawn", recorded_by_agent_id=who, catalog=catalog
    )
    raw = append_checkpoint(raw, purpose="urn:test:gap", recorded_by_agent_id=who, catalog=catalog)
    replacement = {**rows[0], "assertion_id": new_id()}
    raw = append_assertions(
        raw, {"extensions": [replacement]}, recorded_by_agent_id=who, catalog=catalog
    )
    with validate_journal(raw, catalog=catalog) as summary, CanonicalCorpusValidator() as corpus:
        owner.enable_selection(corpus)
        corpus.advance_source(summary, frame_budget=len(summary.frames))
        frames = parse_journal(raw)
        revisions = []
        for sequence in range(len(frames)):
            prefix = b"".join(frame.encoded for frame in frames[: sequence + 1])
            with validate_journal(prefix, catalog=catalog) as oracle:
                selected = owner.HistoricalSelection(
                    corpus, HistoryJournalAnchor.from_mapping(oracle.anchor), byte_budget=4096
                )
                revisions.append(
                    owner.selection_revision(
                        corpus,
                        selected.anchor,
                        kind="extension",
                        subject=subject,
                        predicates=("urn:test:selected", "urn:test:absent"),
                    )
                )
                expected = sorted(
                    (row["assertion_id"], row["value"])
                    for row in oracle.graph_validation.view.get("extensions", ())
                    if row["subject"] == subject and row["property"] == "urn:test:selected"
                )
                actual = list(
                    selected.extensions(subject=subject, properties=("urn:test:selected",))
                )
                assert [(row["assertion_id"], row["value"]) for row in actual] == expected
                point = selected.object(rows[0]["id"])
                raw_point = oracle.graph_validation.objects.get(rows[0]["id"])
                assert (None if point is None else point["assertion_id"]) == (
                    None if raw_point is None else raw_point["assertion_id"]
                )
        assert revisions == [(None, None), ("1", None), ("2", None), ("2", None), ("4", None)]


def test_pages_resume_by_detached_key_and_require_exact_source_coordinates(journal, who, catalog):
    raw, subject, rows = source_extensions(journal, who, catalog, unrelated=64)
    with validate_journal(raw, catalog=catalog) as summary, CanonicalCorpusValidator() as corpus:
        owner.enable_selection(corpus)
        assert owner.source_extent(corpus, summary.journal_id) is None
        corpus.advance_source(summary, frame_budget=3)
        assert owner.source_extent(corpus, summary.journal_id) == (
            len(summary.frames),
            summary.journal_bytes,
        )
        anchor = HistoryJournalAnchor.from_mapping(summary.anchor)
        after, found = None, []
        while True:
            page = owner.selected_rows(
                corpus,
                anchor,
                kind="extension",
                subject=subject,
                predicates=("urn:test:selected",),
                byte_budget=4096,
                row_budget=1,
                after=after,
            )
            assert len(page.rows) <= 1
            found.extend(row["assertion_id"] for row in page.rows)
            if page.complete:
                break
            assert page.after != after
            after = page.after
        assert found == sorted(row["assertion_id"] for row in rows)
        with pytest.raises(ProvenanceValidationError, match="exact corpus source"):
            owner.selected_rows(
                corpus,
                replace(anchor, prefix_sha256="0" * 64),
                kind="extension",
                subject=subject,
                predicates=("urn:test:selected",),
                byte_budget=4096,
            )
        before = corpus._db.total_changes
        with pytest.raises(JournalValidationCapacity, match="byte budget"):
            owner.selected_rows(
                corpus,
                anchor,
                kind="extension",
                subject=subject,
                predicates=("urn:test:selected",),
                byte_budget=1,
            )
        assert corpus._db.total_changes == before


def measure_page(corpus, anchor, subject, predicate, monkeypatch):
    metrics, children, active = defaultdict(int), [], []
    actual_read, actual_object = corpus._source_read, owner.selected_object
    counts = {
        "retained_assertions": corpus._db.execute("SELECT count(*) FROM assertions").fetchone()[0],
        "retained_frames": corpus._db.execute("SELECT count(*) FROM entries").fetchone()[0],
        "anchor_octets": len(canonical_document(anchor.to_mapping())),
    }

    def read(query, parameters):
        result = actual_read(query, parameters)
        target = active[-1] if active else metrics
        target["sql_statements" if active else "selector_sql_statements"] += 1
        if result is not None:
            target["returned_rows" if active else "selector_metadata_rows"] += 1
            target["payload_octets" if "substr(" in query else "metadata_octets"] += sum(
                len(value if isinstance(value, bytes) else str(value).encode())
                for value in result
                if value is not None
            )
        return result

    def stepped():
        metrics["sqlite_vm_instructions"] += 1
        if active:
            active[-1]["sqlite_vm_instructions"] += 1
        return 0

    def decoded(value):
        assert active
        active[-1]["json_decodes"] += 1
        return json.loads(value)

    def select(*args, **kwargs):
        child = defaultdict(int)
        active.append(child)
        try:
            result = actual_object(*args, **kwargs)
        finally:
            active.pop()
        child_dimensions = {**counts, "selected_octets": len(canonical_document(result))}
        children.append(record(growth_binding(actual_object), child_dimensions, child))
        return result

    corpus._db.set_progress_handler(stepped, 1)
    try:
        with source_reads(monkeypatch) as reads, monkeypatch.context() as patch:
            patch.setattr(corpus, "_source_read", read)
            patch.setattr(owner, "selected_object", select)
            patch.setattr(owner, "json", SimpleNamespace(loads=decoded))
            result = owner.selected_rows(
                corpus,
                anchor,
                kind="extension",
                subject=subject,
                predicates=(predicate,),
                byte_budget=4096,
                row_budget=2,
            )
        assert not reads
    finally:
        corpus._db.set_progress_handler(None, 0)
    metrics["selected_rows"] = metrics["selected_object_calls"] = len(result.rows)
    for bound in growth_binding(actual_object).declaration.bounds:
        metrics["object_" + bound.metric] = sum(
            child["metrics"][bound.metric] for child in children
        )
    dimensions = {
        "predicates": 1,
        "row_budget": 2,
        "selected_rows": len(result.rows),
        "indexed_assertions": counts["retained_assertions"],
        **{"object_" + key: value for key, value in counts.items()},
        "object_selected_octets": max(
            (child["dimensions"]["selected_octets"] for child in children), default=0
        ),
    }
    return result, record(
        growth_binding(owner.selected_rows), dimensions, metrics, {"object": children}
    )


@pytest.mark.growth_witness(
    "native-primary-historical-selector-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus_selection.selected_rows",
)
def test_sparse_selection_declared_work_is_independent_of_unrelated_source_extent(
    journal, who, catalog, monkeypatch, record_property
):
    samples = []
    for unrelated in (0, 128):
        raw, subject, _ = source_extensions(journal, who, catalog, unrelated=unrelated)
        with (
            validate_journal(raw, catalog=catalog) as summary,
            CanonicalCorpusValidator() as corpus,
        ):
            owner.enable_selection(corpus)
            corpus.advance_source(summary, frame_budget=len(summary.frames))
            anchor = HistoryJournalAnchor.from_mapping(summary.anchor)
            for predicate in ("urn:test:selected", "urn:test:absent"):
                result, sample = measure_page(corpus, anchor, subject, predicate, monkeypatch)
                assert len(result.rows) == (2 if predicate.endswith("selected") else 0)
                assert sample["metrics"]["selector_sql_statements"] == 65 + len(result.rows)
                samples.append(sample)
    record_property("growth_samples", samples)


def test_member_object_projection_omits_independent_opaque_documentary_bytes(
    graph, who, catalog, monkeypatch
):
    sizes = []
    for opaque in (0, 65536):
        with delivery_source(copy.deepcopy(graph), who, catalog, opaque_octets=opaque) as (
            summary,
            args,
        ):
            with CanonicalCorpusValidator() as corpus:
                owner.enable_selection(corpus)
                corpus.advance_source(summary, frame_budget=len(summary.frames))
                selection = owner.HistoricalSelection(
                    corpus, HistoryJournalAnchor.from_mapping(summary.anchor), byte_budget=4096
                )
                with source_reads(monkeypatch) as reads:
                    association = selection.objects[args["binding"]["delivery_association_id"]]
                    state = selection.objects[association["state"]["object_id"]]
                    occurrence = selection.objects[state["occurrence_id"]]
                    observation = selection.objects[association["verification_observation_id"]]
                assert not reads
                assert "identifiers" not in occurrence and "note" not in observation
                assert observation["evidence"] == [{"basis": "direct_measurement"}]
                assert observation["content"]["size_bytes"] == str(args["byte_count"])
                assert observation["content"]["digests"] == [
                    {"algorithm": "sha-256", "value": args["sha256"]}
                ]
                sizes.append(
                    sum(
                        len(canonical_document(row))
                        for row in (association, state, occurrence, observation)
                    )
                )
    assert sizes[0] == sizes[1]


def test_interval_cover_contains_the_existing_native_hash_domain_without_a_new_archive_cap():
    maximum_hash_bytes = (2**64 - 1) // 8
    assert maximum_hash_bytes // 2 < owner._END
    state = json.loads(CheckpointSHA256().export_state())
    state.update(total_bytes=maximum_hash_bytes, buffer_hex="00" * (maximum_hash_bytes % 64))
    limit = CheckpointSHA256.from_state(json.dumps(state, sort_keys=True, separators=(",", ":")))
    with pytest.raises(ValueError, match="message-length domain"):
        limit.update_bytes(b"x")
    state.update(total_bytes=maximum_hash_bytes + 1, buffer_hex="")
    with pytest.raises(ValueError, match="checkpoint"):
        CheckpointSHA256.from_state(json.dumps(state, sort_keys=True, separators=(",", ":")))
    for first, stop in ((0, 1), (1, 2**63), (2**62 - 1, 2**62 + 1), (1, 2**63 - 1)):
        nodes = list(owner._cover(first, stop))
        assert len(nodes) <= 126
        for point in {max(0, first - 1), first, stop - 1, min(stop, 2**63 - 1)}:
            assert len(set(nodes) & set(owner._path(point))) == int(first <= point < stop)


@pytest.mark.growth_witness(
    "native-primary-selector-revision-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus_selection.selection_revision",
)
def test_context_revision_reuses_facts_across_distinct_unchanged_anchors(
    journal, who, catalog, monkeypatch, record_property
):
    raw, subject, _ = source_extensions(journal, who, catalog)
    raw = append_checkpoint(
        raw, purpose="urn:test:later", recorded_by_agent_id=who, catalog=catalog
    )
    samples = []
    with validate_journal(raw, catalog=catalog) as summary, CanonicalCorpusValidator() as corpus:
        owner.enable_selection(corpus)
        corpus.advance_source(summary, frame_budget=len(summary.frames))
        events = corpus._db.execute("SELECT count(*) FROM primary_selection_events").fetchone()[0]
        revisions = []
        for sequence in (0, 1, 2):
            anchor = HistoryJournalAnchor.from_mapping(summary.anchor_at(sequence))
            metrics = defaultdict(int)
            actual_read = corpus._source_read

            def read(query, parameters, actual_read=actual_read, metrics=metrics):
                result = actual_read(query, parameters)
                metrics["sql_statements"] += 1
                metrics["returned_rows"] += int(result is not None)
                assert "substr(" not in query
                assert "assertion_payloads" not in query
                assert "primary_selection_payloads" not in query
                return result

            def stepped(metrics=metrics):
                metrics["sqlite_vm_instructions"] += 1
                return 0

            corpus._db.set_progress_handler(stepped, 1)
            try:
                with monkeypatch.context() as patch:
                    patch.setattr(corpus, "_source_read", read)
                    revisions.append(
                        owner.selection_revision(
                            corpus,
                            anchor,
                            kind="extension",
                            subject=subject,
                            predicates=("urn:test:selected", "urn:test:absent"),
                        )
                    )
            finally:
                corpus._db.set_progress_handler(None, 0)
            samples.append(
                record(
                    growth_binding(owner.selection_revision),
                    {"predicates": 2, "indexed_events": events},
                    metrics,
                )
            )
        assert revisions == [(None, None), ("1", None), ("1", None)]
    record_property("growth_samples", samples)
