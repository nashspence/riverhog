from __future__ import annotations

import copy
import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_growth_contracts import growth_binding
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    CanonicalCorpusValidator,
    ProvenanceValidationError,
    append_assertions,
    append_correction,
    assertion,
    assertion_reference,
    create_journal,
    external_reference,
    new_id,
    parse_journal,
    reference,
    validate_journal,
)
from riverhog_provenance import corpus as owner_module
from test_corpus_historical_source import SelectionObserver
from test_corpus_source_growth import source_reads
from test_journal_result import history
from test_source_anchor_growth import record


def edge_history(journal, who, catalog):
    with validate_journal(journal, catalog=catalog) as original:
        state = original.states[0]["id"]
        other = assertion(
            "state",
            who,
            occurrence_id=original.states[0]["occurrence_id"],
            extent={"kind": "unknown", "reason": "uncertain extent"},
        )
        edge = assertion(
            "derivation",
            who,
            used_state=reference(state, "state"),
            generated_state=reference(other["id"], "state"),
            kind="copy",
        )
    selected = append_assertions(
        journal,
        {"states": [other], "relations": [edge]},
        recorded_by_agent_id=who,
        catalog=catalog,
    )
    with validate_journal(selected, catalog=catalog) as source:
        target = assertion_reference(source, edge["assertion_id"])
    retired = append_correction(
        selected,
        [target],
        reason="withdrawn",
        recorded_by_agent_id=who,
        catalog=catalog,
    )
    return retired, edge


def test_all_native_prefixes_match_independent_selected_prefix_oracles(journal, who, catalog):
    raw, edge = edge_history(journal, who, catalog)
    with validate_journal(raw, catalog=catalog) as source:
        frames = parse_journal(raw)
        for selected_sequences in ((0,), (1,), (2,), (0, 2), (1, 2), (0, 1, 2)):
            expected = set()
            with CanonicalCorpusValidator() as corpus:
                corpus.advance_source(source, frame_budget=len(frames))
                for sequence in selected_sequences:
                    prefix = b"".join(frame.encoded for frame in frames[: sequence + 1])
                    with validate_journal(prefix, catalog=catalog) as oracle:
                        corpus.select_source(HistoryJournalAnchor.from_mapping(oracle.anchor))
                        for row in oracle.graph_validation.objects.values():
                            if row["type"] == "derivation":
                                expected.add(
                                    (
                                        "derivation",
                                        row["used_state"]["object_id"],
                                        row["generated_state"]["object_id"],
                                    )
                                )
                previous = 0
                while True:
                    progress = corpus.advance_semantics(row_budget=1, byte_budget=4096)
                    assert previous <= progress.processed_assertions <= previous + 1
                    previous = progress.processed_assertions
                    if progress.complete:
                        break
                assert set(corpus._db.execute("SELECT kind,source,target FROM edges")) == expected
                corpus.validate()


def test_add_registers_selection_without_traversing_effective_snapshot(
    journal, monkeypatch, catalog
):
    with validate_journal(journal, catalog=catalog) as source, CanonicalCorpusValidator() as corpus:
        mapping = type(source.graph_validation.objects)

        def forbidden(*_):
            raise AssertionError("corpus add traversed an effective snapshot")

        with monkeypatch.context() as patch:
            patch.setattr(mapping, "values", forbidden)
            corpus.add(source)
        corpus.validate()


@pytest.mark.parametrize(
    "rows,octets", [(0, 4096), (-1, 4096), (True, 4096), (1, 0), (1, -1), (1, True)]
)
def test_semantic_admission_rejects_before_freezing_or_querying(
    journal, catalog, monkeypatch, rows, octets
):
    with validate_journal(journal, catalog=catalog) as source, CanonicalCorpusValidator() as corpus:
        corpus.add(source)
        original = corpus._source_read

        def forbidden(*_):
            raise AssertionError("invalid work admission performed SQL")

        with monkeypatch.context() as patch:
            patch.setattr(corpus, "_source_read", forbidden)
            with pytest.raises(ValueError, match="budget"):
                corpus.advance_semantics(row_budget=rows, byte_budget=octets)
        corpus._source_read = original
        corpus.add(source)


def test_selection_and_sources_are_frozen_during_one_semantic_attempt(journal, who, catalog):
    raw, edge = edge_history(journal, who, catalog)
    with validate_journal(raw, catalog=catalog) as source, CanonicalCorpusValidator() as corpus:
        corpus.add(source)
        corpus.advance_semantics(row_budget=1, byte_budget=4096)
        with pytest.raises(RuntimeError, match="frozen"):
            corpus.select_source(HistoryJournalAnchor.from_mapping(source.anchor))
        with pytest.raises(RuntimeError, match="frozen"):
            corpus.add(source)


def test_capacity_and_interruption_retry_the_same_semantic_atom(journal, who, catalog, monkeypatch):
    raw, edge = edge_history(journal, who, catalog)
    from riverhog_provenance import JournalValidationCapacity

    class Interrupted(BaseException):
        pass

    with validate_journal(raw, catalog=catalog) as source, CanonicalCorpusValidator() as corpus:
        corpus.advance_source(source, frame_budget=3)
        corpus.select_source(HistoryJournalAnchor.from_mapping(source.anchor_at(1)))
        with pytest.raises(JournalValidationCapacity):
            corpus.advance_semantics(row_budget=1, byte_budget=1)
        write = corpus._source_write

        def interrupt(query, parameters=()):
            write(query, parameters)
            if query.startswith("INSERT OR IGNORE INTO edges"):
                raise Interrupted()

        with monkeypatch.context() as patch:
            patch.setattr(corpus, "_source_write", interrupt)
            with pytest.raises(Interrupted):
                corpus.advance_semantics(row_budget=1, byte_budget=4096)
        assert corpus._db.execute("SELECT count(*) FROM edges").fetchone() == (0,)
        first = corpus.advance_semantics(row_budget=1, byte_budget=4096)
        assert first.processed_assertions == 1
        assert corpus.advance_semantics(row_budget=1, byte_budget=4096).complete
        corpus.validate()


@pytest.mark.parametrize(
    "same,missing,result,valid",
    [
        (True, False, "matching_fixity", True),
        (False, False, "matching_fixity", False),
        (True, False, "different", False),
        (False, False, "different", True),
        (True, True, "indeterminate", True),
        (True, True, "matching_fixity", False),
    ],
)
def test_corpus_comparison_uses_native_fixed_size_sha_policy_without_payload_reads(
    graph, who, catalog, monkeypatch, same, missing, result, valid
):
    graph = copy.deepcopy(graph)
    content = graph["descriptions"][0]["content"]
    reported = assertion(
        "reported_description",
        who,
        state=reference(graph["states"][0]["id"], "state"),
        note="x" * 65536,
    )
    if not missing:
        reported["content"] = copy.deepcopy(content)
        reported["content"]["digests"].append({"algorithm": "sha-512", "value": "0" * 128})
    graph["descriptions"].append(reported)
    with validate_journal(
        create_journal(graph, recorded_by_agent_id=who, catalog=catalog), catalog=catalog
    ) as left:
        observed = BoundedSourceObserver(catalog=catalog).observe(
            BytesSource(b"opaque primary bytes" + bytes([0, 255]) if same else b"different")
        )
        child_graph = observed.graph_fragment()
        child_observation = next(
            row for row in child_graph["descriptions"] if row["type"] == "observation"
        )
        child_observation["content"]["digests"].append({"algorithm": "sha-512", "value": "1" * 128})
        child_graph["relations"] = [
            assertion(
                "content_comparison",
                who,
                left_description=external_reference(left, reported["id"]),
                right_description=reference(child_observation["id"], "observation"),
                result=result,
                algorithm="sha-256",
            )
        ]
        raw = create_journal(child_graph, recorded_by_agent_id=who, catalog=catalog)
        with validate_journal(raw, catalog=catalog) as right, CanonicalCorpusValidator() as corpus:
            corpus.add(left)
            corpus.add(right)
            original = corpus._source_read

            def read(query, parameters):
                assert "assertion_payloads" not in query
                return original(query, parameters)

            with source_reads(monkeypatch) as frames, monkeypatch.context() as patch:
                patch.setattr(corpus, "_source_read", read)
                if valid:
                    corpus.validate()
                else:
                    with pytest.raises(ProvenanceValidationError, match="fixity|foreign content"):
                        corpus.validate()
            assert frames == []


def test_native_rejects_a_changed_fixity_under_the_same_description_identity(graph, who, catalog):
    graph = copy.deepcopy(graph)
    reported = assertion(
        "reported_description",
        who,
        state=reference(graph["states"][0]["id"], "state"),
        content=copy.deepcopy(graph["descriptions"][0]["content"]),
    )
    graph["descriptions"].append(reported)
    raw = create_journal(graph, recorded_by_agent_id=who, catalog=catalog)
    with validate_journal(raw, catalog=catalog) as source:
        target = assertion_reference(source, reported["assertion_id"])
    changed = copy.deepcopy(reported)
    changed["assertion_id"] = new_id()
    changed["content"]["size_bytes"] = str(int(changed["content"]["size_bytes"]) + 1)
    with pytest.raises(ProvenanceValidationError):
        append_correction(
            raw,
            [target],
            reason="changed immutable referent",
            assertions={"descriptions": [changed]},
            recorded_by_agent_id=who,
            catalog=catalog,
        )


def semantic_source(graph, who, catalog, assertions, prefixes):
    graph = copy.deepcopy(graph)
    source = graph["states"][0]
    graph["relations"] = []
    for index in range(assertions):
        target = assertion(
            "state",
            who,
            occurrence_id=source["occurrence_id"],
            extent={"kind": "unknown", "reason": str(index)},
        )
        graph["states"].append(target)
        graph["relations"].append(
            assertion(
                "derivation",
                who,
                used_state=reference(source["id"], "state"),
                generated_state=reference(target["id"], "state"),
                kind="copy",
            )
        )
        source = target
    return history(
        create_journal(graph, recorded_by_agent_id=who, catalog=catalog), prefixes, catalog
    )


def measured_semantics(corpus, monkeypatch, *, rows=64, octets=4096):
    observer = SelectionObserver(corpus._db)
    previous = corpus._semantic_processed

    def decoded(value):
        observer.metrics["json_decodes"] += 1
        return json.loads(value)

    original = corpus._source_read

    def read(query, parameters):
        assert "assertion_payloads" not in query
        return original(query, parameters)

    with source_reads(monkeypatch) as frames, monkeypatch.context() as patch:
        patch.setattr(corpus, "_db", observer)
        patch.setattr(corpus, "_source_read", read)
        patch.setattr(owner_module, "json", SimpleNamespace(loads=decoded))
        progress = corpus.advance_semantics(row_budget=rows, byte_budget=octets)
    observer.metrics["processed_rows"] = progress.processed_assertions - previous
    observer.metrics["projected_payload_octets"] = observer.metrics["payload_octets"]
    observer.metrics["source_frame_reads"] = len(frames)
    assert all(cursor.closed for cursor in observer.cursors)
    return (
        progress,
        record(
            growth_binding(owner_module.CanonicalCorpusValidator.advance_semantics),
            {"row_budget": rows, "byte_budget": octets},
            observer.metrics,
        ),
        observer.queries,
    )


@pytest.mark.growth_witness(
    "native-corpus-selected-semantic-union-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus.CanonicalCorpusValidator.advance_semantics",
)
def test_semantic_union_visits_original_assertions_once_across_many_selected_prefixes(
    graph, who, catalog, monkeypatch, record_property
):
    samples, counts = [], []
    for assertions, prefixes in ((1, 1), (17, 17), (17, 129)):
        raw = semantic_source(graph, who, catalog, assertions, prefixes)
        with validate_journal(raw, catalog=catalog) as source, CanonicalCorpusValidator() as corpus:
            corpus.advance_source(source, frame_budget=prefixes)
            expected = {
                (row["type"], row["used_state"]["object_id"], row["generated_state"]["object_id"])
                for row in source.graph_validation.objects.values()
                if row["type"] == "derivation"
            }
            for sequence in range(prefixes):
                corpus.select_source(HistoryJournalAnchor.from_mapping(source.anchor_at(sequence)))
            progress, sample, queries = measured_semantics(corpus, monkeypatch)
            assert progress.complete and progress.processed_assertions == assertions
            assert sum(query.startswith("SELECT a.journal") for query in queries) == assertions + 1
            assert set(corpus._db.execute("SELECT kind,source,target FROM edges")) == expected
            samples.append(sample)
            counts.append(sample["metrics"]["sql_statements"])
            progress, sample, queries = measured_semantics(corpus, monkeypatch)
            assert progress.complete and sample["metrics"]["sql_statements"] == 0
            samples.append(sample)
    assert counts[1] == counts[2]
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "native-corpus-prefix-selection-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus.CanonicalCorpusValidator.select_source",
)
def test_prefix_selection_only_reads_exact_header_and_records_its_position(
    journal, catalog, monkeypatch, record_property
):
    samples = []
    raw = history(journal, 17, catalog)
    with validate_journal(raw, catalog=catalog) as source, CanonicalCorpusValidator() as corpus:
        corpus.advance_source(source, frame_budget=17)
        for sequence in (0, 16, 0):
            anchor = HistoryJournalAnchor.from_mapping(source.anchor_at(sequence))
            count = corpus._db.execute("SELECT count(*) FROM selected_prefixes").fetchone()[0]
            observer = SelectionObserver(corpus._db)
            with source_reads(monkeypatch) as frames, monkeypatch.context() as patch:
                patch.setattr(corpus, "_db", observer)
                corpus.select_source(anchor)
            observer.metrics["source_frame_reads"] = len(frames)
            assert observer.metrics["sql_statements"] == 2 and not frames
            assert all("assertion_payloads" not in query for query in observer.queries)
            samples.append(
                record(
                    growth_binding(owner_module.CanonicalCorpusValidator.select_source),
                    {"selected_prefixes": count},
                    observer.metrics,
                )
            )
    record_property("growth_samples", samples)


def test_byte_quanta_resume_without_repeating_completed_obligations(
    graph, who, catalog, monkeypatch
):
    raw = semantic_source(graph, who, catalog, 17, 1)
    with validate_journal(raw, catalog=catalog) as source, CanonicalCorpusValidator() as corpus:
        corpus.add(source)
        width = corpus._db.execute("SELECT max(length(value)) FROM semantic_inputs").fetchone()[0]
        queries = []
        read = corpus._source_read

        def observed(sql, parameters):
            queries.append(sql)
            return read(sql, parameters)

        monkeypatch.setattr(corpus, "_source_read", observed)
        previous = 0
        while True:
            progress = corpus.advance_semantics(row_budget=64, byte_budget=width)
            assert progress.processed_assertions - previous in (0, 1)
            assert progress.admitted_octets <= width
            previous = progress.processed_assertions
            if progress.complete:
                break
        assert previous == 17
        assert corpus._db.execute("SELECT count(*) FROM edges").fetchone() == (17,)
        assert sum(sql.startswith("SELECT substr(value") for sql in queries) == 17
        assert sum(sql.startswith("SELECT a.journal") for sql in queries) <= 35


def test_selection_requires_every_source_anchor_coordinate_and_open_context(journal, catalog):
    with validate_journal(journal, catalog=catalog) as source, CanonicalCorpusValidator() as corpus:
        corpus.advance_source(source)
        anchor = HistoryJournalAnchor.from_mapping(source.anchor)
        with pytest.raises(ProvenanceValidationError, match="exact corpus source"):
            corpus.select_source(replace(anchor, prefix_sha256="0" * 64))
        assert corpus._db.execute("SELECT count(*) FROM selected_prefixes").fetchone() == (0,)
        corpus.select_source(anchor)
    with pytest.raises(RuntimeError, match="closed"):
        corpus.select_source(anchor)


def test_documentary_source_is_not_an_effective_selected_root(journal, who, catalog):
    raw, edge = edge_history(journal, who, catalog)
    with validate_journal(raw, catalog=catalog) as source, CanonicalCorpusValidator() as corpus:
        corpus.advance_source(source, frame_budget=3)
        progress = corpus.advance_semantics(row_budget=4, byte_budget=4096)
        assert progress.complete and progress.processed_assertions == 1
        assert corpus._db.execute("SELECT count(*) FROM edges").fetchone() == (0,)
        corpus.validate()
