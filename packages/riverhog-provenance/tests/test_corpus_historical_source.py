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
    create_journal,
    encode_entry,
    new_id,
    parse_journal,
    reference,
    selected_delivery_occurrence,
    validate_journal,
)
from riverhog_provenance import corpus as owner
from riverhog_provenance_contracts import canonical_document
from test_corpus_source_growth import measure as measure_source
from test_corpus_source_growth import source_reads
from test_journal_result import history
from test_source_anchor_growth import record


def correction_history(journal, who, catalog):
    first = assertion("agent", who, kind="software", name="original")
    raw = append_assertions(journal, {"agents": [first]}, recorded_by_agent_id=who, catalog=catalog)
    with validate_journal(raw, catalog=catalog) as current:
        target = assertion_reference(current, first["assertion_id"])
    raw = append_correction(
        raw, [target], reason="withdrawn", recorded_by_agent_id=who, catalog=catalog
    )
    raw = append_checkpoint(
        raw, purpose="urn:test:historical-gap", recorded_by_agent_id=who, catalog=catalog
    )
    second = {**first, "assertion_id": new_id(), "name": "replacement"}
    raw = append_assertions(raw, {"agents": [second]}, recorded_by_agent_id=who, catalog=catalog)
    with validate_journal(raw, catalog=catalog) as current:
        target = assertion_reference(current, second["assertion_id"])
    third = {**second, "assertion_id": new_id(), "name": "corrected replacement"}
    raw = append_correction(
        raw,
        [target],
        assertions={"agents": [third]},
        reason="same entry replacement",
        recorded_by_agent_id=who,
        catalog=catalog,
    )
    return raw, (first, second, third)


def test_historical_assertion_matches_independent_native_prefix_oracles(journal, who, catalog):
    raw, rows = correction_history(journal, who, catalog)
    with validate_journal(raw, catalog=catalog) as complete, CanonicalCorpusValidator() as corpus:
        corpus.add(complete)
        frames = parse_journal(raw)
        for sequence in range(len(frames)):
            prefix = b"".join(frame.encoded for frame in frames[: sequence + 1])
            with validate_journal(prefix, catalog=catalog) as oracle:
                selected = HistoryJournalAnchor.from_mapping(oracle.anchor)
                actual = corpus.historical_assertion(selected, rows[0]["id"], byte_budget=65536)
                expected = oracle.graph_validation.objects.get(rows[0]["id"])
                if expected is None:
                    assert actual is None
                else:
                    assert actual.value == expected
                    original = oracle.assertion_entries[expected["assertion_id"]]
                    assert actual.reference == {
                        "scope": "external",
                        "journal_id": oracle.journal_id,
                        "entry": original,
                        "assertion_id": expected["assertion_id"],
                        "object_id": expected["id"],
                        "object_type": expected["type"],
                    }
                    actual.value["name"] = "detached caller view"
                    actual.reference["entry"]["entry_id"] = new_id()
                    again = corpus.historical_assertion(selected, rows[0]["id"], byte_budget=65536)
                    assert again.value == expected
                    assert again.reference["entry"] == original
        corpus.validate()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("journal_id", "urn:uuid:00000000-0000-4000-8000-000000000000"),
        ("through_entry_id", "urn:uuid:00000000-0000-4000-8000-000000000000"),
        ("through_sequence", 1),
        ("through_json_sha256", "0" * 64),
        ("prefix_sha256", "0" * 64),
        ("prefix_bytes", 1),
    ],
)
def test_historical_selection_requires_every_original_anchor_coordinate(
    journal, catalog, field, value
):
    with (
        validate_journal(journal, catalog=catalog) as current,
        CanonicalCorpusValidator() as corpus,
    ):
        corpus.add(current)
        anchor = HistoryJournalAnchor.from_mapping(current.anchor)
        with pytest.raises(ProvenanceValidationError, match="exact corpus source"):
            corpus.historical_assertion(
                replace(anchor, **{field: value}), current.states[0]["id"], byte_budget=65536
            )


@pytest.mark.parametrize("budget", [0, -1, True, "65536", None])
def test_historical_selection_requires_explicit_positive_admission(journal, catalog, budget):
    with (
        validate_journal(journal, catalog=catalog) as current,
        CanonicalCorpusValidator() as corpus,
    ):
        corpus.add(current)
        with pytest.raises(ValueError, match="byte budget"):
            corpus.historical_assertion(
                HistoryJournalAnchor.from_mapping(current.anchor),
                current.states[0]["id"],
                byte_budget=budget,
            )


def test_historical_selection_requires_open_context_exact_anchor_and_canonical_identity(
    journal, catalog
):
    with validate_journal(journal, catalog=catalog) as current:
        anchor = HistoryJournalAnchor.from_mapping(current.anchor)
        identity = current.states[0]["id"]
        with CanonicalCorpusValidator() as corpus:
            corpus.add(current)
            with pytest.raises(TypeError, match="exact history journal anchor"):
                corpus.historical_assertion(current.anchor, identity, byte_budget=65536)
            with pytest.raises(ValueError, match="UUID"):
                corpus.historical_assertion(anchor, identity.upper(), byte_budget=65536)
        with pytest.raises(RuntimeError, match="closed"):
            corpus.historical_assertion(anchor, identity, byte_budget=65536)


def test_same_object_identity_in_distinct_journals_retains_its_selected_source(
    journal, who, catalog
):
    with validate_journal(journal, catalog=catalog) as initial:
        graph = copy.deepcopy(initial.graph)
        graph["agents"][0]["assertion_id"] = new_id()
        graph["agents"][0]["name"] = "another source assertion"
        identity = initial.graph["agents"][0]["id"]
        other_raw = create_journal(graph, recorded_by_agent_id=who, catalog=catalog)
        with validate_journal(other_raw, catalog=catalog) as other:
            with CanonicalCorpusValidator() as corpus:
                corpus.add(initial)
                corpus.add(other)
                for source in (initial, other):
                    result = corpus.historical_assertion(
                        HistoryJournalAnchor.from_mapping(source.anchor),
                        identity,
                        byte_budget=65536,
                    )
                    assert result.value == source.graph_validation.objects[identity]
                    assert result.reference["journal_id"] == source.journal_id
                corpus.validate()


def test_historical_payload_admission_precedes_blob_fetch_and_is_not_a_validity_limit(
    journal, who, catalog, monkeypatch
):
    agent = assertion("agent", who, kind="software", name="x" * 100000)
    raw = append_assertions(journal, {"agents": [agent]}, recorded_by_agent_id=who, catalog=catalog)
    with validate_journal(raw, catalog=catalog) as current, CanonicalCorpusValidator() as corpus:
        corpus.add(current)
        anchor = HistoryJournalAnchor.from_mapping(current.anchor)
        original = current.assertion_entries[agent["assertion_id"]]
        size = len(canonical_document(agent)) + len(canonical_document(original))
        read, queries = corpus._source_read, []

        def observe(query, parameters):
            queries.append(query)
            return read(query, parameters)

        with monkeypatch.context() as patch:
            patch.setattr(corpus, "_source_read", observe)
            with pytest.raises(JournalValidationCapacity, match="admitted byte budget"):
                corpus.historical_assertion(anchor, agent["id"], byte_budget=size - 1)
            assert len(queries) == 2
            assert all("substr(" not in query for query in queries)
            actual = corpus.historical_assertion(anchor, agent["id"], byte_budget=size)
        assert actual.value == agent
        assert actual.reference["entry"] == original


def test_interrupted_retirement_and_replacement_roll_back_with_the_source_terminal(
    journal, who, catalog, monkeypatch
):
    class Interrupted(BaseException):
        pass

    raw, rows = correction_history(journal, who, catalog)
    frames = parse_journal(raw)
    before_raw = b"".join(frame.encoded for frame in frames[:-1])
    with (
        validate_journal(before_raw, catalog=catalog) as before,
        validate_journal(raw, catalog=catalog) as after,
        CanonicalCorpusValidator() as corpus,
    ):
        corpus.add(before)
        write = corpus._source_write
        previous = HistoryJournalAnchor.from_mapping(before.anchor)
        selected = HistoryJournalAnchor.from_mapping(after.anchor)

        def interrupt(query, parameters=()):
            if query.startswith("INSERT INTO source_extents"):
                raise Interrupted()
            return write(query, parameters)

        with monkeypatch.context() as patch:
            patch.setattr(corpus, "_source_write", interrupt)
            with pytest.raises(Interrupted):
                corpus.add(after)
        assert (
            corpus.historical_assertion(previous, rows[0]["id"], byte_budget=65536).value == rows[1]
        )
        assert corpus._db.execute(
            "SELECT retired FROM assertions WHERE journal=? AND assertion=?",
            (before.journal_id, rows[1]["assertion_id"]),
        ).fetchone() == (None,)
        assert (
            corpus._db.execute(
                "SELECT 1 FROM assertions WHERE journal=? AND assertion=?",
                (before.journal_id, rows[2]["assertion_id"]),
            ).fetchone()
            is None
        )
        with pytest.raises(ProvenanceValidationError, match="exact corpus source"):
            corpus.historical_assertion(selected, rows[0]["id"], byte_budget=65536)
        corpus.add(after)
        assert (
            corpus.historical_assertion(selected, rows[0]["id"], byte_budget=65536).value == rows[2]
        )
        corpus.validate()


class SelectionCursor:
    def __init__(self, actual, observer, payload):
        self.actual, self.observer, self.payload = actual, observer, payload
        self.closed = False

    def fetchone(self):
        row = self.actual.fetchone()
        if row is not None:
            self.observer.metrics["returned_rows"] += 1
            size = sum(
                len(value if isinstance(value, bytes) else str(value).encode())
                for value in row
                if value is not None
            )
            self.observer.metrics["payload_octets" if self.payload else "metadata_octets"] += size
        return row

    def close(self):
        self.actual.close()
        self.closed = True
        self.observer.metrics["closed_cursors"] += 1


class SelectionObserver:
    def __init__(self, actual):
        self.actual, self.metrics, self.cursors, self.queries = actual, defaultdict(int), [], []

    def execute(self, query, parameters=()):
        assert all(cursor.closed for cursor in self.cursors)
        self.queries.append(query)
        self.metrics["sql_statements"] += 1
        self.metrics["parameter_octets"] += sum(
            len(value if isinstance(value, bytes) else str(value).encode()) for value in parameters
        )
        cursor = SelectionCursor(self.actual.execute(query, parameters), self, "substr(" in query)
        self.cursors.append(cursor)
        return cursor


def measure_selection(corpus, anchor, identity, monkeypatch):
    observer = SelectionObserver(corpus._db)
    dimensions = {
        "retained_assertions": corpus._db.execute("SELECT count(*) FROM assertions").fetchone()[0],
        "retained_frames": corpus._db.execute("SELECT count(*) FROM entries").fetchone()[0],
        "anchor_octets": len(canonical_document(anchor.to_mapping())),
    }

    def decoded(value):
        observer.metrics["json_decodes"] += 1
        observer.metrics["decoded_octets"] += len(value)
        return json.loads(value)

    def stepped():
        observer.metrics["sqlite_vm_instructions"] += 1
        return 0

    corpus._db.set_progress_handler(stepped, 1)
    try:
        with source_reads(monkeypatch) as reads, monkeypatch.context() as patch:
            patch.setattr(corpus, "_db", observer)
            patch.setattr(owner, "json", SimpleNamespace(loads=decoded))
            result = corpus.historical_assertion(anchor, identity, byte_budget=2**22)
        observer.metrics["source_frame_reads"] = len(reads)
    finally:
        observer.actual.set_progress_handler(None, 0)
    assert all(cursor.closed for cursor in observer.cursors)
    dimensions["selected_octets"] = (
        len(canonical_document(result.value)) + len(canonical_document(result.reference["entry"]))
        if result is not None
        else 0
    )
    sample = record(
        growth_binding(owner.CanonicalCorpusValidator.historical_assertion),
        dimensions,
        observer.metrics,
    )
    return result, sample, observer.queries


@pytest.mark.growth_witness(
    "native-corpus-historical-assertion-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus.CanonicalCorpusValidator.historical_assertion",
)
def test_actual_historical_point_reads_seek_exact_source_and_admit_only_one_payload(
    journal, catalog, monkeypatch, record_property
):
    samples = []
    for count in (1, 17, 129):
        raw = history(journal, count, catalog)
        with (
            validate_journal(raw, catalog=catalog) as current,
            CanonicalCorpusValidator() as corpus,
        ):
            corpus.add(current)
            for sequence in (0, count - 1):
                anchor = HistoryJournalAnchor.from_mapping(current.anchor_at(sequence))
                identity = current.states[0]["id"]
                result, sample, queries = measure_selection(corpus, anchor, identity, monkeypatch)
                assert result.value == current.states[0]
                assert len(queries) == 3
                samples.append(sample)
                result, sample, queries = measure_selection(corpus, anchor, new_id(), monkeypatch)
                assert result is None
                assert len(queries) == 2
                samples.append(sample)
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "native-corpus-retirement-source-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus.CanonicalCorpusValidator._add_source",
)
def test_actual_retirement_source_growth(journal, who, catalog, monkeypatch, record_property):
    raw, rows = correction_history(journal, who, catalog)
    with validate_journal(raw, catalog=catalog) as current, CanonicalCorpusValidator() as corpus:
        samples = [
            measure_source(corpus, current, 0, monkeypatch),
            measure_source(corpus, current, len(current.frames), monkeypatch),
        ]
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "native-corpus-historical-gap-selection-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus.CanonicalCorpusValidator.historical_assertion",
)
def test_actual_historical_gap_growth(journal, who, catalog, monkeypatch, record_property):
    raw, rows = correction_history(journal, who, catalog)
    samples = []
    with validate_journal(raw, catalog=catalog) as current, CanonicalCorpusValidator() as corpus:
        corpus.add(current)
        for sequence, expected in enumerate((None, rows[0], None, None, rows[1], rows[2])):
            anchor = HistoryJournalAnchor.from_mapping(current.anchor_at(sequence))
            result, sample, queries = measure_selection(corpus, anchor, rows[0]["id"], monkeypatch)
            assert (result.value if result else None) == expected
            assert len(queries) == (3 if result else 2)
            samples.append(sample)
    record_property("growth_samples", samples)


@pytest.mark.parametrize(
    "fault", ["valid", "artifact", "role", "slot", "association", "size", "digest", "anchor"]
)
def test_historical_delivery_reuses_native_policy_and_exact_selected_prefix(
    journal, who, catalog, fault
):
    with validate_journal(journal, catalog=catalog) as initial:
        context = assertion("context", who, kind="delivery")
        association = assertion(
            "delivery_association",
            who,
            delivery_context_id=context["id"],
            slot={"kind": "text", "text": "member"},
            role="urn:test:payload",
            state=reference(initial.states[0]["id"], "state"),
            verification_observation_id=initial.graph["descriptions"][0]["id"],
        )
        content = initial.graph["descriptions"][0]["content"]
    selected_raw = append_assertions(
        journal,
        {"contexts": [context], "delivery_associations": [association]},
        recorded_by_agent_id=who,
        catalog=catalog,
    )
    with validate_journal(selected_raw, catalog=catalog) as selected:
        target = assertion_reference(selected, association["assertion_id"])
        binding = {
            "artifact_id": "member",
            "journal": selected.anchor,
            "delivery_association_id": association["id"],
        }
    raw = append_correction(
        selected_raw,
        [target],
        reason="withdrawn delivery",
        recorded_by_agent_id=who,
        catalog=catalog,
    )
    with (
        validate_journal(selected_raw, catalog=catalog) as oracle,
        validate_journal(raw, catalog=catalog) as complete,
        CanonicalCorpusValidator() as corpus,
    ):
        corpus.add(complete)
        arguments = {
            "binding": copy.deepcopy(binding),
            "artifact_id": "member",
            "byte_count": int(content["size_bytes"]),
            "sha256": next(d["value"] for d in content["digests"] if d["algorithm"] == "sha-256"),
            "member_role": "urn:test:payload",
        }
        if fault == "artifact":
            arguments["artifact_id"] = "other"
        elif fault == "role":
            arguments["member_role"] = "urn:test:another"
        elif fault == "slot":
            arguments["artifact_id"] = "other"
            arguments["binding"]["artifact_id"] = "other"
        elif fault == "association":
            arguments["binding"]["delivery_association_id"] = new_id()
        elif fault == "size":
            arguments["byte_count"] += 1
        elif fault == "digest":
            arguments["sha256"] = "0" * 64
        elif fault == "anchor":
            arguments["binding"]["journal"] = complete.anchor
        if fault == "valid":
            expected = selected_delivery_occurrence(oracle, **arguments)
            assert (
                corpus.selected_delivery_occurrence(**arguments, assertion_byte_budget=65536)
                == expected
            )
            assert (
                corpus.selected_delivery_occurrence(**arguments, assertion_byte_budget=65536)[0][
                    "id"
                ]
                == association["state"]["object_id"]
            )
        else:
            with pytest.raises(ProvenanceValidationError):
                selected_delivery_occurrence(oracle, **arguments)
            with pytest.raises(ProvenanceValidationError):
                corpus.selected_delivery_occurrence(**arguments, assertion_byte_budget=65536)


def replacement_ladder(journal, who, count, catalog):
    agent = assertion("agent", who, kind="software", name="generation 0")
    raw = append_assertions(journal, {"agents": [agent]}, recorded_by_agent_id=who, catalog=catalog)
    frames = parse_journal(raw)
    pieces, template, previous, current = [raw], frames[-1].document, frames[-1].reference, agent
    for generation in range(1, count + 1):
        replacement = (
            {**agent, "assertion_id": new_id(), "name": f"generation {generation}"}
            if generation < count
            else None
        )
        document = {
            **template,
            "id": new_id(),
            "sequence": str(generation + 1),
            "entry_kind": "correction",
            "previous_entry": previous,
            "body": {
                "reason": "exact historical interval",
                "retracts": [{"assertion_id": current["assertion_id"], "entry": previous}],
                **({"assertions": {"agents": [replacement]}} if replacement else {}),
            },
        }
        encoded = encode_entry(document, catalog=catalog)
        pieces.append(encoded)
        previous = parse_journal(encoded)[0].reference
        current = replacement
    return b"".join(pieces), agent


@pytest.mark.growth_witness(
    "native-corpus-retired-gap-point-seek-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus.CanonicalCorpusValidator.historical_assertion",
)
def test_retired_gap_uses_one_indexed_birth_seek_across_replacement_ladders(
    journal, who, catalog, monkeypatch, record_property
):
    samples = []
    for count in (1, 17, 129):
        raw, original = replacement_ladder(journal, who, count, catalog)
        with (
            validate_journal(raw, catalog=catalog) as complete,
            CanonicalCorpusValidator() as corpus,
        ):
            corpus.add(complete)
            frames = parse_journal(raw)
            for sequence in (1, count, count + 1):
                prefix = b"".join(frame.encoded for frame in frames[: sequence + 1])
                with validate_journal(prefix, catalog=catalog) as oracle:
                    result, sample, queries = measure_selection(
                        corpus,
                        HistoryJournalAnchor.from_mapping(oracle.anchor),
                        original["id"],
                        monkeypatch,
                    )
                    assert (result.value if result else None) == (
                        oracle.graph_validation.objects.get(original["id"])
                    )
                    if sequence == count + 1:
                        assert result is None
                        assert len(queries) == 2
                        assert sample["metrics"]["json_decodes"] == 0
                    samples.append(sample)
    record_property("growth_samples", samples)
