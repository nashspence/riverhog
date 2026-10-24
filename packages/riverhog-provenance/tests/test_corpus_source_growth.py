from __future__ import annotations

from collections import defaultdict
from contextlib import contextmanager

import pytest
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
    reference,
    validate_journal,
)
from riverhog_provenance import corpus as owner
from riverhog_provenance import journal as journal_owner
from riverhog_provenance.journal_store import _Frames
from test_journal_result import history
from test_source_anchor_growth import Cursor, record
from test_source_anchor_growth import measure as measure_anchor


@contextmanager
def source_reads(monkeypatch):
    reads = []
    item, iterator = _Frames.__getitem__, _Frames.__iter__

    def get(frames, position):
        result = item(frames, position)
        if isinstance(position, int):
            reads.append((position, len(result.encoded)))
        return result

    def iterate(frames):
        for position, frame in enumerate(iterator(frames)):
            reads.append((position, len(frame.encoded)))
            yield frame

    with monkeypatch.context() as patch:
        patch.setattr(_Frames, "__getitem__", get)
        patch.setattr(_Frames, "__iter__", iterate)
        yield reads


def test_identical_corpus_source_does_not_replay_completed_frames(journal, catalog, monkeypatch):
    raw = history(journal, 17, catalog)
    with validate_journal(raw, catalog=catalog) as current, CanonicalCorpusValidator() as corpus:
        corpus.add(current)
        with source_reads(monkeypatch) as reads:
            corpus.add(current)
        assert reads == []
        corpus.validate()


def test_missing_suffix_only_and_older_prefixes_retain_exact_source_rows(
    journal, catalog, monkeypatch
):
    raw = history(journal, 129, catalog)
    with validate_journal(raw, catalog=catalog) as complete, CanonicalCorpusValidator() as corpus:
        for start, stop in [(0, 1), (1, 17), (17, 129), (129, 17), (129, 129)]:
            selected = b"".join(frame.encoded for frame in complete.frames[:stop])
            with validate_journal(selected, catalog=catalog) as current:
                with source_reads(monkeypatch) as reads:
                    corpus.add(current)
                assert [position for position, size in reads] == list(
                    range(start, max(start, stop))
                )
                assert corpus._db.execute("SELECT count(*) FROM entries").fetchone() == (
                    max(start, stop),
                )
        corpus.validate()


@pytest.mark.parametrize("method", ["add", "advance_source"])
def test_divergent_writers_reject_before_any_source_payload_is_loaded(
    journal, who, catalog, monkeypatch, method
):
    with validate_journal(journal, catalog=catalog) as first:
        artifact = first.graph["artifacts"][0]["id"]

    def branch(value):
        return append_assertions(
            journal,
            {
                "extensions": [
                    assertion(
                        "extension",
                        who,
                        subject=reference(artifact, "artifact"),
                        property="urn:test:writer-branch",
                        value={"type": "text", "value": value},
                    )
                ]
            },
            recorded_by_agent_id=who,
            catalog=catalog,
        )

    with (
        validate_journal(branch("left"), catalog=catalog) as left,
        validate_journal(branch("right"), catalog=catalog) as right,
        CanonicalCorpusValidator() as corpus,
    ):
        corpus.add(left)
        before = corpus._db.total_changes
        with source_reads(monkeypatch) as reads:
            with pytest.raises(ProvenanceValidationError, match="divergent writers"):
                getattr(corpus, method)(right)
        assert reads == []
        assert corpus._db.total_changes == before


def test_unfinished_source_entry_rolls_back_all_child_obligations(journal, catalog, monkeypatch):
    class Interrupted(BaseException):
        pass

    with (
        validate_journal(journal, catalog=catalog) as current,
        CanonicalCorpusValidator() as corpus,
    ):
        identity = corpus._identity
        calls = 0

        def interrupt(key, domain, value):
            nonlocal calls
            calls += 1
            if domain == "referent":
                raise Interrupted()
            return identity(key, domain, value)

        with monkeypatch.context() as patch:
            patch.setattr(corpus, "_identity", interrupt)
            with pytest.raises(Interrupted):
                corpus.add(current)
        assert calls > 2
        assert corpus._db.execute("SELECT count(*) FROM source_extents").fetchone() == (0,)
        assert corpus._db.execute("SELECT count(*) FROM entries").fetchone() == (0,)
        assert corpus._db.execute("SELECT count(*) FROM assertions").fetchone() == (0,)
        assert corpus._db.execute("SELECT count(*) FROM identities").fetchone() == (1,)
        corpus.add(current)
        corpus.validate()
        assert corpus._db.execute("SELECT count(*) FROM assertions").fetchone()[0] == len(
            current.assertion_entries
        )


def test_interrupted_suffix_restarts_after_last_complete_source_entry(
    journal, catalog, monkeypatch
):
    class Interrupted(BaseException):
        pass

    raw = history(journal, 17, catalog)
    with validate_journal(raw, catalog=catalog) as current, CanonicalCorpusValidator() as corpus:
        identity = corpus._identity
        stop_id = current.anchor_at(4)["through"]["entry_id"]

        def interrupt(key, domain, value):
            if domain == "entry" and key == stop_id:
                raise Interrupted()
            return identity(key, domain, value)

        with monkeypatch.context() as patch:
            patch.setattr(corpus, "_identity", interrupt)
            with pytest.raises(Interrupted):
                corpus.add(current)
        assert corpus._db.execute(
            "SELECT sequence FROM source_extents WHERE journal=?", (current.journal_id,)
        ).fetchone() == ("3",)
        with source_reads(monkeypatch) as reads:
            corpus.add(current)
        assert [position for position, size in reads] == list(range(4, 17))
        corpus.validate()


@pytest.mark.parametrize("method", ["add", "advance_source"])
def test_closed_source_is_not_accepted_by_completed_prefix_reuse(journal, catalog, method):
    with (
        validate_journal(journal, catalog=catalog) as current,
        CanonicalCorpusValidator() as corpus,
    ):
        corpus.add(current)
        reader = current.borrow()
        reader.close()
        before = corpus._db.total_changes
        with pytest.raises(RuntimeError, match="closed"):
            getattr(corpus, method)(reader)
        assert corpus._db.total_changes == before


def test_late_historical_selection_preserves_retracted_corpus_cycle_obligations(
    journal, who, catalog
):
    with validate_journal(journal, catalog=catalog) as primary:
        other = BoundedSourceObserver(catalog=catalog).observe(BytesSource(b"other"))
        graph = other.graph_fragment()
        graph["relations"] = [
            assertion(
                "derivation",
                who,
                used_state=external_reference(primary, primary.states[0]["id"]),
                generated_state=reference(other.state_id, "state"),
                kind="copy",
            )
        ]
        child_raw = create_journal(graph, recorded_by_agent_id=who, catalog=catalog)
        with validate_journal(child_raw, catalog=catalog) as child:
            edge = assertion(
                "derivation",
                who,
                used_state=external_reference(child, other.state_id),
                generated_state=reference(primary.states[0]["id"], "state"),
                kind="copy",
            )
        historical_raw = append_assertions(
            journal,
            {"relations": [edge]},
            recorded_by_agent_id=who,
            catalog=catalog,
        )
        with validate_journal(historical_raw, catalog=catalog) as historical:
            target = assertion_reference(historical, edge["assertion_id"])
        corrected_raw = append_correction(
            historical_raw,
            [target],
            reason="withdrawn causal edge",
            recorded_by_agent_id=who,
            catalog=catalog,
        )
    with (
        validate_journal(corrected_raw, catalog=catalog) as corrected,
        validate_journal(historical_raw, catalog=catalog) as historical,
        validate_journal(child_raw, catalog=catalog) as child,
        CanonicalCorpusValidator() as corpus,
    ):
        corpus.add(corrected)
        corpus.add(child)
        corpus.validate()
        assert edge["assertion_id"] in corrected.retracted_assertion_ids
        corpus.add(historical)
        with pytest.raises(ProvenanceValidationError, match="cycle"):
            corpus.validate()


class SourceCursor(Cursor):
    def fetchone(self):
        result = self.actual.fetchone()
        if result is not None:
            self.recorder.metrics["returned_rows"] += 1
            self.recorder.metrics["metadata_octets"] += sum(
                len(value.encode() if isinstance(value, str) else value)
                for value in result
                if value is not None
            )
        return result


class SourceObserver:
    def __init__(self, actual):
        self.actual, self.metrics, self.cursors = actual, defaultdict(int), []

    def execute(self, query, parameters=()):
        self.metrics["sql_statements"] += 1
        cursor = SourceCursor(self.actual.execute(query, parameters), self)
        self.cursors.append(cursor)
        return cursor


def measure(corpus, current, first, monkeypatch, *, frame_budget=None, advance=False, through=None):
    selected_frames = len(current.frames) if through is None else through + 1
    stop = selected_frames if frame_budget is None else min(selected_frames, first + frame_budget)
    frames = [current.frames[index] for index in range(first, stop)]
    assertions = references = retractions = 0
    for frame in frames:
        retractions += len(frame.document["body"].get("retracts", ()))
        body = frame.document["body"].get("assertions", {})
        assertions += sum(len(rows) for rows in body.values())
        stack = [body]
        while stack:
            value = stack.pop()
            if isinstance(value, dict):
                if value.get("scope") == "external" and "assertion_id" in value:
                    references += 1
                else:
                    stack.extend(value.values())
            elif isinstance(value, list):
                stack.extend(value)
    dimensions = {
        "new_frames": len(frames),
        "frame_budget": len(current.frames) if frame_budget is None else frame_budget,
        "new_assertions": assertions,
        "new_references": references,
        "new_retractions": retractions,
        "frame_octets": sum(len(frame.encoded) for frame in frames),
        "entry_octets": sum(len(frame.json_bytes) for frame in frames),
    }
    observer, children = SourceObserver(corpus._db), []
    hashed = journal_owner.JournalFrame.sha256.fget

    def anchor(summary, sequence):
        assert all(cursor.closed for cursor in observer.cursors)
        observer.metrics["native_anchor_calls"] += 1
        result, sample = measure_anchor(summary, sequence, monkeypatch)
        children.append(sample)
        return result

    def hash_entry(frame):
        observer.metrics["hashed_entry_octets"] += len(frame.json_bytes)
        return hashed(frame)

    with source_reads(monkeypatch) as reads:
        with monkeypatch.context() as patch:
            patch.setattr(corpus, "_db", observer)
            patch.setattr(journal_owner.JournalSummary, "anchor_at", anchor)
            patch.setattr(journal_owner.JournalFrame, "sha256", property(hash_entry))
            if advance:
                corpus.advance_source(current, frame_budget=frame_budget, through=through)
            else:
                corpus._add_source(current, frame_budget=frame_budget, through=through)
    observer.metrics["admitted_frames"] = len(reads)
    observer.metrics["admitted_frame_octets"] = sum(size for _, size in reads)
    observer.metrics["source_frame_reads"] = len(reads)
    observer.metrics["source_frame_octets"] = sum(size for _, size in reads)
    assert all(cursor.closed for cursor in observer.cursors)
    return record(
        growth_binding(
            owner.CanonicalCorpusValidator.advance_source
            if advance
            else owner.CanonicalCorpusValidator._add_source
        ),
        dimensions,
        observer.metrics,
        {"anchor": children},
    )


@pytest.mark.growth_witness(
    "native-corpus-source-extension-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus.CanonicalCorpusValidator._add_source",
)
def test_actual_corpus_source_extension_visits_only_missing_original_frames(
    journal, catalog, monkeypatch, record_property
):
    samples = []
    for count in (1, 17, 129):
        raw = history(journal, count, catalog)
        with (
            validate_journal(raw, catalog=catalog) as current,
            CanonicalCorpusValidator() as corpus,
        ):
            samples.append(measure(corpus, current, 0, monkeypatch))
            samples.append(measure(corpus, current, count, monkeypatch))
            initial = current.frames[0].encoded
            with validate_journal(initial, catalog=catalog) as shorter:
                samples.append(measure(corpus, shorter, 1, monkeypatch))
            corpus.validate()
    record_property("growth_samples", samples)


def test_bounded_source_advancement_stops_at_the_admitted_frame_budget(
    journal, catalog, monkeypatch
):
    raw = history(journal, 17, catalog)
    with validate_journal(raw, catalog=catalog) as current, CanonicalCorpusValidator() as corpus:
        for completed in range(1, 18):
            with source_reads(monkeypatch) as reads:
                progress = corpus.advance_source(current, frame_budget=1)
            assert [position for position, _ in reads] == [completed - 1]
            assert progress.journal_id == current.journal_id
            assert progress.source_sha256 == current.journal_sha256
            assert progress.source_octets == current.journal_bytes
            assert progress.accepted_frames == completed
            assert progress.complete is (completed == 17)
        with source_reads(monkeypatch) as reads:
            assert corpus.advance_source(current, frame_budget=1) == progress
        assert reads == []
        assert corpus._db.execute("SELECT count(*) FROM entries").fetchone() == (17,)
        first = current.frames[0].encoded
        with validate_journal(first, catalog=catalog) as shorter:
            with source_reads(monkeypatch) as reads:
                selected = corpus.advance_source(shorter, frame_budget=1)
            assert reads == []
            assert selected.complete and selected.accepted_frames == 1
            assert selected.source_sha256 == shorter.journal_sha256
            assert selected.source_sha256 != progress.source_sha256
            assert corpus._db.execute(
                "SELECT sequence FROM source_extents WHERE journal=?", (current.journal_id,)
            ).fetchone() == ("16",)
        # Raw advancement does not discharge selected effective-graph or global obligations.
        corpus.add(current)
        corpus.validate()


@pytest.mark.parametrize("budget", [0, -1, True, "1", None])
def test_source_frame_budget_is_checked_before_any_source_or_corpus_change(
    journal, catalog, budget
):
    with (
        validate_journal(journal, catalog=catalog) as current,
        CanonicalCorpusValidator() as corpus,
    ):
        before = corpus._db.total_changes
        with pytest.raises(ValueError, match="frame budget"):
            corpus.advance_source(current, frame_budget=budget)
        assert corpus._db.total_changes == before


def test_interrupted_bounded_source_resumes_at_only_the_unfinished_entry(
    journal, catalog, monkeypatch
):
    class Interrupted(BaseException):
        pass

    raw = history(journal, 17, catalog)
    with validate_journal(raw, catalog=catalog) as current, CanonicalCorpusValidator() as corpus:
        identity = corpus._identity
        stop_id = current.anchor_at(4)["through"]["entry_id"]

        def interrupt(key, domain, value):
            if domain == "entry" and key == stop_id:
                raise Interrupted()
            return identity(key, domain, value)

        with monkeypatch.context() as patch:
            patch.setattr(corpus, "_identity", interrupt)
            with pytest.raises(Interrupted):
                corpus.advance_source(current, frame_budget=17)
        with source_reads(monkeypatch) as reads:
            progress = corpus.advance_source(current, frame_budget=1)
        assert [position for position, _ in reads] == [4]
        assert progress.accepted_frames == 5
        assert not progress.complete
        assert corpus.advance_source(current, frame_budget=12).complete


@pytest.mark.growth_witness(
    "native-bounded-corpus-source-extension-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus.CanonicalCorpusValidator.advance_source",
)
def test_actual_bounded_source_advancement_preserves_entry_and_payload_budgets(
    journal, catalog, monkeypatch, record_property
):
    samples = []
    for count, budget in ((1, 1), (17, 1), (129, 1), (129, 17)):
        raw = history(journal, count, catalog)
        with (
            validate_journal(raw, catalog=catalog) as current,
            CanonicalCorpusValidator() as corpus,
        ):
            samples.append(
                measure(corpus, current, 0, monkeypatch, frame_budget=budget, advance=True)
            )
            samples.append(
                measure(
                    corpus,
                    current,
                    min(count, budget),
                    monkeypatch,
                    frame_budget=budget,
                    advance=True,
                )
            )
            corpus.add(current)
            samples.append(
                measure(corpus, current, count, monkeypatch, frame_budget=budget, advance=True)
            )
    record_property("growth_samples", samples)
