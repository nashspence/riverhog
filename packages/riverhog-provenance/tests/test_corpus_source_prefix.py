from __future__ import annotations

from dataclasses import replace

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    CanonicalCorpusValidator,
    IncompleteSourceError,
    ProvenanceValidationError,
    append_assertions,
    assertion,
    create_journal,
    external_reference,
    reference,
    validate_journal,
)
from test_corpus_source_growth import measure as measure_source
from test_corpus_source_growth import source_reads
from test_journal_result import history
from test_retained_corpus_workspace import OwnedCorpusSource, open_workspace


@pytest.fixture
def future_reference(journal, who, catalog):
    other = BoundedSourceObserver(catalog=catalog).observe(BytesSource(b"other source"))
    with (
        validate_journal(
            create_journal(
                other.graph_fragment(),
                recorded_by_agent_id=other.observer_agent_id,
                catalog=catalog,
            ),
            catalog=catalog,
        ) as foreign,
        validate_journal(journal, catalog=catalog) as initial,
    ):
        left = next(
            v for v in initial.graph_validation.objects.values() if v["type"] == "observation"
        )
        right = next(
            v for v in foreign.graph_validation.objects.values() if v["type"] == "observation"
        )
        comparison = assertion(
            "content_comparison",
            who,
            left_description=reference(left["id"], "observation"),
            right_description=external_reference(foreign, right["id"]),
            result="different",
            algorithm="sha-256",
        )
    return append_assertions(
        journal, {"relations": [comparison]}, recorded_by_agent_id=who, catalog=catalog
    )


def source_for(summary, catalog, anchors):
    anchors = set(anchors)
    source = OwnedCorpusSource(summary, catalog, anchors)
    source.sources = anchors
    source.binding = replace(
        source.binding, source_manifest_sha256=source.digest(anchors), source_count=len(anchors)
    )
    return source


@pytest.mark.parametrize("retained", [False, True])
def test_exact_prefix_ingestion_excludes_future_documentary_obligations(
    tmp_path, future_reference, catalog, monkeypatch, retained
):
    with validate_journal(future_reference, catalog=catalog) as summary:
        anchor = HistoryJournalAnchor.from_mapping(summary.anchor_at(0))
        owner = source_for(summary, catalog, [anchor])
        path = tmp_path / "corpus"
        path.mkdir()
        corpus = (
            open_workspace(path, owner, create=True) if retained else CanonicalCorpusValidator()
        )
        try:
            with source_reads(monkeypatch) as reads:
                progress = corpus.advance_source(summary, through=0, frame_budget=1)
            assert [position for position, _ in reads] == [0]
            assert progress.complete and progress.accepted_frames == 1
            assert progress.journal_id == anchor.journal_id
            assert progress.source_sha256 == anchor.prefix_sha256
            assert progress.source_octets == anchor.prefix_bytes
            if retained:
                corpus.close()
                corpus = open_workspace(path, owner, epoch=1)
            with source_reads(monkeypatch) as reads:
                assert corpus.advance_source(summary, through=0).complete
            assert reads == []
            db = corpus._corpus._db if retained else corpus._db
            assert db.execute("SELECT sequence FROM entries").fetchall() == [("0",)]
            assert db.execute("SELECT count(*) FROM refs").fetchone() == (0,)
            corpus.select_source(anchor)
            assert corpus.advance_semantics().complete
            while not (dependencies := corpus.advance_dependencies()).complete:
                pass
            assert dependencies.checked_obligations == 0
            for kind in ("derivation", "specialization"):
                assert corpus.advance_order(kind).complete
        finally:
            if retained:
                corpus.close()
            else:
                corpus.__exit__(None, None, None)


def test_prefix_manifest_rejects_implicit_head_before_any_frame_is_loaded(
    tmp_path, future_reference, catalog, monkeypatch
):
    with validate_journal(future_reference, catalog=catalog) as summary:
        anchor = HistoryJournalAnchor.from_mapping(summary.anchor_at(0))
        owner = source_for(summary, catalog, [anchor])
        path = tmp_path / "corpus"
        path.mkdir()
        with open_workspace(path, owner, create=True) as corpus:
            with source_reads(monkeypatch) as reads:
                with pytest.raises(IncompleteSourceError, match="manifest"):
                    corpus.advance_source(summary)
            assert reads == []
            assert corpus._db.execute("SELECT sources FROM corpus_workspace").fetchone() == ("0",)


def test_requested_prefix_extension_resumes_without_replaying_confirmed_frames(
    tmp_path, future_reference, catalog, monkeypatch
):
    with validate_journal(future_reference, catalog=catalog) as summary:
        anchors = [HistoryJournalAnchor.from_mapping(summary.anchor_at(i)) for i in (0, 1)]
        owner = source_for(summary, catalog, anchors)
        path = tmp_path / "corpus"
        path.mkdir()
        with open_workspace(path, owner, create=True) as corpus:
            assert corpus.advance_source(summary, through=0).complete
        with open_workspace(path, owner, epoch=1) as corpus:
            with source_reads(monkeypatch) as reads:
                progress = corpus.advance_source(summary, through=1, frame_budget=1)
            assert [position for position, _ in reads] == [1]
            assert progress.complete and progress.accepted_frames == 2
            assert progress.source_sha256 == summary.journal_sha256
            assert progress.source_octets == summary.journal_bytes
            with source_reads(monkeypatch) as reads:
                assert corpus.advance_source(summary, through=0).complete
                assert corpus.advance_source(summary).complete
            assert reads == []
            assert corpus._db.execute("SELECT sources FROM corpus_workspace").fetchone() == ("2",)
            for anchor in anchors:
                corpus.select_source(anchor)
            assert corpus.advance_semantics().complete
            with pytest.raises(ProvenanceValidationError, match="unresolved"):
                corpus.advance_dependencies()


def test_default_source_admission_still_checks_the_full_supplied_journal(future_reference, catalog):
    with validate_journal(future_reference, catalog=catalog) as summary:
        with CanonicalCorpusValidator() as corpus:
            corpus.add(summary)
            assert corpus.advance_semantics().complete
            with pytest.raises(ProvenanceValidationError, match="unresolved"):
                corpus.advance_dependencies()


@pytest.mark.parametrize("through", [-1, True, False, 1.0, "0", 1])
def test_invalid_prefix_admission_does_no_corpus_or_frame_work(
    journal, catalog, monkeypatch, through
):
    with validate_journal(journal, catalog=catalog) as summary:
        with CanonicalCorpusValidator() as corpus:

            def forbidden(*_args, **_kwargs):
                pytest.fail("invalid source prefix admission performed corpus SQL")

            with monkeypatch.context() as patch, source_reads(monkeypatch) as reads:
                patch.setattr(corpus, "_source_read", forbidden)
                patch.setattr(corpus, "_source_write", forbidden)
                with pytest.raises(ValueError, match="prefix"):
                    corpus.advance_source(summary, through=through)
            assert reads == []


@pytest.mark.growth_witness(
    "native-bounded-corpus-source-prefix-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus.CanonicalCorpusValidator.advance_source",
)
def test_actual_prefix_admission_composes_endpoint_overlap_and_missing_frame_headers(
    journal, catalog, monkeypatch, record_property
):
    samples = []
    raw = history(journal, 129, catalog)
    with validate_journal(raw, catalog=catalog) as current, CanonicalCorpusValidator() as corpus:
        for first, through, budget in (
            (0, 0, 1),
            (1, 0, 1),
            (1, 16, 1),
            (2, 16, 64),
            (1, 0, 1),
            (17, 128, 1),
        ):
            sample = measure_source(
                corpus,
                current,
                first,
                monkeypatch,
                frame_budget=budget,
                through=through,
                advance=True,
            )
            samples.append(sample)
            assert sample["metrics"]["native_anchor_calls"] == (
                sample["dimensions"]["new_frames"] + 1 + (first > 0)
            )
            assert len(sample["delegates"]["anchor"]) == sample["metrics"]["native_anchor_calls"]
        assert corpus._db.execute("SELECT sequence FROM source_extents").fetchone() == ("17",)
    record_property("growth_samples", samples)
