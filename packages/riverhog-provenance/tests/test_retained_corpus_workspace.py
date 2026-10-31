from __future__ import annotations

import hashlib
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_canonical_json import canonical_json_bytes
from riverhog_provenance import (
    CorpusReplayBinding,
    IncompleteSourceError,
    JournalValidationCapacity,
    RetainedCorpusValidator,
    StaleCorpusWorkspace,
    validate_journal,
)
from riverhog_provenance import corpus_workspace as owner
from test_corpus_selection import edge_history
from test_corpus_source_growth import source_reads
from test_journal_result import history


class OwnedCorpusSource:
    def __init__(self, summary, catalog, selections=None):
        self.sources = {HistoryJournalAnchor.from_mapping(summary.anchor)}
        self.selections = set(selections) if selections is not None else self.sources.copy()
        self.binding = CorpusReplayBinding(
            attempt_id="a" * 32,
            directory_id="owner-corpus-directory",
            source_manifest_sha256=self.digest(self.sources),
            source_count=len(self.sources),
            selection_manifest_sha256=self.digest(self.selections),
            selection_count=len(self.selections),
            policy_sha256="b" * 64,
            catalog_sha256=catalog.checkpoint_sha256(),
        )
        self.current = True
        self.calls = []

    @staticmethod
    def digest(anchors):
        return hashlib.sha256(
            canonical_json_bytes(
                sorted(canonical_json_bytes(a.to_mapping()).decode() for a in anchors)
            )
        ).hexdigest()

    def verify_current(self):
        self.calls.append("current")
        return self.current

    def admit_source(self, anchor):
        self.calls.append("source")
        return anchor in self.sources

    def admit_selection(self, anchor):
        self.calls.append("selection")
        return anchor in self.selections


def open_workspace(path, source, *, epoch=0, create=False, **kwargs):
    return RetainedCorpusValidator.open_retained(
        path, source, worker_epoch=epoch, create=create, **kwargs
    )


def directory(tmp_path):
    path = tmp_path / "corpus"
    path.mkdir(mode=0o700)
    return path


def ingest(workspace, summary, source):
    while not workspace.advance_source(summary).complete:
        pass
    for anchor in source.selections:
        workspace.select_source(anchor)


def finish(workspace):
    while not workspace.advance_semantics(row_budget=1).complete:
        pass
    while not workspace.advance_dependencies(row_budget=1).complete:
        pass
    results = []
    for kind in ("derivation", "specialization"):
        while True:
            progress = workspace.advance_order(kind, row_budget=1)
            if progress.complete:
                results.append(progress)
                break
    return results


def test_retained_source_resumes_without_replaying_committed_entries(
    tmp_path, journal, catalog, monkeypatch
):
    raw = history(journal, 17, catalog)
    path = directory(tmp_path)
    with validate_journal(raw, catalog=catalog) as summary:
        source = OwnedCorpusSource(summary, catalog)
        with open_workspace(path, source, create=True) as first:
            assert first.advance_source(summary, frame_budget=7).accepted_frames == 7
        with open_workspace(path, source, epoch=1) as resumed:
            with source_reads(monkeypatch) as reads:
                assert resumed.advance_source(summary, frame_budget=64).complete
            assert [position for position, _ in reads] == list(range(7, 17))
            resumed.select_source(next(iter(source.selections)))
            assert all(result.complete for result in finish(resumed))
        with open_workspace(path, source, epoch=2) as warm:
            with source_reads(monkeypatch) as reads:
                assert warm.advance_semantics().complete
                assert warm.advance_dependencies().complete
                assert warm.advance_order("derivation").complete
            assert reads == []
    assert (path / "corpus.sqlite3").is_file()


@pytest.mark.parametrize("phase", ["source", "semantics", "dependencies", "order"])
def test_actual_process_exit_retains_each_native_phase(tmp_path, journal, who, catalog, phase):
    raw, _ = edge_history(journal, who, catalog)
    path = directory(tmp_path)
    raw_path = tmp_path / "journal"
    raw_path.write_bytes(raw)
    code = """import os,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from riverhog_provenance import validate_journal
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_provenance_contracts import ContractCatalog
from test_retained_corpus_workspace import OwnedCorpusSource,open_workspace,ingest
catalog=ContractCatalog()
with validate_journal(Path(sys.argv[3]).read_bytes(),catalog=catalog) as summary:
    selected=[HistoryJournalAnchor.from_mapping(summary.anchor_at(1))]
    source=OwnedCorpusSource(summary,catalog,selected)
    workspace=open_workspace(Path(sys.argv[2]),source,create=True)
    phase=sys.argv[4]
    if phase=='source':
        workspace.advance_source(summary)
    else:
        ingest(workspace,summary,source)
        workspace.advance_semantics(row_budget=1)
        if phase in ('dependencies','order'):
            while not workspace.advance_semantics(row_budget=1).complete: pass
            workspace.advance_dependencies(row_budget=1)
        if phase=='order':
            while not workspace.advance_dependencies(row_budget=1).complete: pass
            workspace.advance_order('derivation',row_budget=1)
    os._exit(0)
"""
    subprocess.run(
        [
            sys.executable,
            "-c",
            code,
            str(Path(__file__).parent),
            str(path),
            str(raw_path),
            phase,
        ],
        check=True,
    )
    with validate_journal(raw, catalog=catalog) as summary:
        selected = [HistoryJournalAnchor.from_mapping(summary.anchor_at(1))]
        source = OwnedCorpusSource(summary, catalog, selected)
        with open_workspace(path, source, epoch=1) as resumed:
            before = resumed._corpus
            if phase == "semantics":
                assert before._semantic_phase == "walking" and before._semantic_processed == 1
            if phase == "dependencies":
                assert before._semantic_phase == "complete" and before._dependency_phase == "forks"
            if phase == "order":
                assert before._db.execute(
                    "SELECT source_edges FROM corpus_order_derivation_checkpoint"
                ).fetchone() == ("1",)
            if phase == "source":
                ingest(resumed, summary, source)
            results = finish(resumed)
            assert results[0].source_edges == 1
            assert results[0].vertices == results[0].completed_vertices == 2


@pytest.mark.parametrize(
    "field",
    [
        "attempt_id",
        "directory_id",
        "source_manifest_sha256",
        "selection_manifest_sha256",
        "policy_sha256",
        "catalog_sha256",
        "source_count",
        "selection_count",
    ],
)
def test_changed_independent_binding_cannot_adopt_progress(tmp_path, journal, catalog, field):
    path = directory(tmp_path)
    with validate_journal(journal, catalog=catalog) as summary:
        source = OwnedCorpusSource(summary, catalog)
        with open_workspace(path, source, create=True) as first:
            first.advance_source(summary)
        before = (path / "corpus.sqlite3").read_bytes()
        old = getattr(source.binding, field)
        changed = old + 1 if type(old) is int else ("d" * len(old))
        source.binding = replace(source.binding, **{field: changed})
        with pytest.raises((StaleCorpusWorkspace, ValueError), match="binding|catalog"):
            open_workspace(path, source, epoch=1)
        assert (path / "corpus.sqlite3").read_bytes() == before


def test_replacement_fences_old_worker_and_closed_source_before_native_work(
    tmp_path, journal, catalog, monkeypatch
):
    path = directory(tmp_path)
    with validate_journal(journal, catalog=catalog) as summary:
        source = OwnedCorpusSource(summary, catalog)
        with open_workspace(path, source, create=True) as first:
            first.advance_source(summary)
            with open_workspace(path, source, epoch=1) as newer:
                with source_reads(monkeypatch) as reads:
                    with pytest.raises(StaleCorpusWorkspace, match="worker"):
                        first.advance_source(summary)
                assert reads == []
                source.current = False
                with pytest.raises(IncompleteSourceError, match="current"):
                    newer.advance_semantics()


def test_exact_membership_and_independent_counts_are_both_required(tmp_path, journal, catalog):
    path = directory(tmp_path)
    with validate_journal(journal, catalog=catalog) as summary:
        source = OwnedCorpusSource(summary, catalog)
        source.binding = replace(source.binding, source_count=2, selection_count=2)
        anchor = next(iter(source.sources))
        with open_workspace(path, source, create=True) as workspace:
            for _ in range(3):
                workspace.advance_source(summary)
                workspace.select_source(anchor)
            assert workspace._db.execute(
                "SELECT sources,selections FROM corpus_workspace"
            ).fetchone() == ("1", "1")
            with pytest.raises(IncompleteSourceError, match="incomplete"):
                workspace.advance_semantics()
            source.selections.clear()
            with pytest.raises(IncompleteSourceError, match="absent"):
                workspace.select_source(anchor)


def test_changed_currentness_before_commit_rolls_back_complete_atom(tmp_path, journal, catalog):
    path = directory(tmp_path)
    with validate_journal(journal, catalog=catalog) as summary:
        source = OwnedCorpusSource(summary, catalog)
        with open_workspace(path, source, create=True) as workspace:
            original = source.admit_source

            def revoke(anchor):
                source.current = False
                return original(anchor)

            source.admit_source = revoke
            with pytest.raises(IncompleteSourceError, match="current"):
                workspace.advance_source(summary)
            assert workspace._db.execute("SELECT count(*) FROM entries").fetchone() == (0,)
            assert workspace._db.execute(
                "SELECT sources,version FROM corpus_workspace"
            ).fetchone() == ("0", "0")
            source.current, source.admit_source = True, original
            assert workspace.advance_source(summary).complete


def test_creation_reopening_and_callback_ownership(tmp_path, journal, catalog):
    path = directory(tmp_path)
    callbacks = []
    with validate_journal(journal, catalog=catalog) as summary:
        source = OwnedCorpusSource(summary, catalog)
        with pytest.raises(StaleCorpusWorkspace, match="missing"):
            open_workspace(path, source)
        workspace = open_workspace(
            path, source, create=True, on_storage_close=lambda: callbacks.append("closed")
        )
        with pytest.raises(FileExistsError):
            open_workspace(
                path, source, create=True, on_storage_close=lambda: callbacks.append("failed")
            )
        with pytest.raises(ValueError, match="newer"):
            open_workspace(path, source)
        workspace.close()
        workspace.close()
        assert callbacks == ["closed"]
        with pytest.raises(RuntimeError, match="closed"):
            workspace.advance_semantics()
        assert (path / "corpus.sqlite3").is_file()


def test_metadata_capacity_rejects_before_checkpoint_payload(
    tmp_path, journal, catalog, monkeypatch
):
    path = directory(tmp_path)
    with validate_journal(journal, catalog=catalog) as summary:
        source = OwnedCorpusSource(summary, catalog)
        with open_workspace(path, source, create=True) as workspace:
            workspace._metadata_budget = 1
            one = owner._one

            def admitted(db, query, parameters=()):
                assert not query.startswith("SELECT worker_epoch,version,checkpoint")
                return one(db, query, parameters)

            with monkeypatch.context() as patch:
                patch.setattr(owner, "_one", admitted)
                with pytest.raises(JournalValidationCapacity):
                    workspace.advance_source(summary)
            workspace._metadata_budget = 4096
            assert workspace.advance_source(summary).complete
