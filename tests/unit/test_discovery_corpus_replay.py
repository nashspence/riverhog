from __future__ import annotations

import sqlite3

import pytest
from riverhog_core import canonical_discovery_archive_framing as framing
from riverhog_core import canonical_discovery_corpus_replay as replay
from riverhog_core.canonical_discovery_archive_source import ArchiveJournalSource
from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask
from riverhog_core.journal_validation_workspace import lease_journal_validation_workspace
from riverhog_core.services.discovery_validation_owner import DiscoveryValidationOwner
from riverhog_provenance import JournalSummary, JournalValidationBudget

from tests.unit.test_canonical_discovery_corpus_source import case as case
from tests.unit.test_canonical_discovery_corpus_source import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_corpus_source import original as original
from tests.unit.test_canonical_discovery_corpus_source import qualified as qualified
from tests.unit.test_canonical_discovery_corpus_source import seed
from tests.unit.test_discovery_validation_owner import unit


def prepare_source(work, selection):
    target = work.next_corpus_source(selection)
    assert target is not None
    source = ArchiveJournalSource(work.metadata, selection.history.reader, target.journal_id)
    framed = framing.ArchiveJournalFramingWork(source)
    with work.metadata.transaction() as (db, key):
        framing.begin_archive_framing(db, key, framed)
    for _ in range(1000):
        with work.metadata.read() as (db, key):
            page = framing.prepare_archive_framing(
                db, key, framed, byte_budget=65536, frame_budget=64
            )
        if page is None:
            break
        with work.metadata.transaction() as (db, key):
            assert framing.apply_archive_framing(db, key, framed, page)
    else:
        pytest.fail("finite source framing did not complete")
    for _ in range(1000):
        result = work.advance_journal(framed, budget=JournalValidationBudget(rows=64))
        if result is not None and result.complete:
            return framed
    pytest.fail("finite actual source validation did not complete")


def progress(work, selection):
    retained = work.corpus_replay(selection)
    with work.metadata.read() as (db, key):
        return replay.corpus_replay_progress(db, key, retained)


def finish(work, selection):
    phases = set()
    for _ in range(1000):
        before = progress(work, selection)
        phases.add(before.phase)
        after = work.advance_corpus(selection, row_budget=1, byte_budget=65536)
        assert after is not None
        assert after.version == before.version + int(before.phase != "complete")
        assert work._slot.replay_summary is None
        if after.phase == "complete":
            assert work.advance_corpus(selection, byte_budget=65536) == after
            return phases
    pytest.fail("finite actual corpus replay did not complete")


def test_actual_full_source_and_selected_prefixes_share_retained_native_owners(case, qualified):
    app = DiscoveryValidationOwner(case.factory, case.root)
    try:
        with unit(app, case) as work:
            selection, anchors = seed(work.metadata, qualified)
            target = work.next_corpus_source(selection)
            assert target == anchors[-1] and target != anchors[0]
            retained = work.corpus_replay(selection)
            before = progress(work, selection)
            assert work.advance_corpus(selection, byte_budget=65536) is None
            assert progress(work, selection) == before
            framed = prepare_source(work, selection)
            native = work._slot.journal.session
            provider = work._slot.certificates
            first = work.advance_corpus(selection, frame_budget=1, byte_budget=65536)
            assert first.sources == 0 and first.selections == 0
        with unit(app, case) as work:
            assert work.corpus_replay(selection) is retained
            assert work._slot.journal.session is native
            assert work._slot.certificates is provider
            phases = finish(work, selection)
            terminal = progress(work, selection)
            assert terminal.sources == 1 and terminal.selections == len(anchors) == 3
            assert phases == {
                "sources",
                "selections",
                "semantics",
                "dependencies",
                "derivation",
                "specialization",
            }
            assert work.next_corpus_source(selection) is None
            assert native.progress().accepted_sha256 == framed.source.identity.source_sha256
            assert work.metadata.job.state == "active"
    finally:
        app.close()


def test_native_source_commit_before_core_reply_survives_actual_worker_takeover(
    case, qualified, monkeypatch
):
    app = DiscoveryValidationOwner(case.factory, case.root)
    try:
        with unit(app, case) as work:
            selection, _ = seed(work.metadata, qualified)
            prepare_source(work, selection)
            retained = work.corpus_replay(selection)
            before = progress(work, selection)
            directory = work.metadata.custody.directory
            epoch = work.metadata.job.worker_epoch
            child = work._slot.journal.directory
            inodes = tuple(
                p.stat().st_ino
                for p in (
                    directory / "discovery.sqlite3",
                    directory / "corpus.sqlite3",
                    child / "snapshot.sqlite3",
                )
            )

        def interrupted(*args, **kwargs):
            raise InterruptedError("Core confirmation reply lost")

        with monkeypatch.context() as patch:
            patch.setattr(replay, "apply_corpus_replay", interrupted)
            with pytest.raises(InterruptedError, match="reply lost"):
                with unit(app, case) as work:
                    work.advance_corpus(selection, byte_budget=65536)
    finally:
        app.close()
    resumed = DiscoveryValidationOwner(case.factory, case.root)
    try:
        with unit(resumed, case) as work:
            assert work.metadata.job.worker_epoch == epoch + 1
            assert progress(work, selection) == before
            assert work.corpus_replay(selection) is not retained
            prepare_source(work, selection)
            finish(work, selection)
            assert (
                tuple(
                    p.stat().st_ino
                    for p in (
                        directory / "discovery.sqlite3",
                        directory / "corpus.sqlite3",
                        child / "snapshot.sqlite3",
                    )
                )
                == inodes
            )
    finally:
        resumed.close()


def test_initialized_conductor_is_not_recreated_after_control_loss(case, qualified):
    app = DiscoveryValidationOwner(case.factory, case.root)
    try:
        with unit(app, case) as work:
            selection, _ = seed(work.metadata, qualified)
            work.corpus_replay(selection)
            path = work.metadata.custody.directory / "discovery.sqlite3"
            with work.metadata.transaction() as (db, _):
                db.execute("DELETE FROM discovery_corpus_replay").close()
            with pytest.raises(StaleArchiveTask):
                work.advance_corpus(selection, byte_budget=65536)
            with sqlite3.connect(path) as db:
                assert db.execute("SELECT count(*) FROM discovery_corpus_replay").fetchone() == (0,)
    finally:
        app.close()


@pytest.mark.parametrize("lost_reply", [False, True])
def test_uncertain_actual_source_borrow_release_retains_both_custody_domains(
    case, qualified, monkeypatch, lost_reply
):
    app = DiscoveryValidationOwner(case.factory, case.root)
    actual, held, failures = JournalSummary.close, [], 2
    try:
        with unit(app, case) as work:
            selection, _ = seed(work.metadata, qualified)
            prepare_source(work, selection)
            work.corpus_replay(selection)
            slot = work._slot
            parent = work.metadata.custody.directory.parent
            workspace = work.metadata.job.workspace_id
            child = slot.journal.directory

        def uncertain(summary):
            nonlocal failures
            if not held:
                held.append(summary)
            if summary is held[0] and failures:
                failures -= 1
                if lost_reply:
                    actual(summary)
                raise OSError("source release unconfirmed")
            return actual(summary)

        with monkeypatch.context() as patch:
            patch.setattr(JournalSummary, "close", uncertain)
            with pytest.raises(OSError, match="unconfirmed"):
                with unit(app, case) as work:
                    work.advance_corpus(selection, byte_budget=65536)
            assert slot.closing and slot.replay_summary is held[0]
            assert app.active_build_ids() == (case.build_id,)
            with lease_journal_validation_workspace(parent, workspace) as acquired:
                assert acquired is None
            with lease_journal_validation_workspace(child.parent, child.name) as acquired:
                assert acquired is None
            app.close()
            assert slot.replay_summary is None
            assert app.active_build_ids() == ()
        with lease_journal_validation_workspace(parent, workspace) as acquired:
            assert acquired is not None
        with lease_journal_validation_workspace(child.parent, child.name) as acquired:
            assert acquired is not None
    finally:
        app.close()
