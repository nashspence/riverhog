from __future__ import annotations

from contextlib import nullcontext
from copy import copy
from dataclasses import replace

import pytest
from riverhog_core import canonical_discovery_corpus_replay as replay
from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask
from riverhog_core.canonical_discovery_selection import frozen_corpus_selection
from riverhog_core.services.discovery_validation_owner import DiscoveryValidationOwner
from riverhog_provenance import JournalValidationCapacity, StaleCorpusWorkspace, validate_journal

from tests.unit.test_canonical_discovery_corpus_source import case as case
from tests.unit.test_canonical_discovery_corpus_source import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_corpus_source import original as original
from tests.unit.test_canonical_discovery_corpus_source import qualified as qualified
from tests.unit.test_canonical_discovery_corpus_source import seed
from tests.unit.test_discovery_validation_owner import unit


def open_replay(work, selection):
    with work.metadata.read() as (db, key):
        manifest = frozen_corpus_selection(db, key, selection)
    owner = replay.CorpusReplayWork(selection, manifest, work.corpus(selection))
    with work.metadata.transaction() as (db, key):
        replay.begin_corpus_replay(db, key, owner)
    return owner


def prepare(metadata, owner, original, *, byte_budget=65536):
    with metadata.read() as (db, key):
        target = replay.next_corpus_replay_source(db, key, owner)
    source = nullcontext(None) if target is None else original[1].borrow()
    with source as summary, metadata.read() as (db, key):
        return replay.prepare_corpus_replay(
            db, key, owner, summary=summary, frame_budget=1, row_budget=1, byte_budget=byte_budget
        )


def step(metadata, owner, original):
    page = prepare(metadata, owner, original)
    with metadata.transaction() as (db, key):
        return replay.apply_corpus_replay(db, key, owner, page)


def progress(metadata, owner):
    with metadata.read() as (db, key):
        return replay.corpus_replay_progress(db, key, owner)


def finish(metadata, owner, original):
    phases = []
    for _ in range(10000):
        before = progress(metadata, owner)
        phases.append(before.phase)
        if before.phase == "complete":
            return phases
        after = step(metadata, owner, original)
        assert after.version == before.version + 1
        assert after.sources <= owner.manifest.sources.source_count
        assert after.selections <= owner.manifest.selection_count
    pytest.fail("fixture did not complete its finite Native work")


@pytest.mark.parametrize(
    "interrupted_phase",
    ["sources", "selections", "semantics", "dependencies", "derivation", "specialization"],
)
def test_native_commit_before_core_confirmation_resumes_exact_inputs_without_reset(
    case, qualified, original, interrupted_phase
):
    app = DiscoveryValidationOwner(case.factory, case.root)
    try:
        with unit(app, case) as work:
            selection, anchors = seed(work.metadata, qualified)
            owner = open_replay(work, selection)
            while progress(work.metadata, owner).phase != interrupted_phase:
                step(work.metadata, owner, original)
            before = progress(work.metadata, owner)
            prepared = prepare(work.metadata, owner, original)
            assert progress(work.metadata, owner) == before
            directory = work.metadata.custody.directory
            inodes = tuple(
                (directory / name).stat().st_ino for name in ("corpus.sqlite3", "discovery.sqlite3")
            )
            epoch = work.metadata.job.worker_epoch
            # Native has durably advanced; the detached Core result is lost.
            del prepared
    finally:
        app.close()
    resumed = DiscoveryValidationOwner(case.factory, case.root)
    try:
        with unit(resumed, case) as work:
            assert work.metadata.job.worker_epoch == epoch + 1
            owner = open_replay(work, selection)
            assert progress(work.metadata, owner) == before
            assert (
                tuple(
                    (directory / name).stat().st_ino
                    for name in ("corpus.sqlite3", "discovery.sqlite3")
                )
                == inodes
            )
            phases = finish(work.metadata, owner, original)
            terminal = progress(work.metadata, owner)
            assert terminal.sources == 1 and terminal.selections == len(anchors) == 3
            assert terminal.phase == "complete"
            assert phases[-1] == "complete"
            assert step(work.metadata, owner, original) == terminal
            assert work.metadata.job.state == "active"
            assert original[1].anchor == anchors[-1].to_mapping()
    finally:
        resumed.close()


def test_raw_sources_and_selected_prefixes_have_independent_counted_barriers(
    case, qualified, original
):
    app = DiscoveryValidationOwner(case.factory, case.root)
    try:
        with unit(app, case) as work:
            selection, anchors = seed(work.metadata, qualified)
            owner = open_replay(work, selection)
            states = [progress(work.metadata, owner)]
            while states[-1].phase != "semantics":
                states.append(step(work.metadata, owner, original))
            source_steps = [state for state in states if state.phase == "sources"]
            assert [state.sources for state in source_steps] == [0, 0, 0, 1]
            assert all(state.selections == 0 for state in source_steps)
            selected = [state for state in states if state.phase == "selections"]
            assert [state.selections for state in selected] == [0, 1, 2, 3]
            assert all(state.sources == 1 for state in selected)
            assert states[-1].after is None and states[-1].receipt_json is None
            assert len(anchors) == 3
            finish(work.metadata, owner, original)
    finally:
        app.close()


def test_clipped_or_unrequested_source_cannot_advance_full_source_coverage(
    case, qualified, original
):
    app = DiscoveryValidationOwner(case.factory, case.root)
    try:
        with unit(app, case) as work:
            selection, anchors = seed(work.metadata, qualified)
            owner = open_replay(work, selection)
            before = progress(work.metadata, owner)
            with (
                validate_journal(
                    original[0][: anchors[0].prefix_bytes],
                    catalog=case.catalog,
                    require_profiles=False,
                ) as clipped,
                work.metadata.read() as (db, key),
            ):
                with pytest.raises(StaleArchiveTask, match="complete qualified source"):
                    replay.prepare_corpus_replay(db, key, owner, summary=clipped, byte_budget=65536)
                with pytest.raises(StaleArchiveTask, match="complete qualified source"):
                    replay.prepare_corpus_replay(db, key, owner, byte_budget=65536)
            assert progress(work.metadata, owner) == before
            while progress(work.metadata, owner).phase != "selections":
                step(work.metadata, owner, original)
            with work.metadata.read() as (db, key):
                with pytest.raises(StaleArchiveTask, match="does not acquire"):
                    replay.prepare_corpus_replay(
                        db, key, owner, summary=original[1], byte_budget=65536
                    )
    finally:
        app.close()


def test_native_capacity_fault_does_not_publish_core_progress_and_retry_resumes(
    case, qualified, original, monkeypatch
):
    app = DiscoveryValidationOwner(case.factory, case.root)
    try:
        with unit(app, case) as work:
            selection, _ = seed(work.metadata, qualified)
            owner = open_replay(work, selection)
            while progress(work.metadata, owner).phase != "semantics":
                step(work.metadata, owner, original)
            before = progress(work.metadata, owner)
            advance = owner.corpus.advance_semantics

            def capacity(**budget):
                advance(**budget)
                raise JournalValidationCapacity("injected Native capacity boundary")

            with monkeypatch.context() as patch:
                patch.setattr(owner.corpus, "advance_semantics", capacity)
                with pytest.raises(JournalValidationCapacity, match="injected"):
                    prepare(work.metadata, owner, original)
            assert progress(work.metadata, owner) == before
            assert owner._prepared is None
            finish(work.metadata, owner, original)
    finally:
        app.close()


@pytest.mark.parametrize("damage", ["worker", "manifest", "missing", "extra", "count", "receipt"])
def test_changed_original_inputs_or_cursor_never_advance_a_prepared_result(
    case, qualified, original, damage
):
    app = DiscoveryValidationOwner(case.factory, case.root)
    try:
        with unit(app, case) as work:
            selection, _ = seed(work.metadata, qualified)
            owner = open_replay(work, selection)
            while progress(work.metadata, owner).sources != 1:
                step(work.metadata, owner, original)
            page = prepare(work.metadata, owner, original)
            with work.metadata.transaction() as (db, key):
                if damage == "worker":
                    db.execute("UPDATE discovery_corpus_replay SET worker_id='other'").close()
                elif damage == "manifest":
                    owner.manifest = replace(owner.manifest, selection_sha256="e" * 64)
                elif damage == "missing":
                    db.execute("DELETE FROM discovery_corpus_source_inputs").close()
                elif damage == "extra":
                    row = list(
                        db.execute("SELECT * FROM discovery_corpus_source_inputs").fetchone()
                    )
                    row[2] = "urn:uuid:ffffffff-ffff-4fff-8fff-ffffffffffff"
                    row[3] = "f" * 64
                    db.execute(
                        "INSERT INTO discovery_corpus_source_inputs VALUES(?,?,?,?,?,?)", row
                    ).close()
                elif damage == "count":
                    db.execute("UPDATE discovery_corpus_replay SET sources='0'").close()
                else:
                    db.execute("UPDATE discovery_corpus_replay SET receipt_json='{}'").close()
            with pytest.raises(StaleArchiveTask):
                with work.metadata.transaction() as (db, key):
                    replay.apply_corpus_replay(db, key, owner, page)
    finally:
        app.close()


def test_only_current_detached_result_is_accepted_and_core_rollback_retries(
    case, qualified, original
):
    app = DiscoveryValidationOwner(case.factory, case.root)
    try:
        with unit(app, case) as work:
            selection, _ = seed(work.metadata, qualified)
            owner = open_replay(work, selection)
            first = prepare(work.metadata, owner, original)
            second = prepare(work.metadata, owner, original)
            for wrong in (first, copy(second)):
                with pytest.raises(StaleArchiveTask, match="exact detached"):
                    with work.metadata.transaction() as (db, key):
                        replay.apply_corpus_replay(db, key, owner, wrong)
            with pytest.raises(InterruptedError):
                with work.metadata.transaction() as (db, key):
                    replay.apply_corpus_replay(db, key, owner, second)
                    raise InterruptedError("Core commit interrupted")
            assert progress(work.metadata, owner) == second.expected
            with work.metadata.transaction() as (db, key):
                replay.apply_corpus_replay(db, key, owner, second)
            with pytest.raises(StaleArchiveTask, match="current cursor"):
                with work.metadata.transaction() as (db, key):
                    replay.apply_corpus_replay(db, key, owner, second)
            finish(work.metadata, owner, original)
    finally:
        app.close()


def test_terminal_dispatch_rechecks_the_actual_native_file(case, qualified, original):
    app = DiscoveryValidationOwner(case.factory, case.root)
    try:
        with unit(app, case) as work:
            selection, _ = seed(work.metadata, qualified)
            owner = open_replay(work, selection)
            finish(work.metadata, owner, original)
            before = progress(work.metadata, owner)
            path = work.metadata.custody.directory / "corpus.sqlite3"
            held = path.with_suffix(".held")
            path.rename(held)
            path.write_bytes(held.read_bytes())
            path.chmod(0o600)
            with pytest.raises(StaleCorpusWorkspace):
                prepare(work.metadata, owner, original)
            assert progress(work.metadata, owner) == before
    finally:
        app.close()


@pytest.mark.parametrize("damage", ["journal", "digest", "bytes", "frames", "terminal"])
def test_wrong_native_source_result_cannot_count_an_original_input(
    case, qualified, original, monkeypatch, damage
):
    app = DiscoveryValidationOwner(case.factory, case.root)
    try:
        with unit(app, case) as work:
            selection, _ = seed(work.metadata, qualified)
            owner = open_replay(work, selection)
            before = progress(work.metadata, owner)
            advance = owner.corpus.advance_source

            def changed(*args, **kwargs):
                result = advance(*args, **kwargs)
                return replace(
                    result,
                    **{
                        "journal": {"journal_id": "urn:uuid:ffffffff-ffff-4fff-8fff-ffffffffffff"},
                        "digest": {"source_sha256": "e" * 64},
                        "bytes": {"source_octets": result.source_octets + 1},
                        "frames": {"accepted_frames": 4},
                        "terminal": {"complete": True},
                    }[damage],
                )

            with monkeypatch.context() as patch:
                patch.setattr(owner.corpus, "advance_source", changed)
                with pytest.raises(StaleArchiveTask, match="original full source"):
                    prepare(work.metadata, owner, original)
            assert progress(work.metadata, owner) == before
            finish(work.metadata, owner, original)
    finally:
        app.close()
