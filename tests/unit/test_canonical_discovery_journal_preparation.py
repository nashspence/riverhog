from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace

import pytest
from riverhog_core import canonical_discovery_journal_epochs as epochs
from riverhog_core import canonical_discovery_journal_preparation as owner
from riverhog_core import canonical_discovery_journal_registration as registration
from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask
from riverhog_core.canonical_discovery_journal_certificates import DiscoveryJournalGraphCertificates
from riverhog_core.services.discovery_validation_owner import DiscoveryValidationOwner

from tests.unit.test_canonical_discovery_journal_certificates import point
from tests.unit.test_canonical_discovery_journal_rank import exact_ranks
from tests.unit.test_canonical_discovery_journal_source import source_case
from tests.unit.test_canonical_discovery_journal_union import edge, frame
from tests.unit.test_discovery_journal_owner import prepared as owned
from tests.unit.test_discovery_journal_owner import quantum


@contextmanager
def preparation_case(path, raw, *, retained=1, **options):
    with source_case(path, raw, retained=retained, **options) as (case, framing, source):
        with case.metadata.transaction() as (db, key):
            registration.register_discovery_journal(db, key, framing, source.binding)
        original = epochs.DiscoveryJournalEpochWork(framing, source)
        yield case, owner.DiscoveryJournalPreparationWork(original)


def begin(metadata, task):
    with metadata.transaction() as (db, key):
        return owner.begin_discovery_journal_preparation(db, key, task)


def progress(metadata, task):
    with metadata.read() as (db, key):
        return owner.discovery_journal_preparation_progress(db, key, task)


def prepare(metadata, task):
    with metadata.read() as (db, key):
        return owner.prepare_discovery_journal_preparation(db, key, task)


def apply(metadata, task, page, *, wrapper=lambda db: db):
    with metadata.transaction() as (db, key):
        return owner.apply_discovery_journal_preparation(wrapper(db), key, task, page)


def advance_until(metadata, task, predicate):
    while not predicate(progress(metadata, task)):
        page = prepare(metadata, task)
        assert page is not None
        assert apply(metadata, task, page)


def finish(metadata, task):
    phases, items = [], []
    while page := prepare(metadata, task):
        assert page.work_items <= 64
        phases.append(page.expected.phase)
        items.append(page.work_items)
        assert apply(metadata, task, page)
    return progress(metadata, task), phases, items


def controls(metadata):
    with metadata.read() as (db, _):
        return {
            table: db.execute("SELECT * FROM " + table).fetchall()
            for table in (
                "discovery_journal_preparation",
                "discovery_journal_epochs",
                "discovery_journal_epoch_frames",
                "discovery_journal_topology",
            )
        }


def test_actual_stage_lifecycle_preserves_corrections_and_original_certificate_coordinates(
    tmp_path,
):
    raw = frame([edge(0), edge(1)], kind="journal_init")
    raw += frame([edge(2, 1, 0)], kind="correction", retracts=[0, 1])
    with preparation_case(tmp_path / "source", raw) as (case, task):
        initial = begin(case.metadata, task)
        assert begin(case.metadata, task) == initial
        final, phases, _items = finish(case.metadata, task)
        assert final.complete and final.child_complete
        assert phases == sorted(phases, key=owner._PHASES.index)
        assert set(phases) == {"epochs", "topology", "union", "rank"}
        exact_ranks(case.metadata)
        provider = DiscoveryJournalGraphCertificates(case.metadata, task.rank)
        assert point(provider, raw).epoch == 0
        assert point(provider, raw, ordinal=1, left=1, right=0).epoch == 1


def test_one_high_degree_original_atom_stays_bounded_and_complete(tmp_path):
    raw = frame([edge(n, 0, n + 1) for n in range(129)], kind="journal_init")
    with preparation_case(tmp_path / "source", raw) as (case, task):
        begin(case.metadata, task)
        final, _phases, items = finish(case.metadata, task)
        assert final.complete and 64 in items and sum(items) > 129
        exact_ranks(case.metadata)


@pytest.mark.parametrize("transition", [False, True])
def test_parent_cursor_failure_rolls_back_child_atom_or_stage_initialization(tmp_path, transition):
    class Interrupted:
        def __init__(self, db):
            self.db = db

        def __getattr__(self, name):
            return getattr(self.db, name)

        def execute(self, sql, *args):
            if sql.startswith("UPDATE discovery_journal_preparation"):
                raise InterruptedError("parent cursor publication interrupted")
            return self.db.execute(sql, *args)

    raw = frame([edge(0)], kind="journal_init")
    with preparation_case(tmp_path / "source", raw) as (case, task):
        begin(case.metadata, task)
        if transition:
            advance_until(
                case.metadata,
                task,
                lambda p: p.phase == "epochs" and p.child_complete,
            )
        page = prepare(case.metadata, task)
        saved = controls(case.metadata)
        with pytest.raises(InterruptedError):
            apply(case.metadata, task, page, wrapper=Interrupted)
        assert controls(case.metadata) == saved and task._prepared is None
        with pytest.raises(StaleArchiveTask, match="owned"):
            apply(case.metadata, task, page)
        retry = prepare(case.metadata, task)
        assert retry is not page and apply(case.metadata, task, retry)
        assert finish(case.metadata, task)[0].complete


@pytest.mark.parametrize("stage", ["epochs", "topology", "union", "rank"])
def test_initialized_missing_child_fails_without_recreation(tmp_path, stage):
    raw = frame([edge(0)], kind="journal_init")
    with preparation_case(tmp_path / "source", raw) as (case, task):
        begin(case.metadata, task)
        advance_until(case.metadata, task, lambda p: p.phase == stage)
        with case.metadata.transaction() as (db, key):
            db.execute(
                "DELETE FROM "
                + task.stage(stage).table
                + " WHERE build_id=? AND admission_sha256=? AND journal_id=?",
                owner._key(key, task),
            ).close()
        for action in (progress, begin, prepare):
            with pytest.raises(StaleArchiveTask):
                action(case.metadata, task)


def test_independently_changed_child_or_untracked_initialized_stage_is_rejected(tmp_path):
    raw = frame([edge(0)], kind="journal_init")
    with preparation_case(tmp_path / "source", raw) as (case, task):
        with case.metadata.transaction() as (db, key):
            epochs.begin_discovery_journal_epochs(db, key, task.epochs)
        with pytest.raises(StaleArchiveTask, match="initial child"):
            begin(case.metadata, task)
    with preparation_case(tmp_path / "changed", raw) as (case, task):
        begin(case.metadata, task)
        with case.metadata.transaction() as (db, _):
            db.execute("UPDATE discovery_journal_preparation SET child_version='7'").close()
        with pytest.raises(StaleArchiveTask, match="exact initialized"):
            progress(case.metadata, task)


def test_owned_retry_lost_reply_and_original_source_failure_preserve_controls(
    tmp_path, monkeypatch
):
    raw = frame([edge(0)], kind="journal_init")
    with preparation_case(tmp_path / "source", raw) as (case, task):
        begin(case.metadata, task)
        saved = controls(case.metadata)

        def failed(*_args):
            raise OSError("original archive source unavailable")

        with monkeypatch.context() as patch:
            patch.setattr(task.epochs.source, "read_exact", failed)
            with pytest.raises(OSError, match="original archive"):
                prepare(case.metadata, task)
        assert controls(case.metadata) == saved
        page = prepare(case.metadata, task)
        assert prepare(case.metadata, task) is page
        with pytest.raises(StaleArchiveTask, match="owned"):
            apply(case.metadata, task, replace(page))
        with pytest.raises(TimeoutError):
            assert apply(case.metadata, task, page)
            raise TimeoutError("reply lost")
        restored = owner.DiscoveryJournalPreparationWork(task.epochs)
        assert begin(case.metadata, restored) == progress(case.metadata, task)
        assert finish(case.metadata, restored)[0].complete


@pytest.mark.parametrize(
    "raw",
    [
        b"truncated",
        frame([edge(0), edge(0)], kind="journal_init"),
    ],
)
def test_deferred_children_reach_explicit_native_fallback_without_acceptance(tmp_path, raw):
    with preparation_case(tmp_path / "source", raw) as (case, task):
        begin(case.metadata, task)
        assert finish(case.metadata, task)[0].complete
        assert task.rank.union.topology.epochs.source.binding.source_bytes == str(len(raw))


def test_actual_worker_handoff_keeps_stage_child_version_and_original_metadata_inode(tmp_path):
    with owned(tmp_path / "owned") as case:
        with quantum(case) as (work, framing):
            original = epochs.DiscoveryJournalEpochWork(framing, work.journal_source(framing))
            task = owner.DiscoveryJournalPreparationWork(original)
            begin(work.metadata, task)
            advance_until(work.metadata, task, lambda p: p.phase == "topology")
            before = progress(work.metadata, task)
            page = prepare(work.metadata, task)
            old_task = task
            path = work.metadata.custody.directory / "discovery.sqlite3"
            inode, binding = path.stat().st_ino, original.source.binding
        case.pool.close()
        case.pool = DiscoveryValidationOwner(case.factory, case.root)
        with quantum(case) as (work, framing):
            original = epochs.DiscoveryJournalEpochWork(framing, work.journal_source(framing))
            task = owner.DiscoveryJournalPreparationWork(original)
            assert binding == original.source.binding and inode == path.stat().st_ino
            assert begin(work.metadata, task) == before
            with pytest.raises(StaleArchiveTask):
                with work.metadata.transaction() as (db, key):
                    owner.apply_discovery_journal_preparation(db, key, old_task, page)
            assert finish(work.metadata, task)[0].complete
