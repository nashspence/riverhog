from __future__ import annotations

import sqlite3
from contextlib import closing, contextmanager, nullcontext
from dataclasses import replace

import pytest
from riverhog_core import canonical_discovery_archive_framing as framing
from riverhog_core.canonical_discovery_archive_source import ArchiveJournalSource
from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask
from riverhog_provenance import ProvenanceValidationError, validate_journal_chunks

from tests.support.provenance_archive import archive_reader_fixture, discovery_archive_admission
from tests.unit.test_canonical_discovery_archive_source_growth import seed
from tests.unit.test_canonical_discovery_archive_work import drive, open_work, root_for
from tests.unit.test_canonical_discovery_metadata_growth import case_for


@contextmanager
def framing_case(path, raw, *, segments=1, retained=17, opening=None):
    ends = tuple(len(raw) * ordinal // segments for ordinal in range(1, segments))
    reader, objects, journal_id, _ = archive_reader_fixture(raw, segment_ends=ends)
    admission = discovery_archive_admission(root_for(objects))
    db, key = open_work(":memory:", admission)
    path.mkdir(parents=True)
    template = path / "original.sqlite"
    try:
        drive(db, key, admission, reader)
        with closing(sqlite3.connect(template)) as saved:
            db.backup(saved)
    finally:
        db.close()
    with case_for(path / "owned", admission, retained, 17) as case:
        with opening() if opening is not None else nullcontext():
            case.metadata.open()
        seed(case, template, retained)
        source = ArchiveJournalSource(case.metadata, reader, journal_id)
        work = framing.ArchiveJournalFramingWork(source)
        yield case, work


def begin(case, work):
    with case.metadata.transaction() as (db, key):
        return framing.begin_archive_framing(db, key, work)


def progress(case, work):
    with case.metadata.read() as (db, key):
        return framing.archive_framing_progress(db, key, work)


def prepare(case, work, *, byte_budget=65536, frame_budget=64):
    with case.metadata.read() as (db, key):
        return framing.prepare_archive_framing(
            db, key, work, byte_budget=byte_budget, frame_budget=frame_budget
        )


def apply(case, work, page):
    with case.metadata.transaction() as (db, key):
        return framing.apply_archive_framing(db, key, work, page)


@pytest.mark.parametrize(
    ("byte_budget", "frame_budget", "segments"), [(1, 1, 17), (7, 1, 2), (128, 3, 17)]
)
def test_original_frame_positions_resume_across_hostile_bounds(
    tmp_path, byte_budget, frame_budget, segments
):
    raw = b"\x1e{}\n" * 17
    with framing_case(tmp_path / "frames", raw, segments=segments) as (case, work):
        assert begin(case, work).cursor.frame_count == 0
        units = 0
        while page := prepare(case, work, byte_budget=byte_budget, frame_budget=frame_budget):
            assert (
                0
                < page.advance.cursor.byte_offset - page.expected.cursor.byte_offset
                <= byte_budget
            )
            assert len(page.advance.positions) <= frame_budget
            assert apply(case, work, page)
            # Reconstruct only the small source/framing owner at every boundary.
            # Its committed directory is never reset or copied.
            work = framing.ArchiveJournalFramingWork(
                ArchiveJournalSource(
                    case.metadata, work.source.reader, work.source.identity.journal_id
                )
            )
            units += 1
        terminal = progress(case, work)
        assert terminal.phase == "complete" and terminal.failure is None
        assert terminal.cursor.byte_offset == len(raw)
        assert terminal.cursor.frame_count == 17 and units >= 6
        with case.metadata.read() as (db, key):
            for ordinal in range(17):
                position = framing.archive_journal_frame(db, key, work, ordinal)
                assert (position.ordinal, position.byte_offset, position.json_bytes) == (
                    ordinal,
                    ordinal * 4,
                    2,
                )
            assert framing.archive_journal_frame(db, key, work, 17) is None


def test_prepared_retry_is_bounded_by_the_current_call_and_rejects_forgery(tmp_path):
    with framing_case(tmp_path / "retry", b"\x1e{}\n" * 17) as (case, work):
        begin(case, work)
        broad = prepare(case, work)
        assert prepare(case, work) is broad
        narrow = prepare(case, work, byte_budget=1, frame_budget=1)
        assert narrow is not broad and narrow.advance.cursor.byte_offset == 1
        with pytest.raises(StaleArchiveTask):
            apply(case, work, broad)
        with pytest.raises(StaleArchiveTask):
            apply(case, work, replace(narrow))
        assert apply(case, work, narrow)
        assert apply(case, work, narrow) is False


def test_atomic_rollback_and_competing_preparations_preserve_committed_positions(tmp_path):
    with framing_case(tmp_path / "rollback", b"\x1e{}\n" * 17) as (case, work):
        initial = begin(case, work)
        other = framing.ArchiveJournalFramingWork(work.source)
        first = prepare(case, work, frame_budget=3)
        second = prepare(case, other, frame_budget=3)
        with pytest.raises(RuntimeError, match="interrupt"):
            with case.metadata.transaction() as (db, key):
                assert framing.apply_archive_framing(db, key, work, first)
                raise RuntimeError("interrupt")
        assert progress(case, work) == initial
        assert prepare(case, work, frame_budget=3) is first
        assert apply(case, work, first)
        assert apply(case, other, second) is False
        assert progress(case, work).cursor.frame_count == 3
        with case.metadata.read() as (db, key):
            assert framing.archive_journal_frame(db, key, work, 2).byte_offset == 8


@pytest.mark.parametrize("raw", [b"x", b"\x1e{}\nBAD", b"\x1e{}", b"\x1e{}\x1e"])
def test_speculative_framing_failure_is_persisted_without_semantic_acceptance(tmp_path, raw):
    with framing_case(tmp_path / "deferred", raw) as (case, work):
        begin(case, work)
        page = prepare(case, work)
        assert page.advance.failure is not None
        assert apply(case, work, page)
        terminal = progress(case, work)
        assert terminal.phase == "deferred" and terminal.failure == page.advance.failure
        assert prepare(case, work) is None
        # The same original bytes still go through ordered Native validation.
        with pytest.raises(ProvenanceValidationError):
            with validate_journal_chunks((raw,), require_profiles=False):
                pytest.fail("framing metadata supplied semantic acceptance")


@pytest.mark.parametrize("damage", ["counter", "position", "header", "worker"])
def test_changed_control_or_original_position_is_preserved_and_rejected(tmp_path, damage):
    with framing_case(tmp_path / "changed", b"\x1e{}\n" * 17) as (case, work):
        begin(case, work)
        page = prepare(case, work, frame_budget=3)
        assert apply(case, work, page)
        db = case.metadata._db
        if damage == "counter":
            db.execute("UPDATE discovery_archive_framing SET frame_count='03'").close()
        elif damage == "position":
            db.execute("DELETE FROM discovery_archive_frames WHERE ordinal='2'").close()
        elif damage == "header":
            db.execute("UPDATE discovery_archive_framing SET source_sha256=?", ("0" * 64,)).close()
        else:
            db.execute("UPDATE discovery_archive_framing SET worker_id='other'").close()
        with pytest.raises(StaleArchiveTask):
            progress(case, work)
        with pytest.raises(StaleArchiveTask):
            begin(case, work)
        assert db.execute("SELECT count(*) FROM discovery_archive_framing").fetchone() == (1,)


def test_catalog_worker_loss_during_preparation_emits_no_detached_result(tmp_path, monkeypatch):
    with framing_case(tmp_path / "stale", b"\x1e{}\n") as (case, work):
        begin(case, work)
        original = framing.advance_journal_framing

        def lost(*args, **kwargs):
            result = original(*args, **kwargs)
            monkeypatch.setattr(work.source, "verify_current", lambda: False)
            return result

        monkeypatch.setattr(framing, "advance_journal_framing", lost)
        with pytest.raises(StaleArchiveTask):
            prepare(case, work)
        assert work._prepared is None


@pytest.mark.parametrize("damage", ["count", "offset", "ordinal", "position", "terminal"])
def test_native_acknowledgement_cannot_expand_the_independent_source_window(
    tmp_path, monkeypatch, damage
):
    with framing_case(tmp_path / "ack", b"\x1e{}\n" * 17) as (case, work):
        initial = begin(case, work)
        original = framing.advance_journal_framing

        def changed(*args, **kwargs):
            result = original(*args, **kwargs)
            if damage == "count":
                return replace(result, cursor=replace(result.cursor, frame_count=999))
            if damage == "offset":
                return replace(result, cursor=replace(result.cursor, byte_offset=999))
            if damage in {"ordinal", "position"}:
                first = replace(
                    result.positions[0],
                    **({"ordinal": 999} if damage == "ordinal" else {"byte_offset": 1}),
                )
                return replace(result, positions=(first, *result.positions[1:]))
            return replace(result, complete=True)

        monkeypatch.setattr(framing, "advance_journal_framing", changed)
        with pytest.raises(StaleArchiveTask):
            prepare(case, work, frame_budget=3)
        assert progress(case, work) == initial
        assert work._prepared is None


def test_real_worker_epoch_handoff_preserves_original_directory_and_rejects_old_source(tmp_path):
    from riverhog_core import canonical_discovery_corpus_jobs as jobs
    from riverhog_core.canonical_discovery_metadata import DiscoveryMetadata

    with framing_case(tmp_path / "handoff", b"\x1e{}\n" * 17) as (case, work):
        begin(case, work)
        assert apply(case, work, prepare(case, work, frame_budget=3))
        saved = progress(case, work)
        previous = case.metadata
        previous.close()
        claimed = jobs.claim_discovery_worker(case.factory, previous.job)
        assert claimed is not None and claimed.worker_epoch > previous.job.worker_epoch
        current = DiscoveryMetadata(case.factory, claimed, case.admission, previous.custody)
        case.metadata = current
        try:
            current.open()
            resumed = framing.ArchiveJournalFramingWork(
                ArchiveJournalSource(current, work.source.reader, work.source.identity.journal_id)
            )
            assert begin(case, resumed) == saved
            assert work.source.verify_current() is False
            with pytest.raises(StaleArchiveTask):
                progress(case, work)
            while page := prepare(case, resumed):
                assert apply(case, resumed, page)
            assert progress(case, resumed).cursor.frame_count == 17
        finally:
            current.close()
            case.metadata = previous
