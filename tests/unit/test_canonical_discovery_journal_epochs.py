from __future__ import annotations

from dataclasses import replace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core import canonical_discovery_journal_epochs as owner
from riverhog_core import canonical_discovery_journal_registration as registration
from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask
from riverhog_core.services.discovery_validation_owner import DiscoveryValidationOwner
from riverhog_provenance import (
    ProvenanceValidationError,
    iter_journal_frames,
    validate_journal_chunks,
)
from riverhog_provenance.constants import PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
from riverhog_provenance.epoch_boundaries import classify_epoch_boundary

from tests.unit.test_canonical_discovery_journal_source import source_case
from tests.unit.test_discovery_journal_owner import prepared, quantum


def selected(work, framing):
    source = work.journal_source(framing)
    return owner.DiscoveryJournalEpochWork(framing, source)


def begin(metadata, task):
    with metadata.transaction() as (db, key):
        return owner.begin_discovery_journal_epochs(db, key, task)


def progress(metadata, task):
    with metadata.read() as (db, key):
        return owner.discovery_journal_epoch_progress(db, key, task)


def prepare(metadata, task):
    with metadata.read() as (db, key):
        return owner.prepare_discovery_journal_epoch(db, key, task)


def apply(metadata, task, page, *, wrapper=lambda db: db):
    with metadata.transaction() as (db, key):
        return owner.apply_discovery_journal_epoch(wrapper(db), key, task, page)


def finish(metadata, task):
    while page := prepare(metadata, task):
        assert apply(metadata, task, page)
    return progress(metadata, task)


def test_actual_original_epoch_facts_match_native_classifier_and_commit_one_frame(
    tmp_path, monkeypatch
):
    classify = owner.classify_epoch_boundary
    with prepared(tmp_path / "owned") as case:
        with quantum(case) as (work, framing):
            task = selected(work, framing)
            raw = framing.source.read_exact(0, framing.source.identity.source_bytes)
            expected, epoch = [], 0
            for ordinal, frame in enumerate(iter_journal_frames((raw,))):
                fact = classify_epoch_boundary(
                    frame.json_bytes, ordinal=ordinal, previous_epoch=epoch
                )
                expected.append(fact)
                epoch = fact.epoch
            assert not (work.metadata.custody.directory / "journals").exists()
            assert begin(work.metadata, task).next_frame == 0
            calls = []

            def classified(*args, **kwargs):
                assert not work.metadata._db.in_transaction
                calls.append(kwargs["ordinal"])
                return classify(*args, **kwargs)

            monkeypatch.setattr(owner, "classify_epoch_boundary", classified)
            for count, wanted in enumerate(expected, 1):
                before = progress(work.metadata, task)
                page = prepare(work.metadata, task)
                assert page.boundary == wanted
                assert page.progress.next_frame == before.next_frame + 1
                assert prepare(work.metadata, task) is page
                assert apply(work.metadata, task, page)
                assert progress(work.metadata, task).next_frame == count
            assert prepare(work.metadata, task) is None
            final = progress(work.metadata, task)
            assert final.phase == "complete" and final.byte_offset == len(raw)
            assert final.version == len(expected) and calls == list(range(len(expected)))
            with work.metadata.read() as (db, _):
                assert db.execute(
                    "SELECT count(*) FROM discovery_journal_epoch_frames"
                ).fetchone() == (len(expected),)
            assert not (work.metadata.custody.directory / "journals").exists()


def test_epoch_cursor_resumes_same_file_and_binding_after_actual_worker_handoff(tmp_path):
    with prepared(tmp_path / "owned") as case:
        with quantum(case) as (work, framing):
            task = selected(work, framing)
            begin(work.metadata, task)
            assert apply(work.metadata, task, prepare(work.metadata, task))
            before = progress(work.metadata, task)
            old_task, old_page = task, prepare(work.metadata, task)
            binding = task.source.binding
            epoch = work.metadata.job.worker_epoch
            path = work.metadata.custody.directory / "discovery.sqlite3"
            inode = path.stat().st_ino
        case.pool.close()
        case.pool = DiscoveryValidationOwner(case.factory, case.root)
        with quantum(case) as (work, framing):
            task = selected(work, framing)
            assert work.metadata.job.worker_epoch == epoch + 1
            assert task.source.binding == binding
            assert begin(work.metadata, task) == before
            with pytest.raises(StaleArchiveTask):
                with work.metadata.transaction() as (db, key):
                    owner.apply_discovery_journal_epoch(db, key, old_task, old_page)
            assert progress(work.metadata, task) == before
            assert path.stat().st_ino == inode
            assert finish(work.metadata, task).phase == "complete"


def test_lost_commit_reply_observes_published_fact_without_reset_or_reclassification(tmp_path):
    with prepared(tmp_path / "owned") as case:
        with quantum(case) as (work, framing):
            task = selected(work, framing)
            begin(work.metadata, task)
            page = prepare(work.metadata, task)
            with pytest.raises(TimeoutError, match="reply lost"):
                assert apply(work.metadata, task, page)
                raise TimeoutError("checkpoint reply lost")
            expected = progress(work.metadata, task)
        with quantum(case) as (work, framing):
            task = selected(work, framing)
            assert begin(work.metadata, task) == expected
            next_page = prepare(work.metadata, task)
            assert next_page.position.ordinal == expected.next_frame
            assert apply(work.metadata, task, next_page)
            with work.metadata.read() as (db, _):
                assert db.execute(
                    "SELECT count(*) FROM discovery_journal_epoch_frames"
                ).fetchone() == (2,)


def test_interrupted_fact_and_checkpoint_publication_roll_back_together(tmp_path):
    class Interrupted:
        def __init__(self, db):
            self.db = db

        def __getattr__(self, name):
            return getattr(self.db, name)

        def execute(self, sql, *args):
            if sql.startswith("UPDATE discovery_journal_epochs"):
                raise InterruptedError("actual checkpoint publication interrupted")
            return self.db.execute(sql, *args)

    with prepared(tmp_path / "owned") as case:
        with quantum(case) as (work, framing):
            task = selected(work, framing)
            before = begin(work.metadata, task)
            page = prepare(work.metadata, task)
            with pytest.raises(InterruptedError, match="publication interrupted"):
                apply(work.metadata, task, page, wrapper=Interrupted)
            assert progress(work.metadata, task) == before
            with work.metadata.read() as (db, _):
                assert db.execute(
                    "SELECT count(*) FROM discovery_journal_epoch_frames"
                ).fetchone() == (0,)
            assert prepare(work.metadata, task) is page
            assert apply(work.metadata, task, page)
            assert progress(work.metadata, task).next_frame == 1


@pytest.mark.parametrize("damage", ["binding", "position", "fact", "cursor"])
def test_changed_private_facts_or_original_registration_fail_closed(tmp_path, damage):
    with prepared(tmp_path / "owned") as case:
        with quantum(case) as (work, framing):
            task = selected(work, framing)
            begin(work.metadata, task)
            assert apply(work.metadata, task, prepare(work.metadata, task))
            with work.metadata.transaction() as (db, _):
                if damage == "binding":
                    db.execute("UPDATE discovery_journal_children SET binding_json='{}'").close()
                elif damage == "position":
                    db.execute("UPDATE discovery_journal_epoch_frames SET json_bytes='0'").close()
                elif damage == "fact":
                    db.execute("DELETE FROM discovery_journal_epoch_frames").close()
                else:
                    db.execute("UPDATE discovery_journal_epochs SET byte_offset='0'").close()
            with pytest.raises(StaleArchiveTask):
                progress(work.metadata, task)


def test_only_the_actual_prepared_atom_can_publish_and_retry_cannot_duplicate_fact(tmp_path):
    with prepared(tmp_path / "owned") as case:
        with quantum(case) as (work, framing):
            task = selected(work, framing)
            begin(work.metadata, task)
            page = prepare(work.metadata, task)
            with pytest.raises(StaleArchiveTask, match="owned prepared"):
                apply(work.metadata, task, replace(page))
            assert apply(work.metadata, task, page)
            with pytest.raises(StaleArchiveTask, match="owned prepared"):
                apply(work.metadata, task, page)
            assert progress(work.metadata, task).next_frame == 1


def test_speculative_later_unknown_kind_defers_without_accepting_an_invalid_earlier_prefix(
    tmp_path,
):
    raw = b"".join(
        b"\x1e" + canonical_json_bytes({"entry_kind": kind}) + b"\n"
        for kind in ("assertion", "unknown")
    )
    with source_case(tmp_path / "source", raw, segments=1) as (case, framing, source):
        task = owner.DiscoveryJournalEpochWork(framing, source)
        with case.metadata.transaction() as (db, key):
            registration.register_discovery_journal(db, key, framing, source.binding)
        begin(case.metadata, task)
        final = finish(case.metadata, task)
        assert final.phase == "deferred" and final.next_frame == 1
        with pytest.raises(ProvenanceValidationError):
            validate_journal_chunks((raw,))


def test_deferred_original_framing_keeps_its_exact_captured_prefix(tmp_path):
    raw = b"\x1e" + canonical_json_bytes({"entry_kind": "assertion"}) + b"\ntruncated"
    with source_case(tmp_path / "source", raw, segments=1) as (case, framing, source):
        task = owner.DiscoveryJournalEpochWork(framing, source)
        with case.metadata.transaction() as (db, key):
            registration.register_discovery_journal(db, key, framing, source.binding)
        begin(case.metadata, task)
        final = finish(case.metadata, task)
        assert final.phase == "deferred" and final.next_frame == 1
        assert final.byte_offset < len(raw)


def test_existing_maximum_json_entry_is_one_atom_even_with_its_separators(tmp_path):
    template = {"entry_kind": "assertion", "payload": ""}
    empty = canonical_json_bytes(template)
    template["payload"] = "x" * (PROVENANCE_JOURNAL_ENTRY_BYTES_MAX - len(empty))
    encoded = canonical_json_bytes(template)
    assert len(encoded) == PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
    raw = b"\x1e" + encoded + b"\n"
    with source_case(tmp_path / "maximum", raw, segments=17) as (case, framing, source):
        task = owner.DiscoveryJournalEpochWork(framing, source)
        with case.metadata.transaction() as (db, key):
            registration.register_discovery_journal(db, key, framing, source.binding)
        begin(case.metadata, task)
        page = prepare(case.metadata, task)
        assert page.boundary.entry_kind == "assertion"
        assert page.progress.phase == "complete"
        assert apply(case.metadata, task, page)
        assert progress(case.metadata, task).byte_offset == len(raw)
