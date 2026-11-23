"""Actual unfinished Native image capture, restoration and original receiving."""

from __future__ import annotations

from contextlib import closing
from dataclasses import replace

import pytest
import retained_resource
from native_units import unit_budget
from riverhog_provenance import IncompleteSourceError, JournalValidationCapacity, validate_journal
from riverhog_provenance import _journal_image as image_owner
from riverhog_provenance._generation_storage import NativeGenerationStorage
from riverhog_provenance._journal_image import JournalImageRestore
from riverhog_provenance._native_conductor import ControlSlot
from test_retained_journal_session import OwnedSource, finish, open_session

from tests.support.retained_resource import fault_native as fault_native


def scalar(db, sql):
    with closing(db.execute(sql)) as rows:
        return rows.fetchone()[0]


@pytest.fixture
def origin(tmp_path, journal, catalog, journal_constructions):
    path = tmp_path / "working"
    path.mkdir(mode=0o700)
    source = OwnedSource(journal, catalog)
    session = open_session(
        path, source, catalog, epoch=7, create=True, journal_constructions=journal_constructions
    )
    generation = tmp_path / "generations"
    generation.mkdir(mode=0o700)
    storage = NativeGenerationStorage.open(
        generation,
        construction=journal_constructions.retain(),
        page_size=scalar(session._store.db, "PRAGMA page_size"),
        reader_limit=2,
        metadata_octets=1024,
        create=True,
    )
    try:
        yield session, source, storage
    finally:
        session.close()


def unfinished(session):
    for _ in range(1000):
        session.advance(unit_budget(rows=1))
        current = session._ledger.epoch.conductor._received.get(ControlSlot.GENERIC)
        if current is not None and not current.state.done:
            assert session._input.pending
            assert scalar(session._store.db, "SELECT count(*) FROM journal_validation_frame") == 1
            return session._ledger.load()
    raise AssertionError("small source did not reach its unfinished entry")


def capture(session, storage, *, page_limit=2):
    original = session.image_capture()
    try:
        original.begin(storage, "original-grant:1", image_octets=16 * 1024**2)
        while True:
            before_pages, before_octets = original.pages_captured, original.octets_read
            image = original.advance(page_limit=page_limit)
            pages = original.pages_captured - before_pages
            assert 0 < pages <= page_limit
            assert original.octets_read - before_octets == pages * storage.objects.page_size
            if image is not None:
                break
        assert original.pages_captured == image.page_count
        assert original.octets_read == image.page_count * image.page_size
        return image
    finally:
        original.close()
        assert original.received


def test_actual_unfinished_image_restores_exact_input_child_and_semantic_result(
    origin, tmp_path, journal, catalog, journal_constructions
):
    session, source, storage = origin
    before = unfinished(session)
    input_before = session._input.state
    image = capture(session, storage)
    assert image.binding == session._prefix.binding
    assert image.worker_epoch == 7 and image.checkpoint_version == before.checkpoint.version
    session.advance(unit_budget(rows=1))
    assert session._ledger.load().checkpoint.version > before.checkpoint.version
    restored = tmp_path / "restored"
    restored.mkdir(mode=0o700)
    original = JournalImageRestore(storage, image)
    try:
        original.begin(restored, image_octets=16 * 1024**2)
        while True:
            before_pages, before_octets = original.pages_restored, original.octets_written
            done = original.advance(page_limit=3)
            pages = original.pages_restored - before_pages
            assert 0 < pages <= 3
            assert original.octets_written - before_octets == pages * image.page_size
            if done:
                break
        assert original.octets_written == image.page_count * image.page_size
        assert len(storage.roots.readers) == 1
        with pytest.raises(RuntimeError, match="reader is not received"):
            storage.roots.retire(image.identity)
    finally:
        original.close()
        assert original.received
    source.calls.clear()
    with open_session(
        restored, source, catalog, epoch=8, journal_constructions=journal_constructions
    ) as resumed:
        assert resumed._prefix.binding == image.binding
        assert resumed._ledger.load().accepted == before.accepted
        assert resumed._ledger.load().checkpoint.version == image.checkpoint_version + 1
        assert resumed._input.state == input_before
        assert not source.calls
        finish(resumed)
        with resumed.result() as actual, validate_journal(journal, catalog=catalog) as expected:
            assert actual.materialize() == expected.materialize()
        assert not source.calls
    storage.roots.retire(image.identity)


def test_original_capture_excludes_execution_until_its_files_and_storage_receive(origin):
    session, source, storage = origin
    before = unfinished(session)
    first, second = session.image_capture(), session.image_capture()
    try:
        assert first.phase == "new" and first.file.metrics["open_calls"] == 0
        first.begin(storage, "original-grant:1", image_octets=16 * 1024**2)
        with pytest.raises(BlockingIOError, match="capture has not received"):
            session.advance(unit_budget())
        assert session._failure is None and session._ledger.load() == before
        with pytest.raises(BlockingIOError, match="has not received"):
            second.begin(storage, "original-grant:2", image_octets=16 * 1024**2)
        assert second.file.metrics["open_calls"] == 0
        second.close()
        assert first.advance(page_limit=1) is None
        assert first.pages_captured == 1 and session._ledger.load() == before
    finally:
        second.close()
        first.close()
    session.advance(unit_budget(rows=1))
    assert session._ledger.load().checkpoint.version > before.checkpoint.version


def test_capture_capacity_refusal_opens_no_file_or_generation_root(origin):
    session, _source, storage = origin
    original = session.image_capture()
    try:
        with pytest.raises(JournalValidationCapacity, match="baseline admission"):
            original.begin(storage, "original-grant:1", image_octets=1)
        assert original.file.metrics["open_calls"] == 0
        assert scalar(storage.db, "SELECT count(*) FROM generation_roots") == 0
        with pytest.raises(BlockingIOError):
            session.advance(unit_budget())
    finally:
        original.close()
    session.advance(unit_budget())


@pytest.mark.parametrize("fault", ["source", "cancel", "page-write"])
def test_capture_failure_preserves_its_original_cause_and_hold(origin, monkeypatch, fault):
    session, source, storage = origin
    original = session.image_capture()
    original.begin(storage, "original-grant:1", image_octets=16 * 1024**2)
    error = RuntimeError("original capture cause")

    def fail(*args, **kwargs):
        raise error

    if fault == "source":
        monkeypatch.setattr(source, "verify_current", fail)
    elif fault == "page-write":
        monkeypatch.setattr(storage.roots, "replace", fail)
    try:
        with pytest.raises(RuntimeError) as caught:
            original.advance(page_limit=1, check=fail if fault == "cancel" else None)
        assert caught.value is error and original.image is None
        assert original.phase == "failed-retained" and not original.received
        with pytest.raises(BlockingIOError):
            session.advance(unit_budget())
    finally:
        original.close()
    assert original.received and session._failure is None


def test_stale_source_is_fatal_before_any_baseline_file_or_root(origin):
    session, source, storage = origin
    source.current = False
    original = session.image_capture()
    try:
        with pytest.raises(IncompleteSourceError):
            original.begin(storage, "original-grant:1", image_octets=16 * 1024**2)
        assert original.file.metrics["open_calls"] == 0
        assert scalar(storage.db, "SELECT count(*) FROM generation_roots") == 0
    finally:
        original.close()


@pytest.mark.parametrize("damage", ["store", "root", "count", "existing"])
def test_restore_rejects_wrong_identity_incomplete_extent_and_existing_file(
    origin, tmp_path, damage
):
    session, _source, storage = origin
    image = capture(session, storage)
    if damage == "store":
        image = replace(image, store_id="0" * 32)
    elif damage == "root":
        image = replace(image, root="0" * 64)
    elif damage == "count":
        image = replace(image, page_count=image.page_count + 1)
    restored = tmp_path / "restore"
    restored.mkdir(mode=0o700)
    path = restored / "snapshot.sqlite3"
    if damage == "existing":
        path.write_bytes(b"original")
    original = JournalImageRestore(storage, image)
    try:
        with pytest.raises((RuntimeError, ValueError, FileExistsError)):
            original.begin(restored, image_octets=16 * 1024**2)
            while not original.advance(page_limit=4):
                pass
        assert original.phase == "failed-retained"
        if damage in {"store", "root"}:
            assert not path.exists()
        if damage == "existing":
            assert path.read_bytes() == b"original"
    finally:
        original.close()
    assert not storage.roots.readers and original.received


def test_session_close_keeps_original_working_storage_until_capture_receives(origin):
    session, _source, storage = origin
    original = session.image_capture()
    try:
        original.begin(storage, "original-grant:1", image_octets=16 * 1024**2)
        assert original.advance(page_limit=1) is None
        session.close()
        assert not session.storage_received
        assert original.borrow.active
    finally:
        original.close()
    assert original.received and session.storage_received


@pytest.mark.parametrize("fault", ["before_close", "after_close"])
def test_failed_original_file_receiving_keeps_execution_held_until_retry(
    origin, monkeypatch, fault_native, fault
):
    session, _source, storage = origin
    module, _path = fault_native
    monkeypatch.setattr(retained_resource, "Resource", module.Resource)
    monkeypatch.setattr(image_owner, "Borrow", module.Borrow)
    original = session.image_capture()
    try:
        original.begin(storage, "original-grant:1", image_octets=16 * 1024**2)
        original.file._test_fault(fault)
        with pytest.raises(RuntimeError):
            original.close()
        assert not original.received and not original.lease.closed
        assert original.file.received is (fault == "after_close")
        assert not original.borrow.active
        with pytest.raises(BlockingIOError):
            session.advance(unit_budget())
    finally:
        original.close()
    assert original.received
    session.advance(unit_budget())


def test_ordinary_stream_capture_is_passive_and_does_not_consume_or_spool(
    tmp_path, catalog, journal_constructions
):
    from riverhog_provenance.journal_session import JournalValidationSession

    reads = []

    def chunks():
        reads.append("read")
        yield b"not consumed"

    with JournalValidationSession(chunks(), attempt_id="e" * 32, catalog=catalog) as session:
        directory = tmp_path / "generation"
        directory.mkdir(mode=0o700)
        storage = NativeGenerationStorage.open(
            directory,
            construction=journal_constructions.retain(),
            page_size=scalar(session._store.db, "PRAGMA page_size"),
            reader_limit=1,
            metadata_octets=1024,
            create=True,
        )
        original = session.image_capture()
        try:
            with pytest.raises(ValueError, match="no implicit spool"):
                original.begin(storage, "original-grant:1", image_octets=16 * 1024**2)
            assert not reads and original.file.metrics["open_calls"] == 0
            assert scalar(storage.db, "SELECT count(*) FROM generation_roots") == 0
        finally:
            original.close()
