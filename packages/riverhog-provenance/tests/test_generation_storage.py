"""Separate original construction, retained durability profile and final receiving."""

from __future__ import annotations

import sqlite3
from contextlib import closing

import pytest
import retained_resource
from riverhog_provenance import JournalConstruction, JournalStore
from riverhog_provenance import _generation_storage as storage_owner
from riverhog_provenance._generation_page_map import InvalidGenerationMap
from riverhog_provenance._generation_storage import NativeGenerationStorage
from sqlite_admission import SqliteCapacity, SqliteConnectionAdmission

from tests.support.retained_resource import fault_native as fault_native


@pytest.fixture(autouse=True)
def native(monkeypatch, fault_native):
    module, _path = fault_native
    monkeypatch.setattr(retained_resource, "Resource", module.Resource)


@pytest.fixture
def directory(tmp_path):
    path = tmp_path / "owner"
    path.mkdir(mode=0o700)
    return path


def pool(limit=2, verify=lambda: None):
    return SqliteConnectionAdmission(
        connection_limit=limit, cursor_limit=4, operation_limit=4, verify_process=verify
    )


def frame(admission):
    return JournalConstruction(admission.connection_scope(), resource_limit=2)


def scalar(db, query):
    with closing(db.execute(query)) as cursor:
        return cursor.fetchone()[0]


@pytest.mark.parametrize("width", [512, 4096, 65536])
def test_original_generation_storage_uses_extra_and_reopens_its_exact_metadata(directory, width):
    admission = pool()
    original = frame(admission)
    calls = []

    def receive():
        assert original.dependents_received
        calls.append("received")

    store = NativeGenerationStorage.open(
        directory,
        reader_limit=4,
        metadata_octets=256,
        construction=original,
        page_size=width,
        create=True,
        on_storage_close=receive,
    )
    identity = store.store_id
    assert original.storage is store and original.phase == "ready"
    assert scalar(store.db, "PRAGMA journal_mode") == "delete"
    assert scalar(store.db, "PRAGMA synchronous") == 3
    assert scalar(store.db, "PRAGMA fullfsync") == 1
    assert scalar(store.db, "PRAGMA auto_vacuum") == 2
    assert store.objects.page_size == width
    assert original.close_step() and calls == ["received"]
    assert store.storage_received and admission.status.connections == 0
    fresh = frame(admission)
    try:
        reopened = NativeGenerationStorage.open(
            directory, reader_limit=4, metadata_octets=256, construction=fresh, page_size=width
        )
        assert reopened.store_id == identity
        assert scalar(reopened.db, "PRAGMA synchronous") == 3
        assert scalar(reopened.db, "SELECT count(*) FROM generation_objects") == 0
    finally:
        assert fresh.close_step()
    assert (directory / "generations.sqlite3").is_file()


def test_generation_and_working_storage_require_distinct_original_construction_frames(directory):
    admission = pool()
    working = frame(admission)
    retained = JournalStore.open_retained(directory, construction=working, create=True)
    try:
        with pytest.raises(RuntimeError, match="single-attempt"):
            NativeGenerationStorage.open(
                directory,
                reader_limit=4,
                metadata_octets=256,
                construction=working,
                page_size=512,
                create=True,
            )
        assert not (directory / "generations.sqlite3").exists()
        generation = frame(admission)
        try:
            actual = NativeGenerationStorage.open(
                directory,
                reader_limit=4,
                metadata_octets=256,
                construction=generation,
                page_size=512,
                create=True,
            )
            assert actual.db is not retained.db
            assert admission.status.connections == 2
        finally:
            assert generation.close_step()
        assert not retained.storage_received
        assert scalar(retained.db, "SELECT 1") == 1
    finally:
        assert working.close_step()


def test_original_connection_admission_refusal_precedes_generation_file_creation(directory):
    admission = pool(limit=1)
    held = admission.connect(":memory:")
    original = frame(admission)
    try:
        with pytest.raises(SqliteCapacity):
            NativeGenerationStorage.open(
                directory,
                reader_limit=4,
                metadata_octets=256,
                construction=original,
                page_size=512,
                create=True,
            )
        assert not list(directory.iterdir())
        assert original.creator.metrics["open_calls"] == 0
        assert original.close_step()
    finally:
        held.close()


@pytest.mark.parametrize("width", [True, 0, 513, 131072, 1.5])
def test_invalid_original_page_admission_is_passive(directory, width):
    calls = []
    admission = pool(verify=lambda: calls.append("verify"))
    original = frame(admission)
    with pytest.raises(ValueError):
        NativeGenerationStorage.open(
            directory,
            reader_limit=4,
            metadata_octets=256,
            construction=original,
            page_size=width,
            create=True,
        )
    assert not calls and not list(directory.iterdir())
    assert original.close_step()


@pytest.mark.parametrize(
    "fault,expected",
    [
        ("after_open", RuntimeError),
        ("before_close", RuntimeError),
        ("stat_refusal", OSError),
    ],
)
def test_original_creator_failure_keeps_the_same_frame_receivable(directory, fault, expected):
    admission = pool()
    original = frame(admission)
    original.creator._test_fault(fault)
    with pytest.raises(expected):
        NativeGenerationStorage.open(
            directory,
            reader_limit=4,
            metadata_octets=256,
            construction=original,
            page_size=512,
            create=True,
        )
    assert not original.received and original.storage is None
    assert original.close_step() and admission.status.connections == 0


@pytest.mark.parametrize("fault", ["early-callback", "configuration", "schema"])
def test_pre_return_original_storage_is_retained_before_failing_work(directory, monkeypatch, fault):
    admission = pool()
    original = frame(admission)
    expected = RuntimeError("original generation construction failure")
    handles, released = [], []

    def retain(store):
        assert original.storage is store
        handles.append(store)
        if fault == "early-callback":
            raise expected

    def fail(*args, **kwargs):
        assert len(handles) == 1
        raise expected

    if fault == "configuration":
        monkeypatch.setattr(storage_owner, "configure_sqlite_connection", fail)
    if fault == "schema":
        monkeypatch.setattr(storage_owner.GenerationObjectStore, "initialize", fail)
    with pytest.raises(RuntimeError) as caught:
        NativeGenerationStorage.open(
            directory,
            reader_limit=4,
            metadata_octets=256,
            construction=original,
            page_size=512,
            create=True,
            on_storage_open=retain,
            on_storage_close=lambda: released.append(True),
        )
    assert caught.value is expected and handles[0].storage_received and not released
    assert original.close_step() and admission.status.connections == 0


@pytest.mark.parametrize("fault", ["before", "after"])
def test_original_final_callback_receiving_is_retryable_without_closing_twice(directory, fault):
    admission = pool()
    original = frame(admission)
    calls, effects = [], []

    def receive():
        calls.append(1)
        if len(calls) == 1 and fault == "before":
            raise InterruptedError("original final receiving")
        effects.append(1)
        if len(calls) == 1 and fault == "after":
            raise InterruptedError("original final receiving reply lost")

    actual = NativeGenerationStorage.open(
        directory,
        reader_limit=4,
        metadata_octets=256,
        construction=original,
        page_size=512,
        create=True,
        on_storage_close=receive,
    )
    with pytest.raises(InterruptedError):
        original.close_step()
    assert actual._storage._database_closed and not actual.storage_received
    assert admission.status.connections == 0
    assert original.close_step() and actual.storage_received
    assert len(calls) == 2 and len(effects) == (1 if fault == "before" else 2)


@pytest.mark.parametrize("damage", ["mode", "format", "identity", "page-width", "schema"])
def test_incompatible_existing_store_refuses_without_repair_or_erasure(directory, damage):
    admission = pool()
    original = frame(admission)
    actual = NativeGenerationStorage.open(
        directory,
        reader_limit=4,
        metadata_octets=256,
        construction=original,
        page_size=512,
        create=True,
    )
    identity = actual.store_id
    if damage == "mode":
        assert scalar(actual.db, "PRAGMA journal_mode=WAL") == "wal"
    elif damage == "format":
        actual.db.execute("UPDATE generation_metadata SET format='different'").close()
    elif damage == "identity":
        actual.db.execute("UPDATE generation_metadata SET store_id='x'").close()
    elif damage == "page-width":
        actual.db.execute("UPDATE generation_metadata SET page_size=4096").close()
    else:
        actual.db.execute("DROP TABLE generation_objects").close()
    actual.db.commit()
    assert original.close_step()
    original = frame(admission)
    try:
        expected = sqlite3.OperationalError if damage == "schema" else InvalidGenerationMap
        with pytest.raises(expected):
            NativeGenerationStorage.open(
                directory, reader_limit=4, metadata_octets=256, construction=original, page_size=512
            )
        assert original.storage.storage_received
        assert (directory / "generations.sqlite3").is_file()
    finally:
        assert original.close_step()
    assert len(identity) == 32


def test_existing_file_is_not_truncated_and_missing_file_is_not_recreated(directory):
    admission = pool()
    path = directory / "generations.sqlite3"
    path.write_bytes(b"original incomplete optional bytes")
    original = frame(admission)
    with pytest.raises(FileExistsError):
        NativeGenerationStorage.open(
            directory,
            reader_limit=4,
            metadata_octets=256,
            construction=original,
            page_size=512,
            create=True,
        )
    assert original.close_step() and path.read_bytes() == b"original incomplete optional bytes"
    path.unlink()
    original = frame(admission)
    with pytest.raises(InvalidGenerationMap, match="missing"):
        NativeGenerationStorage.open(
            directory, reader_limit=4, metadata_octets=256, construction=original, page_size=512
        )
    assert original.close_step() and not path.exists()


def test_original_wrapper_allocation_failure_retains_the_exact_connection_scope(
    directory, monkeypatch
):
    admission = pool()
    original = frame(admission)
    cause = MemoryError("original generation wrapper allocation")

    def refuse(*args, **options):
        raise cause

    monkeypatch.setattr(storage_owner, "NativeStorage", refuse)
    with pytest.raises(MemoryError) as caught:
        NativeGenerationStorage.open(
            directory,
            reader_limit=4,
            metadata_octets=256,
            construction=original,
            page_size=512,
            create=True,
        )
    assert caught.value is cause
    assert original.storage is None and admission.status.connections == 1
    assert original.close_step() and admission.status.connections == 0


def test_original_final_reader_blocks_construction_receiving_and_late_borrow(directory):
    from riverhog_provenance._native_storage import NativeStorageLease

    admission = pool()
    original = frame(admission)
    actual = NativeGenerationStorage.open(
        directory,
        reader_limit=4,
        metadata_octets=256,
        construction=original,
        page_size=512,
        create=True,
    )
    actual._storage.retain(actual)
    reader = NativeStorageLease(actual._storage)
    try:
        assert not original.close_step()
        assert actual._lease.closed and not actual.storage_received
        assert scalar(reader.storage.connection, "SELECT 1") == 1
        with pytest.raises(RuntimeError, match="closed"):
            actual._storage.retain(actual)
    finally:
        reader.close()
    assert actual.storage_received and admission.status.connections == 0
    assert original.close_step()


def test_owner_named_generation_symlink_refuses_without_touching_target(directory, tmp_path):
    original_bytes = b"original foreign file"
    target = tmp_path / "foreign"
    target.write_bytes(original_bytes)
    (directory / "generations.sqlite3").symlink_to(target)
    admission = pool()
    original = frame(admission)
    with pytest.raises(InvalidGenerationMap, match="owner-created"):
        NativeGenerationStorage.open(
            directory,
            reader_limit=4,
            metadata_octets=256,
            construction=original,
            page_size=512,
            create=True,
        )
    assert original.creator.metrics["open_calls"] == 0
    assert target.read_bytes() == original_bytes
    assert original.close_step() and admission.status.connections == 0
