from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from riverhog_archive_contracts import (
    HistoryJournalAnchor,
    MemberHistoryBuilder,
    MemberHistoryPrimary,
)
from riverhog_archive_contracts import history_builder as builder_owner
from riverhog_client import CompletionRecords
from riverhog_client import completion_records as completion_owner
from riverhog_provenance import journal_store as journal_owner
from riverhog_provenance.journal_store import JournalStore

from tests.support.sqlite_admission import Population


def status(handle, population, expected):
    before = population.active, tuple(db.close_calls for db in population.connections)
    for _ in range(3):
        assert handle.cleanup_complete is expected
    assert (population.active, tuple(db.close_calls for db in population.connections)) == before


def interrupted_removal(monkeypatch, module, root, after):
    remove, calls = module.shutil.rmtree, []

    def interrupted(path, *args, **options):
        if Path(path) != root:
            return remove(path, *args, **options)
        calls.append(path)
        if len(calls) == 1:
            if after:
                remove(path, *args, **options)
            raise OSError("directory removal unconfirmed")
        return remove(path, *args, **options)

    monkeypatch.setattr(module.shutil, "rmtree", interrupted)
    return calls


def builder(population, **ports):
    return MemberHistoryBuilder(
        artifact_id="b" * 64,
        bytes=4,
        sha256="c" * 64,
        primary=MemberHistoryPrimary(
            HistoryJournalAnchor(
                "urn:uuid:11111111-1111-4111-8111-111111111111",
                "urn:uuid:22222222-2222-4222-8222-222222222222",
                0,
                "a" * 64,
                "d" * 64,
                1,
            ),
            "urn:uuid:33333333-3333-4333-8333-333333333333",
        ),
        connection_factory=population,
        **ports,
    )


def test_journal_handle_release_is_distinct_from_shared_storage_release():
    population = Population(limit=1)
    root = JournalStore(connection_factory=population)
    root.freeze()
    first, last = root.borrow(), root.borrow()
    status(root, population, False)
    root.close()
    status(root, population, True)
    status(first, population, False)
    assert population.active == 1
    first.close()
    status(first, population, True)
    status(last, population, False)
    assert population.active == 1 and population.connections[0].close_calls == 0
    last.close()
    status(last, population, True)
    assert population.active == 0 and population.connections[0].close_calls == 1


@pytest.mark.parametrize("after", [False, True])
def test_journal_cleanup_status_keeps_last_lease_until_close_confirmation(after):
    population = Population(limit=1)
    root = JournalStore(connection_factory=population)
    root.freeze()
    reader = root.borrow()
    root.close()
    db = population.connections[0]
    db.close_failures, db.close_after = 1, after
    with pytest.raises(OSError, match="close unconfirmed"):
        reader.close()
    status(root, population, True)
    status(reader, population, False)
    assert population.active == 1 and db.close_calls == 1
    reader.close()
    status(reader, population, True)
    assert population.active == 0 and db.close_calls == 2


@pytest.mark.parametrize("after", [False, True])
def test_journal_cleanup_status_requires_directory_acknowledgement(monkeypatch, after):
    population = Population(limit=1)
    store = JournalStore(connection_factory=population)
    calls = interrupted_removal(monkeypatch, journal_owner, population.attempts[0][0].parent, after)
    with pytest.raises(OSError, match="directory removal unconfirmed"):
        store.close()
    status(store, population, False)
    assert population.active == 0 and len(calls) == 1
    store.close()
    status(store, population, True)
    assert population.connections[0].close_calls == 1 and len(calls) == 2


@pytest.mark.parametrize("borrowed", [False, True])
@pytest.mark.parametrize("after", [False, True])
def test_journal_cleanup_status_requires_retained_owner_release_confirmation(
    tmp_path, borrowed, after
):
    population = Population(limit=1)
    directory = tmp_path / "retained"
    directory.mkdir(mode=0o700)
    calls, released = [], []

    def release():
        calls.append(True)
        if len(calls) == 1:
            if after:
                released.append(True)
            raise OSError("owner release unconfirmed")
        if not released:
            released.append(True)

    root = JournalStore.open_retained(
        directory, create=True, connection_factory=population, on_storage_close=release
    )
    root.freeze()
    handle = root.borrow() if borrowed else root
    if borrowed:
        root.close()
        status(root, population, True)
    with pytest.raises(OSError, match="owner release unconfirmed"):
        handle.close()
    status(handle, population, False)
    assert population.active == 0 and len(calls) == 1
    assert len(released) == int(after) and directory.is_dir()
    handle.close()
    status(handle, population, True)
    assert population.connections[0].close_calls == 1
    assert len(calls) == 2 and len(released) == 1 and directory.is_dir()


@pytest.mark.parametrize("after", [False, True])
def test_builder_cleanup_status_requires_actual_connection_confirmation(after):
    population = Population(limit=1)
    selected = builder(population, on_storage_open=lambda owner: status(owner, population, False))
    db = population.connections[0]
    db.close_failures, db.close_after = 1, after
    with pytest.raises(OSError, match="close unconfirmed"):
        selected.close()
    status(selected, population, False)
    assert population.active == 1 and db.close_calls == 1
    selected.close()
    status(selected, population, True)
    assert population.active == 0 and db.close_calls == 2


@pytest.mark.parametrize("after", [False, True])
def test_builder_cleanup_status_includes_uncertain_verification_child(after):
    population, heads = Population(limit=2), []

    def retained(child):
        heads.append(child)
        population.connections[-1].close_failures = 1
        population.connections[-1].close_after = after

    selected = builder(
        population,
        verification_connection_factory=population,
        on_verification_storage_open=retained,
    )
    with pytest.raises(OSError, match="close unconfirmed"):
        selected.seal()
    assert len(heads) == 1 and population.active == 2
    status(selected, population, False)
    assert [db.close_calls for db in population.connections] == [0, 1]
    selected.close()
    status(selected, population, True)
    assert population.active == 0 and heads[0].cleanup_complete


@pytest.mark.parametrize("after", [False, True])
def test_builder_cleanup_status_does_not_infer_confirmation_from_missing_directory(
    monkeypatch, after
):
    population = Population(limit=1)
    selected = builder(population)
    calls = interrupted_removal(monkeypatch, builder_owner, population.attempts[0][0].parent, after)
    with pytest.raises(OSError, match="directory removal unconfirmed"):
        selected.close()
    status(selected, population, False)
    selected.close()
    status(selected, population, True)
    assert population.connections[0].close_calls == 1 and len(calls) == 2


class Stream:
    def __init__(self, raw):
        self.raw = raw
        self.close_calls = self.writes = 0
        self.failures = 0
        self.after = False
        self.on_read = None

    @property
    def closed(self):
        return self.raw.closed

    def write(self, value):
        self.writes += 1
        return self.raw.write(value)

    def read(self, size):
        result = self.raw.read(size)
        callback, self.on_read = self.on_read, None
        if callback is not None:
            callback()
        return result

    def close(self):
        self.close_calls += 1
        if self.failures:
            self.failures -= 1
            if self.after:
                self.raw.close()
            raise OSError("stream close unconfirmed")
        return self.raw.close()


class Factory:
    def __init__(self):
        self.population = Population(limit=1)
        self.cursors, self.streams, self.handles = [], [], []
        self.statement = None
        self.after = False
        self.on_cursor = self.on_stream = self.on_fetch = None

    def __call__(self, path, **options):
        db = self.population(path, **options)
        factory = self

        class Cursor(sqlite3.Cursor):
            statement = ""
            close_calls = 0

            def execute(self, statement, parameters=()):
                self.statement = statement
                return super().execute(statement, parameters)

            def fetchone(self):
                result = super().fetchone()
                callback, factory.on_fetch = factory.on_fetch, None
                if callback is not None:
                    callback()
                return result

            def close(self):
                self.close_calls += 1
                if factory.statement is not None and factory.statement in self.statement:
                    factory.statement = None
                    if factory.after:
                        super().close()
                    raise OSError("cursor close unconfirmed")
                return super().close()

        make = db.cursor

        def cursor(*args, **options):
            options.setdefault("factory", Cursor)
            result = make(*args, **options)
            factory.cursors.append(result)
            if factory.on_cursor is not None:
                factory.on_cursor(result)
            return result

        db.cursor = cursor
        return db

    def prepare(self, monkeypatch):
        def retain(records):
            self.handles.append(records)
            status(records, self.population, False)

        records = CompletionRecords(connection_factory=self, on_storage_open=retain)
        make = Path.open

        def opened(path, *args, **options):
            raw = make(path, *args, **options)
            if path.parent != records._root:
                return raw
            stream = Stream(raw)
            self.streams.append(stream)
            if self.on_stream is not None:
                self.on_stream(stream)
            return stream

        monkeypatch.setattr(Path, "open", opened)
        return records

    def assert_closed(self):
        assert self.population.active == 0
        assert all(owner.cleanup_complete for owner in self.handles)
        assert all(stream.closed for stream in self.streams)
        assert all(not path.parent.exists() for path, _ in self.population.attempts)


def populate(records):
    for key in ("a", "b"):
        records.add(key, (b"x" * (256 * 1024),))
        records.add_output({"artifact_id": key, "output_id": key}, {"input": key})


def test_completion_close_settles_suspended_cursors_and_actual_payload_streams(monkeypatch):
    factory = Factory()
    records = factory.prepare(monkeypatch)
    populate(records)
    selected, outputs = records.records(), records.output_rows()
    first = next(selected)
    assert next(outputs)["artifact_id"] == "a"
    reader = first.read()
    assert len(next(reader)) == 128 * 1024
    assert len(records._cursors) == 2 and len(records._streams) == 1
    stream = factory.streams[-1]
    records.close()
    status(records, factory.population, True)
    assert stream.closed and stream.close_calls == 1
    assert all(cursor.close_calls == 1 for cursor in factory.cursors)
    for iterator in (selected, outputs, reader):
        with pytest.raises(RuntimeError, match="closed"):
            next(iterator)
    factory.assert_closed()


@pytest.mark.parametrize("stage", ["schema", "write", "point", "rows"])
@pytest.mark.parametrize("after", [False, True])
def test_completion_cursor_uncertainty_precedes_database_and_directory_release(
    monkeypatch, stage, after
):
    factory = Factory()
    factory.after = after
    if stage == "schema":
        factory.statement = "CREATE TABLE records"
        with pytest.raises(OSError, match="cursor close unconfirmed"):
            CompletionRecords(connection_factory=factory, on_storage_open=factory.handles.append)
        records = factory.handles[0]
    else:
        records = factory.prepare(monkeypatch)
        if stage == "rows":
            populate(records)
            rows = records.output_rows()
            next(rows)
            factory.statement = "SELECT value FROM outputs"
            with pytest.raises(OSError, match="cursor close unconfirmed"):
                records.close()
            rows.close()
        else:
            factory.statement = "INSERT INTO records" if stage == "write" else "SELECT size"
            with pytest.raises(OSError, match="cursor close unconfirmed"):
                records.add("a", (b"exact",))
    status(records, factory.population, False)
    assert factory.population.active == 1
    assert factory.population.connections[0].close_calls == 0
    assert records._root.is_dir()
    failed = next(c for c in records._cursors if c.close_calls == 1)
    assert failed.close_calls == 1
    records.close()
    assert failed.close_calls == 2
    factory.assert_closed()


@pytest.mark.parametrize("stage", ["read", "write"])
@pytest.mark.parametrize("after", [False, True])
def test_completion_stream_uncertainty_stays_reachable_without_implicit_retry(
    monkeypatch, stage, after
):
    factory = Factory()
    records = factory.prepare(monkeypatch)
    if stage == "write":

        def fail(stream):
            stream.failures, stream.after = 1, after

        factory.on_stream = fail
        with pytest.raises(OSError, match="stream close unconfirmed"):
            with records:
                records.add("a", (b"exact",))
    else:
        value = records.add("a", (b"x" * (256 * 1024),))
        reader = value.read()
        next(reader)
        stream = factory.streams[-1]
        stream.failures, stream.after = 1, after
        with pytest.raises(OSError, match="stream close unconfirmed"):
            records.close()
        reader.close()
    stream = factory.streams[-1]
    assert stream.close_calls == 1
    status(records, factory.population, False)
    assert factory.population.connections[0].close_calls == 0
    assert records._root.is_dir()
    records.close()
    assert stream.close_calls == 2
    factory.assert_closed()


@pytest.mark.parametrize("resource", ["cursor", "stream"])
@pytest.mark.parametrize("after", [None, False, True])
def test_completion_acquisition_must_settle_before_database_close(monkeypatch, resource, after):
    factory = Factory()
    records = factory.prepare(monkeypatch)
    value = records.add("a", (b"exact",))

    def close_during_acquisition(_result):
        with pytest.raises(RuntimeError, match="acquisition is still active"):
            records.close()
        assert factory.population.connections[0].close_calls == 0

    if resource == "cursor":
        factory.on_cursor = close_during_acquisition

        def call():
            return records.output_count
    else:
        factory.on_stream = close_during_acquisition

        def call():
            return next(value.read())

    with pytest.raises(RuntimeError, match="closed"):
        call()
    status(records, factory.population, False)
    if resource == "cursor":
        assert factory.cursors[-1].statement == ""
    db = factory.population.connections[0]
    if after is not None:
        db.close_failures, db.close_after = 1, after
        with pytest.raises(OSError, match="close unconfirmed"):
            records.close()
        status(records, factory.population, False)
    records.close()
    factory.assert_closed()


def test_completion_close_during_payload_fetch_prevents_chunk_handoff(monkeypatch):
    factory = Factory()
    records = factory.prepare(monkeypatch)
    value = records.add("a", (b"exact",))
    factory.on_stream = lambda stream: setattr(stream, "on_read", records.close)
    with pytest.raises(RuntimeError, match="closed"):
        next(value.read())
    factory.assert_closed()


def test_completion_close_during_point_fetch_prevents_result_handoff(monkeypatch):
    factory = Factory()
    records = factory.prepare(monkeypatch)
    populate(records)
    factory.on_fetch = records.close
    with pytest.raises(RuntimeError, match="closed"):
        records.output_for_key("a")
    factory.assert_closed()


def test_completion_close_during_chunk_iteration_prevents_further_write(monkeypatch):
    factory = Factory()
    records = factory.prepare(monkeypatch)

    def chunks():
        yield b"first"
        records.close()
        yield b"later"

    with pytest.raises(RuntimeError, match="closed"):
        records.add("a", chunks())
    assert factory.streams[-1].writes == 1
    factory.assert_closed()


@pytest.mark.parametrize("after", [False, True])
def test_completion_cleanup_status_requires_directory_confirmation(monkeypatch, after):
    factory = Factory()
    records = factory.prepare(monkeypatch)
    records.add("a", (b"exact",))
    calls = interrupted_removal(monkeypatch, completion_owner, records._root, after)
    with pytest.raises(OSError, match="directory removal unconfirmed"):
        records.close()
    status(records, factory.population, False)
    assert factory.population.active == 0
    records.close()
    status(records, factory.population, True)
    assert factory.population.connections[0].close_calls == 1 and len(calls) == 2
    factory.assert_closed()


def test_completion_normal_generator_release_closes_only_its_resources(monkeypatch):
    factory = Factory()
    records = factory.prepare(monkeypatch)
    populate(records)
    selected, outputs = records.records(), records.output_rows()
    reader = next(selected).read()
    next(outputs)
    next(reader)
    assert len(records._cursors) == 2 and len(records._streams) == 1
    for iterator in (selected, outputs, reader):
        iterator.close()
    assert not records._cursors and not records._streams
    assert factory.streams[-1].closed
    status(records, factory.population, False)
    assert factory.population.active == 1
    assert records.output_count == 2
    records.close()
    factory.assert_closed()
