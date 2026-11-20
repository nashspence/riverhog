from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest
from riverhog_client import initial_tags as owner

from tests.support.sqlite_admission import Capacity, Population


def values(count):
    return tuple(f"camera/{index:04d}" for index in range(count))


class Factory:
    def __init__(self, *, cursor_fault=None, after=False):
        self.population = Population(limit=1)
        self.handles, self.cursors = [], []
        self.cursor_fault, self.after = cursor_fault, after
        self.armed = cursor_fault is not None
        self.on_fetch = None
        self.fail_schema = False
        self.cursor_starts = []

    def __call__(self, path, **options):
        db = self.population(path, **options)
        factory = self

        class Cursor(sqlite3.Cursor):
            statement = ""
            close_calls = 0

            def execute(self, statement, parameters=()):
                self.statement = statement
                return super().execute(statement, parameters)

            def executescript(self, statement):
                self.statement = statement
                if factory.fail_schema:
                    raise InterruptedError("schema interrupted")
                return super().executescript(statement)

            def fetchmany(self, size):
                result = super().fetchmany(size)
                if factory.on_fetch is not None:
                    callback, factory.on_fetch = factory.on_fetch, None
                    callback()
                return result

            def close(self):
                self.close_calls += 1
                if factory.armed and factory.cursor_fault in self.statement:
                    factory.armed = False
                    if factory.after:
                        super().close()
                    raise OSError("cursor close unconfirmed")
                return super().close()

        make = db.cursor
        self.cursor_starts.append(len(self.cursors))

        def cursor(*args, **options):
            options.setdefault("factory", Cursor)
            selected = make(*args, **options)
            self.cursors.append(selected)
            return selected

        db.cursor = cursor
        return db

    def retain(self, prepared):
        assert prepared._connection is self.population.connections[-1]
        assert len(self.cursors) == self.cursor_starts[-1]
        self.handles.append(prepared)

    def prepare(self, tags):
        return owner.prepare_initial_collection_tags(
            tags, connection_factory=self, on_storage_open=self.retain
        )

    def assert_closed(self):
        assert self.population.active == 0
        assert all(handle.cleanup_complete for handle in self.handles)
        assert all(not path.parent.exists() for path, _ in self.population.attempts)


@pytest.mark.parametrize("count", [0, 1, 129, 385])
def test_initial_tags_admission_preserves_complete_identity_and_bounded_replay(count):
    tags, factory = values(count), Factory()
    with owner.prepare_initial_collection_tags(tags) as expected:
        identity = expected.tag_set_identity
    with factory.prepare(reversed(tags)) as prepared:
        assert factory.handles == [prepared]
        assert prepared.tag_set_identity == identity and prepared.count == count
        for _ in range(2):
            batches = tuple(prepared.iter_batches())
            assert tuple(tag for batch in batches for tag in batch) == tags
            assert all(0 < len(batch) <= 100 for batch in batches)
        assert not prepared._cursors
    assert all(cursor.close_calls == 1 for cursor in factory.cursors)
    assert factory.population.peak == 1
    factory.assert_closed()


@pytest.mark.parametrize("unreturned", [False, True])
def test_initial_tags_factory_refusal_precedes_input_consumption_and_retention(unreturned):
    factory, consumed = Factory(), []
    factory.population.refuse = not unreturned
    factory.population.unreturned = unreturned

    def supplied():
        consumed.append(True)
        yield "camera/one"

    with pytest.raises(OSError if unreturned else Capacity):
        factory.prepare(supplied())
    assert not consumed and not factory.handles and not factory.cursors
    assert factory.population.active == int(unreturned)
    if unreturned:
        assert factory.population.connections[0].close_calls == 0
        factory.population.connections[0].close()
    shutil.rmtree(factory.population.attempts[0][0].parent)
    factory.assert_closed()


def test_initial_tags_default_connect_failure_removes_only_its_owned_scratch(monkeypatch):
    paths, make = [], owner.tempfile.mkdtemp

    def created(**options):
        result = make(**options)
        paths.append(Path(result))
        return result

    def fail(*_args, **_options):
        raise OSError("connect refused")

    monkeypatch.setattr(owner.tempfile, "mkdtemp", created)
    monkeypatch.setattr(owner, "sqlite3", SimpleNamespace(connect=fail))
    with pytest.raises(OSError, match="connect refused"):
        owner.prepare_initial_collection_tags(values(1))
    assert len(paths) == 1 and not paths[0].exists()


@pytest.mark.parametrize("stage", ["retention", "schema"])
@pytest.mark.parametrize("after", [None, False, True])
def test_initial_tags_initialization_retains_actual_uncertain_owner(stage, after):
    factory, consumed = Factory(), []

    def retain(prepared):
        factory.retain(prepared)
        db = factory.population.connections[-1]
        if after is not None:
            db.close_failures, db.close_after = 1, after
        if stage == "retention":
            raise InterruptedError("retention interrupted")

        factory.fail_schema = True

    def supplied():
        consumed.append(True)
        yield "camera/one"

    with pytest.raises(InterruptedError if after is None else OSError):
        owner.prepare_initial_collection_tags(
            supplied(), connection_factory=factory, on_storage_open=retain
        )
    assert not consumed and len(factory.handles) == 1
    prepared = factory.handles[0]
    assert factory.population.active == int(after is not None)
    assert factory.population.connections[0].close_calls == 1
    if after is not None:
        assert not prepared.cleanup_complete
        with pytest.raises(RuntimeError, match="closed"):
            next(prepared.iter_batches())
        prepared.close()
    factory.assert_closed()


@pytest.mark.parametrize(
    "statement",
    [
        "CREATE TABLE tags",
        "INSERT INTO tags",
        "INSERT INTO nodes",
        "SELECT encoded FROM nodes",
        "SELECT tag FROM tags",
    ],
)
@pytest.mark.parametrize("after", [False, True])
def test_initial_tags_uncertain_cursor_blocks_connection_and_directory_release(statement, after):
    factory = Factory(cursor_fault=statement, after=after)
    with pytest.raises(OSError, match="cursor close unconfirmed"):
        factory.prepare(values(3))
    prepared = factory.handles[0]
    uncertain = next(
        cursor
        for cursor in factory.cursors
        if cursor.close_calls == 1 and statement in cursor.statement
    )
    assert factory.population.active == 1
    assert factory.population.connections[0].close_calls == 0
    assert prepared._root.is_dir() and not prepared.cleanup_complete
    with pytest.raises(RuntimeError, match="closed"):
        next(prepared.iter_batches())
    assert uncertain.close_calls == 1
    prepared.close()
    assert uncertain.close_calls == 2
    factory.assert_closed()


@pytest.mark.parametrize("after", [False, True])
def test_initial_tags_context_does_not_retry_explicit_uncertain_connection_close(after):
    factory = Factory()
    with pytest.raises(OSError, match="close unconfirmed"):
        with factory.prepare(values(2)) as prepared:
            db = factory.population.connections[0]
            db.close_failures, db.close_after = 1, after
            prepared.close()
    assert db.close_calls == 1 and factory.population.active == 1
    prepared.close()
    assert db.close_calls == 2
    factory.assert_closed()


def test_initial_tags_closing_freezes_independent_suspended_batches():
    factory = Factory()
    prepared = factory.prepare(values(205))
    first, second = prepared.iter_batches(), prepared.iter_batches()
    assert next(first) == next(second) == values(205)[:100]
    assert len(prepared._cursors) == 2
    prepared.close()
    for batches in (first, second, prepared.iter_batches()):
        with pytest.raises(RuntimeError, match="closed"):
            next(batches)
    factory.assert_closed()


def test_initial_tags_close_during_fetch_cannot_return_a_batch():
    factory = Factory()
    prepared = factory.prepare(values(2))
    factory.on_fetch = prepared.close
    with pytest.raises(RuntimeError, match="closed"):
        next(prepared.iter_batches())
    factory.assert_closed()


@pytest.mark.parametrize("after", [False, True])
def test_initial_tags_directory_retry_does_not_reclose_confirmed_connection(monkeypatch, after):
    factory = Factory()
    prepared = factory.prepare(values(1))
    remove, calls = owner.shutil.rmtree, []

    def interrupted(path):
        calls.append(path)
        if len(calls) == 1:
            if after:
                remove(path)
            raise OSError("directory removal unconfirmed")
        return remove(path)

    monkeypatch.setattr(owner.shutil, "rmtree", interrupted)
    with pytest.raises(OSError, match="directory removal unconfirmed"):
        prepared.close()
    assert factory.population.active == 0 and not prepared.cleanup_complete
    assert factory.population.connections[0].close_calls == 1
    prepared.close()
    assert factory.population.connections[0].close_calls == 1 and len(calls) == 2
    factory.assert_closed()


@pytest.mark.parametrize("after", [False, True])
def test_initial_tags_commit_interruption_cannot_publish_ready_intent(after):
    factory = Factory()

    def retain(prepared):
        factory.retain(prepared)
        commit = prepared._connection.commit

        def interrupted():
            if after:
                commit()
            raise InterruptedError("commit unconfirmed")

        prepared._connection.commit = interrupted

    with pytest.raises(InterruptedError, match="commit unconfirmed"):
        owner.prepare_initial_collection_tags(
            values(2), connection_factory=factory, on_storage_open=retain
        )
    assert not factory.handles[0]._ready
    factory.assert_closed()


def test_initial_tags_retention_cannot_return_a_closed_owner():
    factory, consumed = Factory(), []

    def retain(prepared):
        factory.retain(prepared)
        prepared.close()

    def supplied():
        consumed.append(True)
        yield "camera/one"

    with pytest.raises(RuntimeError, match="closed"):
        owner.prepare_initial_collection_tags(
            supplied(), connection_factory=factory, on_storage_open=retain
        )
    assert not consumed and not factory.cursors
    factory.assert_closed()


@pytest.mark.parametrize("state", ["open", "finalized"])
def test_initial_tag_public_creation_helper_forwards_admission_and_exact_batches(state):
    factory, created, added = Factory(), [], []
    tags = values(205)
    with owner.prepare_initial_collection_tags(tags) as expected:
        identity = expected.tag_set_identity

    def create(first, selected_identity):
        assert factory.population.active == 1
        created.append((tuple(first), selected_identity))
        return {"collection_id": "17", "state": state}

    def add(collection, batch):
        assert factory.population.active == 1
        added.append((collection, tuple(batch)))

    result = owner.create_or_resume_with_initial_collection_tags(
        reversed(tags),
        create_or_resume=create,
        add_tags=add,
        connection_factory=factory,
        on_storage_open=factory.retain,
    )
    assert result == {"collection_id": "17", "state": state}
    assert created == [(tags[:100], identity)]
    assert added == ([(17, tags[100:200]), (17, tags[200:])] if state == "open" else [])
    factory.assert_closed()


@pytest.mark.parametrize("after", [None, False, True])
def test_initial_tags_close_during_cursor_creation_blocks_sql_and_retains_returned_cursor(after):
    factory = Factory()
    prepared = factory.prepare(values(2))
    db = factory.population.connections[0]
    make = db.cursor

    def cursor(*args, **options):
        result = make(*args, **options)
        with pytest.raises(RuntimeError, match="cursor acquisition is still active"):
            prepared.close()
        assert db.close_calls == 0 and factory.population.active == 1
        return result

    db.cursor = cursor
    with pytest.raises(RuntimeError, match="closed"):
        next(prepared.iter_batches())
    returned = factory.cursors[-1]
    assert returned.statement == "" and returned.close_calls == 0
    assert prepared._cursors == {returned}
    assert not prepared.cleanup_complete
    if after is not None:
        db.close_failures, db.close_after = 1, after
        with pytest.raises(OSError, match="close unconfirmed"):
            prepared.close()
        assert returned.close_calls == 1 and db.close_calls == 1
        assert factory.population.active == 1
    prepared.close()
    assert returned.close_calls == 1
    factory.assert_closed()
