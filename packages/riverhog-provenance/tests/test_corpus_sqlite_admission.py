from __future__ import annotations

import gc
import shutil
import sqlite3
from pathlib import Path

import pytest
from riverhog_provenance import (
    CanonicalCorpusValidator,
    corpus,
    validate_canonical_corpus,
    validate_journal,
)
from riverhog_provenance import _corpus_order as ordering
from test_corpus_order_growth import edges_for, insert
from test_corpus_order_steps import finish, ready
from test_sqlite_connection_admission import Capacity, no_default_connection

_CONNECT = sqlite3.connect


class Population:
    """Count real opens and release capacity only on confirmed connection close."""

    def __init__(self, limit=3):
        self.limit = limit
        self.active = self.peak = 0
        self.attempts, self.connections = [], []
        self.refuse = self.unreturned = False
        self.fail_schema = False

    def __call__(self, database, **options):
        self.attempts.append((Path(database), options))
        if self.refuse or self.active >= self.limit:
            raise Capacity("connection population exhausted")
        self.active += 1
        self.peak = max(self.peak, self.active)
        owner = self

        class Connection(sqlite3.Connection):
            released = False
            close_failures = 0
            close_after = False
            close_calls = 0

            def execute(self, statement, parameters=()):
                if owner.fail_schema and statement.startswith("CREATE TABLE"):
                    raise InterruptedError("schema initialization interrupted")
                return super().execute(statement, parameters)

            def close(self):
                self.close_calls += 1
                if self.close_failures:
                    self.close_failures -= 1
                    if self.close_after:
                        super().close()
                    raise OSError("connection close unconfirmed")
                super().close()
                if not self.released:
                    self.released = True
                    owner.active -= 1

        db = _CONNECT(database, factory=Connection, **options)
        self.connections.append(db)
        if self.unreturned:
            raise OSError("connection construction unconfirmed")
        return db


@pytest.fixture
def directories(tmp_path, monkeypatch):
    actual, paths = corpus.mkdtemp, []

    def create(**options):
        path = Path(actual(dir=tmp_path, **options))
        paths.append(path)
        return str(path)

    monkeypatch.setattr(corpus, "mkdtemp", create)
    monkeypatch.setattr(ordering, "mkdtemp", create)
    return paths


@pytest.mark.parametrize("helper", [False, True])
def test_corpus_refusal_precedes_schema_and_retains_factory_directory(directories, helper):
    population, handles = Population(), []
    population.refuse = True

    class UnreadSummaries:
        def __iter__(self):
            raise AssertionError("summaries consumed before connection admission")

    with pytest.raises(Capacity):
        if helper:
            validate_canonical_corpus(
                UnreadSummaries(), connection_factory=population, on_storage_open=handles.append
            )
        else:
            CanonicalCorpusValidator(connection_factory=population, on_storage_open=handles.append)
    assert not handles and not population.connections and population.active == 0
    path, options = population.attempts[0]
    assert options == {} and path == directories[0] / "corpus.sqlite3"
    assert directories[0].is_dir() and not list(directories[0].iterdir())
    shutil.rmtree(directories[0])


@pytest.mark.parametrize("after", [False, True])
@pytest.mark.parametrize("child", [False, True])
def test_unreturned_corpus_connection_keeps_independent_files_and_capacity(
    directories, child, after
):
    population = Population()
    current = None
    if child:
        current = CanonicalCorpusValidator(connection_factory=population)
        insert(current, edges_for("chain", 3), "derivation")
        ready(current)
    population.unreturned = True
    with pytest.raises(OSError, match="construction unconfirmed"):
        if child:
            current.advance_order("derivation")
        else:
            CanonicalCorpusValidator(connection_factory=population)
    path = population.attempts[-1][0].parent
    db = population.connections[-1]
    if current is not None:
        assert path != current._directory and current._directory not in path.parents
        assert current._orders == {}
        current.close()
        assert not current._directory.exists()
    del current
    gc.collect()
    assert population.active == 1 and path.is_dir()
    db.execute("CREATE TABLE owned(value TEXT)").close()
    db.execute("INSERT INTO owned VALUES ('original')").close()
    db.commit()
    assert db.execute("SELECT value FROM owned").fetchone() == ("original",)
    db.close_failures, db.close_after = 1, after
    with pytest.raises(OSError, match="close unconfirmed"):
        db.close()
    assert population.active == 1 and path.is_dir()
    db.close()
    assert population.active == 0 and db.close_calls == 2
    shutil.rmtree(path)


@pytest.mark.parametrize("after", [False, True])
@pytest.mark.parametrize("helper", [False, True])
def test_corpus_schema_failure_exposes_retryable_owner_before_first_statement(
    directories, after, helper
):
    population, handles = Population(), []

    def retain(current):
        handles.append(current)
        assert current._db.execute("SELECT count(*) FROM sqlite_master").fetchone() == (0,)
        current._db.close_failures, current._db.close_after = 1, after
        population.fail_schema = True

    with pytest.raises(OSError, match="close unconfirmed"):
        if helper:
            validate_canonical_corpus((), connection_factory=population, on_storage_open=retain)
        else:
            CanonicalCorpusValidator(connection_factory=population, on_storage_open=retain)
    assert len(handles) == population.active == 1 and directories[0].is_dir()
    with pytest.raises(RuntimeError, match="closed"):
        handles[0].advance_semantics()
    handles[0].close()
    assert population.active == 0 and not directories[0].exists()


@pytest.mark.parametrize("after", [False, True])
def test_corpus_source_close_keeps_capacity_and_directory_until_confirmed(directories, after):
    population = Population()
    current = CanonicalCorpusValidator(connection_factory=population)
    db = population.connections[0]
    db.close_failures, db.close_after = 1, after
    with pytest.raises(OSError, match="close unconfirmed"):
        current.close()
    gc.collect()
    assert population.active == 1 and directories[0].is_dir()
    with pytest.raises(RuntimeError, match="closed"):
        current.advance_semantics()
    current.close()
    current.close()
    assert population.active == 0 and db.close_calls == 2 and not directories[0].exists()


@pytest.mark.parametrize("whole", [False, True])
def test_order_refusal_precedes_source_walk_and_preserves_exact_corpus(
    directories, monkeypatch, whole
):
    population = Population(limit=1)
    with CanonicalCorpusValidator(connection_factory=population) as current:
        insert(current, edges_for("chain", 17), "derivation")
        ready(current)
        current._db.commit()
        before = (current._directory / "corpus.sqlite3").read_bytes()
        queries = []
        current._db.set_trace_callback(queries.append)
        with monkeypatch.context() as patch:
            patch.setattr(corpus.sqlite3, "connect", no_default_connection)
            with pytest.raises(Capacity):
                if whole:
                    current._acyclic("derivation")
                else:
                    current.advance_order("derivation")
        current._db.set_trace_callback(None)
        assert queries == [] and current._orders == {} and population.active == 1
        assert (current._directory / "corpus.sqlite3").read_bytes() == before
        path, options = population.attempts[-1]
        assert options == {"isolation_level": None} and path.name == "order.sqlite3"
        assert path.parent.is_dir() and path.parent not in current._directory.parents
        shutil.rmtree(path.parent)
        population.limit = 2
        current.validate()
        assert population.peak == 2 and population.active == 1
    assert population.active == 0 and all(not path.exists() for path in directories)


@pytest.mark.parametrize("after", [False, True])
@pytest.mark.parametrize("whole", [False, True])
def test_order_schema_failure_retains_child_before_initialization_and_parent_release(
    directories, monkeypatch, whole, after
):
    population = Population()
    current = CanonicalCorpusValidator(connection_factory=population)
    insert(current, edges_for("chain", 3), "derivation")
    ready(current)
    write = ordering._CorpusOrder._write
    retained = []

    def interrupted(order, statement, parameters=()):
        assert current._orders["derivation"] is order
        if statement.startswith("CREATE TABLE"):
            retained.append(order)
            order.db.close_failures, order.db.close_after = 2, after
            raise InterruptedError("ordering schema interrupted")
        return write(order, statement, parameters)

    with monkeypatch.context() as patch:
        patch.setattr(ordering._CorpusOrder, "_write", interrupted)
        with pytest.raises(OSError, match="close unconfirmed"):
            if whole:
                current._acyclic("derivation")
            else:
                current.advance_order("derivation")
    (order,) = retained
    assert population.active == 2 and current._orders["derivation"] is order
    gc.collect()
    assert order._directory.is_dir() and current._directory.is_dir()
    with pytest.raises(RuntimeError, match="closed"):
        order.advance(row_budget=1, byte_budget=4096)
    with pytest.raises(OSError, match="close unconfirmed"):
        current.close()
    assert population.active == 2 and current._orders["derivation"] is order
    assert population.connections[0].execute("SELECT count(*) FROM edges").fetchone() == (3,)
    current.close()
    assert not current._orders and population.active == 0
    assert all(not path.exists() for path in directories)
    assert order.db.close_calls == 3 and population.connections[0].close_calls == 1


@pytest.mark.parametrize("after", [False, True])
def test_parent_close_retains_two_order_kinds_and_never_reopens_closed_child(
    directories, monkeypatch, after
):
    population = Population()
    current = CanonicalCorpusValidator(connection_factory=population)
    for kind in ("derivation", "specialization"):
        insert(current, edges_for("chain", 17), kind)
    ready(current)
    current._db.commit()
    before = (current._directory / "corpus.sqlite3").read_bytes()
    with monkeypatch.context() as patch:
        patch.setattr(corpus.sqlite3, "connect", no_default_connection)
        for kind in ("derivation", "specialization"):
            assert not current.advance_order(kind, row_budget=1).complete
        assert population.active == population.peak == 3
        for kind in ("derivation", "specialization"):
            assert finish(current, kind, rows=3).completed_vertices == 18
    assert (current._directory / "corpus.sqlite3").read_bytes() == before
    first, last = (current._orders[kind] for kind in ("derivation", "specialization"))
    last.db.close_failures, last.db.close_after = 1, after
    with pytest.raises(OSError, match="close unconfirmed"):
        current.close()
    assert set(current._orders) == {"specialization"}
    assert population.active == 2 and not first._directory.exists()
    assert last._directory.is_dir() and current._directory.is_dir()
    with pytest.raises(RuntimeError, match="closed"):
        last.advance(row_budget=1, byte_budget=1)
    with pytest.raises(RuntimeError, match="closed"):
        current.advance_order("derivation")
    current.close()
    current.close()
    assert population.active == 0 and population.peak == 3
    assert [db.close_calls for db in population.connections] == [1, 1, 2]
    assert all(not path.exists() for path in directories)


@pytest.mark.parametrize("child", [False, True])
@pytest.mark.parametrize("after", [False, True])
def test_corpus_directory_cleanup_retry_does_not_release_connection_twice(
    directories, monkeypatch, child, after
):
    population = Population()
    current = CanonicalCorpusValidator(connection_factory=population)
    if child:
        ready(current)
        current.advance_order("derivation", row_budget=1)
    path = directories[-1]
    remove, attempts = shutil.rmtree, []

    def interrupted(directory, *args, **kwargs):
        if Path(directory) == path:
            attempts.append(directory)
            if len(attempts) == 1:
                if after:
                    remove(directory, *args, **kwargs)
                raise OSError("directory release unconfirmed")
        return remove(directory, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(shutil, "rmtree", interrupted)
        with pytest.raises(OSError, match="directory release unconfirmed"):
            current.close()
        assert path.exists() is not after
        assert population.connections[-1].close_calls == 1
        assert population.active == int(child)
        current.close()
    assert population.active == 0 and all(db.close_calls == 1 for db in population.connections)
    assert len(attempts) == 2 and all(not directory.exists() for directory in directories)


@pytest.mark.parametrize("child", [False, True])
def test_default_corpus_connect_failure_releases_only_its_private_directory(
    directories, monkeypatch, child
):
    current = None
    if child:
        current = CanonicalCorpusValidator()
        ready(current)

    def refused(*_args, **_options):
        raise sqlite3.OperationalError("cannot open")

    with monkeypatch.context() as patch:
        patch.setattr(corpus.sqlite3, "connect", refused)
        with pytest.raises(sqlite3.OperationalError, match="cannot open"):
            if child:
                current.advance_order("derivation")
            else:
                CanonicalCorpusValidator()
    assert not directories[-1].exists()
    if current is not None:
        assert current._directory.is_dir()
        current.close()
    assert all(not path.exists() for path in directories)


def test_whole_corpus_helper_uses_admission_for_source_and_each_complete_graph_kind(
    directories, monkeypatch, journal, catalog
):
    population, handles = Population(limit=2), []
    with validate_journal(journal, catalog=catalog) as summary:
        with monkeypatch.context() as patch:
            patch.setattr(corpus.sqlite3, "connect", no_default_connection)
            validate_canonical_corpus(
                (summary,), connection_factory=population, on_storage_open=handles.append
            )
        assert summary.journal_bytes == len(journal)
    assert len(handles) == 1 and len(population.connections) == 3
    assert population.peak == 2 and population.active == 0
    assert all(db.close_calls == 1 for db in population.connections)
    assert all(not path.exists() for path in directories)


@pytest.mark.parametrize("child", [False, True])
def test_missing_directory_entry_during_cleanup_does_not_confirm_root_removal(
    directories, monkeypatch, child
):
    population = Population()
    current = CanonicalCorpusValidator(connection_factory=population)
    if child:
        ready(current)
        current.advance_order("derivation", row_budget=1)
    path = directories[-1]
    remove = shutil.rmtree

    def missing(directory, *args, **kwargs):
        if Path(directory) == path:
            raise FileNotFoundError("nested entry disappeared before root release")
        return remove(directory, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(shutil, "rmtree", missing)
        with pytest.raises(FileNotFoundError, match="nested entry"):
            current.close()
        assert path.is_dir() and population.active == int(child)
        assert population.connections[-1].close_calls == 1
    current.close()
    assert population.active == 0 and all(db.close_calls == 1 for db in population.connections)
    assert all(not directory.exists() for directory in directories)
