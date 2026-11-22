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
from test_corpus_order_growth import edges_for, insert
from test_corpus_order_steps import finish, ready
from test_sqlite_connection_admission import no_default_connection

from tests.support.sqlite_admission import Capacity, Population


@pytest.fixture
def directories(tmp_path, monkeypatch):
    actual, paths = corpus.mkdtemp, []

    def create(**options):
        path = Path(actual(dir=tmp_path, **options))
        paths.append(path)
        return str(path)

    monkeypatch.setattr(corpus, "mkdtemp", create)
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
def test_unreturned_corpus_connection_keeps_original_files_and_capacity(directories, after):
    population = Population()
    population.unreturned = True
    with pytest.raises(OSError, match="construction unconfirmed"):
        CanonicalCorpusValidator(connection_factory=population)
    path = population.attempts[-1][0].parent
    db = population.connections[-1]
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
def test_ordering_uses_only_the_original_admitted_corpus_connection(
    directories, monkeypatch, whole
):
    population = Population(limit=1)
    with CanonicalCorpusValidator(connection_factory=population) as current:
        for kind in ("derivation", "specialization"):
            insert(current, edges_for("chain", 17), kind)
        ready(current)
        profile = current._db.execute("PRAGMA synchronous").fetchone()
        with monkeypatch.context() as patch:
            patch.setattr(corpus.sqlite3, "connect", no_default_connection)
            for kind in ("derivation", "specialization"):
                if whole:
                    current._acyclic(kind)
                else:
                    assert finish(current, kind, rows=3).completed_vertices == 18
        assert current._db.execute("PRAGMA synchronous").fetchone() == profile
        assert len(population.attempts) == population.active == population.peak == 1
        assert len(directories) == 1
        assert current._db.execute("SELECT count(*) FROM edges").fetchone() == (34,)
    assert population.active == 0 and not directories[0].exists()


@pytest.mark.parametrize("after", [False, True])
def test_parent_receives_both_order_views_before_retrying_its_original_close(directories, after):
    population = Population(limit=1)
    current = CanonicalCorpusValidator(connection_factory=population)
    for kind in ("derivation", "specialization"):
        insert(current, edges_for("chain", 17), kind)
    ready(current)
    for kind in ("derivation", "specialization"):
        assert not current.advance_order(kind, row_budget=1).complete
    orders = tuple(current._orders.values())
    db = population.connections[0]
    db.close_failures, db.close_after = 1, after
    with pytest.raises(OSError, match="close unconfirmed"):
        current.close()
    assert not current._orders and population.active == 1 and directories[0].is_dir()
    for order in orders:
        with pytest.raises(RuntimeError, match="closed"):
            order.advance(row_budget=1, byte_budget=512)
    with pytest.raises(RuntimeError, match="closed"):
        current.advance_order("derivation")
    current.close()
    current.close()
    assert db.close_calls == 2 and population.active == 0 and population.peak == 1
    assert not directories[0].exists()


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
        assert population.active == 0
        current.close()
    assert population.active == 0 and all(db.close_calls == 1 for db in population.connections)
    assert len(attempts) == 2 and all(not directory.exists() for directory in directories)


def test_default_corpus_connect_failure_releases_only_its_private_directory(
    directories, monkeypatch
):
    def refused(*_args, **_options):
        raise sqlite3.OperationalError("cannot open")

    with monkeypatch.context() as patch:
        patch.setattr(corpus.sqlite3, "connect", refused)
        with pytest.raises(sqlite3.OperationalError, match="cannot open"):
            CanonicalCorpusValidator()
    assert len(directories) == 1 and not directories[0].exists()


def test_whole_corpus_helper_uses_one_admitted_connection_for_source_and_both_graph_kinds(
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
    assert len(handles) == 1 and len(population.connections) == 1
    assert population.peak == 1 and population.active == 0
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
        assert path.is_dir() and population.active == 0
        assert population.connections[-1].close_calls == 1
    current.close()
    assert population.active == 0 and all(db.close_calls == 1 for db in population.connections)
    assert all(not directory.exists() for directory in directories)
