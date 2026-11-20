from __future__ import annotations

import gc
import shutil
from pathlib import Path

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor, MemberHistoryStore
from riverhog_provenance import (
    MemberHistoryClosure,
    MemberHistoryMembership,
    create_journal,
    validate_journal,
)
from riverhog_provenance import history as owner

from tests.support.sqlite_admission import Capacity, Population


@pytest.fixture
def roots(tmp_path, monkeypatch):
    make, paths = owner.mkdtemp, []

    def created(**options):
        path = Path(make(dir=tmp_path, **options))
        paths.append(path)
        return str(path)

    monkeypatch.setattr(owner, "mkdtemp", created)
    return paths


def opened(population, handles, **options):
    def forbidden(*_args):
        pytest.fail("storage-only operation attempted an archive read")

    return MemberHistoryClosure(
        MemberHistoryStore(forbidden),
        forbidden,
        member_role="member",
        connection_factory=population,
        on_storage_open=handles.append if isinstance(handles, list) else handles,
        **options,
    )


def selected(number):
    return HistoryJournalAnchor(
        f"urn:uuid:00000000-0000-4000-8000-{number:012x}",
        f"urn:uuid:10000000-0000-4000-8000-{number:012x}",
        number,
        f"{number:064x}",
        f"{number:064x}",
        number + 1,
    )


def test_history_worklist_admits_before_schema_and_copies_exact_membership(roots):
    population, handles = Population(limit=2), []

    def retained(closure):
        handles.append(closure)
        assert population.active == 1
        assert (
            not population.connections[0]
            .execute("SELECT name FROM sqlite_master WHERE type='table'")
            .fetchall()
        )

    with opened(population, retained) as closure:
        # The application retains the parent; each returned copy stays with it.
        assert population.attempts == [(roots[0] / "closure.sqlite3", {})]
        assert population.connections[0].execute("PRAGMA cache_size").fetchone() == (-512,)
        for number in (2, 1):
            closure._snapshot(selected(number))
        closure._object("exact-structural-object")
        assert closure.membership_image(max_bytes=0) is None
        assert len(population.attempts) == 1
        image = closure.membership_image(max_bytes=1024 * 1024)
        assert population.peak == 2 and population.active == 1
        assert population.connections[1].close_calls == 1 and not roots[1].exists()
        with MemberHistoryMembership(image) as membership:
            assert list(membership.journal_anchors()) == [selected(1), selected(2)]
            assert membership.journal_anchor(selected(1).journal_id) == selected(1)
            assert membership.contains_structure_object("exact-structural-object")
            assert not membership.contains_structure_object("unselected-object")
    assert population.active == 0 and not roots[0].exists()
    assert handles == [closure]


@pytest.mark.parametrize("unreturned", [False, True])
@pytest.mark.parametrize("stage", ["worklist", "copy"])
def test_history_factory_refusal_preserves_independent_unreturned_custody(roots, stage, unreturned):
    population, handles = Population(limit=2), []
    closure = opened(population, handles) if stage == "copy" else None
    population.refuse, population.unreturned = not unreturned, unreturned
    error = OSError if unreturned else Capacity
    with pytest.raises(error):
        if closure is None:
            opened(population, handles)
        else:
            closure.membership_image(max_bytes=1024 * 1024)
    if closure is not None:
        closure.close()
    gc.collect()
    refused_root = roots[-1]
    assert refused_root.is_dir()
    assert population.active == int(unreturned)
    if unreturned:
        connection = population.connections[-1]
        assert connection.close_calls == 0
        assert list(refused_root.glob("*.sqlite3"))
        connection.close()
    else:
        assert not list(refused_root.iterdir())
    assert len(handles) == int(stage == "copy")
    shutil.rmtree(refused_root)
    assert population.active == 0


@pytest.mark.parametrize("stage", ["worklist", "copy"])
def test_history_default_connection_failure_removes_only_its_directory(roots, monkeypatch, stage):
    closure = opened(Population(), []) if stage == "copy" else None

    def refused(_path):
        raise OSError("default connect interrupted")

    monkeypatch.setattr(owner.sqlite3, "connect", refused)
    with pytest.raises(OSError, match="default connect interrupted"):
        if closure is None:
            MemberHistoryClosure(
                MemberHistoryStore(lambda _path: ()), lambda *_: (), member_role="member"
            )
        else:
            closure._connection_factory = None
            closure.membership_image(max_bytes=1024 * 1024)
    assert not roots[-1].exists()
    if closure is not None:
        assert roots[0].exists()
        closure.close()


@pytest.mark.parametrize("stage", ["schema", "retention"])
@pytest.mark.parametrize("after", [None, False, True])
def test_history_initialization_failure_retains_close_ready_owner(roots, stage, after):
    population, handles = Population(), []
    connect = population.__call__

    def factory(path):
        db = connect(path)
        if stage == "schema":
            db.set_authorizer(
                lambda action, *_args: 1 if action == owner.sqlite3.SQLITE_CREATE_TABLE else 0
            )
        if after is not None:
            db.close_failures, db.close_after = 1, after
        return db

    def retained(closure):
        handles.append(closure)
        if stage == "retention":
            raise InterruptedError("retention interrupted")

    expected = (
        OSError
        if after is not None
        else (InterruptedError if stage == "retention" else owner.sqlite3.DatabaseError)
    )
    with pytest.raises(expected):
        MemberHistoryClosure(
            MemberHistoryStore(lambda _path: ()),
            lambda *_: (),
            member_role="member",
            connection_factory=factory,
            on_storage_open=retained,
        )
    assert len(handles) == 1
    assert population.active == int(after is not None)
    assert roots[0].exists() == (after is not None)
    handles[0].close()
    assert population.active == 0 and not roots[0].exists()
    assert population.connections[0].close_calls == (1 if after is None else 2)


@pytest.mark.parametrize("stage", ["worklist", "copy"])
@pytest.mark.parametrize("after", [False, True])
def test_history_uncertain_close_freezes_execution_without_context_retry(roots, stage, after):
    population, handles = Population(limit=2), []
    connect = population.__call__

    def factory(path):
        db = connect(path)
        if stage == "copy" and Path(path).name == "membership.sqlite3":
            db.close_failures, db.close_after = 1, after
        return db

    with pytest.raises(OSError, match="close unconfirmed"):
        with opened(factory, handles) as closure:
            if stage == "worklist":
                population.connections[0].close_failures = 1
                population.connections[0].close_after = after
                closure.close()
            else:
                closure.membership_image(max_bytes=1024 * 1024)
    gc.collect()
    assert handles == [closure] and all(path.exists() for path in roots)
    assert population.connections[-1].close_calls == 1
    calls = len(population.attempts)
    for operation in (
        lambda: closure.contains_journal(selected(1).journal_id),
        lambda: closure.contains_structure_object("any"),
        lambda: closure.membership_image(max_bytes=1024 * 1024),
        lambda: closure.summary_at(selected(1)),
    ):
        with pytest.raises(RuntimeError, match="closed"):
            operation()
    assert len(population.attempts) == calls
    closure.close()
    assert population.active == 0 and not any(path.exists() for path in roots)
    assert population.connections[-1].close_calls == 2
    closure.close()
    assert population.connections[-1].close_calls == 2


@pytest.mark.parametrize("stage", ["worklist", "copy"])
@pytest.mark.parametrize("after", [False, True])
def test_history_directory_retry_does_not_repeat_confirmed_connection_close(
    roots, monkeypatch, stage, after
):
    population, handles = Population(limit=2), []
    closure = opened(population, handles)
    remove, failures = owner.shutil.rmtree, []

    def interrupted(path):
        if not failures and (stage == "worklist" or path != roots[0]):
            failures.append(path)
            if after:
                remove(path)
            raise OSError("cleanup unconfirmed")
        return remove(path)

    monkeypatch.setattr(owner.shutil, "rmtree", interrupted)
    with pytest.raises(OSError, match="cleanup unconfirmed"):
        with closure:
            if stage == "copy":
                closure.membership_image(max_bytes=1024 * 1024)
            else:
                closure.close()
    assert population.connections[-1].close_calls == 1
    closure.close()
    assert population.active == 0 and not any(path.exists() for path in roots)
    assert all(connection.close_calls == 1 for connection in population.connections)


@pytest.mark.parametrize("iterator", ["journal_anchors", "snapshots"])
@pytest.mark.parametrize("after", [False, True])
def test_history_suspended_rows_stop_before_another_step(roots, iterator, after):
    population, handles = Population(), []
    closure = opened(population, handles)
    for number in (1, 2):
        closure._snapshot(selected(number))
    rows = getattr(closure, iterator)()
    assert next(rows) == selected(1)
    db = population.connections[0]
    db.close_failures, db.close_after = 1, after
    with pytest.raises(OSError, match="close unconfirmed"):
        closure.close()
    with pytest.raises(RuntimeError, match="closed"):
        next(rows)
    assert db.close_calls == 1
    closure.close()
    assert population.active == 0 and not roots[0].exists()


@pytest.mark.parametrize("after", [False, True])
def test_history_copy_backup_failure_discards_returned_copy_before_retry(roots, monkeypatch, after):
    population, handles = Population(limit=2), []
    with opened(population, handles) as closure:
        db = population.connections[0]
        backup = type(db).backup

        def interrupted(connection, destination):
            if after:
                backup(connection, destination)
            raise InterruptedError("backup interrupted")

        with monkeypatch.context() as patch:
            patch.setattr(type(db), "backup", interrupted)
            with pytest.raises(InterruptedError, match="backup interrupted"):
                closure.membership_image(max_bytes=1024 * 1024)
        assert population.active == 1 and not roots[-1].exists()
        assert population.connections[-1].close_calls == 1
        assert closure.membership_image(max_bytes=1024 * 1024) is not None
    assert population.active == 0 and not any(path.exists() for path in roots)


@pytest.mark.parametrize("after", [False, True])
def test_history_cache_eviction_retains_failed_and_new_summary(
    roots, graph, who, catalog, monkeypatch, after
):
    sources, anchors = {}, []
    for _ in range(33):
        raw = create_journal(graph, recorded_by_agent_id=who, catalog=catalog)
        with validate_journal(raw, catalog=catalog) as summary:
            anchors.append(HistoryJournalAnchor.from_mapping(summary.anchor))
            sources[summary.journal_id] = raw
    closure = MemberHistoryClosure(
        MemberHistoryStore(lambda _path: ()),
        lambda identity, end: (sources[identity][:end],),
        member_role="member",
        catalog=catalog,
    )
    for anchor in anchors[:32]:
        with closure.summary_at(anchor):
            pass
    cached = next(iter(closure._summaries.values()))
    original, failures = type(cached).close, []

    def interrupted(summary):
        if summary is cached and not failures:
            failures.append(summary)
            if after:
                original(summary)
            raise OSError("cached summary close unconfirmed")
        original(summary)

    monkeypatch.setattr(type(cached), "close", interrupted)
    with pytest.raises(OSError, match="cached summary close unconfirmed"):
        with closure:
            closure.summary_at(anchors[-1])
    assert cached in closure._summaries.values() and closure._pending_summary is not None
    assert len(failures) == 1 and roots[0].exists()
    closure.close()
    assert not closure._summaries and closure._pending_summary is None
    assert not roots[0].exists()


@pytest.mark.parametrize("after", [False, True])
@pytest.mark.parametrize("kind", ["history", "snapshot"])
def test_history_pending_selection_retains_uncertain_cursor_close(roots, monkeypatch, kind, after):
    population, handles = Population(), []
    closure = opened(population, handles)
    closure._snapshot(selected(1))
    db = population.connections[0]
    original = db.execute
    cursors = []

    class Cursor:
        def __init__(self, actual):
            self.actual = actual
            self.closes = 0

        def fetchone(self):
            return self.actual.fetchone()

        def close(self):
            self.closes += 1
            if self.closes == 1:
                if after:
                    self.actual.close()
                raise OSError("cursor close unconfirmed")
            self.actual.close()

    def execute(statement, parameters=()):
        actual = original(statement, parameters)
        if statement.startswith("SELECT") and "WHERE done = 0" in statement:
            cursor = Cursor(actual)
            cursors.append(cursor)
            return cursor
        return actual

    monkeypatch.setattr(db, "execute", execute)
    with pytest.raises(OSError, match="cursor close unconfirmed"):
        with closure:
            getattr(closure, "_pending_" + kind)()
    assert len(cursors) == 1 and cursors[0].closes == 1
    assert population.active == 1 and roots[0].exists()
    with pytest.raises(RuntimeError, match="closed"):
        getattr(closure, "_pending_" + kind)()
    assert len(cursors) == 1
    closure.close()
    assert cursors[0].closes == 2 and population.active == 0 and not roots[0].exists()
