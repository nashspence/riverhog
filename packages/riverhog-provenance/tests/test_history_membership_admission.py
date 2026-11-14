from __future__ import annotations

import gc
import shutil
from pathlib import Path

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor, MemberHistoryStore
from riverhog_provenance import MemberHistoryClosure, MemberHistoryMembership
from riverhog_provenance import history as owner

from tests.support.sqlite_admission import Capacity, Population


def anchor(number):
    return HistoryJournalAnchor(
        f"urn:uuid:00000000-0000-4000-8000-{number:012x}",
        f"urn:uuid:10000000-0000-4000-8000-{number:012x}",
        number,
        f"{number:064x}",
        f"{number:064x}",
        number + 1,
    )


@pytest.fixture(scope="module")
def image():
    # This projection exercises local cache lookup; closure tests own archive verification.
    with MemberHistoryClosure(
        MemberHistoryStore(lambda _path: ()), lambda *_args: (), member_role="member"
    ) as closure:
        for number in (2, 1):
            closure._snapshot(anchor(number))
        closure._object("exact-structure-object")
        result = closure.membership_image(max_bytes=1024 * 1024)
        assert result is not None
        return result


@pytest.fixture
def roots(tmp_path, monkeypatch):
    make, paths = owner.mkdtemp, []

    def created(**options):
        path = Path(make(dir=tmp_path, **options))
        paths.append(path)
        return str(path)

    monkeypatch.setattr(owner, "mkdtemp", created)
    return paths


def test_membership_admits_before_query_setup_and_preserves_exact_read_only_projection(
    image, roots, monkeypatch
):
    population, handles, statements = Population(limit=1), [], []

    def factory(path):
        assert path.read_bytes() == image
        db = population(path)
        db.set_trace_callback(statements.append)
        return db

    def retained(membership):
        handles.append(membership)
        assert population.active == 1 and not statements

    def denied(*_args, **_options):
        pytest.fail("membership bypassed selected factory")

    monkeypatch.setattr(owner.sqlite3, "connect", denied)
    with MemberHistoryMembership(
        image, connection_factory=factory, on_storage_open=retained
    ) as membership:
        assert population.attempts == [(roots[0] / "membership.sqlite3", {})]
        assert statements == ["PRAGMA query_only = ON", "PRAGMA cache_size = -512"]
        assert list(membership.journal_anchors()) == [anchor(1), anchor(2)]
        assert membership.journal_anchor(anchor(2).journal_id) == anchor(2)
        assert membership.journal_anchor(anchor(3).journal_id) is None
        assert membership.contains_structure_object("exact-structure-object")
        assert not membership.contains_structure_object("unselected-structure-object")
        db = population.connections[0]
        assert db.execute("PRAGMA query_only").fetchone() == (1,)
        assert db.execute("PRAGMA cache_size").fetchone() == (-512,)
        with pytest.raises(owner.sqlite3.OperationalError, match="readonly"):
            db.execute("DELETE FROM objects")
        assert (roots[0] / "membership.sqlite3").read_bytes() == image
    assert handles == [membership] and population.active == 0 and population.peak == 1
    assert not roots[0].exists() and db.close_calls == 1
    membership.close()
    assert db.close_calls == 1


@pytest.mark.parametrize("unreturned", [False, True])
def test_membership_refusal_retains_factory_owned_image_and_unreturned_connection(
    image, roots, unreturned
):
    population, handles = Population(), []
    population.refuse, population.unreturned = not unreturned, unreturned
    with pytest.raises(OSError if unreturned else Capacity):
        MemberHistoryMembership(
            image, connection_factory=population, on_storage_open=handles.append
        )
    gc.collect()
    assert not handles and population.active == int(unreturned)
    assert (roots[0] / "membership.sqlite3").read_bytes() == image
    if unreturned:
        assert population.connections[0].close_calls == 0
        population.connections[0].close()
    shutil.rmtree(roots[0])
    assert population.active == 0


def test_membership_default_connection_failure_cleans_its_image(image, roots, monkeypatch):
    def refused(_path):
        raise OSError("default connect interrupted")

    monkeypatch.setattr(owner.sqlite3, "connect", refused)
    with pytest.raises(OSError, match="default connect interrupted"):
        MemberHistoryMembership(image)
    assert not roots[0].exists()


@pytest.mark.parametrize("after", [False, True])
def test_membership_image_write_failure_releases_owned_files_before_admission(
    image, roots, monkeypatch, after
):
    population, handles = Population(), []
    write = Path.write_bytes

    def interrupted(path, data):
        if path.name == "membership.sqlite3":
            if after:
                write(path, data[:19])
            raise InterruptedError("image write interrupted")
        return write(path, data)

    monkeypatch.setattr(Path, "write_bytes", interrupted)
    with pytest.raises(InterruptedError, match="image write interrupted"):
        MemberHistoryMembership(
            image, connection_factory=population, on_storage_open=handles.append
        )
    assert not population.attempts and not handles and not roots[0].exists()


@pytest.mark.parametrize("stage", ["retention", "settings"])
@pytest.mark.parametrize("after", [None, False, True])
def test_membership_initialization_retains_close_ready_owner_through_cleanup_failure(
    image, roots, stage, after
):
    population, handles = Population(), []

    def retained(membership):
        handles.append(membership)
        db = population.connections[0]
        if after is not None:
            db.close_failures, db.close_after = 1, after
        if stage == "retention":
            raise InterruptedError("retention interrupted")
        execute = db.execute

        def interrupted(statement, parameters=()):
            if statement == "PRAGMA query_only = ON":
                raise InterruptedError("query setup interrupted")
            return execute(statement, parameters)

        db.execute = interrupted

    with pytest.raises(InterruptedError if after is None else OSError):
        MemberHistoryMembership(image, connection_factory=population, on_storage_open=retained)
    gc.collect()
    assert len(handles) == 1 and population.active == int(after is not None)
    assert roots[0].exists() == (after is not None)
    handles[0].close()
    assert population.active == 0 and not roots[0].exists()
    assert population.connections[0].close_calls == (1 if after is None else 2)


@pytest.mark.parametrize("after", [False, True])
def test_membership_retention_callback_close_failure_does_not_trigger_implicit_retry(
    image, roots, after
):
    population, handles = Population(), []

    def retained(membership):
        handles.append(membership)
        population.connections[0].close_failures = 1
        population.connections[0].close_after = after
        membership.close()

    with pytest.raises(OSError, match="close unconfirmed"):
        MemberHistoryMembership(image, connection_factory=population, on_storage_open=retained)
    assert population.connections[0].close_calls == 1 and roots[0].exists()
    handles[0].close()
    assert population.active == 0 and not roots[0].exists()


def test_membership_retention_callback_cannot_return_closed_owner(image, roots):
    population, handles = Population(), []

    def retained(membership):
        handles.append(membership)
        membership.close()

    with pytest.raises(RuntimeError, match="closed"):
        MemberHistoryMembership(image, connection_factory=population, on_storage_open=retained)
    assert len(handles) == 1 and population.active == 0 and not roots[0].exists()
    assert population.connections[0].close_calls == 1


@pytest.mark.parametrize("after", [False, True])
def test_membership_explicit_uncertain_close_is_not_retried_by_context_exit(image, roots, after):
    population, handles = Population(), []
    with pytest.raises(OSError, match="close unconfirmed"):
        with MemberHistoryMembership(
            image, connection_factory=population, on_storage_open=handles.append
        ) as membership:
            population.connections[0].close_failures = 1
            population.connections[0].close_after = after
            membership.close()
    gc.collect()
    assert handles == [membership] and roots[0].exists()
    assert population.active == population.connections[0].close_calls == 1
    membership.close()
    assert population.active == 0 and not roots[0].exists()


@pytest.mark.parametrize("after", [False, True])
def test_membership_directory_retry_never_releases_connection_twice(
    image, roots, monkeypatch, after
):
    population, handles, failures = Population(), [], []
    remove = owner.shutil.rmtree

    def interrupted(path):
        if not failures:
            failures.append(path)
            if after:
                remove(path)
            raise OSError("cleanup unconfirmed")
        return remove(path)

    with pytest.raises(OSError, match="cleanup unconfirmed"):
        with MemberHistoryMembership(
            image, connection_factory=population, on_storage_open=handles.append
        ) as membership:
            monkeypatch.setattr(owner.shutil, "rmtree", interrupted)
            membership.close()
    assert population.active == 0 and population.connections[0].close_calls == 1
    membership.close()
    assert not roots[0].exists() and population.connections[0].close_calls == 1


@pytest.mark.parametrize("after", [False, True])
def test_membership_closing_freezes_queries_and_suspended_rows_before_another_step(
    image, roots, after
):
    population, handles, statements = Population(), [], []
    membership = MemberHistoryMembership(
        image, connection_factory=population, on_storage_open=handles.append
    )
    db = population.connections[0]
    db.set_trace_callback(statements.append)
    rows = membership.journal_anchors()
    assert next(rows) == anchor(1)
    db.close_failures, db.close_after = 1, after
    with pytest.raises(OSError, match="close unconfirmed"):
        membership.close()
    before = list(statements)
    for operation in (
        lambda: membership.journal_anchor(anchor(1).journal_id),
        lambda: membership.contains_structure_object("exact-structure-object"),
        lambda: list(membership.journal_anchors()),
        lambda: next(rows),
    ):
        with pytest.raises(RuntimeError, match="closed"):
            operation()
    assert statements == before and db.close_calls == 1
    membership.close()
    assert population.active == 0 and not roots[0].exists()


@pytest.mark.parametrize("kind", ["all", "one", "contains"])
@pytest.mark.parametrize("after", [False, True])
def test_membership_retains_uncertain_query_cursor_until_explicit_cleanup(
    image, roots, monkeypatch, kind, after
):
    population, handles, cursors = Population(), [], []
    membership = MemberHistoryMembership(
        image, connection_factory=population, on_storage_open=handles.append
    )
    db = population.connections[0]
    execute = db.execute

    class Cursor:
        def __init__(self, actual):
            self.actual, self.closes = actual, 0

        def fetchone(self):
            return self.actual.fetchone()

        def close(self):
            self.closes += 1
            if self.closes == 1:
                if after:
                    self.actual.close()
                raise OSError("cursor close unconfirmed")
            self.actual.close()

    def observed(statement, parameters=()):
        cursor = Cursor(execute(statement, parameters))
        cursors.append(cursor)
        return cursor

    monkeypatch.setattr(db, "execute", observed)
    with pytest.raises(OSError, match="cursor close unconfirmed"):
        with membership:
            if kind == "all":
                rows = membership.journal_anchors()
                assert next(rows) == anchor(1)
                rows.close()
            elif kind == "one":
                membership.journal_anchor(anchor(1).journal_id)
            else:
                membership.contains_structure_object("exact-structure-object")
    assert len(cursors) == 1 and cursors[0].closes == 1
    assert roots[0].exists() and population.active == 1
    membership.close()
    assert cursors[0].closes == 2 and population.active == 0 and not roots[0].exists()
