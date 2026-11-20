from __future__ import annotations

import gc
import shutil
from pathlib import Path

import pytest
from riverhog_archive_contracts import (
    PAGE_RECORDS_MAX,
    HistoryJournalAnchor,
    MemberHistoryBuilder,
    MemberHistoryImport,
    MemberHistoryPrimary,
    MemberHistoryRoot,
    MemberHistoryStore,
    provenance_structure_identity,
)
from riverhog_archive_contracts import history_builder as owner

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


def imported():
    return MemberHistoryImport(
        source_identity="0" * 64,
        source_collection_id=17,
        source_archive_root_sha256="a" * 64,
        source_artifact_set_sha256="b" * 64,
        source_artifact_id="c" * 64,
        source_history_sha256="d" * 64,
        source_binding_proof_sha256="e" * 64,
        extent="bound-and-required-history",
        input_state={
            "scope": "external",
            "journal_id": "urn:uuid:11111111-1111-4111-8111-111111111111",
            "entry": {
                "entry_id": "urn:uuid:22222222-2222-4222-8222-222222222222",
                "sequence": "0",
                "json_sha256": "f" * 64,
            },
            "assertion_id": "urn:uuid:33333333-3333-4333-8333-333333333333",
            "object_id": "urn:uuid:44444444-4444-4444-8444-444444444444",
            "object_type": "state",
        },
    )


def builder(**options):
    return MemberHistoryBuilder(
        artifact_id="c" * 64,
        bytes=7,
        sha256="d" * 64,
        primary=MemberHistoryPrimary(anchor(1), "urn:uuid:33333333-3333-4333-8333-333333333333"),
        **options,
    )


@pytest.fixture
def roots(tmp_path, monkeypatch):
    make, paths = owner.mkdtemp, []

    def created(**options):
        path = Path(make(dir=tmp_path, **options))
        paths.append(path)
        return str(path)

    monkeypatch.setattr(owner, "mkdtemp", created)
    return paths


@pytest.mark.parametrize("count", [1, PAGE_RECORDS_MAX + 1, PAGE_RECORDS_MAX * 3 + 1])
def test_builder_admits_selection_before_schema_and_preserves_exact_paged_objects(
    roots, monkeypatch, count
):
    population, handles, statements = Population(limit=1), [], []

    def factory(path):
        db = population(path)
        db.set_trace_callback(statements.append)
        return db

    def retained(value):
        handles.append(value)
        assert population.active == 1 and not statements

    def denied(*_args, **_options):
        pytest.fail("selection bypassed selected factory")

    with monkeypatch.context() as patch:
        patch.setattr(owner.sqlite3, "connect", denied)
        selected = builder(connection_factory=factory, on_storage_open=retained)
    with selected:
        assert population.attempts == [(roots[0] / "selection.sqlite3", {})]
        assert statements[0] == "PRAGMA cache_size = -512"
        assert population.connections[0].execute("PRAGMA cache_size").fetchone() == (-512,)
        for number in reversed(range(2, count + 1)):
            selected.add_root(MemberHistoryRoot(anchor(number), "bound"))
        selected.add_import(imported())
        binding, descriptor = selected.seal()
        before = list(statements)
        assert selected.seal() == (binding, descriptor)
        assert statements == before
        with pytest.raises(ValueError, match="sealed"):
            selected.add_root(MemberHistoryRoot(anchor(count + 1), "bound"))
        objects = tuple(selected.objects())
        pages = tuple(selected.pages(descriptor.roots))
        assert pages[-1].terminal and not pages[-1].records
        assert sum(len(page.records) for page in pages) == count
        assert all(len(page.records) <= PAGE_RECORDS_MAX for page in pages)
        assert len(pages) == (count + PAGE_RECORDS_MAX - 1) // PAGE_RECORDS_MAX + 1
        # Four complete selection reads: two commitments, emitted roots and imports;
        # the explicit roots read adds one. Verification streams each domain once.
        selects = [sql for sql in statements if sql.startswith("SELECT key, value")]
        assert len(selects) == 7
        assert not selected._cursors
        stored = {provenance_structure_identity(raw).relative_path: raw for raw in objects}
        store = MemberHistoryStore(lambda path: (stored[path],))
        assert store.descriptor(binding) == descriptor
        assert set(store.roots(binding, extent="bound-and-required-history")) == {
            MemberHistoryRoot(anchor(number), "bound") for number in range(1, count + 1)
        }
        assert tuple(store.imports(binding)) == (imported(),)
    assert handles == [selected] and population.active == 0 and population.peak == 1
    assert not roots[0].exists() and population.connections[0].close_calls == 1
    selected.close()
    assert population.connections[0].close_calls == 1
    with builder() as reference:
        for number in range(2, count + 1):
            reference.add_root(MemberHistoryRoot(anchor(number), "bound"))
        reference.add_import(imported())
        assert reference.seal() == (binding, descriptor)
        assert tuple(reference.objects()) == objects


@pytest.mark.parametrize("unreturned", [False, True])
def test_builder_refusal_preserves_factory_owned_files_and_unreturned_connection(roots, unreturned):
    population, handles = Population(), []
    population.refuse, population.unreturned = not unreturned, unreturned
    with pytest.raises(OSError if unreturned else Capacity):
        builder(connection_factory=population, on_storage_open=handles.append)
    gc.collect()
    assert not handles and roots[0].exists() and population.active == int(unreturned)
    if unreturned:
        assert population.connections[0].close_calls == 0
        assert (roots[0] / "selection.sqlite3").exists()
        population.connections[0].close()
    shutil.rmtree(roots[0])
    assert population.active == 0


def test_builder_default_connect_failure_cleans_owned_directory(roots, monkeypatch):
    def refused(_path):
        raise OSError("default connect interrupted")

    monkeypatch.setattr(owner.sqlite3, "connect", refused)
    with pytest.raises(OSError, match="default connect interrupted"):
        builder()
    assert not roots[0].exists()


@pytest.mark.parametrize("stage", ["retention", "settings", "schema", "primary"])
@pytest.mark.parametrize("after", [None, False, True])
def test_builder_initialization_retains_actual_owner_until_cleanup_is_confirmed(
    roots, stage, after
):
    population, handles = Population(), []

    def retained(value):
        handles.append(value)
        db = population.connections[0]
        if after is not None:
            db.close_failures, db.close_after = 1, after
        if stage == "retention":
            raise InterruptedError("retention interrupted")
        execute = db.execute
        prefix = {
            "settings": "PRAGMA cache_size",
            "schema": "CREATE TABLE",
            "primary": "INSERT INTO",
        }[stage]

        def interrupted(statement, parameters=()):
            if statement.startswith(prefix):
                raise InterruptedError("initialization interrupted")
            return execute(statement, parameters)

        db.execute = interrupted

    with pytest.raises(InterruptedError if after is None else OSError):
        builder(connection_factory=population, on_storage_open=retained)
    gc.collect()
    assert len(handles) == 1 and population.active == int(after is not None)
    assert roots[0].exists() == (after is not None)
    handles[0].close()
    assert population.active == 0 and not roots[0].exists()
    assert population.connections[0].close_calls == (1 if after is None else 2)


@pytest.mark.parametrize("after", [False, True])
def test_builder_retention_callback_uncertain_close_is_not_retried(roots, after):
    population, handles = Population(), []

    def retained(value):
        handles.append(value)
        population.connections[0].close_failures = 1
        population.connections[0].close_after = after
        value.close()

    with pytest.raises(OSError, match="close unconfirmed"):
        builder(connection_factory=population, on_storage_open=retained)
    assert population.connections[0].close_calls == 1 and roots[0].exists()
    handles[0].close()
    assert population.active == 0 and not roots[0].exists()


def test_builder_retention_callback_cannot_return_closed_owner(roots):
    population, handles = Population(), []

    def retained(value):
        handles.append(value)
        value.close()

    with pytest.raises(RuntimeError, match="closed"):
        builder(connection_factory=population, on_storage_open=retained)
    assert len(handles) == 1 and population.active == 0 and not roots[0].exists()
    assert population.connections[0].close_calls == 1


@pytest.mark.parametrize("after", [False, True])
def test_builder_explicit_close_failure_is_not_retried_by_context_exit(roots, after):
    population, handles = Population(), []
    with pytest.raises(OSError, match="close unconfirmed"):
        with builder(connection_factory=population, on_storage_open=handles.append) as selected:
            population.connections[0].close_failures = 1
            population.connections[0].close_after = after
            selected.close()
    gc.collect()
    assert handles == [selected] and roots[0].exists()
    assert population.active == population.connections[0].close_calls == 1
    selected.close()
    assert population.active == 0 and not roots[0].exists()


@pytest.mark.parametrize("after", [False, True])
def test_builder_directory_retry_keeps_confirmed_connection_close(roots, monkeypatch, after):
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
        with builder(connection_factory=population, on_storage_open=handles.append) as selected:
            monkeypatch.setattr(owner.shutil, "rmtree", interrupted)
            selected.close()
    assert population.active == 0 and population.connections[0].close_calls == 1
    selected.close()
    assert not roots[0].exists() and population.connections[0].close_calls == 1


@pytest.mark.parametrize("after", [False, True])
@pytest.mark.parametrize("stream", ["full-page", "partial-page", "objects"])
def test_builder_closing_freezes_all_work_and_suspended_outputs(roots, after, stream):
    population, statements = Population(), []
    selected = builder(connection_factory=population)
    db = population.connections[0]
    count = PAGE_RECORDS_MAX + 1 if stream == "full-page" else 1
    for number in range(2, count + 1):
        selected.add_root(MemberHistoryRoot(anchor(number), "bound"))
    _binding, descriptor = selected.seal()
    suspended = selected.objects() if stream == "objects" else selected.pages(descriptor.roots)
    next(suspended)
    db.set_trace_callback(statements.append)
    db.close_failures, db.close_after = 1, after
    with pytest.raises(OSError, match="close unconfirmed"):
        selected.close()
    before = list(statements)
    for operation in (
        lambda: selected.add_root(MemberHistoryRoot(anchor(count + 1), "bound")),
        lambda: selected.add_import(imported()),
        selected.seal,
        lambda: list(selected.pages(descriptor.roots)),
        lambda: list(selected.objects()),
        lambda: next(suspended),
        selected.__enter__,
    ):
        with pytest.raises(RuntimeError, match="closed"):
            operation()
    assert statements == before and db.close_calls == 1
    selected.close()
    assert population.active == 0 and not roots[0].exists()


@pytest.mark.parametrize("stage", ["settings", "schema", "primary", "add", "ref", "pages"])
@pytest.mark.parametrize("after", [False, True])
def test_builder_retains_unconfirmed_cursor_before_connection_and_directory_cleanup(
    roots, stage, after
):
    population, handles, cursors = Population(), [], []

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

    def intercept(db, prefix):
        execute = db.execute

        def wrapped(statement, parameters=()):
            cursor = execute(statement, parameters)
            if statement.startswith(prefix):
                cursor = Cursor(cursor)
                cursors.append(cursor)
            return cursor

        db.execute = wrapped

    def retained(value):
        handles.append(value)
        if stage in ("settings", "schema", "primary"):
            intercept(
                population.connections[0],
                {"settings": "PRAGMA cache_size", "schema": "CREATE TABLE", "primary": "INSERT"}[
                    stage
                ],
            )

    with pytest.raises(OSError, match="cursor close unconfirmed"):
        with builder(connection_factory=population, on_storage_open=retained) as selected:
            if stage == "add":
                intercept(population.connections[0], "INSERT")
                selected.add_root(MemberHistoryRoot(anchor(2), "bound"))
            elif stage == "ref":
                intercept(population.connections[0], "SELECT")
                selected.seal()
            elif stage == "pages":
                _binding, descriptor = selected.seal()
                intercept(population.connections[0], "SELECT")
                list(selected.pages(descriptor.roots))
    gc.collect()
    assert len(handles) == len(cursors) == 1 and cursors[0].closes == 1
    assert population.active == 1 and population.connections[0].close_calls == 0
    assert roots[0].exists()
    with pytest.raises(RuntimeError, match="closed"):
        handles[0].seal()
    handles[0].close()
    assert cursors[0].closes == 2 and population.connections[0].close_calls == 1
    assert population.active == 0 and not roots[0].exists()


def test_builder_abandoned_page_stream_closes_cursor_without_closing_owner(roots):
    population = Population()
    with builder(connection_factory=population) as selected:
        for number in range(2, PAGE_RECORDS_MAX + 2):
            selected.add_root(MemberHistoryRoot(anchor(number), "bound"))
        binding, descriptor = selected.seal()
        stream = selected.pages(descriptor.roots)
        assert len(next(stream).records) == PAGE_RECORDS_MAX
        assert len(selected._cursors) == 1
        stream.close()
        assert not selected._cursors and population.active == 1
        assert selected.seal() == (binding, descriptor)
        assert len(tuple(selected.pages(descriptor.roots))) == 3
    assert population.active == 0 and not roots[0].exists()


def test_builder_duplicate_selection_rejection_keeps_owner_and_exact_selection(roots):
    population = Population()
    with builder(connection_factory=population) as selected:
        with pytest.raises(ValueError, match="duplicate member history selection"):
            selected.add_root(MemberHistoryRoot(anchor(1), "bound"))
        selected.add_root(MemberHistoryRoot(anchor(2), "bound"))
        _binding, descriptor = selected.seal()
        assert descriptor.roots.record_count == 2 and not selected._cursors
        assert population.active == 1
    assert population.active == 0 and not roots[0].exists()
