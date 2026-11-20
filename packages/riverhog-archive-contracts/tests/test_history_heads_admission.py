from __future__ import annotations

import gc
import hashlib
import shutil
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import (
    MEMBER_HISTORY_IMPORTS_SCHEMA,
    MEMBER_HISTORY_ROOTS_SCHEMA,
    PAGE_RECORDS_MAX,
    ArchiveProvenanceIdentity,
    CollectionArchiveManifest,
    CollectionArtifactSetIdentity,
    HistoryJournalAnchor,
    MemberHistoryBinding,
    MemberHistoryBuilder,
    MemberHistoryDocument,
    MemberHistoryHeads,
    MemberHistoryImport,
    MemberHistoryPrimary,
    MemberHistoryRoot,
    MemberHistoryStore,
    ProvenanceRootDocument,
    ProvenanceRootIdentity,
    RecordPage,
    RecordSetCommitment,
    SourceMemberHistoryBindingProof,
    binding_tree_commitment,
    provenance_structure_identity,
    source_binding_proof_object_path,
    verify_member_history_sets,
)
from riverhog_archive_contracts import member_history as owner

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


def inherited():
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


def selection(count=3, *, selected_roots=None):
    selected_roots = selected_roots or tuple(
        MemberHistoryRoot(anchor(n), "bound" if n % 2 else "retained") for n in range(1, count + 1)
    )

    def pages(schema, records):
        rows = tuple(
            {"key": row.key, "value": row.to_mapping()}
            for row in sorted(records, key=lambda row: row.key)
        )
        commitment = RecordSetCommitment(schema)
        for row in rows:
            commitment.update(row["key"], row["value"])
        authority = commitment.ref()
        chunks = tuple(
            rows[n : n + PAGE_RECORDS_MAX] for n in range(0, len(rows), PAGE_RECORDS_MAX)
        )
        return authority, tuple(
            RecordPage(authority, n, chunk, False) for n, chunk in enumerate(chunks)
        ) + (RecordPage(authority, len(chunks), (), True),)

    roots, root_pages = pages(MEMBER_HISTORY_ROOTS_SCHEMA, selected_roots)
    imports, import_pages = pages(MEMBER_HISTORY_IMPORTS_SCHEMA, (inherited(),))
    descriptor = MemberHistoryDocument(
        "c" * 64,
        7,
        "d" * 64,
        MemberHistoryPrimary(anchor(1), "urn:uuid:33333333-3333-4333-8333-333333333333"),
        roots,
        imports,
    )
    binding = MemberHistoryBinding(
        descriptor.artifact_id,
        descriptor.bytes,
        descriptor.sha256,
        descriptor.identity,
        len(descriptor.to_json_bytes()),
    )
    objects = {
        provenance_structure_identity(raw).relative_path: raw
        for raw in (
            descriptor.to_json_bytes(),
            *(page.to_json_bytes() for page in (*root_pages, *import_pages)),
        )
    }
    return SimpleNamespace(
        descriptor=descriptor,
        binding=binding,
        root_pages=root_pages,
        import_pages=import_pages,
        selected_roots=selected_roots,
        objects=objects,
    )


def verify(value, **options):
    return verify_member_history_sets(
        value.descriptor,
        root_pages=value.root_pages,
        import_pages=value.import_pages,
        **options,
    )


def builder(value, **options):
    selected = MemberHistoryBuilder(
        artifact_id=value.descriptor.artifact_id,
        bytes=value.descriptor.bytes,
        sha256=value.descriptor.sha256,
        primary=value.descriptor.primary,
        **options,
    )
    for root in value.selected_roots:
        if root.journal != value.descriptor.primary.journal:
            selected.add_root(root)
    selected.add_import(inherited())
    return selected


@pytest.fixture
def roots(tmp_path, monkeypatch):
    make, paths = owner.tempfile.mkdtemp, []

    def created(**options):
        if options.get("prefix") != "riverhog-history-check-":
            return make(**options)
        path = Path(make(dir=tmp_path, **options))
        paths.append(path)
        return str(path)

    monkeypatch.setattr(owner.tempfile, "mkdtemp", created)
    return paths


@pytest.mark.parametrize("count", [1, PAGE_RECORDS_MAX + 1, PAGE_RECORDS_MAX * 3 + 1])
def test_heads_admission_precedes_sql_and_streaming_and_cleanup_precedes_imports(
    roots, monkeypatch, count
):
    value = selection(count)
    population, handles, statements, reads = Population(limit=1), [], [], []

    def factory(path):
        db = population(path)
        db.set_trace_callback(statements.append)
        return db

    def retained(heads):
        handles.append(heads)
        assert isinstance(heads, MemberHistoryHeads)
        assert population.active == 1 and not statements and not reads
        assert not heads.cleanup_complete

    def root_pages():
        for page in value.root_pages:
            assert population.active == 1
            assert statements[:2] == [
                "PRAGMA cache_size = -512",
                "CREATE TABLE bound_heads (journal_id TEXT PRIMARY KEY) WITHOUT ROWID",
            ]
            reads.append(("roots", page.ordinal))
            yield page

    def import_pages():
        assert population.active == 0 and handles[0].cleanup_complete
        assert not roots[0].exists()
        for page in value.import_pages:
            reads.append(("imports", page.ordinal))
            yield page

    def denied(*_args, **_options):
        pytest.fail("heads verification bypassed selected factory")

    monkeypatch.setattr(owner.sqlite3, "connect", denied)
    verify_member_history_sets(
        value.descriptor,
        root_pages=root_pages(),
        import_pages=import_pages(),
        connection_factory=factory,
        on_storage_open=retained,
    )
    assert population.attempts == [(roots[0] / "heads.sqlite3", {})]
    inserts = [sql for sql in statements if sql.startswith("INSERT INTO bound_heads")]
    assert len(inserts) == (count + 1) // 2
    assert statements[-1] == "COMMIT"
    assert reads == [
        *(("roots", page.ordinal) for page in value.root_pages),
        *(("imports", page.ordinal) for page in value.import_pages),
    ]
    assert population.active == 0 and population.peak == 1
    assert population.connections[0].close_calls == 1
    handles[0].close()
    assert population.connections[0].close_calls == 1


@pytest.mark.parametrize("unreturned", [False, True])
def test_heads_refusal_retains_factory_owned_files_before_any_root_page(roots, unreturned):
    value = selection()
    population, handles, reads = Population(), [], []
    population.refuse, population.unreturned = not unreturned, unreturned

    def pages():
        reads.append("read")
        yield from value.root_pages

    with pytest.raises(OSError if unreturned else Capacity):
        verify_member_history_sets(
            value.descriptor,
            root_pages=pages(),
            import_pages=value.import_pages,
            connection_factory=population,
            on_storage_open=handles.append,
        )
    gc.collect()
    assert not handles and not reads and roots[0].exists()
    assert population.active == int(unreturned)
    if unreturned:
        assert population.connections[0].close_calls == 0
        assert (roots[0] / "heads.sqlite3").exists()
        population.connections[0].close()
    shutil.rmtree(roots[0])
    assert population.active == 0


def test_heads_default_connect_failure_removes_only_owned_directory(roots, monkeypatch):
    def refused(_path):
        raise InterruptedError("default connect interrupted")

    monkeypatch.setattr(owner.sqlite3, "connect", refused)
    with pytest.raises(InterruptedError, match="default connect interrupted"):
        verify(selection())
    assert not roots[0].exists()


@pytest.mark.parametrize("stage", ["retention", "settings", "schema"])
@pytest.mark.parametrize("after", [None, False, True])
def test_heads_initialization_retains_actual_close_ready_owner_on_failure(roots, stage, after):
    population, handles = Population(), []

    def retained(heads):
        handles.append(heads)
        db = population.connections[0]
        if after is not None:
            db.close_failures, db.close_after = 1, after
        if stage == "retention":
            raise InterruptedError("retention interrupted")
        execute = db.execute

        def interrupted(statement, parameters=()):
            if statement.startswith("PRAGMA" if stage == "settings" else "CREATE TABLE"):
                raise InterruptedError("initialization interrupted")
            return execute(statement, parameters)

        db.execute = interrupted

    with pytest.raises(InterruptedError if after is None else OSError):
        verify(selection(), connection_factory=population, on_storage_open=retained)
    gc.collect()
    assert len(handles) == 1 and population.active == int(after is not None)
    assert roots[0].exists() == (after is not None)
    assert handles[0].cleanup_complete == (after is None)
    handles[0].close()
    assert handles[0].cleanup_complete and population.active == 0 and not roots[0].exists()
    assert population.connections[0].close_calls == (1 if after is None else 2)


@pytest.mark.parametrize("after", [False, True])
def test_heads_retention_callback_close_failure_is_not_retried(roots, after):
    population, handles = Population(), []

    def retained(heads):
        handles.append(heads)
        population.connections[0].close_failures = 1
        population.connections[0].close_after = after
        heads.close()

    with pytest.raises(OSError, match="close unconfirmed"):
        verify(selection(), connection_factory=population, on_storage_open=retained)
    assert population.connections[0].close_calls == 1 and roots[0].exists()
    handles[0].close()
    assert population.active == 0 and not roots[0].exists()


def test_heads_retention_callback_cannot_return_closed_owner(roots):
    population, handles = Population(), []

    def retained(heads):
        handles.append(heads)
        heads.close()

    with pytest.raises(RuntimeError, match="closed"):
        verify(selection(), connection_factory=population, on_storage_open=retained)
    assert len(handles) == 1 and handles[0].cleanup_complete and population.active == 0
    assert population.connections[0].close_calls == 1 and not roots[0].exists()


@pytest.mark.parametrize("after", [False, True])
def test_heads_context_exit_does_not_retry_explicit_unknown_close(roots, after):
    population, handles = Population(), []
    with pytest.raises(OSError, match="close unconfirmed"):
        with MemberHistoryHeads(
            connection_factory=population, on_storage_open=handles.append
        ) as heads:
            population.connections[0].close_failures = 1
            population.connections[0].close_after = after
            heads.close()
    gc.collect()
    assert handles == [heads] and population.active == 1
    assert population.connections[0].close_calls == 1 and roots[0].exists()
    statements = []
    if not after:
        population.connections[0].set_trace_callback(statements.append)
    for operation in (lambda: heads._add_bound_head(anchor(3).journal_id), heads._commit):
        with pytest.raises(RuntimeError, match="closed"):
            operation()
    assert not statements
    heads.close()
    assert heads.cleanup_complete and population.active == 0 and not roots[0].exists()


@pytest.mark.parametrize("after", [False, True])
def test_heads_directory_uncertainty_blocks_imports_without_double_connection_release(
    roots, monkeypatch, after
):
    value = selection()
    population, handles, failures, reads = Population(), [], [], []
    remove = owner.shutil.rmtree

    def interrupted(path):
        if not failures:
            failures.append(path)
            if after:
                remove(path)
            raise OSError("directory cleanup unconfirmed")
        return remove(path)

    def imports():
        reads.append("imports")
        yield from value.import_pages

    monkeypatch.setattr(owner.shutil, "rmtree", interrupted)
    with pytest.raises(OSError, match="directory cleanup unconfirmed"):
        verify_member_history_sets(
            value.descriptor,
            root_pages=value.root_pages,
            import_pages=imports(),
            connection_factory=population,
            on_storage_open=handles.append,
        )
    assert not reads and not handles[0].cleanup_complete and population.active == 0
    assert population.connections[0].close_calls == 1
    handles[0].close()
    assert handles[0].cleanup_complete and not roots[0].exists()
    assert population.connections[0].close_calls == 1


@pytest.mark.parametrize("stage", ["settings", "schema", "root"])
@pytest.mark.parametrize("after", [False, True])
def test_heads_uncertain_cursor_prevents_later_sql_connection_close_and_import_reads(
    roots, stage, after
):
    population, handles, cursors, reads = Population(), [], [], []
    value = selection()

    class Cursor:
        def __init__(self, actual):
            self.actual, self.closes = actual, 0

        def close(self):
            self.closes += 1
            if self.closes == 1:
                if after:
                    self.actual.close()
                raise OSError("cursor close unconfirmed")
            self.actual.close()

    def retained(heads):
        handles.append(heads)
        db = population.connections[0]
        execute = db.execute
        prefix = {"settings": "PRAGMA", "schema": "CREATE TABLE", "root": "INSERT"}[stage]

        def wrapped(statement, parameters=()):
            cursor = execute(statement, parameters)
            if statement.startswith(prefix):
                cursor = Cursor(cursor)
                cursors.append(cursor)
            return cursor

        db.execute = wrapped

    def imports():
        reads.append("imports")
        yield from value.import_pages

    with pytest.raises(OSError, match="cursor close unconfirmed"):
        verify_member_history_sets(
            value.descriptor,
            root_pages=value.root_pages,
            import_pages=imports(),
            connection_factory=population,
            on_storage_open=retained,
        )
    gc.collect()
    assert not reads and len(handles) == len(cursors) == 1
    assert cursors[0].closes == population.active == 1
    assert population.connections[0].close_calls == 0 and roots[0].exists()
    handles[0].close()
    assert cursors[0].closes == 2 and population.connections[0].close_calls == 1
    assert handles[0].cleanup_complete and population.active == 0 and not roots[0].exists()


@pytest.mark.parametrize("terminate", [False, True])
def test_heads_closed_by_root_source_cannot_finish_or_read_imports(roots, terminate):
    population, handles, reads = Population(), [], []
    value = selection()

    def pages():
        handles[0].close()
        if not terminate:
            yield value.root_pages[0]

    def imports():
        reads.append("imports")
        yield from value.import_pages

    with pytest.raises(RuntimeError, match="closed"):
        verify_member_history_sets(
            value.descriptor,
            root_pages=pages(),
            import_pages=imports(),
            connection_factory=population,
            on_storage_open=handles.append,
        )
    assert not reads and population.active == 0 and handles[0].cleanup_complete
    assert population.connections[0].close_calls == 1 and not roots[0].exists()


@pytest.mark.parametrize(
    "fault",
    [
        "missing-primary",
        "retained-primary",
        "conflicting-heads",
        "root-key",
        "root-authority",
        "root-terminal",
        "import-key",
        "import-terminal",
    ],
)
def test_heads_admission_preserves_exact_history_rejection_and_clean_retry(roots, fault):
    value = selection()
    if fault == "missing-primary":
        value = selection(selected_roots=(MemberHistoryRoot(anchor(2), "bound"),))
    elif fault == "retained-primary":
        value = selection(selected_roots=(MemberHistoryRoot(anchor(1), "retained"),))
    elif fault == "conflicting-heads":
        value = selection(
            selected_roots=(
                MemberHistoryRoot(anchor(1), "bound"),
                MemberHistoryRoot(replace(anchor(1), prefix_sha256="f" * 64), "bound"),
            )
        )
    elif fault in ("root-key", "import-key"):
        field = "root_pages" if fault == "root-key" else "import_pages"
        pages = getattr(value, field)
        changed = ({**pages[0].records[0], "key": "f" * 64}, *pages[0].records[1:])
        setattr(value, field, (replace(pages[0], records=changed), *pages[1:]))
    elif fault == "root-authority":
        value.root_pages = (
            replace(value.root_pages[0], authority=value.descriptor.imports),
            *value.root_pages[1:],
        )
    elif fault == "root-terminal":
        value.root_pages = value.root_pages[:-1]
    elif fault == "import-terminal":
        value.import_pages = value.import_pages[:-1]
    population, handles = Population(limit=1), []
    with pytest.raises(ValueError):
        verify(value, connection_factory=population, on_storage_open=handles.append)
    assert handles[0].cleanup_complete and population.active == 0 and not roots[0].exists()
    verify(selection(), connection_factory=population, on_storage_open=handles.append)
    assert len(handles) == len(population.connections) == 2 and population.peak == 1
    assert population.active == 0 and not roots[1].exists()


@pytest.mark.parametrize("after", [False, True])
def test_heads_commit_failure_cannot_expose_imports_and_known_cleanup_allows_retry(roots, after):
    population, handles, reads = Population(limit=1), [], []
    value = selection()

    def retained(heads):
        handles.append(heads)
        if len(handles) != 1:
            return
        db = population.connections[0]
        commit = db.commit

        def interrupted():
            if after:
                commit()
            raise InterruptedError("commit unconfirmed")

        db.commit = interrupted

    def imports():
        reads.append("imports")
        yield from value.import_pages

    with pytest.raises(InterruptedError, match="commit unconfirmed"):
        verify_member_history_sets(
            value.descriptor,
            root_pages=value.root_pages,
            import_pages=imports(),
            connection_factory=population,
            on_storage_open=retained,
        )
    assert not reads and population.active == 0 and handles[0].cleanup_complete
    verify(value, connection_factory=population, on_storage_open=retained)
    assert len(handles) == 2 and population.peak == 1 and population.active == 0


def test_store_forwards_verification_ports_for_each_exact_descriptor_read(roots):
    value = selection()
    population, handles = Population(limit=1), []
    store = MemberHistoryStore(
        lambda path: (value.objects[path],),
        connection_factory=population,
        on_storage_open=handles.append,
    )
    assert store.descriptor(value.binding) == value.descriptor
    assert set(store.roots(value.binding, extent="complete-retained-history")) == set(
        value.selected_roots
    )
    assert tuple(store.imports(value.binding)) == (inherited(),)
    assert len(handles) == len(population.connections) == len(roots) == 3
    assert all(heads.cleanup_complete for heads in handles)
    assert all(not path.exists() for path in roots)
    assert population.peak == 1 and population.active == 0


@pytest.mark.parametrize("after", [False, True])
def test_store_retainer_keeps_unknown_close_charged_across_independent_reads(roots, after):
    value = selection()
    population, handles = Population(limit=1), []

    def retained(heads):
        handles.append(heads)
        if len(handles) == 1:
            population.connections[0].close_failures = 1
            population.connections[0].close_after = after

    store = MemberHistoryStore(
        lambda path: (value.objects[path],),
        connection_factory=population,
        on_storage_open=retained,
    )
    with pytest.raises(OSError, match="close unconfirmed"):
        store.descriptor(value.binding)
    gc.collect()
    assert population.active == 1 and roots[0].exists() and not handles[0].cleanup_complete
    with pytest.raises(Capacity):
        store.descriptor(value.binding)
    assert population.connections[0].close_calls == 1
    # This refused factory owns its independent root, even without a returned handle.
    shutil.rmtree(roots[1])
    handles[0].close()
    assert store.descriptor(value.binding) == value.descriptor
    assert len(handles) == 2 and population.peak == 1 and population.active == 0


def test_builder_separately_admits_and_retains_selection_and_verification(roots, monkeypatch):
    value = selection()
    population, parents, handles = Population(limit=2), [], []

    def retained(heads):
        handles.append(heads)
        assert population.active == 2 and parents[0]._verification is heads

    def denied(*_args, **_options):
        pytest.fail("builder bypassed selected connection role")

    monkeypatch.setattr(owner.sqlite3, "connect", denied)
    with builder(
        value,
        connection_factory=population,
        on_storage_open=parents.append,
        verification_connection_factory=population,
        on_verification_storage_open=retained,
    ) as selected:
        assert selected.seal() == (value.binding, value.descriptor)
        assert selected._verification is None and handles[0].cleanup_complete
        assert population.active == 1 and population.peak == 2
        assert [p.name for p, _options in population.attempts] == [
            "selection.sqlite3",
            "heads.sqlite3",
        ]
        assert selected.seal() == (value.binding, value.descriptor)
        assert len(population.connections) == 2
    assert population.active == 0 and all(db.close_calls == 1 for db in population.connections)


@pytest.mark.parametrize("stage", ["completion", "retention"])
@pytest.mark.parametrize("after", [False, True])
def test_builder_retains_uncertain_heads_before_handoff_and_freezes_all_work(roots, stage, after):
    value = selection()
    population, parents, handles, statements = Population(limit=2), [], [], []

    def retained(heads):
        handles.append(heads)
        db = population.connections[1]
        db.close_failures, db.close_after = 1, after
        if stage == "retention":
            raise InterruptedError("retention interrupted")

    with pytest.raises(OSError, match="close unconfirmed"):
        with builder(
            value,
            connection_factory=population,
            on_storage_open=parents.append,
            verification_connection_factory=population,
            on_verification_storage_open=retained,
        ) as selected:
            suspended = selected.pages(value.descriptor.roots)
            next(suspended)
            population.connections[0].set_trace_callback(statements.append)
            selected.seal()
    gc.collect()
    assert selected._verification is handles[0] and not handles[0].cleanup_complete
    assert population.active == 2 and population.connections[0].close_calls == 0
    assert population.connections[1].close_calls == 1
    assert selected._root.exists() and roots[0].exists()
    before = list(statements)
    for operation in (
        selected.seal,
        lambda: selected.add_root(MemberHistoryRoot(anchor(4), "bound")),
        lambda: list(selected.pages(value.descriptor.roots)),
        lambda: list(selected.objects()),
        lambda: next(suspended),
    ):
        with pytest.raises(RuntimeError, match="closed"):
            operation()
    assert statements == before
    selected.close()
    assert selected._verification is None and handles[0].cleanup_complete
    assert population.active == 0 and not selected._root.exists() and not roots[0].exists()
    assert [db.close_calls for db in population.connections] == [1, 2]


@pytest.mark.parametrize("after", [False, True])
def test_builder_retains_heads_directory_phase_without_reclosing_its_connection(
    roots, monkeypatch, after
):
    value = selection()
    population, parents, handles, failures = Population(limit=2), [], [], []
    remove = owner.shutil.rmtree

    def interrupted(path):
        if path == roots[0] and not failures:
            failures.append(path)
            if after:
                remove(path)
            raise OSError("directory cleanup unconfirmed")
        return remove(path)

    with pytest.raises(OSError, match="directory cleanup unconfirmed"):
        with builder(
            value,
            connection_factory=population,
            on_storage_open=parents.append,
            verification_connection_factory=population,
            on_verification_storage_open=handles.append,
        ) as selected:
            monkeypatch.setattr(owner.shutil, "rmtree", interrupted)
            selected.seal()
    assert selected._verification is handles[0] and not handles[0].cleanup_complete
    assert population.active == 1 and [db.close_calls for db in population.connections] == [0, 1]
    selected.close()
    assert population.active == 0 and [db.close_calls for db in population.connections] == [1, 1]
    assert not selected._root.exists() and not roots[0].exists()


def test_builder_retries_failed_verification_after_confirmed_child_cleanup(roots):
    value = selection()
    population, handles = Population(limit=2), []

    def retained(heads):
        handles.append(heads)
        if len(handles) == 1:
            raise InterruptedError("retention interrupted")

    with builder(
        value,
        connection_factory=population,
        verification_connection_factory=population,
        on_verification_storage_open=retained,
    ) as selected:
        with pytest.raises(InterruptedError, match="retention interrupted"):
            selected.seal()
        assert handles[0].cleanup_complete and selected._verification is None
        assert population.active == 1 and selected._sealed is None
        assert selected.seal() == (value.binding, value.descriptor)
        assert len(handles) == 2 and population.peak == 2
    assert population.active == 0


def test_builder_can_be_closed_from_child_retention_before_any_child_sql(roots):
    value = selection()
    population, parents, handles, statements = Population(limit=2), [], [], []

    def retained(heads):
        handles.append(heads)
        population.connections[1].set_trace_callback(statements.append)
        parents[0].close()

    with builder(
        value,
        connection_factory=population,
        on_storage_open=parents.append,
        verification_connection_factory=population,
        on_verification_storage_open=retained,
    ) as selected:
        with pytest.raises(RuntimeError, match="closed"):
            selected.seal()
    assert not statements and handles[0].cleanup_complete and population.active == 0
    assert not selected._root.exists() and not roots[0].exists()
    assert [db.close_calls for db in population.connections] == [1, 1]


def test_root_source_interruption_releases_heads_without_reading_imports(roots):
    value = selection()
    population, handles, reads = Population(limit=1), [], []

    def interrupted():
        yield value.root_pages[0]
        raise InterruptedError("root stream interrupted")

    def imports():
        reads.append("imports")
        yield from value.import_pages

    with pytest.raises(InterruptedError, match="root stream interrupted"):
        verify_member_history_sets(
            value.descriptor,
            root_pages=interrupted(),
            import_pages=imports(),
            connection_factory=population,
            on_storage_open=handles.append,
        )
    assert not reads and population.active == 0 and handles[0].cleanup_complete
    verify(value, connection_factory=population, on_storage_open=handles.append)
    assert population.active == 0 and population.peak == 1 and len(handles) == 2


def test_store_source_proof_preserves_exact_binding_before_admitted_history_verification(roots):
    value = selection()
    tree = binding_tree_commitment((value.binding,), target_artifact_id=value.binding.artifact_id)
    provenance = ProvenanceRootDocument(
        archive_generation="c" * 64,
        artifact_set_sha256="d" * 64,
        delivery_context_id="urn:uuid:11111111-1111-4111-8111-111111111111",
        binding_count=1,
        binding_tree_sha256=tree.root_sha256,
        journal_count=3,
        ordered_volume_sha256="e" * 64,
    )
    archive = CollectionArchiveManifest(
        archive_generation=provenance.archive_generation,
        artifact_set=CollectionArtifactSetIdentity(
            1, value.binding.bytes, provenance.artifact_set_sha256
        ),
        ordered_volume_sha256="f" * 64,
        provenance=ArchiveProvenanceIdentity(
            provenance.identity,
            ProvenanceRootIdentity(
                id="provenance-root",
                kind="provenance-root",
                path="provenance/root.json.age",
                plaintext_bytes=len(provenance.to_json_bytes()),
                sha256=provenance.identity,
                stored_bytes=len(provenance.to_json_bytes()) + 128,
                stored_sha256="0" * 64,
            ),
        ),
    )
    assert tree.target_index is not None
    proof = SourceMemberHistoryBindingProof(
        source_identity="a" * 64,
        collection_id=7,
        archive_root=archive.to_json_bytes(),
        provenance_root=provenance.to_json_bytes(),
        binding=value.binding,
        index=tree.target_index,
        siblings=tree.target_siblings,
    )
    selected = replace(
        inherited(),
        source_identity=proof.source_identity,
        source_collection_id=proof.collection_id,
        source_archive_root_sha256=hashlib.sha256(proof.archive_root).hexdigest(),
        source_artifact_set_sha256=provenance.artifact_set_sha256,
        source_artifact_id=value.binding.artifact_id,
        source_history_sha256=value.binding.history_sha256,
        source_binding_proof_sha256=proof.identity,
    )
    value.objects[source_binding_proof_object_path(proof.identity)] = proof.to_json_bytes()
    population, handles = Population(limit=1), []
    store = MemberHistoryStore(
        lambda path: (value.objects[path],),
        connection_factory=population,
        on_storage_open=handles.append,
    )
    assert store.source_proof(selected) == proof
    assert len(handles) == 1 and handles[0].cleanup_complete and population.active == 0
    with pytest.raises(ValueError):
        store.source_proof(replace(selected, source_archive_root_sha256="f" * 64))
    assert len(population.connections) == 1 and not roots[0].exists()
