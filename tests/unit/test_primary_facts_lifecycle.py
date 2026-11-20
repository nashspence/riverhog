"""Actual process death and exact receiving for disposable primary indexes."""

from __future__ import annotations

import json
import os
import selectors
import signal
import subprocess
import sys
from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from riverhog_core.archive_execution_tuning import ArchiveExecutionTuning
from riverhog_core.catalog_db import create_catalog_engine, initialize_db
from riverhog_core.journal_validation_workspace import LostJournalValidationWorkspace
from riverhog_core.primary_facts_reclamation import PrimaryFactsRetirementTurn
from riverhog_core.primary_facts_workspace import PrimaryFactsWorkspace, capture_root_identity
from riverhog_core.services import primary_facts_jobs as jobs
from riverhog_core.services.archive_construction_retention import ArchiveConstructionRetention
from riverhog_core.services.physical_execution import PhysicalExecutionOwner
from riverhog_core.services.upload_primary_facts_retirement import UploadPrimaryFactsRetirement
from riverhog_protocol.errors import Conflict
from sqlalchemy.orm import sessionmaker

from tests.support.retained_resource import fault_native as fault_native
from tests.unit.db_helpers import sqlite_url
from tests.unit.test_archive_upload_execution import _joined

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX original workspace custody")

OWNER, RESTART = "a" * 32, "b" * 32
DATABASES = ("corpus.sqlite3", "checks.sqlite3")
SIDECARS = ("-journal", "-wal", "-shm", "")


class Cleanup:
    """The real session-bound catalog fences used by a direct physical turn."""

    def __init__(self, factory, root):
        self.factory, self.root = factory, root

    def current(self, job):
        with self.factory() as session:
            return jobs.current(session, job)

    def transition(self, job, state):
        with self.factory() as session, session.begin():
            successor = jobs.transition(session, job, state)
            if successor is None:
                raise Conflict("the original primary lifecycle changed")
            return successor

    def finish(self, job):
        with self.factory() as session, session.begin():
            return jobs.finish(session, job)


@contextmanager
def lifecycle_case(path):
    path.mkdir(parents=True, exist_ok=True)
    url = sqlite_url(path / "catalog.sqlite3")
    initialize_db(url)
    engine = create_catalog_engine(url)
    factory = sessionmaker(engine, expire_on_commit=False)
    root = path / "primary"
    root.mkdir(mode=0o700)
    case = SimpleNamespace(
        engine=engine,
        factory=factory,
        root=root,
        root_identity=capture_root_identity(root),
        originals=[],
        cleanup=Cleanup(factory, root),
    )
    try:
        yield case
    finally:
        for original in reversed(case.originals):
            original.close()
        engine.dispose()


def registered(case, *, collection_id=1, owner_id=OWNER):
    with case.factory() as session, session.begin():
        return jobs.register(
            session,
            collection_id=collection_id,
            construction='{"fixture":"primary-lifecycle"}',
            epoch="e" * 64,
            owner_id=owner_id,
            root_identity=case.root_identity,
        )


def current(case, workspace_id):
    with case.factory() as session:
        return jobs.get(session, workspace_id)


def pinned(case, job=None):
    job = registered(case) if job is None else job
    original = PrimaryFactsWorkspace(case.root, job)
    case.originals.append(original)
    assert original.open(creation_current=lambda: case.cleanup.current(job))
    with case.factory() as session, session.begin():
        successor = jobs.pin(session, job, original.namespace_identity)
    assert successor is not None
    original.require_current(successor)
    return successor, original


def retired(case, job):
    with case.factory() as session, session.begin():
        successor = jobs.retire(session, job)
    assert successor is not None
    return successor


def populate(path, *, extent=17, sidecars=True):
    names = tuple(
        database + suffix for database in DATABASES for suffix in (SIDECARS if sidecars else ("",))
    )
    for name in names:
        target = path / name
        with target.open("xb") as stream:
            stream.write(b"p" * extent)
        target.chmod(0o600)
    return names


def advance(case, job, *, budget=65536):
    original = PrimaryFactsRetirementTurn(case.cleanup, job)
    case.originals.append(original)
    result = original.advance(octet_budget=budget)
    assert original.received
    return result


def finish(case, workspace_id, *, budget=65536):
    observations = []
    for _ in range(128):
        job = current(case, workspace_id)
        if job is None:
            assert not (case.root / workspace_id).exists()
            return observations
        result = advance(case, job, budget=budget)
        assert 0 <= result.removed_octets <= budget
        observations.append(result)
    pytest.fail("small primary preparation namespace did not finish bounded receiving")


@contextmanager
def scheduled(case, *, slots=2, owner_id=RESTART):
    tuning = ArchiveExecutionTuning(retained_constructions=slots, terminal_workers=2)
    execution = PhysicalExecutionOwner(tuning)
    retention = ArchiveConstructionRetention(tuning)
    cleanup = UploadPrimaryFactsRetirement(
        case.factory,
        case.root,
        owner_id=owner_id,
        execution=execution,
        retention=retention,
        metadata_budget=1024 * 1024,
        receive_limit=16,
        retry_after=timedelta(microseconds=1),
    )
    try:
        yield cleanup, execution, retention
    finally:
        cleanup.close()
        execution.close()
        assert cleanup.receive_stopped()


def schedule_step(cleanup, execution, *, budget=65536):
    result = cleanup.advance(limit=1, octet_budget=budget)
    _joined(execution)
    cleanup._registry.collect(limit=16)
    return result


def finish_scheduled(case, cleanup, execution, identities, *, budget=65536):
    for _ in range(128):
        if all(current(case, identity) is None for identity in identities):
            assert all(not (case.root / identity).exists() for identity in identities)
            return
        schedule_step(cleanup, execution, budget=budget)
    pytest.fail("bounded restart discovery did not receive its exact registered namespaces")


def _hold(point):
    print(json.dumps({"boundary": point}), flush=True)
    sys.stdin.readline()
    raise AssertionError("crash boundary resumed without the expected process kill")


def child(case, job, *, action, point, budget=65536):
    program = (
        "import sys; from tests.unit.test_primary_facts_lifecycle import child_main; "
        "child_main(*sys.argv[1:])"
    )
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            program,
            str(case.engine.url),
            str(case.root),
            "" if job is None else job.workspace_id,
            action,
            point,
            str(budget),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout is not None
        with selectors.DefaultSelector() as ready:
            ready.register(process.stdout, selectors.EVENT_READ)
            assert ready.select(timeout=30), "child failed to reach its exact crash boundary"
        line = process.stdout.readline()
        if not line:
            stdout, stderr = process.communicate(timeout=5)
            pytest.fail(f"child exited before {point}: {stdout}{stderr}")
        assert json.loads(line) == {"boundary": point}
        process.kill()
        stdout, stderr = process.communicate(timeout=10)
        assert process.returncode == -signal.SIGKILL, (stdout, stderr)
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=10)


def child_main(url, root, identity, action, point, budget):
    root = Path(root)
    engine = create_catalog_engine(url)
    factory = sessionmaker(engine, expire_on_commit=False)
    cleanup = Cleanup(factory, root)
    if action == "uncommitted":
        with factory() as session, session.begin():
            jobs.register(
                session,
                collection_id=1,
                construction='{"fixture":"primary-lifecycle"}',
                epoch="e" * 64,
                owner_id=OWNER,
                root_identity=capture_root_identity(root),
            )
            _hold(point)
    with factory() as session:
        job = jobs.get(session, identity)
    assert job is not None
    if action == "namespace":
        from riverhog_core.retained_workspace_handles import RetainedWorkspaceResource

        if point == "registered":
            _hold(point)
        mkdir = os.mkdir
        acquire = RetainedWorkspaceResource.acquire

        def directory(name, *args, **kwargs):
            result = mkdir(name, *args, **kwargs)
            if point == "directory" and Path(name).name == identity:
                _hold(point)
            return result

        def resource(original, name, **kwargs):
            result = acquire(original, name, **kwargs)
            if point == "lease" and name == ".lease":
                _hold(point)
            return result

        os.mkdir = directory
        RetainedWorkspaceResource.acquire = resource
        original = PrimaryFactsWorkspace(root, job)
        original.open(creation_current=lambda: cleanup.current(job))
    elif action == "cleanup":
        name = "ftruncate" if point == "truncate" else "rmdir" if point == "rmdir" else "unlink"
        operation = getattr(os, name)

        def interrupted(*args, **kwargs):
            result = operation(*args, **kwargs)
            selected = (
                point in {"truncate", "rmdir"}
                or (point == "lease-unlink" and Path(os.fsdecode(args[0])).name == ".lease")
                or (point == "file-unlink" and Path(os.fsdecode(args[0])).name != ".lease")
            )
            if selected:
                _hold(point)
            return result

        setattr(os, name, interrupted)
        original = PrimaryFactsRetirementTurn(cleanup, job)
        original.advance(octet_budget=int(budget))
    elif action == "index":
        from riverhog_core.journal_storage_grants import JournalStorageGrants
        from riverhog_core.services.journal_storage_resources import JournalStorageResources
        from riverhog_core.services.upload_primary_facts import _PrimaryIndex
        from riverhog_provenance import CorpusConstruction, JournalConstruction

        workspace = PrimaryFactsWorkspace(root, job)
        assert workspace.open(creation_current=lambda: cleanup.current(job))
        resources = JournalStorageResources(JournalStorageGrants())
        index = _PrimaryIndex(resources, job, workspace)
        guard = CorpusConstruction.acquire_guard
        creator = JournalConstruction.create_exclusive

        def committed():
            # This is a different catalog connection from both metadata turns.
            with factory() as session:
                selected = jobs.get(session, identity)
            assert selected == job and selected.state == "pinned"
            assert selected.namespace_identity == workspace.namespace_identity

        def corpus(self, path, **kwargs):
            committed()
            result = guard(self, path, **kwargs)
            if point == "corpus-created":
                _hold(point)
            return result

        def memo(self, path):
            committed()
            result = creator(self, path)
            if point == "memo-created":
                _hold(point)
            return result

        CorpusConstruction.acquire_guard = corpus
        JournalConstruction.create_exclusive = memo
        index.open()
        assert index.ready
        if point == "both-ready":
            _hold(point)
    else:
        raise AssertionError(f"unknown child operation {action}")
    raise AssertionError(f"child did not reach {point}")


def test_killed_uncommitted_registration_has_no_namespace_effect(tmp_path):
    with lifecycle_case(tmp_path) as case:
        child(case, None, action="uncommitted", point="registered-uncommitted")
        with case.factory() as session:
            assert jobs.find_pending(session, 1) is None
        assert not any(path.name != ".coordinator" for path in case.root.iterdir())


@pytest.mark.parametrize("point", ["registered", "directory", "lease"])
def test_killed_partial_namespace_resumes_exact_registered_owner(tmp_path, point):
    with lifecycle_case(tmp_path) as case:
        job = registered(case)
        child(case, job, action="namespace", point=point)
        successor = current(case, job.workspace_id)
        assert successor == job and successor.namespace_identity is None
        assert all(not (case.root / job.workspace_id / name).exists() for name in DATABASES)
        finish(case, job.workspace_id)
        assert (case.root / ".coordinator").exists() is (point != "registered")


@pytest.mark.parametrize("point", ["corpus-created", "memo-created", "both-ready"])
def test_killed_index_constructor_keeps_committed_namespace_discoverable(tmp_path, point):
    with lifecycle_case(tmp_path) as case:
        job, original = pinned(case)
        original.close()
        child(case, job, action="index", point=point)
        assert current(case, job.workspace_id) == job
        path = case.root / job.workspace_id
        assert (path / "corpus.sqlite3").is_file()
        assert (path / "checks.sqlite3").exists() is (point != "corpus-created")
        finish(case, job.workspace_id)


def test_real_member_turn_commits_registration_and_pin_before_index_effects(tmp_path, monkeypatch):
    from riverhog_provenance import CorpusConstruction, JournalConstruction

    from tests.unit.test_selected_upload_journal_prefix import _finish, _owner, _state
    from tests.unit.test_shared_primary_admission import shared_primary_finalization

    with shared_primary_finalization(tmp_path, monkeypatch, members=1, fanout=1) as case:
        preparation = case.service._primary_facts
        namespace_open = PrimaryFactsWorkspace.open
        corpus_guard = CorpusConstruction.acquire_guard
        memo_create = JournalConstruction.create_exclusive
        observed = []

        def independent(state):
            with case.factory() as session:
                selected = jobs.find_pending(session, case.collection_id)
            assert selected is not None and selected.state == state
            return selected

        def namespace(self, creation_current):
            selected = independent("allocated")
            assert selected == self.job
            assert not self.path.exists()
            observed.append("committed-registration")
            return namespace_open(self, creation_current)

        def before_database(path):
            selected = independent("pinned")
            original = preparation._owners[case.collection_id]
            assert selected.namespace_identity == original.workspace.namespace_identity
            assert Path(path).parent == preparation._root / selected.workspace_id
            observed.append(Path(path).name)

        def corpus(self, path, **kwargs):
            before_database(path)
            return corpus_guard(self, path, **kwargs)

        def memo(self, path):
            if Path(path).name == "checks.sqlite3":
                before_database(path)
            return memo_create(self, path)

        with monkeypatch.context() as patch:
            patch.setattr(PrimaryFactsWorkspace, "open", namespace)
            patch.setattr(CorpusConstruction, "acquire_guard", corpus)
            patch.setattr(JournalConstruction, "create_exclusive", memo)
            assert _owner(case).advance()
            allocated = independent("allocated")
            path = preparation._root / allocated.workspace_id
            assert not path.exists() and _state(case) == (0, False, None)
            assert _owner(case).advance()
            independent("pinned")
            assert path.is_dir() and all(not (path / name).exists() for name in DATABASES)
            assert _state(case) == (0, False, None)
            _finish(case)
        assert observed == ["committed-registration", "corpus.sqlite3", "checks.sqlite3"]
        assert not preparation.pending
        with case.factory() as session:
            selected = jobs.find_pending(session, case.collection_id)
        assert selected is not None and selected.state == "corpus"
        assert path.is_dir()
        cleanup = case.service._primary_facts_retirement
        direct = SimpleNamespace(factory=case.factory, root=cleanup.root)
        finish_scheduled(direct, cleanup, case.service._archive_execution, [selected.workspace_id])


@pytest.mark.parametrize("selected", ["corpus", "memo"])
@pytest.mark.parametrize("after", [False, True])
def test_uncertain_sqlite_close_retains_lease_and_exact_charge_until_original_receives(
    tmp_path, monkeypatch, selected, after
):
    from riverhog_core.journal_storage_grants import JournalStorageGrants
    from riverhog_core.services.journal_storage_resources import JournalStorageResources
    from riverhog_core.services.upload_primary_facts import _PrimaryIndex

    with lifecycle_case(tmp_path) as case:
        job, workspace = pinned(case)
        resources = JournalStorageResources(JournalStorageGrants(connections=2))
        original = _PrimaryIndex(resources, job, workspace)
        case.originals.append(original)
        original.open()
        target = original.corpus_connection if selected == "corpus" else original.memo
        driver_close = type(target)._driver_close
        calls = []

        def interrupted(connection):
            if connection is target:
                calls.append(True)
                if len(calls) == 1:
                    if after:
                        driver_close(connection)
                    raise OSError("original SQLite close reply is missing")
            return driver_close(connection)

        with monkeypatch.context() as patch:
            patch.setattr(type(target), "_driver_close", interrupted)
            with pytest.raises(OSError, match="close reply is missing"):
                original.close()
            assert not workspace.received and resources.status.connections >= 1
            assert len(calls) == 1
            result = advance(case, job)
            assert not result.advanced and current(case, job.workspace_id) == job
            original.close()
        assert workspace.received and resources.status.connections == 0
        assert len(calls) == 2
        assert all((workspace.path / name).exists() for name in DATABASES)
        resources.close()
        finish(case, job.workspace_id)


@pytest.mark.parametrize("point", ["truncate", "file-unlink", "lease-unlink", "rmdir"])
def test_killed_cleanup_resumes_same_identity_without_payload_replay(tmp_path, monkeypatch, point):
    with lifecycle_case(tmp_path) as case:
        job, original = pinned(case)
        if point in {"truncate", "file-unlink"}:
            populate(original.path, extent=17, sidecars=False)
        original.close()
        assert original.received
        job = retired(case, job)
        if point in {"lease-unlink", "rmdir"}:
            while job.state != "directory":
                advance(case, job)
                job = current(case, job.workspace_id)
        child(case, job, action="cleanup", point=point, budget=1 if point == "truncate" else 64)
        assert current(case, job.workspace_id) == job
        from riverhog_provenance import JournalValidationSession

        def replay(*args, **kwargs):
            pytest.fail("disposable primary cleanup attempted source validation")

        monkeypatch.setattr(JournalValidationSession, "advance", replay)
        finish(case, job.workspace_id)


def test_live_original_excludes_cleanup_while_independent_owner_finishes(tmp_path):
    with lifecycle_case(tmp_path) as case:
        live, original = pinned(case)
        names = populate(original.path)
        before = {name: (original.path / name).read_bytes() for name in names}
        result = advance(case, live)
        assert not result.advanced and result.removed_octets == 0
        assert current(case, live.workspace_id) == live
        peer, peer_original = pinned(case, registered(case, collection_id=2))
        peer_original.close()
        finish(case, peer.workspace_id)
        assert {name: (original.path / name).read_bytes() for name in names} == before
        original.close()
        finish(case, live.workspace_id)


def test_restart_discovery_preserves_live_owner_and_receives_an_independent_namespace(tmp_path):
    with lifecycle_case(tmp_path) as case:
        live, held = pinned(case)
        names = populate(held.path, sidecars=True)
        before = {name: (held.path / name).read_bytes() for name in names}
        orphan, abandoned = pinned(case, registered(case, collection_id=2))
        populate(abandoned.path, sidecars=True)
        abandoned.close()
        with scheduled(case, slots=1) as (cleanup, execution, retention):
            finish_scheduled(case, cleanup, execution, [orphan.workspace_id], budget=7)
            assert current(case, live.workspace_id) == live
            assert {name: (held.path / name).read_bytes() for name in names} == before
            assert not cleanup._registry._contexts
            assert retention._counts == {False: 0, True: 0}
            held.close()
            finish_scheduled(case, cleanup, execution, [live.workspace_id], budget=7)


@pytest.mark.parametrize("field", ["construction", "epoch", "owner_id", "version"])
def test_changed_registration_cannot_supply_disposal_authority(tmp_path, field):
    with lifecycle_case(tmp_path) as case:
        job, original = pinned(case)
        names = populate(original.path, sidecars=False)
        original.close()
        stale = replace(job, **{field: "f" * (32 if field in {"owner_id", "version"} else 64)})
        with pytest.raises(Conflict):
            advance(case, stale)
        assert current(case, job.workspace_id) == job
        assert all(
            (case.root / job.workspace_id / name).read_bytes() == b"p" * 17 for name in names
        )
        finish(case, job.workspace_id)


@pytest.mark.parametrize("target", ["directory", "lease"])
def test_replaced_confirmed_namespace_preserves_both_original_and_replacement(tmp_path, target):
    with lifecycle_case(tmp_path) as case:
        job, original = pinned(case)
        populate(original.path, sidecars=False)
        original.close()
        selected = case.root / job.workspace_id
        original_path = selected if target == "directory" else selected / ".lease"
        moved = original_path.with_name(original_path.name + "-original")
        original_path.rename(moved)
        if target == "directory":
            original_path.mkdir(mode=0o700)
            (original_path / "foreign").write_bytes(b"replacement")
        else:
            original_path.write_bytes(b"replacement")
            original_path.chmod(0o600)
        with pytest.raises((Conflict, LostJournalValidationWorkspace)):
            advance(case, job)
        assert current(case, job.workspace_id) == job
        assert original_path.exists() and moved.exists()
        assert (selected / "foreign" if target == "directory" else original_path).read_bytes() == (
            b"replacement"
        )


@pytest.mark.parametrize("phase", ["corpus", "memo"])
def test_replaced_namespace_at_file_acquisition_cannot_mutate_either_index(
    tmp_path, monkeypatch, phase
):
    import workspace_reclamation
    from riverhog_provenance.corpus_reclamation import StaleCorpusWorkspace

    with lifecycle_case(tmp_path) as case:
        job, original = pinned(case)
        path = original.path
        populate(path, sidecars=False)
        original.close()
        job = retired(case, job)
        while job.state != phase:
            advance(case, job)
            job = current(case, job.workspace_id)
        moved = path.with_name(path.name + "-original")
        actual = workspace_reclamation.FileReclamationOwner._reclaim
        calls = []

        def substituted(receiver, budget):
            calls.append(True)
            path.rename(moved)
            path.mkdir(mode=0o700)
            for name in DATABASES:
                (path / name).write_bytes(b"replacement index")
                (path / name).chmod(0o600)
            return actual(receiver, budget)

        with monkeypatch.context() as patch:
            patch.setattr(workspace_reclamation.FileReclamationOwner, "_reclaim", substituted)
            with pytest.raises(
                (
                    LostJournalValidationWorkspace,
                    workspace_reclamation.InvalidWorkspaceFiles,
                    StaleCorpusWorkspace,
                )
            ):
                advance(case, job, budget=1)
        assert calls == [True] and current(case, job.workspace_id) == job
        assert (
            moved / ("corpus.sqlite3" if phase == "corpus" else "checks.sqlite3")
        ).read_bytes() == (b"p" * 17)
        assert all((path / name).read_bytes() == b"replacement index" for name in DATABASES)


def test_unknown_child_blocks_only_final_namespace_receiving(tmp_path):
    with lifecycle_case(tmp_path) as case:
        job, original = pinned(case)
        path = original.path
        populate(path, sidecars=False)
        (path / "unclassified").write_bytes(b"retain unknown bytes")
        original.close()
        while current(case, job.workspace_id).state != "directory":
            advance(case, current(case, job.workspace_id))
        selected = current(case, job.workspace_id)
        with pytest.raises((Conflict, LostJournalValidationWorkspace)):
            advance(case, selected)
        assert current(case, job.workspace_id) == selected
        assert (path / "unclassified").read_bytes() == b"retain unknown bytes"
        blocked = case.originals[-1]
        assert not blocked.received
        blocked.close()
        assert blocked.received
        (path / "unclassified").unlink()
        finish(case, job.workspace_id)


def unknown_main(url, root, identity, extension, fault):
    """Isolate an intentionally unreceivable original; process exit is no receipt."""
    import importlib.util

    from riverhog_core import retained_workspace_handles as handles

    spec = importlib.util.spec_from_file_location("_retained_resource_test._native", extension)
    native = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(native)
    handles.Resource, handles.Borrow = native.Resource, native.Borrow
    root = Path(root)
    engine = create_catalog_engine(url)
    factory = sessionmaker(engine, expire_on_commit=False)
    case = SimpleNamespace(
        factory=factory, root=root, root_identity=capture_root_identity(root), originals=[]
    )
    case.cleanup = Cleanup(factory, root)
    inode = (root / identity).stat().st_ino
    acquire = handles.RetainedWorkspaceResource.acquire
    selected = []

    def acquired(original, path, **kwargs):
        result = acquire(original, path, **kwargs)
        parent = kwargs.get("parent")
        if path == ".lease" and parent is not None and os.fstat(parent.fileno()).st_ino == inode:
            selected.append((original, original.fileno()))
            original.owner._test_fault(fault)
        return result

    handles.RetainedWorkspaceResource.acquire = acquired
    owner_context = scheduled(case, slots=2)
    cleanup, execution, retention = owner_context.__enter__()
    for _ in range(8):
        schedule_step(cleanup, execution, budget=1)
        if selected:
            break
    assert len(selected) == 1
    original, old = selected[0]
    context = cleanup._registry._contexts[identity]
    assert original.owner.state == "unknown" and context.closing and not context.actor.received
    assert retention._counts[False] == 1 and not execution._slots
    replacement = None
    if fault == "unknown_close_closed":
        path = root.parent / "independent-descriptor"
        path.write_bytes(b"retain independent descriptor")
        replacement = os.open(path, os.O_RDONLY)
        if replacement != old:
            os.dup2(replacement, old, inheritable=False)
            os.close(replacement)
        replacement = old
        replacement_identity = os.fstat(old)
    calls = original.owner.metrics["close_calls"]
    peer = registered(case, collection_id=2)
    finish_scheduled(case, cleanup, execution, [peer.workspace_id], budget=1)
    for _ in range(3):
        schedule_step(cleanup, execution, budget=1)
    assert cleanup._registry._contexts[identity] is context
    assert original.owner.metrics["close_calls"] == calls
    assert not context.actor.received and retention._counts[False] == 1
    if replacement is not None:
        assert os.fstat(replacement) == replacement_identity
        os.close(replacement)
    print(
        json.dumps(
            {
                "original_retained": True,
                "ordinary_charge": retention._counts[False],
                "independent_received": current(case, peer.workspace_id) is None,
                "repeat_close_dispatches": original.owner.metrics["close_calls"] - calls,
                "physical_workers": len(execution._slots),
                "forced_receiving": context.actor.received,
            }
        ),
        flush=True,
    )
    # Test containment only. The next process must establish fresh quiescence.
    os._exit(0)


@pytest.mark.parametrize("fault", ["unknown_close_open", "unknown_close_closed"])
def test_unknown_original_stays_charged_while_peer_cleanup_receives(tmp_path, fault_native, fault):
    _, extension = fault_native
    with lifecycle_case(tmp_path) as case:
        job, workspace = pinned(case)
        populate(workspace.path, extent=17, sidecars=False)
        workspace.close()
        job = retired(case, job)
        program = (
            "import sys; from tests.unit.test_primary_facts_lifecycle import unknown_main; "
            "unknown_main(*sys.argv[1:])"
        )
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                program,
                str(case.engine.url),
                str(case.root),
                job.workspace_id,
                str(extension),
                fault,
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, (result.stdout + result.stderr)[-5000:]
        assert json.loads(result.stdout) == {
            "original_retained": True,
            "ordinary_charge": 1,
            "independent_received": True,
            "repeat_close_dispatches": 0,
            "physical_workers": 0,
            "forced_receiving": False,
        }
        assert current(case, job.workspace_id) == job
        finish(case, job.workspace_id)


def test_known_namespace_blocker_is_recorded_without_losing_original_receiving(tmp_path):
    with lifecycle_case(tmp_path) as case:
        job, workspace = pinned(case)
        workspace.close()
        path = workspace.path
        (path / ".lease").rename(path / "original-lease")
        (path / ".lease").touch(mode=0o600)
        with scheduled(case, slots=1) as (cleanup, execution, retention):
            schedule_step(cleanup, execution)
            blocked = current(case, job.workspace_id)
            assert blocked.failure and blocked.version == job.version
            assert not cleanup._registry._contexts and retention._counts == {False: 0, True: 0}
            schedule_step(cleanup, execution)
            assert current(case, job.workspace_id) == blocked
            assert not cleanup._registry._contexts
            assert (path / "original-lease").exists() and (path / ".lease").exists()
