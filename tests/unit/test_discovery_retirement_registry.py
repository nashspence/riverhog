from __future__ import annotations

import os
import threading
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace

import pytest
from riverhog_core.incremental_emission import VolumeEmissionCapacity
from riverhog_core.retained_workspace_handles import RetainedWorkspaceResource
from riverhog_core.services import discovery_retirement_registry as source
from riverhog_core.services import physical_execution as physical
from riverhog_core.services import workspace_retirement_registry as shared_source
from riverhog_core.services.archive_construction_retention import ArchiveConstructionRetention
from riverhog_core.services.discovery_retirement_registry import DiscoveryRetirementRegistry
from riverhog_protocol.errors import Conflict

from tests.unit.test_archive_upload_execution import _joined
from tests.unit.test_archive_upload_execution import owned as owned
from tests.unit.test_canonical_discovery_child_reclamation import prepare_child
from tests.unit.test_canonical_discovery_retired_metadata import case as case
from tests.unit.test_canonical_discovery_retired_metadata import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_retired_metadata import original as original
from tests.unit.test_canonical_discovery_retired_metadata import qualified as qualified
from tests.unit.test_canonical_discovery_retirement import current
from tests.unit.test_discovery_validation_owner import UncertainClose

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX discovery receiving")
RETRY = timedelta(microseconds=1)


@pytest.fixture
def registry(case, owned):
    pool = owned.make(terminal_workers=2)
    ledger = ArchiveConstructionRetention(pool._tuning)
    value = DiscoveryRetirementRegistry(case.factory, case.root, pool, ledger)
    yield SimpleNamespace(registry=value, pool=pool, ledger=ledger)
    value.close()
    pool.close()
    value.collect(limit=32)
    value.receive_stopped()


def submit(f, job, budget=3, metadata=1024 * 1024):
    return f.registry.submit(job, octet_budget=budget, metadata_budget=metadata, retry_after=RETRY)


def received(f):
    _joined(f.pool)
    return f.registry.collect(limit=4)


def test_actual_quantum_holds_ordinary_charge_through_joined_original_receiving(case, registry):
    job, path, bindings = prepare_child(case, count=1, extent=17)
    assert submit(registry, job) == 1
    context = registry.registry._contexts[job.workspace_id]
    assert not context.reservation.terminal
    _joined(registry.pool)
    assert context.actor._closed and registry.ledger._counts[False] == 1
    assert registry.registry.retained(job.workspace_id)
    assert received(registry) == 2
    assert context.progress.removed_octets == 3
    assert context.original is context.actor
    file = path / "journals" / bindings[0].directory_id / "snapshot.sqlite3"
    assert file.stat().st_size == 14
    assert registry.ledger._counts == {False: 0, True: 0}
    assert not registry.registry.retained(job.workspace_id)


def test_actual_full_chain_receives_each_original_before_a_fresh_actor(case, registry):
    job, path, _ = prepare_child(case, count=1)
    contexts, phases = [], []
    for _ in range(16):
        phases.append(job.reclamation_phase)
        assert submit(registry, job, budget=1024 * 1024) == 1
        context = registry.registry._contexts[job.workspace_id]
        contexts.append(context)
        assert received(registry) == 2
        assert context.closed and context.actor._closed
        if context.progress.catalog_received:
            break
        job = current(case, job.workspace_id)
        assert job is not None
    else:
        pytest.fail("small actual cleanup did not complete")
    assert set(phases) == {
        "children",
        "child_directory",
        "corpus",
        "journals_root",
        "metadata",
        "directory",
    }
    assert len({id(value.actor) for value in contexts}) == len(contexts)
    assert not path.exists() and current(case, job.workspace_id) is None
    assert registry.ledger._counts[False] == 0


def test_published_result_and_closed_actor_wait_for_actual_worker_exit(
    case, registry, owned, monkeypatch
):
    job, _, _ = prepare_child(case, count=1)
    release, returned = owned.event(), threading.Event()
    actual_thread = physical.Thread

    class LateExit(actual_thread):
        def run(self):
            super().run()
            returned.set()
            assert release.wait(10)

    monkeypatch.setattr(physical, "Thread", LateExit)
    assert submit(registry, job) == 1 and returned.wait(5)
    context = registry.registry._contexts[job.workspace_id]
    assert context.actor._closed and registry.pool._slots[context.reservation.key].future.done()
    assert registry.registry.collect(limit=1) == 0
    assert registry.ledger._counts[False] == 1
    assert submit(registry, replace(job, version="f" * 64)) == 0
    release.set()
    assert received(registry) == 2 and registry.ledger._counts[False] == 0


@pytest.mark.parametrize("lost_reply", (False, True))
def test_core_close_retry_keeps_actual_original_and_never_reconstructs(
    case, registry, monkeypatch, lost_reply
):
    from riverhog_core.canonical_discovery_retired_metadata import RetiredDiscoveryMetadata

    job, _, _ = prepare_child(case, count=1)
    opened = RetiredDiscoveryMetadata.open

    def uncertain(metadata):
        opened(metadata)
        metadata._db = UncertainClose(metadata._db, lost_reply=lost_reply)

    monkeypatch.setattr(shared_source, "monotonic", lambda: 1.0)
    with monkeypatch.context() as patch:
        patch.setattr(RetiredDiscoveryMetadata, "open", uncertain)
        assert submit(registry, job) == 1
        assert received(registry) == 1
    context = registry.registry._contexts[job.workspace_id]
    actor = context.actor
    assert context.closing and context.received and not context.closed
    assert actor._custody.verify_current() and registry.ledger._counts[False] == 1
    assert submit(registry, current(case, job.workspace_id)) == 0

    def forbidden(*_args, **_kwargs):
        pytest.fail("receiving reconstructed original work")

    monkeypatch.setattr(registry.registry, "_create", forbidden)
    monkeypatch.setattr(source.DiscoveryRetirementTurn, "_advance", forbidden)
    monkeypatch.setattr(shared_source, "monotonic", lambda: 2.0)
    assert registry.registry.resume_ready(limit=1, retry_after=RETRY) == 1
    assert received(registry) == 2
    assert context.actor is actor and actor._closed and not actor._custody._held
    assert registry.ledger._counts[False] == 0


def test_repeated_parent_close_refusal_preserves_one_original_and_its_charge(
    case, registry, monkeypatch
):
    job, _, _ = prepare_child(case, count=1)
    actual = RetainedWorkspaceResource.close
    refusing = True

    def closed(handle):
        context = registry.registry._contexts.get(job.workspace_id)
        actor = None if context is None else context.actor
        if (
            refusing
            and actor is not None
            and actor._custody is not None
            and handle.borrow.active
            and handle.fileno() == actor._custody._lease
        ):
            raise OSError("actual parent close refused")
        return actual(handle)

    with monkeypatch.context() as patch:
        patch.setattr(RetainedWorkspaceResource, "close", closed)
        monkeypatch.setattr(shared_source, "monotonic", lambda: 1.0)
        assert submit(registry, job) == 1 and received(registry) == 1
        context = registry.registry._contexts[job.workspace_id]
        actor = context.actor
        monkeypatch.setattr(shared_source, "monotonic", lambda: 2.0)
        assert registry.registry.resume_ready(limit=1, retry_after=RETRY) == 1
        assert received(registry) == 1
        assert context.actor is actor and actor._custody.verify_current()
        assert registry.ledger._counts[False] == 1
        refusing = False
        monkeypatch.setattr(shared_source, "monotonic", lambda: 3.0)
        assert registry.registry.resume_ready(limit=1, retry_after=RETRY) == 1
        assert received(registry) == 2
    assert actor._closed and registry.ledger._counts[False] == 0


def test_unknown_constructor_stays_charged_without_reconstruction(case, registry, monkeypatch):
    job, _, _ = prepare_child(case, count=1)
    calls = []

    def unknown(original):
        calls.append(original)
        raise OSError("original constructor outcome unknown")

    monkeypatch.setattr(registry.registry, "_create", unknown)
    assert submit(registry, job) == 1 and received(registry) == 1
    context = registry.registry._contexts[job.workspace_id]
    assert context.actor is None and context.unknown is not None
    assert context.entered and context.received and not context.closed
    assert registry.registry.resume_ready(limit=4, retry_after=RETRY) == 0
    assert submit(registry, replace(job, version="f" * 64)) == 0
    assert calls == [job] and registry.ledger._counts[False] == 1
    registry.registry.close()
    assert not registry.registry.receive_stopped()


def test_metadata_rejection_precedes_retention_and_constructor(case, registry, monkeypatch):
    job, _, _ = prepare_child(case, count=1)

    def forbidden(*_args):
        pytest.fail("unadmitted metadata entered construction")

    monkeypatch.setattr(registry.registry, "_create", forbidden)
    with pytest.raises(VolumeEmissionCapacity):
        submit(registry, job, metadata=1)
    assert registry.ledger._counts[False] == 0
    assert not registry.registry.retained(job.workspace_id)


def test_unused_original_retires_only_after_positive_physical_nonentry(case, registry):
    job, _, _ = prepare_child(case, count=1)
    with registry.pool._lock:
        assert submit(registry, job) == 0
    context = registry.registry._contexts[job.workspace_id]
    assert not context.entered and context.closed and context.received
    assert registry.ledger._counts[False] == 1
    assert registry.registry.collect(limit=1) == 1 and registry.ledger._counts[False] == 0


def test_explicit_shutdown_receiving_keeps_refused_handle_charged(case, registry, monkeypatch):
    job, _, _ = prepare_child(case, count=1)
    actual = RetainedWorkspaceResource.close
    refusing = True

    def closed(handle):
        context = registry.registry._contexts.get(job.workspace_id)
        actor = None if context is None else context.actor
        if (
            refusing
            and actor is not None
            and actor._custody is not None
            and handle.borrow.active
            and handle.fileno() == actor._custody._lease
        ):
            raise OSError("actual shutdown close refused")
        return actual(handle)

    with monkeypatch.context() as patch:
        patch.setattr(RetainedWorkspaceResource, "close", closed)
        assert submit(registry, job) == 1 and received(registry) == 1
        with pytest.raises(Conflict, match="stopped admission"):
            registry.registry.receive_stopped()
        registry.registry.close()
        registry.pool.close()
        assert submit(registry, job) == 0
        with pytest.raises(OSError, match="shutdown close refused"):
            registry.registry.receive_stopped()
        assert registry.ledger._counts[False] == 1
        refusing = False
        assert registry.registry.receive_stopped()
    assert registry.ledger._counts[False] == 0
