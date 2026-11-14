from __future__ import annotations

import hashlib
from datetime import timedelta
from types import SimpleNamespace

import pytest
from riverhog_age import CHUNK_SIZE
from riverhog_core.archive_execution_tuning import ArchiveExecutionTuning
from riverhog_core.primary_facts_workspace import capture_root_identity
from riverhog_core.services.archive_construction_retention import ArchiveConstructionRetention
from riverhog_core.services.archive_finalizer_registry import ArchiveFinalizerRegistry
from riverhog_core.services.collection_uploads import SqlAlchemyCollectionUploadService
from riverhog_core.services.physical_execution import PhysicalExecutionOwner
from riverhog_core.services.upload_primary_facts_retirement import UploadPrimaryFactsRetirement
from riverhog_protocol import CollectionUploadRawDigestBatchDocument
from riverhog_protocol.raw_ingress import ordered_raw_part_commitment

from tests.unit.archive_object_fixtures import MemoryArchiveStore
from tests.unit.test_archive_finalizer_registry import _receive, _scope
from tests.unit.test_archive_upload_execution import _joined
from tests.unit.test_collection_uploads import _CREATOR
from tests.unit.test_primary_facts_lifecycle import (
    Cleanup,
    current,
    finish_scheduled,
    pinned,
    populate,
    registered,
    retired,
    schedule_step,
)
from tests.unit.test_selected_upload_journal_prefix import _owner, _state
from tests.unit.test_shared_primary_admission import shared_primary_finalization


def _second_collection(first):
    service = first.service
    opened = service.create_or_resume(
        idempotency_key="second-primary-preparation",
        ingest_source="fixture",
        archive_store=None,
        use_cache=False,
        custody_mode="producer-retained",
        initiator=_CREATOR,
        event_context=None,
    )
    collection_id = int(opened["collection_id"])
    digest = hashlib.sha256(first.content).hexdigest()
    count, commitment = ordered_raw_part_commitment([digest])
    artifact_id = format(1, "064x")
    service.register_artifacts(
        collection_id,
        [
            {
                "artifact_id": artifact_id,
                "bytes": str(len(first.content)),
                "sha256": digest,
                "raw_parts": {
                    "part_plaintext_bytes": str(CHUNK_SIZE),
                    "part_count": str(count),
                    "ordered_sha256": commitment,
                },
            }
        ],
    )
    service.register_raw_part_digests(
        collection_id,
        CollectionUploadRawDigestBatchDocument(
            artifact_id=artifact_id, first_part="0", sha256s=[digest]
        ),
    )
    volume = service.list_volumes(collection_id)["volumes"][0]
    return SimpleNamespace(
        service=service,
        factory=first.factory,
        provider=first.provider,
        collection_id=collection_id,
        object_id=volume["volume_id"],
        plan_sha256=volume["plan_sha256"],
        content=first.content,
    )


def _turn(registry, execution, case):
    key, target = _scope(case)
    original = _owner(case)
    assert (
        registry.submit(
            key,
            target_incarnation=target,
            selection="primary-admission",
            create=lambda: original,
            advance=lambda owner: owner.advance(),
            resume_original=False,
        )
        == "submitted"
    )
    (completion,) = _receive(registry, execution)
    turn = completion.result()
    assert not original.has_pending and not original.result_resources.pending
    assert registry.retire_idle(key, target_incarnation=target)
    return turn.progressed


def _finish(registry, execution, case, cleanup):
    for _ in range(128):
        schedule_step(cleanup, execution)
        if not _turn(registry, execution, case):
            break
    else:
        pytest.fail("received admission yields did not permit closure completion")
    assert _state(case) == (1, True, None)


def _forward_progress_while_cleanup_remains(case, registry, execution, cleanup):
    """The public poll admits real competing originals with one shared slot."""
    preparation = case.service._primary_facts
    fixture = SimpleNamespace(
        factory=case.factory,
        root=preparation._root,
        root_identity=capture_root_identity(preparation._root),
        originals=[],
        cleanup=Cleanup(case.factory, preparation._root),
    )
    job, workspace = pinned(fixture, registered(fixture, collection_id=1000000))
    populate(workspace.path, extent=65537, sidecars=False)
    workspace.close()
    job = retired(fixture, job)
    key, target = _scope(case)
    outcomes, originals, received = [], [], []

    def maintenance(*, limit):
        progressed = cleanup.advance(limit=limit, octet_budget=1, admit=False)
        for completion in registry.collect(limit=16):
            received.append(completion.result().progressed)
            assert registry.retire_idle(key, target_incarnation=target)
            progressed += 1
        return progressed

    def submit(collection_id, *, now):
        assert collection_id == case.collection_id and now

        def create():
            original = _owner(case)
            originals.append(original)
            return original

        outcome = registry.submit(
            key,
            target_incarnation=target,
            selection="primary-admission",
            create=create,
            advance=lambda original: original.advance(),
            resume_original=False,
        )
        outcomes.append(outcome)
        return outcome

    poll = SimpleNamespace(
        process_due_archive_uploads=maintenance,
        _primary_facts_cleanup_turn=False,
        _primary_facts_retirement=SimpleNamespace(
            advance=lambda **kwargs: cleanup.advance(octet_budget=1, **kwargs)
        ),
        _schedule_ready_finalizations=lambda **kwargs: 0,
        _finalization_due=SimpleNamespace(select=lambda now, **kwargs: [case.collection_id]),
        _submit_finalization_if_due=submit,
    )
    before = preparation.metrics["source_frame_reads"]
    for _ in range(2):
        # Other maintenance callers can poll between finalization polls without
        # consuming the next forward admission opportunity.
        maintenance(limit=1)
        SqlAlchemyCollectionUploadService.process_due_finalizations(poll, limit=1)
        _joined(execution)
    maintenance(limit=1)
    assert "retention-capacity" in outcomes and "submitted" in outcomes
    assert received == [True]
    assert all(
        not original.has_pending and not original.result_resources.pending for original in originals
    )
    assert preparation.metrics["source_frame_reads"] > before
    assert current(fixture, job.workspace_id) is not None
    assert 0 < (workspace.path / "corpus.sqlite3").stat().st_size < 65537
    finish_scheduled(fixture, cleanup, execution, [job.workspace_id])


@pytest.mark.parametrize("capacity", ["owners", "storage"])
def test_received_primary_capacity_defer_releases_its_real_finalizer_reservation(
    tmp_path, monkeypatch, capacity
):
    with shared_primary_finalization(tmp_path, monkeypatch, members=1, fanout=1) as first:
        with monkeypatch.context() as allocation:
            allocation.setattr(
                MemoryArchiveStore,
                "new_collection_archive_storage_prefix",
                lambda self: "archives/memory/second-copy",
            )
            second = _second_collection(first)
        with shared_primary_finalization(
            tmp_path, monkeypatch, members=1, fanout=1, existing_case=second
        ):
            slots = 1 if capacity == "owners" else 2
            tuning = ArchiveExecutionTuning(
                retained_constructions=slots, retained_constructions_per_target=slots
            )
            execution = PhysicalExecutionOwner(tuning)
            retention = ArchiveConstructionRetention(tuning)
            registry = ArchiveFinalizerRegistry(first.factory, execution, retention)
            preparation = first.service._primary_facts
            cleanup = UploadPrimaryFactsRetirement(
                first.factory,
                preparation._root,
                owner_id=preparation.owner_id,
                execution=execution,
                retention=retention,
                metadata_budget=1024 * 1024,
                receive_limit=16,
                retry_after=timedelta(microseconds=1),
            )
            monkeypatch.setattr(preparation, "_max_owners", slots)
            storage = first.service._journal_storage
            fillers = []
            try:
                for _ in range(4):
                    assert _turn(registry, execution, first)
                    assert _state(first) == (0, False, None)
                    if storage.status.connections == 2:
                        break
                else:
                    pytest.fail("registration and pin turns did not reach admitted index work")
                assert set(preparation._owners) == {first.collection_id}
                assert storage.status.connections == 2
                if capacity == "storage":
                    # B can open its two empty index grants, but its first
                    # Native opening has no grant. These independent originals
                    # stay charged while A and then B finish their closures.
                    fillers = [
                        storage.journal() for _ in range(storage.status.connection_limit - 4)
                    ]
                    for original in fillers:
                        original.scope.reserve()
                charged = storage.status.connections
                for _ in range(4):
                    assert _turn(registry, execution, second)
                    assert _state(second) == (0, False, None)
                    if preparation.metrics["capacity_deferrals"]:
                        break
                else:
                    pytest.fail("bounded second construction never reached its capacity refusal")
                assert preparation.metrics["capacity_deferrals"] == 1
                assert set(preparation._owners) == {first.collection_id}
                assert storage.status.connections == charged
                assert registry._contexts == {}
                if capacity == "owners":
                    _forward_progress_while_cleanup_remains(first, registry, execution, cleanup)
                # In the single-slot case an unreceived B would now prevent
                # A's registry admission, stranding the source that blocks B.
                _finish(registry, execution, first, cleanup)
                assert not preparation.pending
                _finish(registry, execution, second, cleanup)
                assert not preparation.pending
                assert storage.status.connections == len(fillers)
                from riverhog_core.services import primary_facts_jobs as jobs

                with first.factory() as session:
                    final = jobs.find_pending(session, second.collection_id)
                if final is not None:
                    finish_scheduled(
                        SimpleNamespace(factory=first.factory, root=preparation._root),
                        cleanup,
                        execution,
                        [final.workspace_id],
                    )
            finally:
                cleanup.close()
                registry.close()
                assert cleanup.receive_stopped()
                for original in reversed(fillers):
                    assert original.close_step()
