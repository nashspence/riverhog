"""Actual rejected Native file disposal alongside its retained semantic executor."""

import os

import pytest
from riverhog_provenance import reclaim_retired_generation_storage as reclaim

from tests.support.retained_resource import fault_native as fault_native
from tests.unit.test_journal_generation_rejection import reject
from tests.unit.test_journal_recovery_frontier import origin as origin
from tests.unit.test_journal_recovery_frontier import snapshot
from tests.unit.test_upload_journal_validation_owner import finish

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX generation file custody")


def test_actual_rejected_native_store_disposal_preserves_live_semantic_execution(origin):
    case = origin
    directory = case.directory
    identity = case.slot.custody.directory_identity()
    rejected = reject(case)
    reader = case.storage.roots.borrow(
        case.frontier.image.identity, expected_root=case.frontier.image.root
    )
    try:
        assert not case.generation.close_step()
        assert reader.read(after=0, page_limit=1).pages
        assert (directory / "generations.sqlite3").exists()
    finally:
        reader.close()
    assert case.generation.close_step() and case.generation.received
    assert not case.slot.construction.received
    before = (directory / "generations.sqlite3").stat().st_size
    calls = removed = 0
    while True:
        original = case.generation.retain_reclamation()
        progress = reclaim(
            directory, receiver=original, expected_directory_identity=identity, octet_budget=4096
        )
        assert original.received
        removed += progress.removed_octets
        calls += 1
        if progress.files_reclaimed:
            break
    assert removed == before and calls == (before + 4095) // 4096
    assert snapshot(case)[0] == rejected
    assert rejected.accounting.held_count_slots == 1
    assert (directory / "snapshot.sqlite3").is_file() and (directory / ".lease").is_file()
    outcome = finish(case.owner, case.work)
    assert outcome.failure is None
    root, job = snapshot(case)
    assert root == rejected and job.state == "complete" and not job.recovery_received


def test_file_receiver_retention_requires_original_native_receiving(origin):
    case = origin
    with pytest.raises(BlockingIOError, match="Native dependencies"):
        case.generation.retain_reclamation()
    assert case.generation.reclamation is None
    assert case.generation.close_step()
    original = case.generation.retain_reclamation()
    assert not original.received and not case.generation.received
    assert case.generation.reclamation is original
    with pytest.raises(BlockingIOError, match="file reclamation attempt"):
        case.generation.retain_reclamation()
    assert case.generation.reclamation is original
    assert case.generation.close_step()
    assert original.received and case.generation.received
    assert original._directory.metrics["open_calls"] == original._file.metrics["open_calls"] == 0
    # A later separately admitted extent never replaces an unresolved original.
    later = case.generation.retain_reclamation()
    assert later is not original and not later.received and original.received
    assert case.generation.close_step() and later.received


@pytest.mark.parametrize("slot", ["_directory", "_file"])
@pytest.mark.parametrize("fault", ["before_close", "after_close", "signal_after_close"])
def test_live_slot_keeps_its_exact_file_receiver_and_lease_after_uncertain_cleanup(
    origin, monkeypatch, fault_native, slot, fault
):
    import errno

    import workspace_reclamation as files_owner
    from riverhog_provenance import JournalValidationCapacity

    from tests.support.validation_workspace import leased_workspace

    case = origin
    rejected = reject(case)
    assert case.generation.close_step()
    native, _path = fault_native
    monkeypatch.setattr(files_owner, "Resource", native.Resource)
    monkeypatch.setattr(files_owner, "Borrow", native.Borrow)
    original = case.generation.retain_reclamation()
    identity = case.slot.custody.directory_identity()

    def refused_sync(_descriptor):
        raise OSError(errno.ENOSPC, "injected original extent sync")

    with monkeypatch.context() as patch:
        patch.setattr(os, "fsync", refused_sync)
        with pytest.raises(JournalValidationCapacity):
            reclaim(
                case.directory,
                receiver=original,
                expected_directory_identity=identity,
                octet_budget=4096,
            )
    size = (case.directory / "generations.sqlite3").stat().st_size
    getattr(original, slot)._test_fault(fault)
    with pytest.raises((RuntimeError, KeyboardInterrupt)):
        case.owner.close()
    assert case.generation.reclamation is original and not case.generation.received
    assert case.slot.generation is case.generation and case.owner._slots
    assert not case.slot.construction.received
    assert snapshot(case)[0] == rejected and rejected.accounting.held_count_slots == 1
    with leased_workspace(case.owner.root, case.job.workspace_id) as busy:
        assert busy is None
    case.owner.close()
    assert original.received and case.generation.received and not case.owner._slots
    assert (case.directory / "generations.sqlite3").stat().st_size == size
    with leased_workspace(case.owner.root, case.job.workspace_id) as free:
        assert free == case.directory


def test_active_file_executor_cannot_lose_its_slot_or_kernel_lease(origin, monkeypatch):
    from threading import Event, Thread

    from tests.support.validation_workspace import leased_workspace

    case = origin
    rejected = reject(case)
    assert case.generation.close_step()
    original = case.generation.retain_reclamation()
    identity = case.slot.custody.directory_identity()
    entered, release = Event(), Event()
    truncate = os.ftruncate
    outcomes = []

    def paused(descriptor, extent):
        entered.set()
        assert release.wait(10), "test did not release its actual extent executor"
        return truncate(descriptor, extent)

    def run():
        try:
            outcomes.append(
                reclaim(
                    case.directory,
                    receiver=original,
                    expected_directory_identity=identity,
                    octet_budget=4096,
                )
            )
        except BaseException as error:
            outcomes.append(error)

    monkeypatch.setattr(os, "ftruncate", paused)
    worker = Thread(target=run)
    worker.start()
    try:
        assert entered.wait(10), "actual extent executor did not enter"
        with pytest.raises(BlockingIOError, match="extent executor"):
            case.owner.close()
        assert case.owner.active_keys() == ((case.work.collection_id, case.work.journal_id),)
        assert case.generation.reclamation is original and not case.generation.received
        assert original._directory_borrow.active and original._file_borrow.active
        assert original.receipt is None
        with leased_workspace(case.owner.root, case.job.workspace_id) as busy:
            assert busy is None
        assert snapshot(case)[0] == rejected and rejected.accounting.reserved_ns > 0
    finally:
        release.set()
        worker.join(timeout=10)
    assert not worker.is_alive() and len(outcomes) == 1
    assert not isinstance(outcomes[0], BaseException) and original.received
    assert original.receipt is not None and original.receipt.directory_identity == identity
    assert original.receipt.progress.removed_octets == 4096
    assert not original.receipt.progress.files_reclaimed
    case.owner.close()
    assert case.generation.received and not case.owner._slots
