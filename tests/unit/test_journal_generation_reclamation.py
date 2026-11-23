"""Actual rejected Native file disposal alongside its retained semantic executor."""

import os

import pytest
from riverhog_provenance import reclaim_retired_generation_storage as reclaim

from tests.support.workspace_reclamation import retained_file_receivers as retained_file_receivers
from tests.unit.test_journal_generation_rejection import reject
from tests.unit.test_journal_recovery_frontier import origin as origin
from tests.unit.test_journal_recovery_frontier import snapshot
from tests.unit.test_upload_journal_validation_owner import finish

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX generation file custody")


def test_actual_rejected_native_store_disposal_preserves_live_semantic_execution(
    origin, retained_file_receivers
):
    case = origin
    directory = case.directory
    pinned = directory.stat()
    identity = pinned.st_dev, pinned.st_ino
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
        original = retained_file_receivers()
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
