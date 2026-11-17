"""Focused actual-owner witnesses; no live provider or production patch."""

from __future__ import annotations

import hashlib
import threading
from pathlib import Path
from typing import Any

import pytest
from a_riverhog_filesystem_store import FilesystemStorageAdapter, FilesystemStorageAdapterConfig
from a_riverhog_filesystem_store.incarnation import provision_storage_root
from adapter_experiment import (
    COLLECTION_ID,
    ObservedCopyService,
    ObservedDestination,
    SimulatedInterruption,
    SqlObservation,
    copy_case,
    exact_evidence,
    fixture_archive,
    measure_s3_scan,
    run_pair,
    s3_scan_fixture,
)
from riverhog_core.catalog_models import ArchiveCopyObjectUploadRecord
from riverhog_protocol.errors import Conflict
from riverhog_storage_adapter_protocol import (
    ObjectLocator,
    ObjectReadRequest,
    StorageAdapterRejection,
    WriteCompleteRequest,
    WriteSegmentListRequest,
    WriteStartRequest,
)
from state_schema import read_snapshot

from tests.unit.archive_object_fixtures import FixtureArchive


@pytest.fixture(scope="module")
def archive() -> FixtureArchive:
    return fixture_archive()


@pytest.mark.parametrize("parts", [16, 64])
def test_real_copy_same_exact_output_without_progress_updates(tmp_path: Path, parts: int) -> None:
    pair = run_pair(tmp_path, parts=parts)
    old, new = pair["arms"]["current"], pair["arms"]["volatile"]
    assert pair["exact_evidence_equal"]
    assert old["sql"]["progress_updates"] == old["progress_calls"] > 0
    assert new["sql"]["progress_updates"] == 0
    assert new["sql"]["mutations"] < old["sql"]["mutations"]
    assert new["sql"]["commits"] < old["sql"]["commits"]
    assert old["source_object_reads"] == new["source_object_reads"]
    assert old["segment_calls"] == new["segment_calls"]
    assert old["provider_sessions_created"] == new["provider_sessions_created"] == 1


@pytest.mark.parametrize("mode", ["current", "volatile"])
def test_progress_loss_replays_original_bytes_in_original_session(
    tmp_path: Path, archive: FixtureArchive, mode: str
) -> None:
    service, source, destination = copy_case(tmp_path, archive, mode)
    service.interrupt_after_progress = True
    with pytest.raises(SimulatedInterruption):
        service.process_due(limit=1)
    with read_snapshot(service._session_factory) as session:
        row = session.get(
            ArchiveCopyObjectUploadRecord, (COLLECTION_ID, "b2", archive.pack_plan.volume_id)
        )
        assert row is not None
        original_token = row.write_token
        assert original_token is not None
        assert (row.uploaded_bytes > 0) is (mode == "current")
    calls_before = destination.segment_calls
    restored = ObservedCopyService(service, mode)
    assert restored.latest is None
    assert restored.requeue_interrupted_jobs_for_startup() == 1
    assert restored.process_due(limit=1) == 1
    assert restored.get(COLLECTION_ID, destination_store="b2")["state"] == "completed"
    assert destination._next_write == 2
    assert source.read.count(archive.pack_plan.volume_id) == 2
    assert destination.segment_calls == calls_before + 16
    assert destination.objects == {
        f"archives/b2/new-copy/{name}": value for name, value in archive.stored_objects.items()
    }


@pytest.mark.parametrize("mode", ["current", "volatile"])
def test_sealed_original_survives_lost_catalog_receipt_without_second_transfer(
    tmp_path: Path, archive: FixtureArchive, mode: str
) -> None:
    service, source, destination = copy_case(tmp_path, archive, mode)
    service.interrupt_before_object_receipt = True
    with pytest.raises(SimulatedInterruption):
        service.process_due(limit=1)
    assert len(destination.objects) == 1
    assert destination._writes == {}
    old_segments = destination.segment_calls
    restored = ObservedCopyService(service, mode)
    restored.requeue_interrupted_jobs_for_startup()
    assert restored.process_due(limit=1) == 1
    assert restored.get(COLLECTION_ID, destination_store="b2")["state"] == "completed"
    assert source.read.count(archive.pack_plan.volume_id) == 1
    assert destination.segment_calls == old_segments
    assert destination._next_write == 2


def test_stale_progress_cannot_recreate_or_overwrite_terminal_receipt(
    tmp_path: Path, archive: FixtureArchive
) -> None:
    service, _source, destination = copy_case(tmp_path, archive, "volatile")
    service.process_due(limit=1)
    before = exact_evidence(service, destination)
    with SqlObservation(service) as observed:
        with pytest.raises(Conflict, match="checkpoint disappeared"):
            service._record_copy_progress(
                collection_id=COLLECTION_ID,
                destination_store="b2",
                object_id=archive.pack_plan.volume_id,
                uploaded_bytes=1,
                uploaded_segments=1,
                total_segments=16,
            )
    assert observed.counts.mutations == 0
    assert observed.counts.commits == 0
    assert service.latest is None
    assert exact_evidence(service, destination) == before


def test_status_is_provider_free_and_has_no_catalog_mutation(
    tmp_path: Path, archive: FixtureArchive
) -> None:
    service, source, destination = copy_case(tmp_path, archive, "volatile")
    with SqlObservation(service) as observed:
        for _ in range(3):
            assert service.get(COLLECTION_ID, destination_store="b2")["state"] == "requested"
    assert observed.counts.mutations == 0
    assert source.prepared == source.read == source.cleaned == []
    assert destination.segment_calls == 0
    # Current get uses session_scope: these are read-only transaction commits.
    assert observed.counts.commits == 3


def test_cancel_receives_original_worker_before_removing_destination(
    tmp_path: Path, archive: FixtureArchive
) -> None:
    entered, release, returned = threading.Event(), threading.Event(), threading.Event()

    class HeldDestination(ObservedDestination):
        def write_segment(self, **kwargs: Any) -> Any:
            entered.set()
            assert release.wait(timeout=5)
            value = super().write_segment(**kwargs)
            returned.set()
            return value

    destination = HeldDestination()
    service, _source, _target = copy_case(tmp_path, archive, "volatile", destination=destination)
    errors = []

    def work() -> None:
        try:
            service.process_due(limit=1)
        except BaseException as error:
            errors.append(error)

    worker = threading.Thread(target=work)
    worker.start()
    try:
        assert entered.wait(timeout=5)
        assert service.cancel(COLLECTION_ID, destination_store="b2")["state"] == "canceling"
        assert worker.is_alive()
        assert not returned.is_set()
        assert destination.discarded_uploads == []
    finally:
        release.set()
        worker.join(timeout=5)
    assert not worker.is_alive()
    assert errors == []
    assert returned.is_set()
    for _ in range(4):
        if service.get(COLLECTION_ID, destination_store="b2")["state"] != "canceling":
            break
        service.process_due(limit=1)
    assert service.get(COLLECTION_ID, destination_store="b2")["state"] == "canceled"
    assert destination.objects == destination._writes == {}
    assert destination.discarded_uploads == ["archives/b2/new-copy"]


def test_s3_current_pagination_repeats_full_provider_part_scan() -> None:
    measured = measure_s3_scan()
    assert measured["public_pages"] == 17
    assert measured["provider_list_parts_calls"] == 51
    assert measured["provider_metadata_rows"] == 2050 * 17


def test_s3_changed_receipts_invalidate_original_traversal() -> None:
    adapter, provider, session = s3_scan_fixture(parts=129)
    first = adapter.list_segments(WriteSegmentListRequest(session=session))
    provider.changed = True
    with pytest.raises(StorageAdapterRejection, match="changed"):
        adapter.list_segments(
            WriteSegmentListRequest(
                session=session,
                after_number=first.next_after_number,
                traversal_token=first.traversal_token,
            )
        )


def test_completed_filesystem_ledger_is_required_after_original_write_retires(
    tmp_path: Path,
) -> None:
    root = tmp_path / "physical-store"
    root.mkdir()
    provision_storage_root(root)
    config = FilesystemStorageAdapterConfig(
        root=root, segment_bytes=65536, read_chunk_bytes=65536, minimum_free_bytes=0
    )
    content = b"x" * 65536 + b"exact-final-part"
    start = WriteStartRequest(
        object_path="objects/original.age",
        expected_bytes=len(content),
        content_type="application/octet-stream",
        required_identity_assertions={"fixture": hashlib.sha256(content).hexdigest()},
        placement_policy="immediate_default",
    )
    with FilesystemStorageAdapter(config) as adapter:
        session = adapter.begin_write(start)
        for number, value in enumerate((content[:65536], content[65536:]), start=1):
            adapter.write_segment(
                session=session, number=number, stored_bytes=len(value), content=value
            )
        page = adapter.list_segments(WriteSegmentListRequest(session=session))
        receipt = adapter.complete_write(
            WriteCompleteRequest(
                session=session,
                completion=page.completion,
                expected_bytes=len(content),
                expected_content_type=start.content_type,
                required_identity_assertions=start.required_identity_assertions,
                expected_placement_policy=start.placement_policy,
            )
        )
    ledgers = list(root.glob("objects/**/segments.sqlite3"))
    assert len(ledgers) == 1
    assert list(root.glob("writes/**/state.json")) == []
    # A restart rebuilds the disposable inventory; exact payload reads still work.
    with FilesystemStorageAdapter(config) as restarted:
        request = ObjectReadRequest(
            object=ObjectLocator(object_path=start.object_path, revision=receipt.revision),
            expected_bytes=len(content),
        )
        stream = restarted.read_object(request)
        try:
            assert b"".join(stream.content) == content
        finally:
            stream.close()
        ledgers[0].unlink()
        with pytest.raises(RuntimeError, match="segment ledger is missing"):
            restarted.read_object(request)
