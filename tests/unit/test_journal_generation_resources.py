"""Actual upload-slot custody through separate Native generation originals."""

from __future__ import annotations

import pytest
import retained_resource
from riverhog_provenance import JournalImageRestore, NativeGenerationStorage
from riverhog_provenance import _generation_storage as storage_owner
from riverhog_provenance import _journal_image as image_owner

from tests.support.retained_resource import fault_native as fault_native
from tests.support.validation_workspace import leased_workspace
from tests.unit.test_archive_write_admission import _guard
from tests.unit.test_journal_recovery_frontier import admitted_origin
from tests.unit.test_journal_recovery_frontier import origin as origin
from tests.unit.test_upload_journal_validation_release_growth import DROP, Lock, Pool, Recorder


def open_storage(case):
    storage = NativeGenerationStorage.open(
        case.directory,
        construction=case.generation_construction,
        page_size=4096,
        reader_limit=2,
        metadata_octets=131072,
        create=True,
        on_storage_open=case.generation.retain_storage,
        on_storage_close=case.slot.receive_generation,
    )
    case.storage = storage
    return storage


def begin_capture(case):
    original = case.slot.session.image_capture()
    case.generation.retain_capture(original)
    original.begin(
        case.storage,
        case.grant.identity,
        image_octets=16 * 1024**2,
        owner_context=case.context.encoded,
    )
    return original


def measure_slot(recorder, case, *, pending=False):
    key = case.work.collection_id, case.work.journal_id, case.work.attempt_id
    with case.slot.lock:
        return recorder.measure(case.owner, DROP, key, case.slot, pending=pending)


def assert_busy(case):
    with leased_workspace(case.owner.root, case.job.workspace_id) as busy:
        assert busy is None


def assert_received(case):
    assert case.generation.received and case.slot.construction.received
    assert not case.owner._slots
    with leased_workspace(case.owner.root, case.job.workspace_id) as free:
        assert free == case.directory
    # Resource closure cannot settle the surviving grant or retire its data.
    assert case.recovery.snapshot(case.binding, guard=_guard) == case.reserved


def test_passive_generation_child_is_retained_without_optional_effects(
    tmp_path, journal_constructions
):
    with admitted_origin(tmp_path, journal_constructions) as case:
        assert case.slot.generation is case.generation
        assert case.generation.construction is not case.slot.construction
        assert case.generation.storage is case.generation.capture is case.generation.restore is None
        assert not (case.directory / "generations.sqlite3").exists()
        assert case.generation.construction.creator.metrics["open_calls"] == 0
        case.owner.close()
        assert_received(case)


def test_partial_generation_open_keeps_the_same_early_original(
    tmp_path, journal_constructions, monkeypatch
):
    with admitted_origin(tmp_path, journal_constructions) as case:
        cause = RuntimeError("original generation configuration failed")

        def fail(*args, **kwargs):
            assert case.slot.generation is case.generation
            assert case.generation.storage is case.generation.construction.storage
            raise cause

        monkeypatch.setattr(storage_owner, "configure_sqlite_connection", fail)
        with pytest.raises(RuntimeError) as caught:
            open_storage(case)
        assert caught.value is cause
        assert case.generation.storage is not None
        assert not case.generation.received
        assert_busy(case)
        case.owner.close()
        assert_received(case)


@pytest.mark.parametrize("fault", ["before_close", "after_close"])
def test_capture_close_failure_keeps_original_children_and_workspace_custody(
    tmp_path, journal_constructions, monkeypatch, fault_native, fault
):
    module, _path = fault_native
    monkeypatch.setattr(retained_resource, "Resource", module.Resource)
    monkeypatch.setattr(image_owner, "Borrow", module.Borrow)
    with admitted_origin(tmp_path, journal_constructions) as case:
        open_storage(case)
        capture = begin_capture(case)
        capture.file._test_fault(fault)
        with pytest.raises(RuntimeError):
            case.owner.close()
        assert case.slot.generation is case.generation
        assert case.generation.capture is capture and not capture.received
        assert not case.generation.received
        assert_busy(case)
        assert case.recovery.snapshot(case.binding, guard=_guard) == case.reserved
        case.owner.close()
        assert_received(case)


def test_last_generation_reader_keeps_the_original_closing_slot_and_lease(origin):
    case = origin
    reader = case.storage.roots.borrow(case.grant.identity, expected_root=case.frontier.image.root)
    try:
        with pytest.raises(BlockingIOError, match="generation readers"):
            case.owner.close()
        assert case.slot.session is None and case.slot.construction.received
        assert not case.generation.received and not case.storage.storage_received
        assert case.owner._slots and case.slot.generation is case.generation
        assert_busy(case)
        assert reader.read(after=0, page_limit=1).pages
    finally:
        reader.close()
    case.owner.close()
    assert_received(case)


def test_last_working_reader_keeps_lease_after_generation_storage_receives(origin):
    case = origin
    case.capture.close()
    current = case.slot.session
    for _ in range(10000):
        if current.advance().complete:
            break
    else:
        pytest.fail("finite original validation did not complete")
    reader = current.result()
    try:
        with pytest.raises(BlockingIOError, match="original Native construction"):
            case.owner.close()
        assert case.generation.received and case.storage.storage_received
        assert case.slot.session is current and not case.slot.construction.received
        assert_busy(case)
        assert reader.anchor
    finally:
        reader.close()
    case.owner.close()
    assert_received(case)


def test_original_restore_files_and_reader_receive_before_workspace_release(origin):
    case = origin
    restore = JournalImageRestore(case.storage, case.frontier.image)
    case.generation.retain_restore(restore)
    destination = case.directory / "restored"
    destination.mkdir(mode=0o700)
    restore.begin(destination, image_octets=16 * 1024**2)
    assert not restore.advance(page_limit=1)
    assert restore.reader is not None and not restore.reader.closed
    assert_busy(case)
    case.owner.close()
    assert restore.received and restore.reader.closed
    assert_received(case)


def test_storage_callback_failure_preserves_the_identical_receiving_frame(origin):
    case = origin
    receive = case.storage._storage.on_close
    observed = []
    cause = InterruptedError("original generation receiving reply lost")

    def callback():
        observed.append(case.generation)
        if len(observed) == 1:
            raise cause
        receive()

    case.storage._storage.on_close = callback
    with pytest.raises(InterruptedError) as caught:
        case.owner.close()
    assert caught.value is cause
    assert not case.generation.received and case.slot.generation is case.generation
    assert case.generation.construction.dependents_received
    assert_busy(case)
    case.owner.close()
    assert observed == [case.generation, case.generation]
    assert_received(case)


def test_generation_admission_never_replaces_or_relabels_an_original(origin, journal_constructions):
    case = origin
    with pytest.raises(RuntimeError, match="cannot replace"):
        case.slot.retain_generation(journal_constructions.retain(), case.context)
    with pytest.raises(RuntimeError, match="passive working capture"):
        case.generation.retain_capture(case.capture)
    unrelated = case.slot.session.image_capture()
    try:
        with pytest.raises(RuntimeError, match="same original Native storage"):
            case.generation.retain_storage(case.storage)
        case.owner.close()
        with pytest.raises(RuntimeError, match="passive working capture"):
            case.generation.retain_capture(unrelated)
        assert_received(case)
    finally:
        unrelated.close()


@pytest.mark.growth_witness(
    "riverhog-generation-resource-receiving-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.journal_generation_resources.JournalGenerationResources.close_step",
)
def test_actual_slot_receiving_composes_generation_and_working_resources(
    tmp_path, record_property, journal_constructions
):
    samples = []
    record_property("growth_samples", samples)
    record_property("generation_row_conditions", [0, 128])
    for unrelated in (0, 128):
        for state in ("passive", "capturing", "captured", "pinned", "restoring", "working-pinned"):
            base = tmp_path / f"{unrelated}-{state}"
            base.mkdir()
            recorder = Recorder()
            with admitted_origin(base, journal_constructions, observe=recorder.kernel) as case:
                case.owner._lock = Lock(case.owner._lock, recorder, pool=True)
                original_slots = case.owner._slots
                case.owner._slots = Pool(recorder)
                case.owner._slots.update(original_slots)
                case.slot.lock = Lock(case.slot.lock, recorder)
                working_storage = case.slot.session._store._storage
                working_storage.on_close = recorder.callback(working_storage.on_close)
                reader = None
                if state != "passive":
                    storage = open_storage(case)
                    storage._storage.on_close = recorder.callback(
                        storage._storage.on_close, generation=True
                    )
                    storage.db.executemany(
                        "INSERT INTO generation_roots(identity,root,status) "
                        "VALUES(?,NULL,'building')",
                        [("other:" + str(number),) for number in range(unrelated)],
                    ).close()
                    storage.db.commit()
                    capture = begin_capture(case)
                    if state in {"captured", "pinned", "restoring", "working-pinned"}:
                        while capture.image is None:
                            capture.advance(page_limit=4)
                        if state == "pinned":
                            reader = storage.roots.borrow(
                                case.grant.identity, expected_root=capture.image.root
                            )
                        elif state == "restoring":
                            restore = JournalImageRestore(storage, capture.image)
                            case.generation.retain_restore(restore)
                            destination = case.directory / "restored"
                            destination.mkdir(mode=0o700)
                            restore.begin(destination, image_octets=16 * 1024**2)
                            assert not restore.advance(page_limit=1)
                        elif state == "working-pinned":
                            capture.close()
                            for _ in range(10000):
                                if case.slot.session.advance().complete:
                                    break
                            else:
                                pytest.fail("finite original validation did not complete")
                            reader = case.slot.session.result()
                try:
                    samples.append(measure_slot(recorder, case, pending=reader is not None))
                    if reader is not None:
                        assert_busy(case)
                        reader.close()
                        samples.append(measure_slot(recorder, case))
                    assert case.generation.received and not case.owner._slots
                    samples.extend(recorder.delegates["generation"])
                finally:
                    if reader is not None:
                        reader.close()
