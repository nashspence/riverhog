"""Actual WAL coalescing, SQLite reconstruction and original bounded receiving."""

from __future__ import annotations

import hashlib
import struct
from contextlib import closing, contextmanager
from dataclasses import asdict, replace
from functools import partial

import pytest
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance._generation_capture_index import (
    GenerationCaptureIndex,
    InvalidCaptureIndex,
)
from riverhog_provenance._generation_wal import GenerationWalInspector
from riverhog_provenance._native_epoch import EpochContext
from riverhog_provenance._native_work import NativeWorkCapacity, WorkAmount, WorkQuantum
from riverhog_provenance.errors import JournalValidationCapacity
from sqlite_admission import SqliteConnectionAdmission

from tests.support.growth_batch import SampleBatch


@contextmanager
def database(path):
    admission = SqliteConnectionAdmission(
        connection_limit=1, cursor_limit=2, operation_limit=8, verify_process=lambda: None
    )
    scope = admission.connection_scope()
    try:
        yield scope.connect(path, isolation_level=None, cached_statements=0)
    finally:
        scope.close()
        assert scope.received and admission.status.connections == admission.status.cursors == 0


def scalar(db, query):
    with closing(db.execute(query)) as cursor:
        return cursor.fetchone()[0]


@contextmanager
def actual_interval(directory, width=512, shrink=False):
    path = directory / "working.sqlite3"
    with database(path) as db:
        db.execute(f"PRAGMA page_size={width}").close()
        assert scalar(db, "PRAGMA journal_mode=WAL") == "wal"
        db.execute("PRAGMA synchronous=OFF").close()
        db.execute("PRAGMA wal_autocheckpoint=0").close()
        db.execute("CREATE TABLE actual(value BLOB)").close()
        for _ in range(10):
            db.execute("INSERT INTO actual VALUES(?)", (b"a" * (width // 2),)).close()
        with closing(db.execute("PRAGMA wal_checkpoint(TRUNCATE)")) as cursor:
            assert cursor.fetchone() == (0, 0, 0)
        baseline = path.read_bytes()
        previous = scalar(db, "PRAGMA page_count")
        if shrink:
            db.execute("DELETE FROM actual").close()
            db.execute("VACUUM").close()
            assert scalar(db, "PRAGMA page_count") < previous
        for _ in range(5):
            db.execute("INSERT INTO actual VALUES(?)", (b"b" * (width // 2),)).close()
        current = scalar(db, "PRAGMA page_count")
        raw = path.with_name(path.name + "-wal").read_bytes()
        inspector = GenerationWalInspector(
            page_size=width, wal_octets=len(raw), previous_pages=previous, current_pages=current
        )
        inspector.start(raw[:32])
        yield inspector, raw, baseline, (5 if shrink else 15)


def frames(inspector, raw):
    width = inspector.page_size + 24
    for start in range(32, len(raw), width):
        yield inspector.frame(raw[start : start + width])


@contextmanager
def index_for(directory, inspector, *, limit=1024, metadata=64):
    with database(directory / "capture.sqlite3") as db:
        index = GenerationCaptureIndex(
            EpochContext(db), inspector, entry_limit=limit, metadata_octets=metadata
        )
        yield index


def output(index, limit=3):
    result = []
    while not index.exhausted:
        batch = index.read(page_limit=limit)
        result.extend(batch.pages)
        assert batch.after == (result[-1].page if result else 0)
    return result


@pytest.mark.parametrize("width", [512, 4096, 65536])
@pytest.mark.parametrize("shrink", [False, True])
def test_real_coalesced_wal_restores_original_sqlite_after_intermediate_shrink_and_regrowth(
    tmp_path, width, shrink
):
    with actual_interval(tmp_path, width, shrink) as (inspector, raw, baseline, expected):
        with index_for(tmp_path, inspector) as index:
            index.initialize()
            for frame in frames(inspector, raw):
                index.record_frame(frame)
                while not index.remove(page_limit=2):
                    pass
            summary = index.finish()
            rows = output(index)
            assert summary.frames == index.work.frame_visits
            assert bool(index.work.tombstones) is shrink
            assert index.work.page_payload_octets == 0
            pages = {
                number + 1: baseline[start : start + width]
                for number, start in enumerate(range(0, len(baseline), width))
            }
            for row in rows:
                if row.offset is None:
                    assert row.sha256 is None
                    pages.pop(row.page, None)
                else:
                    image = raw[row.offset : row.offset + width]
                    assert len(image) == width and hashlib.sha256(image).digest() == row.sha256
                    pages[row.page] = image
            assert set(pages) == set(range(1, summary.page_count + 1))
            restored = tmp_path / "restored.sqlite3"
            with restored.open("xb") as out:
                for page in range(1, summary.page_count + 1):
                    out.write(pages[page])
        with database(restored) as db:
            assert scalar(db, "PRAGMA integrity_check") == "ok"
            assert scalar(db, "SELECT count(*) FROM actual") == expected
            assert (
                scalar(
                    db,
                    "SELECT count(*) FROM actual WHERE value=x'"
                    + (b"b" * (width // 2)).hex()
                    + "'",
                )
                == 5
            )


def synthetic_shrink(previous=(1 << 32) - 2):
    # An independently encoded one-frame interval tests a huge removal extent
    # without allocating or pretending to qualify a huge physical database.
    def checksum(data, state=(0, 0)):
        a, b = state
        for offset in range(0, len(data), 8):
            x = int.from_bytes(data[offset : offset + 4], "little")
            y = int.from_bytes(data[offset + 4 : offset + 8], "little")
            a = (a + x + b) % (1 << 32)
            b = (b + y + a) % (1 << 32)
        return a, b

    header = struct.pack(">6I", 0x377F0682, 3007000, 512, 0, 17, 29)
    state = checksum(header)
    raw = header + struct.pack(">II", *state)
    prefix, data = struct.pack(">II", 1, 2), bytes(512)
    state = checksum(data, checksum(prefix, state))
    raw += struct.pack(">6I", 1, 2, 17, 29, *state) + data
    inspector = GenerationWalInspector(
        page_size=512, wal_octets=len(raw), previous_pages=previous, current_pages=2
    )
    inspector.start(raw[:32])
    return inspector, raw


def test_huge_shrink_frame_only_records_a_cursor_and_each_removal_receives_its_limit(tmp_path):
    inspector, raw = synthetic_shrink()
    with index_for(tmp_path, inspector) as index:
        index.initialize()
        index.record_frame(inspector.frame(raw[32:]))
        assert index.work.sql_calls == 3 and index.work.tombstones == 0
        assert not index.remove(page_limit=3)
        assert index.work.tombstones == 3
        assert scalar(index.epoch.db, "SELECT count(*) FROM capture_pages") == 4
        assert index._state.remove_after == 5
        with pytest.raises(InvalidCaptureIndex, match="unreceived"):
            index.finish()


def test_partial_capacity_refusal_rolls_back_the_original_removal_and_preserves_its_failure(
    tmp_path,
):
    inspector, raw = synthetic_shrink()
    with index_for(tmp_path, inspector, limit=2) as index:
        index.initialize()
        index.record_frame(inspector.frame(raw[32:]))
        before = index._state
        with pytest.raises(JournalValidationCapacity) as caught:
            index.remove(page_limit=3)
        assert scalar(index.epoch.db, "SELECT count(*) FROM capture_pages") == 1
        assert index._state is before
        with pytest.raises(RuntimeError) as repeated:
            index.remove(page_limit=1)
        assert repeated.value.__cause__ is caught.value


@pytest.mark.parametrize("fault", ["copy", "skip"])
def test_lookalike_or_skipped_original_frames_never_enter_capture(tmp_path, fault):
    with actual_interval(tmp_path) as (inspector, raw, _, _):
        with index_for(tmp_path, inspector) as index:
            index.initialize()
            iterator = frames(inspector, raw)
            frame = next(iterator)
            frame = replace(frame) if fault == "copy" else next(iterator)
            with pytest.raises(InvalidCaptureIndex, match="original verified"):
                index.record_frame(frame)
            assert scalar(index.epoch.db, "SELECT count(*) FROM capture_pages") == 0


def test_original_post_sql_check_failure_rolls_back_without_receiving_the_frame(tmp_path):
    inspector, raw = synthetic_shrink(previous=2)
    with index_for(tmp_path, inspector) as index:
        index.initialize()
        before = index._state
        original = KeyboardInterrupt("original capture execution cancellation")
        inserted = []
        index.epoch.db.set_trace_callback(
            lambda sql: inserted.append(sql) if sql.startswith("INSERT") else None
        )

        def check():
            if inserted:
                raise original

        with pytest.raises(KeyboardInterrupt) as caught, index.epoch.execution(check):
            index.record_frame(inspector.frame(raw[32:]))
        assert caught.value is original and index._state is before
        assert scalar(index.epoch.db, "SELECT count(*) FROM capture_pages") == 0
        index.epoch.db.set_trace_callback(None)


@pytest.mark.parametrize("corruption", ["missing", "offset", "digest", "foreign"])
def test_original_output_detects_missing_extra_or_bounded_corrupt_index_rows(tmp_path, corruption):
    inspector, raw = synthetic_shrink(previous=2)
    with index_for(tmp_path, inspector) as index:
        index.initialize()
        index.record_frame(inspector.frame(raw[32:]))
        index.finish()
        commands = {
            "missing": "DELETE FROM capture_pages",
            "offset": "UPDATE capture_pages SET frame_offset='" + "界" * 10000 + "'",
            "digest": "UPDATE capture_pages SET digest=zeroblob(10000)",
            "foreign": "INSERT INTO capture_pages VALUES(2,NULL,NULL)",
        }
        index.epoch.db.execute(commands[corruption]).close()
        with pytest.raises((InvalidCaptureIndex, ValueError, UnicodeError)):
            index.read(page_limit=3)
        assert not index.exhausted and index.work.loaded_octets <= 2 * (index.metadata_octets + 34)


def sample(batch, index, operation, action, units=1):
    before = asdict(index.work)
    action()
    return batch.sample(
        operation,
        {
            "units": units,
            "metadata_octets": index.metadata_octets,
            "previous_pages": index.inspector.previous_pages,
        },
        {k: v - before[k] for k, v in asdict(index.work).items()},
    )


@pytest.mark.growth_witness(
    "riverhog-native-generation-capture-index-initialize/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._generation_capture_index.GenerationCaptureIndex.initialize",
)
def test_capture_initialization_has_only_fixed_schema_work(
    tmp_path, numeric_policy, record_property
):
    operation = GenerationCaptureIndex.initialize
    samples = []
    with SampleBatch((operation,), limit=8, numeric_policy=numeric_policy) as batch:
        for count in (2, 1000, (1 << 32) - 2):
            directory = tmp_path / str(count)
            directory.mkdir()
            inspector, _ = synthetic_shrink(previous=count)
            with index_for(directory, inspector) as index:
                samples.append(sample(batch, index, operation, index.initialize))
    record_property("growth_samples", samples)
    assert len(samples) == 3


def capture_witness(tmp_path, numeric_policy, mode):
    operation = getattr(GenerationCaptureIndex, mode)
    samples = []
    with SampleBatch((operation,), limit=32, numeric_policy=numeric_policy) as batch:
        for width in (512, 4096, 65536):
            directory = tmp_path / str(width)
            directory.mkdir()
            with actual_interval(directory, width, True) as (inspector, raw, _, _):
                with index_for(directory, inspector) as index:
                    queries = []
                    index.epoch.db.set_trace_callback(queries.append)
                    index.initialize()
                    for frame in frames(inspector, raw):
                        action = partial(index.record_frame, frame)
                        if mode == "record_frame":
                            samples.append(sample(batch, index, operation, action))
                        else:
                            action()
                        done = False
                        while not done:

                            def remove(index=index):
                                nonlocal done
                                done = index.remove(page_limit=2)

                            if mode == "remove":
                                samples.append(sample(batch, index, operation, remove, 2))
                            else:
                                remove()
                    if mode == "finish":
                        samples.append(sample(batch, index, operation, index.finish))
                    else:
                        index.finish()
                    while not index.exhausted:
                        action = partial(index.read, page_limit=3)
                        if mode == "read":
                            samples.append(sample(batch, index, operation, action, 3))
                        else:
                            action()
                    actual = sum(query not in {"BEGIN IMMEDIATE", "COMMIT"} for query in queries)
                    assert index.work.sql_calls == actual
                    index.epoch.db.set_trace_callback(None)
    return samples


def distant_shrink_samples(tmp_path, numeric_policy, mode):
    operation = getattr(GenerationCaptureIndex, mode)
    samples = []
    with SampleBatch((operation,), limit=8, numeric_policy=numeric_policy) as batch:
        for previous in (2, 1000, (1 << 32) - 2):
            directory = tmp_path / f"distant-{previous}"
            directory.mkdir()
            inspector, raw = synthetic_shrink(previous=previous)
            with index_for(directory, inspector) as index:
                index.initialize()
                frame = inspector.frame(raw[32:])
                if mode == "record_frame":
                    samples.append(
                        sample(batch, index, operation, partial(index.record_frame, frame))
                    )
                else:
                    index.record_frame(frame)
                    samples.append(
                        sample(batch, index, operation, partial(index.remove, page_limit=3), 3)
                    )
    return samples


@pytest.mark.growth_witness(
    "riverhog-native-generation-capture-index-frame/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._generation_capture_index.GenerationCaptureIndex.record_frame",
)
def test_capture_frame_work_has_no_shrink_extent_factor(tmp_path, numeric_policy, record_property):
    samples = capture_witness(tmp_path, numeric_policy, "record_frame")
    samples.extend(distant_shrink_samples(tmp_path, numeric_policy, "record_frame"))
    record_property("growth_samples", samples)
    assert len(samples) > 3


@pytest.mark.growth_witness(
    "riverhog-native-generation-capture-index-removal/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._generation_capture_index.GenerationCaptureIndex.remove",
)
def test_capture_shrink_work_follows_original_removal_limits(
    tmp_path, numeric_policy, record_property
):
    samples = capture_witness(tmp_path, numeric_policy, "remove")
    samples.extend(distant_shrink_samples(tmp_path, numeric_policy, "remove"))
    record_property("growth_samples", samples)
    assert any(row["metrics"]["tombstones"] == 2 for row in samples)


@pytest.mark.growth_witness(
    "riverhog-native-generation-capture-index-finish/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._generation_capture_index.GenerationCaptureIndex.finish",
)
def test_capture_final_receiving_never_rescans_the_original_interval(
    tmp_path, numeric_policy, record_property
):
    samples = capture_witness(tmp_path, numeric_policy, "finish")
    record_property("growth_samples", samples)
    assert len(samples) == 3


@pytest.mark.growth_witness(
    "riverhog-native-generation-capture-index-read/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._generation_capture_index.GenerationCaptureIndex.read",
)
def test_capture_output_work_follows_returned_pages_without_revisiting_its_prefix(
    tmp_path, numeric_policy, record_property
):
    samples = capture_witness(tmp_path, numeric_policy, "read")
    record_property("growth_samples", samples)
    assert len(samples) > 3


def test_capture_growth_rejects_unaccounted_full_page_payload_storage(tmp_path, numeric_policy):
    operation = GenerationCaptureIndex.record_frame
    inspector, raw = synthetic_shrink(previous=2)
    with index_for(tmp_path, inspector) as index:
        index.initialize()
        frame = inspector.frame(raw[32:])
        with SampleBatch((operation,), limit=1, numeric_policy=numeric_policy) as batch:
            row = sample(batch, index, operation, lambda: index.record_frame(frame))
    row = {**row, "metrics": {**row["metrics"], "page_payload_octets": 1}}
    binding = growth_binding(operation)
    projection = project_bindings((binding,), numeric_policy=numeric_policy)
    with pytest.raises(ValueError, match="actual primitive observations violate"):
        validate_execution_samples(
            projection, binding.declaration.id, [row], numeric_policy=numeric_policy
        )


def test_original_natural_page_limit_does_not_narrow_into_sqlites_signed_carrier(tmp_path):
    inspector, raw = synthetic_shrink(previous=2)
    with index_for(tmp_path, inspector) as index:
        index.initialize()
        index.record_frame(inspector.frame(raw[32:]))
        index.finish()
        row = index.read(page_limit=1 << 100)
        assert len(row.pages) == 1 and row.pages[0].page == 1 and not row.more
        assert index.exhausted


def test_index_sql_and_metadata_debit_the_same_original_work_quantum(tmp_path):
    inspector, raw = synthetic_shrink(previous=2)
    with index_for(tmp_path, inspector) as index:
        quantum = WorkQuantum(WorkAmount(rows=100, octets=10000, source_octets=1, transitions=1))
        with index.epoch.execution(None, work=quantum):
            index.initialize()
            index.record_frame(inspector.frame(raw[32:]))
            assert index.remove(page_limit=1)
            index.finish()
            index.read(page_limit=1)
        assert quantum.consumed.rows == index.work.sql_calls
        assert quantum.consumed.octets == index.work.stored_octets + index.work.loaded_octets
        assert quantum.held == WorkAmount()
        quantum.require_received()


def test_original_pre_writer_reservation_refusal_opens_no_partial_capture_writer(tmp_path):
    inspector, raw = synthetic_shrink(previous=2)
    with index_for(tmp_path, inspector) as index:
        index.initialize()
        frame = inspector.frame(raw[32:])
        quantum = WorkQuantum(WorkAmount(rows=1, octets=10000, source_octets=1, transitions=1))
        queries = []
        index.epoch.db.set_trace_callback(queries.append)
        with pytest.raises(NativeWorkCapacity) as caught, index.epoch.execution(None, work=quantum):
            index.record_frame(frame)
        index.epoch.db.set_trace_callback(None)
        assert quantum.owns_admission_refusal(caught.value)
        assert not queries and index.epoch.quiescent and index._state.frames == 0
        assert quantum.held == WorkAmount()
        quantum.require_received()
