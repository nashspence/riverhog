"""Real SQLite intervals, original checksum work and bounded failure transitions."""

from __future__ import annotations

import hashlib
import struct
from contextlib import closing, contextmanager
from dataclasses import asdict
from types import SimpleNamespace

import pytest
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import _generation_wal as wal_owner
from riverhog_provenance._generation_wal import GenerationWalInspector, InvalidGenerationWal
from sqlite_admission import SqliteConnectionAdmission

from tests.support.growth_batch import SampleBatch


def scalar(db, query):
    with closing(db.execute(query)) as cursor:
        return cursor.fetchone()[0]


@contextmanager
def actual_wal(tmp_path, *, width=512, transactions=4):
    admission = SqliteConnectionAdmission(
        connection_limit=1, cursor_limit=1, operation_limit=4, verify_process=lambda: None
    )
    original = admission.connection_scope()
    path = tmp_path / "working.sqlite3"
    db = original.connect(path, cached_statements=0)
    try:
        db.execute(f"PRAGMA page_size={width}").close()
        assert scalar(db, "PRAGMA journal_mode=WAL") == "wal"
        db.execute("PRAGMA synchronous=OFF").close()
        db.execute("PRAGMA wal_autocheckpoint=0").close()
        db.execute("CREATE TABLE actual(value BLOB)").close()
        db.commit()
        with closing(db.execute("PRAGMA wal_checkpoint(TRUNCATE)")) as cursor:
            assert cursor.fetchone() == (0, 0, 0)
        previous = scalar(db, "PRAGMA page_count")
        for number in range(transactions):
            db.execute(
                "INSERT INTO actual VALUES(?)", (bytes([number + 1]) * (width // 2),)
            ).close()
            db.commit()
        current = scalar(db, "PRAGMA page_count")
        raw = path.with_name(path.name + "-wal").read_bytes()
        yield raw, previous, current
    finally:
        original.close()
        assert original.received and admission.status.connections == 0
        assert admission.status.cursors == 0


def owner(raw, previous, current, *, width=512, check=None):
    return GenerationWalInspector(
        page_size=width,
        wal_octets=len(raw),
        previous_pages=previous,
        current_pages=current,
        check=check,
    )


def packets(raw, width):
    return [raw[offset : offset + width + 24] for offset in range(32, len(raw), width + 24)]


def inspect(validator, raw):
    validator.start(raw[:32])
    results = [validator.frame(packet) for packet in packets(raw, validator.page_size)]
    return results, validator.finish()


@pytest.mark.parametrize("width", [512, 4096, 65536])
@pytest.mark.parametrize("transactions", [1, 4])
def test_original_real_sqlite_wal_interval_preserves_exact_frames_and_commits(
    tmp_path, width, transactions
):
    with actual_wal(tmp_path, width=width, transactions=transactions) as (raw, previous, current):
        validator = owner(raw, previous, current, width=width)
        frames, summary = inspect(validator, raw)
        assert summary.frames == len(frames) == (len(raw) - 32) // (24 + width)
        assert summary.commit_markers == transactions
        assert summary.page_count == current
        assert summary.wal_octets == len(raw)
        assert summary.salts == struct.unpack_from(">II", raw, 16)
        for index, frame in enumerate(frames):
            assert frame.ordinal == index + 1
            assert frame.data_offset == 32 + (24 + width) * index + 24
            assert frame.data == raw[frame.data_offset : frame.data_offset + width]
            assert frame.sha256 == hashlib.sha256(frame.data).digest()
        assert frames[-1].committed_pages == current
        assert validator.work.parsed_octets == len(raw)
        assert validator.work.checksum_pairs == 3 + len(frames) * (1 + width // 8)


def checksum(data, order, state=(0, 0)):
    # Fixture construction uses integer byte decoding rather than the production
    # iterator. Real SQLite-generated intervals supply the independent format proof.
    left, right = state
    for offset in range(0, len(data), 8):
        x = int.from_bytes(data[offset : offset + 4], order)
        y = int.from_bytes(data[offset + 4 : offset + 8], order)
        left = (left + x + right) % (1 << 32)
        right = (right + y + left) % (1 << 32)
    return left, right


def synthetic(*, width=512, order="little", records=((1, 2),)):
    magic = 0x377F0682 if order == "little" else 0x377F0683
    header = struct.pack(">6I", magic, 3007000, width, 0, 17, 29)
    state = checksum(header, order)
    raw = header + struct.pack(">II", *state)
    for number, commit in records:
        data = bytes([number % 256]) * width
        prefix = struct.pack(">II", number, commit)
        state = checksum(data, order, checksum(prefix, order, state))
        raw += struct.pack(">6I", number, commit, 17, 29, *state) + data
    return raw


@pytest.mark.parametrize("order", ["little", "big"])
def test_both_documented_checksum_orders_and_intermediate_shrink_metadata(order):
    raw = synthetic(order=order, records=((1, 2), (1, 1), (2, 2)))
    frames, summary = inspect(owner(raw, 2, 2), raw)
    assert [(frame.previous_pages, frame.committed_pages) for frame in frames] == [
        (2, 2),
        (2, 1),
        (1, 2),
    ]
    assert summary.commit_markers == 3


@pytest.mark.parametrize("field", [0, 4, 8, 16, 24])
def test_changed_header_magic_version_width_salt_or_checksum_refuses(field):
    raw = synthetic()
    header = bytearray(raw[:32])
    header[field] ^= 1
    validator = owner(raw, 2, 2)
    with pytest.raises(InvalidGenerationWal):
        validator.start(bytes(header))
    assert validator._offset == 0
    with pytest.raises(RuntimeError, match="requires reconstruction"):
        validator.start(raw[:32])


@pytest.mark.parametrize("field", [0, 4, 8, 12, 16, 24, 535])
def test_invalid_or_changed_original_frame_never_advances_its_cursor(field):
    raw = synthetic()
    packet = bytearray(raw[32:])
    if field in {0, 4}:
        packet[field : field + 4] = bytes.fromhex("ffffffff")
    else:
        packet[field] ^= 1
    validator = owner(raw, 2, 2)
    validator.start(raw[:32])
    with pytest.raises(InvalidGenerationWal):
        validator.frame(bytes(packet))
    assert validator._offset == 32 and validator._frames == 0
    with pytest.raises(RuntimeError, match="requires reconstruction"):
        validator.frame(raw[32:])


@pytest.mark.parametrize("tail", [-1, 1])
def test_incomplete_original_header_or_trailing_frame_extent_refuses_before_scanning(tail):
    raw = synthetic()
    validator = GenerationWalInspector(
        page_size=512, wal_octets=len(raw) + tail, previous_pages=2, current_pages=2
    )
    with pytest.raises(InvalidGenerationWal, match="incomplete"):
        validator.start(raw[:32])


@pytest.mark.parametrize("tail", [-1, 1])
def test_partial_or_oversized_packet_cannot_advance_an_exact_frozen_interval(tail):
    raw = synthetic()
    validator = owner(raw, 2, 2)
    validator.start(raw[:32])
    packet = raw[32:-1] if tail < 0 else raw[32:] + b"x"
    with pytest.raises(InvalidGenerationWal, match="incomplete or exceeds"):
        validator.frame(packet)
    assert validator._offset == 32


@pytest.mark.parametrize("records", [(), ((1, 0),), ((1, 2), (2, 0))])
def test_header_only_or_uncommitted_tail_cannot_be_received_as_a_complete_generation_interval(
    records,
):
    raw = synthetic(records=records)
    validator = owner(raw, 2, 2)
    validator.start(raw[:32])
    for packet in packets(raw, 512):
        validator.frame(packet)
    with pytest.raises(InvalidGenerationWal, match="no final commit|uncommitted tail"):
        validator.finish()


def test_unexhausted_interval_or_wrong_final_frozen_page_count_refuses():
    raw = synthetic(records=((1, 2), (2, 3)))
    validator = owner(raw, 2, 3)
    validator.start(raw[:32])
    validator.frame(packets(raw, 512)[0])
    with pytest.raises(InvalidGenerationWal, match="not exhausted"):
        validator.finish()
    validator = owner(raw, 2, 4)
    validator.start(raw[:32])
    for packet in packets(raw, 512):
        validator.frame(packet)
    with pytest.raises(InvalidGenerationWal, match="frozen page extent"):
        validator.finish()


def test_empty_interval_requires_unchanged_extent_and_cannot_accept_a_frame():
    validator = owner(b"", 2, 2)
    assert inspect(validator, b"")[1].frames == 0
    assert validator.work.parsed_octets == 0
    with pytest.raises(RuntimeError, match="original transition"):
        validator.finish()
    with pytest.raises(InvalidGenerationWal, match="empty WAL"):
        owner(b"", 2, 3).start(b"")
    validator = owner(b"", 2, 2)
    validator.start(b"")
    with pytest.raises(InvalidGenerationWal, match="exceeds"):
        validator.frame(bytes(536))


@pytest.mark.parametrize("mode", ["start", "frame", "finish"])
def test_original_final_check_failure_preserves_its_cause_and_unreceived_transition(mode):
    raw = synthetic()
    original = KeyboardInterrupt("original frozen capture cancellation")
    calls = []
    active = [False]

    def check():
        calls.append(1)
        if active[0] and len(calls) % 2 == 0:
            raise original

    validator = owner(raw, 2, 2, check=check)
    if mode != "start":
        validator.start(raw[:32])
    if mode == "finish":
        validator.frame(raw[32:])
    before = validator._offset, validator._frames, validator._phase
    active[0] = True
    with pytest.raises(KeyboardInterrupt) as caught:
        {
            "start": lambda: validator.start(raw[:32]),
            "frame": lambda: validator.frame(raw[32:]),
            "finish": validator.finish,
        }[mode]()
    assert caught.value is original
    assert (validator._offset, validator._frames) == before[:2]
    with pytest.raises(RuntimeError) as reused:
        validator.finish()
    assert reused.value.__cause__ is original


def test_independent_iterator_arithmetic_and_hash_calls_match_reported_work(monkeypatch):
    raw = synthetic(records=((1, 2), (2, 2)))
    observed = {"pairs": 0, "octets": 0, "additions": 0, "masks": 0, "hashes": 0}
    actual_unpack, actual_hash = struct.iter_unpack, hashlib.sha256

    class Word(int):
        def __add__(self, other):
            observed["additions"] += 1
            return Word(int(self) + int(other))

        def __radd__(self, other):
            observed["additions"] += 1
            return Word(int(other) + int(self))

        def __and__(self, other):
            observed["masks"] += 1
            return Word(int(self) & int(other))

    def unpack(format, data):
        observed["octets"] += len(data)
        for left, right in actual_unpack(format, data):
            observed["pairs"] += 1
            yield Word(left), Word(right)

    def digest(data):
        observed["hashes"] += len(data)
        return actual_hash(data)

    monkeypatch.setattr(wal_owner, "struct", SimpleNamespace(iter_unpack=unpack))
    monkeypatch.setattr(wal_owner, "hashlib", SimpleNamespace(sha256=digest))
    validator = owner(raw, 2, 2)
    inspect(validator, raw)
    assert validator.work.checksum_pairs == observed["pairs"] == 3 + 2 * 65
    assert validator.work.checksum_octets == observed["octets"]
    assert validator.work.checksum_additions == observed["additions"]
    assert validator.work.checksum_masks == observed["masks"]
    assert validator.work.hashed_octets == observed["hashes"] == 1024


def sample(batch, validator, operation, action):
    before = asdict(validator.work)
    action()
    return batch.sample(
        operation,
        {"page_size": validator.page_size, "page_pairs": validator.page_size // 8},
        {k: v - before[k] for k, v in asdict(validator.work).items()},
    )


def failed_original_check_sample(batch, operation, mode, width):
    raw = synthetic(width=width)
    original = RuntimeError("original frozen interval check")
    active, checks = [False], []

    def check():
        if active[0]:
            checks.append(1)
            if len(checks) == 2:
                raise original

    validator = owner(raw, 2, 2, width=width, check=check)
    if mode != "start":
        validator.start(raw[:32])
    if mode == "finish":
        validator.frame(raw[32:])
    active[0] = True

    def fail():
        with pytest.raises(RuntimeError) as caught:
            if mode == "start":
                validator.start(raw[:32])
            elif mode == "frame":
                validator.frame(raw[32:])
            else:
                validator.finish()
        assert caught.value is original

    result = sample(batch, validator, operation, fail)
    assert result["metrics"]["check_calls"] == len(checks) == 2
    return result


@pytest.mark.growth_witness(
    "riverhog-native-generation-wal-header-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._generation_wal.GenerationWalInspector.start",
)
def test_actual_wal_header_work_at_independent_page_and_interval_extents(
    tmp_path, numeric_policy, record_property
):
    operation = GenerationWalInspector.start
    samples = []
    with SampleBatch((operation,), limit=8, numeric_policy=numeric_policy) as batch:
        for width in (512, 4096, 65536):
            for count in (1, 4):
                directory = tmp_path / f"{width}-{count}"
                directory.mkdir()
                with actual_wal(directory, width=width, transactions=count) as (
                    raw,
                    previous,
                    current,
                ):
                    validator = owner(raw, previous, current, width=width)
                    samples.append(
                        sample(
                            batch,
                            validator,
                            operation,
                            lambda validator=validator, raw=raw: validator.start(raw[:32]),
                        )
                    )
            validator = owner(b"", 2, 2, width=width)
            samples.append(
                sample(
                    batch, validator, operation, lambda validator=validator: validator.start(b"")
                )
            )
            samples.append(failed_original_check_sample(batch, operation, "start", width))
    record_property("growth_samples", samples)
    assert len(samples) == 12


@pytest.mark.growth_witness(
    "riverhog-native-generation-wal-frame-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._generation_wal.GenerationWalInspector.frame",
)
def test_actual_wal_frame_work_does_not_visit_the_prior_interval(
    tmp_path, numeric_policy, record_property
):
    operation = GenerationWalInspector.frame
    samples = []
    with SampleBatch((operation,), limit=32, numeric_policy=numeric_policy) as batch:
        for width in (512, 4096, 65536):
            directory = tmp_path / str(width)
            directory.mkdir()
            with actual_wal(directory, width=width, transactions=4) as (raw, previous, current):
                validator = owner(raw, previous, current, width=width)
                validator.start(raw[:32])
                for packet in packets(raw, width):
                    samples.append(
                        sample(
                            batch,
                            validator,
                            operation,
                            lambda validator=validator, packet=packet: validator.frame(packet),
                        )
                    )
                validator.finish()
            samples.append(failed_original_check_sample(batch, operation, "frame", width))
    record_property("growth_samples", samples)
    assert len(samples) >= 15


@pytest.mark.growth_witness(
    "riverhog-native-generation-wal-finish-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._generation_wal.GenerationWalInspector.finish",
)
def test_actual_wal_interval_receiving_has_fixed_final_work(
    tmp_path, numeric_policy, record_property
):
    operation = GenerationWalInspector.finish
    samples = []
    with SampleBatch((operation,), limit=8, numeric_policy=numeric_policy) as batch:
        for width in (512, 4096, 65536):
            for count in (1, 4):
                directory = tmp_path / f"{width}-{count}"
                directory.mkdir()
                with actual_wal(directory, width=width, transactions=count) as (
                    raw,
                    previous,
                    current,
                ):
                    validator = owner(raw, previous, current, width=width)
                    validator.start(raw[:32])
                    for packet in packets(raw, width):
                        validator.frame(packet)
                    samples.append(sample(batch, validator, operation, validator.finish))
            validator = owner(b"", 2, 2, width=width)
            validator.start(b"")
            samples.append(sample(batch, validator, operation, validator.finish))
            samples.append(failed_original_check_sample(batch, operation, "finish", width))
    record_property("growth_samples", samples)
    assert len(samples) == 12


@pytest.mark.parametrize(
    "metric", ["checksum_pairs", "checksum_additions", "checksum_masks", "hashed_octets"]
)
def test_independent_growth_ceiling_rejects_extra_original_checksum_or_hash_work(
    metric, numeric_policy
):
    raw = synthetic()
    validator = owner(raw, 2, 2)
    validator.start(raw[:32])
    operation = GenerationWalInspector.frame
    with SampleBatch((operation,), limit=1, numeric_policy=numeric_policy) as batch:
        row = sample(batch, validator, operation, lambda: validator.frame(raw[32:]))
    invalid = {**row, "metrics": {**row["metrics"], metric: row["metrics"][metric] + 1}}
    projection = project_bindings((growth_binding(operation),), numeric_policy=numeric_policy)
    with pytest.raises(ValueError, match="actual primitive observations violate"):
        validate_execution_samples(
            projection,
            growth_binding(operation).declaration.id,
            [invalid],
            numeric_policy=numeric_policy,
        )
