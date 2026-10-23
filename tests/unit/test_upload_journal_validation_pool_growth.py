from __future__ import annotations

import builtins
import hashlib
import os
import sqlite3
from contextlib import contextmanager

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.services import upload_journal_validation_owner as owner
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import JournalValidationSession

from tests.unit.test_upload_journal_source_growth import octets

RESERVE, KEYS = (
    owner.UploadJournalValidationOwner._reserve,
    owner.UploadJournalValidationOwner.active_keys,
)
SLOT = owner._Slot


def key(number):
    return number + 1, f"urn:uuid:{number:08x}-0000-4000-8000-000000000000", f"{number:032x}"


class Recorder:
    def __init__(self):
        self.active = False
        self.metrics = {}
        self.pool_held = False

    def add(self, metric, amount=1):
        if self.active:
            self.metrics[metric] += amount


class Lock:
    def __init__(self, actual, recorder, *, pool=False):
        self.actual, self.recorder, self.pool = actual, recorder, pool

    def acquire(self, blocking=True, timeout=-1):
        self.recorder.add("pool_lock_acquisitions" if self.pool else "slot_lock_attempts")
        if not self.pool:
            self.recorder.add("blocking_slot_lock_attempts", int(blocking))
        result = self.actual.acquire(blocking, timeout)
        if self.pool and result:
            assert not self.recorder.pool_held
            self.recorder.pool_held = True
        return result

    def release(self):
        if self.pool:
            assert self.recorder.pool_held
            self.recorder.pool_held = False
            self.recorder.add("pool_lock_releases")
        self.actual.release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *_args):
        self.release()


class Slots(dict):
    def __init__(self, recorder):
        self.recorder = recorder
        super().__init__()

    def get(self, value, default=None):
        self.recorder.add("map_lookups")
        self.recorder.add("map_key_argument_octets", octets(value))
        return super().get(value, default)

    def __len__(self):
        self.recorder.add("map_length_reads")
        return super().__len__()

    def __setitem__(self, value, slot):
        self.recorder.add("map_insertions")
        self.recorder.add("map_key_argument_octets", octets(value))
        super().__setitem__(value, slot)

    def __iter__(self):
        for value in super().__iter__():
            self.recorder.add("map_key_visits")
            yield value


@contextmanager
def observe(recorder, operation):
    declaration = growth_binding(operation).declaration
    recorder.metrics = {bound.metric: 0 for bound in declaration.bounds}

    def allocated():
        recorder.add("allocated_slots")
        slot = SLOT()
        slot.lock = Lock(slot.lock, recorder)
        return slot

    def forbidden(metric, *_args, **_kwargs):
        recorder.add(metric)
        pytest.fail("pool-only operation crossed its execution boundary")

    def filesystem(*args, **kwargs):
        return forbidden("filesystem_calls", *args, **kwargs)

    def catalog(*args, **kwargs):
        return forbidden("catalog_calls", *args, **kwargs)

    def native(*args, **kwargs):
        return forbidden("native_session_calls", *args, **kwargs)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(owner, "_Slot", allocated)
        patch.setattr(builtins, "open", filesystem)
        for name in ("open", "stat", "lstat", "scandir", "listdir", "mkdir", "unlink"):
            patch.setattr(os, name, filesystem)
        patch.setattr(sqlite3, "connect", catalog)
        for name in ("open_retained", "progress", "advance", "result", "close"):
            patch.setattr(JournalValidationSession, name, native)
        recorder.active = True
        try:
            yield
        finally:
            recorder.active = False


def measure(current, recorder, operation, selected, *, closed=False):
    existing = current._slots.get(selected)
    admitted = not current._closed and (
        not existing.lock.actual.locked()
        if existing is not None
        else len(current._slots) < current.concurrency
    )
    pending = len(current._slots)
    keys = tuple(current._slots)
    dimensions = {
        "configured_slots": current.concurrency,
        "pending_slots": pending,
        "input_key_octets": octets(selected) if operation is RESERVE else 0,
        "retained_key_octets": sum(octets(value) for value in keys),
    }
    projection = project_bindings((growth_binding(operation),))
    binding = growth_binding(operation)
    result, failure = None, None
    with observe(recorder, operation):
        try:
            result = operation(current, selected) if operation is RESERVE else operation(current)
        except RuntimeError as error:
            failure = error
        if operation is RESERVE:
            recorder.metrics["returned_slots"] = int(result is not None)
        else:
            recorder.metrics["returned_keys"] = len(result)
            recorder.metrics["returned_key_octets"] = octets(result)
    assert bool(failure) == bool(closed and operation is RESERVE)
    assert not recorder.pool_held
    if operation is KEYS:
        assert result == tuple((collection, journal) for collection, journal, _attempt in keys)
        assert tuple(current._slots) == keys
    elif result is None:
        assert not admitted
        assert tuple(current._slots) == keys
    else:
        assert admitted
        assert current._slots[selected] is result
        result.lock.release()
    sample = {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical_json_bytes(projection["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": dimensions,
        "metrics": recorder.metrics,
        "delegates": {},
    }
    validate_execution_samples(projection, binding.declaration.id, [sample])
    return sample


def samples(root, operation):
    values = []
    for capacity in (1, 2, 17, 1025):
        for count in sorted({0, 1, capacity - 1, capacity}):
            recorder = Recorder()

            def factory(recorder=recorder):
                recorder.add("catalog_calls")
                pytest.fail("pool reservation opened a catalog session")

            current = owner.UploadJournalValidationOwner(factory, root, concurrency=capacity)
            current._lock = Lock(current._lock, recorder, pool=True)
            current._slots = Slots(recorder)
            for number in range(count):
                slot = current._reserve(key(number))
                assert slot is not None
                slot.lock = Lock(slot.lock, recorder)
                slot.lock.release()
            try:
                values.append(measure(current, recorder, operation, key(capacity + 1)))
                if operation is RESERVE and count:
                    held = current._slots[key(0)]
                    assert held.lock.acquire(blocking=False)
                    try:
                        values.append(measure(current, recorder, operation, key(0)))
                        values.append(measure(current, recorder, operation, key(capacity + 2)))
                    finally:
                        held.lock.release()
                    values.append(measure(current, recorder, operation, key(0)))
                current.close()
                values.append(measure(current, recorder, operation, key(0), closed=True))
            finally:
                current.close()
    return values


@pytest.mark.growth_witness(
    "riverhog-validation-pool-reservation-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_owner.UploadJournalValidationOwner._reserve",
)
def test_actual_pool_reservations_are_nonblocking_and_independent(tmp_path, record_property):
    record_property("growth_samples", samples(tmp_path, RESERVE))


@pytest.mark.growth_witness(
    "riverhog-validation-pool-keys-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_owner.UploadJournalValidationOwner.active_keys",
)
def test_actual_pool_key_snapshots_visit_only_configured_slots(tmp_path, record_property):
    record_property("growth_samples", samples(tmp_path, KEYS))
