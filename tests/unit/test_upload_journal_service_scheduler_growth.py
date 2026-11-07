from __future__ import annotations

import builtins
import inspect
import io
import os
import sqlite3
from collections import defaultdict
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from riverhog_core.services import collection_uploads as owner
from riverhog_growth_contracts import growth_binding
from sqlalchemy import event, func, select

from tests.unit import test_upload_journal_service_caller_growth as callers
from tests.unit import test_upload_journal_service_coordination_growth as coordination
from tests.unit import test_upload_journal_validation_pool_growth as pool
from tests.unit import test_upload_journal_validation_release_growth as release
from tests.unit import test_upload_journal_validation_retirement_loop_growth as retirement
from tests.unit import test_upload_journal_validation_sweep_growth as sweeps
from tests.unit.test_upload_journal_source_growth import octets

ROOT = owner.SqlAlchemyCollectionUploadService.process_due_provenance_journal_validations
CHILDREN = dict(owner._service_scheduler_children())


def unwrap(lock):
    while isinstance(lock, (release.Lock, pool.Lock)):
        lock = lock.actual
    return lock


class Recorder:
    def __init__(self, case, current):
        self.case, self.current = case, current
        self.caller = callers.Recorder(case, current)
        self.active, self.depth = False, 0
        case.readers = 0
        case.root = current._journal_validation_retirement.root

    @contextmanager
    def child(self):
        self.depth += 1
        try:
            yield
        finally:
            self.depth -= 1

    def guard(self, actual):
        def guarded(*args, **kwargs):
            if self.active and self.depth == 0:
                self.own["unscoped_operations"] += 1
                raise AssertionError("scheduler performed work outside its declared children")
            return actual(*args, **kwargs)

        return guarded

    def key(self, value):
        self.maximum_key_octets = max(self.maximum_key_octets, octets(value))
        recorder = self

        class Key(tuple):
            def compared(self, other):
                if recorder.active and recorder.depth == 0:
                    recorder.own["ordering_comparisons"] += 1
                    recorder.own["ordering_operand_octets"] += octets(self) + octets(other)

            def __lt__(self, other):
                self.compared(other)
                return tuple.__lt__(self, other)

            def __gt__(self, other):
                self.compared(other)
                return tuple.__gt__(self, other)

            def __le__(self, other):
                self.compared(other)
                return tuple.__le__(self, other)

        return Key(value)

    def sweep(self, selected):
        with self.child():
            recorder = sweeps.Recorder()
            old_lock, old_slots = selected._lock, selected._slots
            old_slot_locks = {id(slot): slot.lock for slot in old_slots.values()}
            callbacks, live = [], []
            selected._lock = release.Lock(unwrap(old_lock), recorder, pool=True)
            for key, slot in old_slots.items():
                slot.lock = release.Lock(unwrap(slot.lock), recorder)
                if slot.source is not None:
                    storage = slot.session._store._storage
                    callbacks.append((storage, storage.on_close))
                    storage.on_close = recorder.callback(storage.on_close)
                    live.append((key, slot, storage))
            try:
                observed = recorder.measure(self.case, selected, live, {}, cold=False, busy=False)
                self.children["sweep"].append(observed)
            finally:
                remaining = dict(selected._slots)
                old_slots.clear()
                old_slots.update(remaining)
                selected._slots, selected._lock = old_slots, old_lock
                for slot in remaining.values():
                    slot.lock = old_slot_locks[id(slot)]
                for storage, callback in callbacks:
                    if not storage._closed:
                        storage.on_close = callback
        return None

    def keys(self, selected):
        with self.child():
            recorder = pool.Recorder()
            old_lock, old_slots = selected._lock, selected._slots
            slots = pool.Slots(recorder)
            slots.update(old_slots)
            selected._slots, selected._lock = (
                slots,
                pool.Lock(unwrap(old_lock), recorder, pool=True),
            )
            dimensions = {
                "configured_slots": selected.concurrency,
                "pending_slots": len(slots),
                "input_key_octets": 0,
                "retained_key_octets": octets(tuple(slots)),
            }
            try:
                with pool.observe(recorder, CHILDREN["keys"]):
                    result = CHILDREN["keys"](selected)
                    recorder.metrics["returned_keys"] = len(result)
                    recorder.metrics["returned_key_octets"] = octets(result)
                self.children["keys"].append(
                    coordination.sample(CHILDREN["keys"], dimensions, recorder.metrics)
                )
            finally:
                selected._slots, selected._lock = old_slots, old_lock
            result = tuple(self.key(value) for value in result)
        recorder = self

        class Active(tuple):
            def __iter__(self):
                for value in super().__iter__():
                    if recorder.active and recorder.depth == 0:
                        recorder.own["active_key_visits"] += 1
                        recorder.own["active_key_octets"] += octets(value)
                    yield value

        return Active(result)

    def retired(self, selected, *, limit=1, octet_budget=65536):
        self.own["retirement_unit_limit_per_call"] = max(
            self.own["retirement_unit_limit_per_call"], limit
        )
        with self.child():
            result, observed = retirement.measure(
                self.case, selected, limit=limit, budget=octet_budget, cold=False
            )
            self.children["retirement"].append(observed)
            return result

    def selected(self, factory, *, after, excluding):
        self.maximum_key_octets = max(
            self.maximum_key_octets, octets(after), *(octets(value) for value in excluding)
        )
        self.own["excluded_keys_per_call"] = max(self.own["excluded_keys_per_call"], len(excluding))
        self.own["exclusion_key_references"] += len(excluding)
        self.own["exclusion_operand_octets"] += octets(excluding)
        with self.child():
            result, observed = coordination.catalog_measure(
                self.case,
                CHILDREN["select"],
                lambda: CHILDREN["select"](factory, after=after, excluding=excluding),
                (after, excluding),
                len(excluding),
                False,
            )
            self.children["select"].append(observed)
            return None if result is None else self.key(result)

    def validated(self, selected, collection_id, journal_id):
        assert selected is self.current
        with self.child():
            result, observed = self.caller.measure(
                collection_id,
                journal_id,
                interrupt=self.interrupt,
                expected_error=self.expected_error,
            )
            self.children["validation"].append(observed)
            if self.caller.last_error is not None:
                raise self.caller.last_error
            return result

    def scheduled(self, selected, collection_id):
        assert selected is self.current
        from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord as Volume
        from riverhog_core.catalog_models import CollectionUploadRecord as Upload

        with self.child():
            before = coordination.rows(self.case.engine, Upload)
            own_dimensions = {
                "retained_rows": sum(map(len, before.values())),
                "metadata_row_octets": max(
                    256, *(octets(row) for values in before.values() for row in values)
                ),
            }
            with self.case.factory() as session:
                upload = session.get(Upload, collection_id)
                volumes = session.scalar(select(func.count()).select_from(Volume)) or 0
                selected_volumes = (
                    session.scalar(
                        select(func.count())
                        .select_from(Volume)
                        .where(Volume.collection_id == collection_id)
                    )
                    or 0
                )
                text = "" if upload is None else upload.planner_checkpoint_json
                checkpoint = None if upload is None else owner._planner_checkpoint(upload)
                admitted = int(
                    checkpoint is not None
                    and checkpoint.closed
                    and checkpoint.next_sequence > 0
                    and owner._has_complete_payload_seal(upload)
                )
            dimensions = {
                "checkpoint_octets": octets(text),
                "volume_rows": selected_volumes,
                "catalog_volume_rows": volumes,
                "count_admitted": admitted,
                "lookup_octets": octets(collection_id),
                "counter_octets": octets(selected_volumes),
            }
            recorder, children, clocks = coordination.Recorder(), [], []
            child = owner._ready_for_finalization
            now = owner.utc_timestamp_now

            def clock():
                value = now()
                clocks.append(value)
                return value

            def readiness(session, upload):
                prior = dict(recorder.metrics)
                try:
                    return child(session, upload)
                finally:
                    metrics = {
                        name: value - prior.get(name, 0) for name, value in recorder.metrics.items()
                    }
                    metrics.update(connection_statements=0, changed_rows=0, source_payload_octets=0)
                    metrics["sealed_count_calls"] = metrics["sealed_count_calls_limit"] = metrics[
                        "sql_statements"
                    ]
                    recorder.active = False
                    try:
                        children.append(coordination.sample(child, dimensions, metrics))
                    finally:
                        recorder.active = True

            with (
                recorder.observe(self.case.engine, cold=False),
                coordination.codecs(recorder),
                pytest.MonkeyPatch.context() as patch,
            ):
                patch.setattr(owner, "_ready_for_finalization", readiness)
                patch.setattr(owner, "utc_timestamp_now", clock)
                result = CHILDREN["schedule"](selected, collection_id)
            metrics = dict(recorder.metrics)
            for name in ("sealed_count_calls", "sealed_count_calls_limit"):
                metrics[name] = sum(row["metrics"][name] for row in children)
            metrics["readiness_calls"] = metrics["readiness_calls_limit"] = len(children)
            own_dimensions["control_octets"] = octets((collection_id, *clocks))
            own_dimensions["readiness_calls"] = len(children)
            for item in growth_binding(child).declaration.dimensions:
                own_dimensions["readiness_" + item.name] = dimensions[item.name] if children else 0
            self.children["schedule"].append(
                coordination.sample(
                    CHILDREN["schedule"], own_dimensions, metrics, {"readiness": children}
                )
            )
            return result

    def measure(self, *, limit=1, interrupt=False, expected_error=None):
        self.interrupt, self.expected_error = interrupt, expected_error
        result, error = None, None
        self.own = defaultdict(int)
        self.children = {tag: [] for tag in CHILDREN}
        self.maximum_key_octets = octets(self.current._journal_validation_after)
        self.ranges = 0

        def ranges(*args):
            frame = inspect.currentframe().f_back
            if not self.active or self.depth or frame.f_code is not ROOT.__code__:
                return builtins.range(*args)
            metric = "outer_iterations" if self.ranges == 0 else "fresh_iterations"
            self.ranges += 1

            def iterate():
                for value in builtins.range(*args):
                    self.own[metric] += 1
                    yield value

            return iterate()

        def sorted_keys(values, *args, **kwargs):
            if self.active and self.depth == 0:
                self.own["sort_calls"] += 1
            return builtins.sorted(values, *args, **kwargs)

        def sql(*_args):
            if self.active and self.depth == 0:
                self.own["unscoped_operations"] += 1
                raise AssertionError("scheduler performed unscoped SQL")

        event.listen(self.case.engine, "before_cursor_execute", sql)
        try:
            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(
                    type(self.current._journal_validation_owner),
                    "release_stale",
                    lambda selected: self.sweep(selected),
                )
                patch.setattr(
                    type(self.current._journal_validation_owner),
                    "active_keys",
                    lambda selected: self.keys(selected),
                )
                patch.setattr(
                    type(self.current._journal_validation_retirement),
                    "advance",
                    lambda selected, **kwargs: self.retired(selected, **kwargs),
                )
                patch.setattr(owner, "_next_upload_journal_validation", self.selected)
                patch.setattr(
                    type(self.current),
                    "_advance_provenance_journal_validation",
                    lambda selected, *args: self.validated(selected, *args),
                )
                patch.setattr(
                    type(self.current),
                    "_schedule_finalization_if_ready",
                    lambda selected, *args: self.scheduled(selected, *args),
                )
                patch.setattr(owner, "range", ranges, raising=False)
                patch.setattr(owner, "sorted", sorted_keys, raising=False)
                for module, name in (
                    (builtins, "open"),
                    (io, "open"),
                    (sqlite3, "connect"),
                    *(
                        (os, name)
                        for name in (
                            "open",
                            "close",
                            "stat",
                            "lstat",
                            "read",
                            "write",
                            "listdir",
                            "scandir",
                            "mkdir",
                            "unlink",
                            "rmdir",
                            "rename",
                            "replace",
                        )
                    ),
                    (owner, "canonical_json_bytes"),
                ):
                    patch.setattr(module, name, self.guard(getattr(module, name)))
                patch.setattr(
                    owner,
                    "json",
                    SimpleNamespace(
                        loads=self.guard(owner.json.loads),
                        dumps=self.guard(owner.json.dumps),
                        JSONDecodeError=owner.json.JSONDecodeError,
                    ),
                )
                patch.setattr(
                    owner, "hashlib", SimpleNamespace(sha256=self.guard(owner.hashlib.sha256))
                )
                self.active = True
                try:
                    result = ROOT(self.current, limit=limit)
                except Exception as caught:
                    if expected_error is None or not isinstance(caught, expected_error):
                        raise
                    error = caught
                finally:
                    self.active = False
        finally:
            event.remove(self.case.engine, "before_cursor_execute", sql)
        if expected_error is not None:
            assert isinstance(error, expected_error) and error is self.caller.last_error
        else:
            assert error is None and type(result) is int
        self.own["progressed_units"] = 0 if result is None else result
        dimensions = {
            "unit_limit": max(0, int(limit)),
            "configured_slots": self.current._config.journal_validation_concurrency,
            "maximum_key_octets": self.maximum_key_octets,
        }
        for tag, children in self.children.items():
            dimensions[tag + "_calls"] = len(children)
            self.own[tag + "_calls"] = self.own[tag + "_calls_limit"] = len(children)
            for item in growth_binding(CHILDREN[tag]).declaration.dimensions:
                dimensions[tag + "_" + item.name] = max(
                    (child["dimensions"][item.name] for child in children), default=0
                )
            metric = {
                "sweep": "sql_statements",
                "keys": "returned_keys",
                "retirement": "outer_iterations",
                "select": "sql_statements",
                "validation": "comparison_operations",
                "schedule": "readiness_calls",
            }[tag]
            self.own[tag + "_" + metric] = sum(child["metrics"][metric] for child in children)
            assert all(
                all(
                    value <= dimensions[tag + "_" + name]
                    for name, value in child["dimensions"].items()
                )
                for child in children
            )
        return result, coordination.sample(ROOT, dimensions, self.own, self.children)


def initialized(case, current):
    from riverhog_core.catalog_models import CollectionUploadRecord as Upload

    with case.factory() as session, session.begin():
        session.get(
            Upload, 1
        ).planner_checkpoint_json = owner.incremental_volume_planner_checkpoint_bytes(
            owner.new_incremental_volume_planner(policy=current._config.volume_policy)
        ).decode()


def small_samples(root):
    from tests.unit import test_upload_journal_service_unit_growth as units

    samples = []
    for case, current in units.real_case(root):
        initialized(case, current)
        recorder = Recorder(case, current)
        for limit in (-1, 0, 1):
            result, observed = recorder.measure(limit=limit)
            assert result == int(limit > 0)
            samples.append(observed)
        assert recorder.caller.units.native_calls == 1
        for phase in (1, 2):
            callers.advance_fixture(current, phase)
            result, observed = recorder.measure(limit=1)
            assert result == 1
            samples.append(observed)
        assert callers.terminal(case.factory)["state"] == "sealed"
        assert recorder.caller.units.native_calls == 3
        result, observed = recorder.measure(limit=1)
        assert result == 0 and observed["metrics"]["retirement_calls"] == 1
        samples.append(observed)
        from riverhog_core.services import upload_journal_validation_jobs as jobs

        from tests.unit.test_staged_journal_range import JOURNAL, Journal

        with case.factory() as session, session.begin():
            row = session.get(Journal, (1, JOURNAL))
            row.state = "failed"
            jobs.retire_source(session, 1, JOURNAL, row.capture_attempt_id)
        result, observed = recorder.measure(limit=2)
        assert result == 2 and observed["metrics"]["retirement_calls"] == 2
        samples.append(observed)
    return samples


def busy_samples(root):
    import random
    from dataclasses import replace

    from tests.unit import test_upload_journal_service_unit_growth as units

    samples = []
    for capacity in (1, 2, 7):
        for case, current in units.real_case(root / str(capacity), unrelated=17):
            initialized(case, current)
            current._config = replace(current._config, journal_validation_concurrency=capacity)
            current._journal_validation_owner.concurrency = capacity
            order = list(range(capacity))
            random.Random(17).shuffle(order)
            held = []
            try:
                for number in order:
                    key = (
                        number + 2,
                        f"urn:uuid:{number + 2:08x}-0000-4000-8000-000000000000",
                        f"{number + 2:032x}",
                    )
                    slot = current._journal_validation_owner._reserve(key)
                    assert slot is not None
                    held.append(slot.lock)
                recorder = Recorder(case, current)
                result, observed = recorder.measure(limit=1)
                assert result == 1
                assert observed["metrics"]["validation_calls"] == capacity + 1
                samples.append(observed)
                result, observed = recorder.measure(limit=2)
                assert result == 0
                assert observed["metrics"]["validation_calls"] == capacity + 1
                assert observed["metrics"]["outer_iterations"] == 1
                samples.append(observed)
                assert all(
                    slot.session is None
                    for slot in current._journal_validation_owner._slots.values()
                )
            finally:
                for lock in held:
                    lock.release()
    return samples


def rejection_samples(root):
    from sqlalchemy.exc import OperationalError

    from tests.unit import test_upload_journal_service_unit_growth as units
    from tests.unit.test_journal_capture import checkpoint

    samples = []
    for case, current in units.real_case(root):
        initialized(case, current)
        recorder = Recorder(case, current)
        before = checkpoint(case.factory)
        result, observed = recorder.measure(
            limit=3, interrupt=True, expected_error=OperationalError
        )
        assert result is None and checkpoint(case.factory) == before
        assert observed["metrics"]["validation_calls"] == 1
        assert observed["metrics"]["schedule_calls"] == 0
        samples.append(observed)
        result, observed = recorder.measure(limit=1)
        assert result == 1 and checkpoint(case.factory) != before
        samples.append(observed)
    return samples


@pytest.mark.growth_witness(
    "riverhog-validation-service-scheduler-lifecycle/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation=(
        "riverhog_core.services.collection_uploads.SqlAlchemyCollectionUploadService.pr"
        "ocess_due_provenance_journal_validations"
    ),
)
def test_actual_scheduler_runs_validation_readiness_and_retirement(tmp_path, record_property):
    record_property("growth_samples", small_samples(tmp_path))


@pytest.mark.growth_witness(
    "riverhog-validation-service-scheduler-busy/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation=(
        "riverhog_core.services.collection_uploads.SqlAlchemyCollectionUploadService.pr"
        "ocess_due_provenance_journal_validations"
    ),
)
def test_actual_scheduler_bounds_real_busy_reservations(tmp_path, record_property):
    record_property("growth_samples", busy_samples(tmp_path))


@pytest.mark.growth_witness(
    "riverhog-validation-service-scheduler-rejections/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation=(
        "riverhog_core.services.collection_uploads.SqlAlchemyCollectionUploadService.pr"
        "ocess_due_provenance_journal_validations"
    ),
)
def test_actual_scheduler_preserves_rejection_identity_and_retries(tmp_path, record_property):
    record_property("growth_samples", rejection_samples(tmp_path))
