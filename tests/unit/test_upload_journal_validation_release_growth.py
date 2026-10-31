from __future__ import annotations

import builtins
import hashlib
import os
import sqlite3
import sys
import threading
from contextlib import ExitStack, contextmanager
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.catalog_models import CollectionUploadRecord
from riverhog_core.services import collection_uploads as source_owner
from riverhog_core.services import upload_journal_epochs as epochs
from riverhog_core.services import upload_journal_rank as rank
from riverhog_core.services import upload_journal_replay as replay
from riverhog_core.services import upload_journal_topology as topology
from riverhog_core.services import upload_journal_union as union
from riverhog_core.services import upload_journal_validation_owner as owner
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import JournalValidationSession
from sqlalchemy import event, select

from tests.unit.test_journal_capture import advance, retained
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_staged_journal_range import JOURNAL, Chunk, Journal
from tests.unit.test_upload_journal_owned_source_growth import snapshot

DROP, CLOSE = owner.UploadJournalValidationOwner._drop, owner.UploadJournalValidationOwner.close
NATIVE_CLOSE = JournalValidationSession.close


def native_dimensions(callback):
    return {
        item.name: int(callback and item.name == "release_callbacks")
        for item in growth_binding(NATIVE_CLOSE).declaration.dimensions
    }


def releases(session):
    storage = session._store._storage
    return (
        not session._closed
        and session._store._cleanup.alive
        and storage._readers == 1
        and storage.on_close is not None
    )


class Pool(dict):
    def __init__(self, recorder):
        super().__init__()
        self.recorder = recorder

    def get(self, key, default=None):
        self.recorder.add("pool_map_lookups")
        return super().get(key, default)

    def __delitem__(self, key):
        self.recorder.add("pool_map_removals")
        super().__delitem__(key)

    def clear(self):
        self.recorder.add("pool_map_clears")
        super().clear()

    def values(self):
        for value in super().values():
            self.recorder.add("pool_slot_visits")
            yield value

    def items(self):
        for item in super().items():
            self.recorder.add("pool_slot_visits")
            yield item


class Lock:
    def __init__(self, actual, recorder, *, pool=False):
        self.actual, self.recorder, self.pool = actual, recorder, pool
        self.waiting = None

    def acquire(self, blocking=True, timeout=-1):
        self.recorder.add("pool_lock_acquisitions" if self.pool else "slot_lock_attempts")
        if not self.pool:
            self.recorder.add("blocking_slot_lock_attempts", int(blocking))
            if self.waiting is not None and blocking:
                self.waiting.set()
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


class Recorder:
    def __init__(self):
        self.active, self.pool_held, self.native_active, self.in_callback = (
            False,
            False,
            False,
            False,
        )
        self.thread = None
        self.values = {}
        self.native_values = {}
        self.delegates = {}
        self.native_connections = []
        self.callbacks = 0
        self.metadata = {}
        for operation in (DROP, CLOSE, NATIVE_CLOSE):
            binding = growth_binding(operation)
            projection = project_bindings((binding,))
            self.metadata[operation] = (
                binding,
                projection,
                hashlib.sha256(
                    canonical_json_bytes(projection["declarations"][binding.declaration.id])
                ).hexdigest(),
            )

    def recording(self):
        return self.active and self.thread == threading.get_ident()

    def add(self, metric, amount=1):
        if self.recording():
            self.values[metric] += amount

    def syscall(self, metric, action, *args, **kwargs):
        if self.recording():
            assert not self.pool_held
        result = action(*args, **kwargs)
        self.add(metric)
        if metric == "closed_files":
            if self.recording() and self.native_active:
                self.native_values[metric] += 1
            if self.in_callback:
                self.add("lease_descriptor_closes")
        return result

    def callback(self, action):
        def invoke():
            assert not self.pool_held
            self.callbacks += 1
            self.add("release_callbacks")
            self.add("lease_release_callbacks")
            was_native = self.native_active
            if self.recording() and was_native:
                self.native_values["release_callbacks"] += 1
            self.native_active, self.in_callback = False, True
            try:
                return action()
            finally:
                self.native_active, self.in_callback = was_native, False

        return invoke

    def close(self, session):
        if not self.recording():
            return NATIVE_CLOSE(session)
        assert not self.pool_held
        self.native_values = {
            bound.metric: 0 for bound in growth_binding(NATIVE_CLOSE).declaration.bounds
        }
        self.native_active = True
        try:
            return NATIVE_CLOSE(session)
        finally:
            self.native_active = False
            self.add("native_close_calls")
            assert self.native_values["release_callbacks"] in {0, 1}
            callback = bool(self.native_values["release_callbacks"])
            tag = "release" if callback else "retain"
            node = self.sample(NATIVE_CLOSE, native_dimensions(callback), self.native_values, {})
            self.delegates[tag].append(node)

    def sample(self, operation, dimensions, metrics, delegates):
        binding, _projection, digest = self.metadata[operation]
        return {
            "declaration": binding.declaration.id,
            "declaration_sha256": digest,
            "dimensions": dimensions,
            "metrics": metrics.copy(),
            "delegates": delegates,
        }

    @contextmanager
    def kernel(self, engine, *, catalog_reads=False):
        recorder = self
        connect = sqlite3.connect

        class Connection(sqlite3.Connection):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                recorder.native_connections.append(self)
                self.closed, self.last_changes = False, 0
                self.set_trace_callback(self.trace)
                self.set_progress_handler(self.tick, 1)
                self.set_authorizer(self.authorize)

            def trace(self, _statement):
                if recorder.recording():
                    recorder.values["sql_statements"] += 1
                    if recorder.native_active:
                        recorder.native_values["sql_statements"] += 1

            def tick(self):
                if recorder.recording():
                    recorder.values["sqlite_vm_instructions"] += 1
                    if recorder.native_active:
                        recorder.native_values["sqlite_vm_instructions"] += 1
                return 0

            def authorize(self, action, *_args):
                if recorder.recording() and action in {
                    sqlite3.SQLITE_READ,
                    sqlite3.SQLITE_INSERT,
                    sqlite3.SQLITE_UPDATE,
                    sqlite3.SQLITE_DELETE,
                }:
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            def execute(self, *args, **kwargs):
                if recorder.recording():
                    raise AssertionError("retained release queried its native database")
                return super().execute(*args, **kwargs)

            def close(self):
                if recorder.recording():
                    assert not recorder.pool_held and recorder.native_active
                    recorder.values["closed_databases"] += 1
                    recorder.native_values["closed_databases"] += 1
                if not self.closed:
                    self.last_changes = self.total_changes
                self.closed = True
                return super().close()

        def observed_connect(*args, **kwargs):
            if self.recording() and (not catalog_reads or "factory" not in kwargs):
                raise AssertionError("retained release opened a database")
            kwargs.setdefault("factory", Connection)
            return connect(*args, **kwargs)

        def sql(*_args):
            self.add("catalog_calls")
            if self.recording():
                assert not self.pool_held
                if not catalog_reads:
                    raise AssertionError("retained release queried the catalog")

        def file_call(function, metric):
            def call(*args, **kwargs):
                if self.recording() and metric != "closed_files":
                    raise AssertionError("retained release performed path or payload work")
                return self.syscall(metric, function, *args, **kwargs)

            return call

        real_encode, real_loads, real_hash = (
            canonical_json_bytes,
            __import__("json").loads,
            hashlib.sha256,
        )

        def record(metric, amount):
            if not getattr(self, "source_active", False):
                self.add(metric, amount)
            if self.recording() and self.native_active:
                self.native_values[metric] += amount

        def encode(*args, **kwargs):
            value = real_encode(*args, **kwargs)
            record("encoded_json_octets", len(value))
            return value

        def loads(value, *args, **kwargs):
            record(
                "decoded_json_octets", len(value.encode()) if isinstance(value, str) else len(value)
            )
            return real_loads(value, *args, **kwargs)

        class Hash:
            def __init__(self, actual):
                self.actual = actual

            def update(self, value):
                record("native_hashed_octets", len(value))
                self.actual.update(value)

            def copy(self):
                return Hash(self.actual.copy())

            def __getattr__(self, name):
                return getattr(self.actual, name)

        def digest(value=b"", *args, **kwargs):
            record("native_hashed_octets", len(value))
            return Hash(real_hash(value, *args, **kwargs))

        class Json:
            def __init__(self, original):
                self.original = original
                self.loads = loads

            def __getattr__(self, name):
                return getattr(self.original, name)

        event.listen(engine, "before_cursor_execute", sql)
        try:
            with pytest.MonkeyPatch.context() as patch:
                for name, module in tuple(sys.modules.items()):
                    if name.startswith("riverhog_provenance.") and module is not None:
                        if hasattr(module, "canonical_json_bytes"):
                            patch.setattr(module, "canonical_json_bytes", encode)
                        if hasattr(module, "hashlib"):
                            patch.setattr(module, "hashlib", SimpleNamespace(sha256=digest))
                        if hasattr(module, "json"):
                            patch.setattr(module, "json", Json(module.json))
                patch.setattr(sqlite3, "connect", observed_connect)
                patch.setattr(
                    JournalValidationSession, "close", lambda session: self.close(session)
                )
                for name, metric in (
                    ("open", "path_opens"),
                    ("close", "closed_files"),
                    ("stat", "path_probes"),
                    ("fstat", "path_probes"),
                    ("fsync", "path_syncs"),
                    ("mkdir", "path_mutations"),
                    ("unlink", "path_mutations"),
                    ("rmdir", "path_mutations"),
                    ("scandir", "directory_scans"),
                    ("listdir", "directory_scans"),
                ):
                    patch.setattr(os, name, file_call(getattr(os, name), metric))
                patch.setattr(builtins, "open", file_call(builtins.open, "path_opens"))
                yield
        finally:
            event.remove(engine, "before_cursor_execute", sql)

    def measure(self, current, operation, key=None, slot=None):
        selected = [slot] if operation is DROP else list(current._slots.values())
        dimensions = {
            "configured_slots": current.concurrency,
            "pending_slots": len(current._slots),
            "release_calls": sum(
                value.session is not None and releases(value.session) for value in selected
            ),
            "retain_calls": sum(
                value.session is not None and not releases(value.session) for value in selected
            ),
        }
        self.values = {bound.metric: 0 for bound in growth_binding(operation).declaration.bounds}
        self.delegates = {"release": [], "retain": []}
        changes = {
            connection: connection.total_changes
            for connection in self.native_connections
            if not connection.closed
        }
        self.thread, self.active = threading.get_ident(), True
        try:
            if operation is DROP:
                operation(current, key, slot)
            else:
                operation(current)
        finally:
            self.active = False
        dimensions["release_calls"] = len(self.delegates["release"])
        dimensions["retain_calls"] = len(self.delegates["retain"])
        self.values["native_close_classes"] = self.values["native_close_calls"]
        assert self.values["lease_descriptor_closes"] == 4 * dimensions["release_calls"]
        assert self.values["closed_databases"] == dimensions["release_calls"]
        for tag, children in self.delegates.items():
            self.values[tag + "_calls"] = self.values[tag + "_calls_limit"] = len(children)
            for bound in growth_binding(NATIVE_CLOSE).declaration.bounds:
                self.values[tag + "_" + bound.metric] = sum(
                    child["metrics"][bound.metric] for child in children
                )
        sample = self.sample(operation, dimensions, self.values, self.delegates)
        validate_execution_samples(
            project_bindings((growth_binding(operation),)),
            growth_binding(operation).declaration.id,
            [sample],
        )
        assert not self.pool_held
        assert all(
            (connection.last_changes if connection.closed else connection.total_changes) == before
            for connection, before in changes.items()
        )
        return sample


def copy_row(row, **changes):
    return type(row)(
        **{**{column.key: getattr(row, column.key) for column in row.__table__.columns}, **changes}
    )


def finite_work(factory, collection, catalog):
    for _ in range(100000):
        selected = source_owner._read_upload_journal_capture(factory, collection, JOURNAL)
        if selected is None:
            break
        assert source_owner._commit_upload_journal_capture(factory, selected, advance(selected))
    else:
        pytest.fail("finite source capture did not finish")
    work = source_owner._read_upload_journal_validation(factory, collection, JOURNAL)
    assert work is not None
    for function in (
        epochs.advance_upload_journal_epoch_scan,
        topology.advance_upload_journal_topology_capture,
        union.advance_upload_journal_epoch_union,
        rank.advance_upload_journal_epoch_rank,
    ):
        for _ in range(100000):
            if function(factory, work):
                break
        else:
            pytest.fail("finite source preparation did not finish")
    assert replay.prepare_upload_journal_replay(factory, work, catalog=catalog)
    return work


@contextmanager
def build_cases(root, *, large=True):
    specifications = [
        (0, 0, 0, False, 0),
        (1, 1, 0, False, 0),
        (2, 2, 0, False, 0),
        (2, 1, 0, True, 2),
    ]
    if large:
        specifications.extend(
            (
                (17, 17, 0, False, 0),
                (1025, 2, 0, False, 0),
                (1, 1, 65537, False, 0),
                (1, 1, 1024 * 1024, False, 0),
                (1, 1, 0, True, 0),
            )
        )
    with ExitStack() as resources:
        cases = []
        catalog = source_owner.admission_provenance_catalog()
        for index, (slots, live, payload, accepted, readers) in enumerate(specifications):
            path = root / str(index)
            engine, factory = resources.enter_context(
                retained(path, source(payload, journal_id=JOURNAL))
            )
            factory.configure(autoflush=False, expire_on_commit=False)
            with factory() as session, session.begin():
                original_upload = session.get(CollectionUploadRecord, 1)
                original = session.get(Journal, (1, JOURNAL))
                chunks = list(
                    session.scalars(
                        select(Chunk).where(Chunk.collection_id == 1, Chunk.journal_id == JOURNAL)
                    )
                )
                for number in range(2, live + 1):
                    session.add(
                        copy_row(
                            original_upload,
                            collection_id=number,
                            idempotency_key=f"parallel-{number}",
                            archive_storage_prefix=f"collections/{number}",
                        )
                    )
                    session.flush()
                    journal = copy_row(
                        original,
                        collection_id=number,
                        capture_phase="pending",
                        capture_attempt_id=None,
                        capture_directory_id=None,
                        capture_version=0,
                    )
                    session.add(journal)
                    session.flush()
                    session.add_all(copy_row(chunk, collection_id=number) for chunk in chunks)
                    session.flush()
                    source_owner._begin_upload_journal_capture(session, journal)
            works = [finite_work(factory, number, catalog) for number in range(1, live + 1)]
            cases.append(
                SimpleNamespace(
                    engine=engine,
                    factory=factory,
                    path=path,
                    catalog=catalog,
                    works=works,
                    slots=slots,
                    accepted=accepted,
                    readers=readers,
                )
            )
        yield cases


@pytest.fixture(scope="module")
def release_cases(tmp_path_factory):
    with build_cases(tmp_path_factory.mktemp("owner-release-inputs")) as cases:
        yield cases


@contextmanager
def live_owner(case, *, recorder_type=Recorder):
    recorder = recorder_type()
    current = owner.UploadJournalValidationOwner(
        case.factory, case.path / "native", concurrency=max(1, case.slots)
    )
    current._lock, current._slots = Lock(current._lock, recorder, pool=True), Pool(recorder)
    borrowed, selected = [], []
    with recorder.kernel(case.engine):
        try:
            for work in case.works:
                key = work.collection_id, work.journal_id, work.attempt_id
                slot = current._reserve(key)
                assert slot is not None
                slot.lock = Lock(slot.lock, recorder)
                try:
                    assert current._open(work, slot, case.catalog) is None
                finally:
                    slot.lock.release()
                storage = slot.session._store._storage
                assert storage.on_close is not None
                storage.on_close = recorder.callback(storage.on_close)
                if case.accepted:
                    for _ in range(100000):
                        if slot.session.advance().complete:
                            break
                    else:
                        pytest.fail("genuine accepted fixture did not complete")
                    assert slot.session.progress().accepted_frames == work.cursor.frame_count
                    borrowed.extend(slot.session.result() for _ in range(case.readers))
                selected.append((key, slot, storage))
            for number in range(len(case.works), case.slots):
                slot = current._reserve((100000 + number, JOURNAL, f"{number:032x}"))
                assert slot is not None
                slot.lock = Lock(slot.lock, recorder)
                slot.lock.release()
            yield current, recorder, selected, borrowed
        finally:
            current.close()
            for reader in borrowed:
                reader.close()
            assert all(storage._closed for _key, _slot, storage in selected)


def verify_custody(current, selected, borrowed):
    for _key, slot, storage in selected:
        if borrowed:
            assert not storage._closed and storage._readers == len(borrowed)
            with owner.lease_journal_validation_workspace(
                current.root, slot.job.workspace_id
            ) as directory:
                assert directory is None
        else:
            assert storage._closed and storage._readers == 0
            with owner.lease_journal_validation_workspace(
                current.root, slot.job.workspace_id
            ) as directory:
                assert directory == storage.directory


def release_samples(cases, operation, *, samples):
    for case in cases:
        with live_owner(case) as (current, recorder, selected, borrowed):
            before = snapshot(case.engine)
            progresses = {key: slot.session.progress() for key, slot, _storage in selected}
            if operation is DROP:
                for key, slot, _storage in selected:
                    slot.lock.acquire()
                    try:
                        samples.append(recorder.measure(current, operation, key, slot))
                        assert key not in current._slots and slot.session is None
                        samples.append(recorder.measure(current, operation, key, slot))
                        replacement = current._reserve(key)
                        assert replacement is not None
                        replacement.lock = Lock(replacement.lock, recorder)
                        replacement.lock.release()
                        samples.append(recorder.measure(current, operation, key, slot))
                        assert current._slots[key] is replacement
                    finally:
                        slot.lock.release()
                if not selected:
                    key = (1, JOURNAL, "a" * 32)
                    slot = owner._Slot()
                    slot.lock.acquire()
                    try:
                        samples.append(recorder.measure(current, operation, key, slot))
                    finally:
                        slot.lock.release()
            else:
                samples.append(recorder.measure(current, operation))
                assert current._closed and not current._slots
                samples.append(recorder.measure(current, operation))
            assert snapshot(case.engine) == before
            verify_custody(current, selected, borrowed)
            for reader in borrowed:
                reader.close()
            borrowed.clear()
            assert recorder.callbacks == len(selected)
            verify_custody(current, selected, borrowed)
            replacement = owner.UploadJournalValidationOwner(
                case.factory, current.root, concurrency=max(1, len(selected))
            )
            try:
                for work in case.works:
                    key = work.collection_id, work.journal_id, work.attempt_id
                    slot = replacement._reserve(key)
                    assert slot is not None
                    try:
                        assert replacement._open(work, slot, case.catalog) is None
                        actual = slot.session.progress()
                        expected = progresses[key]
                        for name in (
                            "phase",
                            "accepted_frames",
                            "accepted_octets",
                            "accepted_sha256",
                            "source_octets",
                            "complete",
                        ):
                            assert getattr(actual, name) == getattr(expected, name)
                    finally:
                        slot.lock.release()
            finally:
                replacement.close()


def waiting_shutdown_sample(case, *, samples):
    with live_owner(case) as (current, recorder, selected, borrowed):
        _key, slot, _storage = selected[0]
        before = snapshot(case.engine)
        waiting = threading.Event()
        slot.lock.waiting = waiting
        assert slot.lock.acquire(blocking=False)
        completed, errors = [], []

        def shutdown():
            try:
                completed.append(recorder.measure(current, CLOSE))
            except BaseException as error:
                errors.append(error)

        worker = threading.Thread(target=shutdown, daemon=True)
        worker.start()
        try:
            assert waiting.wait(30)
            assert current._closed and current._slots.get(_key) is slot
            assert not recorder.pool_held
            assert current._lock.actual.acquire(blocking=False)
            current._lock.actual.release()
            with pytest.raises(RuntimeError):
                current._reserve((1000000, JOURNAL, "f" * 32))
            for reader in borrowed:
                reader.close()
            borrowed.clear()
        finally:
            slot.lock.release()
        worker.join(30)
        assert not worker.is_alive()
        if errors:
            raise errors[0]
        assert len(completed) == 1
        samples.extend(completed)
        assert snapshot(case.engine) == before
        verify_custody(current, selected, borrowed)


@pytest.mark.growth_witness(
    "riverhog-validation-owner-drop-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_owner.UploadJournalValidationOwner._drop",
)
def test_actual_slot_drop_composes_native_release_and_preserves_replacement(
    release_cases, record_property
):
    samples = []
    record_property("growth_samples", samples)
    release_samples(release_cases, DROP, samples=samples)


@pytest.mark.growth_witness(
    "riverhog-validation-owner-close-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_owner.UploadJournalValidationOwner.close",
)
def test_actual_pool_shutdown_preserves_final_reader_custody(release_cases, record_property):
    samples = []
    record_property("growth_samples", samples)
    release_samples(release_cases, CLOSE, samples=samples)
    for case in release_cases:
        if len(case.works) == 1 and (case.readers or case.slots == 1 and not case.accepted):
            waiting_shutdown_sample(case, samples=samples)
