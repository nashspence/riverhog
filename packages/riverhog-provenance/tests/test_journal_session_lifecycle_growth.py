from __future__ import annotations

import os
import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager
from dataclasses import replace
from functools import partial
from pathlib import Path
from tempfile import mkdtemp

import pytest
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import journal_store as store_owner
from riverhog_provenance.journal_session import JournalValidationSession
from test_child_admission_growth import octets
from test_entry_replay_graph_growth import graph_histories as graph_histories
from test_entry_replay_growth import REGIMES, observe_replay
from test_entry_replay_growth import admission_histories as admission_histories
from test_journal_session_growth import ATTEMPT, finite_source, session_fixture
from test_journal_session_growth import native_session_checkpoints as native_session_checkpoints
from test_semantic_edge import observed_work, parameter_octets
from test_semantic_owner import snapshot


class LifecycleCursor(sqlite3.Cursor):
    def record(self, row):
        owner = self.connection.observer
        if owner.recording and row is not None:
            owner.metrics["returned_rows"] += 1
            owner.metrics["returned_metadata_octets"] += parameter_octets(row)
        return row

    def fetchone(self):
        return self.record(super().fetchone())

    def fetchall(self):
        return [self.record(row) for row in super().fetchall()]

    def __next__(self):
        return self.record(super().__next__())


class LifecycleConnection(sqlite3.Connection):
    def __init__(self, *args, observer, **kwargs):
        self.observer = observer
        super().__init__(*args, **kwargs)
        self.closed = False
        self.set_trace_callback(self.trace)
        self.set_progress_handler(self.tick, 1)
        self.set_authorizer(self.authorize)

    def trace(self, _statement):
        if self.observer.recording:
            self.observer.metrics["sql_statements"] += 1

    def tick(self):
        if self.observer.recording:
            self.observer.metrics["sqlite_vm_instructions"] += 1
        return 0

    def authorize(self, action, table, column, _database, _trigger):
        if (
            self.observer.recording
            and action == sqlite3.SQLITE_READ
            and table in {"assertions", "frames", "identities", "journal_validation_frame"}
            and column == "value"
        ):
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    def execute(self, sql, parameters=()):
        owner = self.observer
        if owner.recording:
            owner.attempts["sql_attempts"] += 1
            owner.statements.append((sql, parameters, None))
            if sql.startswith(("SELECT", "UPDATE", "DELETE")) and "sqlite_master" not in sql:
                # The sole empty-frame probe belongs to fresh initialization.
                if sql == "SELECT 1 FROM frames LIMIT 1":
                    assert owner.initializing
                else:
                    owner.recording = False
                    try:
                        with closing(
                            super().execute("EXPLAIN QUERY PLAN " + sql, parameters)
                        ) as cursor:
                            plan = " ".join(row[3] for row in cursor.fetchall())
                    finally:
                        owner.recording = True
                    assert "SEARCH" in plan and "SCAN" not in plan and "USE TEMP" not in plan
        before = self.total_changes
        result = self.cursor(factory=LifecycleCursor).execute(sql, parameters)
        if owner.recording:
            delta = self.total_changes - before
            owner.changes += delta
            owner.metrics["changed_rows"] += delta
        return result

    def executescript(self, script):
        if self.observer.recording:
            self.observer.attempts["sql_attempts"] += 1
        before = self.total_changes
        result = super().executescript(script)
        if self.observer.recording:
            delta = self.total_changes - before
            self.observer.changes += delta
            self.observer.metrics["changed_rows"] += delta
        return result

    def close(self):
        result = super().close()
        self.closed = True
        if self.observer.recording:
            self.observer.metrics["closed_databases"] += 1
        return result


class LifecycleRecorder:
    def __init__(self, *, initializing=False):
        self.recording = False
        self.initializing = initializing
        self.attempts = {"database_open_attempts": 0, "file_open_attempts": 0, "sql_attempts": 0}
        self.changes = 0
        self.statements = []
        self.connections = []
        self.metrics = {
            name: 0
            for name in (
                "sql_statements",
                "returned_rows",
                "changed_rows",
                "returned_metadata_octets",
                "bound_parameter_octets",
                "written_value_octets",
                "sqlite_vm_instructions",
                "native_hashed_octets",
                "decoded_json_octets",
                "encoded_json_octets",
                "source_payload_octets",
                "adjacency_rows",
                "source_verifications",
                "opened_databases",
                "closed_databases",
                "opened_files",
                "closed_files",
                "file_syncs",
                "directory_scans",
                "directory_entries",
                "unlinks",
                "removed_directories",
                "release_callbacks",
                "retention_callbacks",
            )
        }

    @property
    def total_changes(self):
        return self.changes

    def source(self, original):
        owner = self

        class Source:
            @property
            def binding(self):
                return original.binding

            def verify_current(self):
                recording = owner.recording
                if recording:
                    owner.metrics["source_verifications"] += 1
                    owner.recording = False
                try:
                    return original.verify_current()
                finally:
                    owner.recording = recording

            def read_window(self, offset, maximum_octets):
                result = original.read_window(offset, maximum_octets)
                if owner.recording:
                    owner.metrics["source_payload_octets"] += len(result)
                return result

            def read_exact(self, offset, length):
                result = original.read_exact(offset, length)
                if owner.recording:
                    owner.metrics["source_payload_octets"] += len(result)
                return result

        return Source()

    def callback(self, original):
        def release():
            recording = self.recording
            if recording:
                self.metrics["release_callbacks"] += 1
                self.recording = False
            try:
                return original()
            finally:
                self.recording = recording

        return release

    def retention_callback(self, original):
        def retain(value):
            recording = self.recording
            if recording:
                self.metrics["retention_callbacks"] += 1
                self.recording = False
            try:
                return original(value)
            finally:
                self.recording = recording

        return retain

    @contextmanager
    def kernel(self):
        owner = self
        real_connect, real_scandir = sqlite3.connect, os.scandir

        def connect(*args, **kwargs):
            assert "factory" not in kwargs
            kwargs["factory"] = lambda *values, **options: LifecycleConnection(
                *values, observer=owner, **options
            )
            if owner.recording:
                owner.attempts["database_open_attempts"] += 1
            result = real_connect(*args, **kwargs)
            owner.connections.append(result)
            if owner.recording:
                owner.metrics["opened_databases"] += 1
            return result

        def file_operation(function, metric):
            def invoke(*args, **kwargs):
                if owner.recording and metric == "opened_files":
                    owner.attempts["file_open_attempts"] += 1
                result = function(*args, **kwargs)
                if owner.recording:
                    owner.metrics[metric] += 1
                return result

            return invoke

        class Scan(Iterator):
            def __init__(self, value):
                self.value = value

            def __iter__(self):
                return self

            def __next__(self):
                result = next(self.value)
                if owner.recording:
                    owner.metrics["directory_entries"] += 1
                return result

            def __enter__(self):
                self.value.__enter__()
                return self

            def __exit__(self, *args):
                return self.value.__exit__(*args)

            def close(self):
                return self.value.close()

        def scandir(*args, **kwargs):
            result = real_scandir(*args, **kwargs)
            if owner.recording:
                owner.metrics["directory_scans"] += 1
            return Scan(result)

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(sqlite3, "connect", connect)
            patch.setattr(os, "scandir", scandir)
            for name, metric in (
                ("open", "opened_files"),
                ("close", "closed_files"),
                ("fsync", "file_syncs"),
                ("unlink", "unlinks"),
                ("rmdir", "removed_directories"),
            ):
                patch.setattr(os, name, file_operation(getattr(os, name), metric))
            yield

    def measured(self, operation, catalog, dimensions, invoke):
        self.metrics = {name: 0 for name in self.metrics}
        self.statements, self.changes = [], 0
        self.attempts = {name: 0 for name in self.attempts}

        # Codec/hash adapters in observe_replay execute the exact real operations.
        # The kernel records SQL and explicit syscalls; it adds no second codec wrapper.
        def inputs(parent, child, _args, _kwargs):
            tags = [
                name
                for name, candidate in growth_binding(operation).delegates
                if candidate is child
            ]
            if tags and "source_checks" in parent:
                tag = tags[0]
                return {
                    item.name: parent[tag + "_" + item.name]
                    for item in growth_binding(child).declaration.dimensions
                }
            return {
                item.name: parent[item.name]
                for item in growth_binding(child).declaration.dimensions
            }

        with observe_replay(
            self,
            catalog,
            dimensions,
            root_operation=operation,
            input_dimensions=inputs,
        ) as (frame, complete):
            self.recording = True
            try:
                result = invoke()
            finally:
                self.recording = False
        primitive = observed_work(self, {key: 0 for key in self.metrics}, 0, 0)
        if operation.__name__ == "open_retained":
            primitive.update(self.attempts)
            primitive.update(
                opened_database_limit=primitive["opened_databases"],
                closed_database_limit=primitive["closed_databases"],
                source_check_limit=primitive["source_verifications"],
                transition_call_limit=sum(
                    len(frame["delegates"][tag]) for tag in ("initialize", "replacement")
                ),
                lifecycle_call_limit=sum(map(len, frame["delegates"].values())),
            )
        sample = complete(frame, primitive)
        validate_execution_samples(
            project_bindings((growth_binding(operation),)),
            growth_binding(operation).declaration.id,
            [sample],
        )
        return result, sample


def lifecycle_dimensions(
    operation,
    *,
    metadata,
    rows=0,
    checkpoint_rows=0,
    initializing=False,
    default_catalog=False,
    release_callback=False,
    catalog_octets=0,
    catalog_entries=0,
):
    retained = operation.__name__ == "open_retained"
    closing = operation is JournalValidationSession.close
    values = {
        "retained_rows": rows,
        "metadata_row_octets": metadata,
        "constructed_catalog_octets": catalog_octets if default_catalog else 0,
        "constructed_catalog_entries": catalog_entries if default_catalog else 0,
        "default_catalog": int(default_catalog),
        "source_checks": int(retained),
        "release_callbacks": int(release_callback),
    }
    if retained:
        values.update(connection_admissions=1, rejected_connections=0)
    for tag, child in growth_binding(operation).delegates:
        values[tag + "_calls"] = int(
            initializing
            if tag == "initialize"
            else not initializing
            if tag == "replacement"
            else retained
        )
        for item in growth_binding(child).declaration.dimensions:
            if item.name == "retained_rows":
                value = (
                    1
                    if initializing and tag == "initialize"
                    else 2
                    if initializing
                    else checkpoint_rows
                )
            else:
                value = metadata
            values[tag + "_" + item.name] = value
    assert not closing or values["source_checks"] == 0
    return values


def packaged_catalog():
    from importlib.resources import files

    from riverhog_provenance_contracts import schemas

    entries = tuple(files(schemas.__package__).joinpath("schemas").iterdir())
    size = sum(len(entry.read_bytes()) for entry in entries if entry.name.endswith(".json"))
    return size, len(entries)


def constructor_samples(
    tmp_path, catalog, build, *, samples, default_cases=True, stream=True, retained=True
):
    from uuid import UUID

    from riverhog_provenance_contracts import schemas

    catalog_octets, catalog_entries = packaged_catalog()
    selections = (False, True, "cold") if default_cases else (False,)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(store_owner, "uuid4", lambda: UUID("f" * 32))
        for default in selections if stream else ():
            if default == "cold":
                schemas._sealed_core.cache_clear()
            recorder = LifecycleRecorder(initializing=True)
            operation = JournalValidationSession.__init__
            dimensions = lifecycle_dimensions(
                operation,
                metadata=32768,
                initializing=True,
                default_catalog=bool(default),
                catalog_octets=catalog_octets,
                catalog_entries=catalog_entries,
            )
            retained_handles = []
            retain = recorder.retention_callback(retained_handles.append)
            with recorder.kernel():
                session, sample = recorder.measured(
                    operation,
                    catalog,
                    dimensions,
                    lambda default=default, retain=retain: JournalValidationSession(
                        (),
                        attempt_id=ATTEMPT,
                        catalog=None if default else catalog,
                        connection_factory=lambda *values, **options: sqlite3.connect(
                            *values, **options
                        ),
                        on_storage_open=retain,
                    ),
                )
                samples.append(sample)
                assert retained_handles == [session]
                assert sample["metrics"]["retention_callbacks"] == 1
                try:
                    assert session._ledger.load().checkpoint.version == 0
                    assert session._input.cursor.byte_offset == 0
                    assert session._replay._current is None
                    assert not session._store.db.in_transaction
                    assert session._store.frame_count == 0
                finally:
                    session.close()
        for ordinal, regime in enumerate((REGIMES[0], REGIMES[8], REGIMES[12]) if retained else ()):
            case = build(regime)
            source = finite_source({**case, "regime": regime}, catalog, window=7)
            directory = tmp_path / f"retained-create-{ordinal}"
            directory.mkdir()
            recorder = LifecycleRecorder(initializing=True)
            operation = JournalValidationSession.open_retained.__func__
            metadata = 32768 + 2 * octets((source.binding.to_json(), str(regime[4])))
            dimensions = lifecycle_dimensions(operation, metadata=metadata, initializing=True)
            callback_calls, retained_handles = [], []
            with recorder.kernel():
                session, sample = recorder.measured(
                    operation,
                    catalog,
                    dimensions,
                    partial(
                        JournalValidationSession.open_retained,
                        directory,
                        recorder.source(source),
                        attempt_id=ATTEMPT,
                        worker_epoch=regime[4],
                        create=True,
                        catalog=catalog,
                        on_storage_close=recorder.callback(partial(callback_calls.append, 1)),
                        on_storage_open=recorder.retention_callback(retained_handles.append),
                    ),
                )
                samples.append(sample)
                assert sample["metrics"]["opened_databases"] == 1
                assert sample["metrics"]["closed_databases"] == 0
                assert sample["metrics"]["retention_callbacks"] == 1
                assert retained_handles == [session]
                try:
                    before = session._ledger.load()
                    assert before.checkpoint.version == before.checkpoint.frame_ordinal == 0
                    assert session._input.cursor.byte_offset == 0
                    assert session._replay._current is None
                    assert callback_calls == []
                    assert source.calls == [("verify",)]
                    assert not session._store.db.in_transaction
                    assert snapshot(session._store.db)["journal_validation_frame"] == []
                finally:
                    session.close()
            assert callback_calls == [1]
            assert (directory / "snapshot.sqlite3").is_file()


def replacement_samples(catalog, cases, *, samples):
    seen = set()
    for case in cases:
        if case["mode"] != "retained":
            continue
        key = (case["regime"], case["key"], case["binding"].source_sha256)
        if key in seen:
            continue
        seen.add(key)
        recorder = LifecycleRecorder()
        with recorder.kernel(), session_fixture(case, catalog, cold=True) as previous:
            directory = previous._store._storage.directory
            original = snapshot(previous._store.db)
            before = previous._ledger.load()
            epoch = previous._ledger.worker_epoch + 1
            source = previous._source
            input_state = previous._input.state
            oracle_directory = Path(mkdtemp(prefix="lifecycle-oracle-", dir=directory.parent))
            with closing(sqlite3.connect(oracle_directory / "snapshot.sqlite3")) as destination:
                previous._store.db.backup(destination)
            previous.close()
            with closing(
                JournalValidationSession.open_retained(
                    oracle_directory,
                    source,
                    attempt_id=ATTEMPT,
                    worker_epoch=epoch,
                    catalog=catalog,
                )
            ) as oracle:
                expected = snapshot(oracle._store.db)
                expected_input = oracle._input.state
                expected_parent = oracle._ledger.load()
            operation = JournalValidationSession.open_retained.__func__
            metadata = 32768 + 2 * octets((source.binding.to_json(), before, str(epoch)))
            dimensions = lifecycle_dimensions(
                operation,
                metadata=metadata,
                rows=sum(len(rows) for rows in original.values()),
                checkpoint_rows=sum(
                    len(original[name])
                    for name in ("journal_validation", "journal_prefix", "frames")
                ),
            )
            session, sample = recorder.measured(
                operation,
                catalog,
                dimensions,
                lambda directory=directory, recorder=recorder, source=source, epoch=epoch: (
                    JournalValidationSession.open_retained(
                        directory,
                        recorder.source(source),
                        attempt_id=ATTEMPT,
                        worker_epoch=epoch,
                        catalog=catalog,
                    )
                ),
            )
            samples.append(sample)
            try:
                after = session._ledger.load()
                assert after.prefix == before.prefix
                assert after.checkpoint == replace(
                    before.checkpoint,
                    version=before.checkpoint.version + 1,
                )
                assert session._ledger.worker_epoch == epoch
                assert session._input.state == input_state
                assert session._replay._current is None
                changed = snapshot(session._store.db)
                assert changed == expected
                assert after == expected_parent and session._input.state == expected_input
                assert {
                    name: rows for name, rows in changed.items() if name != "journal_validation"
                } == {name: rows for name, rows in original.items() if name != "journal_validation"}

            finally:
                session.close()
    assert seen


def close_samples(catalog, cases, *, samples):
    selected = {}
    for case in cases:
        key = case["mode"], case["regime"], case["key"], case["binding"].source_sha256
        selected.setdefault(key, case)
    for case in selected.values():
        recorder = LifecycleRecorder()
        with recorder.kernel(), session_fixture(case, catalog, cold=False) as session:
            storage = session._store._storage
            assert storage._readers == 1
            directory = storage.directory
            retained = case["mode"] == "retained"
            operation = JournalValidationSession.close
            dimensions = lifecycle_dimensions(operation, metadata=0)
            _, sample = recorder.measured(operation, catalog, dimensions, session.close)
            samples.append(sample)
            assert not session._store._cleanup.alive and storage._closed
            assert storage._readers == 0
            assert directory.exists() == retained
            _, again = recorder.measured(operation, catalog, dimensions, session.close)
            samples.append(again)
            assert all(value == 0 for value in again["metrics"].values())


def reader_lifetime_samples(tmp_path, catalog, build, *, samples):
    case = build(REGIMES[0])
    source = finite_source({**case, "regime": REGIMES[0]}, catalog, window=65536)
    for retained in (False, True):
        for readers in (0, 2):
            recorder = LifecycleRecorder(initializing=True)
            released = []
            with recorder.kernel():
                if retained:
                    directory = tmp_path / f"reader-lease-{readers}"
                    directory.mkdir()
                    session = JournalValidationSession.open_retained(
                        directory,
                        source,
                        attempt_id=ATTEMPT,
                        worker_epoch=2,
                        create=True,
                        catalog=catalog,
                        on_storage_close=recorder.callback(partial(released.append, 1)),
                    )
                else:
                    session = JournalValidationSession(
                        (case["raw"],), attempt_id=ATTEMPT, catalog=catalog
                    )
                borrowed = []
                try:
                    for _ in range(100000):
                        if session.advance().complete:
                            break
                    else:
                        pytest.fail("finite native reader-lifetime fixture did not finish")
                    borrowed = [session.result() for _ in range(readers)]
                    storage = session._store._storage
                    assert storage._readers == readers + 1
                    assert released == []
                    operation = JournalValidationSession.close
                    dimensions = lifecycle_dimensions(
                        operation,
                        metadata=0,
                        release_callback=retained and not readers,
                    )
                    _, sample = recorder.measured(operation, catalog, dimensions, session.close)
                    samples.append(sample)
                    assert storage._readers == readers
                    assert storage._closed == (not readers)
                    assert sample["metrics"]["closed_databases"] == int(not readers)
                    assert released == ([1] if retained and not readers else [])
                    for index, reader in enumerate(borrowed):
                        assert reader.journal_bytes == len(case["raw"])
                        assert (
                            list(reader.graph["artifacts"])
                            == case["candidate"].document["body"]["assertions"]["artifacts"]
                        )
                        reader.close()
                        assert storage._readers == readers - index - 1
                    assert storage._closed
                    assert released == ([1] if retained else [])
                    assert storage.directory.exists() == retained
                finally:
                    session.close()
                    for reader in borrowed:
                        reader.close()


@pytest.mark.growth_witness(
    "riverhog-native-session-lifecycle-construction/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.journal_session.JournalValidationSession.__init__",
)
def test_actual_native_session_construction(
    tmp_path, catalog, admission_histories, record_property
):
    samples = []
    record_property("growth_samples", samples)
    constructor_samples(tmp_path, catalog, admission_histories, samples=samples, retained=False)


@pytest.mark.growth_witness(
    "riverhog-native-session-lifecycle-retained-open/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.journal_session.JournalValidationSession.open_retained",
)
def test_actual_native_retained_session_construction(
    tmp_path, catalog, admission_histories, record_property
):
    samples = []
    record_property("growth_samples", samples)
    constructor_samples(tmp_path, catalog, admission_histories, samples=samples, stream=False)


@pytest.mark.growth_witness(
    "riverhog-native-session-lifecycle-replacement/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.journal_session.JournalValidationSession.open_retained",
)
def test_actual_native_session_replacement(catalog, native_session_checkpoints, record_property):
    samples = []
    record_property("growth_samples", samples)
    _, cases = native_session_checkpoints
    replacement_samples(catalog, cases, samples=samples)


@pytest.mark.growth_witness(
    "riverhog-native-session-lifecycle-close/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.journal_session.JournalValidationSession.close",
)
def test_actual_native_session_close(catalog, native_session_checkpoints, record_property):
    samples = []
    record_property("growth_samples", samples)
    _, cases = native_session_checkpoints
    close_samples(catalog, cases, samples=samples)


@pytest.mark.growth_witness(
    "riverhog-native-session-lifecycle-reader-release/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.journal_session.JournalValidationSession.close",
)
def test_actual_native_session_reader_lifetimes(
    tmp_path, catalog, admission_histories, record_property
):
    samples = []
    record_property("growth_samples", samples)
    reader_lifetime_samples(tmp_path, catalog, admission_histories, samples=samples)
