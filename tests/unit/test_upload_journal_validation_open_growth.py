from __future__ import annotations

import fcntl
import hashlib
import importlib
import os
import sqlite3
import sys
from collections import defaultdict
from contextlib import contextmanager, nullcontext
from dataclasses import asdict, fields
from pathlib import Path
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.journal_validation_workspace import LostJournalValidationWorkspace
from riverhog_core.services import upload_journal_validation_owner as owner
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import graph_certificates as certificate_codec
from riverhog_provenance import replay_binding as codec_owner
from sqlalchemy import event

from tests.unit import test_upload_journal_certificate_lifecycle_growth as certificate_tests
from tests.unit import test_upload_journal_owned_source_growth as fences
from tests.unit import test_upload_journal_source_construction_growth as construction
from tests.unit import test_upload_journal_validation_jobs_growth as job_tests
from tests.unit.test_journal_capture import retained
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_staged_journal_range import JOURNAL
from tests.unit.test_upload_journal_certificates import ready
from tests.unit.test_upload_journal_source_growth import octets, source_metadata_octets

jobs, source_owner = owner.jobs, owner.source_owner
certificates_owner, replay = fences.certificates_owner, fences.replay
OPEN = owner.UploadJournalValidationOwner._open
CHILDREN = dict(owner._open_children())
NATIVE, PROGRESS = CHILDREN["native"], CHILDREN["progress"]
FUNCTIONS = tuple(
    dict.fromkeys(
        (
            *fences.FUNCTIONS,
            *(
                operation
                for tag, operation in CHILDREN.items()
                if tag not in {"native", "progress"}
            ),
        )
    )
)
LOCATIONS = {
    CHILDREN["construct_source"]: (owner.UploadJournalValidationSource, "__init__"),
    CHILDREN["construct_certificate"]: (owner.UploadJournalGraphCertificates, "__init__"),
    **{
        getattr(jobs, name): (jobs, name)
        for name in (
            "select_job",
            "outcome",
            "creation_current",
            "claim_worker",
            "publish_progress",
        )
    },
}


def native_helpers():
    path = Path(fences.__file__).resolve().parents[2] / "packages/riverhog-provenance/tests"
    with pytest.MonkeyPatch.context() as patch:
        patch.syspath_prepend(str(path))
        return (
            importlib.import_module("test_journal_session_lifecycle_growth"),
            importlib.import_module("test_journal_session_growth"),
        )


class Recorder(fences.Recorder):
    def __init__(self, case):
        super().__init__(fences.OWNED_CURRENT, {}, functions=FUNCTIONS, locations=LOCATIONS)
        self.case = case
        self.records = defaultdict(list)
        self.native_recorder = None
        self.inside_native = False
        self.projections = {}
        for operation in (*FUNCTIONS, OPEN, NATIVE, PROGRESS):
            binding = growth_binding(operation)
            projection = project_bindings((binding,))
            self.projections[operation] = projection
            self.digests.update(
                {
                    identity: hashlib.sha256(canonical_json_bytes(value)).hexdigest()
                    for identity, value in projection["declarations"].items()
                }
            )
            for bound in binding.declaration.bounds:
                self.full.setdefault(bound.metric, 0)
                self.application.setdefault(bound.metric, 0)
        self.lease_values = defaultdict(int)
        self.lease_active = False
        self.own_path_probes = 0

    def codec_recording(self):
        return self.active and (not self.inside_native or not self.native_recorder.recording)

    @contextmanager
    def paused(self):
        active, self.active = self.active, False
        lease, self.lease_active = self.lease_active, False
        native = None if self.native_recorder is None else self.native_recorder.recording
        if native is not None:
            self.native_recorder.recording = False
        try:
            yield
        finally:
            self.active = active
            self.lease_active = lease
            if native is not None:
                self.native_recorder.recording = native

    def dimensions(self, function, args, kwargs):
        with self.paused():
            rows = fences.snapshot(self.case.engine)
            width = max([1, *(octets(row) for values in rows.values() for row in values)])
            if function is CHILDREN["construct_source"]:
                binding = replay._binding(args[2], self.case.catalog)
                return {
                    "source_control_octets": sum(
                        octets(getattr(binding, item.name)) for item in fields(binding)
                    )
                }
            if function is fences.PROBE:
                return {
                    "retained_journals": len(rows[certificates_owner.Journal.__tablename__])
                    + len(rows[certificates_owner.Scan.__tablename__])
                }
            if function is fences.CURRENT:
                current = args[0]
                _good, raw = fences.PROBE(self.case.factory, current.source)
                return {
                    "retained_journals": len(rows[certificates_owner.Journal.__tablename__])
                    + len(rows[certificates_owner.Scan.__tablename__]),
                    "binding_present": int(raw is not None),
                    "binding_octets": octets(raw),
                    "source_metadata_octets": source_metadata_octets(current.source),
                }
            if function in {CHILDREN["construct_certificate"], fences.CERTIFICATE}:
                dimensions, *_ = certificate_tests.dimensions(self.case)
                return dimensions
            if function in {fences.SOURCE, fences.OWNED_CURRENT}:
                current = args[0]
                calls = {
                    "worker": (current.factory, current.job),
                    "source": (current,),
                    "current": (current.source,),
                    "certificate": (current.certificates,),
                }
                values = {"metadata_row_octets": width}
                for tag, child in growth_binding(function).delegates:
                    values[tag + "_calls"] = 1
                    values.update(
                        {
                            tag + "_" + name: value
                            for name, value in self.dimensions(child, calls[tag], {}).items()
                        }
                    )
                return values
            job = (
                args[0]
                if function is jobs.outcome
                else args[1]
                if function is fences.WORKER
                else args[2]
                if len(args) > 2
                else None
            )
            controls = [self.case.source_binding, self.case.certificate_binding]
            if job is not None:
                controls.extend(
                    (
                        job.source_binding_json,
                        job.certificate_binding_json,
                        job.progress_json,
                        job.outcome_json,
                        job.failure,
                    )
                )
                width = max(width, octets(tuple(asdict(job).values())))
            if function is jobs.publish_progress and args[3] is not None:
                controls.append(job_tests.canonical(args[3]))
            controls.extend(value for value in kwargs.values() if isinstance(value, str))
            return {
                "retained_rows": len(rows[jobs.Job.__tablename__])
                + len(rows[jobs.Journal.__tablename__]),
                "metadata_row_octets": max(width, octets(controls)),
                "control_octets": octets(controls),
            }

    def invoke(self, function, action, args=(), kwargs=None):
        if self.active:
            assert not self.owner._lock.locked()
        dimensions = self.dimensions(function, args, kwargs or {})
        binding = self.bindings[function]
        before = self.full.copy(), self.application.copy()
        changes = sum(db.total_changes for db in self.native)
        node = {
            "declaration": binding.declaration.id,
            "declaration_sha256": self.digests[binding.declaration.id],
            "dimensions": dimensions,
            "delegates": {tag: [] for tag, _ in binding.delegates},
        }
        self.stack.append((function, node))
        result, error = None, None
        try:
            result = action()
            return result
        except BaseException as caught:
            error = caught
            raise
        finally:
            self.stack.pop()
            if error is not None:
                raise error
            index = int(function in self.application_functions)
            now = (self.full, self.application)[index]
            values = {name: value - before[index][name] for name, value in now.items()}
            values["changed_rows"] = sum(db.total_changes for db in self.native) - changes
            values["connection_statements"] = values["driver_statements"] - values["sql_statements"]
            if function is fences.PROBE:
                values["binding_octets"] = octets(None if result is None else result[1])
            elif function is fences.CURRENT:
                values["binding_octets"] = values["returned_metadata_octets"]
            for tag, children in node["delegates"].items():
                values[tag + "_calls"] = values[tag + "_calls_limit"] = len(children)
                for bound in growth_binding(dict(binding.delegates)[tag]).declaration.bounds:
                    values[tag + "_" + bound.metric] = sum(
                        child["metrics"][bound.metric] for child in children
                    )
            node["metrics"] = {
                bound.metric: values[bound.metric] for bound in binding.declaration.bounds
            }
            validate_execution_samples(self.projections[function], binding.declaration.id, [node])
            if self.stack:
                parent, parent_node = self.stack[-1]
                tags = [tag for tag, child in growth_binding(parent).delegates if child is function]
                assert len(tags) == 1
                parent_node["delegates"][tags[0]].append(node)
            else:
                self.records[function].append(node)

    @contextmanager
    def observe(self, engine, *, cold):
        recorder = self
        engine.dispose()
        dbapi, connect = engine.dialect.dbapi, engine.dialect.dbapi.connect
        real_uuid = jobs.uuid4
        real_loads, real_encode, real_hash = (
            codec_owner.json.loads,
            canonical_json_bytes,
            hashlib.sha256,
        )

        class Cursor(sqlite3.Cursor):
            application = False
            closed = False

            def execute(self, sql, parameters=()):
                recorder.executing_application = self.application
                if recorder.active:
                    recorder.full["bound_parameter_octets"] += octets(parameters)
                    if self.application:
                        recorder.application["bound_parameter_octets"] += octets(parameters)
                return super().execute(sql, parameters)

            def fetchone(self):
                recorder.executing_application = self.application
                return recorder.row(self, super().fetchone())

            def fetchall(self):
                recorder.executing_application = self.application
                return [recorder.row(self, row) for row in super().fetchall()]

            def fetchmany(self, *args):
                recorder.executing_application = self.application
                return [recorder.row(self, row) for row in super().fetchmany(*args)]

            def __next__(self):
                recorder.executing_application = self.application
                return recorder.row(self, super().__next__())

            def close(self):
                super().close()
                self.closed = True

        class Connection(sqlite3.Connection):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                recorder.native.append(self)
                self.set_progress_handler(self.tick, 1)
                self.set_trace_callback(self.trace)
                self.set_authorizer(self.authorize)

            def cursor(self, *args, **kwargs):
                kwargs["factory"] = Cursor
                cursor = super().cursor(*args, **kwargs)
                recorder.cursors.append(cursor)
                return cursor

            def trace(self, _statement):
                if recorder.active:
                    recorder.full["driver_statements"] += 1
                    if recorder.executing_application:
                        recorder.application["driver_statements"] += 1

            def tick(self):
                if recorder.active:
                    recorder.full["sqlite_vm_instructions"] += 1
                    if recorder.executing_application:
                        recorder.application["sqlite_vm_instructions"] += 1
                return 0

            def authorize(self, action, table, column, _database, _trigger):
                if (
                    recorder.active
                    and action == sqlite3.SQLITE_READ
                    and column in {"content", "canonical_json", "exact_json", "schema_json"}
                ):
                    function = recorder.stack[-1][0] if recorder.stack else None
                    if column != "content" or table not in recorder.payload_operations.get(
                        function, ()
                    ):
                        return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

        def observed_connect(*args, **kwargs):
            kwargs.setdefault("factory", Connection)
            return connect(*args, **kwargs)

        def sql(_conn, cursor, statement, parameters, _context, many):
            assert not many and isinstance(cursor, Cursor)
            cursor.application = True
            if recorder.active:
                self.full["sql_statements"] += 1
                self.application["sql_statements"] += 1
                self.statements.append((statement, parameters))

        def record(name, value):
            if self.codec_recording():
                self.full[name] += value
                self.application[name] += value

        def loads(value, *args, **kwargs):
            record("json_decodes", 1)
            record("json_decoded_octets", octets(value))
            return real_loads(value, *args, **kwargs)

        def encode(*args, **kwargs):
            value = real_encode(*args, **kwargs)
            record("json_encodes", 1)
            record("json_encoded_octets", len(value))
            return value

        class Hash:
            def __init__(self, actual):
                self.actual = actual

            def update(self, value):
                record("hashed_octets", len(value))
                self.actual.update(value)

            def copy(self):
                return Hash(self.actual.copy())

            def __getattr__(self, name):
                return getattr(self.actual, name)

        def sha256(value=b"", *args, **kwargs):
            record("hashed_octets", len(value))
            return Hash(real_hash(value, *args, **kwargs))

        def uuid4():
            record("uuid_values", 1)
            return real_uuid()

        event.listen(engine, "before_cursor_execute", sql)
        try:
            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(dbapi, "connect", observed_connect)
                patch.setattr(jobs, "uuid4", uuid4)
                if not cold:
                    with engine.connect():
                        pass
                for module in (
                    source_owner,
                    certificates_owner,
                    replay,
                    codec_owner,
                    certificate_codec,
                    jobs,
                ):
                    if hasattr(module, "canonical_json_bytes"):
                        patch.setattr(module, "canonical_json_bytes", encode)
                    if hasattr(module, "hashlib"):
                        patch.setattr(module, "hashlib", SimpleNamespace(sha256=sha256))
                    if hasattr(module, "json"):
                        patch.setattr(module, "json", SimpleNamespace(loads=loads))
                for function, (target, name) in self.locations.items():

                    def call(*args, _function=function, **kwargs):
                        return self.invoke(
                            _function, lambda: _function(*args, **kwargs), args, kwargs
                        )

                    patch.setattr(target, name, call)
                self.active = True
                try:
                    yield
                finally:
                    self.active = False
        finally:
            event.remove(engine, "before_cursor_execute", sql)
        assert not engine.pool.checkedout()
        assert all(cursor.closed for cursor in self.cursors)
        for statement, parameters in self.statements:
            with engine.connect() as connection:
                with connection.exec_driver_sql(
                    "EXPLAIN QUERY PLAN " + statement, parameters
                ) as result:
                    plan = " ".join(row[3] for row in result)
            assert "SCAN" not in plan and "USE TEMP" not in plan
            if statement.lstrip().upper().startswith(("SELECT", "UPDATE", "DELETE")):
                assert "SEARCH" in plan

    @contextmanager
    def controls(self):
        recorder = self
        match, uuid = codec_owner.re.fullmatch, codec_owner.require_canonical_uuid_urn
        previous = sys.getprofile()

        def add(name, amount=1):
            if recorder.codec_recording():
                recorder.full[name] += amount
                recorder.application[name] += amount

        def fullmatch(pattern, value, *args, **kwargs):
            add("regex_calls")
            add("regex_input_octets", octets(value))
            return match(pattern, value, *args, **kwargs)

        def canonical_uuid(value):
            add("uuid_validations")
            add("uuid_input_octets", octets(value))
            return uuid(value)

        def profile(frame, event, arg):
            if previous is not None:
                previous(frame, event, arg)
            if event == "call" and frame.f_code in construction.CALLS:
                add(construction.CALLS[frame.f_code])
            if (
                event == "return"
                and frame.f_code is CHILDREN["construct_source"].__code__
                and recorder.active
            ):
                binding = frame.f_locals["self"]._binding
                add("binding_fields", len(fields(binding)))
                add(
                    "scalar_text_octets",
                    octets((binding.source_bytes, binding.source_version, binding.frame_count)),
                )

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(codec_owner, "re", SimpleNamespace(fullmatch=fullmatch))
            patch.setattr(codec_owner, "require_canonical_uuid_urn", canonical_uuid)
            sys.setprofile(profile)
            try:
                yield
            finally:
                sys.setprofile(previous)

    @contextmanager
    def kernel(self):
        recorder = self
        actual_lease = owner.lease_journal_validation_workspace
        groups = {
            "open": "open_calls",
            "close": "close_calls",
            "stat": "stat_calls",
            "fstat": "stat_calls",
            "getuid": "uid_calls",
            "mkdir": "mkdir_calls",
            "fsync": "fsync_calls",
        }
        with pytest.MonkeyPatch.context() as patch:
            for name, metric in groups.items():
                actual = getattr(os, name)

                def call(*args, _actual=actual, _name=name, _metric=metric, **kwargs):
                    if recorder.active and recorder.lease_active:
                        recorder.lease_values[_metric] += 1
                        if _name in {"open", "mkdir", "stat"}:
                            recorder.lease_values["path_octets"] += len(os.fsencode(args[0]))
                    return _actual(*args, **kwargs)

                patch.setattr(os, name, call)
            actual_flock = fcntl.flock

            def flock(fd, flags):
                if recorder.active and recorder.lease_active:
                    recorder.lease_values["flock_calls"] += 1
                    assert flags == fcntl.LOCK_UN or flags & fcntl.LOCK_NB
                return actual_flock(fd, flags)

            patch.setattr(fcntl, "flock", flock)
            actual_exists = Path.exists

            def exists(path, *args, **kwargs):
                if recorder.active and not recorder.lease_active and not recorder.inside_native:
                    recorder.own_path_probes += 1
                return actual_exists(path, *args, **kwargs)

            patch.setattr(Path, "exists", exists)

            @contextmanager
            def lease(*args, **kwargs):
                context = actual_lease(*args, **kwargs)
                recorder.lease_active = True
                try:
                    directory = context.__enter__()
                finally:
                    recorder.lease_active = False
                try:
                    yield directory
                finally:
                    recorder.lease_active = True
                    try:
                        context.__exit__(*sys.exc_info())
                    finally:
                        recorder.lease_active = False

            patch.setattr(owner, "lease_journal_validation_workspace", lease)
            yield

    def native_call(self, operation, args, kwargs):
        assert not self.owner._lock.locked()
        lifecycle, session_tests = self.helpers
        if self.native_recorder is None:
            base = lifecycle.LifecycleRecorder

            class NativeRecorder(base):
                def measured(self, operation, catalog, dimensions, invoke):
                    self.metrics = {name: 0 for name in self.metrics}
                    pending, seen = [operation], set()
                    while pending:
                        child = pending.pop()
                        if child in seen:
                            continue
                        seen.add(child)
                        binding = growth_binding(child)
                        for bound in binding.declaration.bounds:
                            self.metrics.setdefault(bound.metric, 0)
                        pending.extend(child for _tag, child in binding.delegates)
                    self.metrics.setdefault("sha256_compression_calls", 0)
                    self.statements, self.changes = [], 0
                    self.attempts = {name: 0 for name in self.attempts}

                    def inputs(parent, child, _args, _kwargs):
                        tags = [
                            tag
                            for tag, candidate in growth_binding(operation).delegates
                            if candidate is child
                        ]
                        fields = growth_binding(child).declaration.dimensions
                        if tags and all(tags[0] + "_" + item.name in parent for item in fields):
                            return {item.name: parent[tags[0] + "_" + item.name] for item in fields}
                        return {item.name: parent[item.name] for item in fields}

                    error = None
                    with lifecycle.observe_replay(
                        self,
                        catalog,
                        dimensions,
                        root_operation=operation,
                        input_dimensions=inputs,
                        operation_owners={
                            session_tests._checkpoint_progress: session_tests.session_owner
                        },
                    ) as (frame, complete):
                        self.recording = True
                        try:
                            result = invoke()
                        except BaseException as caught:
                            error, result = caught, None
                        finally:
                            self.recording = False
                    if error is not None:
                        raise error
                    work = lifecycle.observed_work(self, {key: 0 for key in self.metrics}, 0, 0)
                    if operation is NATIVE:
                        work.update(self.attempts)
                        work.update(
                            opened_database_limit=work["opened_databases"],
                            closed_database_limit=work["closed_databases"],
                            source_check_limit=work["source_verifications"],
                            transition_call_limit=sum(
                                len(frame["delegates"][tag])
                                for tag in ("initialize", "replacement")
                            ),
                            lifecycle_call_limit=sum(map(len, frame["delegates"].values())),
                        )
                    sample = complete(frame, work)
                    validate_execution_samples(
                        project_bindings((growth_binding(operation),)),
                        growth_binding(operation).declaration.id,
                        [sample],
                    )
                    return result, sample, error

            self.native_recorder = NativeRecorder()
        native = self.native_recorder
        with self.paused():
            if operation is NATIVE:
                cls, directory, selected = args
                path = directory / "snapshot.sqlite3"
                rows, checkpoint_rows, controls, initializing = 0, 0, [], True
                if path.exists():
                    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as db:
                        names = [
                            row[0]
                            for row in db.execute(
                                "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT "
                                "LIKE 'sqlite_%'"
                            )
                        ]
                        for name in names:
                            count = db.execute('SELECT count(*) FROM "' + name + '"').fetchone()[0]
                            rows += count
                            if name in {"journal_validation", "journal_prefix", "frames"}:
                                checkpoint_rows += count
                            if name in {"journal_validation", "journal_prefix"}:
                                controls.extend(
                                    tuple(db.execute('SELECT * FROM "' + name + '"').fetchall())
                                )
                        initializing = "journal_validation" not in names
                metadata = 16384 + 2 * octets(
                    (
                        selected.binding.to_json(),
                        kwargs["certificates"].binding.to_json(),
                        controls,
                        str(kwargs["worker_epoch"]),
                    )
                )
                dimensions = lifecycle.lifecycle_dimensions(
                    operation,
                    metadata=metadata,
                    rows=rows,
                    checkpoint_rows=checkpoint_rows,
                    initializing=initializing,
                )
                dimensions["source_checks"] = 1
                dimensions["load_calls"] = 1
                native.initializing = initializing
                proxy = native.source(selected)
                kwargs = dict(kwargs)
                if kwargs.get("on_storage_open") is not None:
                    kwargs["on_storage_open"] = native.retention_callback(kwargs["on_storage_open"])

                def action():
                    return operation(cls, directory, proxy, **kwargs)
            else:
                current = args[0]
                before = current._ledger.load()
                original = session_tests.snapshot(current._store.db)
                width = 16384 + 2 * octets(
                    (
                        before,
                        current._ledger.binding,
                        current._source_binding,
                        getattr(current._input, "state", None),
                    )
                )
                leaf = session_tests.progress_dimensions(current, before)
                dimensions = {
                    "retained_rows": sum(map(len, original.values())),
                    "metadata_row_octets": width,
                    "source_verifications": 1,
                    "load_calls": 1,
                    "load_retained_rows": sum(
                        len(original[name])
                        for name in ("journal_validation", "journal_prefix", "frames")
                    ),
                    "load_metadata_row_octets": width,
                    "progress_calls": 1,
                    "progress_metadata_row_octets": leaf["metadata_row_octets"],
                    "progress_padding_blocks": leaf["padding_blocks"],
                }

                def action():
                    return operation(*args, **kwargs)

        self.inside_native = True
        try:
            with native.kernel() if operation is NATIVE else nullcontext():
                result, sample, error = native.measured(
                    operation, self.case.catalog, dimensions, action
                )
        finally:
            self.inside_native = False
        self.records[operation].append(sample)
        if error is not None:
            raise error
        return result

    def measure(self, current, slot, *, cold):
        self.owner = current
        self.helpers = native_helpers()
        self.native_recorder = None
        self.records.clear()
        self.lease_values.clear()
        self.own_path_probes = 0
        self.full = {name: 0 for name in self.full}
        self.application = {name: 0 for name in self.application}
        with self.paused():
            allrows = fences.snapshot(self.case.engine)
            width = max([1, *(octets(row) for values in allrows.values() for row in values)])
        result, error = None, None
        with self.kernel(), self.controls(), self.observe(self.case.engine, cold=cold):
            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(
                    owner.JournalValidationSession,
                    "open_retained",
                    classmethod(lambda *args, **kwargs: self.native_call(NATIVE, args, kwargs)),
                )
                patch.setattr(
                    owner.JournalValidationSession,
                    "progress",
                    lambda *args, **kwargs: self.native_call(PROGRESS, args, kwargs),
                )
                try:
                    result = OPEN(current, self.case.work, slot, self.case.catalog)
                except BaseException as caught:
                    error = caught
        binding = growth_binding(OPEN)
        dimensions = {
            "metadata_row_octets": width,
            "binding_output_octets": octets(
                (self.case.source_binding, self.case.certificate_binding)
            ),
            "lease_root_path_octets": len(os.fsencode(current.root)),
            "lease_workspace_identity_octets": 32,
        }
        children = {}
        metrics = {bound.metric: 0 for bound in binding.declaration.bounds}
        raw = set(self.full) & set(metrics)
        for name in raw:
            metrics[name] = self.full[name]
        metrics["changed_rows"] = sum(db.total_changes for db in self.native)
        for tag, operation in CHILDREN.items():
            records = self.records[operation]
            children[tag] = records
            dimensions[tag + "_calls"] = len(records)
            metrics[tag + "_calls"] = metrics[tag + "_calls_limit"] = len(records)
            for item in growth_binding(operation).declaration.dimensions:
                dimensions[tag + "_" + item.name] = max(
                    (node["dimensions"][item.name] for node in records), default=0
                )
            for bound in growth_binding(operation).declaration.bounds:
                metrics[tag + "_" + bound.metric] = sum(
                    node["metrics"][bound.metric] for node in records
                )
            if operation in {NATIVE, PROGRESS}:
                aliases = {
                    "driver_statements": "sql_statements",
                    "json_decoded_octets": "decoded_json_octets",
                    "json_encoded_octets": "encoded_json_octets",
                    "hashed_octets": "native_hashed_octets",
                }
                for name in raw:
                    metrics[name] += sum(
                        node["metrics"].get(aliases.get(name, name), 0) for node in records
                    )
        for name, value in self.lease_values.items():
            metrics["lease_" + name] = value
        metrics["lease_creation_callbacks"] = len(self.records[jobs.creation_current])
        metrics["own_path_probes"] = self.own_path_probes
        sample = {
            "declaration": binding.declaration.id,
            "declaration_sha256": self.digests[binding.declaration.id],
            "dimensions": dimensions,
            "metrics": metrics,
            "delegates": children,
        }
        validate_execution_samples(self.projections[OPEN], binding.declaration.id, [sample])
        return result, error, sample


@contextmanager
def build_cases(root, *, large=True):
    catalog = source_owner.admission_provenance_catalog()
    specifications = [(0, 1, False), (65537, 17, False), (0, 1, True)]
    if large:
        specifications.extend(((1024 * 1024, 1, False), (0, 1025, False)))
    from contextlib import ExitStack

    with ExitStack() as resources:
        cases = []
        for index, (payload, journals, malformed) in enumerate(specifications):
            path = root / str(index)
            engine, factory = resources.enter_context(
                retained(
                    path,
                    source(payload, journal_id=JOURNAL) + (b"malformed tail" if malformed else b""),
                )
            )
            factory.configure(autoflush=False, expire_on_commit=False)
            work, certificates = ready(factory)
            case = SimpleNamespace(
                engine=engine,
                factory=factory,
                work=work,
                catalog=catalog,
                mode=certificates._mode,
                path=path,
                malformed=malformed,
                recover=payload == 0 and journals == 1 and not malformed,
                source_binding=certificates.binding.source.to_json(),
                certificate_binding=certificates.binding.to_json(),
            )
            with factory() as session, session.begin():
                original = session.get(jobs.Journal, (work.collection_id, work.journal_id))
                saved = {
                    column.key: getattr(original, column.key)
                    for column in jobs.Journal.__table__.columns
                }
                session.add_all(
                    jobs.Journal(
                        **{
                            **saved,
                            "journal_id": f"urn:uuid:{number:08x}-0000-4000-8000-000000000000",
                        }
                    )
                    for number in range(1, journals)
                )
            cases.append(case)
        yield cases


@pytest.fixture(scope="module")
def open_cases(tmp_path_factory):
    with build_cases(tmp_path_factory.mktemp("owner-open-inputs")) as cases:
        yield cases


@pytest.mark.growth_witness(
    "riverhog-validation-owner-open-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_owner.UploadJournalValidationOwner._open",
)
def test_actual_opening_preserves_provider_costs_and_transferred_custody(
    open_cases, record_property
):
    samples = []
    record_property("growth_samples", samples)
    for case in open_cases:
        root = case.path / "native"
        current = owner.UploadJournalValidationOwner(case.factory, root)
        second = owner.UploadJournalValidationOwner(case.factory, root)
        key = case.work.collection_id, case.work.journal_id, case.work.attempt_id
        slot = current._reserve(key)
        assert slot is not None
        try:
            recorder = Recorder(case)
            result, error, sample = recorder.measure(current, slot, cold=True)
            samples.append(sample)
            if error is not None:
                raise error
            assert result is None and slot.session is not None and slot.source is not None
            assert sample["delegates"]["native"][0]["metrics"]["retention_callbacks"] == 1
            progress = slot.session.progress()
            job = slot.job
            assert progress.accepted_frames == 0 and job.worker_epoch == 1
            busy = second._reserve(key)
            assert busy is not None
            try:
                result, error, sample = Recorder(case).measure(second, busy, cold=False)
                samples.append(sample)
                if error is not None:
                    raise error
                assert result == owner.ValidationAdvance(False) and busy.session is None
                assert job_tests.selected(case.factory)["worker_epoch"] == job.worker_epoch
            finally:
                busy.lock.release()
            slot.lock.release()
            current.close()
            current = owner.UploadJournalValidationOwner(case.factory, root)
            slot = current._reserve(key)
            result, error, sample = Recorder(case).measure(current, slot, cold=False)
            samples.append(sample)
            if error is not None:
                raise error
            assert result is None and slot.job.worker_epoch == job.worker_epoch + 1
            after = slot.session.progress()
            assert (
                after.accepted_frames == progress.accepted_frames
                and after.accepted_sha256 == progress.accepted_sha256
            )
            if case.recover:
                catalog_progress = jobs.decode_progress(slot.job.progress_json)
                for _ in range(10000):
                    accepted = slot.session.advance()
                    if accepted.accepted_frames:
                        break
                else:
                    pytest.fail("finite native source did not accept its first frame")
                assert accepted.accepted_frames > catalog_progress.accepted_frames
                slot.lock.release()
                current.close()
                current = owner.UploadJournalValidationOwner(case.factory, root)
                slot = current._reserve(key)
                result, error, sample = Recorder(case).measure(current, slot, cold=True)
                samples.append(sample)
                if error is not None:
                    raise error
                assert result is None
                adopted = slot.session.progress()
                published = jobs.decode_progress(slot.job.progress_json)
                assert (
                    adopted.accepted_frames == published.accepted_frames == accepted.accepted_frames
                )
                assert (
                    adopted.accepted_octets == published.accepted_octets == accepted.accepted_octets
                )
                assert (
                    adopted.accepted_sha256 == published.accepted_sha256 == accepted.accepted_sha256
                )
            slot.lock.release()
            for _ in range(10000):
                step = current.advance(case.work, catalog=case.catalog)
                if step.outcome is not None:
                    break
            else:
                pytest.fail("finite owner input did not complete")
            assert bool(step.outcome.failure) == case.malformed
            slot = current._reserve(key)
            assert slot is not None
            result, error, sample = Recorder(case).measure(current, slot, cold=False)
            samples.append(sample)
            if error is not None:
                raise error
            assert result == owner.ValidationAdvance(False, step.outcome)
            assert slot.session is None and sample["dimensions"]["native_calls"] == 0
            with case.factory() as session, session.begin():
                record = session.get(jobs.Job, job.workspace_id)
                state, failure = record.state, record.failure
                record.state, record.failure = "lost", "exact retained storage lost"
            try:
                result, error, sample = Recorder(case).measure(current, slot, cold=True)
                samples.append(sample)
                assert isinstance(error, LostJournalValidationWorkspace)
                assert slot.session is None and sample["dimensions"]["native_calls"] == 0
            finally:
                with case.factory() as session, session.begin():
                    record = session.get(jobs.Job, job.workspace_id)
                    record.state, record.failure = state, failure
            with case.factory() as session, session.begin():
                journal = session.get(jobs.Journal, (case.work.collection_id, case.work.journal_id))
                previous_state = journal.state
                journal.state = "failed"
            try:
                result, error, sample = Recorder(case).measure(current, slot, cold=False)
                samples.append(sample)
                if error is not None:
                    raise error
                assert result == owner.ValidationAdvance(False)
                assert (
                    sample["dimensions"]["select_calls"]
                    == sample["dimensions"]["native_calls"]
                    == 0
                )
            finally:
                with case.factory() as session, session.begin():
                    session.get(
                        jobs.Journal, (case.work.collection_id, case.work.journal_id)
                    ).state = previous_state
        finally:
            if slot.lock.locked():
                slot.lock.release()
            current.close()
            second.close()
