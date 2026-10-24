from __future__ import annotations

import hashlib
import sqlite3
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.journal_validation_workspace import lease_journal_validation_workspace
from riverhog_core.services import collection_uploads as source_owner
from riverhog_core.services import upload_journal_certificates as certificates_owner
from riverhog_core.services import upload_journal_replay as replay
from riverhog_core.services import upload_journal_validation_jobs as jobs
from riverhog_core.services import upload_journal_validation_owner as owner
from riverhog_core.services.upload_journal_source import UploadJournalValidationSource
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import InvalidJournalGraphCertificate
from riverhog_provenance import replay_binding as codec_owner
from sqlalchemy import event, select

from tests.unit.test_upload_journal_certificate_lifecycle_growth import TABLES
from tests.unit.test_upload_journal_source_growth import octets, source_metadata_octets
from tests.unit.test_upload_journal_validation_jobs_growth import build_cases as job_cases
from tests.unit.test_upload_journal_validation_jobs_growth import read_job, reset

FUNCTIONS = (
    owner._OwnedSource.verify_source,
    owner._OwnedSource.verify_current,
    jobs.worker_current,
    UploadJournalValidationSource.verify_current,
    replay._probe,
    certificates_owner.UploadJournalGraphCertificates.verify_current,
)
SOURCE, OWNED_CURRENT, WORKER, CURRENT, PROBE, CERTIFICATE = FUNCTIONS
APPLICATION = {UploadJournalValidationSource.verify_current, replay._probe}
BASE_METRICS = (
    "sql_statements",
    "driver_statements",
    "connection_statements",
    "returned_rows",
    "returned_metadata_octets",
    "bound_parameter_octets",
    "sqlite_vm_instructions",
    "changed_rows",
    "source_payload_octets",
    "binding_octets",
    "json_decodes",
    "json_encodes",
    "json_decoded_octets",
    "json_encoded_octets",
    "hashed_octets",
    "uuid_values",
    "loaded_payload_octets",
    "payload_rows",
    "assembled_piece_octets",
)


def snapshot(engine):
    with engine.connect() as connection:
        return {
            model.__tablename__: tuple(
                connection.exec_driver_sql(f'SELECT * FROM "{model.__tablename__}"').fetchall()
            )
            for model in (*TABLES, jobs.Job)
        }


def input_dimensions(case, current, certificates, worker_good, source_good):
    rows = snapshot(case.engine)
    row_octets = max(octets(row) for values in rows.values() for row in values)
    with case.factory() as session:
        raw = session.scalar(select(certificates_owner.Scan.replay_binding_json))
    if not source_good:
        raw = None
    manifest = dict(zip(certificates_owner._MANIFEST_FIELDS, certificates._manifest, strict=True))
    source_json = certificates.binding.source.to_json()
    manifest_json = canonical_json_bytes(
        {
            "source": source_json,
            "mode": certificates._mode,
            "preparation": {
                name: str(value) if type(value) is int else value
                for name, value in manifest.items()
            },
        }
    )
    values = {
        jobs.worker_current: {
            "retained_rows": len(rows[jobs.Job.__tablename__])
            + len(rows[jobs.Journal.__tablename__]),
            "metadata_row_octets": row_octets,
            "control_octets": octets(
                (
                    current.job.source_binding_json,
                    current.job.certificate_binding_json,
                    current.job.progress_json,
                    current.job.outcome_json,
                    current.job.failure,
                )
            ),
        },
        replay._probe: {
            "retained_journals": len(rows[certificates_owner.Journal.__tablename__])
            + len(rows[certificates_owner.Scan.__tablename__])
        },
        UploadJournalValidationSource.verify_current: {
            "retained_journals": len(rows[certificates_owner.Journal.__tablename__])
            + len(rows[certificates_owner.Scan.__tablename__]),
            "binding_present": int(raw is not None),
            "binding_octets": octets(raw),
            "source_metadata_octets": source_metadata_octets(case.work),
        },
        certificates_owner.UploadJournalGraphCertificates.verify_current: {
            "retained_rows": sum(len(rows[model.__tablename__]) for model in TABLES),
            "metadata_row_octets": row_octets,
            "checkpoint_octets": sum(
                octets(manifest[key])
                for key in ("union_cursor_json", "topology_cursor_json", "rank_cursor_json")
            ),
            "source_binding_octets": len(source_json.encode()),
            "manifest_octets": len(manifest_json),
        },
    }
    for mode in ("verify_source", "verify_current"):
        function = getattr(owner._OwnedSource, mode)
        declaration = growth_binding(function).declaration
        fields = {"metadata_row_octets": row_octets}
        for tag, child in owner._source_fence_children(mode):
            fields[tag + "_calls"] = (
                1
                if tag in {"worker", "source"}
                else int(worker_good if tag == "current" else current.source_success)
            )
            fields.update({tag + "_" + name: value for name, value in values[child].items()})
        assert set(fields) == {item.name for item in declaration.dimensions}
        values[function] = fields
    return values, rows


class Recorder:
    def __init__(
        self,
        operation,
        inputs,
        *,
        functions=FUNCTIONS,
        application=APPLICATION,
        locations=None,
        payload_operations=None,
    ):
        self.application_functions = application
        self.locations = {
            SOURCE: (owner._OwnedSource, "verify_source"),
            OWNED_CURRENT: (owner._OwnedSource, "verify_current"),
            WORKER: (jobs, "worker_current"),
            CURRENT: (UploadJournalValidationSource, "verify_current"),
            PROBE: (replay, "_probe"),
            CERTIFICATE: (certificates_owner.UploadJournalGraphCertificates, "verify_current"),
            **(locations or {}),
        }
        self.payload_operations = payload_operations or {}
        self.operation, self.inputs = operation, inputs
        self.bindings = {function: growth_binding(function) for function in functions}
        self.projection = project_bindings((self.bindings[operation],))
        self.digests = {
            identity: hashlib.sha256(canonical_json_bytes(value)).hexdigest()
            for identity, value in self.projection["declarations"].items()
        }
        self.full = {name: 0 for name in BASE_METRICS}
        self.application = {name: 0 for name in BASE_METRICS}
        self.active, self.executing_application = False, False
        self.stack, self.native, self.cursors, self.statements = [], [], [], []

    def invoke(self, function, action):
        binding = self.bindings[function]
        before = self.full.copy(), self.application.copy()
        node = {
            "declaration": binding.declaration.id,
            "declaration_sha256": self.digests[binding.declaration.id],
            "dimensions": self.inputs[function],
            "delegates": {tag: [] for tag, _ in binding.delegates},
        }
        self.stack.append((function, node))
        result = None
        try:
            result = action()
            return result
        finally:
            self.stack.pop()
            index = int(function in self.application_functions)
            now = (self.full, self.application)[index]
            values = {name: value - before[index][name] for name, value in now.items()}
            values["connection_statements"] = values["driver_statements"] - values["sql_statements"]
            if function is PROBE:
                values["binding_octets"] = octets(None if result is None else result[1])
            elif function is CURRENT:
                values["binding_octets"] = values["returned_metadata_octets"]
            values["source_rows"] = values["payload_rows"]
            values["loaded_segment_octets"] = values["loaded_payload_octets"]
            values["returned_octets"] = octets(result) if isinstance(result, bytes) else 0
            for tag, children in node["delegates"].items():
                values[tag + "_calls"] = values[tag + "_calls_limit"] = len(children)
                for bound in self.bindings[dict(binding.delegates)[tag]].declaration.bounds:
                    values[tag + "_" + bound.metric] = sum(
                        child["metrics"][bound.metric] for child in children
                    )
            node["metrics"] = {
                bound.metric: values[bound.metric] for bound in binding.declaration.bounds
            }
            if self.stack:
                parent, parent_node = self.stack[-1]
                tag = next(
                    tag for tag, child in self.bindings[parent].delegates if child is function
                )
                parent_node["delegates"][tag].append(node)
            else:
                self.sample = node

    def row(self, cursor, row):
        if self.active and row is not None:
            blobs = [value for value in row if isinstance(value, bytes)]
            assert not blobs or self.stack[-1][0] in self.payload_operations
            for values in (self.full, self.application) if cursor.application else (self.full,):
                values["returned_rows"] += 1
                values["returned_metadata_octets"] += sum(
                    octets(value) for value in row if not isinstance(value, bytes)
                )
                values["source_payload_octets"] += sum(map(len, blobs))
                values["loaded_payload_octets"] += sum(map(len, blobs))
                values["payload_rows"] += int(bool(blobs))
        return row

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
            kwargs["factory"] = Connection
            return connect(*args, **kwargs)

        def sql(_conn, cursor, statement, parameters, _context, many):
            assert not many and isinstance(cursor, Cursor)
            cursor.application = True
            if recorder.active:
                self.full["sql_statements"] += 1
                self.application["sql_statements"] += 1
                self.statements.append((statement, parameters))

        def record(name, value):
            if self.active:
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
                for module in (source_owner, certificates_owner, replay, codec_owner, jobs):
                    if hasattr(module, "canonical_json_bytes"):
                        patch.setattr(module, "canonical_json_bytes", encode)
                    if hasattr(module, "hashlib"):
                        patch.setattr(module, "hashlib", SimpleNamespace(sha256=sha256))
                    if hasattr(module, "json"):
                        patch.setattr(module, "json", SimpleNamespace(loads=loads))
                for function, (target, name) in self.locations.items():

                    def call(*args, _function=function, **kwargs):
                        return self.invoke(_function, lambda: _function(*args, **kwargs))

                    patch.setattr(target, name, call)
                self.active = True
                try:
                    yield
                finally:
                    self.active = False
        finally:
            event.remove(engine, "before_cursor_execute", sql)
        assert not engine.pool.checkedout() and not sum(db.total_changes for db in self.native)
        assert all(cursor.closed for cursor in self.cursors)
        for statement, parameters in self.statements:
            with engine.connect() as connection:
                with connection.exec_driver_sql(
                    "EXPLAIN QUERY PLAN " + statement, parameters
                ) as result:
                    plan = " ".join(row[3] for row in result)
            assert "SEARCH" in plan and "SCAN" not in plan and "USE TEMP" not in plan


@contextmanager
def changed(case, kind, certificates):
    model = (
        jobs.Job
        if kind in {"worker", "worker-binding", "missing"}
        else certificates_owner.Journal
        if kind == "source"
        else certificates_owner.Scan
    )
    with case.factory() as session, session.begin():
        row = (
            session.scalar(select(model).where(model.workspace_id == case.active["workspace_id"]))
            if model is jobs.Job
            else session.get(model, (case.work.collection_id, case.work.journal_id))
        )
        fields = {column.key: getattr(row, column.key) for column in model.__table__.columns}
        key = tuple(getattr(row, column.key) for column in model.__table__.primary_key.columns)
        if kind == "missing":
            session.delete(row)
        elif kind == "worker":
            row.worker_epoch += 1
        elif kind == "worker-binding":
            row.certificate_binding_json = row.certificate_binding_json[:-1] + " "
        elif kind == "source":
            row.state = "failed"
        elif kind == "manifest":
            row.version += 1
        elif kind == "binding":
            row.replay_binding_json = "{"
        elif kind == "replacement":
            row.replay_binding_json = replace(
                certificates.binding.source,
                catalog_sha256=(
                    "0" if certificates.binding.source.catalog_sha256[0] != "0" else "1"
                )
                + certificates.binding.source.catalog_sha256[1:],
            ).to_json()
        else:
            raise AssertionError(kind)
    try:
        yield
    finally:
        with case.factory() as session, session.begin():
            row = session.get(model, key)
            if row is None:
                session.add(model(**fields))
            else:
                for name, value in fields.items():
                    setattr(row, name, value)


@contextmanager
def build_cases(root, *, large=True):
    with job_cases(root, large=large) as cases:
        yield cases


@pytest.fixture(scope="module")
def fence_cases(tmp_path_factory):
    with build_cases(tmp_path_factory.mktemp("owned-source-fence-inputs")) as cases:
        yield cases


def measure(case, current, certificates, mode, *, cold, kind=None):
    worker_good = kind not in {"worker", "worker-binding", "missing"}
    current.source_success = worker_good and kind not in {"source", "binding", "replacement"}
    inputs, before = input_dimensions(case, current, certificates, worker_good, kind != "source")
    function = getattr(owner._OwnedSource, mode)
    recorder = Recorder(function, inputs)
    error = (
        source_owner._StaleUploadJournalValidation
        if kind in {"worker", "worker-binding", "missing", "source"}
        else RuntimeError
        if kind in {"binding", "replacement"}
        else InvalidJournalGraphCertificate
        if kind == "manifest" and mode == "verify_current"
        else None
    )
    result, caught = None, None
    with recorder.observe(case.engine, cold=cold):
        try:
            result = getattr(current, mode)()
        except BaseException as failure:
            caught = failure
    if error is None:
        if caught is not None:
            raise caught
        assert result is True
    else:
        if caught is not None and not isinstance(caught, error):
            raise caught
        assert isinstance(caught, error)
    assert snapshot(case.engine) == before
    validate_execution_samples(
        recorder.projection, growth_binding(function).declaration.id, [recorder.sample]
    )
    return recorder.sample


def fence_samples(cases, mode, *, samples):
    for case in cases:
        reset(case, case.active)
        catalog = source_owner.admission_provenance_catalog()
        certificates = certificates_owner.UploadJournalGraphCertificates(
            case.factory, case.work, catalog=catalog
        )
        selected = UploadJournalValidationSource(case.factory, case.work, catalog=catalog)
        current = owner._OwnedSource(case.factory, selected, read_job(case.active), certificates)
        root = Path(case.engine.url.database).parent / "native"
        with lease_journal_validation_workspace(root, current.job.workspace_id) as directory:
            assert directory is not None
            for cold in (False, True):
                samples.append(measure(case, current, certificates, mode, cold=cold))
            for kind in (
                "worker",
                "worker-binding",
                "missing",
                "source",
                "binding",
                "replacement",
                "manifest",
            ):
                original = snapshot(case.engine)
                with changed(case, kind, certificates):
                    for cold in (False, True):
                        samples.append(
                            measure(case, current, certificates, mode, cold=cold, kind=kind)
                        )
                assert snapshot(case.engine) == original
        reset(case)


@pytest.mark.growth_witness(
    "riverhog-owned-source-fence-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_owner._OwnedSource.verify_source",
)
def test_actual_owned_source_fence_composes_worker_and_source_with_driver_work(
    fence_cases, record_property
):
    samples = []
    record_property("growth_samples", samples)
    fence_samples(fence_cases, "verify_source", samples=samples)


@pytest.mark.growth_witness(
    "riverhog-owned-current-fence-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_owner._OwnedSource.verify_current",
)
def test_actual_owned_current_fence_composes_exact_manifest_and_source_checks(
    fence_cases, record_property
):
    samples = []
    record_property("growth_samples", samples)
    fence_samples(fence_cases, "verify_current", samples=samples)
