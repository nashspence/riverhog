from __future__ import annotations

import hashlib
import sqlite3
from contextlib import ExitStack, contextmanager
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.services import collection_uploads as source_owner
from riverhog_core.services import upload_journal_certificates as owner
from riverhog_core.services import upload_journal_rank as rank
from riverhog_core.services import upload_journal_union as union
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import replay_binding as binding_owner
from riverhog_provenance import topology_checkpoint
from sqlalchemy import event, update

from tests.unit.test_journal_capture import retained
from tests.unit.test_journal_epoch_union import edge, raw_edges
from tests.unit.test_upload_journal_certificates import ready
from tests.unit.test_upload_journal_source_growth import octets


class Recorder:
    def __init__(self):
        self.active = False
        self.metrics = {
            name: 0
            for name in (
                "driver_statements",
                "sql_statements",
                "returned_rows",
                "returned_metadata_octets",
                "bound_parameter_octets",
                "sqlite_vm_instructions",
                "json_decodes",
                "json_encodes",
                "json_decoded_octets",
                "json_encoded_octets",
                "hashed_octets",
            )
        }
        self.native, self.statements = [], []

    def row(self, value):
        if self.active and value is not None:
            self.metrics["returned_rows"] += 1
            assert not any(isinstance(item, bytes) for item in value)
            self.metrics["returned_metadata_octets"] += octets(value)
        return value

    def trace(self, _statement):
        if self.active:
            self.metrics["driver_statements"] += 1

    def tick(self):
        if self.active:
            self.metrics["sqlite_vm_instructions"] += 1
        return 0

    @contextmanager
    def observe(self, engine, *, cold):
        observer = self
        engine.dispose()
        dbapi, connect = engine.dialect.dbapi, engine.dialect.dbapi.connect
        real_loads, real_encode, real_hash = (
            rank.json.loads,
            owner.canonical_json_bytes,
            owner.hashlib.sha256,
        )

        class Cursor(sqlite3.Cursor):
            def execute(self, sql, parameters=()):
                if observer.active:
                    observer.metrics["bound_parameter_octets"] += octets(parameters)
                return super().execute(sql, parameters)

            def fetchone(self):
                return observer.row(super().fetchone())

            def fetchall(self):
                return [observer.row(row) for row in super().fetchall()]

            def fetchmany(self, *args):
                return [observer.row(row) for row in super().fetchmany(*args)]

            def __next__(self):
                return observer.row(super().__next__())

        class Connection(sqlite3.Connection):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                observer.native.append(self)
                self.set_trace_callback(observer.trace)
                self.set_progress_handler(observer.tick, 1)
                self.set_authorizer(self.authorize)

            def cursor(self, *args, **kwargs):
                kwargs["factory"] = Cursor
                return super().cursor(*args, **kwargs)

            def authorize(self, action, _table, column, _database, _trigger):
                if (
                    observer.active
                    and action == sqlite3.SQLITE_READ
                    and column
                    in {
                        "content",
                        "canonical_json",
                        "exact_json",
                        "schema_json",
                    }
                ):
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

        def observed_connect(*args, **kwargs):
            kwargs["factory"] = Connection
            return connect(*args, **kwargs)

        def sql(_connection, _cursor, statement, parameters, _context, many):
            assert not many
            if observer.active:
                observer.metrics["sql_statements"] += 1
                observer.statements.append((statement, parameters))

        def loads(value, *args, **kwargs):
            observer.metrics["json_decodes"] += 1
            observer.metrics["json_decoded_octets"] += octets(value)
            return real_loads(value, *args, **kwargs)

        def encode(*args, **kwargs):
            observer.metrics["json_encodes"] += 1
            value = real_encode(*args, **kwargs)
            observer.metrics["json_encoded_octets"] += len(value)
            return value

        class Hash:
            def __init__(self, actual):
                self.actual = actual

            def update(self, value):
                observer.metrics["hashed_octets"] += len(value)
                self.actual.update(value)

            def copy(self):
                return Hash(self.actual.copy())

            def __getattr__(self, name):
                return getattr(self.actual, name)

        def sha256(value=b"", *args, **kwargs):
            observer.metrics["hashed_octets"] += len(value)
            return Hash(real_hash(value, *args, **kwargs))

        event.listen(engine, "before_cursor_execute", sql)
        try:
            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(dbapi, "connect", observed_connect)
                if not cold:
                    with engine.connect():
                        pass
                for module in (union, topology_checkpoint, rank, binding_owner):
                    patch.setattr(module, "json", SimpleNamespace(loads=loads))
                for module in (owner, union, topology_checkpoint, rank, binding_owner):
                    patch.setattr(module, "canonical_json_bytes", encode)
                patch.setattr(owner, "hashlib", SimpleNamespace(sha256=sha256))
                self.active = True
                try:
                    yield
                finally:
                    self.active = False
        finally:
            event.remove(engine, "before_cursor_execute", sql)
        self.metrics["connection_statements"] = (
            self.metrics["driver_statements"] - self.metrics["sql_statements"]
        )
        self.metrics["changed_rows"] = sum(db.total_changes for db in self.native)
        self.metrics["source_payload_octets"] = 0
        assert self.metrics["changed_rows"] == 0 and engine.pool.checkedout() == 0
        for statement, parameters in self.statements:
            with engine.connect() as connection:
                with connection.exec_driver_sql(
                    "EXPLAIN QUERY PLAN " + statement, parameters
                ) as result:
                    plan = " ".join(row[3] for row in result)
            assert "SEARCH" in plan and "SCAN" not in plan and "USE TEMP" not in plan


TABLES = (
    owner.Journal,
    owner.Scan,
    owner.Frame,
    union.Fact,
    owner.Arc,
    owner.Graph,
    owner.Ranked,
    owner.Result,
)


def snapshot(engine):
    with engine.connect() as connection:
        return {
            model.__tablename__: tuple(
                connection.exec_driver_sql(f'SELECT * FROM "{model.__tablename__}"').fetchall()
            )
            for model in TABLES
        }


def dimensions(case):
    rows = snapshot(case.engine)
    with case.factory() as session:
        scan = session.get(owner.Scan, (case.work.collection_id, case.work.journal_id))
        manifest = tuple(getattr(scan, name) for name in owner._MANIFEST_FIELDS)
        checkpoint_octets = sum(
            octets(getattr(scan, name))
            for name in (
                "union_cursor_json",
                "topology_cursor_json",
                "rank_cursor_json",
            )
        )
    binding = owner._binding(case.work, case.catalog).to_json()
    encoded = canonical_json_bytes(
        {
            "source": binding,
            "mode": case.mode,
            "preparation": {
                key: str(value) if type(value) is int else value
                for key, value in zip(owner._MANIFEST_FIELDS, manifest, strict=True)
            },
        }
    )
    return (
        {
            "retained_rows": sum(map(len, rows.values())),
            "metadata_row_octets": max(octets(row) for values in rows.values() for row in values),
            "checkpoint_octets": checkpoint_octets,
            "source_binding_octets": len(binding.encode()),
            "manifest_octets": len(encoded),
        },
        manifest,
        hashlib.sha256(encoded).hexdigest(),
        rows,
    )


@contextmanager
def build_cases(root, *, large=True):
    with ExitStack() as resources:
        cases = []
        specifications = [
            (1, "star", False),
            (17, "star", False),
            (2, "cycle", False),
            (1, "deferred", False),
            (1, "star", True),
        ]
        if large:
            specifications.extend(((129, "chain", False), (1025, "star", False)))
        for index, (edges, shape, wide) in enumerate(specifications):
            values = (
                [edge(0, 0, 100), edge(1, 100, 0)]
                if shape == "cycle"
                else [edge(number, number, number + 1) for number in range(edges)]
                if shape == "chain"
                else [edge(number) for number in range(edges)]
            )
            raw = raw_edges(values) + (b"malformed tail" if shape == "deferred" else b"")
            engine, factory = resources.enter_context(retained(root / str(index), raw))
            work, source = ready(factory)
            catalog = source_owner.admission_provenance_catalog()
            if wide:
                with factory() as session, session.begin():
                    session.execute(update(owner.Scan).values(version=2**200)).close()
                source = owner.UploadJournalGraphCertificates(factory, work, catalog=catalog)
            cases.append(
                SimpleNamespace(
                    engine=engine,
                    factory=factory,
                    work=work,
                    catalog=catalog,
                    mode=source._mode,
                    source=source,
                )
            )
        yield cases


@pytest.fixture(scope="module")
def certificate_cases(tmp_path_factory):
    with build_cases(tmp_path_factory.mktemp("certificate-lifecycle-inputs")) as cases:
        yield cases


def measure(case, operation, action, *, cold):
    inputs, manifest, expected_digest, before = dimensions(case)
    binding = growth_binding(operation)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    recorder = Recorder()
    with recorder.observe(case.engine, cold=cold):
        result = action()
    assert snapshot(case.engine) == before
    if operation is owner.UploadJournalGraphCertificates.__init__:
        assert result._manifest == manifest and result.binding.manifest_sha256 == expected_digest
        assert result.binding.source == owner._binding(case.work, case.catalog)
    metrics = {bound.metric: recorder.metrics[bound.metric] for bound in binding.declaration.bounds}
    sample = {
        "declaration": binding.declaration.id,
        "declaration_sha256": digest,
        "dimensions": inputs,
        "metrics": metrics,
        "delegates": {},
    }
    validate_execution_samples(projection, binding.declaration.id, [sample])
    return result, sample


def constructor_samples(cases, *, samples):
    for case in cases:
        for cold in (False, True):
            _, sample = measure(
                case,
                owner.UploadJournalGraphCertificates.__init__,
                lambda case=case: owner.UploadJournalGraphCertificates(
                    case.factory, case.work, catalog=case.catalog
                ),
                cold=cold,
            )
            samples.append(sample)


def currentness_samples(cases, *, samples):
    for case in cases:
        for cold in (False, True):
            result, sample = measure(
                case,
                owner.UploadJournalGraphCertificates.verify_current,
                case.source.verify_current,
                cold=cold,
            )
            assert result is True
            samples.append(sample)
        with case.factory() as session, session.begin():
            scan = session.get(owner.Scan, (case.work.collection_id, case.work.journal_id))
            version = scan.version
            scan.version += 1
        try:
            for cold in (False, True):
                result, sample = measure(
                    case,
                    owner.UploadJournalGraphCertificates.verify_current,
                    case.source.verify_current,
                    cold=cold,
                )
                assert result is False
                samples.append(sample)
        finally:
            with case.factory() as session, session.begin():
                session.get(
                    owner.Scan, (case.work.collection_id, case.work.journal_id)
                ).version = version


@pytest.mark.growth_witness(
    "riverhog-certificate-constructor-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_certificates.UploadJournalGraphCertificates.__init__",
)
def test_actual_certificate_constructor_includes_checkpoint_and_driver_work(
    certificate_cases, record_property
):
    samples = []
    record_property("growth_samples", samples)
    constructor_samples(certificate_cases, samples=samples)


@pytest.mark.growth_witness(
    "riverhog-certificate-currentness-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_certificates.UploadJournalGraphCertificates.verify_current",
)
def test_actual_certificate_currentness_includes_pool_and_manifest_work(
    certificate_cases, record_property
):
    samples = []
    record_property("growth_samples", samples)
    currentness_samples(certificate_cases, samples=samples)
