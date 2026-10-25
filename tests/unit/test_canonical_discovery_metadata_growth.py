from __future__ import annotations

import builtins
import hashlib
import io
import os
import sqlite3
from collections import defaultdict
from contextlib import closing, contextmanager
from dataclasses import asdict
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core import canonical_discovery_metadata as owner
from riverhog_core.canonical_discovery_index import begin_index_build
from riverhog_core.catalog_db import Base, create_catalog_engine
from riverhog_core.catalog_discovery_validation_models import DiscoveryValidationJobRecord as Job
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexGenerationRecord as Generation,
)
from riverhog_core.journal_validation_workspace import lease_journal_validation_workspace
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance_contracts import ContractCatalog
from sha256_continuation import CheckpointSHA256
from sqlalchemy.orm import sessionmaker

from tests.support.catalog_work import CatalogRecorder, octets
from tests.unit.test_canonical_discovery_corpus_jobs import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_corpus_jobs import original as original
from tests.unit.test_canonical_discovery_corpus_jobs import qualified as qualified
from tests.unit.test_canonical_discovery_index import _collection

_REAL_CONNECT = sqlite3.connect
SPECIFICATIONS = (
    (1, 1),
    (17, 1),
    (1025, 1),
    (1, 17),
    (17, 17),
    (1025, 17),
    (1, 1025),
    (17, 1025),
    (1025, 1025),
)


class CoreCursor(sqlite3.Cursor):
    def execute(self, statement, parameters=()):
        rec = self.connection.observer
        if rec.active:
            rec.metrics["core_parameter_octets"] += octets(parameters)
            rec.statements.append((statement, parameters))
        try:
            result = super().execute(statement, parameters)
        except BaseException:
            self.close()
            raise
        if rec.active and self.rowcount > 0:
            rec.metrics["core_changed_rows"] += self.rowcount
        return result

    def record(self, row):
        rec = self.connection.observer
        if rec.active and row is not None:
            rec.metrics["core_returned_rows"] += 1
            rec.metrics["core_returned_octets"] += octets(row)
        return row

    def fetchone(self):
        return self.record(super().fetchone())

    def fetchall(self):
        return [self.record(row) for row in super().fetchall()]

    def fetchmany(self, *args):
        return [self.record(row) for row in super().fetchmany(*args)]

    def __next__(self):
        return self.record(super().__next__())


class CoreConnection(sqlite3.Connection):
    observer = None

    def execute(self, statement, parameters=()):
        return self.cursor(factory=CoreCursor).execute(statement, parameters)

    def close(self):
        if self.observer is not None and self.observer.active:
            self.observer.metrics["core_closed_connections"] += 1
        super().close()


class Recorder:
    def __init__(self):
        self.active = False
        self.metrics = defaultdict(int)
        self.statements = []
        self.catalog = CatalogRecorder(jobs)
        self.live = set()

    def trace(self, _sql):
        if self.active:
            self.metrics["core_sql_statements"] += 1

    def tick(self):
        if self.active:
            self.metrics["core_vm_instructions"] += 1
        return 0

    def core_connect(self, *args, **kwargs):
        kwargs["factory"] = CoreConnection
        db = _REAL_CONNECT(*args, **kwargs)
        db.observer = self
        db.set_trace_callback(self.trace)
        db.set_progress_handler(self.tick, 1)
        if self.active:
            self.metrics["core_opened_connections"] += 1
        return db

    @contextmanager
    def connect_only(self):
        with pytest.MonkeyPatch.context() as patch:

            def connect(path, *args, **kwargs):
                if "discovery.sqlite3" in str(path):
                    return self.core_connect(path, *args, **kwargs)
                return _REAL_CONNECT(path, *args, **kwargs)

            patch.setattr(owner.sqlite3, "connect", connect)
            yield

    @contextmanager
    def observe(self, case, *, cold):
        with self.catalog.observe(case.engine, cold=cold), pytest.MonkeyPatch.context() as patch:
            catalog_connect = owner.sqlite3.connect

            def connect(path, *args, **kwargs):
                if "discovery.sqlite3" in str(path):
                    return self.core_connect(path, *args, **kwargs)
                return catalog_connect(path, *args, **kwargs)

            patch.setattr(owner.sqlite3, "connect", connect)
            for name, metric in (
                ("open", "open_calls"),
                ("close", "close_calls"),
                ("stat", "stat_calls"),
                ("fstat", "stat_calls"),
                ("getuid", "uid_calls"),
                ("fsync", "fsync_calls"),
            ):
                actual = getattr(os, name)

                def invoke(*args, _name=name, _metric=metric, _actual=actual, **kwargs):
                    if self.active:
                        self.metrics[_metric] += 1
                        if _name in {"open", "stat"}:
                            self.metrics["encoded_path_octets"] += len(os.fsencode(args[0]))
                    result = _actual(*args, **kwargs)
                    if self.active and _name == "open":
                        self.live.add(result)
                    if self.active and _name == "close":
                        self.live.discard(args[0])
                    return result

                patch.setattr(os, name, invoke)
            update = CheckpointSHA256.update

            def updated(instance, data):
                if self.active:
                    self.metrics["hashed_octets"] += len(data)
                return update(instance, data)

            patch.setattr(CheckpointSHA256, "update", updated)
            export = CheckpointSHA256.export_state

            def exported(instance):
                result = export(instance)
                if self.active:
                    self.metrics["encoded_octets"] += len(result.encode())
                return result

            patch.setattr(CheckpointSHA256, "export_state", exported)

            def forbidden(*_args, **_kwargs):
                if self.active:
                    raise AssertionError(
                        "storage boundary attempted Python payload I/O or enumeration"
                    )

            for module, name in (
                (builtins, "open"),
                (io, "open"),
                (os, "read"),
                (os, "pread"),
                (os, "listdir"),
                (os, "scandir"),
            ):
                if hasattr(module, name):
                    actual = getattr(module, name)

                    def guarded(*args, _actual=actual, **kwargs):
                        if self.active:
                            forbidden()
                        return _actual(*args, **kwargs)

                    patch.setattr(module, name, guarded)
            self.active = True
            try:
                yield
            finally:
                self.active = False

    def sample(self, operation, dimensions):
        metrics = dict(self.metrics)
        mapping = {
            "sql_statements": "catalog_sql_statements",
            "driver_statements": "catalog_driver_statements",
            "returned_rows": "catalog_returned_rows",
            "returned_metadata_octets": "catalog_returned_octets",
            "bound_parameter_octets": "catalog_parameter_octets",
            "sqlite_vm_instructions": "catalog_vm_instructions",
            "changed_rows": "catalog_changed_rows",
            "uuid_values": "uuid_values",
        }
        for source, destination in mapping.items():
            metrics[destination] = self.catalog.metrics[source]
        metrics["hashed_octets"] = (
            metrics.get("hashed_octets", 0) + self.catalog.metrics["hashed_octets"]
        )
        binding = growth_binding(getattr(owner.DiscoveryMetadata, operation))
        projection = project_bindings((binding,))
        sample = {
            "declaration": binding.declaration.id,
            "declaration_sha256": hashlib.sha256(
                canonical_json_bytes(projection["declarations"][binding.declaration.id])
            ).hexdigest(),
            "dimensions": dimensions,
            "metrics": {
                bound.metric: metrics.get(bound.metric, 0) for bound in binding.declaration.bounds
            },
            "delegates": {},
        }
        validate_execution_samples(projection, binding.declaration.id, [sample])
        return sample


@contextmanager
def case_for(root, admission, retained, width, *, template=None, catalog=None):
    catalog = ContractCatalog() if catalog is None else catalog
    root.mkdir()
    if template is not None:
        with closing(_REAL_CONNECT(template.path)) as original:
            with closing(_REAL_CONNECT(root / "catalog.sqlite3")) as copied:
                original.backup(copied)
    engine = create_catalog_engine("sqlite:///" + str(root / "catalog.sqlite3"))
    if template is None:
        Base.metadata.create_all(engine)
    factory = sessionmaker(engine, expire_on_commit=False)
    try:
        if template is None:
            with factory() as session, session.begin():
                collection = _collection()
                collection.archive_generation = admission.root.archive_generation
                collection.archive_root_sha256 = admission.archive_sha256
                collection.provenance_identity = admission.provenance_sha256
                collection.artifact_set_identity = admission.root.artifact_set_sha256
                session.add(collection)
                session.flush()
                build_id = begin_index_build(session, collection_id=1)
            selected = jobs.select_discovery_job(
                factory, build_id=build_id, admission=admission, catalog=catalog
            )
            assert selected is not None
            with factory() as session, session.begin():
                build = session.get(Generation, build_id)
                values = {
                    column.key: getattr(build, column.key)
                    for column in Generation.__table__.columns
                }
                for index in range(retained):
                    session.add(Generation(**dict(values, build_id="retained-" + str(index))))
                session.flush()
                for index in range(retained):
                    session.add(
                        Job(
                            **dict(
                                asdict(selected),
                                workspace_id=f"{index + 1:032x}",
                                attempt_id=f"{index + 2000:032x}",
                                build_id="retained-" + str(index),
                                state="lost",
                                failure="x" * (width if index == 0 else 1),
                            )
                        )
                    )
        else:
            selected = jobs.select_discovery_job(
                factory, build_id=template.build_id, admission=admission, catalog=catalog
            )
            assert selected is not None
        captured = []
        lease_root = root / "owned"
        with lease_journal_validation_workspace(
            lease_root,
            selected.workspace_id,
            create=True,
            creation_current=lambda: jobs.discovery_worker_current(factory, selected),
            on_custody=captured.append,
        ) as path:
            assert path is not None
            claimed = jobs.claim_discovery_worker(factory, selected)
            assert claimed is not None
            metadata = owner.DiscoveryMetadata(factory, claimed, admission, captured[0])
            try:
                yield SimpleNamespace(
                    engine=engine,
                    factory=factory,
                    metadata=metadata,
                    path=path,
                    retained=retained,
                    width=width,
                    admission=admission,
                )
            finally:
                metadata.close()
    finally:
        engine.dispose()


@pytest.fixture(scope="module")
def catalog_templates(tmp_path_factory, fixed_inputs):
    admission, _ = fixed_inputs
    base = tmp_path_factory.mktemp("metadata-catalog-controls")
    result = {}
    for retained, width in SPECIFICATIONS:
        root = base / f"{retained}-{width}"
        with case_for(root, admission, retained, width) as case:
            result[(retained, width)] = SimpleNamespace(
                path=root / "catalog.sqlite3", build_id=case.metadata.job.build_id
            )
    return result


def seed_core(case):
    db = case.metadata._db
    original = db.execute("SELECT * FROM discovery_archive_tasks").fetchone()
    db.execute("BEGIN IMMEDIATE").close()
    try:
        for index in range(case.retained):
            row = list(original)
            row[0] = "retained-" + str(index)
            # Unrelated retained metadata controls are not Native acceptance evidence.
            if index == 0:
                row[4] = "x" * case.width
            db.execute(
                "INSERT INTO discovery_archive_tasks VALUES(" + ",".join("?" * len(row)) + ")", row
            ).close()
        db.commit()
    except BaseException:
        db.rollback()
        raise


def measure(case, operation, variant, cold):
    recorder = Recorder()
    metadata = case.metadata
    if operation != "open" or variant != "new":
        with recorder.connect_only():
            metadata.open()
        seed_core(case)
    if operation == "open" and variant != "new":
        job = metadata.job
        metadata.close()
        claimed = jobs.claim_discovery_worker(case.factory, job)
        assert claimed is not None
        metadata = case.metadata = owner.DiscoveryMetadata(
            case.factory, claimed, case.admission, metadata.custody
        )
    control = octets(
        (
            tuple(asdict(metadata.job).values()),
            case.admission.root_json,
            case.admission.binding_json,
            metadata._expected,
        )
    )
    dimensions = {
        "retained_core_rows": 0 if operation == "open" and variant == "new" else case.retained + 2,
        "retained_catalog_rows": 3 * case.retained + 3,
        "control_octets": control,
        "path_octets": len(os.fsencode(case.path / "discovery.sqlite3")),
    }
    if variant == "closed":
        metadata.close()
    if variant == "stale":
        metadata.custody._held = False
    try:
        with recorder.observe(case, cold=cold):
            if operation == "open":
                metadata.open()
            elif operation == "verify_current":
                assert metadata.verify_current() is (variant not in {"closed", "stale"})
            elif operation in {"read", "transaction"}:
                with getattr(metadata, operation)():
                    pass
            else:
                metadata.close()
    finally:
        if variant == "stale":
            metadata.custody._held = True
    assert not recorder.live or operation == "open"
    for statement, parameters in recorder.statements:
        if statement.lstrip().upper().startswith("SELECT"):
            with _REAL_CONNECT(case.path / "discovery.sqlite3") as check:
                rows = check.execute("EXPLAIN QUERY PLAN " + statement, parameters).fetchall()
            plan = " ".join(str(row[3]).upper() for row in rows)
            if statement == "SELECT format FROM discovery_work_format LIMIT 2":
                # New-file initialization checks at most two format markers.
                assert operation == "open" and variant == "new"
                continue
            assert "SCAN " not in plan and "TEMP B-TREE" not in plan, plan
    return recorder.sample(operation, dimensions)


def exercise(tmp_path, fixed_inputs, catalog_templates, operation, record_property):
    admission, _ = fixed_inputs
    variants = {
        "open": ("new", "reopen"),
        "verify_current": ("current", "closed", "stale"),
        "read": ("current",),
        "transaction": ("current",),
        "close": ("current", "closed"),
    }[operation]
    samples = []
    for retained, width in SPECIFICATIONS:
        for cold in (False, True):
            for variant in variants:
                root = tmp_path / f"{retained}-{width}-{cold}-{variant}"
                with case_for(
                    root, admission, retained, width, template=catalog_templates[(retained, width)]
                ) as case:
                    samples.append(measure(case, operation, variant, cold))
    record_property("growth_samples", samples)
    record_property(
        "metadata_conditions",
        "Actual catalog and Core SQLite driver/VM/complete parameter/result work, exact descriptor "
        "calls, fixed-domain hashing and no Python payload I/O; independent unrelated retained "
        "row/metadata widths 1/17/1025, cold/warm catalog connections. These are storage/fence "
        "observations, not Native semantic acceptance or whole-collection qualification.",
    )


@pytest.mark.growth_witness(
    "canonical-discovery-metadata-open/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_metadata.DiscoveryMetadata.open",
)
def test_measured_metadata_open(tmp_path, fixed_inputs, catalog_templates, record_property):
    exercise(tmp_path, fixed_inputs, catalog_templates, "open", record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-metadata-current/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_metadata.DiscoveryMetadata.verify_current",
)
def test_measured_metadata_current(tmp_path, fixed_inputs, catalog_templates, record_property):
    exercise(tmp_path, fixed_inputs, catalog_templates, "verify_current", record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-metadata-read/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_metadata.DiscoveryMetadata.read",
)
def test_measured_metadata_read(tmp_path, fixed_inputs, catalog_templates, record_property):
    exercise(tmp_path, fixed_inputs, catalog_templates, "read", record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-metadata-transaction/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_metadata.DiscoveryMetadata.transaction",
)
def test_measured_metadata_transaction(tmp_path, fixed_inputs, catalog_templates, record_property):
    exercise(tmp_path, fixed_inputs, catalog_templates, "transaction", record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-metadata-close/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_metadata.DiscoveryMetadata.close",
)
def test_measured_metadata_close(tmp_path, fixed_inputs, catalog_templates, record_property):
    exercise(tmp_path, fixed_inputs, catalog_templates, "close", record_property)
