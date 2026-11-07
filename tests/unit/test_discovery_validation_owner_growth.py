from __future__ import annotations

import builtins
import fcntl
import hashlib
import io
import os
import sqlite3
import sys
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import asdict
from types import SimpleNamespace

import pytest
import sha256_continuation
from riverhog_canonical_json import canonical_json_bytes, require_canonical_json
from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core.catalog_db import create_catalog_engine
from riverhog_core.services import discovery_validation_owner as owner
from riverhog_growth_contracts import (
    evaluate_bound,
    growth_binding,
    project_bindings,
    validate_execution_samples,
)
from riverhog_provenance import CorpusReplayBinding
from riverhog_provenance import corpus_workspace as native_owner
from sha256_continuation import CheckpointSHA256
from sqlalchemy.orm import sessionmaker

from tests.support.catalog_work import CatalogRecorder, octets
from tests.unit.test_canonical_discovery_corpus_source import seed
from tests.unit.test_canonical_discovery_metadata_growth import (
    SPECIFICATIONS,
)
from tests.unit.test_canonical_discovery_metadata_growth import (
    catalog_templates as catalog_templates,
)
from tests.unit.test_canonical_discovery_metadata_growth import (
    fixed_inputs as fixed_inputs,
)
from tests.unit.test_canonical_discovery_metadata_growth import (
    original as original,
)
from tests.unit.test_canonical_discovery_metadata_growth import (
    qualified as qualified,
)
from tests.unit.test_discovery_validation_owner import UncertainClose, independent_build

_CONNECT = sqlite3.connect


class Cursor(sqlite3.Cursor):
    def execute(self, statement, parameters=()):
        rec, kind = self.connection.observer, self.connection.kind
        before = self.connection.total_changes
        result = super().execute(statement, parameters)
        if rec.active:
            rec.metrics[kind + "_parameter_octets"] += octets(parameters)
            rec.metrics[kind + "_changed_rows"] += self.connection.total_changes - before
            rec.statements.append((kind, statement, parameters, self.connection))
        return result

    def record(self, row):
        rec, kind = self.connection.observer, self.connection.kind
        if row is not None and rec.active:
            rec.metrics[kind + "_returned_rows"] += 1
            rec.metrics[kind + "_returned_octets"] += octets(row)
            rec.maximum_control = max(rec.maximum_control, octets(row))
        return row

    def fetchone(self):
        return self.record(super().fetchone())

    def fetchall(self):
        return [self.record(row) for row in super().fetchall()]

    def fetchmany(self, *args):
        return [self.record(row) for row in super().fetchmany(*args)]

    def __next__(self):
        return self.record(super().__next__())


class Connection(sqlite3.Connection):
    def execute(self, statement, parameters=()):
        return self.cursor(factory=Cursor).execute(statement, parameters)

    def close(self):
        if self.observer.active:
            self.observer.metrics[self.kind + "_closed_connections"] += 1
        super().close()


class Slots(dict):
    def __init__(self, recorder):
        super().__init__()
        self.recorder = recorder

    def visit(self, count=1):
        if self.recorder.active:
            self.recorder.metrics["pool_visits"] += count

    def get(self, key, default=None):
        self.visit()
        return super().get(key, default)

    def __setitem__(self, key, value):
        self.visit()
        result = super().__setitem__(key, value)
        if self.recorder.active:
            self.recorder.maximum_slots = max(self.recorder.maximum_slots, len(self))
        return result

    def __delitem__(self, key):
        self.visit()
        return super().__delitem__(key)

    def __iter__(self):
        for key in super().__iter__():
            self.visit()
            yield key

    def items(self):
        for key, value in super().items():
            self.visit()
            yield key, value


class Recorder:
    def __init__(self):
        self.active = False
        self.metrics = defaultdict(int)
        self.statements = []
        self.connections = []
        self.maximum_control = 0
        self.native_binding_width = 0
        self.native_checkpoint_width = 0
        self.maximum_slots = 0

    def connect(self, path, *args, **kwargs):
        kind = "core" if "discovery.sqlite3" in str(path) else "native"
        kwargs["factory"] = Connection
        db = _CONNECT(path, *args, **kwargs)
        db.kind, db.observer, db.location = kind, self, str(path)

        def trace(_sql):
            if self.active:
                self.metrics[kind + "_sql_statements"] += 1

        def tick():
            if self.active:
                self.metrics[kind + "_vm_instructions"] += 1
            return 0

        db.set_trace_callback(trace)
        db.set_progress_handler(tick, 1)
        self.connections.append(db)
        if self.active:
            self.metrics[kind + "_opened_connections"] += 1
        return db

    @contextmanager
    def connect_only(self):
        with pytest.MonkeyPatch.context() as patch:
            actual_connect = sqlite3.connect

            def connect(path, *args, **kwargs):
                if any(
                    name in str(path)
                    for name in ("discovery.sqlite3", "corpus.sqlite3", "snapshot.sqlite3")
                ):
                    return self.connect(path, *args, **kwargs)
                return actual_connect(path, *args, **kwargs)

            patch.setattr(sqlite3, "connect", connect)
            for name, metric in (
                ("open", "open_calls"),
                ("close", "close_calls"),
                ("stat", "stat_calls"),
                ("fstat", "stat_calls"),
                ("lstat", "stat_calls"),
                ("getuid", "uid_calls"),
                ("mkdir", "mkdir_calls"),
                ("fsync", "fsync_calls"),
            ):
                actual = getattr(os, name)

                def counted(*args, _name=name, _metric=metric, _actual=actual, **kwargs):
                    if self.active:
                        self.metrics[_metric] += 1
                        if _name in {"open", "stat", "lstat", "mkdir"}:
                            self.metrics["encoded_path_octets"] += len(os.fsencode(args[0]))
                    return _actual(*args, **kwargs)

                patch.setattr(os, name, counted)
            actual_flock = fcntl.flock

            def flock(descriptor, flags):
                if self.active:
                    self.metrics["flock_calls"] += 1
                    assert flags == fcntl.LOCK_UN or flags & fcntl.LOCK_NB
                return actual_flock(descriptor, flags)

            patch.setattr(fcntl, "flock", flock)
            yield

    @contextmanager
    def observe(self, case, *, cold):
        self.metrics = defaultdict(int)
        self.statements = []
        self.catalog = CatalogRecorder(jobs)
        self.maximum_slots = len(case.pool._slots)
        with self.catalog.observe(case.engine, cold=cold), pytest.MonkeyPatch.context() as patch:
            actual_connect = sqlite3.connect

            def connect(path, *args, **kwargs):
                if any(
                    name in str(path)
                    for name in ("discovery.sqlite3", "corpus.sqlite3", "snapshot.sqlite3")
                ):
                    return self.connect(path, *args, **kwargs)
                return actual_connect(path, *args, **kwargs)

            patch.setattr(sqlite3, "connect", connect)
            actual_lease = owner.lease_journal_validation_workspace

            def lease(*args, **kwargs):
                for name, metric in (
                    ("creation_current", "creation_callbacks"),
                    ("on_custody", "custody_callbacks"),
                ):
                    callback = kwargs.get(name)
                    if callback is not None:

                        def invoked(*values, _callback=callback, _metric=metric):
                            if self.active:
                                self.metrics[_metric] += 1
                            return _callback(*values)

                        kwargs[name] = invoked
                return actual_lease(*args, **kwargs)

            patch.setattr(owner, "lease_journal_validation_workspace", lease)
            retain = owner._Slot.retain_corpus

            def retained(slot, native):
                if self.active:
                    self.metrics["native_opening_callbacks"] += 1
                return retain(slot, native)

            patch.setattr(owner._Slot, "retain_corpus", retained)
            current = owner.DiscoveryCorpusSource.verify_current

            def checked(source):
                if self.active:
                    self.metrics["native_source_checks"] += 1
                return current(source)

            patch.setattr(owner.DiscoveryCorpusSource, "verify_current", checked)
            encoded_binding, checkpoint = CorpusReplayBinding.to_bytes, native_owner._checkpoint

            def binding_bytes(binding):
                result = encoded_binding(binding)
                if self.active:
                    self.native_binding_width = max(self.native_binding_width, len(result))
                return result

            def checkpoint_bytes(corpus):
                result = checkpoint(corpus)
                if self.active:
                    self.native_checkpoint_width = max(self.native_checkpoint_width, len(result))
                return result

            patch.setattr(CorpusReplayBinding, "to_bytes", binding_bytes)
            patch.setattr(native_owner, "_checkpoint", checkpoint_bytes)
            for reader in case.readers:
                actual_read = reader._read_object

                def read(path, _read=actual_read):
                    if self.active:
                        self.metrics["source_reads"] += 1
                    for block in _read(path):
                        if self.active:
                            self.metrics["source_payload_octets"] += len(block)
                        yield block

                patch.setattr(reader, "_read_object", read)
            actual_hash = hashlib.sha256

            class Hash:
                def __init__(instance, raw=b"", **kwargs):
                    if self.active:
                        self.metrics["hashed_octets"] += len(raw)
                    instance.actual = actual_hash(raw, **kwargs)

                def update(instance, raw):
                    if self.active:
                        self.metrics["hashed_octets"] += len(raw)
                    instance.actual.update(raw)

                def copy(instance):
                    copied = object.__new__(Hash)
                    copied.actual = instance.actual.copy()
                    return copied

                def __getattr__(instance, name):
                    return getattr(instance.actual, name)

            def encode(value):
                result = canonical_json_bytes(value)
                if self.active:
                    self.metrics["encoded_octets"] += len(result)
                return result

            def decode(raw):
                if self.active:
                    self.metrics["decoded_octets"] += len(raw)
                return require_canonical_json(raw)

            for module in tuple(sys.modules.values()):
                if module is not None and getattr(module, "__name__", "").startswith(
                    (
                        "riverhog_core.canonical_discovery",
                        "riverhog_archive_contracts",
                        "riverhog_canonical_json",
                        "riverhog_provenance",
                    )
                ):
                    if getattr(module, "sha256", None) is actual_hash:
                        patch.setattr(module, "sha256", Hash)
                    if getattr(module, "canonical_json_bytes", None) is canonical_json_bytes:
                        patch.setattr(module, "canonical_json_bytes", encode)
                    if getattr(module, "require_canonical_json", None) is require_canonical_json:
                        patch.setattr(module, "require_canonical_json", decode)
            update, export = CheckpointSHA256.update_bytes, CheckpointSHA256.export_state
            restore, compress = CheckpointSHA256.from_state, sha256_continuation.sha256_compress

            def updated(instance, raw):
                if self.active:
                    self.metrics["hashed_octets"] += len(raw)
                return update(instance, raw)

            def exported(instance):
                result = export(instance)
                if self.active:
                    self.metrics["encoded_octets"] += len(result.encode())
                return result

            def restored(cls, raw):
                if self.active:
                    self.metrics["decoded_octets"] += len(raw.encode())
                return restore(raw)

            def block(state, raw):
                if self.active:
                    self.metrics["sha256_block_octets"] += len(raw)
                return compress(state, raw)

            patch.setattr(CheckpointSHA256, "update_bytes", updated)
            patch.setattr(CheckpointSHA256, "export_state", exported)
            patch.setattr(CheckpointSHA256, "from_state", classmethod(restored))
            patch.setattr(sha256_continuation, "sha256_compress", block)
            for module, name in (
                (builtins, "open"),
                (io, "open"),
                (os, "fdopen"),
                (os, "read"),
                (os, "pread"),
                (os, "listdir"),
                (os, "scandir"),
            ):
                actual = getattr(module, name)

                def forbidden(*args, _actual=actual, **kwargs):
                    if self.active:
                        raise AssertionError("owner lifecycle attempted payload I/O or enumeration")
                    return _actual(*args, **kwargs)

                patch.setattr(module, name, forbidden)
            self.active = True
            try:
                yield
            finally:
                self.active = False
        for source, target in {
            "sql_statements": "catalog_sql_statements",
            "driver_statements": "catalog_driver_statements",
            "connection_statements": "catalog_connection_statements",
            "returned_rows": "catalog_returned_rows",
            "returned_metadata_octets": "catalog_returned_octets",
            "bound_parameter_octets": "catalog_parameter_octets",
            "sqlite_vm_instructions": "catalog_vm_instructions",
            "changed_rows": "catalog_changed_rows",
            "uuid_values": "uuid_values",
            "json_encodes": "catalog_json_encodes",
            "json_decodes": "catalog_json_decodes",
            "json_encoded_octets": "catalog_json_encoded_octets",
            "json_decoded_octets": "catalog_json_decoded_octets",
        }.items():
            self.metrics[target] = self.catalog.metrics[source]
        self.metrics["hashed_octets"] += self.catalog.metrics["hashed_octets"]

    def sample(self, root, dimensions):
        for kind, statement, parameters, db in self.statements:
            if statement.lstrip().upper().startswith("SELECT"):
                if statement == "SELECT format FROM discovery_work_format LIMIT 2":
                    continue
                try:
                    plan = " ".join(
                        str(row[3]).upper()
                        for row in db.execute("EXPLAIN QUERY PLAN " + statement, parameters)
                    )
                except sqlite3.ProgrammingError:
                    with _CONNECT(db.location.replace("mode=rw", "mode=ro"), uri=True) as check:
                        plan = " ".join(
                            str(row[3]).upper()
                            for row in check.execute("EXPLAIN QUERY PLAN " + statement, parameters)
                        )
                assert "SCAN " not in plan and "TEMP B-TREE" not in plan, (kind, plan)
        dimensions = dict(
            dimensions,
            control_octets=max(dimensions["control_octets"], self.maximum_control),
            native_binding_octets=max(
                dimensions["native_binding_octets"], self.native_binding_width
            ),
            native_checkpoint_octets=max(
                dimensions["native_checkpoint_octets"], self.native_checkpoint_width
            ),
            active_slots=max(dimensions["active_slots"], self.maximum_slots),
        )
        binding = growth_binding(root)
        projection = project_bindings((binding,))
        sample = {
            "declaration": binding.declaration.id,
            "declaration_sha256": hashlib.sha256(
                canonical_json_bytes(projection["declarations"][binding.declaration.id])
            ).hexdigest(),
            "dimensions": dimensions,
            "metrics": {
                bound.metric: self.metrics[bound.metric] for bound in binding.declaration.bounds
            },
            "delegates": {},
        }
        try:
            validate_execution_samples(projection, binding.declaration.id, [sample])
        except ValueError as error:
            violations = [
                {
                    "metric": bound.metric,
                    "actual": sample["metrics"][bound.metric],
                    "bound": evaluate_bound(binding, bound.metric, dimensions),
                }
                for bound in binding.declaration.bounds
                if sample["metrics"][bound.metric]
                > evaluate_bound(binding, bound.metric, dimensions)
            ]
            raise AssertionError(violations) from error
        return sample


@contextmanager
def case_for(root, admission, template, *, capacity=2):
    root.mkdir()
    with _CONNECT(template.path) as source, _CONNECT(root / "catalog.sqlite3") as copied:
        source.backup(copied)
    engine = create_catalog_engine("sqlite:///" + str(root / "catalog.sqlite3"))
    factory = sessionmaker(engine, expire_on_commit=False)
    catalog = owner.ContractCatalog()
    pool = owner.DiscoveryValidationOwner(factory, root / "owned", concurrency=capacity)
    rec = Recorder()
    pool._slots = Slots(rec)
    case = SimpleNamespace(
        engine=engine,
        factory=factory,
        catalog=catalog,
        admission=admission,
        build_id=template.build_id,
        root=root / "owned",
        pool=pool,
        rec=rec,
        readers=[],
    )
    try:
        with rec.connect_only():
            yield case
    finally:
        pool.close()
        engine.dispose()


def context(case):
    return case.pool.work(build_id=case.build_id, admission=case.admission, catalog=case.catalog)


def dimensions(case, *, retained, width):
    selected = jobs.select_discovery_job(
        case.factory, build_id=case.build_id, admission=case.admission, catalog=case.catalog
    )
    controls = octets(
        (tuple(asdict(selected).values()), case.admission.root_json, case.admission.binding_json)
    )
    values = {
        "retained_core_rows": 0,
        "retained_catalog_rows": 0,
        "retained_native_rows": 0,
        "active_slots": len(case.pool._slots),
        "control_octets": max(controls, width),
        "path_octets": len(os.fsencode(case.root / selected.workspace_id / "discovery.sqlite3")),
        "native_binding_octets": len(selected.binding.to_bytes())
        if selected.source_manifest_sha256 is not None
        else 0,
        "native_checkpoint_octets": 0,
        "native_counter_octets": max(2, len(str(selected.worker_epoch + 1))),
    }
    paths = (
        ("catalog", case.root.parent / "catalog.sqlite3"),
        ("core", case.root / selected.workspace_id / "discovery.sqlite3"),
        ("native", case.root / selected.workspace_id / "corpus.sqlite3"),
    )
    for kind, path in paths:
        if not path.exists():
            continue
        with _CONNECT(path.as_uri() + "?mode=ro", uri=True) as db:
            tables = db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            rows = 0
            for (table,) in tables:
                for row in db.execute(f'SELECT * FROM "{table}"'):
                    rows += 1
                    values["control_octets"] = max(values["control_octets"], octets(row))
            values["retained_" + kind + "_rows"] = rows
            if kind == "native":
                checkpoint = db.execute(
                    "SELECT checkpoint,worker_epoch,version,sources,selections "
                    "FROM corpus_workspace WHERE singleton=1"
                ).fetchone()
                if checkpoint:
                    values["native_checkpoint_octets"] = len(checkpoint[0])
                    values["native_counter_octets"] = max(
                        values["native_counter_octets"], *(len(value) for value in checkpoint[1:])
                    )
    return values


def seed_retained(case, work, retained, width):
    db = work.metadata._db
    row = db.execute("SELECT * FROM discovery_archive_tasks").fetchone()
    db.execute("BEGIN IMMEDIATE").close()
    try:
        for index in range(retained):
            values = list(row)
            values[0] = "retained-" + str(index)
            if index == 0:
                values[4] = "x" * width
            db.execute(
                "INSERT INTO discovery_archive_tasks VALUES(" + ",".join("?" * len(values)) + ")",
                values,
            ).close()
        db.commit()
    except BaseException:
        db.rollback()
        raise


def seed_native(corpus, retained, width):
    corpus._db.executemany(
        "INSERT INTO identities VALUES(?,?,?)",
        [
            (f"retained-{index}", "controlled", b"x" * (width if index == 0 else 1))
            for index in range(retained)
        ],
    )
    corpus._db.commit()


def ready(case, qualified, retained, width, *, native=True):
    with context(case) as work:
        assert work is not None
        selection, _ = seed(work.metadata, qualified)
        seed_retained(case, work, retained, width)
        case.readers.append(selection.history.reader)
        corpus = work.corpus(selection) if native else None
        if corpus is not None:
            seed_native(corpus, retained, width)
    return selection, corpus


def one(case, operation, dims, *, cold, action):
    with case.rec.observe(case, cold=cold):
        result = action()
    return result, case.rec.sample(operation, dims)


def exercise_case(case, qualified, retained, width, cold, mode, samples):
    admission = case.admission
    root = getattr(
        owner.DiscoveryValidationWork if mode == "corpus" else owner.DiscoveryValidationOwner, mode
    )

    def measure(action):
        dims = dimensions(case, retained=retained, width=width)
        result, sample = one(case, root, dims, cold=cold, action=action)
        samples.append(sample)
        return result

    if mode == "_reserve":
        slot = measure(lambda: case.pool._reserve(case.build_id))
        assert slot is not None
        slot.lock.release()
        slot.lock.acquire()
        try:
            assert measure(lambda: case.pool._reserve(case.build_id)) is None
        finally:
            slot.lock.release()
        return
    if mode == "_open":
        slot = case.pool._reserve(case.build_id)
        try:
            assert measure(lambda: case.pool._open(case.build_id, slot, admission, case.catalog))
        finally:
            slot.lock.release()
        _, _ = ready(case, qualified, retained, width)
        case.pool.release_stale()
        slot = case.pool._slots[case.build_id]
        with slot.lock:
            case.pool._drop(case.build_id, slot)
        slot = case.pool._reserve(case.build_id)
        try:
            assert measure(lambda: case.pool._open(case.build_id, slot, admission, case.catalog))
        finally:
            slot.lock.release()
        return
    if mode == "work":

        def empty_quantum():
            with context(case) as work:
                assert work is not None

        measure(empty_quantum)
    selection, native = ready(case, qualified, retained, width, native=mode != "corpus")
    if mode == "corpus":
        with context(case) as work:
            native = measure(lambda: work.corpus(selection))
            seed_native(native, retained, width)
            assert measure(lambda: work.corpus(selection)) is native
        slot = case.pool._slots[case.build_id]
        with slot.lock:
            case.pool._drop(case.build_id, slot)
        with context(case) as work:
            assert measure(lambda: work.corpus(selection)) is not native
    elif mode == "active_build_ids":
        keys = measure(case.pool.active_build_ids)
        assert keys == (case.build_id,)
        # Returned logical identity octets are accounted independently.
        samples[-1]["metrics"]["pool_result_octets"] = octets(keys)
        binding = growth_binding(root)
        validate_execution_samples(
            project_bindings((binding,)), binding.declaration.id, [samples[-1]]
        )
    elif mode == "work":

        def empty_quantum():
            with context(case) as work:
                assert work is not None

        measure(empty_quantum)
        slot = case.pool._slots[case.build_id]
        with slot.lock:

            def busy_quantum():
                with context(case) as work:
                    assert work is None

            measure(busy_quantum)
        directory = slot.metadata.custody.directory
        ctx = context(case)

        def exceptional_quantum():
            assert ctx.__enter__() is not None
            error = RuntimeError("controlled caller body exception")
            assert ctx.__exit__(type(error), error, None) is False

        measure(exceptional_quantum)
        assert case.pool.active_build_ids() == ()
        with context(case) as restarted:
            assert restarted is not None
            native = restarted.corpus(selection)
        slot = case.pool._slots[case.build_id]
        native._corpus._db = UncertainClose(native._db, lost_reply=True, failures=2)
        slot.metadata.custody._held = False

        def unconfirmed_quantum():
            with pytest.raises(OSError, match="unconfirmed"):
                with context(case):
                    pytest.fail("unconfirmed cleanup reopened work")

        measure(unconfirmed_quantum)
        assert case.pool.active_build_ids() == (case.build_id,)
        measure(unconfirmed_quantum)
        assert case.pool.active_build_ids() == (case.build_id,)

        def confirmed_cleanup():
            with context(case) as work:
                assert work is None

        measure(confirmed_cleanup)
        assert case.pool.active_build_ids() == ()
        (directory / ".lease").unlink()

        def lost_quantum():
            with pytest.raises(owner.LostJournalValidationWorkspace):
                with context(case):
                    pytest.fail("accepted lost custody reopened work")

        measure(lost_quantum)
        measure(lost_quantum)
        assert case.pool.active_build_ids() == ()
    elif mode == "_drop":
        slot = case.pool._slots[case.build_id]
        with slot.lock:
            measure(lambda: case.pool._drop(case.build_id, slot))
            measure(lambda: case.pool._drop(case.build_id, slot))
    elif mode == "release_stale":
        measure(case.pool.release_stale)
        slot = case.pool._slots[case.build_id]
        with slot.lock:
            measure(case.pool.release_stale)
        slot.metadata.custody._held = False
        measure(case.pool.release_stale)
        assert case.pool.active_build_ids() == ()
    else:
        measure(case.pool.close)
        measure(case.pool.close)


def exercise(tmp_path, fixed_inputs, qualified, catalog_templates, record_property, mode):
    admission, _ = fixed_inputs
    samples = []
    for retained, width in SPECIFICATIONS:
        for cold in (False, True):
            path = tmp_path / f"{retained}-{width}-{cold}"
            with case_for(path, admission, catalog_templates[(retained, width)]) as case:
                exercise_case(case, qualified, retained, width, cold, mode, samples)
    record_property("growth_samples", samples)
    record_property(
        "owner_conditions",
        "Actual selected catalog, real kernel lease, owned Core and Native SQLite, complete "
        "rows/parameters, VM, codecs/hash/block work and persistent descriptor hooks; no Python "
        "payload I/O or enumeration. Independent unrelated retained controls and widths "
        "1/17/1025, cold/warm catalog; fresh/warm/reopen/busy/stale/repeated cleanup branches. "
        "Context body algorithms and full collection acceptance remain separately qualified.",
    )


@pytest.mark.growth_witness(
    "core-discovery-owner-reserve-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.discovery_validation_owner.DiscoveryValidationOwner._reserve",
)
def test_owner_reserve_work(tmp_path, fixed_inputs, qualified, catalog_templates, record_property):
    exercise(tmp_path, fixed_inputs, qualified, catalog_templates, record_property, "_reserve")


@pytest.mark.growth_witness(
    "core-discovery-owner-open-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.discovery_validation_owner.DiscoveryValidationOwner._open",
)
def test_owner_open_work(tmp_path, fixed_inputs, qualified, catalog_templates, record_property):
    exercise(tmp_path, fixed_inputs, qualified, catalog_templates, record_property, "_open")


@pytest.mark.growth_witness(
    "core-discovery-owner-work-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.discovery_validation_owner.DiscoveryValidationOwner.work",
)
def test_owner_work_work(tmp_path, fixed_inputs, qualified, catalog_templates, record_property):
    exercise(tmp_path, fixed_inputs, qualified, catalog_templates, record_property, "work")


@pytest.mark.growth_witness(
    "core-discovery-owner-corpus-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.discovery_validation_owner.DiscoveryValidationWork.corpus",
)
def test_owner_corpus_work(tmp_path, fixed_inputs, qualified, catalog_templates, record_property):
    exercise(tmp_path, fixed_inputs, qualified, catalog_templates, record_property, "corpus")


@pytest.mark.growth_witness(
    "core-discovery-owner-drop-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.discovery_validation_owner.DiscoveryValidationOwner._drop",
)
def test_owner_drop_work(tmp_path, fixed_inputs, qualified, catalog_templates, record_property):
    exercise(tmp_path, fixed_inputs, qualified, catalog_templates, record_property, "_drop")


@pytest.mark.growth_witness(
    "core-discovery-owner-active-build-ids-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.discovery_validation_owner.DiscoveryValidationOwner.active_build_ids",
)
def test_owner_active_build_ids_work(
    tmp_path, fixed_inputs, qualified, catalog_templates, record_property
):
    exercise(
        tmp_path, fixed_inputs, qualified, catalog_templates, record_property, "active_build_ids"
    )


@pytest.mark.growth_witness(
    "core-discovery-owner-release-stale-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.discovery_validation_owner.DiscoveryValidationOwner.release_stale",
)
def test_owner_release_stale_work(
    tmp_path, fixed_inputs, qualified, catalog_templates, record_property
):
    exercise(tmp_path, fixed_inputs, qualified, catalog_templates, record_property, "release_stale")


@pytest.mark.growth_witness(
    "core-discovery-owner-close-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.discovery_validation_owner.DiscoveryValidationOwner.close",
)
def test_owner_close_work(tmp_path, fixed_inputs, qualified, catalog_templates, record_property):
    exercise(tmp_path, fixed_inputs, qualified, catalog_templates, record_property, "close")


@pytest.mark.growth_witness(
    "core-discovery-owner-close-retry-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.discovery_validation_owner.DiscoveryValidationOwner.close",
)
def test_owner_actual_close_retries_and_independent_slots(
    tmp_path, fixed_inputs, qualified, catalog_templates, record_property
):
    cleanup_regimes(tmp_path, fixed_inputs, qualified, catalog_templates, record_property, "close")


@pytest.mark.growth_witness(
    "core-discovery-owner-stale-retry-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.discovery_validation_owner.DiscoveryValidationOwner.release_stale",
)
def test_owner_actual_stale_retries_and_independent_slots(
    tmp_path, fixed_inputs, qualified, catalog_templates, record_property
):
    cleanup_regimes(
        tmp_path, fixed_inputs, qualified, catalog_templates, record_property, "release_stale"
    )


def cleanup_regimes(tmp_path, fixed_inputs, qualified, catalog_templates, record_property, mode):
    admission, _ = fixed_inputs
    samples = []
    root = getattr(owner.DiscoveryValidationOwner, mode)
    for count in (1, 2, 8):
        for failure in (None, "native-before", "native-lost", "core-before", "core-lost"):
            path = tmp_path / f"{count}-{failure}"
            with case_for(path, admission, catalog_templates[(17, 17)], capacity=count) as case:
                builds = [case.build_id] + [
                    independent_build(case, index + 2) for index in range(count - 1)
                ]
                for build_id in builds:
                    with case.pool.work(
                        build_id=build_id, admission=admission, catalog=case.catalog
                    ) as work:
                        assert work is not None
                        selection, _ = seed(work.metadata, qualified)
                        if selection.history.reader not in case.readers:
                            case.readers.append(selection.history.reader)
                        seed_retained(case, work, 17, 17)
                        seed_native(work.corpus(selection), 17, 17)
                first = case.pool._slots[case.build_id]
                dims = dimensions(case, retained=17, width=17)
                if failure is not None:
                    lost = failure.endswith("lost")
                    if failure.startswith("native"):
                        first.corpus._corpus._db = UncertainClose(first.corpus._db, lost_reply=lost)
                    else:
                        first.metadata._db = UncertainClose(first.metadata._db, lost_reply=lost)
                if mode == "release_stale":
                    for slot in case.pool._slots.values():
                        slot.metadata.custody._held = False
                with case.rec.observe(case, cold=True):
                    if failure is None:
                        getattr(case.pool, mode)()
                    else:
                        with pytest.raises(OSError, match="unconfirmed"):
                            getattr(case.pool, mode)()
                samples.append(case.rec.sample(root, dims))
                assert len(case.pool._slots) == int(failure is not None)
                dims = dict(dims, active_slots=len(case.pool._slots))
                with case.rec.observe(case, cold=False):
                    getattr(case.pool, mode)()
                samples.append(case.rec.sample(root, dims))
                assert case.pool.active_build_ids() == ()
    record_property("growth_samples", samples)
    record_property(
        "owner_cleanup_conditions",
        "Actual independent selected builds with 1/2/8 occupied slots and real held kernel "
        "leases, Core and Native storage; both failure-before-close and lost close replies. "
        "One uncertain slot stays reachable while independent slots close. Retried closure "
        "never reopens work, replays payload or removes a slot before all handles confirm.",
    )
