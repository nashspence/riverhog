import hashlib
import json
import sqlite3
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace

import pytest
import riverhog_canonical_json as canonical
from riverhog_core.services import collection_uploads as owner
from riverhog_core.services import collection_uploads as source_owner
from riverhog_core.services import upload_journal_certificates as certificates
from riverhog_core.services import upload_journal_epochs as epochs
from riverhog_core.services import upload_journal_rank as rank
from riverhog_core.services import upload_journal_replay as replay
from riverhog_core.services import upload_journal_topology as topology
from riverhog_core.services import upload_journal_union as union
from riverhog_growth_contracts import growth_binding
from riverhog_provenance import framing, replay_binding, topology_checkpoint
from sqlalchemy import event

from tests.unit import test_upload_journal_validation_advance_growth as native
from tests.unit.test_journal_capture import retained
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_journal_validation_handoff import service_for, terminal
from tests.unit.test_staged_journal_range import EXTENT, JOURNAL
from tests.unit.test_upload_journal_service_coordination_growth import sample
from tests.unit.test_upload_journal_source_growth import octets


class CoreRecorder:
    def __init__(self, *, interrupt=False):
        self.interrupt = interrupt
        self.interrupted = False
        self.cursors = []
        self.active = False
        self.codecs_active = True
        self.metrics = defaultdict(int)
        self.connections = []
        self.statements = []

    def row(self, row):
        if self.active and row is not None:
            self.metrics["returned_rows"] += 1
            self.metrics["source_payload_octets"] += sum(
                len(value) for value in row if isinstance(value, bytes)
            )
            self.metrics["returned_metadata_octets"] += sum(
                octets(value) for value in row if not isinstance(value, bytes)
            )
        return row

    @contextmanager
    def observe(self, engine):
        observer = self
        connect = engine.dialect.dbapi.connect
        loads, dumps, encode, sha256 = (
            json.loads,
            json.dumps,
            canonical.canonical_json_bytes,
            hashlib.sha256,
        )
        engine.dispose()

        class Cursor(sqlite3.Cursor):
            closed = False

            def close(self):
                self.closed = True
                return super().close()

            def execute(self, sql, parameters=()):
                if observer.active:
                    observer.metrics["bound_parameter_octets"] += octets(parameters)
                return super().execute(sql, parameters)

            def executemany(self, sql, parameters):
                def rows():
                    for row in parameters:
                        if observer.active:
                            observer.metrics["bound_parameter_octets"] += octets(row)
                        yield row

                return super().executemany(sql, rows())

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
                observer.connections.append(self)
                self.set_trace_callback(self.trace)
                self.set_progress_handler(self.tick, 1)
                self.set_authorizer(self.authorize)

            def cursor(self, *args, **kwargs):
                kwargs["factory"] = Cursor
                cursor = super().cursor(*args, **kwargs)
                if observer.active:
                    observer.cursors.append(cursor)
                return cursor

            def trace(self, _sql):
                if observer.active:
                    observer.metrics["driver_statements"] += 1

            def tick(self):
                if observer.active:
                    observer.metrics["sqlite_vm_instructions"] += 1
                return int(observer.active and observer.interrupted)

            def authorize(self, action, table, column, _database, _trigger):
                if observer.active and action == sqlite3.SQLITE_READ:
                    if column in {"canonical_json", "exact_json", "schema_json"}:
                        return sqlite3.SQLITE_DENY
                    if column == "content" and table not in {
                        source_owner.CollectionUploadProvenanceJournalChunkRecord.__tablename__,
                        source_owner.CollectionUploadProvenanceCaptureSegmentRecord.__tablename__,
                    }:
                        return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

        def connected(*args, **kwargs):
            kwargs["factory"] = Connection
            return connect(*args, **kwargs)

        def application(_connection, _cursor, statement, parameters, _context, many):
            if observer.active:
                observer.metrics["sql_statements"] += 1
                observer.statements.append((statement, parameters, many))
                if observer.interrupt and statement.startswith(
                    "UPDATE collection_upload_provenance_journals "
                ):
                    observer.interrupted = True

        def decoded(value, *args, **kwargs):
            if self.active and self.codecs_active:
                self.metrics["json_decodes"] += 1
                self.metrics["json_decoded_octets"] += octets(value)
            return loads(value, *args, **kwargs)

        def encoded(value, *args, **kwargs):
            observing = self.active and self.codecs_active
            if observing:
                self.metrics["json_encodes"] += 1
            result = encode(value, *args, **kwargs)
            if observing:
                self.metrics["json_encoded_octets"] += len(result)
            return result

        def dumped(value, *args, **kwargs):
            observing = self.active and self.codecs_active
            if observing:
                self.metrics["json_encodes"] += 1
            result = dumps(value, *args, **kwargs)
            if observing:
                self.metrics["json_encoded_octets"] += len(result.encode())
            return result

        class Hash:
            def __init__(self, value):
                self.actual = value

            def update(self, value):
                if observer.active:
                    observer.metrics["hashed_octets"] += len(value)
                self.actual.update(value)

            def copy(self):
                return Hash(self.actual.copy())

            def __getattr__(self, name):
                return getattr(self.actual, name)

        def hashed(value=b"", *args, **kwargs):
            if self.active:
                self.metrics["hashed_octets"] += len(value)
            return Hash(sha256(value, *args, **kwargs))

        event.listen(engine, "before_cursor_execute", application)
        try:
            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(engine.dialect.dbapi, "connect", connected)
                patch.setattr(
                    canonical,
                    "json",
                    SimpleNamespace(
                        loads=decoded, dumps=dumps, JSONDecodeError=json.JSONDecodeError
                    ),
                )
                patch.setattr(canonical, "canonical_json_bytes", encoded)
                for module in (
                    source_owner,
                    epochs,
                    topology,
                    topology_checkpoint,
                    union,
                    rank,
                    replay,
                    replay_binding,
                ):
                    if hasattr(module, "json"):
                        patch.setattr(
                            module,
                            "json",
                            SimpleNamespace(
                                loads=decoded, dumps=dumped, JSONDecodeError=json.JSONDecodeError
                            ),
                        )
                    if hasattr(module, "canonical_json_bytes"):
                        patch.setattr(module, "canonical_json_bytes", encoded)
                    if module in (source_owner, epochs, topology, union, rank, replay) and hasattr(
                        module, "hashlib"
                    ):
                        patch.setattr(module, "hashlib", SimpleNamespace(sha256=hashed))
                with engine.connect():
                    pass
                self.active = True
                try:
                    yield
                finally:
                    self.active = False
        finally:
            event.remove(engine, "before_cursor_execute", application)
        self.metrics["connection_statements"] = (
            self.metrics["driver_statements"] - self.metrics["sql_statements"]
        )
        self.metrics["changed_rows"] = sum(db.total_changes for db in self.connections)
        assert engine.pool.checkedout() == 0
        assert all(cursor.closed for cursor in self.cursors)


ROOT = owner.SqlAlchemyCollectionUploadService._advance_provenance_journal_validation_unit
CHILDREN = dict(owner._service_validation_unit_children())


def inputs(engine, raw):
    from sqlalchemy import inspect

    rows, width = 0, 4096
    with engine.connect() as connection:
        for name in inspect(connection).get_table_names():
            with connection.exec_driver_sql('SELECT * FROM "' + name + '"') as result:
                for row in result:
                    rows += 1
                    width = max(
                        width, sum(octets(value) for value in row if not isinstance(value, bytes))
                    )
    frames = raw.split(b"\x1e")[1:]
    extent = owner._JOURNAL_CAPTURE_SEGMENT_OCTETS
    return {
        "retained_rows": rows,
        "metadata_row_octets": width,
        "transport_extent_octets": max(
            (len(raw[offset : offset + EXTENT]) for offset in range(0, len(raw), EXTENT)), default=0
        ),
        "maximum_frame_pages": max(
            ((len(frame) + 1 + extent - 1) // extent for frame in frames), default=0
        ),
    }


class UnitRecorder:
    def __init__(self, case, current):
        self.case = case
        self.current = current
        self.native_recorder = None
        self.context = None
        self.core = None
        self.parts = []
        self.children = None
        self.interrupt = False
        self.native_calls = 0
        lifecycle, implementation = native.opening.native_helpers()
        self.lifecycle, self.implementation = lifecycle, implementation
        import importlib

        with pytest.MonkeyPatch.context() as patch:
            patch.syspath_prepend(
                str(Path(__file__).resolve().parents[2] / "packages/riverhog-provenance/tests")
            )
            self.preparation = importlib.import_module("test_preparation_growth")
        self.frames = {
            hashlib.sha256(atom[:-1]).hexdigest(): atom[:-1] for atom in case.raw.split(b"\x1e")[1:]
        }

    def start(self):
        self.core = CoreRecorder(interrupt=self.interrupt)
        self.context = self.core.observe(self.case.engine)
        self.context.__enter__()

    def finish(self):
        if self.context is not None:
            self.context.__exit__(None, None, None)
            self.parts.append(self.core)
            self.context = None

    @contextmanager
    def paused_codecs(self):
        previous = self.core.codecs_active
        self.core.codecs_active = False
        try:
            yield
        finally:
            self.core.codecs_active = previous

    def prepared(self, tag, *args, **kwargs):
        operation = CHILDREN[tag]
        with self.paused_codecs():
            recorder = self.preparation.Recorder()
            if tag == "topology":
                cursor = args[0]
                document = json.loads(self.frames[cursor.json_sha256])
                body = document.get("body", {})
                assertions = body.get("assertions", {})
                values = (
                    [*body.get("retracts", [])[cursor.retracts.index :]]
                    if cursor.retracts.count
                    else []
                )
                values.extend(assertions.get("relations", [])[cursor.relations.index :])
                dimensions = {
                    "item_budget": kwargs.get("item_budget", 64),
                    "octet_budget": kwargs.get("octet_budget", 65536),
                    "maximum_item_octets": max(
                        (len(canonical.canonical_json_bytes(value)) for value in values), default=0
                    ),
                }
            else:
                dimensions = {"input_octets": len(args[0])}
                if tag == "plan":
                    dimensions["range_fields"] = self.preparation.range_fields(args[0])
            result = None
            try:
                with recorder.observe():
                    result = operation(*args, **kwargs)
                recorder.metrics["returned_cursors" if tag == "plan" else "returned_facts"] += (
                    len(result.facts) if tag == "topology" else 1
                )
                return result
            finally:
                self.children[tag].append(self.preparation.sample(operation, dimensions, recorder))

    def framed(self, cursor, chunk, **kwargs):
        visited = 0
        actual = framing._octets

        def octets(data):
            nonlocal visited
            for value in actual(data):
                visited += 1
                yield value

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(framing, "_octets", octets)
            result = CHILDREN["framing"](cursor, chunk, **kwargs)
        with self.paused_codecs():
            dimensions = {
                "consumed_octets": result.cursor.byte_offset - cursor.byte_offset,
                "byte_budget": kwargs["byte_budget"],
                "frame_budget": kwargs["frame_budget"],
            }
            metrics = {
                "visited_octets": visited,
                "consumed_octets": dimensions["consumed_octets"],
                "returned_positions": len(result.positions),
                "retained_content_octets": 0,
            }
            assert all(
                not isinstance(value, (str, bytes)) for value in asdict(result.cursor).values()
            )
            self.children["framing"].append(sample(CHILDREN["framing"], dimensions, metrics))
        return result

    def advanced(self, selected, work, *, catalog, budget=None):
        assert self.case.engine.pool.checkedout() == 0
        self.finish()
        self.case.work = work
        self.case.catalog = catalog
        provider = certificates.UploadJournalGraphCertificates(
            self.case.factory, work, catalog=catalog
        )
        self.case.mode = provider._mode
        self.case.source_binding = provider.binding.source.to_json()
        self.case.certificate_binding = provider.binding.to_json()
        if self.native_recorder is None:
            native.populate(self.case)
            self.native_recorder = native.Recorder(self.case)
        try:
            result, observed = self.native_recorder.measure(
                selected, cold=False, budget=budget or native.owner.JournalValidationBudget()
            )
            self.children["owner"].append(observed)
            self.native_calls += 1
            return result
        finally:
            self.start()

    def measure(self, collection_id=1, journal_id=JOURNAL, *, interrupt=False, expected_error=None):
        dimensions = inputs(self.case.engine, self.case.raw)
        dimensions["metadata_row_octets"] = max(
            dimensions["metadata_row_octets"], octets((collection_id, journal_id))
        )
        self.parts = []
        self.children = {tag: [] for tag in CHILDREN}
        self.interrupt = interrupt
        error = None
        try:
            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(
                    self.lifecycle,
                    "LifecycleConnection",
                    native.mixed_connection(self.implementation, self.lifecycle),
                )
                patch.setattr(
                    type(self.current._journal_validation_owner),
                    "advance",
                    lambda selected, work, **kwargs: self.advanced(selected, work, **kwargs),
                )
                patch.setattr(owner, "advance_journal_framing", self.framed)
                patch.setattr(
                    epochs,
                    "classify_epoch_boundary",
                    lambda *args, **kwargs: self.prepared("epoch", *args, **kwargs),
                )
                patch.setattr(
                    topology,
                    "plan_topology_frame",
                    lambda *args, **kwargs: self.prepared("plan", *args, **kwargs),
                )
                patch.setattr(
                    topology,
                    "advance_topology_frame",
                    lambda *args, **kwargs: self.prepared("topology", *args, **kwargs),
                )
                self.start()
                try:
                    ROOT(self.current, collection_id, journal_id)
                finally:
                    self.finish()
        except Exception as caught:
            if expected_error is None or not isinstance(caught, expected_error):
                raise
            error = caught
        self.last_error = error
        if expected_error is None:
            if error is not None:
                raise error
        else:
            assert isinstance(error, expected_error)
        metrics = defaultdict(int)
        for part in self.parts:
            for key, value in part.metrics.items():
                metrics[key] += value
                metrics["core_" + key] += value
        for tag, children in self.children.items():
            dimensions[tag + "_calls"] = len(children)
            metrics[tag + "_calls"] = metrics[tag + "_calls_limit"] = len(children)
            for item in growth_binding(CHILDREN[tag]).declaration.dimensions:
                dimensions[tag + "_" + item.name] = max(
                    (child["dimensions"][item.name] for child in children), default=0
                )
            selected_metric = {
                "framing": "consumed_octets",
                "epoch": "json_decoded_octets",
                "plan": "hashed_octets",
                "topology": "visited_octets",
                "owner": "sql_statements",
            }[tag]
            metrics["native_" + tag + "_" + selected_metric] = sum(
                child["metrics"][selected_metric] for child in children
            )
        self.explain()
        return sample(ROOT, dimensions, metrics, self.children)

    def explain(self):
        from riverhog_core.catalog_models import (
            CollectionUploadProvenanceCaptureSegmentRecord,
            CollectionUploadProvenanceEpochScanRecord,
            CollectionUploadProvenanceFrameRecord,
            CollectionUploadProvenanceJournalRecord,
        )

        protected = {
            model.__tablename__
            for model in (
                CollectionUploadProvenanceJournalRecord,
                CollectionUploadProvenanceFrameRecord,
                CollectionUploadProvenanceCaptureSegmentRecord,
                CollectionUploadProvenanceEpochScanRecord,
            )
        }
        with self.case.engine.connect() as connection:
            for part in self.parts:
                for sql, parameters, many in part.statements:
                    if not sql.startswith("SELECT") or many:
                        continue
                    with connection.exec_driver_sql(
                        "EXPLAIN QUERY PLAN " + sql, parameters
                    ) as rows:
                        plans = tuple(str(row[3]).upper() for row in rows)
                    assert not any(
                        "SCAN " + name.upper() in plan for name in protected for plan in plans
                    )


def real_case(root, *, text_octets=0, unrelated=0, raw=None):
    from riverhog_core.catalog_models import CollectionUploadProvenanceJournalRecord as Journal
    from riverhog_core.checkpoint_sha256 import CheckpointSHA256

    from tests.unit.test_finalization_readiness_growth import clone_upload

    raw = source(text_octets, journal_id=JOURNAL) if raw is None else raw
    with retained(root, raw) as (engine, factory):
        factory.configure(autoflush=False, expire_on_commit=False)
        if unrelated:
            clone_upload(factory, 2)
            with factory() as session, session.begin():
                session.add_all(
                    Journal(
                        collection_id=2,
                        journal_id="unrelated-" + str(i),
                        bytes=0,
                        sha256="0" * 64,
                        content_hash_state=CheckpointSHA256().export_state(),
                        state="sealed",
                    )
                    for i in range(unrelated)
                )
        current = service_for(factory)
        try:
            yield SimpleNamespace(engine=engine, factory=factory, raw=raw), current
        finally:
            current.close()


@pytest.mark.growth_witness(
    "riverhog-validation-service-unit-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation=(
        "riverhog_core.services.collection_uploads.SqlAlchemyCollectionUploadService._advance_pro"
        "venance_journal_validation_unit"
    ),
)
def test_real_validation_service_units(tmp_path, record_property):
    samples = []
    baseline = None
    for index, (text_octets, unrelated) in enumerate(((0, 0), (0, 1025), (65537, 17))):
        for case, current in real_case(
            tmp_path / str(index), text_octets=text_octets, unrelated=unrelated
        ):
            recorder = UnitRecorder(case, current)
            first = len(samples)
            prepared = 0
            for _ in range(100):
                observed = recorder.measure()
                samples.append(observed)
                if terminal(case.factory)["state"] != "validating":
                    break
                if recorder.native_calls in {1, 2} and prepared != recorder.native_calls:
                    slot = next(iter(current._journal_validation_owner._slots.values()))
                    session = slot.session
                    for _fixture_step in range(10000):
                        if (
                            bool(session._input.pending)
                            if recorder.native_calls == 1
                            else session._ledger.load().checkpoint.phase == "terminal"
                        ):
                            break
                        assert not session.advance().complete
                    else:
                        raise AssertionError(
                            "actual Native fixture did not reach selected boundary"
                        )
                    prepared = recorder.native_calls
            else:
                raise AssertionError("actual service unit did not finish")
            assert terminal(case.factory)["state"] == "sealed"
            assert recorder.native_calls == 3
            if index == 0:
                baseline = samples[first:]
            if index == 1:
                assert len(samples[first:]) == len(baseline)
                for before, after in zip(baseline, samples[first:], strict=True):
                    for metric in (
                        "core_sql_statements",
                        "core_returned_rows",
                        "core_source_payload_octets",
                        "core_hashed_octets",
                        "core_changed_rows",
                    ):
                        assert before["metrics"][metric] == after["metrics"][metric]
                    assert (
                        after["metrics"]["core_sqlite_vm_instructions"]
                        <= before["metrics"]["core_sqlite_vm_instructions"] + 1024
                    )
            assert all(
                observed["metrics"]["core_changed_rows"] <= 64 + 64 + 16 + 64 + 64 + 256
                for observed in samples[-3:]
            )
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-validation-service-unit-rejections/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation=(
        "riverhog_core.services.collection_uploads.SqlAlchemyCollectionUploadService._advance_pro"
        "venance_journal_validation_unit"
    ),
)
def test_validation_service_absence_rollback_and_retry(tmp_path, record_property):
    from sqlalchemy.exc import OperationalError

    from tests.unit.test_journal_capture import checkpoint

    samples = []
    for case, current in real_case(tmp_path):
        recorder = UnitRecorder(case, current)
        samples.append(recorder.measure(journal_id="absent"))
        before = checkpoint(case.factory)
        samples.append(recorder.measure(interrupt=True, expected_error=OperationalError))
        assert checkpoint(case.factory) == before
        assert samples[-1]["metrics"]["framing_calls"] == 1
        assert samples[-1]["metrics"]["owner_calls"] == 0
        samples.append(recorder.measure())
        assert checkpoint(case.factory) != before
    from sqlalchemy import update

    from tests.unit.test_journal_epoch_scan import finish as finish_epochs
    from tests.unit.test_journal_validation_handoff import complete_capture

    for case, current in real_case(tmp_path / "malformed"):
        work = complete_capture(case.factory)
        finish_epochs(case.factory, work)
        with case.factory() as session, session.begin():
            session.execute(update(epochs.Scan).values(topology_cursor_json="x" * 65537)).close()
        before = checkpoint(case.factory)
        recorder = UnitRecorder(case, current)
        samples.append(recorder.measure(expected_error=RuntimeError))
        assert checkpoint(case.factory) == before
        assert samples[-1]["metrics"]["core_json_decoded_octets"] >= 65537
        assert samples[-1]["metrics"]["owner_calls"] == 0
        assert terminal(case.factory)["state"] == "validating"
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-validation-service-unit-preparation/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation=(
        "riverhog_core.services.collection_uploads.SqlAlchemyCollectionUploadService._advance_pro"
        "venance_journal_validation_unit"
    ),
)
def test_validation_service_large_preparation_windows(tmp_path, record_property):
    from tests.unit.test_journal_epoch_scan import finish as finish_epochs
    from tests.unit.test_journal_epoch_scan import progress as epoch_progress
    from tests.unit.test_journal_epoch_scan import transcript
    from tests.unit.test_journal_epoch_union import edge, raw_edges
    from tests.unit.test_journal_topology_capture import captured
    from tests.unit.test_journal_validation_handoff import complete_capture

    samples = []
    for count in (129, 1025):
        raw = transcript(["journal_init"] + ["assertion"] * (count - 1))
        for case, current in real_case(tmp_path / ("frames-" + str(count)), raw=raw):
            complete_capture(case.factory)
            recorder = UnitRecorder(case, current)
            samples.append(recorder.measure())
            assert epoch_progress(case.factory)[1] == min(count, 64)
            assert samples[-1]["metrics"]["epoch_calls"] == min(count, 64)
            assert samples[-1]["metrics"]["owner_calls"] == 0
        raw = raw_edges([edge(index) for index in range(count)])
        for case, current in real_case(tmp_path / ("items-" + str(count)), raw=raw):
            work = complete_capture(case.factory)
            finish_epochs(case.factory, work)
            recorder = UnitRecorder(case, current)
            samples.append(recorder.measure())
            assert len(captured(case.factory)) == min(count, 64)
            assert samples[-1]["metrics"]["plan_calls"] == 1
            assert samples[-1]["metrics"]["topology_calls"] == 1
            assert samples[-1]["metrics"]["owner_calls"] == 0
            if count > 64:
                samples.append(recorder.measure())
                assert len(captured(case.factory)) == 128
                assert samples[-1]["metrics"]["plan_calls"] == 0
                assert samples[-1]["metrics"]["topology_calls"] == 1
                assert samples[-1]["metrics"]["owner_calls"] == 0
    record_property("growth_samples", samples)


@pytest.mark.parametrize("first", ("collection_uploads", "upload_journal_validation_owner"))
def test_service_unit_declaration_has_one_owner_in_either_import_order(first):
    import subprocess
    import sys

    code = f"""
import importlib
from riverhog_growth_contracts import growth_binding
importlib.import_module("riverhog_core.services.{first}")
from riverhog_core.services import collection_uploads as source
from riverhog_core.services import upload_journal_validation_owner as owner
operation=source.SqlAlchemyCollectionUploadService._advance_provenance_journal_validation_unit
binding=growth_binding(operation)
assert binding.operation is operation
assert binding.declaration.id == "riverhog-server.validation-service-unit.growth/v1"
assert dict(binding.delegates)["owner"] is owner.UploadJournalValidationOwner.advance
"""
    subprocess.run([sys.executable, "-B", "-c", code], check=True, capture_output=True)
