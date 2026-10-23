from __future__ import annotations

import hashlib
import importlib
import json
import os
import sqlite3
import sys
import traceback
from collections import defaultdict
from contextlib import ExitStack, contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.services import upload_journal_validation_owner as owner
from riverhog_growth_contracts import (
    growth_binding,
    project_bindings,
    validate_execution_samples,
)
from riverhog_provenance._entry_replay import EntryReplay
from riverhog_provenance.journal_store import JournalStore
from sqlalchemy import select

from tests.unit import test_upload_journal_certificate_edge_growth as certificate_edges
from tests.unit import test_upload_journal_owned_source_growth as fences
from tests.unit import test_upload_journal_owned_source_read_growth as reads
from tests.unit import test_upload_journal_validation_open_growth as opening
from tests.unit import test_upload_journal_validation_pool_growth as pool
from tests.unit import test_upload_journal_validation_release_growth as release
from tests.unit.test_staged_journal_range import Chunk
from tests.unit.test_upload_journal_source_growth import octets

ROOT = owner.UploadJournalValidationOwner.advance
SWEEP = owner.UploadJournalValidationOwner.release_stale
RESERVE = owner.UploadJournalValidationOwner._reserve
DROP = owner.UploadJournalValidationOwner._drop
OPEN = owner.UploadJournalValidationOwner._open
PROGRESS, ADVANCE, RESULT, CLOSE = (
    getattr(owner.JournalValidationSession, name)
    for name in ("progress", "advance", "result", "close")
)
EDGE = owner.UploadJournalGraphCertificates.edge
EDGE_PROBE = owner.UploadJournalGraphCertificates._probe
CHILDREN = dict(owner._advance_children())


class EvidenceFailure(RuntimeError):
    pass


def mixed_connection(native, lifecycle):
    lifecycle_connection = lifecycle.LifecycleConnection

    class Connection(native.SessionConnection, lifecycle_connection):
        work_observing = False
        raw_authorizer = None

        def set_trace_callback(self, callback):
            if self.work_observing and callback is not None:
                actual = callback

                def callback(sql):
                    return actual(sql) if self.recording else None

            return sqlite3.Connection.set_trace_callback(self, callback)

        def set_progress_handler(self, callback, steps):
            if self.work_observing and callback is not None:
                actual = callback

                def callback():
                    return actual() if self.recording else 0

            return sqlite3.Connection.set_progress_handler(self, callback, steps)

        @contextmanager
        def pause(self):
            recording, self.recording = self.recording, False
            authorizer = self.raw_authorizer
            sqlite3.Connection.set_authorizer(self, None)
            try:
                yield
            finally:
                self.recording = recording
                self.set_authorizer(authorizer)

        def execute(self, sql, parameters=()):
            if self.work_observing:
                return super().execute(sql, parameters)
            if self.observer.recording:
                return lifecycle_connection.execute(self, sql, parameters)
            return sqlite3.Connection.execute(self, sql, parameters)

        def set_authorizer(self, callback):
            self.raw_authorizer = callback
            if self.work_observing:
                return super().set_authorizer(callback)
            return sqlite3.Connection.set_authorizer(self, callback)

        def close(self):
            recorder = getattr(self, "parent_recorder", None)
            if recorder is not None:
                recorder.add("closed_databases")
            return super().close()

        @contextmanager
        def observe(self):
            self.work_observing = True
            try:
                with super().observe() as metrics:
                    yield metrics
            finally:
                self.work_observing = False
                self.set_trace_callback(self.trace)
                self.set_progress_handler(self.tick, 1)
                self.set_authorizer(self.authorize)

    return Connection


class ConcreteSource:
    def __init__(self, current, case):
        self.current, self.case, self.raw = current, case, case.raw

    @property
    def binding(self):
        return self.current.binding

    @property
    def window(self):
        session = self.case.session
        offset = session._input.cursor.byte_offset
        prefix = self.case.work.cursor.byte_offset
        page = owner.source_owner._JOURNAL_CAPTURE_SEGMENT_OCTETS
        if offset < prefix:
            return min(page - offset % page, prefix - offset)
        return min(
            page, next((end - offset for start, end in self.case.spans if start <= offset < end), 0)
        )

    def verify_current(self):
        return self.current.verify_current()

    def read_window(self, offset, size):
        return self.current.read_window(offset, size)

    def read_exact(self, offset, size):
        return self.current.read_exact(offset, size)


class Pool(pool.Slots):
    def items(self):
        for pair in dict.items(self):
            self.recorder.add("pool_slot_visits")
            yield pair

    def __delitem__(self, key):
        self.recorder.add("pool_map_removals")
        return dict.__delitem__(self, key)


class Lock(release.Lock):
    def locked(self):
        return self.actual.locked()

    def release(self):
        if not self.pool:
            self.recorder.add("slot_lock_releases")
        return super().release()


class Recorder(opening.Recorder):
    def __init__(self, case):
        super().__init__(case)
        self.helpers = opening.native_helpers()
        self.lifecycle, self.native_tests = self.helpers
        self.pool_held = False
        self.dbs = []
        self.work_contexts = {}
        self.native_child_costs = defaultdict(lambda: defaultdict(int))
        self.in_work = False
        self.budget = None
        self.failures = []
        self.callback_active = False
        self.work_changes = {}
        self.wrapped_storages = set()
        self.edge_kinds = []
        self.native_capacity = None
        self.recording_error = None
        self.native_error = None
        self.error_format_calls = 0
        self.rejected_snapshot_preserved = False
        self.functions = (
            *opening.FUNCTIONS,
            *reads.READ_FUNCTIONS,
            ROOT,
            SWEEP,
            RESERVE,
            OPEN,
            DROP,
            EDGE,
            EDGE_PROBE,
            PROGRESS,
            ADVANCE,
            RESULT,
            CLOSE,
        )
        self.locations.update(
            {
                SWEEP: (owner.UploadJournalValidationOwner, "release_stale"),
                RESERVE: (owner.UploadJournalValidationOwner, "_reserve"),
                OPEN: (owner.UploadJournalValidationOwner, "_open"),
                DROP: (owner.UploadJournalValidationOwner, "_drop"),
                reads.WINDOW: (owner._OwnedSource, "read_window"),
                reads.EXACT: (owner._OwnedSource, "read_exact"),
                reads.SOURCE_WINDOW: (owner.UploadJournalValidationSource, "read_window"),
                reads.SOURCE_EXACT: (owner.UploadJournalValidationSource, "read_exact"),
                reads.CAPTURE: (owner.source_owner, "_read_upload_journal_capture_segment"),
                EDGE: (owner.UploadJournalGraphCertificates, "edge"),
                EDGE_PROBE: (owner.UploadJournalGraphCertificates, "_probe"),
            }
        )
        self.payload_operations = {
            reads.CAPTURE: {
                owner.source_owner.CollectionUploadProvenanceCaptureSegmentRecord.__tablename__
            },
            reads.SOURCE_WINDOW: {reads.Chunk.__tablename__},
        }
        self.application_functions = self.application_functions | {
            reads.SOURCE_WINDOW,
            reads.SOURCE_EXACT,
            reads.CAPTURE,
        }
        for function in self.functions:
            binding = growth_binding(function)
            self.bindings[function] = binding
            projection = project_bindings((binding,))
            self.projections[function] = projection
            self.digests.update(
                {
                    identity: hashlib.sha256(canonical_json_bytes(value)).hexdigest()
                    for identity, value in projection["declarations"].items()
                }
            )
            for bound in binding.declaration.bounds:
                self.full.setdefault(bound.metric, 0)
                self.application.setdefault(bound.metric, 0)
        self.full.setdefault("slot_lock_releases", 0)

    def add(self, metric, amount=1):
        if self.active:
            self.full[metric] += amount
            aliases = {
                "map_lookups": "pool_map_lookups",
                "map_insertions": "pool_map_insertions",
                "map_removals": "pool_map_removals",
            }
            if metric in aliases:
                self.full[aliases[metric]] += amount

    @contextmanager
    def paused(self):
        with ExitStack() as contexts:
            for db in self.dbs:
                if not db.closed:
                    contexts.enter_context(db.pause())
            with super().paused():
                yield

    def codec_recording(self):
        return (
            self.active and not any(db.recording for db in self.dbs) and super().codec_recording()
        )

    @contextmanager
    def controls(self):
        with super().controls():
            previous = sys.getprofile()
            real_loads = json.loads
            real_error_string = owner.ProvenanceValidationError.__str__

            def error_string(error):
                if self.active and error is self.native_error:
                    assert len(error.args) == 1 and type(error.args[0]) is str
                    self.full["error_formatted_octets"] += octets(error.args[0])
                    self.error_format_calls += 1
                return real_error_string(error)

            real_identity = opening.certificates_owner.require_canonical_uuid_urn

            def identity(value):
                if self.codec_recording():
                    self.full["validated_identity_octets"] += octets(value)
                return real_identity(value)

            def loads(value, *args, **kwargs):
                if self.active and not self.in_work and any(db.recording for db in self.dbs):
                    self.full["json_decoded_octets"] += octets(value)
                return real_loads(value, *args, **kwargs)

            class Json:
                def __init__(self, original):
                    self.original = original
                    self.loads = loads

                def __getattr__(self, name):
                    return getattr(self.original, name)

            def profile(frame, event, arg):
                if previous is not None:
                    previous(frame, event, arg)
                if (
                    self.active
                    and event == "return"
                    and frame.f_code is reads.SOURCE_EXACT.__code__
                ):
                    size = sum(map(len, frame.f_locals.get("pieces", ())))
                    self.full["assembled_piece_octets"] += size
                    self.application["assembled_piece_octets"] += size

            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(owner.ProvenanceValidationError, "__str__", error_string)
                patch.setattr(opening.certificates_owner, "require_canonical_uuid_urn", identity)
                patch.setattr(opening.certificate_codec, "require_canonical_uuid_urn", identity)
                for name, module in tuple(sys.modules.items()):
                    if (
                        name.startswith("riverhog_provenance.")
                        and module is not None
                        and hasattr(module, "json")
                    ):
                        patch.setattr(module, "json", Json(module.json))
                sys.setprofile(profile)
                try:
                    yield
                finally:
                    sys.setprofile(previous)

    @contextmanager
    def kernel(self):
        with super().kernel():
            actual_close = os.close

            def close(fd):
                if self.active and self.callback_active:
                    assert not self.owner._lock.locked()
                    self.add("closed_files")
                    self.add("lease_descriptor_closes")
                return actual_close(fd)

            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(os, "close", close)
                yield

    def dimensions(self, function, args, kwargs):
        with self.paused():
            if function in {ROOT, SWEEP, RESERVE, DROP}:
                current = args[0]
                dimensions = {
                    "configured_slots": current.concurrency,
                    "pending_slots": len(current._slots),
                }
                if function is RESERVE:
                    dimensions.update(
                        input_key_octets=octets(args[1]),
                        retained_key_octets=octets(tuple(current._slots)),
                    )
                elif function is ROOT:
                    rows = fences.snapshot(self.case.engine)
                    width = max([1, *(octets(row) for values in rows.values() for row in values)])
                    dimensions.update(
                        row_budget=self.budget.rows,
                        metadata_row_octets=16384 + 2 * width,
                        error_control_octets=octets(self.recording_error)
                        if self.recording_error is not None
                        else 0,
                    )
                return dimensions
            if function is OPEN:
                rows = fences.snapshot(self.case.engine)
                return {
                    "metadata_row_octets": max(
                        [1, *(octets(row) for values in rows.values() for row in values)]
                    ),
                    "binding_output_octets": octets(
                        (self.case.source_binding, self.case.certificate_binding)
                    ),
                    "lease_root_path_octets": len(os.fsencode(self.owner.root)),
                    "lease_workspace_identity_octets": 32,
                }
            if function in {reads.WINDOW, reads.EXACT}:
                selected, offset, size = args
                fields = {
                    "metadata_row_octets": super().dimensions(
                        fences.OWNED_CURRENT, (selected,), {}
                    )["metadata_row_octets"]
                }
                for tag, child in growth_binding(function).delegates:
                    fields[tag + "_calls"] = 1
                    inputs = self.dimensions(
                        child,
                        ((selected,) if tag == "verify" else (selected.source, offset, size)),
                        {},
                    )
                    fields.update({tag + "_" + name: value for name, value in inputs.items()})
                return fields
            if function in {reads.SOURCE_WINDOW, reads.SOURCE_EXACT}:
                selected, offset, size = args
                values = reads.read_dimensions(self.case, selected, function, offset, size)
                values["retained_journals"] = super().dimensions(fences.CURRENT, (selected,), {})[
                    "retained_journals"
                ]
                return values
            if function is reads.CAPTURE:
                return {
                    "captured_segments": (self.case.work.cursor.byte_offset + reads.PAGE - 1)
                    // reads.PAGE
                }
            if function is EDGE:
                from types import SimpleNamespace

                selected = SimpleNamespace(
                    factory=self.case.factory, work=self.case.work, source=args[0]
                )
                row, _proof, binding_octets, proof_octets = certificate_edges.oracle(
                    selected, kwargs
                )
                rows = fences.snapshot(self.case.engine)
                width = max(
                    [octets(row), *(octets(row) for values in rows.values() for row in values)]
                )
                return {
                    "retained_rows": sum(map(len, rows.values())),
                    "metadata_row_octets": width,
                    "probe_calls": 1,
                    "request_octets": octets(kwargs),
                    "binding_octets": binding_octets,
                    "proof_octets": proof_octets,
                }
            if function is EDGE_PROBE:
                parent, node = self.stack[-1]
                assert parent is EDGE
                return {
                    item.name: node["dimensions"][item.name]
                    for item in growth_binding(EDGE_PROBE).declaration.dimensions
                }
            return super().dimensions(function, args, kwargs)

    def attach(self, operation, sample):
        if self.stack:
            parent, node = self.stack[-1]
            tags = [tag for tag, child in self.bindings[parent].delegates if child is operation]
            assert len(tags) == 1
            node["delegates"][tags[0]].append(sample)
        else:
            self.records[operation].append(sample)

    def validate(self, operation, sample):
        try:
            validate_execution_samples(
                self.projections[operation], growth_binding(operation).declaration.id, [sample]
            )
        except ValueError as error:
            record = {"declaration": sample["declaration"], "error_class": type(error).__name__}
            declared = {item.name for item in growth_binding(operation).declaration.dimensions}
            record["missing_inputs"] = sorted(declared - set(sample["dimensions"]))
            record["extra_inputs"] = sorted(set(sample["dimensions"]) - declared)
            pointer = error.__traceback__
            while pointer is not None:
                local = pointer.tb_frame.f_locals
                if pointer.tb_frame.f_code.co_name == "_validate_sample":
                    bound, expected = local.get("bound"), local.get("expected")
                    if bound is not None and type(expected) is int:
                        record.update(
                            metric=bound.metric,
                            expected=expected,
                            observed=local["metrics"][bound.metric],
                        )
                pointer = pointer.tb_next
            self.failures.append(record)
            raise EvidenceFailure from error

    def invoke(self, function, action, args=(), kwargs=None):
        if not self.active:
            return action()
        if function is EDGE:
            self.edge_kinds.append(kwargs["kind"])
        if function is DROP and sys.exception() is not None:
            error = sys.exception()
            self.failures.append(
                {
                    "error_class": type(error).__name__,
                    "locations": [
                        {"file": Path(row.filename).name, "line": row.lineno, "function": row.name}
                        for row in traceback.extract_tb(error.__traceback__)
                    ],
                }
            )
        if function is DROP and args[2].session is not None:
            self.finish_work(args[2].session._store.db)
        assert not self.owner._lock.locked()
        dimensions = self.dimensions(function, args, kwargs or {})
        binding = self.bindings[function]
        before = self.full.copy(), self.application.copy()
        changes = sum(db.total_changes for db in self.native)
        lease = self.lease_values.copy()
        path_probes = self.own_path_probes
        node = {
            "declaration": binding.declaration.id,
            "declaration_sha256": self.digests[binding.declaration.id],
            "dimensions": dimensions,
            "delegates": {tag: [] for tag, _ in binding.delegates},
        }
        self.stack.append((function, node))
        error, result = None, None
        try:
            with ExitStack() as contexts:
                if function not in {ROOT, SWEEP, OPEN, DROP}:
                    for db in self.dbs:
                        if not db.closed:
                            contexts.enter_context(db.pause())
                result = action()
            return result
        except BaseException as caught:
            error = caught
            raise
        finally:
            self.stack.pop()
            if error is not None:
                raise error
            if function is ROOT:
                for db in tuple(self.work_contexts):
                    self.finish_work(db)
            index = int(function in self.application_functions)
            now = (self.full, self.application)[index]
            values = {name: value - before[index][name] for name, value in now.items()}
            values["changed_rows"] = (
                values["changed_rows"] + sum(db.total_changes for db in self.native) - changes
            )
            values["connection_statements"] = values["driver_statements"] - values["sql_statements"]
            values["source_rows"] = values["payload_rows"]
            values["loaded_segment_octets"] = values["loaded_payload_octets"]
            values["returned_octets"] = (
                octets(result) if isinstance(result, bytes) else values["returned_octets"]
            )
            if function is fences.PROBE:
                values["binding_octets"] = octets(None if result is None else result[1])
            elif function is fences.CURRENT:
                values["binding_octets"] = values["returned_metadata_octets"]
            elif function is RESERVE:
                values["returned_slots"] = int(result is not None)
            elif function is OPEN:
                for name, value in self.lease_values.items():
                    values["lease_" + name] = value - lease.get(name, 0)
                values["lease_creation_callbacks"] = len(node["delegates"]["creation"])
                values["own_path_probes"] = self.own_path_probes - path_probes
            elif function is DROP:
                values["native_close_classes"] = values["native_close_calls"]
                values["closed_files"] -= values["lease_descriptor_closes"]
            for tag, records in node["delegates"].items():
                child = dict(binding.delegates)[tag]
                if function in {ROOT, SWEEP, DROP, OPEN}:
                    dimensions[tag + "_calls"] = len(records)
                    for item in growth_binding(child).declaration.dimensions:
                        if tag + "_" + item.name in {
                            item.name for item in binding.declaration.dimensions
                        }:
                            dimensions[tag + "_" + item.name] = max(
                                (record["dimensions"][item.name] for record in records), default=0
                            )
                values[tag + "_calls"] = values[tag + "_calls_limit"] = len(records)
                for bound in growth_binding(child).declaration.bounds:
                    values[tag + "_" + bound.metric] = sum(
                        record["metrics"][bound.metric] for record in records
                    )
            if function is ROOT:
                values["source_read_calls_limit"] = len(node["delegates"]["window"]) + len(
                    node["delegates"]["exact"]
                )
                values["returned_octets"] = sum(
                    record["metrics"]["returned_octets"]
                    for tag in ("window", "exact")
                    for record in node["delegates"][tag]
                )
            node["metrics"] = {
                bound.metric: values[bound.metric] for bound in binding.declaration.bounds
            }
            self.validate(function, node)
            self.attach(function, node)

    def native_work(self, operation, session, *args):
        db = session._store.db
        with self.paused(), pytest.MonkeyPatch.context() as patch:
            child_tests = importlib.import_module("test_entry_replay_child_growth")
            semantic_dimensions = child_tests.semantic_child_dimensions

            def certified_dimensions(current, row_budget):
                certificates = current.certificates
                if certificates is None:
                    return semantic_dimensions(current, row_budget)
                assert current.load().phase == "walk"
                value = current.atom.nodes.values[0]
                source_key, target_key = (
                    ("used_state", "generated_state")
                    if current.kind == "derivation"
                    else ("specific", "general")
                )
                proof = certificates.edge(
                    frame_ordinal=int(current.source.sequence),
                    frame_sha256=current.source.sha256,
                    kind=current.kind,
                    source=value[source_key]["object_id"],
                    target=value[target_key]["object_id"],
                    byte_budget=self.budget.octets,
                )
                assert proof is not None
                with pytest.MonkeyPatch.context() as without_provider:
                    without_provider.setattr(current, "certificates", None)
                    dimensions = semantic_dimensions(current, row_budget)
                dimensions["edge_admission_calls"] = 0
                return dimensions

            patch.setattr(child_tests, "semantic_child_dimensions", certified_dimensions)
            for name, original in (
                ("progress", PROGRESS),
                ("advance", ADVANCE),
                ("result", RESULT),
            ):
                patch.setattr(owner.JournalValidationSession, name, original)
            if operation is ADVANCE and self.recording_error is not None:
                dimensions, _ = self.native_tests.rejected_recording_dimensions(session, args[0])
            elif operation is ADVANCE and self.native_capacity is not None:
                dimensions, _ = self.native_tests.rejected_input_dimensions(
                    session, args[0], self.native_capacity
                )
            else:
                dimensions, _ = self.native_tests.session_dimensions(
                    session, args[0] if operation is ADVANCE else self.budget, operation, cold=False
                )
        rejected_original, rejected_input = None, None
        if operation is ADVANCE and self.recording_error is not None:
            with self.paused():
                rejected_original = self.native_tests.snapshot(db)
                rejected_input = session._input.state
        before, changes, start = db.metrics.copy(), db.total_changes, len(db.statements)
        self.in_work = True
        rejected = None
        try:
            with self.native_tests.session_observer(
                session,
                dimensions,
                operation,
                failed_operations=(EntryReplay.advance, JournalStore._check_entry_recording)
                if self.recording_error is not None
                else (),
            ) as (
                frame,
                complete,
            ):
                try:
                    result = operation(session, *args)
                except owner.JournalValidationCapacity as error:
                    assert operation is ADVANCE and self.native_capacity is not None
                    rejected, result = error, None
                except owner.ProvenanceValidationError as error:
                    assert operation is ADVANCE and self.recording_error is not None
                    assert error.args == (self.recording_error,)
                    rejected, result = error, None
                    self.native_error = error
        finally:
            self.in_work = False
        if operation is ADVANCE and (
            self.native_capacity is not None or self.recording_error is not None
        ):
            assert rejected is not None
        if operation is ADVANCE and self.recording_error is not None:
            assert session._failure is rejected and not session._closed
            with self.paused():
                assert self.native_tests.snapshot(db) == rejected_original
                assert session._input.state is rejected_input
            self.rejected_snapshot_preserved = True
        values = self.native_tests.observed_work(db, before, changes, start)
        values.update(
            indexed_containers=db.all_node_containers,
            index_python_opcode_events=db.all_node_opcodes,
        )
        try:
            sample = complete(frame, values)
        except ValueError as error:
            raise EvidenceFailure from error
        self.validate(operation, sample)
        self.attach(operation, sample)
        aliases = {
            "driver_statements": "sql_statements",
            "json_decoded_octets": "decoded_json_octets",
            "json_encoded_octets": "encoded_json_octets",
            "hashed_octets": "native_hashed_octets",
        }
        for metric in self.full:
            self.full[metric] += values.get(aliases.get(metric, metric), 0)
        for name, value in values.items():
            self.native_child_costs[db][name] += value
        if rejected is not None:
            raise rejected
        return result

    def start_work(self, session, owned):
        self.case.session = session
        db = session._store.db
        self.dbs.append(db)
        db.parent_recorder = self
        session._source = session._input.source = self.native_tests.SourcePorts(
            ConcreteSource(owned, self.case), db
        )
        storage = session._store._storage
        if id(storage) not in self.wrapped_storages:
            callback = storage.on_close

            def release_callback():
                self.add("release_callbacks")
                self.add("lease_release_callbacks")
                active, self.callback_active = self.callback_active, True
                try:
                    return callback()
                finally:
                    self.callback_active = active

            storage.on_close = release_callback
            self.wrapped_storages.add(id(storage))
        context = db.observe()
        context.__enter__()
        self.work_changes[db] = db.total_changes
        self.work_contexts[db] = context

    def finish_work(self, db):
        context = self.work_contexts.pop(db, None)
        if context is not None:
            totals = self.native_tests.observed_work(
                db, dict.fromkeys(db.metrics, 0), self.work_changes[db], 0
            )
            aliases = {
                "driver_statements": "sql_statements",
                "json_decoded_octets": "decoded_json_octets",
                "json_encoded_octets": "encoded_json_octets",
                "hashed_octets": "native_hashed_octets",
            }
            for metric in self.full:
                name = aliases.get(metric, metric)
                if name in totals:
                    outside = totals[name] - self.native_child_costs[db][name]
                    assert outside >= 0
                    self.full[metric] += outside
            context.__exit__(None, None, None)

    def native_call(self, operation, args, kwargs):
        assert operation is opening.NATIVE
        result = super().native_call(operation, args, kwargs)
        sample = self.records[operation].pop()
        self.attach(operation, sample)
        aliases = {
            "driver_statements": "sql_statements",
            "json_decoded_octets": "decoded_json_octets",
            "json_encoded_octets": "encoded_json_octets",
            "hashed_octets": "native_hashed_octets",
        }
        for metric in self.full:
            self.full[metric] += sample["metrics"].get(aliases.get(metric, metric), 0)
        self.start_work(result, args[2])
        return result

    def native_close(self, session):
        db = session._store.db
        self.finish_work(db)
        before = self.full.copy()
        result = CLOSE(session)
        values = {name: value - before[name] for name, value in self.full.items()}
        values["closed_files"] -= values["lease_descriptor_closes"]
        dimensions = release.native_dimensions(bool(values["release_callbacks"]))
        sample = {
            "declaration": growth_binding(CLOSE).declaration.id,
            "declaration_sha256": self.digests[growth_binding(CLOSE).declaration.id],
            "dimensions": dimensions,
            "metrics": {
                bound.metric: values[bound.metric]
                for bound in growth_binding(CLOSE).declaration.bounds
            },
            "delegates": {},
        }
        self.validate(CLOSE, sample)
        parent, node = self.stack[-1]
        assert parent is DROP
        node["delegates"]["release" if values["release_callbacks"] else "retain"].append(sample)
        self.add("native_close_calls")
        return result

    def measure(self, current, *, cold, budget, native_capacity=None, recording_error=None):
        self.native_capacity = native_capacity
        self.recording_error = recording_error
        self.native_error = None
        self.error_format_calls = 0
        self.rejected_snapshot_preserved = False
        self.owner, self.budget = current, budget
        self.dbs.clear()
        self.native_child_costs.clear()
        self.work_changes.clear()
        self.records.clear()
        self.native.clear()
        self.cursors.clear()
        self.statements.clear()
        self.lease_values.clear()
        self.own_path_probes = 0
        self.full = dict.fromkeys(self.full, 0)
        self.application = dict.fromkeys(self.application, 0)

        def unwrap(lock):
            while isinstance(lock, release.Lock):
                lock = lock.actual
            return lock

        current._lock = Lock(unwrap(current._lock), self, pool=True)
        selected = Pool(self)
        selected.update(current._slots)
        current._slots = selected
        for slot in selected.values():
            slot.lock = Lock(unwrap(slot.lock), self)
        error, result = None, None
        with ExitStack() as resources:
            for slot in selected.values():
                if slot.session is not None:
                    session = slot.session
                    self.start_work(session, slot.source)
            resources.callback(lambda: [self.finish_work(db) for db in tuple(self.work_contexts)])
            with self.kernel(), self.controls(), self.observe(self.case.engine, cold=cold):
                with pytest.MonkeyPatch.context() as patch:
                    for name, operation in (
                        ("progress", PROGRESS),
                        ("advance", ADVANCE),
                        ("result", RESULT),
                    ):
                        patch.setattr(
                            owner.JournalValidationSession,
                            name,
                            lambda session, *args, _operation=operation: self.native_work(
                                _operation, session, *args
                            ),
                        )
                    patch.setattr(
                        owner.JournalValidationSession,
                        "open_retained",
                        classmethod(
                            lambda *args, **kwargs: self.native_call(opening.NATIVE, args, kwargs)
                        ),
                    )
                    patch.setattr(
                        owner.JournalValidationSession,
                        "close",
                        lambda session: self.native_close(session),
                    )
                    actual_slot = owner._Slot

                    def slot():
                        self.add("allocated_slots")
                        value = actual_slot()
                        value.lock = Lock(value.lock, self)
                        return value

                    patch.setattr(owner, "_Slot", slot)
                    try:
                        result = self.invoke(
                            ROOT,
                            lambda: ROOT(
                                current, self.case.work, catalog=self.case.catalog, budget=budget
                            ),
                            (current, self.case.work),
                        )
                    except BaseException as caught:
                        error = caught
        if error is not None:
            raise error
        return result, self.records[ROOT][-1]


def populate(case):
    with case.factory() as session:
        chunks = session.execute(
            select(Chunk.byte_offset, Chunk.content).order_by(Chunk.ordinal)
        ).all()
    case.raw = b"".join(row.content for row in chunks)
    case.spans = tuple((row.byte_offset, row.byte_offset + len(row.content)) for row in chunks)
    case.binding_json = case.source_binding
    case.journals = 1


def finite_cases(witness, root, *, large):
    owner = witness.owner
    samples = []
    with witness.opening.build_cases(root, large=large) as cases:
        for case in cases:
            if case.malformed:
                continue
            populate(case)
            lifecycle, native = witness.opening.native_helpers()
            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(
                    lifecycle, "LifecycleConnection", witness.mixed_connection(native, lifecycle)
                )
                current = owner.UploadJournalValidationOwner(
                    case.factory, case.path / "parent-native"
                )
                key = case.work.collection_id, case.work.journal_id, case.work.attempt_id
                try:
                    recorder = witness.Recorder(case)
                    result, sample = recorder.measure(
                        current, cold=True, budget=owner.JournalValidationBudget(source_octets=7)
                    )
                    samples.append(sample)
                    assert result.progressed and result.outcome is None
                    slot = current._slots[key]
                    session = slot.session
                    assert (
                        session.progress().source_octets
                        == owner.jobs.decode_progress(slot.job.progress_json).source_octets
                        == 7
                    )
                    assert slot.source.source._page is not None
                    # Drive actual fixture work outside the chosen measured units.
                    for _ in range(10000):
                        if session._input.pending:
                            break
                        assert not session.advance().complete
                    else:
                        raise AssertionError("finite source did not expose a selected atom")
                    result, sample = recorder.measure(
                        current, cold=False, budget=owner.JournalValidationBudget()
                    )
                    samples.append(sample)
                    assert result.progressed and result.outcome is None
                    assert sample["metrics"]["exact_calls"] == 1
                    for _ in range(10000):
                        if session._ledger.load().checkpoint.phase == "terminal":
                            break
                        assert not session.advance().complete
                    else:
                        raise AssertionError("finite source did not reach terminal phase")
                    expected = session._store.frame_reference(-1)
                    database = session._store.db
                    result, sample = recorder.measure(
                        current, cold=False, budget=owner.JournalValidationBudget()
                    )
                    samples.append(sample)
                    assert (
                        result.progressed
                        and result.outcome is not None
                        and result.outcome.failure is None
                    )
                    assert result.outcome.terminal_entry_id == expected["entry_id"]
                    assert result.outcome.terminal_sequence == int(expected["sequence"])
                    assert result.outcome.terminal_json_sha256 == expected["json_sha256"]
                    assert not current._slots and session._closed and database.closed
                    assert sample["metrics"]["result_calls"] == sample["metrics"]["drop_calls"] == 1
                    assert (
                        sample["metrics"]["historical_payload_octets"]
                        == sample["metrics"]["source_payload_octets"]
                        == 0
                    )
                    assert sample["metrics"]["closed_databases"] == 1
                    assert sample["metrics"]["closed_files"] == 4
                    result, sample = recorder.measure(
                        current, cold=False, budget=owner.JournalValidationBudget()
                    )
                    samples.append(sample)
                    assert (
                        not result.progressed
                        and result.outcome is not None
                        and result.outcome.failure is None
                    )
                    assert not current._slots and sample["metrics"]["native_calls"] == 0
                finally:
                    current.close()
                for capacity, busy in ((1, True), (2, True), (1, False), (2, False)):
                    current = owner.UploadJournalValidationOwner(
                        case.factory, case.path / f"pool-{capacity}", concurrency=capacity
                    )
                    held = []
                    try:
                        for index in range(capacity):
                            selected = (
                                key if busy and index == 0 else (key[0] + index + 1, key[1], key[2])
                            )
                            held.append(current._reserve(selected))
                        result, sample = witness.Recorder(case).measure(
                            current, cold=True, budget=owner.JournalValidationBudget()
                        )
                        samples.append(sample)
                        assert not result.progressed and result.outcome is None
                        assert (
                            sample["metrics"]["native_calls"]
                            == sample["metrics"]["open_calls"]
                            == 0
                        )
                    finally:
                        for slot in held:
                            slot.lock.release()
                        current.close()
    return samples


def graph_case(witness, root):
    from copy import deepcopy

    from riverhog_provenance import (
        BoundedSourceObserver,
        BytesSource,
        assertion,
        create_journal,
        new_id,
        reference,
    )
    from riverhog_provenance import _generic_semantics as semantics

    from tests.unit.test_journal_capture import retained
    from tests.unit.test_staged_journal_range import JOURNAL
    from tests.unit.test_upload_journal_certificate_edge_growth import ready

    owner = witness.owner
    samples = []
    observed = BoundedSourceObserver().observe(BytesSource(b"owner certificate input"))
    graph = observed.graph_fragment()
    first = graph["states"][0]
    second = deepcopy(first)
    second["id"], second["assertion_id"] = new_id(), new_id()
    graph["states"].append(second)
    graph.setdefault("relations", []).extend(
        (
            assertion(
                "derivation",
                observed.observer_agent_id,
                used_state=reference(first["id"], "state"),
                generated_state=reference(second["id"], "state"),
                kind="copy",
            ),
            assertion(
                "specialization",
                observed.observer_agent_id,
                specific=reference(first["id"], "state"),
                general=reference(second["id"], "state"),
            ),
        )
    )
    raw = create_journal(graph, recorded_by_agent_id=observed.observer_agent_id, journal_id=JOURNAL)
    with retained(root, raw) as (engine, factory):
        factory.configure(autoflush=False, expire_on_commit=False)
        work, provider = ready(factory)
        case = SimpleNamespace(
            engine=engine,
            factory=factory,
            work=work,
            catalog=owner.source_owner.admission_provenance_catalog(),
            mode=provider._mode,
            source_binding=provider.binding.source.to_json(),
            certificate_binding=provider.binding.to_json(),
        )
        populate(case)
        lifecycle, native = witness.opening.native_helpers()
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(
                lifecycle, "LifecycleConnection", witness.mixed_connection(native, lifecycle)
            )
            current = owner.UploadJournalValidationOwner(factory, root / "parent-native")
            recorder = witness.Recorder(case)
            try:
                result, sample = recorder.measure(
                    current, cold=True, budget=owner.JournalValidationBudget()
                )
                samples.append(sample)
                session = next(iter(current._slots.values())).session
                for _ in range(10000):
                    cached = session._replay._current
                    child = None if cached is None else getattr(cached, "child", None)
                    selected = False
                    generic = session._ledger.load().checkpoint.phase == "row" and child is not None
                    if generic:
                        state = child.load()
                        frame = (
                            semantics._Stack(child, state, semantics._Meter(65536)).peek()
                            if state.phase == "walk"
                            else None
                        )
                        selected = (
                            frame is not None
                            and frame.operation == "row"
                            and frame.stage == 2
                            and child.atom.nodes.values[frame.node]["type"]
                            in {"derivation", "specialization"}
                        )
                    if selected:
                        result, sample = recorder.measure(
                            current, cold=False, budget=owner.JournalValidationBudget(rows=1)
                        )
                        samples.append(sample)
                        if set(recorder.edge_kinds) == {"derivation", "specialization"}:
                            break
                    else:
                        assert not session.advance(
                            owner.JournalValidationBudget(rows=1)
                            if generic
                            else owner.JournalValidationBudget()
                        ).complete
                else:
                    raise AssertionError(
                        "finite graph did not request its retained edge certificate"
                    )
                assert sample["metrics"]["certificate_calls"] == 1
                assert set(recorder.edge_kinds) == {"derivation", "specialization"}
            finally:
                current.close()
    return samples


@pytest.mark.growth_witness(
    "riverhog-validation-owner-advance-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_owner.UploadJournalValidationOwner.advance",
)
def test_actual_owner_composes_opening_execution_publication_and_custody(tmp_path, record_property):
    samples = finite_cases(sys.modules[__name__], tmp_path / "owner-advance", large=True)
    samples.extend(graph_case(sys.modules[__name__], tmp_path / "owner-graph"))
    record_property("growth_samples", samples)


def owner_capacity_cases(witness, root, *, large):

    owner = witness.owner
    samples = []
    with witness.opening.build_cases(root, large=large) as cases:
        for case in cases:
            if case.malformed:
                continue
            witness.populate(case)
            lifecycle, native = witness.opening.native_helpers()
            with witness.pytest.MonkeyPatch.context() as patch:
                patch.setattr(
                    lifecycle, "LifecycleConnection", witness.mixed_connection(native, lifecycle)
                )
                current = owner.UploadJournalValidationOwner(
                    case.factory, case.path / "capacity-native"
                )
                recorder = witness.Recorder(case)
                key = case.work.collection_id, case.work.journal_id, case.work.attempt_id
                try:
                    result, sample = recorder.measure(
                        current, cold=True, budget=owner.JournalValidationBudget(source_octets=7)
                    )
                    samples.append(sample)
                    assert result.progressed and result.outcome is None
                    slot = current._slots[key]
                    session = slot.session
                    pending = None
                    for kind in ("metadata", "atom"):
                        if kind == "atom":
                            if len(case.raw) < 65536:
                                continue
                            for _ in range(10000):
                                if session._input.pending:
                                    break
                                assert not session.advance().complete
                            else:
                                raise AssertionError(
                                    "finite retained source did not frame its atom"
                                )
                            pending = session._input.state.pending
                            budget = owner.JournalValidationBudget(
                                octets=2 * pending.json_bytes + 65535
                            )
                        else:
                            budget = owner.JournalValidationBudget(
                                octets=16384, source_octets=16384
                            )
                        before = native.snapshot(session._store.db)
                        input_state = session._input.state
                        job = witness.opening.job_tests.selected(case.factory)
                        for _ in range(2):
                            result, sample = recorder.measure(
                                current, cold=False, budget=budget, native_capacity=kind
                            )
                            samples.append(sample)
                            assert not result.progressed and result.outcome is None
                            assert current._slots[key] is slot and slot.session is session
                            assert session._failure is None and not session._closed
                            assert session._input.state is input_state
                            assert native.snapshot(session._store.db) == before
                            assert witness.opening.job_tests.selected(case.factory) == job
                            assert sample["metrics"]["native_calls"] == 1
                            assert (
                                sample["metrics"]["drop_calls"]
                                == sample["metrics"]["publish_calls"]
                                == 0
                            )
                            assert (
                                sample["metrics"]["source_payload_octets"]
                                == sample["metrics"]["loaded_payload_octets"]
                                == 0
                            )
                            rejected = sample["delegates"]["native"][0]
                            assert (
                                rejected["metrics"]["changed_rows"]
                                == rejected["metrics"]["source_port_octets"]
                                == 0
                            )
                            assert (
                                rejected["metrics"]["source_atoms"]
                                == rejected["metrics"]["source_windows"]
                                == 0
                            )
                            assert all(
                                not records
                                for tag, records in rejected["delegates"].items()
                                if tag != "load"
                            )
                            assert len(rejected["delegates"]["load"]) == 1
                        if kind == "atom":
                            assert session._input.state.pending is pending
                            result, sample = recorder.measure(
                                current, cold=False, budget=owner.JournalValidationBudget()
                            )
                            samples.append(sample)
                            assert result.progressed and result.outcome is None
                            assert sample["metrics"]["exact_calls"] == 1
                            assert slot.session is session and session._failure is None
                finally:
                    current.close()
    return samples


@pytest.mark.growth_witness(
    "riverhog-validation-owner-input-capacity-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_owner.UploadJournalValidationOwner.advance",
)
def test_actual_owner_retains_unchanged_native_input_after_capacity(tmp_path, record_property):
    samples = owner_capacity_cases(
        sys.modules[__name__], tmp_path / "owner-input-capacity", large=True
    )
    record_property("growth_samples", samples)


def recording_cases(witness, root, *, large):
    witness.opening.native_helpers()
    from test_journal_session_recording_rejection_growth import recording_source

    from tests.unit.test_journal_capture import retained
    from tests.unit.test_staged_journal_range import JOURNAL
    from tests.unit.test_upload_journal_certificate_edge_growth import ready

    owner = witness.owner
    samples = []
    catalog = owner.source_owner.admission_provenance_catalog()
    for payload in (0, 65537, 1024 * 1024) if large else (0,):
        for kind, message in (
            ("recorder", "entry recorder must resolve to an effective agent"),
            ("context", "recording context does not resolve"),
        ):
            path = root / f"{payload}-{kind}"
            raw = recording_source(kind, payload, catalog, journal_id=JOURNAL)
            with retained(path, raw) as (engine, factory):
                factory.configure(autoflush=False, expire_on_commit=False)
                work, provider = ready(factory)
                case = witness.SimpleNamespace(
                    engine=engine,
                    factory=factory,
                    path=path,
                    work=work,
                    catalog=catalog,
                    mode=provider._mode,
                    source_binding=provider.binding.source.to_json(),
                    certificate_binding=provider.binding.to_json(),
                )
                witness.populate(case)
                lifecycle, native = witness.opening.native_helpers()
                with witness.pytest.MonkeyPatch.context() as patch:
                    patch.setattr(
                        lifecycle,
                        "LifecycleConnection",
                        witness.mixed_connection(native, lifecycle),
                    )
                    current = owner.UploadJournalValidationOwner(factory, path / "native")
                    recorder = witness.Recorder(case)
                    try:
                        result, sample = recorder.measure(
                            current,
                            cold=True,
                            budget=owner.JournalValidationBudget(source_octets=7),
                        )
                        samples.append(sample)
                        assert result.progressed and result.outcome is None
                        slot = next(iter(current._slots.values()))
                        session = slot.session
                        for _ in range(10000):
                            before = session.progress()
                            if before.phase == "recording" and before.accepted_frames == 1:
                                break
                            assert not session.advance(
                                owner.JournalValidationBudget(rows=1)
                            ).complete
                        else:
                            raise AssertionError(
                                "finite source did not reach its rejected recording predicate"
                            )
                        result, sample = recorder.measure(
                            current,
                            cold=False,
                            budget=owner.JournalValidationBudget(),
                            recording_error=message,
                        )
                        samples.append(sample)
                        assert result.progressed and result.outcome.failure == message
                        assert (
                            recorder.rejected_snapshot_preserved
                            and recorder.error_format_calls == 1
                        )
                        assert (
                            sample["metrics"]["error_formatted_octets"]
                            == sample["dimensions"]["error_control_octets"]
                            == witness.octets(message)
                        )
                        assert (
                            sample["metrics"]["native_calls"]
                            == sample["metrics"]["publish_calls"]
                            == sample["metrics"]["drop_calls"]
                            == 1
                        )
                        assert (
                            sample["metrics"]["result_calls"]
                            == sample["metrics"]["source_payload_octets"]
                            == sample["metrics"]["loaded_payload_octets"]
                            == 0
                        )
                        assert (
                            sample["metrics"]["closed_files"] == 4
                            and sample["metrics"]["closed_databases"] == 1
                        )
                        assert not current._slots and session._closed
                        job = witness.opening.job_tests.selected(factory)
                        assert job["state"] == "failed" and job["failure"] == message
                        assert owner.jobs.decode_progress(job["progress_json"]) == before
                        result2, cached = recorder.measure(
                            current, cold=False, budget=owner.JournalValidationBudget()
                        )
                        samples.append(cached)
                        assert not result2.progressed and result2.outcome == result.outcome
                        assert (
                            cached["metrics"]["native_calls"]
                            == cached["metrics"]["error_formatted_octets"]
                            == 0
                        )
                    finally:
                        current.close()
    return samples


@pytest.mark.growth_witness(
    "riverhog-validation-owner-recording-rejection-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_owner.UploadJournalValidationOwner.advance",
)
def test_actual_owner_publishes_failed_recording_at_last_accepted_prefix(tmp_path, record_property):
    samples = recording_cases(
        sys.modules[__name__], tmp_path / "owner-recording-rejection", large=True
    )
    record_property("growth_samples", samples)
