"""Thread-attributed observations of the maintained Native retirement owners.

Test barriers postpone worker entry, not the scheduling return. Observer-only
snapshots and explicit test joins are outside operation scopes. The original
worker, context, actor and ordinary reservation remain the implementation's.
"""

from __future__ import annotations

import builtins
import hashlib
import inspect
import os
import threading
import traceback
from collections import OrderedDict, defaultdict
from contextlib import contextmanager
from dataclasses import asdict
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.services import catalog_retirement
from riverhog_core.services import journal_retirement_registry as journal
from riverhog_core.services import physical_execution as physical
from riverhog_core.services import upload_journal_validation_retirement as owner
from riverhog_core.services import workspace_retirement_registry as registry
from riverhog_core.services.archive_construction_retention import ArchiveConstructionRetention
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples

from tests.unit import test_upload_journal_validation_retirement_catalog_growth as catalog
from tests.unit import test_upload_journal_validation_retirement_growth as units
from tests.unit.test_physical_workspace_execution_growth import observed as physical_observed
from tests.unit.test_physical_workspace_execution_growth import width
from tests.unit.test_workspace_construction_retention_growth import measured as retention_observed

ROOT = owner.UploadJournalValidationRetirement.advance
WORKER = journal.JournalRetirementRegistry._run
TURN = journal.JournalRetirementTurn
REG = journal.JournalRetirementRegistry
RESULT = physical._PhysicalExecutionResult
PHYSICAL = physical.PhysicalExecutionOwner
RETENTION = ArchiveConstructionRetention


def sample(operation, inputs, own, children):
    binding = growth_binding(operation)
    dimensions = dict(inputs)
    metrics = dict(own)
    for tag, child in binding.delegates:
        rows = children[tag]
        cb = growth_binding(child)
        dimensions[tag + "_calls"] = len(rows)
        metrics[tag + "_calls"] = metrics[tag + "_calls_limit"] = len(rows)
        for item in cb.declaration.dimensions:
            dimensions[tag + "_" + item.name] = max(
                (row["dimensions"][item.name] for row in rows), default=0
            )
        for bound in cb.declaration.bounds:
            metrics[tag + "_" + bound.metric] = sum(row["metrics"][bound.metric] for row in rows)
    dimensions = {
        item.name: dimensions.get(item.name, 0) for item in binding.declaration.dimensions
    }
    metrics = {bound.metric: metrics.get(bound.metric, 0) for bound in binding.declaration.bounds}
    projected = project_bindings((binding,))
    row = {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical_json_bytes(projected["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": dimensions,
        "metrics": metrics,
        "delegates": children,
    }
    validate_execution_samples(projected, binding.declaration.id, [row])
    return row


def exception_shape(outcome):
    initial = outcome._future.exception()
    pending = [initial] if initial is not None else []
    seen, edges, frames = set(), 0, 0
    while pending:
        error = pending.pop()
        if id(error) in seen:
            continue
        seen.add(id(error))
        tb = error.__traceback__
        while tb is not None:
            frames += 1
            tb = tb.tb_next
        for child in (error.__cause__, error.__context__):
            if child is not None:
                edges += 1
                pending.append(child)
        if isinstance(error, BaseExceptionGroup):
            edges += len(error.exceptions)
            pending.extend(error.exceptions)
    return len(seen), edges, frames


class Observation:
    def __init__(self, case, original, *, cold=False, expected_error=None, fault=None):
        self.case, self.original = case, original
        self.cold, self.expected_error, self.fault = cold, expected_error, fault
        self.main = threading.get_ident()
        self.local = threading.local()
        self.schedules, self.workers, self.links, self.exits = [], [], [], []
        self.gates = []
        self.gate_lock = threading.Condition()
        self.originals = {}
        self.infrastructure = False
        self.allow_failure = False
        lines, first = inspect.getsourcelines(ROOT)
        self.candidate_line = first + next(
            i for i, line in enumerate(lines) if "for _candidate in" in line
        )

    @property
    def stack(self):
        if not hasattr(self.local, "stack"):
            self.local.stack = []
        return self.local.stack

    def root(self):
        return self.stack[0] if self.stack and self.stack[0].operation is ROOT else None

    def _invoke(self, operation, function, args, kwargs):
        parent = self.stack[-1] if self.stack else None
        frame = SimpleNamespace(
            operation=operation,
            own=defaultdict(int),
            inputs={},
            children={tag: [] for tag, _ in growth_binding(operation).delegates},
            phase=None,
            row=None,
        )
        frame.inputs["other_workspaces"] = dict.__len__(self.original.registry._contexts)
        self.stack.append(frame)
        result, failure = None, None
        try:
            result = self._measure(frame, function, args, kwargs)
            return result
        except BaseException as error:
            failure = type(error).__name__
            raise
        finally:
            self.stack.pop()
            if frame.row is None:
                frame.row = sample(operation, frame.inputs, frame.own, frame.children)
            if parent is not None:
                matches = [
                    tag
                    for tag, child in growth_binding(parent.operation).delegates
                    if child is operation
                ]
                assert len(matches) == 1, (parent.operation, operation)
                parent.children[matches[0]].append(frame.row)
            elif operation is ROOT:
                self.schedules.append(frame.row)
            elif operation is WORKER:
                self.workers.append(frame.row)
            self.exits.append((growth_binding(operation).declaration.id, failure))

    def _measure(self, frame, fn, args, kwargs):
        op = frame.operation
        case, current = self.case, self.original.cleanup
        root = self.root()
        if op is ROOT:
            frame.inputs.update(
                unit_limit=max(0, kwargs.get("limit", 1)),
                octet_budget=kwargs.get("octet_budget", 4 * 1024**2),
            )
            result = fn(*args, **kwargs)
            frame.own["scheduling_events"] = result
            assert result == sum(
                frame.own[name]
                for name in (
                    "collected_outcomes",
                    "withdrawn_reservations",
                    "resumed_closes",
                    "new_submissions",
                )
            )
            return result
        if op is self.originals[owner.UploadJournalValidationRetirement, "_select"]:
            if root is not None:
                root.own["exclusion_references"] += len(args[1])
            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(type(current), "_select", op)
                result, frame.row = catalog.measure(
                    case, current, "_select", args[1], cold=self.cold
                )
            return result
        if op is self.originals[owner.UploadJournalValidationRetirement, "_blocked"]:
            job, failure = args[1:]
            persisted = catalog.read(case, job.workspace_id)
            changed = int(
                persisted == job
                and persisted.collection_id is None
                and persisted.failure != failure
            )
            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(type(current), "_blocked", op)
                result, frame.row = catalog.measure(
                    case,
                    current,
                    "_blocked",
                    job,
                    proposed=failure,
                    cold=self.cold,
                    changed=changed,
                )
            return result
        if op is self.originals[owner.UploadJournalValidationRetirement, "_advance"]:
            errors = []
            result, frame.row = units.measure(
                case,
                current,
                args[1],
                args[2],
                turn=kwargs["turn"],
                cold=self.cold,
                error=self.expected_error,
                fault=self.fault,
                observed_error=errors,
            )
            if errors[0] is not None:
                raise errors[0]
            return result
        if op is self.originals[REG, "_scope"]:
            reg, job, budget = args
            controls = {
                **asdict(job),
                "root": str(reg._cleanup.root),
                "target_incarnation": "native-journal:" + str(reg._cleanup.root / job.workspace_id),
            }
            exact = {
                name: str(value) if type(value) is int else value
                for name, value in controls.items()
            }
            frame.inputs["control_octets"] = len(canonical_json_bytes(exact))

            def encode(value):
                result = canonical_json_bytes(value)
                frame.own["encode_calls"] += 1
                frame.own["encoded_octets"] += len(result)
                return result

            def digest(value):
                frame.own["hashed_octets"] += len(value)
                return hashlib.sha256(value)

            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(journal, "canonical_json_bytes", encode)
                patch.setattr(catalog_retirement, "canonical_json_bytes", encode)
                patch.setattr(journal, "hashlib", SimpleNamespace(sha256=digest))
                return fn(*args, **kwargs)
        if op is self.originals[REG, "_accept_progress"]:
            frame.own["result_checks"] = 1
            return fn(*args, **kwargs)
        if op is self.originals[REG, "_create"]:
            frame.own["actors_constructed"] = 1
            return fn(*args, **kwargs)
        if op is self.originals[TURN, "advance"]:
            result = fn(*args, **kwargs)
            assert type(result) is bool
            frame.own["progress_returns"] = int(result)
            return result
        if op is self.originals[TURN, "_receive"]:
            actor = args[0]
            callbacks = list(actor._handles._callbacks)
            frame.inputs["callbacks"] = frame.own["callbacks_limit"] = len(callbacks)
            resources = [actor._files._file, actor._files._directory]
            for operation, _ in callbacks:
                receiver = getattr(operation, "__self__", None)
                if hasattr(receiver, "owner"):
                    resources.append(receiver.owner)
            before = [resource.metrics for resource in resources]
            mapped = {}
            for operation, _arguments in callbacks:

                def observed(*values, _operation=operation):
                    frame.own["callback_invocations"] += 1
                    return _operation(*values)

                mapped[observed] = operation
            actor._handles._callbacks[:] = [
                (wrapper, arguments)
                for wrapper, (_, arguments) in zip(mapped, callbacks, strict=True)
            ]
            try:
                result = fn(*args, **kwargs)
                frame.own["positive_receive"] = int(actor.received)
                return result
            finally:
                actor._handles._callbacks[:] = [
                    (mapped.get(operation, operation), values)
                    for operation, values in actor._handles._callbacks
                ]
                for resource, beginning in zip(resources, before, strict=True):
                    after = resource.metrics
                    for source, metric in (
                        ("close_calls", "native_close_calls"),
                        ("open_calls", "native_open_calls"),
                        ("fstat_calls", "native_stat_calls"),
                        ("dup_calls", "native_dup_calls"),
                    ):
                        frame.own[metric] += after[source] - beginning[source]
        if op is self.originals[TURN, "close"]:
            return fn(*args, **kwargs)
        if op is WORKER:
            frame.own["context_entries"] = 1
            return fn(*args, **kwargs)
        if op in (
            self.originals[PHYSICAL, "try_submit_workspace"],
            self.originals[PHYSICAL, "collect_workspace"],
        ):
            pool = args[0]
            collect = op is self.originals[PHYSICAL, "collect_workspace"]
            slots = len(pool._slots)
            target = kwargs.get("target_incarnation", "")
            control = (
                width((args[1], target, id(kwargs.get("owner")), True))
                if not collect
                else max(
                    (
                        width((s.key, s.target_incarnation, id(s.owner), True))
                        for s in pool._slots.values()
                    ),
                    default=1,
                )
            )
            frame.inputs.update(
                retained_slots=slots,
                quantum=kwargs["limit"] if collect else 1,
                control_octets_per_step=control,
            )
            with physical_observed(pool, pytest.MonkeyPatch()) as metrics:
                result = fn(*args, **kwargs)
                metrics["results_received"] = len(result) if collect else 0
            frame.own.update(metrics)
            if root is not None:
                if collect:
                    root.own["collected_outcomes"] += len(result)
                elif result == "submitted":
                    root.own["resumed_closes" if root.phase == "resume" else "new_submissions"] += 1
            return result
        if op in (
            self.originals[RETENTION, "try_retain_workspace"],
            self.originals[RETENTION, "retire"],
        ):
            ledger = args[0]
            released = op is self.originals[RETENTION, "retire"]
            target = args[1].target_incarnation if released else kwargs["target_incarnation"]
            frame.inputs.update(
                retained_slots=len(ledger._reservations),
                control_octets_per_step=width((args[1], target, id(kwargs["owner"]), False)),
            )
            with retention_observed(ledger) as metrics:
                result = fn(*args, **kwargs)
            frame.own.update(metrics)
            if root is not None and released and result:
                root.own["withdrawn_reservations"] += 1
            return result
        if op is self.originals[PHYSICAL, "status"]:
            frame.own["status_calls"] = 1
            slot = args[0]._slots.get(args[1])
            with pytest.MonkeyPatch.context() as patch:
                if slot:
                    actual = slot.worker.is_alive

                    def alive():
                        frame.own["worker_liveness_calls"] += 1
                        return actual()

                    patch.setattr(slot.worker, "is_alive", alive)
                return fn(*args, **kwargs)
        if op is self.originals[RESULT, "release_exception_frames"]:
            nodes, edges, frames = exception_shape(args[0])
            frame.inputs.update(
                exception_nodes=nodes, exception_edges=edges, traceback_frames=frames
            )
            original_clear = traceback.clear_frames

            def clear(tb):
                frame.own["clear_frames_calls"] += 1
                current = tb
                while current is not None:
                    frame.own["traceback_frame_visits"] += 1
                    current = current.tb_next
                return original_clear(tb)

            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(traceback, "clear_frames", clear)
                result = fn(*args, **kwargs)
            # Every cause/context/group edge in the original finite graph is
            # processed by the maintained traversal; repeat edges are not lost.
            frame.own["graph_edges"] = edges
            return result
        raise AssertionError(op)

    @contextmanager
    def installed(self):
        operations = (
            (owner.UploadJournalValidationRetirement, "_select"),
            (owner.UploadJournalValidationRetirement, "_advance"),
            (owner.UploadJournalValidationRetirement, "_blocked"),
            (REG, "_scope"),
            (REG, "_accept_progress"),
            (REG, "_create"),
            (REG, "_run"),
            (TURN, "advance"),
            (TURN, "close"),
            (TURN, "_receive"),
            (PHYSICAL, "try_submit_workspace"),
            (PHYSICAL, "collect_workspace"),
            (PHYSICAL, "status"),
            (RETENTION, "try_retain_workspace"),
            (RETENTION, "retire"),
            (RESULT, "release_exception_frames"),
        )
        observer = self

        class Contexts(OrderedDict):
            def record(self, key=None):
                frame = observer.root()
                if frame is not None:
                    frame.own["context_mapping_steps"] += 1

            def __getitem__(self, key):
                self.record(key)
                return super().__getitem__(key)

            def __setitem__(self, key, value):
                self.record(key)
                return super().__setitem__(key, value)

            def __delitem__(self, key):
                self.record(key)
                return super().__delitem__(key)

            def __contains__(self, key):
                self.record(key)
                return super().__contains__(key)

            def __len__(self):
                self.record()
                return super().__len__()

            def get(self, key, default=None):
                self.record(key)
                return super().get(key, default)

            def popitem(self, *args, **kwargs):
                self.record()
                frame = observer.root()
                if frame is not None:
                    frame.own["context_rotations"] += 1
                return super().popitem(*args, **kwargs)

            def __iter__(self):
                assert observer.root() is None, "coordination enumerated original contexts"
                return super().__iter__()

            def items(self):
                assert observer.root() is None, "coordination enumerated original contexts"
                return super().items()

            def values(self):
                assert observer.root() is None, "coordination enumerated original contexts"
                return super().values()

        self.original.registry._contexts = Contexts(self.original.registry._contexts)
        with pytest.MonkeyPatch.context() as patch:
            for cls, name in operations:
                function = getattr(cls, name)
                self.originals[cls, name] = function

                def invoke(*args, _fn=function, **kwargs):
                    if _fn is WORKER:
                        gate = SimpleNamespace(
                            entered=threading.Event(),
                            release=threading.Event(),
                            context=args[1],
                            thread=threading.current_thread(),
                        )
                        with self.gate_lock:
                            self.gates.append(gate)
                            self.gate_lock.notify_all()
                        gate.entered.set()
                        assert gate.release.wait(60), "fixture did not release original worker"
                        assert threading.get_ident() != self.main
                    return self._invoke(_fn, _fn, args, kwargs)

                patch.setattr(cls, name, invoke)
            for name, phase in (("submit", "new"), ("resume_ready", "resume")):
                fn = getattr(registry.WorkspaceRetirementRegistry, name)

                def phased(*args, _fn=fn, _phase=phase, **kwargs):
                    root = self.root()
                    previous = root.phase if root else None
                    if root:
                        root.phase = _phase
                    try:
                        return _fn(*args, **kwargs)
                    finally:
                        if root:
                            root.phase = previous

                patch.setattr(registry.WorkspaceRetirementRegistry, name, phased)

            def ranges(*args):
                caller = inspect.currentframe().f_back
                root = self.root()
                if root is None or caller.f_code is not ROOT.__code__:
                    return builtins.range(*args)
                metric = (
                    "candidate_iterations"
                    if caller.f_lineno == self.candidate_line
                    else "outer_iterations"
                )

                def iterate():
                    for value in builtins.range(*args):
                        root.own[metric] += 1
                        yield value

                return iterate()

            patch.setattr(owner, "range", ranges, raising=False)

            def format_error(value):
                result = builtins.str(value)
                if self.stack and self.stack[-1].operation is self.originals[TURN, "advance"]:
                    frame = self.stack[-1]
                    frame.own["error_format_calls"] += 1
                    frame.own["error_formatted_octets"] += len(result.encode())
                    frame.inputs["error_octets"] = len(result.encode())
                return result

            patch.setattr(journal, "str", format_error, raising=False)
            actual_log = registry._LOG.warning

            def log(message, *args, **kwargs):
                frame = self.root()
                if frame:
                    size = sum(len(str(value).encode()) for value in (message, *args))
                    frame.own["log_calls"] += 1
                    frame.own["log_control_octets_total"] += size
                    frame.inputs["log_control_octets"] = max(
                        size, frame.inputs.get("log_control_octets", 0)
                    )
                return actual_log(message, *args, **kwargs)

            patch.setattr(registry._LOG, "warning", log)
            for name in (
                "open",
                "close",
                "fstat",
                "stat",
                "fsync",
                "ftruncate",
                "unlink",
                "rmdir",
                "read",
                "write",
                "listdir",
                "scandir",
            ):
                fn = getattr(os, name)

                def file_call(*args, _fn=fn, _name=name, **kwargs):
                    frame = self.root()
                    if frame is not None and not any(
                        x.operation
                        is self.originals[owner.UploadJournalValidationRetirement, "_select"]
                        for x in self.stack
                    ):
                        frame.own["inline_file_calls"] += 1
                        raise AssertionError("coordination performed workspace I/O: " + _name)
                    if (
                        self.stack
                        and self.stack[-1].operation is self.originals[TURN, "_receive"]
                        and _name in {"ftruncate", "unlink", "rmdir", "write", "fsync"}
                    ):
                        self.stack[-1].own["mutating_file_calls"] += 1
                    return _fn(*args, **kwargs)

                patch.setattr(os, name, file_call)
            try:
                yield self
            finally:
                for gate in self.gates:
                    gate.release.set()
                for gate in self.gates:
                    gate.thread.join(timeout=60)
                    assert not gate.thread.is_alive()

    def schedule(self, *, limit=1, budget=3):
        return self._invoke(
            ROOT, ROOT, (self.original.cleanup,), {"limit": limit, "octet_budget": budget}
        )

    def finish_workers(self):
        # Worker creation/start happened before submit returned. Gates may not
        # yet have been entered; a bounded test wait is not a coordinator wait.
        slots = tuple(self.original.pool._slots.values())
        for slot in slots:
            with self.gate_lock:
                assert self.gate_lock.wait_for(
                    lambda slot=slot: any(g.thread is slot.worker for g in self.gates), timeout=60
                ), "original worker did not reach its test entry barrier"
                gate = next(g for g in self.gates if g.thread is slot.worker)
            gate.release.set()
            slot.worker.join(timeout=60)
            assert not slot.worker.is_alive()
            if not self.allow_failure:
                slot.future.result()
            context = gate.context
            self.links.append(
                {
                    "context": id(context),
                    "reservation": id(context.reservation),
                    "worker": id(slot.worker),
                    "key": asdict(slot.key),
                    "terminal": slot.terminal,
                    "ordinary": not context.reservation.terminal,
                    "actor": None if context.actor is None else id(context.actor),
                    "actor_received": bool(context.actor is not None and context.actor.received),
                    "ordinary_still_retained": id(context.reservation)
                    in self.original.ledger._reservations,
                }
            )
