from __future__ import annotations

import hashlib
import inspect
import json
import sqlite3
import sys
from contextlib import closing, contextmanager
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import _assertion_staging as staging_owner
from riverhog_provenance import _correction_targets as target_owner
from riverhog_provenance import _entry_replay as replay_owner
from riverhog_provenance import (
    append_correction,
    assertion,
    assertion_reference,
    encode_entry,
    new_id,
    parse_journal,
    reference,
    validate_journal,
)
from riverhog_provenance import journal as journal_owner
from riverhog_provenance._entry_replay import EntryReplay, ReplayBudget
from riverhog_provenance._generic_semantics import GenericSemantics
from riverhog_provenance._observation_coverage import ObservationCoverage
from riverhog_provenance._online_dag import OnlineDag
from riverhog_provenance._semantic_edge import SemanticEdge
from riverhog_provenance._semantic_program import Nodes
from riverhog_provenance._session_checkpoint import ValidationLedger
from riverhog_provenance.journal_store import JournalStore
from test_child_admission_growth import octets
from test_entry_replay import owned_summary
from test_entry_replay_growth import REGIMES, observe_replay, owner
from test_entry_replay_growth import admission_histories as admission_histories
from test_entry_replay_publication_growth import restore
from test_entry_replay_row_growth import RowConnection, RowCursor, node_inputs
from test_generic_semantics import (
    code_tree,
    semantic_child_dimensions,
    semantic_child_input_dimensions,
    semantic_terminal_dimensions,
)
from test_semantic_edge import observed_work
from test_semantic_owner import snapshot

FAMILIES = {
    "entry": replay_owner.EntryPreparation,
    "targets": replay_owner.CorrectionTargets,
    "staging": replay_owner.AssertionStaging,
    "correction": replay_owner.CorrectionWork,
    "generic": GenericSemantics,
    "finish": replay_owner.SemanticFinish,
}
KINDS = {"entry", "targets", "staging", "retire", "seed", "generic", "row", "finish"}
DOMAINS = {
    "entry": ("identities", "changes", "journal_prefix", "journal_entry_preparation"),
    "targets": ("assertions", "frames", "correction_targets"),
    "staging": (
        "identities",
        "assertions",
        "changes",
        "activity_projection",
        "binding_projection",
        "delivery_projection",
        "description_projection",
        "observation_projection",
        "observation_observers",
    ),
    "finish": ("assertions", "changes", "maps", "fixities", "findings"),
}
TRAJECTORIES = (
    (REGIMES[0], False),
    (REGIMES[3], False),
    (REGIMES[5], False),
    (REGIMES[8], False),
    (REGIMES[12], False),
    (REGIMES[2], True),
    (REGIMES[0], "graph"),
)


_SQLITE_JSON_LOADS = json.loads
_RESET_CHANGED = (
    "DELETE FROM changes WHERE assertion_id IN (SELECT assertion_id FROM changes "
    "ORDER BY assertion_id LIMIT ?) RETURNING assertion_id"
)

_STAGING_IDENTITY_LINK = (
    "SELECT "
    "a.object_id,a.category,a.type,a.state_id,a.observation_id,a.sequence,a.active, "
    "i.domain,length(i.value),r.domain FROM changes c LEFT JOIN assertions a ON "
    "a.assertion_id=c.assertion_id LEFT JOIN identities i ON i.id=a.assertion_id "
    "LEFT JOIN identities r ON r.id=a.object_id WHERE c.assertion_id=?"
)


class ChildCursor(RowCursor):
    def __next__(self):
        return self.record(sqlite3.Cursor.__next__(self))

    def record(self, row):
        result = super().record(row)
        if row is None:
            return result
        db, sql = self.connection, self.sql
        family = db.family
        if family in {"targets", "correction"}:
            if family == "targets" and sql.startswith("SELECT t.position"):
                if "a.active, length(" in sql:
                    size = octets(row[:3]) + row[5]
                elif "WHERE t.position >= ?" in sql:
                    size = octets(row[:3])
                else:
                    size = None
                if size is not None:
                    db.weights[row[1]] = size
                    db.metrics["inspected_octets"] += size
            elif family == "correction":
                if sql.startswith("SELECT rowid,"):
                    db.weights[row[0]] = row[1]
                    db.metrics["inspected_octets"] += row[1]
                elif sql.startswith("SELECT assertion_id FROM assertions"):
                    db.metrics["inspected_octets"] += len(row[0].encode())
        elif family == "staging" and sql.startswith("SELECT domain,length(value)"):
            db.prior_sizes[self.parameters[0]] = row[1]
            db.metrics["inspected_octets"] += row[1]
        if family == "finish" and sql.startswith("SELECT value "):
            size = len(row[0])
            db.metrics["returned_metadata_octets"] -= size
            db.metrics["returned_atom_octets"] += size
            db.metrics["payload_fetched_octets"] += size
        return result


class ChildConnection(RowConnection):
    family = None

    def execute(self, sql, parameters=()):
        if not self.recording:
            return sqlite3.Connection.execute(self, sql, parameters)
        self.active_sql, self.active_parameters = sql, parameters
        if sql == "SELECT value FROM assertions WHERE assertion_id = ? AND active = 1":
            assert parameters == (self.selected_identity,)
            assert self.selected_length <= self.byte_budget
        cursor = self.cursor(factory=ChildCursor)
        cursor.sql, cursor.parameters = sql, parameters
        self.statements.append((sql, parameters, cursor))
        cursor.execute(sql, parameters)
        family = self.family
        if family == "targets":
            if sql.startswith(("INSERT INTO correction_targets", "DELETE FROM correction_targets")):
                self.metrics["admitted_octets"] += self.weights[parameters[1]]
            elif sql.startswith("UPDATE assertions SET active"):
                self.metrics["admitted_octets"] += self.weights[parameters[0]]
        elif family == "correction":
            if sql.startswith("DELETE FROM "):
                assert cursor.rowcount == 1
                self.metrics["admitted_octets"] += self.weights[parameters[0]]
            elif sql.startswith("INSERT OR IGNORE INTO changes"):
                self.metrics["admitted_octets"] += len(parameters[0].encode())
        elif family == "staging":
            if sql.startswith("INSERT"):
                size = octets(parameters)
                if "INTO identities" in sql:
                    size += len("referent" if "referent" in sql else "assertion")
                elif "INTO assertions" in sql:
                    size += 1
                self.metrics["admitted_octets"] += size
                if "INTO observation_observers" in sql:
                    self.metrics["inspected_octets"] += size
            elif sql.startswith("UPDATE observation_projection"):
                self.metrics["admitted_octets"] += octets(parameters) + 1
            elif sql.startswith("SELECT value=?"):
                self.metrics["admitted_octets"] += self.prior_sizes[parameters[1]]
        return cursor

    def set_authorizer(self, callback):
        if callback is None:
            return sqlite3.Connection.set_authorizer(self, None)

        def authorize(action, table, column, database, trigger):
            if action == sqlite3.SQLITE_READ and column == "value":
                sql = self.active_sql
                if table in {"frames", "journal_validation_frame"}:
                    return sqlite3.SQLITE_DENY
                if table == "identities":
                    if sql == _STAGING_IDENTITY_LINK:
                        return sqlite3.SQLITE_OK
                    return (
                        sqlite3.SQLITE_OK
                        if sql.startswith(
                            (
                                "SELECT domain,length(value)",
                                "SELECT domain,typeof(value),length(value)",
                                "SELECT value=?",
                            )
                        )
                        else sqlite3.SQLITE_DENY
                    )
                if table == "assertions":
                    if sql.startswith("SELECT length(value)"):
                        return sqlite3.SQLITE_OK
                    if sql == "SELECT value FROM assertions WHERE assertion_id = ? AND active = 1":
                        return (
                            sqlite3.SQLITE_OK
                            if self.selected_length is not None
                            and self.byte_budget is not None
                            and self.selected_length <= self.byte_budget
                            and self.active_parameters == (self.selected_identity,)
                            else sqlite3.SQLITE_DENY
                        )
                    if self.family == "finish" and sql.startswith("SELECT value "):
                        return sqlite3.SQLITE_OK
                    # Target retirement inspects logical row length, never the payload.
                    if (
                        self.family == "targets"
                        and sql.startswith("SELECT t.position")
                        and "length(" in sql
                    ):
                        return sqlite3.SQLITE_OK
                    return sqlite3.SQLITE_DENY
            return callback(action, table, column, database, trigger)

        return sqlite3.Connection.set_authorizer(self, authorize)

    @contextmanager
    def physical_observer(self):
        entered = self.in_transaction
        self.metrics = {
            name: 0
            for name in (
                "sql_statements",
                "returned_rows",
                "changed_rows",
                "returned_metadata_octets",
                "adjacency_rows",
                "source_payload_octets",
                "sqlite_vm_instructions",
                "source_hashed_octets",
                "native_hashed_octets",
                "sha256_compression_calls",
                "decoded_json_octets",
                "encoded_json_octets",
                "native_schema_calls",
                "published_metadata_octets",
                "published_payload_octets",
                "returned_atom_octets",
                "historical_payload_octets",
            )
        }
        self.statements = []
        before = self.total_changes

        def trace(_sql):
            self.metrics["sql_statements"] += 1

        def tick():
            self.metrics["sqlite_vm_instructions"] += 1
            return 0

        self.set_trace_callback(trace)
        self.set_progress_handler(tick, 1)
        self.set_authorizer(lambda *_args: sqlite3.SQLITE_OK)
        self.recording = True
        try:
            yield self.metrics
        finally:
            self.recording = False
            self.set_trace_callback(None)
            self.set_progress_handler(None, 0)
            self.set_authorizer(None)
            self.metrics["changed_rows"] = self.total_changes - before
        assert self.in_transaction == entered
        assert all(cursor.closed for _sql, _parameters, cursor in self.statements)
        bounded_probes = {
            "SELECT 1 FROM generic_semantic_frames LIMIT 1",
            "SELECT 1 FROM generic_semantic_keys LIMIT 1",
            "SELECT 1 FROM generic_semantic_outputs LIMIT 1",
        }
        for sql, parameters, _cursor in self.statements:
            if not sql.startswith(("SELECT", "UPDATE", "DELETE")):
                continue
            with closing(self.execute("EXPLAIN QUERY PLAN " + sql, parameters)) as cursor:
                plan = " ".join(row[3] for row in cursor.fetchall())
            assert "USE TEMP" not in plan
            bounded_deletion = (
                sql == _RESET_CHANGED and len(parameters) == 1 and 1 <= parameters[0] <= 64
            )
            assert (
                sql in bounded_probes or bounded_deletion or "SEARCH" in plan and "SCAN" not in plan
            )

    @contextmanager
    def observe(self):
        self.weights, self.prior_sizes = {}, {}
        with self.physical_observer() as metrics:
            metrics.update(
                admitted_octets=0,
                inspected_octets=0,
                payload_fetched_octets=0,
                historical_frame_octets=0,
                historical_payload_fetched_octets=0,
                json_function_calls=0,
            )
            real_loads = _SQLITE_JSON_LOADS

            def extract(value, path):
                metrics["json_function_calls"] += 1
                assert path in {"$.object_id", "$.at"}
                decoded = real_loads(value)
                selected = decoded.get(path[2:]) if isinstance(decoded, dict) else None
                assert selected is None or type(selected) in {str, int, float, bool}
                return int(selected) if type(selected) is bool else selected

            self.create_function("json_extract", 2, extract, deterministic=True)
            with pytest.MonkeyPatch.context() as patch:
                canonical = target_owner.canonical_json_bytes
                document, signature = (
                    staging_owner.canonical_document,
                    journal_owner._identity_signature,
                )

                def target(value):
                    encoded = canonical(value)
                    if (
                        self.family == "targets"
                        and type(value) is dict
                        and set(value) == {"entry", "assertion_id"}
                    ):
                        self.weights[value["assertion_id"]] = len(encoded)
                        metrics["inspected_octets"] += len(encoded)
                    return encoded

                def current_document(value):
                    encoded = document(value)
                    if self.family == "staging":
                        metrics["inspected_octets"] += len(encoded)
                    return encoded

                def current_signature(value):
                    encoded = signature(value)
                    if self.family == "staging":
                        metrics["inspected_octets"] += len(encoded)
                    return encoded

                patch.setattr(target_owner, "canonical_json_bytes", target)
                patch.setattr(staging_owner, "canonical_document", current_document)
                patch.setattr(journal_owner, "_identity_signature", current_signature)
                yield metrics


@contextmanager
def node_kernels():
    operations = (Nodes.__init__, ObservationCoverage.__init__)
    allowed = {operation: code_tree(inspect.unwrap(operation).__code__) for operation in operations}
    allowed[operations[0]] |= allowed[operations[1]]
    active = []

    def trace(frame, event, _argument):
        if event == "call":
            if not any(frame.f_code in item["allowed"] for item in active):
                return None
            frame.f_trace_opcodes = True
            return trace
        if event == "opcode":
            for item in active:
                if frame.f_code in item["allowed"]:
                    item["events"] += 1
        return trace

    @contextmanager
    def kernel(operation, args):
        if operation not in allowed:
            yield None
            return
        tracker = {"allowed": allowed[operation], "events": 0}
        outer, previous, frame = not active, sys.gettrace(), sys._getframe()
        previous_opcodes = frame.f_trace_opcodes
        if outer:
            frame.f_trace_opcodes = True
            sys.settrace(trace)
        active.append(tracker)

        def complete(_result):
            assert tracker["events"] > 0
            if operation is operations[0]:
                return {
                    "indexed_containers": len(args[0].values),
                    "python_opcode_events": tracker["events"],
                }
            return {
                "indexed_rows": len(args[0].statuses),
                "retained_key_octets": sum(
                    len(a.encode()) + len(b.encode()) for a, b in args[0].statuses
                ),
                "python_opcode_events": tracker["events"],
            }

        try:
            yield complete
        finally:
            assert active.pop() is tracker
            if outer:
                sys.settrace(previous)
                frame.f_trace_opcodes = previous_opcodes

    yield kernel
    assert not active


def family_for(kind):
    return (
        "correction"
        if kind in {"retire", "seed"}
        else "generic"
        if kind in {"generic", "row"}
        else kind
    )


def trajectories(root, catalog, build, specifications=TRAJECTORIES):
    """Save genuine pre-unit checkpoints, then validate the completed native trajectory."""
    result = []
    for regime in sorted(
        {regime for regime, _variant in specifications}, key=lambda value: value[0]
    ):
        build(regime)
    for ordinal, (regime, correction) in enumerate(specifications):
        case = dict(build(regime))
        if correction == "graph":
            document = deepcopy(case["candidate"].document)
            assertions = document["body"]["assertions"]
            first = assertions["states"][0]
            second = deepcopy(first)
            second["id"], second["assertion_id"] = new_id(), new_id()
            assertions["states"].append(second)
            assertions.setdefault("relations", []).extend(
                [
                    assertion(
                        "derivation",
                        case["who"],
                        used_state=reference(first["id"], "state"),
                        generated_state=reference(second["id"], "state"),
                        kind="copy",
                    ),
                    assertion(
                        "specialization",
                        case["who"],
                        specific=reference(first["id"], "state"),
                        general=reference(second["id"], "state"),
                    ),
                ]
            )
            case["raw"] = encode_entry(document, catalog=catalog)
            case["candidate"] = parse_journal(case["raw"])[0]
        elif correction:
            raw = case["raw"][: -len(case["candidate"].encoded)]
            with validate_journal(raw, catalog=catalog) as initial:
                value = deepcopy(initial.graph["artifacts"][0])
                target = assertion_reference(initial, value["assertion_id"])
            value["assertion_id"] = new_id()
            case["raw"] = append_correction(
                raw,
                [target],
                reason="Replace documentary assertion.",
                recorded_by_agent_id=case["who"],
                assertions={"artifacts": [value]},
                catalog=catalog,
            )
            case["candidate"] = parse_journal(case["raw"])[-1]
        with closing(JournalStore()) as store:
            current = restore(store, case, catalog)
            current.admit(case["candidate"])
            seen = set()
            for _ in range(100000):
                before = current.ledger.load()
                phase = before.checkpoint.phase
                if phase == "frame":
                    break
                kind = phase.removesuffix("-admission")
                if kind in KINDS:
                    family = family_for(kind)
                    cached = current._current
                    if phase.endswith("-admission"):
                        key = kind, "admission"
                    else:
                        child = current._child(
                            before.checkpoint, cached, kind, before.checkpoint.child_id
                        )
                        state = child.load()
                        key = kind, state.phase
                        if isinstance(child, GenericSemantics):
                            key += (child.kind,)
                            if state.phase == "edge":
                                edge = child._edge_work(state, child._prefix())
                                key += (edge.load().phase, edge.checker.progress().phase)
                            if state.phase == "walk":
                                row = store.db.execute(
                                    (
                                        "SELECT value FROM generic_semantic_frames "
                                        "WHERE attempt_id=? AND frame_id=?"
                                    ),
                                    (child.attempt_id, state.top),
                                ).fetchone()
                                frame = json.loads(row[0])
                                key += (frame["operation"], frame["stage"])
                    if key not in seen:
                        seen.add(key)
                        path = root / f"trajectory-{ordinal}-checkpoint-{len(seen)}.sqlite3"
                        with closing(sqlite3.connect(path)) as db:
                            store.db.backup(db)
                        result.append(
                            {
                                **case,
                                "path": path,
                                "before": before,
                                "cached": replace(
                                    cached, child_kind=None, child_id=None, child=None
                                ),
                                "key": key,
                                "family": family,
                            }
                        )
                current.advance(ReplayBudget(rows=16))
            else:
                pytest.fail("finite actual child trajectory did not complete")
            with validate_journal(case["raw"], catalog=catalog) as expected:
                with owned_summary(store, current.prefix.finish()) as actual:
                    assert actual.materialize() == expected.materialize()
                    assert actual.journal_sha256 == hashlib.sha256(case["raw"]).hexdigest()
    return result


def dimensions_for(current, case, *, cold_child, cold_row, budget):
    """Derive only input facts on the setup pager, before any measured call."""
    original = snapshot(current.db)
    before, family = case["before"], case["family"]
    state = before.checkpoint
    kind = state.phase.removesuffix("-admission")
    admission = state.phase.endswith("-admission")
    probe = replace(current._current)
    child = current._child(state, probe, kind, "0" * 32 if admission else state.child_id)
    retained = None if admission else child.load()
    done = not admission and retained.done
    header = tuple(
        rows
        for name, rows in original.items()
        if name.startswith("journal_") and name != "journal_validation_frame"
    )
    width = 16384 + 2 * octets(
        (before, header, getattr(child, "binding", None), getattr(child, "source_binding", None))
    )
    rows = sum(len(values) for values in original.values())
    row_values = {item.name: 0 for item in growth_binding(EntryReplay._row).declaration.dimensions}
    row_present = kind in {"generic", "row"}
    row_constructor = int(row_present and (admission or cold_child))
    row_completion = int(kind == "generic" and done)
    if row_present:
        row_values.update(
            indexed_assertions=len(original["assertions"]),
            changed_identities=len(original["changes"]),
            atom_octets=len(probe.atom.encoded),
            cold_row=int(cold_row),
            **node_inputs(probe.row),
        )
    constructor_cold = bool(row_constructor and cold_row)
    remaining = current._remaining() - (row_values["atom_octets"] if constructor_cold else 0)
    values = {
        "retained_rows": rows,
        "row_budget": budget.rows,
        "metadata_row_octets": width,
        "bare_load_calls": int(not admission),
        "constructor_frame_octets": len(case["candidate"].json_bytes)
        if (admission or cold_child) and family in {"entry", "targets", "staging", "finish"}
        else 0,
        "planning_byte_budget": remaining if not admission and not done else 0,
        "generic_schema_steps": budget.rows
        if family == "generic" and not admission and not done
        else 0,
    }
    delegates = growth_binding(EntryReplay._child_step).delegates
    for tag, operation in delegates:
        active = tag in {"fence", "publication"}
        child_values = {}
        if tag.startswith(family + "_"):
            stage = tag.removeprefix(family + "_")
            active = stage in (
                {"admission", "admit"}
                if admission
                else {"result"}
                if done
                else {"planning", "application"}
            )
            if stage == "result" and family not in {"entry", "generic"}:
                active = False
        if tag in {"row_constructor", "row_completion"}:
            active = bool(row_constructor if tag == "row_constructor" else row_completion)
            child_values = dict(row_values)
            if tag == "row_completion":
                child_values["cold_row"] = int(cold_row and not row_constructor)
        elif tag in {"fence", "publication"}:
            child_values = {
                "retained_rows": sum(
                    len(original[name])
                    for name in ("journal_validation", "journal_prefix", "frames")
                ),
                "metadata_row_octets": width,
            }
        elif active:
            declaration = growth_binding(operation).declaration
            phase = operation.__name__ not in {"prepare_admission", "_admit"}
            domain = (
                sum(len(original[name]) for name in DOMAINS[family]) if family in DOMAINS else rows
            )
            child_values = {
                "retained_rows": domain if phase else rows,
                "metadata_row_octets": width,
                "row_budget": budget.rows,
                "step_budget": budget.rows,
                "byte_budget": remaining,
                "control_octets": width,
                "max_atom_octets": max(
                    [
                        len(case["candidate"].json_bytes),
                        *(
                            octets(row)
                            for name in ("assertions", "maps", "findings")
                            for row in original[name]
                        ),
                    ]
                ),
            }
            if family == "generic" and phase:
                extras = {
                    item.name: 0 for item in declaration.dimensions if item.name not in child_values
                }
                if done:
                    extras.update(semantic_terminal_dimensions(child))
                elif child.operation == "row" and child.kind in {"derivation", "specialization"}:
                    extras.update(semantic_child_dimensions(child, budget.rows))
                child_values.update(extras)
        values[tag + "_calls"] = int(active)
        for item in growth_binding(operation).declaration.dimensions:
            values[tag + "_" + item.name] = child_values.get(item.name, 0)
    return values, original


@contextmanager
def child_observer(current, case, dimensions):
    db = current.db
    binding = growth_binding(EntryReplay._child_step)
    roots = dict(binding.delegates)
    root_operations = set(roots.values())
    row_operation = EntryReplay._row
    selected_operation = replay_owner.read_changed_assertion
    owners = {
        operation: FAMILIES[family]
        for family in FAMILIES
        for operation in root_operations
        if operation.__qualname__.startswith(FAMILIES[family].__name__ + ".")
    }
    owners.update(
        {
            ValidationLedger._fence: ValidationLedger,
            ValidationLedger._publish: ValidationLedger,
            EntryReplay._row: EntryReplay,
            Nodes.__init__: Nodes,
            ObservationCoverage.__init__: ObservationCoverage,
            replay_owner.read_changed_assertion: replay_owner,
            SemanticEdge.prepare_admission: SemanticEdge,
            SemanticEdge._admit: SemanticEdge,
            SemanticEdge.prepare: SemanticEdge,
            SemanticEdge._apply: SemanticEdge,
            SemanticEdge.result: SemanticEdge,
        }
    )
    for name in ("_prepare", "_apply", "_transition"):
        operation = getattr(OnlineDag, name, None)
        if operation is not None:
            owners[operation] = OnlineDag
    pending = [growth_binding(EntryReplay._child_step)]
    while pending:
        for _name, operation in pending.pop().delegates:
            if operation.__qualname__.startswith("OnlineDag."):
                owners[operation] = OnlineDag
            if growth_binding(operation).delegates:
                pending.append(growth_binding(operation))
    node_operation, coverage_operation = Nodes.__init__, ObservationCoverage.__init__

    def selector(parent, operation, _args, _kwargs, tags):
        assert operation is row_operation and set(tags) == {"row_constructor", "row_completion"}
        tag = (
            "row_constructor"
            if dimensions["row_constructor_calls"] and not parent["delegates"]["row_constructor"]
            else "row_completion"
        )
        assert dimensions[tag + "_calls"] == 1
        return tag

    def inputs(parent, operation, args, kwargs):
        if operation in root_operations:
            if operation is row_operation:
                calls = db.row_invocations
                tag = (
                    "row_constructor"
                    if dimensions["row_constructor_calls"] and calls == 0
                    else "row_completion"
                )
                db.row_invocations += 1
            else:
                tag = next(name for name, child in binding.delegates if child is operation)
            result = {
                item.name: dimensions[tag + "_" + item.name]
                for item in growth_binding(operation).declaration.dimensions
            }
            if operation.__name__ == "prepare":
                assert (
                    kwargs.get("row_budget", kwargs.get("step_budget")) == dimensions["row_budget"]
                )
                assert kwargs["byte_budget"] == result["byte_budget"]
            return result
        if operation is selected_operation:
            db.byte_budget = kwargs["byte_budget"]
            return {
                item.name: parent[item.name]
                for item in growth_binding(operation).declaration.dimensions
            }
        if operation is node_operation:
            return {
                item.name: parent["atom_input_items" if item.name == "input_items" else item.name]
                for item in growth_binding(operation).declaration.dimensions
            }
        if operation is coverage_operation:
            return {
                "input_rows": parent["coverage_rows"],
                "key_octets": parent["coverage_key_octets"],
            }
        return semantic_child_input_dimensions(parent, operation)

    with node_kernels() as kernel:

        @contextmanager
        def context(operation, args):
            previous = db.family
            selected = operation.__qualname__.split(".")[0]
            db.family = next(
                (family for family, cls in FAMILIES.items() if cls.__name__ == selected), previous
            )
            try:
                with kernel(operation, args) as callback:
                    yield callback
            finally:
                db.family = previous

        def primitive(operation, args, _result, work):
            if operation is row_operation:
                # The row's physical kernel metrics are the actual descendants.
                work.update(
                    indexed_containers=db.node_containers,
                    index_python_opcode_events=db.node_opcodes,
                )
                db.node_containers = db.node_opcodes = 0
            if operation is node_operation:
                db.node_containers += work["indexed_containers"]
                db.node_opcodes += work["python_opcode_events"]
            if any(
                item.metric == "metadata_octets"
                for item in growth_binding(operation).declaration.bounds
            ):
                work["metadata_octets"] = work["returned_metadata_octets"]
            work.setdefault("historical_payload_octets", 0)
            return work

        db.row_invocations = db.node_containers = db.node_opcodes = 0
        with observe_replay(
            db,
            current.catalog,
            dimensions,
            root_operation=EntryReplay._child_step,
            input_dimensions=inputs,
            operation_owners=owners,
            kernel_observer=context,
            delegate_selector=selector,
            validation_policy=lambda schema, value: None,
            primitive_adapter=primitive,
        ) as observed:
            yield observed


def child_samples(root, catalog, build, *, samples, specifications=TRAJECTORIES, early=False):
    binding = growth_binding(EntryReplay._child_step)
    projection = project_bindings((binding,))
    checkpoints = trajectories(root, catalog, build, specifications)
    for ordinal, case in enumerate(checkpoints):
        generic = case["family"] == "generic"
        admission = case["before"].checkpoint.phase.endswith("-admission")
        modes = (
            ((True, False), (True, True))
            if generic and admission
            else ((False, False), (True, False), (True, True))
            if generic
            else ((True, False),)
            if admission
            else ((False, False), (True, False))
        )
        budgets = (1, 16, 64) if not admission and (ordinal % 7 == 0) and not early else (16,)
        for cold_child, cold_row in modes:
            for rows in budgets:
                with closing(JournalStore()) as store:
                    with closing(sqlite3.connect(case["path"])) as seed:
                        seed.backup(store.db)
                    current = owner(store, catalog, case["source_binding"], case["epoch"])
                    current._current = replace(case["cached"])
                    budget = ReplayBudget(rows=rows)
                    current._start(case["before"], budget)
                    dimensions, original = dimensions_for(
                        current, case, cold_child=cold_child, cold_row=cold_row, budget=budget
                    )
                    # A separate uninstrumented native execution supplies the exact unit oracle.
                    oracle_path = root / "unit-oracle.sqlite3"
                    with closing(sqlite3.connect(oracle_path)) as oracle_db:
                        store.db.backup(oracle_db)
                    with closing(JournalStore()) as oracle_store:
                        with closing(sqlite3.connect(oracle_path)) as seed:
                            seed.backup(oracle_store.db)
                        oracle = owner(oracle_store, catalog, case["source_binding"], case["epoch"])
                        oracle._current = replace(case["cached"])
                        oracle._start(case["before"], budget)
                        with pytest.MonkeyPatch.context() as patch:
                            patch.setattr(
                                replay_owner, "uuid4", lambda: SimpleNamespace(hex="f" * 32)
                            )
                            expected = oracle._child_step(case["before"], oracle._current)
                        expected_rows = snapshot(oracle_store.db)
                    path = store._storage.directory / "snapshot.sqlite3"
                    store.db.close()
                    store._storage.connection = sqlite3.connect(path, factory=ChildConnection)
                    store.db.execute("PRAGMA synchronous=FULL").close()
                    store.db.execute("PRAGMA cache_size=-64").close()
                    current = owner(store, catalog, case["source_binding"], case["epoch"])
                    current._current = replace(case["cached"])
                    current._start(case["before"], budget)
                    if not cold_child and not admission:
                        current._child(
                            case["before"].checkpoint,
                            current._current,
                            case["before"].checkpoint.phase,
                            case["before"].checkpoint.child_id,
                        )
                    if cold_row:
                        current._current.row_key = current._current.row_sha256 = None
                        current._current.row = current._current.atom = None
                    with pytest.MonkeyPatch.context() as patch:
                        patch.setattr(replay_owner, "uuid4", lambda: SimpleNamespace(hex="f" * 32))
                        with child_observer(current, case, dimensions) as (observed, complete):
                            with current.db.observe():
                                result = current._child_step(case["before"], current._current)
                    work = observed_work(current.db, {key: 0 for key in current.db.metrics}, 0, 0)
                    work.update(
                        changed_rows=current.db.metrics["changed_rows"],
                        indexed_containers=sum(
                            child["metrics"]["indexed_containers"]
                            for tag in ("row_constructor", "row_completion")
                            for child in observed["delegates"][tag]
                        ),
                        index_python_opcode_events=sum(
                            child["metrics"]["index_python_opcode_events"]
                            for tag in ("row_constructor", "row_completion")
                            for child in observed["delegates"][tag]
                        ),
                    )
                    sample = complete(observed, work)
                    validate_execution_samples(projection, binding.declaration.id, [sample])
                    samples.append(sample)
                    assert result == expected == current.ledger.load()
                    assert snapshot(current.db) == expected_rows
                    assert (
                        not current.db.in_transaction
                        and snapshot(current.db)["journal_prefix"] == original["journal_prefix"]
                    )
                    for tag, children in sample["delegates"].items():
                        assert len(children) == dimensions[tag + "_calls"]
    assert samples


@pytest.mark.growth_witness(
    "riverhog-entry-replay-child-step-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._entry_replay.EntryReplay._child_step",
)
def test_actual_cold_and_warm_child_admission_resume_and_completion(
    tmp_path, catalog, admission_histories, record_property
):
    samples = []
    record_property("growth_samples", samples)
    child_samples(tmp_path, catalog, admission_histories, samples=samples)
