from __future__ import annotations

import hashlib
import sqlite3
from contextlib import closing, contextmanager
from dataclasses import asdict, is_dataclass, replace

import pytest
import test_assertion_staging as staging
import test_correction_projection as correction
import test_correction_targets as targets
import test_entry_preparation as preparation
import test_generic_semantics as generic
import test_row_semantics as row_semantics
import test_semantic_edge as edge
import test_semantic_finish as finish
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance._generic_semantics import GenericSemantics
from riverhog_provenance._semantic_edge import SemanticEdge
from test_generic_semantics import connect, identity
from test_semantic_owner import assert_uncommitted_hidden, snapshot

REGIMES = ((0, 1), (17, 1), (129, 17), (4097, 129))
HEADERS = {
    "entry": "journal_entry_preparation",
    "staging": "journal_assertion_staging",
    "targets": "journal_correction_targets",
    "correction": "journal_correction_work",
    "generic": "journal_generic_semantics",
    "finish": "journal_semantic_finish",
    "edge": "journal_semantic_edges",
}


def variants(family):
    if family == "correction":
        return ("retire", "seed")
    if family == "generic":
        return ("generic", "row")
    if family == "edge":
        return ("derivation", "specialization")
    return (family,)


def create_case(path, family, variant, catalog):
    if family == "targets":
        source = targets.create(path, count=3)
        return lambda db, attempt: targets.work(db, source, attempt=attempt)
    if family == "correction":
        correction.create(path)
        with closing(connect(path)) as db:
            if variant == "retire":
                correction.fill(db, "maps", 3)
            else:
                correction.assertions(db, 3, 2)
            db.commit()
        return lambda db, attempt: correction.work(db, variant, attempt_id=attempt)
    if family in {"entry", "staging"}:
        staging.create(path)
        source = preparation.frame() if family == "entry" else staging.frame([generic.agent()])
        selected = preparation if family == "entry" else staging
        return lambda db, attempt: selected.work(db, source, attempt_id=attempt)
    if family == "finish":
        finish.create(path)
        return lambda db, attempt: finish.work(db, attempt_id=attempt)
    if family == "generic":
        row = (
            generic.subject(0)
            if variant == "generic"
            else row_semantics.subject("journal_subject", 0)
        )
        generic.create(path, row)
        return lambda db, attempt: generic.work(
            db, row, catalog, operation=variant, attempt_id=attempt
        )
    assert family == "edge"
    edge.create(path, variant)
    return lambda db, attempt: edge.work(db, kind=variant, attempt_id=attempt)


def drain(current):
    for _ in range(10000):
        state = current.load()
        if state.done:
            return state
        if isinstance(current, GenericSemantics):
            batch = current.prepare(step_budget=64, byte_budget=16 * 1024 * 1024)
        elif isinstance(current, SemanticEdge):
            batch = current.prepare(row_budget=64)
        else:
            batch = current.prepare(row_budget=64, byte_budget=16 * 1024 * 1024)
        current.commit(batch)
    pytest.fail("the finite prior child fixture did not complete")


def octets(value):
    if value is None:
        return 0
    if is_dataclass(value):
        return octets(asdict(value))
    if isinstance(value, dict):
        return sum(octets(key) + octets(item) for key, item in value.items())
    if isinstance(value, (tuple, list)):
        return sum(octets(item) for item in value)
    return len(value if isinstance(value, bytes) else str(value).encode())


class AdmissionCursor(sqlite3.Cursor):
    closed = False

    def record(self, row):
        if row is not None:
            self.connection.metrics["returned_rows"] += 1
            self.connection.metrics["returned_metadata_octets"] += octets(row)
            if "FROM dag_edges AS edges" in self.sql or "FROM dag_incoming AS incoming" in self.sql:
                self.connection.metrics["adjacency_rows"] += 1
        return row

    def fetchone(self):
        return self.record(super().fetchone())

    def fetchall(self):
        return [self.record(row) for row in super().fetchall()]

    def __next__(self):
        return self.record(super().__next__())

    def close(self):
        self.closed = True
        return super().close()


class AdmissionConnection(sqlite3.Connection):
    recording = False

    def execute(self, sql, parameters=()):
        if not self.recording:
            return super().execute(sql, parameters)
        cursor = self.cursor(factory=AdmissionCursor)
        cursor.sql = sql
        self.statements.append((sql, parameters, cursor))
        size = octets(parameters)
        self.metrics["bound_parameter_octets"] += size
        if sql.startswith(("INSERT", "UPDATE", "DELETE")):
            self.metrics["written_value_octets"] += size
        return cursor.execute(sql, parameters)

    @contextmanager
    def observe(self):
        entered = self.in_transaction
        before = self.total_changes
        self.metrics = {
            name: 0
            for name in (
                "sql_statements",
                "returned_rows",
                "changed_rows",
                "returned_metadata_octets",
                "bound_parameter_octets",
                "written_value_octets",
                "adjacency_rows",
                "source_payload_octets",
                "sqlite_vm_instructions",
            )
        }
        self.statements = []
        denied = []

        def trace(_sql):
            self.metrics["sql_statements"] += 1

        def tick():
            self.metrics["sqlite_vm_instructions"] += 1
            return 0

        def authorize(action, table, column, _database, _trigger):
            if (
                action == sqlite3.SQLITE_READ
                and table in {"assertions", "identities", "frames"}
                and column == "value"
            ):
                denied.append((table, column))
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        self.set_trace_callback(trace)
        self.set_progress_handler(tick, 1)
        self.set_authorizer(authorize)
        self.recording = True
        try:
            yield self.metrics
        finally:
            self.recording = False
            self.set_trace_callback(None)
            self.set_progress_handler(None, 0)
            self.set_authorizer(None)
            self.metrics["changed_rows"] = self.total_changes - before
        assert not denied and self.in_transaction == entered
        assert all(cursor.closed for _sql, _parameters, cursor in self.statements)
        for sql, parameters, _cursor in self.statements:
            if not sql.startswith(("SELECT", "UPDATE", "DELETE")):
                continue
            with closing(self.execute("EXPLAIN QUERY PLAN " + sql, parameters)) as cursor:
                plan = " ".join(row[3] for row in cursor.fetchall())
            bounded_probe = sql in {
                "SELECT 1 FROM generic_semantic_frames LIMIT 1",
                "SELECT 1 FROM generic_semantic_keys LIMIT 1",
                "SELECT 1 FROM generic_semantic_outputs LIMIT 1",
            }
            assert "USE TEMP" not in plan
            assert bounded_probe or "SEARCH" in plan and "SCAN" not in plan, (sql, plan)


def samples_for(tmp_path, catalog, family, measured_operation, *, samples):
    for variant in variants(family):
        for history, width in REGIMES:
            for completed in (False, True):
                label = f"{variant}-{history}-{completed}"
                path = tmp_path / (label + "-setup.sqlite3")
                cold = tmp_path / (label + "-cold.sqlite3")
                factory = create_case(path, family, variant, catalog)
                attempt = 'owned-µ"\\-' * width
                with closing(connect(path)) as db:
                    db.executemany(
                        "INSERT INTO identities VALUES (?,?,?)",
                        (
                            (
                                identity(100000 + number),
                                "entry",
                                b"unread" * (65536 if number == 0 else 1),
                            )
                            for number in range(history)
                        ),
                    ).close()
                    db.commit()
                    if completed:
                        previous = factory(db, "prior-" + attempt)
                        previous.initialize()
                        prior_state = drain(previous)
                        # The admission fixture presents a completed owner whose
                        # prior temporary projection has already been retired.
                        if family == "targets":
                            db.execute("DELETE FROM correction_targets").close()
                        elif family == "staging":
                            db.execute("DELETE FROM changes").close()
                        db.commit()
                        if history == 4097:
                            state = replace(prior_state, version=2**200)
                            predicate, parameters = (
                                ("operation=?", (variant,))
                                if family == "correction"
                                else ("kind=?", (variant,))
                                if family == "edge"
                                else ("singleton=1", ())
                            )
                            db.execute(
                                f"UPDATE {HEADERS[family]} SET checkpoint=? WHERE {predicate}",
                                (state.to_json(), *parameters),
                            ).close()
                        db.commit()
                    expected = factory(db, attempt).prepare_admission()
                    # This fixed-shape owner proposal describes the actual point
                    # input headers; no observed SQL work sets its byte envelope.
                    dimensions = {
                        "retained_rows": sum(len(rows) for rows in snapshot(db).values()),
                        "metadata_row_octets": 1024 + 2 * octets(expected),
                    }
                    with closing(sqlite3.connect(cold)) as stored:
                        db.backup(stored)
                with closing(connect(cold, factory=AdmissionConnection)) as db:
                    current = factory(db, attempt)
                    before = snapshot(db)
                    if measured_operation == "prepare_admission":
                        changed = db.total_changes
                        with db.observe() as metrics:
                            batch = current.prepare_admission()
                        assert (
                            batch == expected
                            and not db.in_transaction
                            and db.total_changes == changed
                        )
                    else:
                        assert measured_operation == "_admit"
                        batch = current.prepare_admission()
                        assert batch == expected
                        db.execute("BEGIN IMMEDIATE").close()
                        try:
                            with db.observe() as metrics:
                                state = current._admit(batch)
                            assert db.in_transaction and state == batch.after
                            assert_uncommitted_hidden(cold, before)
                        finally:
                            db.rollback()
                    assert snapshot(db) == before
                    binding = growth_binding(getattr(type(current), measured_operation))
                    projection = project_bindings((binding,))
                    sample = {
                        "declaration": binding.declaration.id,
                        "declaration_sha256": hashlib.sha256(
                            canonical_json_bytes(projection["declarations"][binding.declaration.id])
                        ).hexdigest(),
                        "dimensions": dimensions,
                        "metrics": metrics,
                        "delegates": {},
                    }
                    validate_execution_samples(projection, binding.declaration.id, [sample])
                    samples.append(sample)
                    db.execute("BEGIN IMMEDIATE").close()
                    try:
                        after = current._admit(batch)
                        assert db.in_transaction and after == batch.after
                        db.commit()
                    except BaseException:
                        db.rollback()
                        raise
                    assert current.load() == batch.after
                    after_rows = snapshot(db)
                    allowed = {HEADERS[family]} | (
                        {"generic_semantic_frames"} if family == "generic" else set()
                    )
                    assert all(
                        after_rows[name] == rows
                        for name, rows in before.items()
                        if name not in allowed
                    )


@pytest.mark.growth_witness(
    "riverhog-entry-admission-plan-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._entry_preparation.EntryPreparation.prepare_admission",
)
def test_actual_entry_admission_plan(tmp_path, catalog, record_property):
    samples = []
    record_property("growth_samples", samples)
    samples_for(tmp_path, catalog, "entry", "prepare_admission", samples=samples)
    assert len(samples) == 8


@pytest.mark.growth_witness(
    "riverhog-entry-admission-application-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._entry_preparation.EntryPreparation._admit",
)
def test_actual_entry_admission_application(tmp_path, catalog, record_property):
    samples = []
    record_property("growth_samples", samples)
    samples_for(tmp_path, catalog, "entry", "_admit", samples=samples)
    assert len(samples) == 8


@pytest.mark.growth_witness(
    "riverhog-staging-admission-plan-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._assertion_staging.AssertionStaging.prepare_admission",
)
def test_actual_staging_admission_plan(tmp_path, catalog, record_property):
    samples = []
    record_property("growth_samples", samples)
    samples_for(tmp_path, catalog, "staging", "prepare_admission", samples=samples)
    assert len(samples) == 8


@pytest.mark.growth_witness(
    "riverhog-staging-admission-application-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._assertion_staging.AssertionStaging._admit",
)
def test_actual_staging_admission_application(tmp_path, catalog, record_property):
    samples = []
    record_property("growth_samples", samples)
    samples_for(tmp_path, catalog, "staging", "_admit", samples=samples)
    assert len(samples) == 8


@pytest.mark.growth_witness(
    "riverhog-targets-admission-plan-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._correction_targets.CorrectionTargets.prepare_admission",
)
def test_actual_targets_admission_plan(tmp_path, catalog, record_property):
    samples = []
    record_property("growth_samples", samples)
    samples_for(tmp_path, catalog, "targets", "prepare_admission", samples=samples)
    assert len(samples) == 8


@pytest.mark.growth_witness(
    "riverhog-targets-admission-application-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._correction_targets.CorrectionTargets._admit",
)
def test_actual_targets_admission_application(tmp_path, catalog, record_property):
    samples = []
    record_property("growth_samples", samples)
    samples_for(tmp_path, catalog, "targets", "_admit", samples=samples)
    assert len(samples) == 8


@pytest.mark.growth_witness(
    "riverhog-correction-admission-plan-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._correction_projection.CorrectionWork.prepare_admission",
)
def test_actual_correction_admission_plan(tmp_path, catalog, record_property):
    samples = []
    record_property("growth_samples", samples)
    samples_for(tmp_path, catalog, "correction", "prepare_admission", samples=samples)
    assert len(samples) == 16


@pytest.mark.growth_witness(
    "riverhog-correction-admission-application-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._correction_projection.CorrectionWork._admit",
)
def test_actual_correction_admission_application(tmp_path, catalog, record_property):
    samples = []
    record_property("growth_samples", samples)
    samples_for(tmp_path, catalog, "correction", "_admit", samples=samples)
    assert len(samples) == 16


@pytest.mark.growth_witness(
    "riverhog-generic-admission-plan-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._generic_semantics.GenericSemantics.prepare_admission",
)
def test_actual_generic_admission_plan(tmp_path, catalog, record_property):
    samples = []
    record_property("growth_samples", samples)
    samples_for(tmp_path, catalog, "generic", "prepare_admission", samples=samples)
    assert len(samples) == 16


@pytest.mark.growth_witness(
    "riverhog-generic-admission-application-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._generic_semantics.GenericSemantics._admit",
)
def test_actual_generic_admission_application(tmp_path, catalog, record_property):
    samples = []
    record_property("growth_samples", samples)
    samples_for(tmp_path, catalog, "generic", "_admit", samples=samples)
    assert len(samples) == 16


@pytest.mark.growth_witness(
    "riverhog-finish-admission-plan-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._semantic_finish.SemanticFinish.prepare_admission",
)
def test_actual_finish_admission_plan(tmp_path, catalog, record_property):
    samples = []
    record_property("growth_samples", samples)
    samples_for(tmp_path, catalog, "finish", "prepare_admission", samples=samples)
    assert len(samples) == 8


@pytest.mark.growth_witness(
    "riverhog-finish-admission-application-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._semantic_finish.SemanticFinish._admit",
)
def test_actual_finish_admission_application(tmp_path, catalog, record_property):
    samples = []
    record_property("growth_samples", samples)
    samples_for(tmp_path, catalog, "finish", "_admit", samples=samples)
    assert len(samples) == 8


@pytest.mark.growth_witness(
    "riverhog-edge-admission-plan-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._semantic_edge.SemanticEdge.prepare_admission",
)
def test_actual_edge_admission_plan(tmp_path, catalog, record_property):
    samples = []
    record_property("growth_samples", samples)
    samples_for(tmp_path, catalog, "edge", "prepare_admission", samples=samples)
    assert len(samples) == 16


@pytest.mark.growth_witness(
    "riverhog-edge-admission-application-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._semantic_edge.SemanticEdge._admit",
)
def test_actual_edge_admission_application(tmp_path, catalog, record_property):
    samples = []
    record_property("growth_samples", samples)
    samples_for(tmp_path, catalog, "edge", "_admit", samples=samples)
    assert len(samples) == 16


@pytest.mark.parametrize(
    "family,variant",
    [
        ("entry", "entry"),
        ("staging", "staging"),
        ("targets", "targets"),
        ("correction", "retire"),
        ("correction", "seed"),
        ("generic", "generic"),
        ("generic", "row"),
        ("finish", "finish"),
        ("edge", "derivation"),
        ("edge", "specialization"),
    ],
)
def test_superseded_admission_cannot_mutate_the_winner(tmp_path, catalog, family, variant):
    path = tmp_path / "stale.sqlite3"
    factory = create_case(path, family, variant, catalog)
    with closing(connect(path)) as db:
        stale = factory(db, "stale-attempt")
        batch = stale.prepare_admission()
        winner = factory(db, "winning-attempt")
        winner.initialize()
        before = snapshot(db)
        db.execute("BEGIN IMMEDIATE").close()
        try:
            with pytest.raises(RuntimeError):
                stale._admit(batch)
            assert db.in_transaction
        finally:
            db.rollback()
        assert snapshot(db) == before
