from __future__ import annotations

import copy
import hashlib
import sqlite3
import subprocess
import sys
from contextlib import closing, contextmanager
from dataclasses import replace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    append_correction,
    assertion,
    assertion_reference,
    journal,
    journal_store,
    new_id,
    parse_journal,
    reference,
    validate_journal,
)
from riverhog_provenance import _assertion_staging as implementation
from riverhog_provenance._assertion_staging import (
    AssertionStaging,
    AssertionStagingCapacity,
    StaleAssertionStaging,
)
from riverhog_provenance._journal_prefix import PrefixState
from riverhog_provenance.errors import ProvenanceValidationError
from riverhog_provenance.journal import JournalFrame, _identity_signature
from sha256_continuation import CheckpointSHA256


def identity(ordinal):
    return f"urn:uuid:00000000-0000-4000-8000-{ordinal:012x}"


def connect(path, **options):
    db = sqlite3.connect(path, check_same_thread=False, **options)
    db.execute("PRAGMA synchronous=FULL").close()
    db.execute("PRAGMA cache_size=-64").close()
    db.execute("PRAGMA temp_store=FILE").close()
    return db


def create(path):
    store = journal_store.JournalStore()
    try:
        with closing(connect(path)) as db:
            store.db.backup(db)
    finally:
        store.close()
    prior = JournalFrame(canonical_json_bytes({"id": identity(1), "sequence": "0"}))
    prefix = PrefixState(
        1,
        len(prior.encoded),
        identity(2),
        canonical_json_bytes(prior.reference).decode(),
        CheckpointSHA256(prior.encoded).export_state(),
        False,
    )
    with closing(connect(path)) as db:
        db.execute(
            "INSERT INTO frames VALUES (?,?,?)",
            ("0", prior.json_bytes, canonical_json_bytes(prior.reference)),
        )
        db.execute(
            "INSERT INTO journal_prefix VALUES (1,?,?,?)",
            ("prefix-attempt", b"owned-exact-source", prefix.to_json()),
        )
        db.commit()


def row(ordinal, payload=0):
    value = {
        "id": identity(1000000 + ordinal),
        "assertion_id": identity(100 + ordinal),
        "type": "agent",
        "kind": "software",
    }
    if payload:
        value["name"] = "x" * payload
    return value


def frame(rows=(), **fields):
    values = list(rows)
    category = (
        {
            "extension": "extensions",
            "activity": "activities",
            "locator_binding": "locator_bindings",
            "context": "contexts",
            "observation": "descriptions",
        }.get(values[0]["type"], "agents")
        if values
        else "agents"
    )
    return JournalFrame(
        canonical_json_bytes(
            {
                "journal_id": identity(2),
                "sequence": "1",
                "body": {"assertions": {category: values}},
                **fields,
            }
        )
    )


def work(db, source, **options):
    return AssertionStaging(
        db,
        **{
            "attempt_id": "owned-staging-attempt",
            "frame": source,
            "catalog_sha256": "a" * 64,
            "require_profiles": False,
            **options,
        },
    )


def contents(db):
    return {
        name: db.execute("SELECT * FROM " + name + " ORDER BY rowid").fetchall()
        for name in (
            "identities",
            "assertions",
            "changes",
            "frames",
            "journal_prefix",
            "journal_assertion_staging",
            "activity_projection",
            "binding_projection",
            "delivery_projection",
            "description_projection",
            "observation_projection",
            "observation_observers",
        )
    }


def logical_octets(values):
    return sum(
        len(value) if isinstance(value, bytes) else len(str(value).encode())
        for value in values
        if value is not None
    )


def proposed_octets(category, value, sequence="1"):
    signature = _identity_signature(value)
    total = sum(
        logical_octets(values)
        for values in (
            (value["assertion_id"], "assertion", b""),
            (value["id"], "referent", signature),
            (
                value["assertion_id"],
                value["id"],
                category,
                value["type"],
                value.get("state", {}).get("object_id"),
                value.get("observation_id"),
                sequence,
                canonical_json_bytes(value),
                1,
            ),
            (value["assertion_id"],),
        )
    )
    if value["type"] in {"usage", "generation"}:
        total += logical_octets(
            (value["assertion_id"], value["activity_id"], value["state"]["object_id"])
        )
    if value["type"] == "activity":
        total += logical_octets(
            (
                value["assertion_id"],
                value["kind"],
                value.get("started_at"),
                value.get("ended_at"),
            )
        )
    if value["type"] == "locator_binding":
        total += logical_octets(
            (
                value["assertion_id"],
                value["temporal_scope"].get("at", value["temporal_scope"].get("start")),
            )
        )
    if value["type"] in {"context", "observation"}:
        total += logical_octets(
            (
                value["assertion_id"],
                value["kind"] if value["type"] == "context" else value["state"]["object_id"],
            )
        )
    if value["type"] in {"state", "activity"}:
        total += logical_octets(
            (
                value["assertion_id"],
                value.get("extent", {}).get("kind"),
                value.get("outcome"),
                0 if value.get("associations") else 1,
            )
        )
    total += logical_octets(
        (
            value["assertion_id"],
            value.get("occurrence_id"),
            value.get("kind"),
            value.get("source_context_id"),
        )
    )
    return total


def test_reopen_each_unit_stages_native_source_order_and_keeps_accepted_prefix(tmp_path):
    path = tmp_path / "resume.sqlite3"
    create(path)
    values = [row(n) for n in reversed(range(17))]
    source = frame(values)
    with closing(connect(path)) as db:
        work(db, source).initialize()
        original = contents(db)
    for _ in range(20):
        with closing(connect(path)) as db:
            current = work(db, source)
            before = current.load()
            state = current.advance(row_budget=3)
            assert 0 <= state.position - before.position <= 3
            assert not db.in_transaction
            actual = contents(db)
            assert actual["frames"] == original["frames"]
            assert actual["journal_prefix"] == original["journal_prefix"]
            assert [item[0] for item in actual["assertions"]] == [
                item["assertion_id"] for item in values[: state.position]
            ]
            assert len(actual["identities"]) == state.position * 2
            assert len(actual["changes"]) == state.position
            if state.done:
                assert current.advance() == state
                break
    else:
        pytest.fail("staging did not complete")


@pytest.mark.parametrize(
    "fault,message",
    [
        ("assertion-reuse", "cannot be reused"),
        ("domain", "immutable referent"),
        ("signature", "immutable referent"),
        ("active-object", "duplicate effective"),
        ("same-domain-id", "immutable referent"),
    ],
)
def test_native_identity_errors_leave_the_current_unit_unpublished(tmp_path, fault, message):
    path = tmp_path / "identity.sqlite3"
    create(path)
    value = row(1)
    if fault == "same-domain-id":
        value["id"] = value["assertion_id"]
    source = frame([value])
    with closing(connect(path)) as db:
        if fault == "assertion-reuse":
            db.execute(
                "INSERT INTO identities VALUES (?, 'assertion', ?)", (value["assertion_id"], b"")
            )
        elif fault in {"domain", "signature", "active-object"}:
            signature = _identity_signature(value)
            db.execute(
                "INSERT INTO identities VALUES (?,?,?)",
                (
                    value["id"],
                    "entry" if fault == "domain" else "referent",
                    signature + b"x" if fault == "signature" else signature,
                ),
            )
            if fault == "active-object":
                db.execute(
                    "INSERT INTO assertions VALUES (?,?,?,?,?,?,?,?,1)",
                    (
                        identity(900),
                        value["id"],
                        "agents",
                        "agent",
                        None,
                        None,
                        "0",
                        canonical_json_bytes(value),
                    ),
                )
        db.commit()
        current = work(db, source)
        current.initialize()
        before = contents(db)
        with pytest.raises(ProvenanceValidationError, match=message):
            current.advance()
        assert contents(db) == before
        assert not db.in_transaction


@pytest.mark.parametrize("budget", [1, 2, 64])
@pytest.mark.parametrize(
    "fault,message",
    [
        ("assertion", "cannot be reused"),
        ("object", "duplicate effective"),
        ("domain", "cannot be reused"),
    ],
)
def test_duplicates_across_source_pages_keep_native_errors(tmp_path, budget, fault, message):
    path = tmp_path / "duplicate.sqlite3"
    create(path)
    first, second = row(1), row(2)
    if fault == "assertion":
        second["assertion_id"] = first["assertion_id"]
    elif fault == "object":
        second["id"] = first["id"]
    else:
        second["assertion_id"] = first["id"]
    source = frame([first, second])
    with closing(connect(path)) as db:
        current = work(db, source)
        current.initialize()
        original_prefix = db.execute("SELECT * FROM journal_prefix").fetchall()
        with pytest.raises(ProvenanceValidationError, match=message):
            while not current.advance(row_budget=budget).done:
                pass
        assert current.load().position == (1 if budget == 1 else 0)
        assert db.execute("SELECT * FROM journal_prefix").fetchall() == original_prefix


def test_in_array_order_is_preserved_instead_of_sorting_assertion_uuids(tmp_path):
    path = tmp_path / "diagnostic.sqlite3"
    create(path)
    first, later = row(20), row(10)
    with closing(connect(path)) as db:
        db.execute("INSERT INTO identities VALUES (?, 'referent', ?)", (first["id"], b"changed"))
        db.execute(
            "INSERT INTO identities VALUES (?, 'assertion', ?)", (later["assertion_id"], b"")
        )
        db.commit()
        current = work(db, frame([first, later]))
        current.initialize()
        with pytest.raises(ProvenanceValidationError, match="immutable referent"):
            current.advance()


def test_retired_object_can_be_reasserted_with_its_exact_immutable_signature(tmp_path):
    path = tmp_path / "retired.sqlite3"
    create(path)
    value = row(1)
    with closing(connect(path)) as db:
        db.execute(
            "INSERT INTO identities VALUES (?, 'referent', ?)",
            (value["id"], _identity_signature(value)),
        )
        db.execute(
            "INSERT INTO assertions VALUES (?,?,?,?,?,?,?,?,0)",
            (
                identity(900),
                value["id"],
                "agents",
                "agent",
                None,
                None,
                "0",
                canonical_json_bytes(value),
            ),
        )
        db.commit()
        current = work(db, frame([value]))
        current.initialize()
        assert current.advance().done
        assert db.execute("SELECT assertion_id FROM assertions WHERE active=1").fetchall() == [
            (value["assertion_id"],)
        ]


def test_source_byte_boundary_publishes_only_preceding_admitted_rows(tmp_path):
    path = tmp_path / "bytes.sqlite3"
    create(path)
    first, second = row(1), row(2, 65537)
    source = frame([first, second])
    with closing(connect(path)) as db:
        current = work(db, source)
        current.initialize()
        assert current.advance(byte_budget=proposed_octets("agents", first)).position == 1
        before = contents(db)
        with pytest.raises(AssertionStagingCapacity, match="source atom"):
            current.advance(byte_budget=65536)
        assert contents(db) == before
        assert current.advance(byte_budget=200000).done


def test_existing_immutable_value_is_admitted_before_comparison(tmp_path):
    path = tmp_path / "signature-bytes.sqlite3"
    create(path)
    value = row(1)
    with closing(connect(path)) as db:
        db.execute("INSERT INTO identities VALUES (?, 'referent', ?)", (value["id"], b"x" * 65537))
        db.commit()
        current = work(db, frame([value]))
        current.initialize()
        before = contents(db)
        with pytest.raises(AssertionStagingCapacity, match="signature atom"):
            current.advance(byte_budget=65536)
        assert contents(db) == before
        with pytest.raises(ProvenanceValidationError, match="immutable referent"):
            current.advance(byte_budget=200000)
        assert contents(db) == before


@pytest.mark.parametrize("field", ["attempt_id", "frame", "catalog_sha256", "require_profiles"])
def test_exact_source_catalog_mode_and_attempt_fence_restoration(tmp_path, field):
    path = tmp_path / "binding.sqlite3"
    create(path)
    source = frame([row(1)])
    changed = {
        "attempt_id": "replacement",
        "frame": frame([row(2)]),
        "catalog_sha256": "b" * 64,
        "require_profiles": True,
    }
    with closing(connect(path)) as db:
        work(db, source).initialize()
        before = contents(db)
        options = {field: changed[field]}
        if field == "frame":
            replacement = work(db, changed[field])
        else:
            replacement = work(db, source, **options)
        with pytest.raises(StaleAssertionStaging):
            replacement.advance()
        assert contents(db) == before


@pytest.mark.parametrize(
    "field,value",
    [
        ("attempt_id", "replacement"),
        ("binding", b"replacement"),
        ("checkpoint", "replaced-checkpoint"),
    ],
)
def test_accepted_prefix_replacement_fences_prepared_writes(tmp_path, field, value):
    path = tmp_path / "prefix.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        current = work(db, frame([row(1)]))
        current.initialize()
        batch = current.prepare()
        db.execute("UPDATE journal_prefix SET " + field + "=?", (value,))
        db.commit()
        before = contents(db)
        with pytest.raises(StaleAssertionStaging):
            current.commit(batch)
        assert contents(db) == before


def test_competing_connections_and_finished_proposals_are_fenced(tmp_path):
    path = tmp_path / "workers.sqlite3"
    create(path)
    source = frame([row(1), row(2)])
    with closing(connect(path)) as owner, closing(connect(path)) as stale:
        current = work(owner, source)
        current.initialize()
        batch = work(stale, source).prepare(row_budget=1)
        current.advance(row_budget=1)
        before = contents(owner)
        with pytest.raises(StaleAssertionStaging):
            work(stale, source).commit(batch)
        assert contents(owner) == before
        assert current.advance().done
        finished = current.prepare()
        owner.execute("DELETE FROM changes")
        owner.commit()
        work(stale, frame([row(3)]), attempt_id="replacement").initialize()
        before = contents(owner)
        for action in (current.load, lambda: current.commit(finished)):
            with pytest.raises(StaleAssertionStaging):
                action()
            assert contents(owner) == before


def test_late_statement_failure_rolls_back_identities_assertions_changes_and_cursor(tmp_path):
    path = tmp_path / "rollback.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        current = work(db, frame([row(1), row(2)]))
        current.initialize()
        db.execute(
            "CREATE TRIGGER fail_second BEFORE INSERT ON assertions WHEN NEW.assertion_id='"
            + row(2)["assertion_id"]
            + "' BEGIN SELECT RAISE(ABORT,'late failure'); END"
        )
        db.commit()
        before = contents(db)
        with pytest.raises(ProvenanceValidationError, match="duplicate effective"):
            current.advance(row_budget=2)
        assert contents(db) == before
        assert not db.in_transaction


@pytest.mark.parametrize(
    "damage",
    [
        "algorithm",
        "count",
        "checkpoint",
        "position",
        "assertion",
        "description",
        "description-value",
        "change",
        "identity",
        "referent",
        "active",
    ],
)
def test_corrupt_or_missing_checkpoint_links_fail_without_reset(tmp_path, damage):
    path = tmp_path / "corrupt.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        current = work(db, frame([row(1), row(2)]))
        current.initialize()
        state = current.advance(row_budget=1)
        if damage == "algorithm":
            db.execute("UPDATE journal_assertion_staging SET algorithm='unknown'")
        elif damage == "count":
            db.execute("UPDATE journal_assertion_staging SET row_count=7")
        elif damage == "checkpoint":
            db.execute("UPDATE journal_assertion_staging SET checkpoint='{}'")
        elif damage == "position":
            db.execute(
                "UPDATE journal_assertion_staging SET checkpoint=?",
                (replace(state, position=9).to_json(),),
            )
        elif damage == "active":
            db.execute("UPDATE assertions SET active=0")
        elif damage == "description":
            db.execute("DELETE FROM description_projection")
        elif damage == "description-value":
            db.execute("UPDATE description_projection SET kind='person'")
        elif damage in {"assertion", "change"}:
            db.execute("DELETE FROM " + ("assertions" if damage == "assertion" else "changes"))
        else:
            db.execute(
                "DELETE FROM identities WHERE id=?",
                (row(1)["assertion_id"] if damage == "identity" else row(1)["id"],),
            )
        db.commit()
        before = contents(db)
        with pytest.raises(RuntimeError):
            current.advance()
        assert contents(db) == before


def test_ordinary_work_versions_use_decimal_text_without_a_machine_integer_ceiling(tmp_path):
    path = tmp_path / "version.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        current = work(db, frame([row(1)]))
        state = current.initialize()
        db.execute(
            "UPDATE journal_assertion_staging SET checkpoint=?",
            (replace(state, version=2**200).to_json(),),
        )
        db.commit()
        assert current.advance().version == 2**200 + 1


@pytest.mark.parametrize(
    "name,value",
    [
        ("row_budget", 0),
        ("row_budget", 65),
        ("row_budget", True),
        ("byte_budget", 0),
        ("byte_budget", True),
    ],
)
def test_explicit_work_budget_validation_leaves_state_unchanged(tmp_path, name, value):
    path = tmp_path / "budget.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        current = work(db, frame([row(1)]))
        current.initialize()
        before = contents(db)
        with pytest.raises(ValueError):
            current.advance(**{name: value})
        assert contents(db) == before


def test_native_correction_uses_staging_without_early_prefix_publication(
    journal, who, catalog, monkeypatch
):
    observed = []
    original = AssertionStaging.advance

    def advance(self, **options):
        prefix = self.db.execute("SELECT * FROM journal_prefix").fetchall()
        before = self.load()
        state = original(self, row_budget=1, byte_budget=options["byte_budget"])
        assert self.db.execute("SELECT * FROM journal_prefix").fetchall() == prefix
        assert state.position - before.position <= 1
        assert not self.db.in_transaction
        observed.append(self.sequence)
        return state

    monkeypatch.setattr(AssertionStaging, "advance", advance)
    with validate_journal(journal, catalog=catalog) as old:
        agent = copy.deepcopy(old.graph["agents"][0])
        retired = agent["assertion_id"]
        reference = assertion_reference(old, retired)
    agent["assertion_id"] = new_id()
    agent["name"] = "Corrected label"
    raw = append_correction(
        journal,
        [reference],
        reason="label",
        assertions={"agents": [agent]},
        recorded_by_agent_id=who,
        catalog=catalog,
    )
    with validate_journal(raw, catalog=catalog) as summary:
        assert summary.journal_sha256 == hashlib.sha256(raw).hexdigest()
        assert retired in summary.retracted_assertion_ids
        assert summary.graph["agents"][0]["name"] == "Corrected label"
        assert [item.encoded for item in summary.frames] == [
            item.encoded for item in parse_journal(raw)
        ]
    assert {"0", "1"} <= set(observed)


CRASH_PROGRAM = r"""
import os,sqlite3,sys
from riverhog_provenance._assertion_staging import AssertionStaging
from riverhog_provenance.journal import JournalFrame
class Connection(sqlite3.Connection):
    armed=False
    def execute(self, sql, parameters=()):
        result=super().execute(sql,parameters)
        for seam,prefix in (('header','UPDATE journal_assertion_staging'),
                            ('identity','INSERT INTO identities'),
                            ('referent','INSERT OR IGNORE INTO identities'),
                            ('assertion','INSERT INTO assertions'),
                            ('description','INSERT INTO description_projection'),
                            ('change','INSERT INTO changes')):
            if self.armed and sys.argv[3]==seam and sql.startswith(prefix): os._exit(73)
        return result
    def commit(self):
        if self.armed and sys.argv[3]=='before-commit': os._exit(73)
        super().commit()
        if self.armed and sys.argv[3]=='after-commit': os._exit(73)
db=sqlite3.connect(sys.argv[1],factory=Connection)
db.execute('PRAGMA synchronous=FULL').close()
source=JournalFrame(open(sys.argv[2],'rb').read())
current=AssertionStaging(db,attempt_id='owned-staging-attempt',frame=source,
                         catalog_sha256='a'*64,require_profiles=False)
batch=current.prepare(row_budget=2)
if sys.argv[3]=='prepared': os._exit(73)
db.armed=True
current.commit(batch)
raise SystemExit(99)
"""


@pytest.mark.parametrize(
    "seam",
    [
        "prepared",
        "header",
        "identity",
        "referent",
        "assertion",
        "description",
        "change",
        "before-commit",
        "after-commit",
    ],
)
@pytest.mark.parametrize("resumed", [False, True])
def test_real_process_death_preserves_one_atomic_staging_unit(tmp_path, seam, resumed):
    path = tmp_path / "kill.sqlite3"
    create(path)
    source = frame([row(1), row(2), row(3)])
    source_path = tmp_path / "admitted.json"
    source_path.write_bytes(source.json_bytes)
    with closing(connect(path)) as db:
        current = work(db, source)
        current.initialize()
        if resumed:
            current.advance(row_budget=1)
        batch = current.prepare(row_budget=2)
        before = contents(db)
        current.commit(batch)
        expected = contents(db)
        for value in batch.rows:
            db.execute("DELETE FROM changes WHERE assertion_id=?", (value.assertion_id,))
            db.execute(
                "DELETE FROM description_projection WHERE assertion_id=?", (value.assertion_id,)
            )
            db.execute("DELETE FROM assertions WHERE assertion_id=?", (value.assertion_id,))
            db.execute(
                "DELETE FROM identities WHERE id IN (?,?)", (value.assertion_id, value.object_id)
            )
        db.execute("UPDATE journal_assertion_staging SET checkpoint=?", (batch.before_json,))
        db.commit()
        assert contents(db) == before
    result = subprocess.run(
        [sys.executable, "-c", CRASH_PROGRAM, str(path), str(source_path), seam],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 73, result.stderr
    with closing(connect(path)) as db:
        assert db.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        assert contents(db) == (expected if seam == "after-commit" else before)
        current = work(db, source)
        while not current.advance(row_budget=1).done:
            pass


class WorkCursor(sqlite3.Cursor):
    closed = False

    def record(self, value):
        if value is not None:
            self.connection.metrics["returned_rows"] += 1
            self.connection.metrics["metadata_octets"] += logical_octets(value)
            if self.sql.startswith("SELECT domain,length(value)"):
                self.connection.prior_sizes[self.parameters[0]] = value[1]
                self.connection.metrics["inspected_octets"] += value[1]
            if "FROM identities" in self.sql:
                self.connection.metrics["historical_payload_fetched_octets"] += sum(
                    len(item) for item in value if isinstance(item, bytes)
                )
        return value

    def fetchone(self):
        return self.record(super().fetchone())

    def close(self):
        self.closed = True
        return super().close()


class WorkConnection(sqlite3.Connection):
    recording = False

    def execute(self, sql, parameters=()):
        if not self.recording:
            return super().execute(sql, parameters)
        cursor = self.cursor(factory=WorkCursor)
        cursor.sql, cursor.parameters = sql, parameters
        self.statements.append((sql, parameters, cursor))
        cursor.execute(sql, parameters)
        octets = 0
        if sql.startswith("UPDATE journal_assertion_staging"):
            self.metrics["written_value_octets"] += len(parameters[0].encode())
        elif sql.startswith("INSERT"):
            octets = logical_octets(parameters)
            if "INTO identities" in sql:
                octets += len("referent" if "referent" in sql else "assertion")
            elif "INTO assertions" in sql:
                octets += 1
            self.metrics["admitted_octets"] += octets
            if "INTO observation_observers" in sql:
                self.metrics["inspected_octets"] += octets
            if cursor.rowcount:
                self.metrics["written_value_octets"] += octets
        elif sql.startswith("UPDATE observation_projection"):
            octets = logical_octets(parameters) + 1
            self.metrics["admitted_octets"] += octets
            self.metrics["written_value_octets"] += octets
        elif sql.startswith("SELECT value=?"):
            self.metrics["admitted_octets"] += self.prior_sizes[parameters[1]]
        return cursor

    @contextmanager
    def observe(self):
        self.metrics = {
            name: 0
            for name in (
                "sql_statements",
                "returned_rows",
                "changed_rows",
                "metadata_octets",
                "admitted_octets",
                "inspected_octets",
                "historical_payload_fetched_octets",
                "written_value_octets",
                "sqlite_vm_instructions",
            )
        }
        self.statements, self.prior_sizes = [], {}
        before = self.total_changes
        canonical, signature = implementation.canonical_document, journal._identity_signature

        def document(value):
            encoded = canonical(value)
            self.metrics["inspected_octets"] += len(encoded)
            return encoded

        def immutable(value):
            encoded = signature(value)
            self.metrics["inspected_octets"] += len(encoded)
            return encoded

        def trace(_sql):
            self.metrics["sql_statements"] += 1

        def tick():
            self.metrics["sqlite_vm_instructions"] += 1
            return 0

        def authorize(action, table, column, _database, _trigger):
            return (
                sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_READ
                and table in {"frames", "assertions"}
                and column == "value"
                else sqlite3.SQLITE_OK
            )

        implementation.canonical_document, journal._identity_signature = document, immutable
        self.set_trace_callback(trace)
        self.set_progress_handler(tick, 1)
        self.set_authorizer(authorize)
        self.recording = True
        try:
            yield self.metrics
        finally:
            self.recording = False
            implementation.canonical_document, journal._identity_signature = canonical, signature
            self.set_trace_callback(None)
            self.set_progress_handler(None, 0)
            self.set_authorizer(None)
            self.metrics["changed_rows"] = self.total_changes - before
        assert not self.in_transaction
        assert all(cursor.closed for _sql, _parameters, cursor in self.statements)
        for sql, parameters, _cursor in self.statements:
            if not sql.startswith(("SELECT", "UPDATE")):
                continue
            with closing(self.execute("EXPLAIN QUERY PLAN " + sql, parameters)) as cursor:
                plan = " ".join(item[3] for item in cursor.fetchall())
            assert "SEARCH" in plan and "SCAN" not in plan and "USE TEMP" not in plan, (sql, plan)


@pytest.mark.growth_witness(
    "riverhog-assertion-staging-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._assertion_staging.AssertionStaging.advance",
)
def test_actual_cold_indexed_staging_rows_bytes_and_vm_work(tmp_path, record_property):
    binding = growth_binding(AssertionStaging.advance)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    samples = []
    regimes = [
        (0, 0, 64, 0, 0, False),
        (1, 0, 1, 0, 0, False),
        (15, 0, 16, 0, 0, False),
        (16, 0, 16, 0, 0, False),
        (17, 0, 16, 0, 0, False),
        (129, 0, 64, 1, 0, False),
        (1025, 0, 64, 17, 0, False),
        (17, 4097, 16, 0, 0, False),
        (17, 0, 64, 0, 65537, False),
        (17, 0, 16, 0, 0, True),
        (17, 4097, 16, 1, 0, True),
        (17, 0, 16, 0, 65537, True),
    ]
    activity_start = len(regimes)
    regimes.extend(
        [
            (0, 0, 16, 0, 0, False),
            (1, 0, 1, 0, 0, False),
            (15, 0, 16, 0, 0, False),
            (16, 0, 16, 0, 0, False),
            (17, 0, 16, 1, 0, False),
            (129, 0, 64, 17, 0, False),
            (17, 4097, 16, 0, 0, False),
            (17, 0, 16, 0, 65537, False),
        ]
    )
    binding_start = len(regimes)
    regimes.extend(regimes[activity_start:binding_start])
    context_start = len(regimes)
    regimes.extend(regimes[activity_start:binding_start])
    observation_start = len(regimes)
    regimes.extend(regimes[activity_start:binding_start])
    observation_template = BoundedSourceObserver().observe(BytesSource(b"metadata witness"))
    observation_template = next(
        value
        for value in observation_template.graph_fragment()["descriptions"]
        if value["type"] == "observation"
    )
    for terminal in (False, True):
        for index, (count, history, quantum, resumed, payload, existing) in enumerate(regimes):
            path = tmp_path / f"{terminal}-{index}.sqlite3"
            create(path)
            values = [row(n, payload) for n in range(count)]
            category = "agents"
            if index >= observation_start:
                category = "descriptions"
                values = [
                    {
                        **copy.deepcopy(observation_template),
                        "id": value["id"],
                        "assertion_id": value["assertion_id"],
                        "note": "x" * (payload or 1),
                    }
                    for value in values
                ]
            elif index >= context_start:
                category = "contexts"
                values = [
                    {
                        "id": value["id"],
                        "assertion_id": value["assertion_id"],
                        "type": "context",
                        "evidence": [],
                        "kind": "execution_environment" if ordinal % 2 else "delivery",
                        "label": "x" * (payload or 1),
                    }
                    for ordinal, value in enumerate(values)
                ]
            elif index >= binding_start:
                category = "locator_bindings"
                values = [
                    assertion(
                        "locator_binding",
                        identity(9000000),
                        object_id=value["id"],
                        assertion_id=value["assertion_id"],
                        target=reference(identity(9000001), "state"),
                        context_id=identity(9000002),
                        locator={
                            "kind": "opaque",
                            "value": {"kind": "text", "text": "x" * (payload or 1)},
                        },
                        temporal_scope=(
                            {"kind": "instant", "at": "0001-01-01T00:00:00Z"}
                            if ordinal % 2
                            else {"kind": "interval", "end": "9999-12-31T23:59:59.999999999Z"}
                        ),
                    )
                    for ordinal, value in enumerate(values)
                ]
            elif index >= activity_start:
                category = "activities"
                for value in values:
                    value.update(
                        type="activity",
                        kind="materialization",
                        started_at="0001-01-01T00:00:00Z",
                        ended_at="9999-12-31T23:59:59.999999999Z",
                    )
            if existing and payload:
                category = "extensions"
                values = [
                    assertion(
                        "extension",
                        identity(9000000),
                        object_id=value["id"],
                        assertion_id=value["assertion_id"],
                        subject=reference(identity(9000001), "artifact"),
                        property="urn:test:property",
                        value={"type": "text", "value": "x" * payload},
                    )
                    for value in values
                ]
            source = frame(values)
            with closing(connect(path)) as db:
                for n in range(history):
                    prior = row(20000 + n)
                    db.execute(
                        "INSERT INTO identities VALUES (?, 'referent', ?)",
                        (prior["id"], _identity_signature(prior)),
                    )
                    db.execute(
                        "INSERT INTO assertions VALUES (?,?,?,?,?,?,?,?,0)",
                        (
                            prior["assertion_id"],
                            prior["id"],
                            "agents",
                            "agent",
                            None,
                            None,
                            "0",
                            canonical_json_bytes(prior),
                        ),
                    )
                if existing:
                    for value in values:
                        db.execute(
                            "INSERT INTO identities VALUES (?, 'referent', ?)",
                            (value["id"], _identity_signature(value)),
                        )
                db.commit()
                current = work(db, source)
                current.initialize()
                if terminal:
                    while not current.advance(byte_budget=4000000).done:
                        pass
                else:
                    for _ in range(resumed):
                        current.advance(row_budget=1)
                retained = sum(
                    db.execute("SELECT count(*) FROM " + name).fetchone()[0]
                    for name in (
                        "identities",
                        "assertions",
                        "changes",
                        "activity_projection",
                        "binding_projection",
                        "delivery_projection",
                        "description_projection",
                        "observation_projection",
                        "observation_observers",
                    )
                )
                maximum = max(
                    (
                        proposed_octets(category, value)
                        + (len(_identity_signature(value)) if existing else 0)
                        for value in values
                    ),
                    default=0,
                )
            with closing(connect(path, factory=WorkConnection)) as db:
                current = work(db, source)
                admission = (
                    220000
                    if payload and (existing or index >= activity_start)
                    else 100000
                    if payload
                    else 65536
                )
                with db.observe() as metrics:
                    current.advance(row_budget=quantum, byte_budget=admission)
                sample = {
                    "declaration": binding.declaration.id,
                    "declaration_sha256": digest,
                    "dimensions": {
                        "retained_rows": retained,
                        "row_budget": quantum,
                        "byte_budget": admission,
                        "max_atom_octets": maximum,
                    },
                    "metrics": {
                        bound.metric: metrics[bound.metric] for bound in binding.declaration.bounds
                    },
                    "delegates": {},
                }
                validate_execution_samples(projection, binding.declaration.id, [sample])
                samples.append(sample)
    from test_observation_staging import growth_samples as observer_growth_samples

    samples.extend(observer_growth_samples(tmp_path))
    from test_edge_row_semantics import staging_growth_samples

    samples.extend(staging_growth_samples(tmp_path))
    assert len(samples) == 142
    record_property("growth_samples", samples)


@pytest.mark.parametrize("completed", [False, True])
def test_explicit_initialization_cannot_replace_the_same_attempt(tmp_path, completed):
    path = tmp_path / "initialize.sqlite3"
    create(path)
    source = frame([row(1), row(2)])
    with closing(connect(path)) as db:
        current = work(db, source)
        current.initialize()
        if completed:
            assert current.advance().done
            db.execute("DELETE FROM changes")
            db.commit()
        before = contents(db)
        with pytest.raises(StaleAssertionStaging):
            current.initialize()
        assert contents(db) == before
        if not completed:
            with pytest.raises(StaleAssertionStaging):
                work(db, source, attempt_id="replacement").initialize()
            assert contents(db) == before


def test_admission_requires_the_previous_change_queue_to_be_retired(tmp_path):
    path = tmp_path / "previous-changes.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        db.execute("INSERT INTO changes VALUES (?)", (identity(500),))
        db.commit()
        before = contents(db)
        with pytest.raises(RuntimeError, match="retired change queue"):
            work(db, frame([row(1)])).initialize()
        assert contents(db) == before
