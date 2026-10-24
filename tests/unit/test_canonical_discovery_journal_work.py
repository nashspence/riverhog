from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
from dataclasses import replace
from uuid import uuid4

import pytest
from riverhog_core.canonical_discovery_declared_source_work import (
    DeclaredAssertionSource,
    prepare_declared_source_page,
)
from riverhog_core.canonical_discovery_fact_work import (
    FactTaskKey,
    StaleFactTask,
    fact_extraction_cursor,
    finish_assertion,
)
from riverhog_core.canonical_discovery_journal_work import (
    JournalTaskKey,
    QualifiedJournalSource,
    StaleJournalTask,
    acknowledge_assertion,
    apply_journal_page,
    begin_journal,
    finish_journal,
    initialize_journal_work,
    journal_progress,
    prepare_journal_page,
)
from riverhog_core.canonical_discovery_source_work import (
    AdmittedAssertionSource,
    apply_source_page,
    prepare_lexical_source_page,
)
from riverhog_provenance import (
    append_checkpoint,
    append_correction,
    create_journal,
    validate_journal,
)

from tests.support.canonical_discovery import native_scalar_fixture
from tests.support.canonical_discovery_work import RecordedConnection, write

ADMISSION = hashlib.sha256(b"verified archive admission").hexdigest()


def open_work(path):
    db = sqlite3.connect(path, isolation_level=None)
    initialize_journal_work(db)
    return db


def bind(db, source):
    key = JournalTaskKey("build", source.source_key, "worker")
    with write(db):
        begin_journal(db, key, source)
    return key


def drive(db, key, source, budget):
    while journal_progress(db, key).cursor.phase != "done":
        page = prepare_journal_page(db, key, source, action_budget=budget)
        assert 1 <= len(page.actions) <= budget
        with write(db):
            apply_journal_page(db, key, source, page)


def finish_child(db, key, source, assertion_id, budget=128):
    row = db.execute(
        "SELECT born_sequence,source_group,source_item,task_id,origin_entry_json "
        "FROM discovery_journal_assertions WHERE build_id=? AND source_key=? AND assertion_id=?",
        (key.build_id, key.source_key, assertion_id),
    ).fetchone()
    assertion = source.assertion(int(row[0]), row[1], row[2])
    child = AdmittedAssertionSource(source.source_key, json.loads(row[4])["json_sha256"], assertion)
    child_key = FactTaskKey(key.build_id, row[3], key.source_key, key.worker_id)
    declared = DeclaredAssertionSource(child)
    while (cursor := fact_extraction_cursor(db, child_key)[1]).category != "done":
        if cursor.category in {"canonical", "scalars"}:
            page = prepare_lexical_source_page(db, child_key, child, action_budget=budget)
        else:
            page = prepare_declared_source_page(db, child_key, declared, action_budget=budget)
        with write(db):
            apply_source_page(db, child_key, page)
    with write(db):
        finish_assertion(
            db,
            child_key,
            source_terminal=child.canonical_sha256,
            expected_version=fact_extraction_cursor(db, child_key)[0].version,
        )


def corrected_fixture():
    fixture = native_scalar_fixture(3)
    with validate_journal(fixture.raw, catalog=fixture.catalog, require_profiles=True) as summary:
        target = {"entry": summary.anchor_at(0)["through"], "assertion_id": fixture.assertion_id}
        who = summary.frames[0].document["recorded_by_agent_id"]
    raw = append_correction(
        fixture.raw,
        [target],
        reason="fixture retirement",
        recorded_by_agent_id=who,
        catalog=fixture.catalog,
    )
    raw = append_checkpoint(
        raw,
        recorded_by_agent_id=who,
        purpose="urn:test:fixture:checkpoint",
        catalog=fixture.catalog,
    )
    return fixture, raw, target


@pytest.mark.parametrize("budget", [1, 7, 128])
def test_native_intervals_child_terminals_and_exact_retries(tmp_path, budget):
    fixture, raw, target = corrected_fixture()
    db = open_work(tmp_path / "work.sqlite")
    with validate_journal(raw, catalog=fixture.catalog, require_profiles=True) as summary:
        with QualifiedJournalSource(summary, ADMISSION) as source:
            key = bind(db, source)
            with write(db):
                assert begin_journal(db, key, source) == journal_progress(db, key)
            drive(db, key, source, budget)
            progress = journal_progress(db, key)
            assert progress.cursor.sequence == 3
            assert progress.assertions == 7
            assert progress.retractions == 1
            assert progress.pending == 7
            with write(db), pytest.raises(StaleJournalTask, match="incomplete"):
                finish_journal(db, key, expected_version=progress.version)
            with write(db), pytest.raises(StaleFactTask, match="completion"):
                acknowledge_assertion(db, key, fixture.assertion_id)
            actual = db.execute(
                "SELECT origin_entry_json,born_sequence,retired_sequence "
                "FROM discovery_journal_assertions WHERE assertion_id=?",
                (fixture.assertion_id,),
            ).fetchone()
            assert json.loads(actual[0]) == target["entry"]
            assert actual[1:] == ("0", "1")
            assert int(actual[1]) <= 0 < int(actual[2])
            assert not (int(actual[1]) <= 1 < int(actual[2]))
            ids = [
                row[0]
                for row in db.execute(
                    "SELECT assertion_id FROM discovery_journal_assertions"
                ).fetchall()
            ]
            for assertion_id in ids:
                finish_child(db, key, source, assertion_id)
                with write(db):
                    acknowledged = acknowledge_assertion(db, key, assertion_id)
                    assert acknowledge_assertion(db, key, assertion_id) == acknowledged
            progress = journal_progress(db, key)
            assert progress.pending == 0
            with write(db):
                completed = finish_journal(db, key, expected_version=progress.version)
                assert completed.complete
                assert finish_journal(db, key, expected_version=progress.version) == completed
    db.close()


def test_page_failure_rolls_back_child_and_parent_together(tmp_path):
    fixture = native_scalar_fixture(1)
    db = open_work(tmp_path / "work.sqlite")
    with validate_journal(fixture.raw, catalog=fixture.catalog) as summary:
        with QualifiedJournalSource(summary, ADMISSION) as source:
            key = bind(db, source)
            before = journal_progress(db, key)
            page = prepare_journal_page(db, key, source, action_budget=128)
            db.execute(
                "CREATE TRIGGER fail_child BEFORE INSERT ON discovery_journal_assertions "
                "BEGIN SELECT RAISE(ABORT,'injected child failure'); END"
            )
            with write(db), pytest.raises(sqlite3.IntegrityError, match="injected"):
                apply_journal_page(db, key, source, page)
            assert journal_progress(db, key) == before
            assert db.execute("SELECT count(*) FROM discovery_fact_tasks").fetchone() == (0,)
            assert db.execute("SELECT count(*) FROM discovery_journal_assertions").fetchone() == (
                0,
            )
            db.execute("DROP TRIGGER fail_child")
            with write(db):
                apply_journal_page(db, key, source, page)
            with write(db), pytest.raises(StaleJournalTask, match="version"):
                apply_journal_page(db, key, source, page)
    db.close()


def test_reopen_resumes_direct_positions_and_preserves_raw_retirement(tmp_path):
    fixture, raw, _ = corrected_fixture()
    path = tmp_path / "work.sqlite"
    db = open_work(path)
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        with QualifiedJournalSource(summary, ADMISSION) as source:
            key = bind(db, source)
            for _ in range(4):
                page = prepare_journal_page(db, key, source, action_budget=1)
                with write(db):
                    apply_journal_page(db, key, source, page)
            before = journal_progress(db, key)
    db.close()
    db = open_work(path)
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        with QualifiedJournalSource(summary, ADMISSION) as source:
            assert bind(db, source) == key
            assert journal_progress(db, key) == before
            drive(db, key, source, 1)
            assert journal_progress(db, key).assertions == 7
            assert db.execute(
                "SELECT retired_sequence FROM discovery_journal_assertions WHERE assertion_id=?",
                (fixture.assertion_id,),
            ).fetchone() == ("1",)
    db.close()


def test_source_prefix_admission_worker_and_prepared_page_fences(tmp_path):
    fixture, raw, _ = corrected_fixture()
    db = open_work(tmp_path / "work.sqlite")
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        with QualifiedJournalSource(summary, ADMISSION) as source:
            with QualifiedJournalSource(summary, ADMISSION, through=0) as prefix:
                with QualifiedJournalSource(summary, "b" * 64) as other_admission:
                    assert (
                        len({source.source_key, prefix.source_key, other_admission.source_key}) == 3
                    )
            key = bind(db, source)
            with write(db), pytest.raises(StaleJournalTask, match="registration"):
                begin_journal(db, replace(key, worker_id="replacement"), source)
            with pytest.raises(StaleJournalTask, match="worker"):
                prepare_journal_page(
                    db, replace(key, worker_id="replacement"), source, action_budget=1
                )
            with write(db), pytest.raises(ValueError, match="no active"):
                prepare_journal_page(db, key, source, action_budget=1)
            page = prepare_journal_page(db, key, source, action_budget=128)
            forged = replace(page, actions=page.actions[1:])
            with write(db), pytest.raises(StaleJournalTask, match="current detached"):
                apply_journal_page(db, key, source, forged)
            with write(db):
                apply_journal_page(db, key, source, page)
            source.close()
            with pytest.raises(StaleJournalTask, match="closed"):
                prepare_journal_page(db, key, source, action_budget=1)
    db.close()


def test_independent_lease_survives_original_close(tmp_path):
    fixture = native_scalar_fixture(0)
    db = open_work(tmp_path / "work.sqlite")
    summary = validate_journal(fixture.raw, catalog=fixture.catalog)
    with QualifiedJournalSource(summary, ADMISSION) as source:
        summary.close()
        key = bind(db, source)
        drive(db, key, source, 7)
        assert journal_progress(db, key).assertions == 7
    with pytest.raises(RuntimeError):
        QualifiedJournalSource(summary, ADMISSION)
    db.close()


@pytest.mark.parametrize("damage", ["position", "predecessor", "retirement"])
def test_source_coverage_rejects_omissions_and_changed_origin(tmp_path, damage):
    fixture, raw, _ = corrected_fixture()
    db = open_work(tmp_path / "work.sqlite")
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        with QualifiedJournalSource(summary, ADMISSION) as source:
            key = bind(db, source)
            if damage == "position":
                db.execute("UPDATE discovery_journal_tasks SET phase='end'")
                with pytest.raises(StaleJournalTask, match="skipped"):
                    prepare_journal_page(db, key, source, action_budget=128)
            else:
                page = prepare_journal_page(db, key, source, action_budget=128)
                with write(db):
                    apply_journal_page(db, key, source, page)
                if damage == "predecessor":
                    db.execute("UPDATE discovery_journal_tasks SET last_entry_json='{}'")
                else:
                    db.execute(
                        "UPDATE discovery_journal_assertions SET origin_entry_json='{}' "
                        "WHERE assertion_id=?",
                        (fixture.assertion_id,),
                    )
                before = journal_progress(db, key)
                page = prepare_journal_page(db, key, source, action_budget=128)
                with write(db), pytest.raises(StaleJournalTask):
                    apply_journal_page(db, key, source, page)
                assert journal_progress(db, key) == before
    db.close()


@pytest.mark.parametrize("width", [1, 17, 257])
def test_wide_native_array_uses_direct_items_without_reencoding_completed_rows(
    tmp_path, monkeypatch, width
):
    import riverhog_core.canonical_discovery_journal_work as work

    fixture = native_scalar_fixture(1)
    with validate_journal(fixture.raw, catalog=fixture.catalog) as original:
        document = original.frames[0].document
        graph = copy.deepcopy(document["body"]["assertions"])
        template = graph["extensions"][0]
        for _ in range(width - 1):
            row = copy.deepcopy(template)
            row["id"], row["assertion_id"] = f"urn:uuid:{uuid4()}", f"urn:uuid:{uuid4()}"
            graph["extensions"].append(row)
        raw = create_journal(
            graph, recorded_by_agent_id=document["recorded_by_agent_id"], catalog=fixture.catalog
        )
    encoded_assertions = []
    original_encode = work.canonical_json_bytes

    def encode(value):
        assert not db.in_transaction
        if isinstance(value, dict) and "assertion_id" in value and "type" in value:
            encoded_assertions.append(value["assertion_id"])
        return original_encode(value)

    db = open_work(tmp_path / "work.sqlite")
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        with QualifiedJournalSource(summary, ADMISSION) as source:
            key = bind(db, source)
            monkeypatch.setattr(work, "canonical_json_bytes", encode)
            drive(db, key, source, 1)
            assert len(encoded_assertions) == width + 6
            assert len(set(encoded_assertions)) == width + 6
    db.close()


@pytest.mark.growth_witness(
    "canonical-discovery-journal-page-preparation/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_work.prepare_journal_page",
)
def test_measured_journal_pages_bound_core_reads_and_actions(tmp_path, record_property):
    from riverhog_canonical_json import canonical_json_bytes
    from riverhog_growth_contracts import (
        growth_binding,
        project_bindings,
        validate_execution_samples,
    )

    binding = growth_binding(prepare_journal_page)
    projection = project_bindings((binding,))
    declaration_sha = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    samples = []
    fixture, raw, _ = corrected_fixture()
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        for budget in (1, 7, 128):
            db = open_work(tmp_path / f"budget-{budget}.sqlite")
            with QualifiedJournalSource(summary, ADMISSION) as source:
                key = bind(db, source)
                while journal_progress(db, key).cursor.phase != "done":
                    recorder = RecordedConnection(db)
                    page = prepare_journal_page(recorder, key, source, action_budget=budget)
                    sample = {
                        "declaration": binding.declaration.id,
                        "declaration_sha256": declaration_sha,
                        "dimensions": {"action_budget": budget},
                        "metrics": {
                            "sql_statements": recorder.metrics["sql_statements"],
                            "returned_rows": recorder.metrics["returned_rows"],
                            "prepared_actions": len(page.actions),
                        },
                        "delegates": {},
                    }
                    validate_execution_samples(projection, binding.declaration.id, [sample])
                    samples.append(sample)
                    with write(db):
                        apply_journal_page(db, key, source, page)
            db.close()
    assert len(samples) >= 20
    record_property("growth_samples", samples)


def test_correction_new_assertion_birth_and_selected_prefix_are_independent(tmp_path):
    fixture = native_scalar_fixture(1)
    with validate_journal(fixture.raw, catalog=fixture.catalog) as original:
        document = original.frames[0].document
        target = {"entry": original.anchor_at(0)["through"], "assertion_id": fixture.assertion_id}
        replacement = copy.deepcopy(document["body"]["assertions"]["extensions"][0])
        replacement["id"] = f"urn:uuid:{uuid4()}"
        replacement["assertion_id"] = f"urn:uuid:{uuid4()}"
        raw = append_correction(
            fixture.raw,
            [target],
            reason="fixture replacement",
            recorded_by_agent_id=document["recorded_by_agent_id"],
            assertions={"extensions": [replacement]},
            catalog=fixture.catalog,
        )
    db = open_work(tmp_path / "work.sqlite")
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        for through in (0, 1):
            with QualifiedJournalSource(summary, ADMISSION, through=through) as source:
                key = bind(db, source)
                drive(db, key, source, 7)
                expected = 7 + through
                assert journal_progress(db, key).assertions == expected
                assert db.execute(
                    "SELECT born_sequence,retired_sequence FROM discovery_journal_assertions "
                    "WHERE source_key=? AND assertion_id=?",
                    (key.source_key, fixture.assertion_id),
                ).fetchone() == ("0", "1" if through else None)
                born = db.execute(
                    "SELECT born_sequence,retired_sequence FROM discovery_journal_assertions "
                    "WHERE source_key=? AND assertion_id=?",
                    (key.source_key, replacement["assertion_id"]),
                ).fetchone()
                assert born == (("1", None) if through else None)
    db.close()


@pytest.mark.growth_witness(
    "canonical-discovery-journal-page-apply/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_work.apply_journal_page",
)
def test_measured_journal_page_apply_bounds_atomic_child_registration(
    tmp_path, monkeypatch, record_property
):
    import riverhog_core.canonical_discovery_journal_work as work
    from riverhog_canonical_json import canonical_json_bytes
    from riverhog_growth_contracts import (
        growth_binding,
        project_bindings,
        validate_execution_samples,
    )

    binding = growth_binding(apply_journal_page)
    projection = project_bindings((binding,))
    declaration_sha = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    actual_sha256 = hashlib.sha256
    samples = []
    fixture, raw, _ = corrected_fixture()
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        for budget in (1, 7, 128):
            db = open_work(tmp_path / f"apply-{budget}.sqlite")
            with QualifiedJournalSource(summary, ADMISSION) as source:
                key = bind(db, source)
                while journal_progress(db, key).cursor.phase != "done":
                    page = prepare_journal_page(db, key, source, action_budget=budget)
                    recorder = RecordedConnection(db)

                    def observed_hash(value=b"", recorder=recorder, **kwargs):
                        recorder.metrics["hashed_octets"] += len(value)
                        return actual_sha256(value, **kwargs)

                    with monkeypatch.context() as patch:
                        patch.setattr(work.hashlib, "sha256", observed_hash)
                        with write(db):
                            apply_journal_page(recorder, key, source, page)
                    sample = {
                        "declaration": binding.declaration.id,
                        "declaration_sha256": declaration_sha,
                        "dimensions": {"page_actions": len(page.actions)},
                        "metrics": {
                            "sql_statements": recorder.metrics["sql_statements"],
                            "returned_rows": recorder.metrics["returned_rows"],
                            "hashed_octets": recorder.metrics["hashed_octets"],
                        },
                        "delegates": {},
                    }
                    validate_execution_samples(projection, binding.declaration.id, [sample])
                    samples.append(sample)
            db.close()
    record_property("growth_samples", samples)


def test_acknowledgement_rolls_back_and_invalidates_speculative_parent_page(tmp_path):
    fixture = native_scalar_fixture(1)
    db = open_work(tmp_path / "work.sqlite")
    with validate_journal(fixture.raw, catalog=fixture.catalog) as summary:
        with QualifiedJournalSource(summary, ADMISSION) as source:
            key = bind(db, source)
            first = prepare_journal_page(db, key, source, action_budget=2)
            assertion_id = first.actions[1].assertion.assertion_id
            with write(db):
                apply_journal_page(db, key, source, first)
            finish_child(db, key, source, assertion_id)
            before = journal_progress(db, key)
            page = prepare_journal_page(db, key, source, action_budget=1)
            db.execute(
                "CREATE TRIGGER fail_ack BEFORE UPDATE ON discovery_journal_tasks "
                "BEGIN SELECT RAISE(ABORT,'injected ack failure'); END"
            )
            with write(db), pytest.raises(sqlite3.IntegrityError, match="injected ack"):
                acknowledge_assertion(db, key, assertion_id)
            assert journal_progress(db, key) == before
            assert db.execute(
                "SELECT acknowledged FROM discovery_journal_assertions"
            ).fetchone() == (0,)
            db.execute("DROP TRIGGER fail_ack")
            with write(db):
                assert acknowledge_assertion(db, key, assertion_id).pending == 0
            with write(db), pytest.raises(StaleJournalTask, match="version"):
                apply_journal_page(db, key, source, page)
            with pytest.raises(StaleJournalTask, match="coordinate"):
                source.assertion(0, first.actions[1].assertion.group, -1)
    db.close()
