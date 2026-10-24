from __future__ import annotations

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.canonical_discovery_fact_work import fact_extraction_cursor
from riverhog_core.canonical_discovery_journal_extraction import JournalFactExtraction
from riverhog_core.canonical_discovery_journal_work import (
    JournalTaskKey,
    QualifiedJournalSource,
    StaleJournalTask,
    journal_progress,
    pending_journal_assertion,
)
from riverhog_provenance import JournalFrame, validate_journal

from tests.support.canonical_discovery_work import (
    ADMISSION,
    corrected_journal_fixture,
    open_journal_work,
)


def run(driver, budget):
    results = []
    while True:
        result = driver.advance(action_budget=budget)
        assert 0 <= result.actions <= budget
        results.append(result.phase)
        if result.parent.complete:
            return results


def stored_facts(db):
    # Attempt identifiers and storage transition versions are operational state.
    return db.execute(
        "SELECT a.born_sequence,a.source_group,a.source_item,s.kind,s.record_key,"
        "s.ordinal,s.exact_bytes FROM discovery_journal_assertions a "
        "JOIN discovery_fact_segments s ON s.build_id=a.build_id AND s.task_id=a.task_id "
        "ORDER BY length(a.born_sequence),a.born_sequence,a.source_group,a.source_item,"
        "s.kind,length(s.record_key),s.record_key,s.ordinal"
    ).fetchall()


@pytest.mark.parametrize("budget", [1, 7, 128])
def test_source_fact_driver_finishes_every_child_and_matches_exact_facts(tmp_path, budget):
    fixture, raw, _ = corrected_journal_fixture()
    expected = None
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        for actual_budget in dict.fromkeys((128, budget)):
            db = open_journal_work(tmp_path / f"work-{actual_budget}.sqlite")
            with QualifiedJournalSource(summary, ADMISSION) as source:
                key = JournalTaskKey("build", source.source_key, "worker")
                driver = JournalFactExtraction(db, key, source)
                phases = run(driver, actual_budget)
                assert {
                    "registered",
                    "facts",
                    "sealed-assertion",
                    "acknowledged",
                    "complete",
                } <= set(phases)
                progress = journal_progress(db, key)
                assert progress.complete and progress.pending == 0
                assert progress.assertions == 7 and progress.retractions == 1
                facts = stored_facts(db)
                assert facts
                if expected is None:
                    expected = facts
                else:
                    assert facts == expected
                assert driver.advance(action_budget=actual_budget).actions == 0
            db.close()


def test_driver_retains_one_native_entry_across_all_its_children(tmp_path, monkeypatch):
    import riverhog_core.canonical_discovery_journal_extraction as extraction

    fixture, raw, _ = corrected_journal_fixture()
    db = open_journal_work(tmp_path / "work.sqlite")
    loaded = []
    encoded = []
    original_document = JournalFrame.document.fget
    original_source = extraction.AdmittedAssertionSource

    def document(frame):
        assert not db.in_transaction
        result = original_document(frame)
        loaded.append(result["sequence"])
        return result

    def source(*args, **kwargs):
        assert not db.in_transaction
        result = original_source(*args, **kwargs)
        encoded.append(result.assertion_id)
        return result

    with validate_journal(raw, catalog=fixture.catalog) as summary:
        with QualifiedJournalSource(summary, ADMISSION) as qualified:
            key = JournalTaskKey("build", qualified.source_key, "worker")
            driver = JournalFactExtraction(db, key, qualified)
            monkeypatch.setattr(JournalFrame, "document", property(document))
            monkeypatch.setattr(extraction, "AdmittedAssertionSource", source)
            run(driver, 7)
    assert loaded == ["0", "1", "2"]
    assert len(encoded) == len(set(encoded)) == 7
    db.close()


def test_driver_reopens_mid_assertion_and_skips_completed_source_work(tmp_path, monkeypatch):
    fixture, raw, _ = corrected_journal_fixture()
    path = tmp_path / "work.sqlite"
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        with QualifiedJournalSource(summary, ADMISSION) as source:
            db = open_journal_work(path)
            key = JournalTaskKey("build", source.source_key, "worker")
            driver = JournalFactExtraction(db, key, source)
            while True:
                result = driver.advance(action_budget=7)
                if result.phase == "facts":
                    assert fact_extraction_cursor(db, result.child)[0].version > 0
                    break
            before = journal_progress(db, key)
            pending = pending_journal_assertion(db, key)
            position = fact_extraction_cursor(db, pending.key)
            db.close()
    loaded = []
    original_document = JournalFrame.document.fget

    def document(frame):
        result = original_document(frame)
        loaded.append(result["sequence"])
        return result

    with validate_journal(raw, catalog=fixture.catalog) as summary:
        with QualifiedJournalSource(summary, ADMISSION) as source:
            db = open_journal_work(path)
            driver = JournalFactExtraction(db, key, source)
            assert journal_progress(db, key) == before
            assert fact_extraction_cursor(db, pending.key) == position
            monkeypatch.setattr(JournalFrame, "document", property(document))
            run(driver, 7)
            assert loaded == ["0", "1", "2"]
            assert journal_progress(db, key).complete
            db.close()


@pytest.mark.parametrize("terminal", ["sealed-assertion", "acknowledged"])
def test_terminal_restart_does_not_reload_completed_assertion(tmp_path, monkeypatch, terminal):
    import riverhog_core.canonical_discovery_journal_extraction as extraction

    fixture, raw, _ = corrected_journal_fixture()
    db = open_journal_work(tmp_path / "work.sqlite")
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        with QualifiedJournalSource(summary, ADMISSION) as source:
            key = JournalTaskKey("build", source.source_key, "worker")
            driver = JournalFactExtraction(db, key, source)
            while True:
                result = driver.advance(action_budget=128)
                if (
                    result.phase == "facts"
                    and fact_extraction_cursor(db, result.child)[1].category == "done"
                ):
                    break
            if terminal == "acknowledged":
                assert driver.advance(action_budget=128).phase == "sealed-assertion"
            replacement = JournalFactExtraction(db, key, source)

            def forbidden(*_args, **_kwargs):
                raise AssertionError("completed assertion was reconstructed")

            with monkeypatch.context() as patch:
                patch.setattr(extraction, "AdmittedAssertionSource", forbidden)
                assert replacement.advance(action_budget=128).phase == terminal
            run(replacement, 128)
    db.close()


def test_driver_rejects_stale_worker_closed_source_and_unaccounted_queue(tmp_path):
    fixture, raw, _ = corrected_journal_fixture()
    db = open_journal_work(tmp_path / "work.sqlite")
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        with QualifiedJournalSource(summary, ADMISSION) as source:
            key = JournalTaskKey("build", source.source_key, "worker")
            driver = JournalFactExtraction(db, key, source)
            driver.advance(action_budget=128)
            with pytest.raises(StaleJournalTask, match="registration"):
                JournalFactExtraction(
                    db, JournalTaskKey("build", source.source_key, "other"), source
                )
            db.execute("DELETE FROM discovery_journal_assertions")
            with pytest.raises(StaleJournalTask, match="unaccounted"):
                driver.advance(action_budget=128)
            source.close()
            with pytest.raises(StaleJournalTask, match="closed"):
                driver.advance(action_budget=128)
    db.close()


@pytest.mark.growth_witness(
    "canonical-discovery-journal-extraction-step/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_extraction.JournalFactExtraction.advance",
)
def test_measured_driver_bounds_core_sql_actions_and_transactions(tmp_path, record_property):
    import hashlib

    from riverhog_growth_contracts import (
        growth_binding,
        project_bindings,
        validate_execution_samples,
    )

    from tests.support.canonical_discovery_work import RecordedConnection

    class Recorder(RecordedConnection):
        def commit(self):
            self.metrics["commits"] += 1
            super().commit()

        def rollback(self):
            self.metrics["rollbacks"] += 1
            super().rollback()

    binding = growth_binding(JournalFactExtraction.advance)
    projection = project_bindings((binding,))
    declaration_sha = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    samples = []
    fixture, raw, _ = corrected_journal_fixture()
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        for budget in (1, 7, 128):
            db = open_journal_work(tmp_path / f"measured-{budget}.sqlite")
            with QualifiedJournalSource(summary, ADMISSION) as source:
                key = JournalTaskKey("build", source.source_key, "worker")
                driver = JournalFactExtraction(db, key, source)
                recorder = Recorder(db)
                driver.db = recorder
                maxima = {}
                while True:
                    recorder.metrics = {
                        "sql_statements": 0,
                        "returned_rows": 0,
                        "commits": 0,
                        "rollbacks": 0,
                    }
                    result = driver.advance(action_budget=budget)
                    metrics = dict(recorder.metrics, work_actions=result.actions)
                    sample = {
                        "declaration": binding.declaration.id,
                        "declaration_sha256": declaration_sha,
                        "dimensions": {"action_budget": budget},
                        "metrics": metrics,
                        "delegates": {},
                    }
                    validate_execution_samples(projection, binding.declaration.id, [sample])
                    previous = maxima.get(result.phase)
                    if previous is None or any(
                        value > previous[name] for name, value in metrics.items()
                    ):
                        samples.append(sample)
                        maxima[result.phase] = (
                            metrics
                            if previous is None
                            else {
                                name: max(previous[name], value) for name, value in metrics.items()
                            }
                        )
                    if result.parent.complete:
                        break
                assert set(maxima) == {
                    "registered",
                    "facts",
                    "sealed-assertion",
                    "acknowledged",
                    "complete",
                }
            db.close()
    record_property("growth_samples", samples)


def test_pending_children_keep_native_numeric_sequence_order_across_decimal_width(tmp_path):
    import copy
    from uuid import uuid4

    from riverhog_core.canonical_discovery_journal_work import (
        apply_journal_page,
        prepare_journal_page,
    )
    from riverhog_provenance import append_assertion_batches

    from tests.support.canonical_discovery import native_scalar_fixture
    from tests.support.canonical_discovery_work import write

    fixture = native_scalar_fixture(0)
    with validate_journal(fixture.raw, catalog=fixture.catalog) as original:
        document = original.frames[0].document
        template = document["body"]["assertions"]["extensions"][0]
        batches = []
        for _ in range(12):
            row = copy.deepcopy(template)
            row["id"], row["assertion_id"] = f"urn:uuid:{uuid4()}", f"urn:uuid:{uuid4()}"
            batches.append({"extensions": [row]})
        raw = append_assertion_batches(
            fixture.raw,
            batches,
            recorded_by_agent_id=document["recorded_by_agent_id"],
            catalog=fixture.catalog,
        )
    db = open_journal_work(tmp_path / "work.sqlite")
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        with QualifiedJournalSource(summary, ADMISSION) as source:
            key = JournalTaskKey("build", source.source_key, "worker")
            driver = JournalFactExtraction(db, key, source)
            # Retained parent work can already contain children from multiple entries.
            while journal_progress(db, key).cursor.phase != "done":
                page = prepare_journal_page(db, key, source, action_budget=128)
                with write(db):
                    apply_journal_page(db, key, source, page)
            acknowledged = []
            while True:
                result = driver.advance(action_budget=128)
                if result.phase == "acknowledged":
                    value = db.execute(
                        "SELECT born_sequence FROM discovery_journal_assertions WHERE task_id=?",
                        (result.child.task_id,),
                    ).fetchone()[0]
                    acknowledged.append(int(value))
                if result.parent.complete:
                    break
            assert acknowledged == [0] * 7 + list(range(1, 13))
    db.close()


def test_failed_fact_page_retries_from_saved_cursor_with_same_exact_output(tmp_path):
    import sqlite3

    fixture, raw, _ = corrected_journal_fixture()
    expected = None
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        for inject in (False, True):
            db = open_journal_work(tmp_path / f"failure-{inject}.sqlite")
            with QualifiedJournalSource(summary, ADMISSION) as source:
                key = JournalTaskKey("build", source.source_key, "worker")
                driver = JournalFactExtraction(db, key, source)
                driver.advance(action_budget=128)
                if inject:
                    parent = journal_progress(db, key)
                    pending = pending_journal_assertion(db, key)
                    checkpoint = fact_extraction_cursor(db, pending.key)
                    db.execute(
                        "CREATE TRIGGER fail_driver BEFORE INSERT ON discovery_fact_segments "
                        "BEGIN SELECT RAISE(ABORT,'injected driver failure'); END"
                    )
                    with pytest.raises(sqlite3.IntegrityError, match="injected driver"):
                        driver.advance(action_budget=128)
                    assert not db.in_transaction
                    assert journal_progress(db, key) == parent
                    assert fact_extraction_cursor(db, pending.key) == checkpoint
                    db.execute("DROP TRIGGER fail_driver")
                run(driver, 128)
                actual = stored_facts(db)
                if expected is None:
                    expected = actual
                else:
                    assert actual == expected
            db.close()
