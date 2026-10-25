from __future__ import annotations

import hashlib
import sqlite3
from contextlib import closing
from dataclasses import replace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.canonical_discovery_dependency_source import (
    initialize_dependency_source,
    next_source_assertion,
)
from riverhog_core.canonical_discovery_fact_work import FactTaskKey
from riverhog_core.canonical_discovery_journal_extraction import JournalFactExtraction
from riverhog_core.canonical_discovery_journal_work import (
    JournalAssertion,
    JournalTaskKey,
    QualifiedJournalSource,
    StaleJournalTask,
)
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import validate_journal

from tests.support.canonical_discovery_work import (
    ADMISSION,
    MeasuredConnection,
    corrected_journal_fixture,
    metadata_octets,
    write,
)


@pytest.fixture(scope="module")
def completed(tmp_path_factory):
    fixture, raw, target = corrected_journal_fixture()
    path = tmp_path_factory.mktemp("dependency-source") / "qualified.sqlite"
    db = sqlite3.connect(path, isolation_level=None)
    initialize_dependency_source(db)
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        with QualifiedJournalSource(summary, ADMISSION) as source:
            key = JournalTaskKey("build", source.source_key, "worker")
            driver = JournalFactExtraction(db, key, source)
            while not driver.advance(action_budget=128).parent.complete:
                pass
            expected = db.execute(
                "SELECT assertion_id,born_sequence,source_group,source_item "
                "FROM discovery_journal_assertions ORDER BY "
                "length(born_sequence),born_sequence,source_group,source_item"
            ).fetchall()
            db.close()
            yield path, key, expected, target


@pytest.fixture
def owned(tmp_path, completed):
    original, key, expected, target = completed
    path = tmp_path / "work.sqlite"
    with closing(sqlite3.connect(original)) as source:
        db = sqlite3.connect(path, isolation_level=None)
        source.backup(db)
    try:
        yield db, key, expected, target, path
    finally:
        db.close()


def walk(db, key, through, after=None):
    values = []
    while (current := next_source_assertion(db, key, through=through, after=after)) is not None:
        assert current != after
        values.append(current)
        after = current
    return values, after


def test_original_cursor_extends_prefix_without_repeating_retired_or_active_sources(owned):
    db, key, expected, target, _ = owned
    initial, after = walk(db, key, 0)
    assert target["assertion_id"] in {row.assertion_id for row in initial}
    assert all(row.sequence == 0 for row in initial)
    assert next_source_assertion(db, key, through=0, after=after) is None
    later, terminal = walk(db, key, 2, after)
    actual = initial + later
    assert [
        (row.assertion_id, str(row.sequence), row.group, row.item) for row in actual
    ] == expected
    assert len({row.assertion_id for row in actual}) == len(expected)
    assert next_source_assertion(db, key, through=2, after=terminal) is None
    assert next_source_assertion(db, key, through=2, after=terminal) is None


def test_source_cursor_reopens_at_original_coordinates_without_mutations(owned):
    db, key, expected, _, path = owned
    first = next_source_assertion(db, key, through=2)
    before = db.total_changes
    with closing(sqlite3.connect(path, isolation_level=None)) as reopened:
        remaining, _ = walk(reopened, key, 2, first)
        assert len(remaining) == len(expected) - 1
        assert reopened.total_changes == 0
    assert db.total_changes == before


@pytest.mark.parametrize("field", ["build_id", "source_key", "worker_id"])
def test_cursor_cannot_cross_its_original_owner_binding(owned, field):
    db, key, _, _, _ = owned
    first = next_source_assertion(db, key, through=2)
    value = "0" * 64 if field == "source_key" else "other"
    changed = replace(first, key=replace(first.key, **{field: value}))
    with pytest.raises(StaleJournalTask, match="another source"):
        next_source_assertion(db, key, through=2, after=changed)
    with pytest.raises(StaleJournalTask, match="binding"):
        next_source_assertion(db, replace(key, **{field: value}), through=2)


@pytest.mark.parametrize("through", [-1, 3, True, "2"])
def test_prefix_requires_an_exact_in_range_original_sequence(owned, through):
    db, key, _, _, _ = owned
    with pytest.raises(ValueError, match="prefix"):
        next_source_assertion(db, key, through=through)


@pytest.mark.parametrize(
    "field,value",
    [
        ("complete", 0),
        ("pending", "1"),
        ("phase", "assertions"),
        ("terminal_version", None),
        ("version", "0"),
    ],
)
def test_source_requires_qualified_input_and_child_terminals(owned, field, value):
    db, key, _, _, _ = owned
    allowed = {"complete", "pending", "phase", "terminal_version", "version"}
    assert field in allowed
    db.execute("UPDATE discovery_journal_tasks SET " + field + "=?", (value,))
    with pytest.raises(StaleJournalTask, match="completed original"):
        next_source_assertion(db, key, through=2)


def fixture_row(db, key, *, sequence, group, item, identity):
    # Selector-only original-coordinate fixtures vary the index independently.
    # Source and child qualification run through the actual driver above.
    values = list(db.execute("SELECT * FROM discovery_journal_assertions LIMIT 1").fetchone())
    values[2], values[6], values[8], values[9], values[10] = (
        identity,
        str(sequence),
        group,
        item,
        identity + "-task",
    )
    db.execute(
        "INSERT INTO discovery_journal_assertions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", values
    )
    return JournalAssertion(
        FactTaskKey(key.build_id, values[10], key.source_key, key.worker_id),
        identity,
        sequence,
        group,
        item,
        values[5],
        values[11],
    )


def test_decimal_width_transition_and_empty_intermediate_prefix(owned):
    db, key, _, _, _ = owned
    db.execute(
        "DELETE FROM discovery_journal_assertions WHERE assertion_id NOT IN "
        "(SELECT assertion_id FROM discovery_journal_assertions LIMIT 1)"
    )
    # Preserve one original template while constructing isolated seek regimes.
    ninth = fixture_row(db, key, sequence=9, group="extensions", item=0, identity="ninth")
    tenth = fixture_row(db, key, sequence=10, group="extensions", item=0, identity="tenth")
    db.execute("UPDATE discovery_journal_tasks SET entry_count='11',sequence='11'")
    assert next_source_assertion(db, key, through=9, after=ninth) is None
    assert next_source_assertion(db, key, through=10, after=ninth) == tenth
    assert next_source_assertion(db, key, through=10, after=tenth) is None


@pytest.mark.growth_witness(
    "canonical-discovery-documentary-source-next/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_dependency_source.next_source_assertion",
)
def test_measured_original_cursor_seeks_past_dense_completed_source_coordinates(
    owned, monkeypatch, record_property
):
    db, key, _, _, _ = owned
    binding = growth_binding(next_source_assertion)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    samples = []
    for width in (1, 17, 1025, 8193):
        with write(db):
            positions = [
                fixture_row(
                    db,
                    key,
                    sequence=0,
                    group="zz-dense",
                    item=index,
                    identity=f"dense-{width}-{index}",
                )
                for index in range(width)
            ]
        total = db.execute("SELECT count(*) FROM discovery_journal_assertions").fetchone()[0]
        rows = db.execute("SELECT * FROM discovery_journal_tasks").fetchall()
        rows += db.execute("SELECT * FROM discovery_journal_assertions").fetchall()
        metadata = max(sum(map(metadata_octets, row)) for row in rows)
        for after in (None, positions[-2] if width > 1 else positions[0], positions[-1]):
            recorder = MeasuredConnection(db)
            actual_hash = hashlib.sha256

            def hashed(value=b"", recorder=recorder, actual_hash=actual_hash, **kwargs):
                recorder.metrics["hashed_octets"] += len(value)
                return actual_hash(value, **kwargs)

            def tick(recorder=recorder):
                recorder.metrics["sqlite_vm_instructions"] += 1
                return 0

            db.set_progress_handler(tick, 1)
            try:
                with monkeypatch.context() as patch:
                    patch.setattr(hashlib, "sha256", hashed)
                    selected = next_source_assertion(recorder, key, through=0, after=after)
            finally:
                db.set_progress_handler(None, 0)
            if after is None:
                assert selected is not None
            elif after == positions[-1]:
                # Later regimes retain earlier rows, so compare the actual next
                # original coordinate rather than assuming the source is empty.
                if selected is not None:
                    assert (selected.group, selected.item) > (after.group, after.item)
            else:
                assert selected is not None and selected.item == after.item + 1
            sample = {
                "declaration": binding.declaration.id,
                "declaration_sha256": digest,
                "dimensions": {"registered_children": total, "metadata_octets": metadata},
                "metrics": {
                    bound.metric: recorder.metrics[bound.metric]
                    for bound in binding.declaration.bounds
                },
                "delegates": {},
            }
            validate_execution_samples(projection, binding.declaration.id, [sample])
            samples.append(sample)
        db.execute("DELETE FROM discovery_journal_assertions WHERE source_group='zz-dense'")
    record_property("growth_samples", samples)
