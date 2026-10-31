from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing
from dataclasses import replace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core import canonical_discovery_dependency_facts as owner
from riverhog_core.canonical_discovery_dependency_source import (
    initialize_dependency_source,
    next_source_assertion,
)
from riverhog_core.canonical_discovery_fact_work import FactSegmentConflict, StaleFactTask
from riverhog_core.canonical_discovery_journal_extraction import JournalFactExtraction
from riverhog_core.canonical_discovery_journal_work import (
    JournalTaskKey,
    QualifiedJournalSource,
    StaleJournalTask,
)
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import append_correction, validate_journal

from tests.support.canonical_discovery_work import (
    ADMISSION,
    MeasuredConnection,
    metadata_octets,
    write,
)
from tests.unit.test_canonical_discovery_source_work import native_documentary_fixture


@pytest.fixture(scope="module")
def qualified(tmp_path_factory):
    fixture = native_documentary_fixture()
    with validate_journal(fixture.raw, catalog=fixture.catalog) as original:
        target = {
            "entry": original.anchor_at(0)["through"],
            "assertion_id": fixture.assertion_id,
        }
        who = original.frames[0].document["recorded_by_agent_id"]
    raw = append_correction(
        fixture.raw,
        [target],
        reason="documentary retirement",
        recorded_by_agent_id=who,
        catalog=fixture.catalog,
    )
    path = tmp_path_factory.mktemp("documentary-facts") / "qualified.sqlite"
    db = sqlite3.connect(path, isolation_level=None)
    initialize_dependency_source(db)
    with validate_journal(raw, catalog=fixture.catalog) as native:
        with QualifiedJournalSource(native, ADMISSION) as source:
            key = JournalTaskKey("build", source.source_key, "worker")
            driver = JournalFactExtraction(db, key, source)
            while not driver.advance(action_budget=128).parent.complete:
                pass
            after = None
            while (current := next_source_assertion(db, key, through=1, after=after)) is not None:
                if current.assertion_id == fixture.assertion_id:
                    assertion = current
                after = current
            row = db.execute(
                "SELECT record_key,exact_bytes FROM discovery_fact_segments "
                "WHERE task_id=? AND kind='documentary-reference'",
                (assertion.key.task_id,),
            ).fetchone()
            assert row is not None
            db.close()
            yield path, assertion, int(row[0]), json.loads(row[1])


@pytest.fixture
def owned(tmp_path, qualified):
    original, assertion, node, reference = qualified
    path = tmp_path / "work.sqlite"
    with closing(sqlite3.connect(original)) as source:
        db = sqlite3.connect(path, isolation_level=None)
        source.backup(db)
    try:
        yield db, assertion, node, reference, path
    finally:
        db.close()


def read(db, assertion, *, after=None, budget=65536):
    return owner.next_documentary_reference(db, assertion, after=after, octet_budget=budget)


def test_reads_original_retired_assertion_reference_and_reopens_without_writes(owned):
    db, assertion, node, reference, path = owned
    before = db.total_changes
    selected = read(db, assertion)
    assert selected == owner.DocumentaryReference(
        owner.DocumentaryPosition(assertion, node), reference
    )
    assert selected.reference is not reference
    assert read(db, assertion, after=selected.position) is None
    assert db.total_changes == before
    with closing(sqlite3.connect(path, isolation_level=None)) as reopened:
        assert read(reopened, assertion) == selected
        assert read(reopened, assertion, after=selected.position) is None
        assert reopened.total_changes == 0
    selected.reference["entry"]["sequence"] = "999"
    assert read(db, assertion).reference == reference


def test_actual_completed_source_assertions_without_documentary_facts_are_explicitly_empty(owned):
    db, assertion, _, _, _ = owned
    key = JournalTaskKey(assertion.key.build_id, assertion.key.source_key, assertion.key.worker_id)
    first = next_source_assertion(db, key, through=1)
    while first.assertion_id == assertion.assertion_id:
        first = next_source_assertion(db, key, through=1, after=first)
    assert first is not None
    assert read(db, first) is None


@pytest.mark.parametrize("field", ["build_id", "task_id", "source_key", "worker_id"])
def test_reader_rejects_foreign_source_or_worker(owned, field):
    db, assertion, _, _, _ = owned
    value = "0" * 64 if field == "source_key" else "other"
    changed = replace(assertion, key=replace(assertion.key, **{field: value}))
    with pytest.raises((StaleFactTask, StaleJournalTask)):
        read(db, changed)
    cursor = read(db, assertion)
    with pytest.raises(StaleFactTask, match="another original"):
        read(db, changed, after=cursor.position)


@pytest.mark.parametrize(
    "field,value",
    [
        ("assertion_id", "other"),
        ("sequence", 1),
        ("group", "other"),
        ("item", 100),
        ("origin_entry_json", "{}"),
        ("canonical_stream_ref", "0" * 64),
    ],
)
def test_reader_requires_every_original_assertion_coordinate(owned, field, value):
    db, assertion, _, _, _ = owned
    with pytest.raises(StaleFactTask, match="original binding"):
        read(db, replace(assertion, **{field: value}))


@pytest.mark.parametrize(
    "table,field,value",
    [
        ("discovery_journal_tasks", "complete", 0),
        ("discovery_journal_assertions", "acknowledged", 0),
        ("discovery_fact_tasks", "worker_id", "other"),
        ("discovery_fact_tasks", "origin_sha256", "0" * 64),
        ("discovery_fact_tasks", "canonical_stream_ref", "0" * 64),
        ("discovery_fact_tasks", "complete", 0),
        ("discovery_fact_tasks", "open_streams", 1),
        ("discovery_fact_tasks", "terminal_version", None),
        ("discovery_fact_tasks", "version", 0),
    ],
)
def test_reader_requires_source_acknowledgement_and_exact_fact_terminal(owned, table, field, value):
    db, assertion, _, _, _ = owned
    allowed = {"discovery_journal_tasks", "discovery_journal_assertions", "discovery_fact_tasks"}
    assert table in allowed
    db.execute("UPDATE " + table + " SET " + field + "=?", (value,))
    with pytest.raises((StaleFactTask, StaleJournalTask)):
        read(db, assertion)


def replace_references(db, assertion, values):
    """Isolated physical-reader regimes; real source qualification is separate above."""
    key = assertion.key
    identity = (key.build_id, key.task_id, "documentary-reference")
    with write(db):
        db.execute(
            "DELETE FROM discovery_fact_segments WHERE build_id=? AND task_id=? AND kind=?",
            identity,
        )
        db.execute(
            "DELETE FROM discovery_fact_streams WHERE build_id=? AND task_id=? AND kind=?",
            identity,
        )
        for node, reference in values.items():
            raw = canonical_json_bytes(reference)
            parts = [raw[i : i + 4096] for i in range(0, len(raw), 4096)]
            db.execute(
                "INSERT INTO discovery_fact_streams VALUES (?,?,?,?,?,?,?,?,?)",
                (*identity, str(node), len(parts), len(raw), len(parts[-1]), 1, 100000),
            )
            for ordinal, part in enumerate(parts):
                db.execute(
                    "INSERT INTO discovery_fact_segments VALUES (?,?,?,?,?,?,?,?)",
                    (
                        *identity,
                        str(node),
                        ordinal,
                        part,
                        hashlib.sha256(part).hexdigest(),
                        ordinal,
                    ),
                )
        counts = db.execute(
            "SELECT count(*),coalesce(sum(fragments),0),coalesce(sum(octets),0) "
            "FROM discovery_fact_streams WHERE build_id=? AND task_id=?",
            (key.build_id, key.task_id),
        ).fetchone()
        db.execute(
            "UPDATE discovery_fact_tasks SET streams=?,fragments=?,octets=?,"
            "version=100002,terminal_version=100001 WHERE build_id=? AND task_id=?",
            (*counts, key.build_id, key.task_id),
        )


def test_segmented_invalid_reference_requires_capacity_then_native_rejects_complete_value(owned):
    db, assertion, _, reference, _ = owned
    wide = {**reference, "entry": {**reference["entry"], "sequence": "9" * 16384}}
    replace_references(db, assertion, {1: wide})
    raw = canonical_json_bytes(wide)
    with pytest.raises(owner.DocumentaryReferenceCapacity, match="exceeds capacity"):
        read(db, assertion, budget=len(raw) - 1)
    with pytest.raises(ValueError, match="sequence63"):
        read(db, assertion, budget=len(raw))
    db.execute(
        "DELETE FROM discovery_fact_segments WHERE kind='documentary-reference' AND ordinal=3"
    )
    with pytest.raises(FactSegmentConflict, match="fragment"):
        read(db, assertion, budget=len(raw))


def test_original_node_text_order_uses_payload_independent_positions(owned):
    db, assertion, _, reference, _ = owned
    replace_references(db, assertion, {1: reference, 10: reference, 2: reference})
    cursor = None
    actual = []
    while (current := read(db, assertion, after=cursor)) is not None:
        actual.append(current.position.node)
        cursor = current.position
    assert actual == [1, 10, 2]
    assert not hasattr(cursor, "reference")


def test_fragment_versions_cannot_repeat_inside_a_segmented_value(owned):
    db, assertion, _, reference, _ = owned
    wide = {**reference, "entry": {**reference["entry"], "sequence": "9" * 16384}}
    replace_references(db, assertion, {1: wide})
    db.execute(
        "UPDATE discovery_fact_segments SET staged_version=0 "
        "WHERE kind='documentary-reference' AND ordinal=1"
    )
    with pytest.raises(FactSegmentConflict, match="fragment"):
        read(db, assertion)


@pytest.mark.parametrize("budget", [0, -1, True, "4096"])
def test_capacity_requires_explicit_positive_octets(owned, budget):
    db, assertion, _, _, _ = owned
    with pytest.raises(ValueError, match="capacity"):
        read(db, assertion, budget=budget)


@pytest.mark.parametrize("fault", ["missing", "digest", "ordinal", "short", "version", "extra"])
def test_fragment_integrity_failures_never_become_absence(owned, fault):
    db, assertion, node, _, _ = owned
    identity = (assertion.key.build_id, assertion.key.task_id, "documentary-reference", str(node))
    clause = " WHERE build_id=? AND task_id=? AND kind=? AND record_key=?"
    if fault == "missing":
        db.execute("DELETE FROM discovery_fact_segments" + clause, identity)
    elif fault == "extra":
        db.execute(
            "INSERT INTO discovery_fact_segments "
            "SELECT build_id,task_id,kind,record_key,ordinal+1,exact_bytes,sha256,staged_version "
            "FROM discovery_fact_segments" + clause,
            identity,
        )
    else:
        field, value = {
            "digest": ("sha256", "0" * 64),
            "ordinal": ("ordinal", 1),
            "short": ("exact_bytes", b"{}"),
            "version": ("staged_version", 100000),
        }[fault]
        db.execute(
            "UPDATE discovery_fact_segments SET " + field + "=?" + clause, (value, *identity)
        )
    with pytest.raises(FactSegmentConflict, match="fragment"):
        read(db, assertion)


@pytest.mark.parametrize(
    "field,value",
    [
        ("complete", 0),
        ("terminal_version", None),
        ("octets", 1),
        ("fragments", 0),
        ("last_fragment_bytes", 0),
        ("record_key", "01"),
    ],
)
def test_corrupt_stream_terminals_never_become_absence(owned, field, value):
    db, assertion, _, _, _ = owned
    db.execute(
        "UPDATE discovery_fact_streams SET " + field + "=? WHERE kind='documentary-reference'",
        (value,),
    )
    with pytest.raises(FactSegmentConflict):
        read(db, assertion)


def test_complete_nonreference_and_noncanonical_bytes_fail_native_or_codec_validation(owned):
    db, assertion, _, reference, _ = owned
    replace_references(db, assertion, {1: {"type": "json", "value": reference}})
    with pytest.raises(FactSegmentConflict, match="Native external"):
        read(db, assertion)
    replace_references(db, assertion, {1: reference})
    raw = b" " + canonical_json_bytes(reference)
    digest = hashlib.sha256(raw).hexdigest()
    db.execute(
        "UPDATE discovery_fact_segments SET exact_bytes=?,sha256=? "
        "WHERE kind='documentary-reference'",
        (raw, digest),
    )
    db.execute(
        "UPDATE discovery_fact_streams SET octets=?,last_fragment_bytes=? "
        "WHERE kind='documentary-reference'",
        (len(raw), len(raw)),
    )
    with pytest.raises(ValueError):
        read(db, assertion)


@pytest.mark.growth_witness(
    "canonical-discovery-documentary-reference-next/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_dependency_facts.next_documentary_reference",
)
def test_reference_reader_measures_dense_seeks_full_segments_and_all_codec_bytes(
    owned, monkeypatch, record_property
):
    db, assertion, _, reference, path = owned
    binding = growth_binding(owner.next_documentary_reference)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    samples = []
    for width in (1, 17, 1025):
        for sequence in ("0", "17", str((1 << 63) - 1), "9" * 4096, "9" * 16384):
            # Valid references stay in Native's sequence63 domain. Oversized
            # physical values prove complete segmented verification on rejection.
            invalid_native = len(sequence) > 19
            wide = {**reference, "entry": {**reference["entry"], "sequence": sequence}}
            values = {1000000 + index: wide for index in range(width)}
            replace_references(db, assertion, values)
            ordered = sorted(values, key=str)
            for reopen in (False, True):
                with closing(sqlite3.connect(path, isolation_level=None)) as reopened:
                    actual_db = reopened if reopen else db
                    rows = actual_db.execute("SELECT * FROM discovery_journal_tasks").fetchall()
                    rows += actual_db.execute(
                        "SELECT * FROM discovery_journal_assertions"
                    ).fetchall()
                    rows += actual_db.execute("SELECT * FROM discovery_fact_tasks").fetchall()
                    metadata = max(sum(map(metadata_octets, row)) for row in rows)
                    registered = sum(
                        actual_db.execute("SELECT count(*) FROM " + table).fetchone()[0]
                        for table in (
                            "discovery_journal_tasks",
                            "discovery_journal_assertions",
                            "discovery_fact_tasks",
                            "discovery_fact_streams",
                            "discovery_fact_segments",
                        )
                    )
                    for after_node in (None, ordered[-2] if width > 1 else None, ordered[-1]):
                        after = (
                            owner.DocumentaryPosition(assertion, after_node)
                            if after_node is not None
                            else None
                        )
                        remaining = [
                            node
                            for node in ordered
                            if after_node is None or str(node) > str(after_node)
                        ]
                        expected = remaining[0] if remaining else None
                        raw = canonical_json_bytes(wide) if expected is not None else b""
                        recorder = MeasuredConnection(actual_db)
                        recorder.metrics.update(decoded_octets=0, native_reference_calls=0)
                        actual_hash = hashlib.sha256
                        actual_decode = owner.require_canonical_json
                        actual_encode = canonical_json_bytes
                        actual_native = owner.inspect_documentary_node

                        def hashed(value=b"", recorder=recorder, actual_hash=actual_hash, **kwargs):
                            recorder.metrics["hashed_octets"] += len(value)
                            return actual_hash(value, **kwargs)

                        def encode(value, recorder=recorder, actual_encode=actual_encode):
                            result = actual_encode(value)
                            recorder.metrics["encoded_octets"] += len(result)
                            return result

                        def decode(value, recorder=recorder, actual_decode=actual_decode):
                            recorder.metrics["decoded_octets"] += len(value)
                            return actual_decode(value)

                        def native(value, recorder=recorder, actual_native=actual_native):
                            recorder.metrics["native_reference_calls"] += 1
                            return actual_native(value)

                        def tick(recorder=recorder):
                            recorder.metrics["sqlite_vm_instructions"] += 1
                            return 0

                        actual_db.set_progress_handler(tick, 1)
                        try:
                            with monkeypatch.context() as patch:
                                patch.setattr(hashlib, "sha256", hashed)
                                patch.setattr(owner, "require_canonical_json", decode)
                                patch.setitem(
                                    actual_decode.__globals__, "canonical_json_bytes", encode
                                )
                                patch.setattr(owner, "inspect_documentary_node", native)
                                if invalid_native and expected is not None:
                                    with pytest.raises(ValueError, match="sequence63"):
                                        read(recorder, assertion, after=after)
                                    selected = None
                                else:
                                    selected = read(recorder, assertion, after=after)
                        finally:
                            actual_db.set_progress_handler(None, 0)
                        if not invalid_native:
                            assert (selected.position.node if selected else None) == expected
                        if selected is not None:
                            assert selected.reference == wide
                        assert recorder.metrics["hashed_octets"] == len(raw)
                        assert recorder.metrics["decoded_octets"] == len(
                            assertion.origin_entry_json.encode()
                        ) + len(raw)
                        assert (
                            recorder.metrics["encoded_octets"] == recorder.metrics["decoded_octets"]
                        )
                        sample = {
                            "declaration": binding.declaration.id,
                            "declaration_sha256": digest,
                            "dimensions": {
                                "registered_rows": registered,
                                "metadata_octets": metadata,
                                "reference_octets": len(raw),
                                "fragments": (len(raw) + 4095) // 4096,
                            },
                            "metrics": {
                                bound.metric: recorder.metrics[bound.metric]
                                for bound in binding.declaration.bounds
                            },
                            "delegates": {},
                        }
                        validate_execution_samples(projection, binding.declaration.id, [sample])
                        samples.append(sample)
    record_property("growth_samples", samples)
