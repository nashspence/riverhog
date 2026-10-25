from __future__ import annotations

import hashlib
import sqlite3
from contextlib import contextmanager
from dataclasses import replace

import pytest
from riverhog_core.canonical_discovery_commitment import (
    CommitmentKey,
    StaleCommitmentWork,
    append_commitment_leaf,
    apply_commitment_step,
    begin_commitment,
    begin_commitment_fold,
    branch_digest,
    commitment_progress,
    fact_fragment_leaf,
    fact_terminal_leaf,
    initialize_commitment_work,
    prepare_commitment_step,
)

KEY = CommitmentKey("build", "tree", "worker", "a" * 64, "b" * 64)


@contextmanager
def write(db):
    db.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        db.rollback()
        raise
    else:
        db.commit()


def open_work(path):
    db = sqlite3.connect(path, isolation_level=None)
    initialize_commitment_work(db)
    with write(db):
        begin_commitment(db, KEY, initial_source_key=b"0")
    return db


@pytest.fixture
def db(tmp_path):
    current = open_work(tmp_path / "tree.sqlite3")
    try:
        yield current
    finally:
        current.close()


def leaf(index):
    # The tree oracle uses independently specified raw leaf preimages.
    return hashlib.sha256(b"\x00" + index.to_bytes(8, "big")).digest()


def append(db, index):
    current = commitment_progress(db, KEY)
    with write(db):
        return append_commitment_leaf(
            db,
            KEY,
            expected_version=current.version,
            source_key=str(index).encode(),
            next_source_key=str(index + 1).encode(),
            digest=leaf(index),
        )


def advance(db):
    prepared = prepare_commitment_step(db, KEY)
    with write(db):
        result = apply_commitment_step(db, KEY, prepared)
    return prepared, result


def drain_carry(db):
    while commitment_progress(db, KEY).pending_digest is not None:
        advance(db)


def add_leaves(db, count):
    for index in range(count):
        append(db, index)
        drain_carry(db)


def begin_fold(db, count):
    progress = commitment_progress(db, KEY)
    with write(db):
        return begin_commitment_fold(
            db,
            KEY,
            expected_version=progress.version,
            source_terminal=str(count).encode(),
        )


def oracle(leaves):
    # RFC 9162 largest-power-of-two split, independently of the frontier algorithm.
    if not leaves:
        return hashlib.sha256(b"").digest()
    if len(leaves) == 1:
        return leaves[0]
    split = 1 << ((len(leaves) - 1).bit_length() - 1)
    return hashlib.sha256(b"\x01" + oracle(leaves[:split]) + oracle(leaves[split:])).digest()


def test_exact_fragment_and_terminal_byte_vectors():
    assert fact_fragment_leaf(b"value", b"0:0", 0, b"a\x00b").hex() == (
        "725259ebc09522b6f1a19cc9fa15d1cd6d063a99274ddf09d612ca9d9a4ede52"
    )
    assert fact_terminal_leaf(b"value", b"0:0", octets=0, fragments=0).hex() == (
        "126732f9fd8064aa97c63f1d000fdd3ff06d36942283ca05f17d69243fc294b1"
    )
    assert fact_terminal_leaf(b"value", b"0:0", octets=3, fragments=1).hex() == (
        "7623105051e16b2a2c5f3c8d861742b2bd40f8ad4547f7d4a08323a93570000e"
    )
    assert fact_fragment_leaf(b"value", b"0:0", 1, b"a\x00b") != fact_fragment_leaf(
        b"value", b"0:0", 0, b"a\x00b"
    )
    assert fact_fragment_leaf(b"a", b"b:c", 0, b"x") != fact_fragment_leaf(b"a:b", b"c", 0, b"x")
    assert fact_terminal_leaf(b"value", b"0:0", octets=4096, fragments=1) != (
        fact_terminal_leaf(b"value", b"0:0", octets=4097, fragments=2)
    )
    assert branch_digest(leaf(0), leaf(1)) != branch_digest(leaf(1), leaf(0))


@pytest.mark.parametrize("size", [0, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 64, 129])
def test_tree_matches_independent_rfc9162_shape(db, size):
    add_leaves(db, size)
    begin_fold(db, size)
    while commitment_progress(db, KEY).phase != "complete":
        advance(db)
    actual = commitment_progress(db, KEY)
    assert actual.root == oracle([leaf(i) for i in range(size)])
    assert actual.leaf_count == actual.fold_leaf_count == size
    assert db.execute("SELECT count(*) FROM discovery_commitment_frontier").fetchone() == (0,)


@pytest.mark.parametrize("budget", [1, 2, 7, 128])
def test_restart_resumes_saved_leaf_carry_and_fold_without_replay(tmp_path, budget):
    path = tmp_path / f"restart-{budget}.sqlite3"
    db = open_work(path)
    total = 129
    steps = 0
    visited = set()
    reopened_phases = set()
    try:
        while commitment_progress(db, KEY).phase != "complete":
            for _ in range(budget):
                progress = commitment_progress(db, KEY)
                if progress.phase == "complete":
                    break
                assert progress.version not in visited
                visited.add(progress.version)
                if progress.phase == "leaves" and progress.pending_digest is None:
                    if progress.leaf_count < total:
                        append(db, progress.leaf_count)
                    else:
                        begin_fold(db, total)
                else:
                    advance(db)
                steps += 1
            progress = commitment_progress(db, KEY)
            reopened_phases.add("carry" if progress.pending_digest is not None else progress.phase)
            db.close()
            db = open_work(path)
            assert commitment_progress(db, KEY) == progress
        result = commitment_progress(db, KEY)
        assert result.root == oracle([leaf(i) for i in range(total)])
        assert result.leaf_count == total
        assert steps == result.version
        if budget == 1:
            assert {"leaves", "carry", "fold", "complete"} <= reopened_phases
    finally:
        db.close()


def test_a_long_carry_is_individually_checkpointed_at_every_level(db):
    add_leaves(db, 7)
    append(db, 7)
    levels = []
    while commitment_progress(db, KEY).pending_digest is not None:
        prepared, progress = advance(db)
        levels.append(prepared.level)
        assert progress.leaf_count == 8
    assert levels == [0, 1, 2, 3]
    assert db.execute("SELECT level, digest FROM discovery_commitment_frontier").fetchall() == [
        ("3", oracle([leaf(i) for i in range(8)]))
    ]


def test_fold_places_each_higher_subtree_on_the_left_of_the_right_suffix(db):
    add_leaves(db, 7)
    begin_fold(db, 7)
    observed = []
    while commitment_progress(db, KEY).phase != "complete":
        prepared, progress = advance(db)
        if prepared.operation == "fold":
            observed.append((prepared.level, progress.fold_leaf_count, progress.fold_digest))
    assert [item[:2] for item in observed] == [(0, 1), (1, 3), (2, 7)]
    assert observed[0][2] == leaf(6)
    assert observed[1][2] == oracle([leaf(i) for i in range(4, 7)])
    assert observed[2][2] == oracle([leaf(i) for i in range(7)])


@pytest.mark.parametrize(
    "field",
    ["build_id", "task_id", "worker_id", "source_manifest_sha256", "extraction_policy_sha256"],
)
def test_changed_source_worker_or_policy_cannot_consume_a_prepared_step(db, field):
    append(db, 0)
    prepared = prepare_commitment_step(db, KEY)
    changed = replace(KEY, **{field: "c" * 64 if field.endswith("sha256") else "other"})
    before = commitment_progress(db, KEY)
    with write(db), pytest.raises(StaleCommitmentWork):
        apply_commitment_step(db, changed, prepared)
    assert commitment_progress(db, KEY) == before


def test_replayed_append_and_prepared_step_do_not_advance_twice(db):
    first = append(db, 0)
    prepared, placed = advance(db)
    with write(db), pytest.raises(StaleCommitmentWork):
        apply_commitment_step(db, KEY, prepared)
    with write(db), pytest.raises(StaleCommitmentWork):
        append_commitment_leaf(
            db, KEY, expected_version=0, source_key=b"0", next_source_key=b"1", digest=leaf(0)
        )
    assert first.leaf_count == placed.leaf_count == 1
    assert commitment_progress(db, KEY) == placed


@pytest.mark.parametrize("operation", ["carry", "fold", "complete"])
def test_changed_prepared_digest_is_rejected_before_any_write(db, operation):
    add_leaves(db, 1)
    if operation == "carry":
        append(db, 1)
    else:
        begin_fold(db, 1)
        if operation == "complete":
            advance(db)
    step = prepare_commitment_step(db, KEY)
    assert step.operation == operation
    before = commitment_progress(db, KEY)
    with write(db), pytest.raises(StaleCommitmentWork, match="digest changed"):
        apply_commitment_step(db, KEY, replace(step, result_digest=b"x" * 32))
    assert commitment_progress(db, KEY) == before


@pytest.mark.parametrize("phase", ["carry", "fold"])
def test_frontier_write_and_checkpoint_rollback_together_after_interruption(db, phase):
    add_leaves(db, 1)
    if phase == "carry":
        append(db, 1)
    else:
        begin_fold(db, 1)
    prepared = prepare_commitment_step(db, KEY)
    before = commitment_progress(db, KEY)
    frontier = db.execute("SELECT * FROM discovery_commitment_frontier").fetchall()
    db.executescript("""
        CREATE TRIGGER stop_checkpoint BEFORE UPDATE ON discovery_commitment_tasks
        BEGIN SELECT RAISE(ABORT, 'interrupted checkpoint'); END;
    """)
    with write(db), pytest.raises(sqlite3.IntegrityError, match="interrupted checkpoint"):
        apply_commitment_step(db, KEY, prepared)
    assert commitment_progress(db, KEY) == before
    assert db.execute("SELECT * FROM discovery_commitment_frontier").fetchall() == frontier
    db.execute("DROP TRIGGER stop_checkpoint")
    with write(db):
        apply_commitment_step(db, KEY, prepared)


def test_lost_frontier_and_skipped_fold_levels_cannot_complete(db):
    add_leaves(db, 7)
    begin_fold(db, 7)
    prepared = prepare_commitment_step(db, KEY)
    higher = db.execute(
        "SELECT digest FROM discovery_commitment_frontier WHERE level = 2"
    ).fetchone()[0]
    with write(db), pytest.raises(StaleCommitmentWork, match="skipped"):
        apply_commitment_step(
            db, KEY, replace(prepared, level=2, frontier_digest=higher, result_digest=higher)
        )
    db.execute("DELETE FROM discovery_commitment_frontier WHERE level = 1")
    advance(db)
    advance(db)
    with pytest.raises(StaleCommitmentWork, match="frontier loss"):
        prepare_commitment_step(db, KEY)
    assert commitment_progress(db, KEY).phase == "fold"


def test_terminal_cannot_bypass_unfinished_source_or_carry(db):
    append(db, 0)
    with pytest.raises(StaleCommitmentWork):
        begin_fold(db, 1)
    drain_carry(db)
    with pytest.raises(StaleCommitmentWork):
        begin_fold(db, 0)
    begin_fold(db, 1)
    with pytest.raises(StaleCommitmentWork):
        append(db, 1)
    advance(db)
    advance(db)
    with pytest.raises(StaleCommitmentWork):
        prepare_commitment_step(db, KEY)


def test_owner_transaction_and_exact_registration_are_required(db):
    with pytest.raises(ValueError, match="owner-managed"):
        append_commitment_leaf(
            db, KEY, expected_version=0, source_key=b"0", next_source_key=b"1", digest=leaf(0)
        )
    with write(db), pytest.raises(ValueError, match="outside"):
        prepare_commitment_step(db, KEY)
    with write(db), pytest.raises(StaleCommitmentWork):
        begin_commitment(db, KEY, initial_source_key=b"changed")
    with write(db):
        assert begin_commitment(db, KEY, initial_source_key=b"0").version == 0
    with write(db), pytest.raises(ValueError, match="no active transaction"):
        initialize_commitment_work(db)


def test_foreign_work_format_is_not_silently_reinitialized(tmp_path):
    with sqlite3.connect(tmp_path / "foreign.sqlite3") as db:
        db.execute("CREATE TABLE discovery_commitment_format(format TEXT PRIMARY KEY)")
        db.execute("INSERT INTO discovery_commitment_format VALUES ('foreign')")
        db.commit()
        with pytest.raises(StaleCommitmentWork, match="unsupported"):
            initialize_commitment_work(db)


@pytest.mark.parametrize(
    "fragment",
    [b"", b"x" * 4097],
    ids=("empty", "oversized"),
)
def test_invalid_fragment_width_is_not_a_different_leaf_protocol(fragment):
    with pytest.raises(ValueError):
        fact_fragment_leaf(b"value", b"key", 0, fragment)


@pytest.mark.parametrize("octets,fragments", [(0, 1), (1, 0), (4096, 2), (4097, 1), (-1, 0)])
def test_record_terminal_requires_exact_fixed_fragment_totals(octets, fragments):
    with pytest.raises(ValueError):
        fact_terminal_leaf(b"value", b"key", octets=octets, fragments=fragments)


@pytest.mark.parametrize("ordinal", [True, -1, 1.0])
def test_leaf_ordinal_requires_its_exact_integer_type(ordinal):
    with pytest.raises(ValueError):
        fact_fragment_leaf(b"value", b"key", ordinal, b"x")


class RecordedCursor:
    def __init__(self, actual, recorder):
        self.actual, self.recorder = actual, recorder

    def fetchone(self):
        row = self.actual.fetchone()
        self.recorder.metrics["returned_rows"] += int(row is not None)
        return row

    def close(self):
        self.actual.close()


class RecordedConnection:
    def __init__(self, actual):
        self.actual = actual
        self.metrics = {"sql_statements": 0, "returned_rows": 0, "hashed_octets": 0}

    @property
    def in_transaction(self):
        return self.actual.in_transaction

    def execute(self, sql, parameters=()):
        self.metrics["sql_statements"] += 1
        return RecordedCursor(self.actual.execute(sql, parameters), self)


@pytest.mark.growth_witness(
    "canonical-discovery-merkle-step-preparation/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_commitment.prepare_commitment_step",
)
def test_actual_preparation_has_fixed_reads_and_one_pair_hash_across_frontiers(
    monkeypatch, record_property
):
    import riverhog_core.canonical_discovery_commitment as owner
    from riverhog_canonical_json import canonical_json_bytes
    from riverhog_growth_contracts import (
        growth_binding,
        project_bindings,
        validate_execution_samples,
    )

    binding = growth_binding(prepare_commitment_step)
    projection = project_bindings((binding,))
    declaration_sha = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    actual_sha256 = hashlib.sha256
    samples = []
    for count in (0, 1, 3, 31, 1023):
        db = open_work(":memory:")
        try:
            add_leaves(db, count)
            if count:
                append(db, count)
            else:
                begin_fold(db, 0)
            while True:
                levels = db.execute(
                    "SELECT count(*) FROM discovery_commitment_frontier"
                ).fetchone()[0]
                recorder = RecordedConnection(db)

                def observed_hash(value=b"", recorder=recorder, **kwargs):
                    recorder.metrics["hashed_octets"] += len(value)
                    return actual_sha256(value, **kwargs)

                with monkeypatch.context() as patch:
                    patch.setattr(owner.hashlib, "sha256", observed_hash)
                    prepared = prepare_commitment_step(recorder, KEY)
                sample = {
                    "declaration": binding.declaration.id,
                    "declaration_sha256": declaration_sha,
                    "dimensions": {"frontier_levels": levels},
                    "metrics": dict(recorder.metrics),
                    "delegates": {},
                }
                validate_execution_samples(projection, binding.declaration.id, [sample])
                samples.append(sample)
                with write(db):
                    progress = apply_commitment_step(db, KEY, prepared)
                if progress.phase == "complete":
                    break
                if progress.phase == "leaves" and progress.pending_digest is None:
                    begin_fold(db, count + 1)
            assert commitment_progress(db, KEY).root == oracle(
                [leaf(i) for i in range(count + 1 if count else 0)]
            )
        finally:
            db.close()
    record_property("growth_samples", samples)


@pytest.mark.parametrize(
    "change",
    [
        "fold_leaf_count = 2",
        "fold_level = 20",
        "pending_level = 20",
        "root = zeroblob(32)",
    ],
)
def test_corrupt_durable_checkpoint_is_an_explicit_failure(db, change):
    append(db, 0)
    db.execute("UPDATE discovery_commitment_tasks SET " + change)
    with pytest.raises(StaleCommitmentWork, match="invalid durable"):
        commitment_progress(db, KEY)


def test_frontier_level_cannot_allocate_a_carry_beyond_admitted_leaves(db):
    add_leaves(db, 1)
    begin_fold(db, 1)
    db.execute("UPDATE discovery_commitment_frontier SET level = 30")
    with pytest.raises(StaleCommitmentWork, match="frontier level"):
        prepare_commitment_step(db, KEY)


def test_global_counts_and_versions_cross_sqlite_integer_extent_without_rounding(db):
    # A synthetic work checkpoint tests representation, not source coverage.
    count = 2**63
    prior = leaf(0)
    db.execute(
        "UPDATE discovery_commitment_tasks SET version=?,leaf_count=?,next_source_key=?",
        (str(count), str(count), str(count).encode()),
    )
    db.execute(
        "INSERT INTO discovery_commitment_frontier VALUES (?,?,?,?)",
        (KEY.build_id, KEY.task_id, "63", prior),
    )
    value = leaf(1)
    with write(db):
        current = append_commitment_leaf(
            db,
            KEY,
            expected_version=count,
            source_key=str(count).encode(),
            next_source_key=str(count + 1).encode(),
            digest=value,
        )
    assert current.version == count + 1 and current.leaf_count == count + 1
    drain_carry(db)
    begin_fold(db, count + 1)
    while commitment_progress(db, KEY).phase != "complete":
        advance(db)
    final = commitment_progress(db, KEY)
    assert final.leaf_count == final.fold_leaf_count == count + 1
    assert final.root == branch_digest(prior, value)
    raw = db.execute(
        "SELECT typeof(version),typeof(leaf_count),typeof(fold_leaf_count) "
        "FROM discovery_commitment_tasks"
    ).fetchone()
    assert raw == ("text", "text", "text")


def test_numeric_frontier_order_across_decimal_widths(db):
    # Independent retained subtree digests exercise 2, 9, 10 ordering.
    levels = (2, 9, 10)
    count = sum(1 << level for level in levels)
    db.execute(
        "UPDATE discovery_commitment_tasks SET leaf_count=?,next_source_key=?",
        (str(count), str(count).encode()),
    )
    for level in levels:
        db.execute(
            "INSERT INTO discovery_commitment_frontier VALUES (?,?,?,?)",
            (KEY.build_id, KEY.task_id, str(level), leaf(level)),
        )
    begin_fold(db, count)
    observed = []
    while commitment_progress(db, KEY).phase != "complete":
        prepared, _ = advance(db)
        if prepared.operation == "fold":
            observed.append(prepared.level)
    assert observed == list(levels)
    assert commitment_progress(db, KEY).root == branch_digest(
        leaf(10), branch_digest(leaf(9), leaf(2))
    )


@pytest.mark.parametrize("value", ["01", "-1", "1.0", "", "1e20"])
def test_noncanonical_decimal_checkpoint_is_rejected(db, value):
    db.execute("UPDATE discovery_commitment_tasks SET leaf_count=?", (value,))
    with pytest.raises(StaleCommitmentWork, match="invalid durable"):
        commitment_progress(db, KEY)
