from __future__ import annotations

import hashlib
import sqlite3
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path

import pytest
from riverhog_core.canonical_discovery_fact_work import (
    FACT_FRAGMENT_BYTES,
    FactExtractionCursor,
    FactSegmentConflict,
    FactSegmentWrite,
    FactStackFrame,
    FactStreamKey,
    FactStreamTerminal,
    FactTaskKey,
    FactTaskProgress,
    StaleFactTask,
    advance_assertion,
    begin_assertion,
    fact_extraction_cursor,
    fact_stack_top,
    fact_task_progress,
    finish_assertion,
    finish_fact_stream,
    initialize_fact_work,
    stage_fact_segment,
)

SOURCE = hashlib.sha256(b"exact admitted journal source").hexdigest()
ORIGIN = hashlib.sha256(b"exact verified entry header").hexdigest()
CANONICAL = hashlib.sha256(b"exact canonical assertion").hexdigest()
TASK = FactTaskKey("build", "assertion-task", SOURCE, "worker-attempt")


def stream(label: str) -> FactStreamKey:
    return FactStreamKey("scalar", hashlib.sha256(label.encode()).hexdigest())


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


def begin(db):
    return begin_assertion(
        db, TASK, assertion_id="assertion", origin_sha256=ORIGIN, canonical_stream_ref=CANONICAL
    )


@pytest.fixture
def db(tmp_path: Path):
    connection = sqlite3.connect(tmp_path / "facts.sqlite3", isolation_level=None)
    initialize_fact_work(connection)
    with write(connection):
        begin(connection)
    try:
        yield connection
    finally:
        connection.close()


def test_segment_terminals_and_exact_retries_survive_database_reopen(db) -> None:
    first = stream("long value")
    empty = stream("empty value")
    content = b"x" * FACT_FRAGMENT_BYTES + b"\x00\xfftail"
    with write(db):
        assert stage_fact_segment(
            db, TASK, first, ordinal=0, exact_bytes=content[:4096], expected_version=0
        ) == FactTaskProgress(1, 1, 1, 1, 4096, False)
        assert (
            stage_fact_segment(
                db, TASK, first, ordinal=1, exact_bytes=content[4096:], expected_version=1
            ).version
            == 2
        )
        assert finish_fact_stream(
            db, TASK, empty, fragments=0, octets=0, expected_version=2
        ) == FactTaskProgress(3, 2, 1, 2, len(content), False)
    path = db.execute("PRAGMA database_list").fetchone()[2]
    reopened = sqlite3.connect(path, isolation_level=None)
    try:
        initialize_fact_work(reopened)
        with write(reopened):
            assert begin(reopened).version == 3
            assert (
                stage_fact_segment(
                    reopened, TASK, first, ordinal=0, exact_bytes=content[:4096], expected_version=0
                ).version
                == 3
            )
            assert (
                finish_fact_stream(
                    reopened, TASK, first, fragments=2, octets=len(content), expected_version=3
                ).version
                == 4
            )
            assert complete_source_cursor(reopened) == 9
            assert finish_assertion(
                reopened, TASK, source_terminal=CANONICAL, expected_version=9
            ) == FactTaskProgress(10, 2, 0, 2, len(content), True)
        with write(reopened):
            assert (
                finish_assertion(
                    reopened, TASK, source_terminal=CANONICAL, expected_version=9
                ).version
                == 10
            )
            assert (
                finish_fact_stream(
                    reopened, TASK, empty, fragments=0, octets=0, expected_version=2
                ).version
                == 10
            )
            assert (
                stage_fact_segment(
                    reopened, TASK, first, ordinal=1, exact_bytes=content[4096:], expected_version=1
                ).version
                == 10
            )
        stored = reopened.execute(
            "SELECT exact_bytes FROM discovery_fact_segments ORDER BY ordinal"
        ).fetchall()
        assert b"".join(row[0] for row in stored) == content
        assert reopened.execute(
            "SELECT fragments, octets, complete FROM discovery_fact_streams WHERE record_key = ?",
            (empty.record_key,),
        ).fetchone() == (0, 0, 1)
    finally:
        reopened.close()


@pytest.mark.parametrize(
    "fault", ["gap", "bytes", "retry-version", "version", "short-tail", "terminal"]
)
def test_conflicting_work_does_not_change_the_durable_checkpoint(db, fault) -> None:
    selected = stream("value")
    with write(db):
        stage_fact_segment(db, TASK, selected, ordinal=0, exact_bytes=b"abc", expected_version=0)
    before = fact_task_progress(db, TASK)
    with pytest.raises((FactSegmentConflict, StaleFactTask)):
        with write(db):
            if fault == "gap":
                stage_fact_segment(
                    db, TASK, selected, ordinal=2, exact_bytes=b"x", expected_version=1
                )
            elif fault == "bytes":
                stage_fact_segment(
                    db, TASK, selected, ordinal=0, exact_bytes=b"abd", expected_version=0
                )
            elif fault == "retry-version":
                stage_fact_segment(
                    db, TASK, selected, ordinal=0, exact_bytes=b"abc", expected_version=1
                )
            elif fault == "version":
                finish_fact_stream(db, TASK, selected, fragments=1, octets=3, expected_version=0)
            elif fault == "short-tail":
                stage_fact_segment(
                    db, TASK, selected, ordinal=1, exact_bytes=b"x", expected_version=1
                )
            else:
                finish_fact_stream(db, TASK, selected, fragments=1, octets=4, expected_version=1)
    assert fact_task_progress(db, TASK) == before
    assert db.execute("SELECT exact_bytes FROM discovery_fact_segments").fetchall() == [(b"abc",)]


def test_empty_streams_and_complete_streams_reject_new_data(db) -> None:
    selected = stream("empty")
    with write(db):
        finish_fact_stream(db, TASK, selected, fragments=0, octets=0, expected_version=0)
    before = fact_task_progress(db, TASK)
    with pytest.raises(FactSegmentConflict):
        with write(db):
            stage_fact_segment(db, TASK, selected, ordinal=0, exact_bytes=b"x", expected_version=1)
    assert fact_task_progress(db, TASK) == before
    with pytest.raises(FactSegmentConflict):
        with write(db):
            finish_fact_stream(db, TASK, selected, fragments=1, octets=1, expected_version=0)


@pytest.mark.parametrize("field", ["build_id", "source_key", "worker_id"])
def test_changed_worker_source_or_build_cannot_replay_existing_data(db, field) -> None:
    selected = stream("bound value")
    with write(db):
        stage_fact_segment(db, TASK, selected, ordinal=0, exact_bytes=b"exact", expected_version=0)
    changed = replace(TASK, **{field: "f" * 64 if field == "source_key" else "replacement"})
    with pytest.raises(StaleFactTask):
        with write(db):
            stage_fact_segment(
                db, changed, selected, ordinal=0, exact_bytes=b"exact", expected_version=0
            )
    assert fact_task_progress(db, TASK).version == 1


@pytest.mark.parametrize("fault", ["origin", "source-reference", "assertion"])
def test_changed_assertion_source_obligation_is_rejected(db, fault) -> None:
    with pytest.raises(StaleFactTask):
        with write(db):
            begin_assertion(
                db,
                TASK,
                assertion_id="replacement" if fault == "assertion" else "assertion",
                origin_sha256="f" * 64 if fault == "origin" else ORIGIN,
                canonical_stream_ref="f" * 64 if fault == "source-reference" else CANONICAL,
            )
    assert fact_task_progress(db, TASK) == FactTaskProgress(0, 0, 0, 0, 0, False)


def test_assertion_cannot_finish_with_open_stream_or_changed_source(db) -> None:
    selected = stream("unfinished")
    with write(db):
        stage_fact_segment(db, TASK, selected, ordinal=0, exact_bytes=b"exact", expected_version=0)
    with pytest.raises(FactSegmentConflict):
        with write(db):
            finish_assertion(db, TASK, source_terminal=CANONICAL, expected_version=1)
    with pytest.raises(StaleFactTask):
        with write(db):
            finish_assertion(db, TASK, source_terminal="f" * 64, expected_version=1)
    assert fact_task_progress(db, TASK) == FactTaskProgress(1, 1, 1, 1, 5, False)


def test_interruption_rolls_back_segment_and_parent_checkpoint_together(tmp_path: Path) -> None:
    class Stopped(BaseException):
        pass

    class InterruptedConnection(sqlite3.Connection):
        interrupt_segment = False

        def execute(self, sql, parameters=()):
            result = super().execute(sql, parameters)
            if self.interrupt_segment and sql.startswith("INSERT INTO discovery_fact_segments"):
                raise Stopped
            return result

    path = tmp_path / "interrupted.sqlite3"
    connection = sqlite3.connect(path, isolation_level=None, factory=InterruptedConnection)
    initialize_fact_work(connection)
    with write(connection):
        begin(connection)
    connection.interrupt_segment = True
    try:
        with pytest.raises(Stopped):
            with write(connection):
                stage_fact_segment(
                    connection,
                    TASK,
                    stream("interrupted"),
                    ordinal=0,
                    exact_bytes=b"preserved",
                    expected_version=0,
                )
    finally:
        connection.close()
    with sqlite3.connect(path, isolation_level=None) as resumed:
        initialize_fact_work(resumed)
        assert fact_task_progress(resumed, TASK) == FactTaskProgress(0, 0, 0, 0, 0, False)
        assert resumed.execute("SELECT * FROM discovery_fact_streams").fetchall() == []
        assert resumed.execute("SELECT * FROM discovery_fact_segments").fetchall() == []
        with write(resumed):
            assert (
                stage_fact_segment(
                    resumed,
                    TASK,
                    stream("interrupted"),
                    ordinal=0,
                    exact_bytes=b"preserved",
                    expected_version=0,
                ).version
                == 1
            )


def test_owner_transaction_and_exact_private_format_are_required(tmp_path: Path) -> None:
    with sqlite3.connect(tmp_path / "format.sqlite3", isolation_level=None) as connection:
        initialize_fact_work(connection)
        with pytest.raises(ValueError, match="owner-managed"):
            begin(connection)
        with write(connection):
            with pytest.raises(ValueError, match="no active"):
                initialize_fact_work(connection)
        connection.execute("UPDATE discovery_work_format SET format = 'another-format'")
        with pytest.raises(StaleFactTask, match="unsupported"):
            initialize_fact_work(connection)


@pytest.mark.parametrize("ordinal,data", [(-1, b"x"), (0, b""), (0, b"x" * 4097)])
def test_invalid_fragment_input_cannot_create_partial_streams(db, ordinal, data) -> None:
    with pytest.raises(ValueError):
        with write(db):
            stage_fact_segment(
                db,
                TASK,
                stream("invalid"),
                ordinal=ordinal,
                exact_bytes=data,
                expected_version=0,
            )
    assert fact_task_progress(db, TASK) == FactTaskProgress(0, 0, 0, 0, 0, False)


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
        self.metrics = {
            "sql_statements": 0,
            "returned_rows": 0,
            "hashed_octets": 0,
            "bound_fragment_octets": 0,
        }

    @property
    def in_transaction(self):
        return self.actual.in_transaction

    def execute(self, sql, parameters=()):
        self.metrics["sql_statements"] += 1
        self.metrics["bound_fragment_octets"] += sum(
            len(value) for value in parameters if isinstance(value, bytes)
        )
        return RecordedCursor(self.actual.execute(sql, parameters), self)


@pytest.mark.growth_witness(
    "canonical-discovery-fact-segment-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_fact_work.stage_fact_segment",
)
def test_segment_transition_reads_only_exact_work_across_backlogs(
    tmp_path, monkeypatch, record_property
):
    import riverhog_core.canonical_discovery_fact_work as owner
    from riverhog_canonical_json import canonical_json_bytes
    from riverhog_growth_contracts import (
        growth_binding,
        project_bindings,
        validate_execution_samples,
    )

    binding = growth_binding(stage_fact_segment)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    actual_sha256 = hashlib.sha256
    records = []
    for backlog, width in ((0, 1), (17, 4096), (1025, 4096)):
        with sqlite3.connect(
            tmp_path / f"backlog-{backlog}.sqlite3", isolation_level=None
        ) as current:
            initialize_fact_work(current)
            with write(current):
                begin(current)
                for index in range(backlog):
                    version = index * 2
                    selected = stream(f"prior-{index}")
                    stage_fact_segment(
                        current,
                        TASK,
                        selected,
                        ordinal=0,
                        exact_bytes=b"prior",
                        expected_version=version,
                    )
                    finish_fact_stream(
                        current,
                        TASK,
                        selected,
                        fragments=1,
                        octets=5,
                        expected_version=version + 1,
                    )
            selected = stream("current")
            version = backlog * 2
            for case in ("new", "retry", "changed", "source", "version", "interrupted"):
                recorder = RecordedConnection(current)

                def observed_hash(value=b"", recorder=recorder, **kwargs):
                    recorder.metrics["hashed_octets"] += len(value)
                    return actual_sha256(value, **kwargs)

                key = replace(TASK, source_key="f" * 64) if case == "source" else TASK
                data = b"y" * width if case == "changed" else b"x" * width
                expected = version + 1 if case == "version" else version
                target = stream("interrupted") if case == "interrupted" else selected
                if case == "interrupted":
                    current.set_authorizer(
                        lambda action, table, *_: (
                            sqlite3.SQLITE_DENY
                            if action == sqlite3.SQLITE_INSERT
                            and table == "discovery_fact_segments"
                            else sqlite3.SQLITE_OK
                        )
                    )
                current.execute("BEGIN IMMEDIATE")
                try:
                    with monkeypatch.context() as patch:
                        patch.setattr(owner.hashlib, "sha256", observed_hash)
                        if case in {"new", "retry"}:
                            result = stage_fact_segment(
                                recorder,
                                key,
                                target,
                                ordinal=0,
                                exact_bytes=data,
                                expected_version=expected,
                            )
                            assert result.version == version + 1
                        else:
                            if case == "interrupted":
                                expected = version + 1
                            with pytest.raises(
                                (FactSegmentConflict, StaleFactTask, sqlite3.DatabaseError)
                            ):
                                stage_fact_segment(
                                    recorder,
                                    key,
                                    target,
                                    ordinal=0,
                                    exact_bytes=data,
                                    expected_version=expected,
                                )
                    sample = {
                        "declaration": binding.declaration.id,
                        "declaration_sha256": digest,
                        "dimensions": {"fragment_octets": width},
                        "metrics": dict(recorder.metrics),
                        "delegates": {},
                    }
                    validate_execution_samples(projection, binding.declaration.id, [sample])
                    records.append(sample)
                finally:
                    current.set_authorizer(None)
                    current.commit()
                assert fact_task_progress(current, TASK).version == version + 1
                assert fact_task_progress(current, TASK).streams == backlog + 1
    record_property("growth_samples", records)


def complete_source_cursor(db):
    progress, cursor = fact_extraction_cursor(db, TASK)
    categories = ("canonical", "scalars", "names", "profiles", "edges", "done")
    for category in categories[categories.index(cursor.category) + 1 :]:
        cursor = replace(cursor, category=category)
        progress = advance_assertion(
            db, TASK, expected_version=progress.version, next_cursor=cursor
        )
    return progress.version


def test_extraction_action_stack_and_cursor_reopen_at_exact_physical_position(db) -> None:
    selected = stream("canonical assertion")
    first = FactExtractionCursor(value_position=4096, fragment_ordinal=1, stack_depth=1)
    with write(db):
        assert (
            advance_assertion(
                db,
                TASK,
                expected_version=0,
                next_cursor=first,
                segment=FactSegmentWrite(selected, 0, b"x" * 4096),
                stack_action="push",
                stack_frame=FactStackFrame(17, 0, 3),
            ).version
            == 1
        )
    path = db.execute("PRAGMA database_list").fetchone()[2]
    with sqlite3.connect(path, isolation_level=None) as resumed:
        initialize_fact_work(resumed)
        progress, cursor = fact_extraction_cursor(resumed, TASK)
        assert progress.version == 1 and cursor == first
        assert fact_stack_top(resumed, TASK) == FactStackFrame(17, 0, 3)
        second = replace(first, value_position=4099, fragment_ordinal=2)
        with write(resumed):
            advance_assertion(
                resumed,
                TASK,
                expected_version=1,
                next_cursor=second,
                segment=FactSegmentWrite(selected, 1, b"end"),
                stack_action="replace",
                stack_frame=FactStackFrame(17, 1, 3),
            )
            assert fact_stack_top(resumed, TASK) == FactStackFrame(17, 1, 3)
            advance_assertion(
                resumed,
                TASK,
                expected_version=2,
                next_cursor=second,
                terminal=FactStreamTerminal(selected, 2, 4099),
            )
            final = replace(second, stack_depth=0)
            advance_assertion(
                resumed, TASK, expected_version=3, next_cursor=final, stack_action="pop"
            )
            assert fact_stack_top(resumed, TASK) is None
            assert complete_source_cursor(resumed) == 9
            assert finish_assertion(
                resumed, TASK, source_terminal=CANONICAL, expected_version=9
            ) == FactTaskProgress(10, 1, 0, 2, 4099, True)


def test_failed_category_transition_rolls_back_its_prepared_fragment_and_cursor(db) -> None:
    with pytest.raises(FactSegmentConflict, match="unterminated"):
        with write(db):
            advance_assertion(
                db,
                TASK,
                expected_version=0,
                next_cursor=FactExtractionCursor(category="scalars"),
                segment=FactSegmentWrite(stream("unfinished canonical"), 0, b"exact"),
            )
    assert fact_extraction_cursor(db, TASK) == (
        FactTaskProgress(0, 0, 0, 0, 0, False),
        FactExtractionCursor(),
    )
    assert db.execute("SELECT * FROM discovery_fact_segments").fetchall() == []


def test_assertion_requires_terminal_extraction_category_even_without_open_streams(db) -> None:
    with pytest.raises(FactSegmentConflict, match="checkpoint is unfinished"):
        with write(db):
            finish_assertion(db, TASK, source_terminal=CANONICAL, expected_version=0)
    with write(db):
        version = complete_source_cursor(db)
        assert version == 5
        assert finish_assertion(
            db, TASK, source_terminal=CANONICAL, expected_version=version
        ).complete


@pytest.mark.parametrize("depth", [1, 17, 129, 1025])
def test_deep_stack_stores_frames_separately_and_reads_only_its_exact_tip(db, depth) -> None:
    cursor = FactExtractionCursor()
    with write(db):
        for index in range(depth):
            cursor = replace(cursor, stack_depth=index + 1)
            advance_assertion(
                db,
                TASK,
                expected_version=index,
                next_cursor=cursor,
                stack_action="push",
                stack_frame=FactStackFrame(index, 0, index),
            )
    statements = []
    db.set_trace_callback(statements.append)
    try:
        assert fact_stack_top(db, TASK) == FactStackFrame(depth - 1, 0, depth - 1)
    finally:
        db.set_trace_callback(None)
    assert len(statements) == 3
    assert not any("ORDER BY" in query or "COUNT(" in query for query in statements)
    assert db.execute("SELECT COUNT(*) FROM discovery_fact_stack").fetchone() == (depth,)
    assert len(db.execute("SELECT * FROM discovery_fact_cursors").fetchone()) == 11


@pytest.mark.parametrize("fault", ["skip", "depth", "negative", "hash", "stack", "backwards"])
def test_malformed_cursor_cannot_commit_any_extraction_work(db, fault) -> None:
    cursor = FactExtractionCursor()
    if fault == "backwards":
        with write(db):
            advance_assertion(
                db,
                TASK,
                expected_version=0,
                next_cursor=FactExtractionCursor(record_ordinal=1),
            )
    version = fact_task_progress(db, TASK).version
    before = fact_extraction_cursor(db, TASK)
    cursor = {
        "skip": replace(cursor, category="profiles"),
        "depth": replace(cursor, stack_depth=1),
        "negative": replace(cursor, source_position=-1),
        "hash": replace(cursor, hash_state="x" * 513),
        "stack": cursor,
        "backwards": cursor,
    }[fault]
    with pytest.raises((ValueError, FactSegmentConflict)):
        with write(db):
            advance_assertion(
                db,
                TASK,
                expected_version=version,
                next_cursor=cursor,
                segment=FactSegmentWrite(stream("must not commit"), 0, b"data"),
                stack_action="pop" if fault == "stack" else "none",
            )
    assert fact_extraction_cursor(db, TASK) == before


def test_replayed_advance_cannot_move_the_extraction_checkpoint_twice(db) -> None:
    cursor = FactExtractionCursor(value_position=3, fragment_ordinal=1)
    with write(db):
        advance_assertion(
            db,
            TASK,
            expected_version=0,
            next_cursor=cursor,
            segment=FactSegmentWrite(stream("value"), 0, b"abc"),
        )
    with pytest.raises(StaleFactTask):
        with write(db):
            advance_assertion(
                db,
                TASK,
                expected_version=0,
                next_cursor=cursor,
                segment=FactSegmentWrite(stream("value"), 0, b"abc"),
            )
    assert fact_extraction_cursor(db, TASK) == (FactTaskProgress(1, 1, 1, 1, 3, False), cursor)


def test_interrupted_cursor_commit_cannot_leave_advanced_data_or_stack(tmp_path: Path) -> None:
    class Stopped(BaseException):
        pass

    class InterruptedConnection(sqlite3.Connection):
        stop_on_cursor = False

        def execute(self, sql, parameters=()):
            result = super().execute(sql, parameters)
            if self.stop_on_cursor and sql.startswith("UPDATE discovery_fact_cursors"):
                raise Stopped
            return result

    path = tmp_path / "cursor-stop.sqlite3"
    db = sqlite3.connect(path, isolation_level=None, factory=InterruptedConnection)
    initialize_fact_work(db)
    with write(db):
        begin(db)
    db.stop_on_cursor = True
    try:
        with pytest.raises(Stopped):
            with write(db):
                advance_assertion(
                    db,
                    TASK,
                    expected_version=0,
                    next_cursor=FactExtractionCursor(stack_depth=1, value_position=3),
                    segment=FactSegmentWrite(stream("interrupted"), 0, b"abc"),
                    stack_action="push",
                    stack_frame=FactStackFrame(1, 2, 3),
                )
    finally:
        db.close()
    with sqlite3.connect(path, isolation_level=None) as resumed:
        initialize_fact_work(resumed)
        assert fact_extraction_cursor(resumed, TASK) == (
            FactTaskProgress(0, 0, 0, 0, 0, False),
            FactExtractionCursor(),
        )
        assert fact_stack_top(resumed, TASK) is None
        assert resumed.execute("SELECT * FROM discovery_fact_segments").fetchall() == []
        assert resumed.execute("SELECT * FROM discovery_fact_stack").fetchall() == []


def test_native_large_array_stages_by_saved_source_position_across_batches_and_reopen(
    tmp_path: Path,
) -> None:
    from riverhog_canonical_json import canonical_json_bytes
    from riverhog_provenance import validate_journal

    from tests.support.canonical_discovery import native_scalar_fixture

    fixture = native_scalar_fixture(20_001)
    with validate_journal(fixture.raw, catalog=fixture.catalog, require_profiles=True) as summary:
        document = summary.frames[0].document
        row = document["body"]["assertions"]["extensions"][0]
        values = row["value"]["value"]["data"]
        canonical = canonical_json_bytes(row)
        source_reference = hashlib.sha256(canonical).hexdigest()
        source_key = hashlib.sha256(
            canonical_json_bytes(
                {
                    "admission": "native-source-fixture",
                    "journal": summary.journal_id,
                    "prefix_sha256": summary.journal_sha256,
                }
            )
        ).hexdigest()
        key = FactTaskKey("large-build", "large-assertion", source_key, "source-worker")
        path = tmp_path / "native-fact-work.sqlite3"
        db = sqlite3.connect(path, isolation_level=None)
        db.execute("PRAGMA journal_mode = WAL")
        db.execute("PRAGMA synchronous = FULL")
        initialize_fact_work(db)
        with write(db):
            begin_assertion(
                db,
                key,
                assertion_id=fixture.assertion_id,
                origin_sha256=summary.anchor_at(0)["through"]["json_sha256"],
                canonical_stream_ref=source_reference,
            )
        canonical_key = stream("native canonical assertion")
        reopened = set()
        steps = 0
        try:
            while True:
                progress, cursor = fact_extraction_cursor(db, key)
                prepared = []
                for _ in range(128):
                    if cursor.category == "canonical":
                        offset = cursor.value_position
                        if offset < len(canonical):
                            block = canonical[offset : offset + FACT_FRAGMENT_BYTES]
                            segment = FactSegmentWrite(
                                canonical_key, cursor.fragment_ordinal, block
                            )
                            terminal = None
                            next_cursor = replace(
                                cursor,
                                value_position=offset + len(block),
                                fragment_ordinal=cursor.fragment_ordinal + 1,
                            )
                        else:
                            segment = None
                            terminal = FactStreamTerminal(
                                canonical_key, cursor.fragment_ordinal, len(canonical)
                            )
                            next_cursor = FactExtractionCursor(category="scalars")
                    else:
                        index = cursor.source_position
                        if index == len(values):
                            break
                        selected = FactStreamKey("profile-array-scalar", str(index))
                        if not cursor.value_position:
                            block = canonical_json_bytes(values[index])
                            segment = FactSegmentWrite(selected, 0, block)
                            terminal = None
                            next_cursor = replace(cursor, value_position=len(block))
                        else:
                            segment = None
                            terminal = FactStreamTerminal(selected, 1, cursor.value_position)
                            next_cursor = replace(
                                cursor,
                                record_ordinal=index + 1,
                                source_position=index + 1,
                                value_position=0,
                            )
                    prepared.append((next_cursor, segment, terminal))
                    cursor = next_cursor
                if not prepared:
                    break
                with write(db):
                    for next_cursor, segment, terminal in prepared:
                        progress = advance_assertion(
                            db,
                            key,
                            expected_version=progress.version,
                            next_cursor=next_cursor,
                            segment=segment,
                            terminal=terminal,
                        )
                        steps += 1
                if cursor.source_position >= 1000 * (len(reopened) + 1) and len(reopened) < 3:
                    reopened.add(cursor.source_position)
                    db.close()
                    db = sqlite3.connect(path, isolation_level=None)
                    initialize_fact_work(db)
                    assert fact_extraction_cursor(db, key) == (progress, cursor)
            progress, cursor = fact_extraction_cursor(db, key)
            assert cursor.category == "scalars" and cursor.source_position == len(values)
            assert cursor.value_position == 0
            assert len(reopened) == 3
            assert progress.version == steps
            assert progress.streams == len(values) + 1 and progress.open_streams == 0
            assert progress.fragments == len(values) + (len(canonical) + 4095) // 4096
            assert progress.octets == len(canonical) + sum(len(str(value)) for value in values)
            stored = db.execute(
                "SELECT exact_bytes FROM discovery_fact_segments "
                "WHERE build_id = ? AND task_id = ? "
                "AND kind = ? AND record_key = ? ORDER BY ordinal",
                (key.build_id, key.task_id, canonical_key.kind, canonical_key.record_key),
            )
            assert b"".join(part[0] for part in stored) == canonical
            for index, expected in enumerate(values):
                assert db.execute(
                    "SELECT exact_bytes FROM discovery_fact_segments "
                    "WHERE build_id = ? AND task_id = ? AND kind = 'profile-array-scalar' "
                    "AND record_key = ? AND ordinal = 0",
                    (key.build_id, key.task_id, str(index)),
                ).fetchone() == (canonical_json_bytes(expected),)
            # This fixture stages canonical bytes and the declared array. The
            # remaining extraction categories stay pending until their owners run.
            assert not progress.complete
            with pytest.raises(FactSegmentConflict, match="checkpoint is unfinished"):
                with write(db):
                    finish_assertion(
                        db,
                        key,
                        source_terminal=source_reference,
                        expected_version=progress.version,
                    )
        finally:
            db.close()


def test_distinct_source_ordinals_keep_identical_value_bytes_as_distinct_streams(db) -> None:
    first, second = FactStreamKey("scalar", "0"), FactStreamKey("scalar", "1")
    with write(db):
        stage_fact_segment(db, TASK, first, ordinal=0, exact_bytes=b"same", expected_version=0)
        finish_fact_stream(db, TASK, first, fragments=1, octets=4, expected_version=1)
        stage_fact_segment(db, TASK, second, ordinal=0, exact_bytes=b"same", expected_version=2)
        finish_fact_stream(db, TASK, second, fragments=1, octets=4, expected_version=3)
    assert fact_task_progress(db, TASK) == FactTaskProgress(4, 2, 0, 2, 8, False)
    assert db.execute(
        "SELECT record_key, exact_bytes FROM discovery_fact_segments ORDER BY record_key"
    ).fetchall() == [("0", b"same"), ("1", b"same")]
