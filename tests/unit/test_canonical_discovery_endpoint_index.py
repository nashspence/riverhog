from __future__ import annotations

import hashlib
import sqlite3
from collections import Counter, defaultdict
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_core import canonical_discovery_endpoint_index as index
from riverhog_core import canonical_discovery_endpoint_source as reader
from riverhog_core.canonical_discovery_fact_work import StaleFactTask
from riverhog_core.canonical_discovery_journal_extraction import JournalFactExtraction
from riverhog_core.canonical_discovery_journal_fragments import JournalFactCapacity
from riverhog_core.canonical_discovery_journal_work import (
    JournalTaskKey,
    QualifiedJournalSource,
    StaleJournalTask,
)
from riverhog_core.canonical_discovery_rows import core_edges
from riverhog_provenance import JournalFrame, validate_journal

from tests.support.canonical_discovery_work import (
    ADMISSION,
    corrected_journal_fixture,
    open_journal_work,
    write,
)
from tests.unit.test_canonical_discovery_endpoint_source import mixed_case as mixed_case
from tests.unit.test_canonical_discovery_endpoint_source import mixed_originals as mixed_originals
from tests.unit.test_canonical_discovery_journal_extraction import run

BEGIN = index.begin_endpoint_index
PROGRESS = index.endpoint_index_progress
PREPARE = index.prepare_endpoint_index
APPLY = index.apply_endpoint_index
CANDIDATE = index.next_endpoint_candidate


@pytest.fixture
def indexed_case(mixed_case):
    case = mixed_case
    index.initialize_endpoint_index(case.db)
    case.work = index.EndpointIndexWork(case.source, case.anchor)
    with write(case.db):
        initial = BEGIN(case.db, case.key, case.work)
    assert initial.version == 0
    return case


def advance(case):
    page = PREPARE(case.db, case.key, case.work, octet_budget=4096)
    with write(case.db):
        result = APPLY(case.db, case.key, case.work, page)
        assert APPLY(case.db, case.key, case.work, page) == result
    return page, result


def drive(case, *, on_step=None):
    pages = []
    while not PROGRESS(case.db, case.key, case.work).complete:
        page, result = advance(case)
        assert result.version == page.expected.version + 1
        assert result.endpoints - page.expected.endpoints in (0, 1)
        pages.append(page)
        if on_step:
            on_step(page)
    return pages


def before_edge(case):
    while True:
        progress = PROGRESS(case.db, case.key, case.work)
        if progress.phase == "streams":
            row = index._next_stream(case.db, case.key, progress)
            if row is not None and row[0] == "edge-header":
                return progress
        assert not progress.complete
        advance(case)


def rows(case):
    return case.db.execute(
        "SELECT " + index._ENDPOINT_COLUMNS + " FROM discovery_endpoints "
        "WHERE build_id=? AND source_key=? ORDER BY assertion_id,record_key",
        (case.key.build_id, case.key.source_key),
    ).fetchall()


def test_original_edges_are_indexed_once_without_source_decode_or_prefix_expansion(
    indexed_case, monkeypatch
):
    case = indexed_case
    expected = {
        (row["assertion_id"], str(ordinal)): edge
        for frame in case.native.frames
        for group in frame.document["body"].get("assertions", {}).values()
        for row in group
        for ordinal, edge in enumerate(core_edges(row))
    }
    assertion_count = case.db.execute(
        "SELECT count(*) FROM discovery_journal_assertions WHERE build_id=? AND source_key=?",
        (case.key.build_id, case.key.source_key),
    ).fetchone()[0]
    stream_count = case.db.execute(
        "SELECT sum(f.streams) FROM discovery_journal_assertions a JOIN discovery_fact_tasks f "
        "ON f.build_id=a.build_id AND f.task_id=a.task_id WHERE a.build_id=? AND a.source_key=?",
        (case.key.build_id, case.key.source_key),
    ).fetchone()[0]
    actual, calls = index.read_journal_endpoint, Counter()

    def read(*args, **kwargs):
        assert not args[0].in_transaction
        calls[args[4:6]] += 1
        return actual(*args, **kwargs)

    def forbidden(*args, **kwargs):
        pytest.fail("endpoint indexing reconstructed an original assertion")

    with monkeypatch.context() as patch:
        patch.setattr(index, "read_journal_endpoint", read)
        patch.setattr(case.source, "entry", forbidden)
        patch.setattr(case.source, "assertion", forbidden)
        patch.setattr(JournalFrame, "document", property(forbidden))
        pages = drive(case)
    result = PROGRESS(case.db, case.key, case.work)
    assert result.assertions == assertion_count
    assert result.endpoints == len(expected)
    assert len(pages) == stream_count + 2 * assertion_count + 1
    assert calls == Counter({coordinate: 1 for coordinate in expected})
    for row in rows(case):
        edge = expected[row[2:4]]
        exact = (
            edge.exact_foreign_reference
            if edge.target_scope == "external"
            else edge.target_id.encode()
        )
        assert row[6:12] == (
            edge.role,
            "context" if edge.role.startswith("context:") else edge.role,
            edge.target_scope,
            edge.target_type,
            hashlib.sha256(exact).hexdigest(),
            len(exact),
        )
        assert row[12:] == ("0", None)
    terminal = PREPARE(case.db, case.key, case.work, octet_budget=4096)
    with write(case.db):
        assert APPLY(case.db, case.key, case.work, terminal) == result


def test_cold_reopen_resumes_each_original_directory_coordinate(indexed_case):
    case = indexed_case
    visited = []

    def reopen(page):
        visited.append((page.expected.after, page.expected.stream_after, page.expected.phase))
        case.db.close()
        case.db = sqlite3.connect(case.path, isolation_level=None)
        case.work = index.EndpointIndexWork(case.source, case.anchor)
        with write(case.db):
            assert BEGIN(case.db, case.key, case.work) == page.result

    pages = drive(case, on_step=reopen)
    assert len(visited) == len(set(visited)) == len(pages)
    assert rows(case)


def test_output_and_cursor_rollback_together_and_exact_commit_retry_adds_nothing(indexed_case):
    case = indexed_case
    before = before_edge(case)
    page = PREPARE(case.db, case.key, case.work, octet_budget=4096)
    assert page.endpoint is not None
    case.db.execute("""
        CREATE TEMP TRIGGER refuse_endpoint_cursor BEFORE UPDATE ON discovery_endpoint_tasks
        WHEN NEW.endpoints != OLD.endpoints
        BEGIN SELECT RAISE(ABORT,'injected endpoint cursor failure'); END
    """).close()
    with write(case.db):
        with pytest.raises(sqlite3.IntegrityError, match="injected"):
            APPLY(case.db, case.key, case.work, page)
        assert PROGRESS(case.db, case.key, case.work) == before
        assert rows(case) == []
    case.db.execute("DROP TRIGGER refuse_endpoint_cursor").close()
    case.db.execute("BEGIN IMMEDIATE").close()
    APPLY(case.db, case.key, case.work, page)
    assert len(rows(case)) == 1
    case.db.rollback()
    assert rows(case) == []
    assert PROGRESS(case.db, case.key, case.work) == before
    with write(case.db):
        after = APPLY(case.db, case.key, case.work, page)
        assert APPLY(case.db, case.key, case.work, page) == after
    assert len(rows(case)) == 1 and after.endpoints == 1


def test_capacity_and_preparation_identity_preserve_the_unfinished_atom(indexed_case):
    case = indexed_case
    before = before_edge(case)
    with pytest.raises(JournalFactCapacity):
        PREPARE(case.db, case.key, case.work, octet_budget=1)
    assert PROGRESS(case.db, case.key, case.work) == before
    page = PREPARE(case.db, case.key, case.work, octet_budget=4096)
    with write(case.db):
        with pytest.raises(StaleJournalTask, match="exact detached"):
            APPLY(case.db, case.key, case.work, replace(page))
        with pytest.raises(ValueError, match="transaction"):
            PREPARE(case.db, case.key, case.work, octet_budget=4096)
    page = PREPARE(case.db, case.key, case.work, octet_budget=4096)
    PREPARE(case.db, case.key, case.work, octet_budget=4096)
    with write(case.db):
        with pytest.raises(StaleJournalTask, match="exact detached"):
            APPLY(case.db, case.key, case.work, page)
    assert PROGRESS(case.db, case.key, case.work) == before


@pytest.mark.parametrize(
    "damage",
    ("deleted-stream", "unfinished-stream", "stream-terminal", "missing-child", "changed-child"),
)
def test_original_directory_and_child_damage_cannot_become_index_completion(indexed_case, damage):
    case = indexed_case
    before = before_edge(case)
    child = before.after.key.task_id
    case.db.execute("PRAGMA foreign_keys=OFF").close()
    with write(case.db):
        if damage == "deleted-stream":
            case.db.execute(
                "DELETE FROM discovery_fact_streams WHERE build_id=? AND task_id=? "
                "AND kind='edge-header' AND record_key='0'",
                (case.key.build_id, child),
            ).close()
        elif damage in {"unfinished-stream", "stream-terminal"}:
            field, value = (
                ("complete", 0) if damage == "unfinished-stream" else ("terminal_version", 10**9)
            )
            case.db.execute(
                "UPDATE discovery_fact_streams SET " + field + "=? WHERE build_id=? AND task_id=? "
                "AND kind='edge-header' AND record_key='0'",
                (value, case.key.build_id, child),
            ).close()
        elif damage == "missing-child":
            case.db.execute(
                "DELETE FROM discovery_fact_cursors WHERE build_id=? AND task_id=?",
                (case.key.build_id, child),
            ).close()
            case.db.execute(
                "DELETE FROM discovery_fact_tasks WHERE build_id=? AND task_id=?",
                (case.key.build_id, child),
            ).close()
        else:
            case.db.execute(
                "UPDATE discovery_fact_tasks SET "
                "version=version+1,terminal_version=terminal_version+1 "
                "WHERE build_id=? AND task_id=?",
                (case.key.build_id, child),
            ).close()
    with pytest.raises((StaleJournalTask, StaleFactTask)):
        drive(case)
    assert not case.db.execute(
        "SELECT phase='complete' FROM discovery_endpoint_tasks WHERE build_id=? AND source_key=?",
        (case.key.build_id, case.key.source_key),
    ).fetchone()[0]


def test_stream_change_between_preparation_and_commit_is_rejected(indexed_case):
    case = indexed_case
    before = before_edge(case)
    page = PREPARE(case.db, case.key, case.work, octet_budget=4096)
    with write(case.db):
        case.db.execute(
            "UPDATE discovery_fact_streams SET complete=0 WHERE build_id=? AND task_id=? "
            "AND kind='edge-header' AND record_key=?",
            (case.key.build_id, before.after.key.task_id, page.endpoint.record_key),
        ).close()
    with write(case.db):
        with pytest.raises(StaleJournalTask, match="stream changed"):
            APPLY(case.db, case.key, case.work, page)
    assert rows(case) == []


@pytest.mark.parametrize(
    "damage", ("output", "header", "source-version", "worker", "source-closed")
)
def test_lost_index_controls_or_source_ownership_fail_closed(indexed_case, damage):
    case = indexed_case
    before_edge(case)
    advance(case)
    if damage == "source-closed":
        case.source.close()
    else:
        with write(case.db):
            if damage == "output":
                case.db.execute("DELETE FROM discovery_endpoints").close()
            elif damage == "header":
                case.db.execute("UPDATE discovery_endpoint_tasks SET algorithm='other'").close()
            elif damage == "source-version":
                case.db.execute("UPDATE discovery_journal_tasks SET version='999999'").close()
            else:
                case.db.execute(
                    "UPDATE discovery_endpoint_tasks SET worker_id='replacement'"
                ).close()
    with pytest.raises(StaleJournalTask):
        PROGRESS(case.db, case.key, case.work)


def test_partial_source_or_other_exact_anchor_is_not_an_index_input(indexed_case):
    case = indexed_case
    with pytest.raises(StaleJournalTask, match="complete original"):
        index.EndpointIndexWork(case.source, replace(case.anchor, prefix_sha256="f" * 64))
    with write(case.db):
        with pytest.raises(StaleJournalTask):
            BEGIN(case.db, replace(case.key, worker_id="other"), case.work)
    with pytest.raises(StaleJournalTask, match="completed source terminal"):
        CANDIDATE(
            case.db,
            case.key,
            case.work,
            role="value",
            target_scope="local",
            target_type="state",
            target_sha256="0" * 64,
        )


def test_retired_assertions_keep_exact_original_intervals_and_candidate_coordinates(tmp_path):
    fixture, raw, _target = corrected_journal_fixture()
    with validate_journal(raw, catalog=fixture.catalog) as native:
        with QualifiedJournalSource(native, ADMISSION) as source:
            db = open_journal_work(tmp_path / "interval.sqlite3")
            try:
                key = JournalTaskKey("build", source.source_key, "worker")
                run(JournalFactExtraction(db, key, source), 128)
                index.initialize_endpoint_index(db)
                work = index.EndpointIndexWork(
                    source, HistoryJournalAnchor.from_mapping(native.anchor)
                )
                case = SimpleNamespace(db=db, key=key, work=work)
                with write(db):
                    BEGIN(db, key, work)
                drive(case)
                old = [row for row in rows(case) if row[2] == fixture.assertion_id]
                assert old and all(row[12:] == ("0", "1") for row in old)
                candidate = old[0]
                found = CANDIDATE(
                    db,
                    key,
                    work,
                    role=candidate[7],
                    target_scope=candidate[8],
                    target_type=candidate[9],
                    target_sha256=candidate[10],
                )
                assert found == candidate
            finally:
                db.close()


def test_digest_collision_returns_candidates_without_claiming_original_byte_equality(
    indexed_case, monkeypatch
):
    case = indexed_case

    class Collision:
        def hexdigest(self):
            return "a" * 64

    monkeypatch.setattr(reader, "hashlib", SimpleNamespace(sha256=lambda raw: Collision()))
    drive(case)
    groups = defaultdict(list)
    for row in rows(case):
        groups[row[7:11]].append(row)
    assert len(groups) > 1
    for (role, scope, kind, digest), expected in groups.items():
        after, actual = None, []
        while (
            row := CANDIDATE(
                case.db,
                case.key,
                case.work,
                role=role,
                target_scope=scope,
                target_type=kind,
                target_sha256=digest,
                after=after,
            )
        ) is not None:
            actual.append(row)
            after = row[2:4]
        assert actual == expected
        for row in actual:
            endpoint = reader.read_journal_endpoint(
                case.db, case.key, case.source, case.anchor, row[2], row[3], octet_budget=4096
            )
            assert endpoint.assertion.key.task_id == row[4]
            assert endpoint.target_stream.record_key == row[3]
    assert (
        CANDIDATE(
            case.db,
            case.key,
            case.work,
            role="absent",
            target_scope="local",
            target_type="state",
            target_sha256="a" * 64,
        )
        is None
    )


def test_private_index_versions_preserve_arbitrary_width_controls(indexed_case):
    from riverhog_canonical_json import format_scalar

    case = indexed_case
    wide = "1" + "0" * 5000
    with write(case.db):
        case.db.execute("UPDATE discovery_endpoint_tasks SET version=?", (wide,)).close()
    before = PROGRESS(case.db, case.key, case.work)
    page, after = advance(case)
    assert page.expected == before
    assert format_scalar("nonnegative", after.version) == "1" + "0" * 4999 + "1"


@pytest.mark.parametrize("damage", ("missing", "changed"))
def test_saved_directory_record_remains_required_after_cursor_commit(indexed_case, damage):
    case = indexed_case
    before_edge(case)
    _page, after = advance(case)
    case.db.execute("PRAGMA foreign_keys=OFF").close()
    with write(case.db):
        if damage == "missing":
            sql = "DELETE FROM discovery_fact_streams "
        else:
            sql = "UPDATE discovery_fact_streams SET complete=0 "
        case.db.execute(
            sql + "WHERE build_id=? AND task_id=? AND kind=? AND record_key=?",
            (case.key.build_id, after.after.key.task_id, *after.stream_after),
        ).close()
    with pytest.raises(StaleJournalTask, match="directory record"):
        PROGRESS(case.db, case.key, case.work)
