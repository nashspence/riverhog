from __future__ import annotations

import copy
import json
import sqlite3
import time
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core import canonical_discovery_endpoint_index as index
from riverhog_core import canonical_discovery_endpoint_source as endpoint_source
from riverhog_core.canonical_discovery_archive_work import (
    admit_archive_journal,
    initialize_archive_work,
)
from riverhog_core.canonical_discovery_corpus_inputs import (
    CorpusSourceInputWork,
    complete_corpus_source_inputs,
    initialize_corpus_source_inputs,
)
from riverhog_core.canonical_discovery_journal_extraction import JournalFactExtraction
from riverhog_core.canonical_discovery_journal_fragments import JournalFactCapacity
from riverhog_core.canonical_discovery_journal_lookup import documentary_journal_assertion
from riverhog_core.canonical_discovery_journal_work import JournalTaskKey
from riverhog_core.canonical_discovery_outgoing_endpoints import next_assertion_endpoint
from riverhog_core.canonical_discovery_rows import core_edges
from riverhog_core.canonical_discovery_selector_work import initialize_selector_work
from riverhog_provenance import JournalFrame, assertion, validate_journal

from tests.support.canonical_discovery import native_scalar_fixture
from tests.support.canonical_discovery_work import open_journal_work, write
from tests.support.catalog_work import octets
from tests.unit.test_canonical_discovery_activity_selector_growth import exercise_activity
from tests.unit.test_canonical_discovery_activity_selectors import (
    APPLY as ACTIVITY_APPLY,
)
from tests.unit.test_canonical_discovery_activity_selectors import (
    PREPARE as ACTIVITY_PREPARE,
)
from tests.unit.test_canonical_discovery_corpus_inputs import commit_input, register_archive
from tests.unit.test_canonical_discovery_endpoint_index import (
    APPLY,
    BEGIN,
    CANDIDATE,
    PREPARE,
    PROGRESS,
    drive,
)
from tests.unit.test_canonical_discovery_endpoint_source import mixed_case as mixed_case
from tests.unit.test_canonical_discovery_endpoint_source import mixed_originals as mixed_originals
from tests.unit.test_canonical_discovery_journal_extraction import run
from tests.unit.test_canonical_discovery_journal_fragments_growth import dimensions, retain
from tests.unit.test_canonical_discovery_journal_lookup_growth import observe
from tests.unit.test_journal_validation_workspace_reference_growth import sample


@contextmanager
def observed(case, monkeypatch):
    actual_index_decode = index.require_canonical_json
    actual_endpoint_decode = endpoint_source.require_canonical_json
    actual_scalar = index.format_scalar
    with observe(case, monkeypatch) as recorder:

        def decoded(raw, actual):
            recorder.metrics["decoded_octets"] += len(raw)
            return actual(raw)

        def scalar(kind, value):
            result = actual_scalar(kind, value)
            recorder.metrics["scalar_encoded_octets"] += len(result.encode())
            return result

        with monkeypatch.context() as patch:
            patch.setattr(
                index, "require_canonical_json", lambda raw: decoded(raw, actual_index_decode)
            )
            patch.setattr(
                endpoint_source,
                "require_canonical_json",
                lambda raw: decoded(raw, actual_endpoint_decode),
            )
            patch.setattr(index, "format_scalar", scalar)
            patch.setattr(endpoint_source, "format_scalar", scalar)
            yield recorder


def values(case, base, progress, page=None, payload=0):
    header = case.db.execute(
        "SELECT * FROM discovery_endpoint_tasks WHERE build_id=? AND source_key=?",
        (case.key.build_id, case.key.source_key),
    ).fetchone()
    return {
        "retained_rows": base["retained_rows"] + progress.endpoints + int(header is not None),
        "metadata_octets": max(base["metadata_octets"], octets(header), octets((progress, page))),
        "endpoint_octets": payload,
    }


def exercise(case, monkeypatch, record_property, target):
    samples = []
    retain(case, 0, 1)
    retained, base = 1, dimensions(case)
    index.initialize_endpoint_index(case.db)
    case.work = index.EndpointIndexWork(case.source, case.anchor)

    def invoke(operation, inputs, call):
        if operation is not target:
            return call(case.db)
        with observed(case, monkeypatch) as recorder:
            result = call(recorder)
        samples.append(sample(operation, inputs, recorder.metrics))
        return result

    initial = index.EndpointIndexProgress(0, "assertions", 0, 0, None, None, 0, 0, 0, None, None)
    inputs = values(case, base, initial)
    with write(case.db):
        invoke(BEGIN, inputs, lambda db: BEGIN(db, case.key, case.work))
    phases, endpoints = set(), 0
    while True:
        current = PROGRESS(case.db, case.key, case.work)
        phases.add(current.phase)
        next_row = (
            index._next_stream(case.db, case.key, current) if current.phase == "streams" else None
        )
        if retained == 1 and next_row is not None and next_row[0] == "edge-header":
            retain(case, retained, 17)
            retained, base = 17, dimensions(case)
        elif retained == 17 and current.endpoints:
            retain(case, retained, 1025)
            retained, base = 1025, dimensions(case)
        payload = 0
        if next_row is not None and next_row[0] == "edge-header":
            payload = case.db.execute(
                "SELECT sum(octets) FROM discovery_fact_streams WHERE build_id=? AND task_id=? "
                "AND record_key=? AND kind IN "
                "('edge-header','edge-role','edge-target','edge-foreign')",
                (case.key.build_id, current.after.key.task_id, next_row[1]),
            ).fetchone()[0]
        inputs = values(case, base, current, payload=payload)
        if target is BEGIN:
            with write(case.db):
                invoke(BEGIN, inputs, lambda db: BEGIN(db, case.key, case.work))
        invoke(PROGRESS, inputs, lambda db: PROGRESS(db, case.key, case.work))
        if current.complete:
            break
        if payload and target is PREPARE:
            for _retry in range(2):
                with pytest.raises(JournalFactCapacity):
                    invoke(
                        PREPARE,
                        inputs,
                        lambda db: PREPARE(db, case.key, case.work, octet_budget=1),
                    )
            # Failed calls still produce primitive evidence.
            with observed(case, monkeypatch) as recorder:
                with pytest.raises(JournalFactCapacity):
                    PREPARE(recorder, case.key, case.work, octet_budget=1)
            samples.append(sample(PREPARE, inputs, recorder.metrics))
        page = invoke(
            PREPARE,
            inputs,
            lambda db: PREPARE(db, case.key, case.work, octet_budget=4096),
        )
        inputs = values(case, base, current, page=page, payload=payload)
        with write(case.db):
            result = invoke(
                APPLY, inputs, lambda db, _page=page: APPLY(db, case.key, case.work, _page)
            )
            assert (
                invoke(
                    APPLY,
                    values(case, base, result, page=page),
                    lambda db, _page=page: APPLY(db, case.key, case.work, _page),
                )
                == result
            )
        endpoints += int(page.endpoint is not None)
    assert retained == 1025 and endpoints == current.endpoints > 1
    assert phases == {"assertions", "streams", "complete"}
    candidates = case.db.execute(
        "SELECT index_role,target_scope,target_type,target_sha256 FROM discovery_endpoints "
        "WHERE build_id=? AND source_key=? GROUP BY 1,2,3,4",
        (case.key.build_id, case.key.source_key),
    ).fetchall()
    for role, scope, kind, digest in [*candidates, ("absent", "local", "state", "0" * 64)]:
        after = None
        while True:
            inputs = values(case, base, current)
            row = invoke(
                CANDIDATE,
                inputs,
                lambda db, role=role, scope=scope, kind=kind, digest=digest, after=after: CANDIDATE(
                    db,
                    case.key,
                    case.work,
                    role=role,
                    target_scope=scope,
                    target_type=kind,
                    target_sha256=digest,
                    after=after,
                ),
            )
            if row is None:
                break
            after = row[2:4]
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-endpoint-index-begin/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_endpoint_index.begin_endpoint_index",
)
def test_endpoint_index_registration_seeks_original_controls(
    mixed_case, monkeypatch, record_property
):
    exercise(mixed_case, monkeypatch, record_property, BEGIN)


@pytest.mark.growth_witness(
    "riverhog-endpoint-index-progress/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_endpoint_index.endpoint_index_progress",
)
def test_endpoint_index_progress_ignores_retained_pending_rows(
    mixed_case, monkeypatch, record_property
):
    exercise(mixed_case, monkeypatch, record_property, PROGRESS)


@pytest.mark.growth_witness(
    "riverhog-endpoint-index-prepare/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_endpoint_index.prepare_endpoint_index",
)
def test_endpoint_index_prepares_one_original_stream_or_boundary(
    mixed_case, monkeypatch, record_property
):
    exercise(mixed_case, monkeypatch, record_property, PREPARE)


@pytest.mark.growth_witness(
    "riverhog-endpoint-index-apply/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_endpoint_index.apply_endpoint_index",
)
def test_endpoint_index_commits_and_retries_one_atomic_cursor(
    mixed_case, monkeypatch, record_property
):
    exercise(mixed_case, monkeypatch, record_property, APPLY)


@pytest.mark.growth_witness(
    "riverhog-endpoint-index-candidate/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_endpoint_index.next_endpoint_candidate",
)
def test_endpoint_inverse_candidates_seek_exact_keys(mixed_case, monkeypatch, record_property):
    exercise(mixed_case, monkeypatch, record_property, CANDIDATE)


@pytest.fixture(scope="module", params=(1, 17, 1025))
def degree_index(request, tmp_path_factory):
    degree = request.param
    started = time.perf_counter()
    fixture = native_scalar_fixture(0)
    with validate_journal(fixture.raw, catalog=fixture.catalog) as original:
        document = copy.deepcopy(original.frames[0].document)
    recorder = document["recorded_by_agent_id"]
    context = assertion("context", recorder, kind="opaque", label="shared endpoint")
    graph = document["body"]["assertions"]
    graph.setdefault("contexts", []).append(context)
    graph["activities"][0]["contexts"] = [
        {"context_id": context["id"], "role": "other", "role_uri": "urn:test:edge:" + str(i)}
        for i in range(degree)
    ]
    activity = graph["activities"][0]
    expected = {
        str(i): edge
        for i, edge in enumerate(core_edges(activity))
        if edge.role.startswith("context:")
    }
    assert len(expected) == degree
    raw = JournalFrame(canonical_json_bytes(document)).encoded
    with validate_journal(raw, catalog=fixture.catalog) as native:
        print(
            json.dumps(
                {
                    "phase": "Native-qualified",
                    "degree": degree,
                    "seconds": time.perf_counter() - started,
                }
            ),
            flush=True,
        )
        path = tmp_path_factory.mktemp("native-endpoint-degree-" + str(degree)) / "degree.sqlite3"
        db = open_journal_work(path)
        db.execute("PRAGMA foreign_keys=ON").close()
        initialize_archive_work(db)
        initialize_corpus_source_inputs(db)
        initialize_selector_work(db)
        archive_key, admission = register_archive(db, raw, native)
        with admit_archive_journal(db, archive_key, admission, native) as source:
            case = SimpleNamespace(
                db=db,
                path=path,
                source=source,
                anchor=HistoryJournalAnchor.from_mapping(native.anchor),
                archive_key=archive_key,
                admission=admission,
                native=native,
            )
            try:
                commit_input(db, archive_key, CorpusSourceInputWork(admission, source))
                with write(db):
                    case.manifest = complete_corpus_source_inputs(db, archive_key, admission)
                case.key = JournalTaskKey(
                    archive_key.build_id, source.source_key, archive_key.worker_id
                )
                run(JournalFactExtraction(db, case.key, source), 128)
                print(
                    json.dumps(
                        {
                            "phase": "facts-qualified",
                            "degree": degree,
                            "seconds": time.perf_counter() - started,
                        }
                    ),
                    flush=True,
                )
                index.initialize_endpoint_index(db)
                case.work = index.EndpointIndexWork(source, case.anchor)
                with write(db):
                    BEGIN(db, case.key, case.work)
                drive(case)
                print(
                    json.dumps(
                        {
                            "phase": "index-qualified",
                            "degree": degree,
                            "seconds": time.perf_counter() - started,
                        }
                    ),
                    flush=True,
                )
                case.degree, case.activity, case.expected = degree, activity, expected
                yield case
            finally:
                case.db.close()


@pytest.mark.growth_witness(
    "riverhog-endpoint-index-native-high-degree/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_endpoint_index.next_endpoint_candidate",
)
def test_native_high_degree_edges_keep_independent_coordinates_and_bounded_inverse_reads(
    degree_index, monkeypatch, record_property
):
    case = degree_index
    db, source, path = case.db, case.source, case.path
    degree, activity, expected = case.degree, case.activity, case.expected
    progress = PROGRESS(db, case.key, case.work)
    base = dimensions(case)
    first = endpoint_source.read_journal_endpoint(
        db,
        case.key,
        source,
        case.anchor,
        activity["assertion_id"],
        next(iter(expected)),
        octet_budget=4096,
    )
    samples, actual, after = [], {}, None
    position = 0
    while True:
        arguments = dict(
            role="context",
            target_scope="local",
            target_type="context",
            target_sha256=first.target_sha256,
            after=after,
        )
        if position in {0, degree // 2, degree - 1, degree}:
            for cold in (False, True):
                if cold:
                    case.db.close()
                    case.db = sqlite3.connect(path, isolation_level=None)
                    case.db.execute("PRAGMA foreign_keys=ON").close()
                    db = case.db
                    case.work = index.EndpointIndexWork(source, case.anchor)
                inputs = values(case, base, progress)
                for _retry in range(3):
                    with observed(case, monkeypatch) as measured:
                        row = CANDIDATE(measured, case.key, case.work, **arguments)
                    samples.append(sample(CANDIDATE, inputs, measured.metrics))
        else:
            row = CANDIDATE(db, case.key, case.work, **arguments)
        if row is None:
            break
        assert row[2] == activity["assertion_id"] and row[3] not in actual
        actual[row[3]] = row[6]
        after = row[2:4]
        position += 1
    assert actual == {record: edge.role for record, edge in expected.items()}
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-original-outgoing-endpoint-native-degree/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_outgoing_endpoints.next_assertion_endpoint",
)
def test_original_outgoing_reads_bound_each_step_at_actual_native_degrees(
    degree_index, monkeypatch, record_property
):
    case = degree_index
    activity = case.activity
    original = documentary_journal_assertion(
        case.db, case.key, case.source, case.anchor, activity["assertion_id"]
    )
    expected = {str(i): edge for i, edge in enumerate(core_edges(activity))}
    progress = PROGRESS(case.db, case.key, case.work)
    base = dimensions(case)
    payload = case.db.execute(
        "SELECT max(octets) FROM (SELECT sum(octets) AS octets FROM discovery_fact_streams "
        "WHERE build_id=? AND task_id=? "
        "AND kind IN ('edge-header','edge-role','edge-target','edge-foreign') "
        "GROUP BY record_key)",
        (case.key.build_id, original.key.task_id),
    ).fetchone()[0]
    inputs = values(case, base, progress, payload=payload)
    samples, actual, after, position = [], {}, None, 0
    while True:
        if position in {0, len(expected) // 2, len(expected) - 1, len(expected)}:
            for cold in (False, True):
                if cold:
                    case.db.close()
                    case.db = sqlite3.connect(case.path, isolation_level=None)
                    case.db.execute("PRAGMA foreign_keys=ON").close()
                    case.work = index.EndpointIndexWork(case.source, case.anchor)
                for _retry in range(3):
                    with observed(case, monkeypatch) as measured:
                        result = next_assertion_endpoint(
                            measured,
                            case.key,
                            case.work,
                            case.anchor,
                            original,
                            octet_budget=4096,
                            after=after,
                        )
                    samples.append(sample(next_assertion_endpoint, inputs, measured.metrics))
        else:
            result = next_assertion_endpoint(
                case.db,
                case.key,
                case.work,
                case.anchor,
                original,
                octet_budget=4096,
                after=after,
            )
        if result is None:
            break
        assert result.record_key not in actual and (after is None or result.record_key > after)
        actual[result.record_key] = result.edge
        after = result.record_key
        position += 1
    assert actual == expected
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-activity-selector-native-degree-prepare/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_activity_selectors.prepare_activity_selector",
)
def test_activity_preparation_walks_one_original_endpoint_at_native_high_degrees(
    degree_index, tmp_path, monkeypatch, record_property
):
    exercise_activity(
        degree_index,
        tmp_path,
        monkeypatch,
        record_property,
        ACTIVITY_PREPARE,
        degree=degree_index.degree,
    )


@pytest.mark.growth_witness(
    "riverhog-activity-selector-native-degree-replay/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_selector_work.apply_journal_selector",
)
def test_activity_replay_and_cold_retry_do_not_repeat_the_high_degree_prefix(
    degree_index, tmp_path, monkeypatch, record_property
):
    exercise_activity(
        degree_index,
        tmp_path,
        monkeypatch,
        record_property,
        ACTIVITY_APPLY,
        degree=degree_index.degree,
    )
