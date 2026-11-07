from __future__ import annotations

import copy
import sqlite3
from contextlib import contextmanager

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core import canonical_discovery_endpoint_index as index
from riverhog_core import canonical_discovery_endpoint_matches as matches
from riverhog_core.canonical_discovery_journal_fragments import JournalFactCapacity
from riverhog_provenance_contracts import ExternalReference

from tests.support.catalog_work import octets
from tests.unit.test_canonical_discovery_endpoint_index_growth import observed
from tests.unit.test_canonical_discovery_endpoint_matches import (
    MATCH,
    original_edges,
    target_for,
)
from tests.unit.test_canonical_discovery_endpoint_matches import interval_index as interval_index
from tests.unit.test_canonical_discovery_endpoint_matches import matching_case as matching_case
from tests.unit.test_canonical_discovery_endpoint_matches import matching_facts as matching_facts
from tests.unit.test_canonical_discovery_endpoint_matches import matching_index as matching_index
from tests.unit.test_canonical_discovery_endpoint_source import mixed_originals as mixed_originals
from tests.unit.test_canonical_discovery_journal_fragments_growth import dimensions, retain
from tests.unit.test_journal_validation_workspace_reference_growth import sample


@contextmanager
def measured(case, monkeypatch):
    scalar, equal = matches.format_scalar, matches._equal_bytes
    with observed(case, monkeypatch) as recorder:
        recorder.metrics["compared_octets"] = 0

        def encoded(kind, value):
            result = scalar(kind, value)
            recorder.metrics["scalar_encoded_octets"] += len(result.encode())
            return result

        def compared(left, right):
            recorder.metrics["compared_octets"] += len(left) + len(right)
            return equal(left, right)

        with monkeypatch.context() as patch:
            patch.setattr(matches, "format_scalar", encoded)
            patch.setattr(matches, "_equal_bytes", compared)
            yield recorder


def input_dimensions(case, target, base):
    header = case.db.execute(
        "SELECT * FROM discovery_endpoint_tasks WHERE build_id=? AND source_key=?",
        (case.key.build_id, case.key.source_key),
    ).fetchone()
    endpoints = case.db.execute(
        "SELECT count(*) FROM discovery_endpoints WHERE build_id=? AND source_key=?",
        (case.key.build_id, case.key.source_key),
    ).fetchone()[0]
    payload = case.db.execute(
        "SELECT max(octets) FROM (SELECT sum(s.octets) AS octets FROM discovery_fact_streams s "
        "JOIN discovery_journal_assertions a ON a.build_id=s.build_id AND a.task_id=s.task_id "
        "WHERE a.build_id=? AND a.source_key=? "
        "AND s.kind IN ('edge-header','edge-role','edge-target','edge-foreign') "
        "GROUP BY a.assertion_id,s.record_key)",
        (case.key.build_id, case.key.source_key),
    ).fetchone()[0]
    exact = (
        target.object_id.encode()
        if target.scope == "local"
        else canonical_json_bytes(target.model_dump(mode="json"))
    )
    return {
        "retained_rows": base["retained_rows"] + endpoints + 1,
        "metadata_octets": max(base["metadata_octets"], octets(header), octets(target)),
        "target_octets": len(exact),
        "endpoint_octets": payload or 0,
    }


@pytest.mark.growth_witness(
    "riverhog-original-endpoint-exact-match/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_endpoint_matches.advance_endpoint_match",
)
def test_actual_endpoint_comparisons_and_interval_skips_have_bounded_original_work(
    matching_case, interval_index, monkeypatch, record_property
):
    samples, retained, visited = [], 0, set()
    case = matching_case
    edges = original_edges(case.native)
    queries = {(edge.role, target_for(edge)) for _, _, edge in edges}
    context = next(target for role, target in queries if role == "context:source")
    queries.add(("context", context))
    foreign = next(target for _, target in queries if isinstance(target, ExternalReference))
    absent = foreign.model_dump(mode="json")
    absent = copy.deepcopy(absent)
    absent["entry"]["sequence"] = "9223372036854775805"
    queries.add(("value", ExternalReference.model_validate(absent)))
    queries.add(("absent", context))
    for extent in (1, 17, 1025):
        retain(case, retained, extent)
        retained, base = extent, dimensions(case)
        for role, target in queries:
            inputs = input_dimensions(case, target, base)
            for cold in (False, True):
                if cold:
                    case.db.close()
                    case.db = sqlite3.connect(case.path, isolation_level=None)
                    case.work = index.EndpointIndexWork(case.source, case.anchor)
                after = None
                while True:
                    for _retry in range(3):
                        with measured(case, monkeypatch) as recorder:
                            result = MATCH(
                                recorder,
                                case.key,
                                case.work,
                                case.anchor,
                                role=role,
                                target=target,
                                assertion_state="documentary",
                                octet_budget=4096,
                                after=after,
                            )
                        samples.append(sample(MATCH, inputs, recorder.metrics))
                    if result.complete:
                        break
                    visited.add(result.endpoint.edge.target_scope if result.endpoint else "pending")
                    after = result.after
            with measured(case, monkeypatch) as recorder:
                with pytest.raises(JournalFactCapacity):
                    MATCH(
                        recorder,
                        case.key,
                        case.work,
                        case.anchor,
                        role=role,
                        target=target,
                        assertion_state="documentary",
                        octet_budget=1,
                    )
            samples.append(sample(MATCH, inputs, recorder.metrics))
    assert visited == {"local", "external", "pending"}
    case = interval_index
    base = dimensions(case)
    inputs = input_dimensions(case, case.target, base)
    for sequence in (0, 1, 3):
        anchor = HistoryJournalAnchor.from_mapping(case.native.anchor_at(sequence))
        for state in ("documentary", "effective"):
            after = None
            while True:
                with measured(case, monkeypatch) as recorder:
                    result = MATCH(
                        recorder,
                        case.key,
                        case.work,
                        anchor,
                        role="subject",
                        target=case.target,
                        assertion_state=state,
                        octet_budget=4096,
                        after=after,
                    )
                samples.append(sample(MATCH, inputs, recorder.metrics))
                if result.complete:
                    break
                after = result.after
    record_property("growth_samples", samples)
