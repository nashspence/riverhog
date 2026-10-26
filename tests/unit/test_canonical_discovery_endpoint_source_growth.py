from __future__ import annotations

import hashlib
import sqlite3

import pytest
from riverhog_core import canonical_discovery_endpoint_source as endpoints
from riverhog_core.canonical_discovery_journal_fragments import JournalFactCapacity

from tests.support.catalog_work import octets
from tests.unit.test_canonical_discovery_endpoint_source import mixed_case as mixed_case
from tests.unit.test_canonical_discovery_endpoint_source import mixed_originals as mixed_originals
from tests.unit.test_canonical_discovery_journal_fragments_growth import dimensions, retain
from tests.unit.test_canonical_discovery_journal_lookup_growth import observe
from tests.unit.test_journal_validation_workspace_reference_growth import sample

READ = endpoints.read_journal_endpoint


@pytest.mark.growth_witness(
    "riverhog-discovery-original-endpoint-read/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_endpoint_source.read_journal_endpoint",
)
def test_original_endpoint_reads_ignore_retained_pending_work_and_retry_cold(
    mixed_case, monkeypatch, record_property
):
    case = mixed_case
    original = case.assertion
    candidates = [
        (identity, record, length)
        for identity, record, length in case.db.execute(
            "SELECT a.assertion_id,s.record_key,sum(s.octets) "
            "FROM discovery_journal_assertions a JOIN discovery_fact_streams s "
            "ON s.build_id=a.build_id AND s.task_id=a.task_id "
            "WHERE a.build_id=? AND a.source_key=? "
            "AND s.kind IN ('edge-header','edge-role','edge-target','edge-foreign') "
            "GROUP BY a.assertion_id,s.record_key ORDER BY a.assertion_id,s.record_key",
            (case.key.build_id, case.key.source_key),
        )
    ]
    assert candidates
    samples, retained, scopes, roles = [], 0, set(), set()
    actual_sha256, actual_decode, actual_encode = (
        hashlib.sha256,
        endpoints.require_canonical_json,
        endpoints.format_scalar,
    )
    for extent in (1, 17, 1025):
        case.assertion = original
        retain(case, retained, extent)
        retained = extent
        values = dimensions(case)
        for identity, record, size in candidates:
            for cold in (False, True):
                if cold:
                    case.db.close()
                    case.db = sqlite3.connect(case.path, isolation_level=None)
                inputs = dict(
                    values,
                    endpoint_octets=size,
                    metadata_octets=max(
                        values["metadata_octets"], octets((identity, record, case.anchor))
                    ),
                )
                for _retry in range(3):
                    with observe(case, monkeypatch) as recorder:
                        recorder.metrics["hashed_octets"] = 0

                        def hashed(raw=b""):
                            recorder.metrics["hashed_octets"] += len(raw)
                            return actual_sha256(raw)

                        def decoded(raw):
                            recorder.metrics["decoded_octets"] += len(raw)
                            return actual_decode(raw)

                        def encoded(kind, value):
                            result = actual_encode(kind, value)
                            recorder.metrics["scalar_encoded_octets"] += len(result.encode())
                            return result

                        with monkeypatch.context() as patch:
                            patch.setattr(hashlib, "sha256", hashed)
                            patch.setattr(endpoints, "require_canonical_json", decoded)
                            patch.setattr(endpoints, "format_scalar", encoded)
                            result = READ(
                                recorder,
                                case.key,
                                case.source,
                                case.anchor,
                                identity,
                                record,
                                octet_budget=4096,
                            )
                    samples.append(sample(READ, inputs, recorder.metrics))
                    scopes.add(result.edge.target_scope)
                    roles.add(result.index_role)
        for _retry in range(3):
            with observe(case, monkeypatch) as recorder:
                recorder.metrics["hashed_octets"] = 0
                with pytest.raises(JournalFactCapacity):
                    READ(
                        recorder,
                        case.key,
                        case.source,
                        case.anchor,
                        candidates[0][0],
                        candidates[0][1],
                        octet_budget=1,
                    )
            samples.append(
                sample(READ, dict(values, endpoint_octets=candidates[0][2]), recorder.metrics)
            )
    assert scopes == {"local", "external"} and "context" in roles
    record_property("growth_samples", samples)
