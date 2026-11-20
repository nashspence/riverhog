from __future__ import annotations

import sqlite3

import pytest
from riverhog_core.canonical_discovery_endpoint_index import (
    EndpointIndexWork,
    endpoint_index_progress,
)
from riverhog_core.canonical_discovery_journal_fragments import JournalFactCapacity

from tests.unit.test_canonical_discovery_endpoint_index_growth import observed, values
from tests.unit.test_canonical_discovery_journal_fragments_growth import dimensions, retain
from tests.unit.test_canonical_discovery_outgoing_endpoints import NEXT, header
from tests.unit.test_canonical_discovery_outgoing_endpoints import interval_index as interval_index
from tests.unit.test_canonical_discovery_outgoing_endpoints import matching_case as matching_case
from tests.unit.test_canonical_discovery_outgoing_endpoints import matching_facts as matching_facts
from tests.unit.test_canonical_discovery_outgoing_endpoints import matching_index as matching_index
from tests.unit.test_canonical_discovery_outgoing_endpoints import (
    mixed_originals as mixed_originals,
)
from tests.unit.test_journal_validation_workspace_reference_growth import sample


@pytest.mark.growth_witness(
    "riverhog-original-outgoing-endpoint-retained-rows/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_outgoing_endpoints.next_assertion_endpoint",
)
def test_outgoing_original_seeks_and_capacity_retries_ignore_retained_work(
    matching_case, monkeypatch, record_property
):
    case = matching_case
    case.db.execute("PRAGMA foreign_keys=ON").close()
    originals = [
        header(case, row[0])
        for row in case.db.execute(
            "SELECT assertion_id FROM discovery_journal_assertions "
            "WHERE build_id=? AND source_key=? ORDER BY assertion_id",
            (case.key.build_id, case.key.source_key),
        ).fetchall()
    ]
    payload = case.db.execute(
        "SELECT max(octets) FROM (SELECT sum(octets) AS octets FROM discovery_fact_streams "
        "WHERE build_id=? AND kind IN ('edge-header','edge-role','edge-target','edge-foreign') "
        "GROUP BY task_id,record_key)",
        (case.key.build_id,),
    ).fetchone()[0]
    samples, retained, empty, nonempty = [], 0, 0, 0
    for extent in (1, 17, 1025, 8193):
        retain(case, retained, extent)
        retained = extent
        base = dimensions(case)
        inputs = values(
            case, base, endpoint_index_progress(case.db, case.key, case.work), payload=payload
        )
        for original in originals:
            after = None
            while True:
                for cold in (False, True):
                    if cold:
                        case.db.close()
                        case.db = sqlite3.connect(case.path, isolation_level=None)
                        case.db.execute("PRAGMA foreign_keys=ON").close()
                        case.work = EndpointIndexWork(case.source, case.anchor)
                    for _retry in range(3):
                        with observed(case, monkeypatch) as measured:
                            result = NEXT(
                                measured,
                                case.key,
                                case.work,
                                case.anchor,
                                original,
                                octet_budget=4096,
                                after=after,
                            )
                        samples.append(sample(NEXT, inputs, measured.metrics))
                if result is None:
                    empty += after is None
                    break
                nonempty += 1
                with observed(case, monkeypatch) as measured:
                    with pytest.raises(JournalFactCapacity):
                        NEXT(
                            measured,
                            case.key,
                            case.work,
                            case.anchor,
                            original,
                            octet_budget=1,
                            after=after,
                        )
                samples.append(sample(NEXT, inputs, measured.metrics))
                after = result.record_key
    assert empty > 0 and nonempty > 0
    record_property("growth_samples", samples)
