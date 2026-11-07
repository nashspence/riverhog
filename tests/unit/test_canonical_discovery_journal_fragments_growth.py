from __future__ import annotations

import hashlib
import sqlite3
from types import SimpleNamespace

import pytest
from riverhog_core import canonical_discovery_journal_fragments as fragments
from riverhog_core.canonical_discovery_fact_work import (
    FactStreamKey,
    FactTaskKey,
    begin_assertion,
    finish_fact_stream,
    stage_fact_segment,
)

from tests.support.canonical_discovery_work import write
from tests.support.catalog_work import octets
from tests.unit.test_canonical_discovery_journal_fragments import (
    CANONICAL,
    task,
)
from tests.unit.test_canonical_discovery_journal_fragments import (
    case as case,
)
from tests.unit.test_canonical_discovery_journal_fragments import (
    original_facts as original_facts,
)
from tests.unit.test_canonical_discovery_journal_lookup_growth import observe
from tests.unit.test_journal_validation_workspace_reference_growth import sample

READ = fragments.read_journal_fact_fragment


def dimensions(case):
    rows, width = (
        0,
        octets((case.key, case.source.binding_json, case.source.terminal_json, case.anchor)),
    )
    for table in (
        "discovery_journal_tasks",
        "discovery_journal_assertions",
        "discovery_fact_tasks",
        "discovery_fact_streams",
    ):
        for row in case.db.execute("SELECT * FROM " + table):
            rows += 1
            width = max(width, octets(row))
    for row in case.db.execute(
        "SELECT build_id,task_id,kind,record_key,ordinal,sha256,staged_version "
        "FROM discovery_fact_segments"
    ):
        rows += 1
        width = max(width, octets(row))
    return {"retained_rows": rows, "metadata_octets": width}


def retain(case, first, stop):
    reference = case.source.reference_for_assertion(
        case.assertion,
        object_id=fragments.documentary_journal_assertion(
            case.db,
            case.key,
            case.source,
            case.anchor,
            case.assertion,
        ).referent_id,
        object_type="extension",
    )
    with write(case.db):
        for index in range(first, stop):
            key = FactTaskKey("independent-" + str(index), "pending", case.key.source_key, "worker")
            begin_assertion(
                case.db,
                key,
                assertion_id=case.assertion,
                origin_sha256=reference["entry"]["json_sha256"],
                canonical_stream_ref=hashlib.sha256(case.canonical).hexdigest(),
            )
            stream = FactStreamKey("unrelated", "0")
            stage_fact_segment(
                case.db,
                key,
                stream,
                ordinal=0,
                exact_bytes=b"x" * 4096,
                expected_version=0,
            )
            finish_fact_stream(
                case.db,
                key,
                stream,
                fragments=1,
                octets=4096,
                expected_version=1,
            )


@pytest.mark.growth_witness(
    "riverhog-discovery-source-fact-fragment-read/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_fragments.read_journal_fact_fragment",
)
def test_actual_physical_reads_capacity_and_retries_seek_past_retained_work(
    case, monkeypatch, record_property
):
    child = task(case)
    streams = [
        (FactStreamKey(kind, key), count, length)
        for kind, key, count, length in case.db.execute(
            "SELECT kind,record_key,fragments,octets FROM discovery_fact_streams "
            "WHERE build_id=? AND task_id=? AND (kind='canonical' OR octets>4096 OR octets=0)",
            (case.key.build_id, child),
        )
    ]
    assert any(stream == CANONICAL and count > 1 for stream, count, _ in streams)
    assert any(length == 0 for _, _, length in streams)
    samples, retained = [], 0
    actual_sha256 = hashlib.sha256
    for extent in (1, 17, 1025):
        retain(case, retained, extent)
        retained = extent
        values = dimensions(case)
        for stream, count, length in streams:
            positions = sorted({0, count // 2, max(0, count - 1), count})
            for ordinal in positions:
                size = 0 if ordinal == count else min(4096, length - ordinal * 4096)
                for cold in (False, True):
                    if cold:
                        case.db.close()
                        case.db = sqlite3.connect(case.path, isolation_level=None)
                    for reject in (False, True) if size > 1 else (False,):
                        inputs = dict(
                            values,
                            fragment_octets=0 if reject else size,
                            metadata_octets=max(
                                values["metadata_octets"],
                                octets((stream, ordinal, size)),
                            ),
                        )
                        for _retry in range(3):
                            with observe(case, monkeypatch) as recorder:
                                recorder.metrics["hashed_octets"] = 0

                                def measured(raw):
                                    recorder.metrics["hashed_octets"] += len(raw)
                                    return actual_sha256(raw)

                                with monkeypatch.context() as patch:
                                    patch.setattr(
                                        fragments, "hashlib", SimpleNamespace(sha256=measured)
                                    )
                                    if reject:
                                        with pytest.raises(fragments.JournalFactCapacity):
                                            READ(
                                                recorder,
                                                case.key,
                                                case.source,
                                                case.anchor,
                                                case.assertion,
                                                stream,
                                                ordinal=ordinal,
                                                octet_budget=size - 1,
                                            )
                                    else:
                                        result = READ(
                                            recorder,
                                            case.key,
                                            case.source,
                                            case.anchor,
                                            case.assertion,
                                            stream,
                                            ordinal=ordinal,
                                            octet_budget=max(1, size),
                                        )
                                        assert len(result.exact_bytes) == size
                                        assert result.terminal == (ordinal == count)
                            samples.append(sample(READ, inputs, recorder.metrics))
    record_property("growth_samples", samples)
