from __future__ import annotations

import hashlib
import os
from dataclasses import asdict, replace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core import canonical_discovery_corpus_source as owner
from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples

from tests.support.catalog_work import octets
from tests.unit.test_canonical_discovery_corpus_source import seed
from tests.unit.test_canonical_discovery_metadata_growth import (
    SPECIFICATIONS,
    Recorder,
    case_for,
    seed_core,
)
from tests.unit.test_canonical_discovery_metadata_growth import (
    catalog_templates as catalog_templates,
)
from tests.unit.test_canonical_discovery_metadata_growth import (
    fixed_inputs as fixed_inputs,
)
from tests.unit.test_canonical_discovery_metadata_growth import (
    original as original,
)
from tests.unit.test_canonical_discovery_metadata_growth import (
    qualified as qualified,
)
from tests.unit.test_canonical_discovery_selection import _observe


def record(operation, dimensions, storage, core):
    metrics = dict(storage.metrics)
    for key in ("sql_statements", "returned_rows", "returned_octets", "parameter_octets"):
        metrics["core_" + key] = core.metrics[key]
    metrics["core_vm_instructions"] = core.metrics["sqlite_vm_instructions"]
    for metric in (
        "encoded_octets",
        "decoded_octets",
        "hashed_octets",
        "sha256_block_octets",
        "written_octets",
        "source_reads",
        "native_anchor_calls",
    ):
        metrics[metric] = core.metrics.get(metric, 0)
    for source, target in {
        "sql_statements": "catalog_sql_statements",
        "driver_statements": "catalog_driver_statements",
        "returned_rows": "catalog_returned_rows",
        "returned_metadata_octets": "catalog_returned_octets",
        "bound_parameter_octets": "catalog_parameter_octets",
        "sqlite_vm_instructions": "catalog_vm_instructions",
        "changed_rows": "catalog_changed_rows",
        "uuid_values": "uuid_values",
    }.items():
        metrics[target] = storage.catalog.metrics[source]
    binding = growth_binding(getattr(owner.DiscoveryCorpusSource, operation))
    projection = project_bindings((binding,))
    sample = {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical_json_bytes(projection["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": dimensions,
        "metrics": {
            bound.metric: metrics.get(bound.metric, 0) for bound in binding.declaration.bounds
        },
        "delegates": {},
    }
    validate_execution_samples(projection, binding.declaration.id, [sample])
    return sample


def measured(case, work, source, anchor, operation, variant, cold, dimensions, monkeypatch):
    metadata = case.metadata
    db = metadata._db
    recorder = Recorder()
    db.observer = recorder
    db.set_trace_callback(recorder.trace)
    before = db.total_changes
    if variant == "stale":
        metadata.custody._held = False
    if variant == "wrong":
        anchor = replace(anchor, prefix_bytes=anchor.prefix_bytes + 1)
    try:
        with (
            recorder.observe(case, cold=cold),
            _observe(db, monkeypatch, work.history.reader) as core,
        ):
            metadata._db = core
            try:
                if operation == "__init__":
                    if variant == "stale":
                        with pytest.raises(StaleArchiveTask):
                            owner.DiscoveryCorpusSource(metadata, work)
                    else:
                        assert owner.DiscoveryCorpusSource(metadata, work).binding == source.binding
                elif operation == "verify_current":
                    assert source.verify_current() is (variant != "stale")
                elif variant == "stale":
                    with pytest.raises(StaleArchiveTask):
                        getattr(source, operation)(anchor)
                else:
                    assert getattr(source, operation)(anchor) is (variant != "wrong")
            finally:
                metadata._db = db
    finally:
        metadata.custody._held = True
    assert db.total_changes == before
    recorder.metrics["core_changed_rows"] = db.total_changes - before
    for statement, parameters in core.statements:
        if statement.lstrip().upper().startswith("SELECT"):
            plan = " ".join(
                str(row[3]).upper()
                for row in db.execute("EXPLAIN QUERY PLAN " + statement, parameters)
            )
            assert "SCAN " not in plan and "TEMP B-TREE" not in plan, plan
    return record(operation, dimensions, recorder, core)


def exercise(
    tmp_path, fixed_inputs, qualified, catalog_templates, monkeypatch, record_property, operation
):
    admission, _ = fixed_inputs
    samples = []
    for retained, width in SPECIFICATIONS:
        with case_for(
            tmp_path / f"{retained}-{width}",
            admission,
            retained,
            width,
            template=catalog_templates[(retained, width)],
        ) as case:
            initial = Recorder()
            with initial.connect_only():
                case.metadata.open()
            work, anchors = seed(case.metadata, qualified)
            seed_core(case)
            source = owner.DiscoveryCorpusSource(case.metadata, work)
            db = case.metadata._db
            tables = [
                row[0]
                for row in db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'discovery_%'"
                )
            ]
            retained_rows, row_width = 0, 1
            for table in tables:
                for row in db.execute(f"SELECT * FROM {table}"):
                    retained_rows += 1
                    row_width = max(row_width, octets(row))
            dimensions = {
                "retained_core_rows": retained_rows,
                "retained_catalog_rows": 3 * retained + 3,
                "metadata_octets": max(
                    row_width,
                    octets(
                        (
                            tuple(asdict(case.metadata.job).values()),
                            case.admission.root_json,
                            case.admission.binding_json,
                            case.metadata._expected,
                            anchors,
                        )
                    ),
                ),
                "path_octets": len(os.fsencode(case.path / "discovery.sqlite3")),
            }
            variants = ("current", "stale") + (
                ("wrong",) if operation in {"admit_source", "admit_selection"} else ()
            )
            anchor = anchors[-1] if operation == "admit_source" else anchors[0]
            for cold in (False, True):
                for variant in variants:
                    samples.append(
                        measured(
                            case,
                            work,
                            source,
                            anchor,
                            operation,
                            variant,
                            cold,
                            dimensions,
                            monkeypatch,
                        )
                    )
    record_property("growth_samples", samples)
    record_property(
        "source_conditions",
        "Actual owned Core/catalog SQL, complete rows/parameters, SQLite VM, codecs, SHA "
        "continuation/block work and descriptor checks. Independent unrelated retained rows "
        "and widths 1/17/1025, cold/warm catalog, current/stale/exact/nonmember inputs. "
        "No archive payload replay or mutation; this is not whole-collection acceptance.",
    )


@pytest.mark.growth_witness(
    "core-discovery-corpus-source-construction-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_corpus_source.DiscoveryCorpusSource.__init__",
)
def test_source_construction_work(
    tmp_path, fixed_inputs, qualified, catalog_templates, monkeypatch, record_property
):
    exercise(
        tmp_path,
        fixed_inputs,
        qualified,
        catalog_templates,
        monkeypatch,
        record_property,
        "__init__",
    )


@pytest.mark.growth_witness(
    "core-discovery-corpus-source-current-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_corpus_source.DiscoveryCorpusSource.verify_current",
)
def test_source_current_work(
    tmp_path, fixed_inputs, qualified, catalog_templates, monkeypatch, record_property
):
    exercise(
        tmp_path,
        fixed_inputs,
        qualified,
        catalog_templates,
        monkeypatch,
        record_property,
        "verify_current",
    )


@pytest.mark.growth_witness(
    "core-discovery-corpus-source-membership-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_corpus_source.DiscoveryCorpusSource.admit_source",
)
def test_source_membership_work(
    tmp_path, fixed_inputs, qualified, catalog_templates, monkeypatch, record_property
):
    exercise(
        tmp_path,
        fixed_inputs,
        qualified,
        catalog_templates,
        monkeypatch,
        record_property,
        "admit_source",
    )


@pytest.mark.growth_witness(
    "core-discovery-corpus-selection-membership-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_corpus_source.DiscoveryCorpusSource.admit_selection",
)
def test_selection_membership_work(
    tmp_path, fixed_inputs, qualified, catalog_templates, monkeypatch, record_property
):
    exercise(
        tmp_path,
        fixed_inputs,
        qualified,
        catalog_templates,
        monkeypatch,
        record_property,
        "admit_selection",
    )
