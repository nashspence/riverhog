from __future__ import annotations

import hashlib
import sys
from contextlib import contextmanager
from dataclasses import replace

import pytest
from riverhog_archive_contracts import BOUND_HISTORY_EXTENT, MemberHistoryImport
from riverhog_canonical_json import canonical_json_bytes, require_canonical_json
from riverhog_core.canonical_discovery_primary_work import (
    apply_history_primary,
    prepare_history_primary,
    verify_history_import_primary,
)
from riverhog_growth_contracts import (
    evaluate_bound,
    growth_binding,
    project_bindings,
    validate_execution_samples,
)
from riverhog_provenance import ProvenanceValidationError

from tests.support.canonical_discovery_work import observe_metadata, write
from tests.unit.test_canonical_discovery_primary_work import imported_primary, primary_work


def octets(values):
    return sum(
        0 if value is None else len(value) if isinstance(value, bytes) else len(str(value).encode())
        for value in values
    )


def dimensions(db, key, imports, record):
    widths = [
        octets((key.build_id, key.admission_sha256, key.worker_id, imports.authority_json, record))
    ]
    count = 0
    tables = db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    for (table,) in tables:
        for row in db.execute(f'SELECT * FROM "{table}"'):
            count += 1
            widths.append(octets(row))
    return {"registered_records": count, "metadata_octets": max(widths)}


@contextmanager
def observe(db, owner, frame_type, monkeypatch):
    with observe_metadata(db, monkeypatch, reader=owner.history.reader) as recorder:
        recorder.metrics.update(decoded_octets=0, native_calls=0, source_frame_reads=0)
        changed = db.total_changes
        recorder.maximum_metadata = 0
        execute = recorder.execute

        def observed_execute(sql, parameters=()):
            cursor = execute(sql, parameters)
            fetch = cursor.fetchone

            def fetched():
                row = fetch()
                if row is not None:
                    recorder.maximum_metadata = max(recorder.maximum_metadata, octets(row))
                return row

            cursor.fetchone = fetched
            return cursor

        recorder.execute = observed_execute

        def decode(raw):
            recorder.metrics["decoded_octets"] += len(raw)
            return require_canonical_json(raw)

        def native(*_args, **_kwargs):
            recorder.metrics["native_calls"] += 1
            raise AssertionError("import equality replayed Native semantic work")

        def frame(*_args):
            recorder.metrics["source_frame_reads"] += 1
            raise AssertionError("import equality replayed an original source frame")

        with monkeypatch.context() as patch:
            for module in tuple(sys.modules.values()):
                if module is not None and getattr(module, "__name__", "").startswith(
                    (
                        "riverhog_archive_contracts",
                        "riverhog_canonical_json",
                        "riverhog_core.canonical_discovery",
                    )
                ):
                    if getattr(module, "require_canonical_json", None) is require_canonical_json:
                        patch.setattr(module, "require_canonical_json", decode)
            patch.setattr(owner.corpus, "selected_delivery_support", native)
            patch.setattr(owner.corpus, "historical_assertion", native)
            patch.setattr(frame_type, "__getitem__", frame)
            patch.setattr(frame_type, "__iter__", frame)
            try:
                yield recorder
            finally:
                recorder.metrics["changed_rows"] = db.total_changes - changed
                recorder.metrics["object_reads"] = recorder.metrics["source_reads"]
    for sql, parameters in recorder.statements:
        if sql.lstrip().startswith("SELECT"):
            plan = " ".join(
                str(row[3]).upper() for row in db.execute("EXPLAIN QUERY PLAN " + sql, parameters)
            )
            assert "SCAN " not in plan and "TEMP B-TREE" not in plan, plan


def sample(dims, metrics):
    binding = growth_binding(verify_history_import_primary)
    projection = project_bindings((binding,))
    result = {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical_json_bytes(projection["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": dims,
        "metrics": {bound.metric: metrics[bound.metric] for bound in binding.declaration.bounds},
        "delegates": {},
    }
    try:
        validate_execution_samples(projection, binding.declaration.id, [result])
    except ValueError as error:
        violations = [
            {
                "metric": bound.metric,
                "actual": result["metrics"][bound.metric],
                "bound": evaluate_bound(binding, bound.metric, dims),
            }
            for bound in binding.declaration.bounds
            if result["metrics"][bound.metric] > evaluate_bound(binding, bound.metric, dims)
        ]
        raise AssertionError(violations) from error
    return result


@pytest.mark.growth_witness(
    "canonical-discovery-history-import-primary/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_primary_work.verify_history_import_primary",
)
def test_actual_import_primary_equality_is_indexed_metadata_point_work(
    tmp_path, monkeypatch, record_property
):
    samples = []
    for retained in (1, 17, 1025):
        for width in (1, 17, 128):
            path = tmp_path / f"{retained}-{width}.sqlite"
            with primary_work(
                path, retained=True, inherited_extent=BOUND_HISTORY_EXTENT, worker_id="w" * width
            ) as values:
                db, key, owner, imports, record, history = imported_primary(values)
                frame_type = values[-1]
                page = prepare_history_primary(db, key, owner, history, metadata_byte_budget=65536)
                with write(db):
                    apply_history_primary(db, key, owner, page)
                    row = db.execute("SELECT * FROM discovery_history_primary_bindings").fetchone()
                    for index in range(retained):
                        unrelated = list(row)
                        unrelated[0] = f"retained-{index}"
                        db.execute(
                            "INSERT INTO discovery_history_primary_bindings VALUES("
                            + ",".join("?" * len(unrelated))
                            + ")",
                            unrelated,
                        ).close()
                dims = dimensions(db, key, imports, record)
                for cold in (True, False):
                    if cold:
                        db.execute("PRAGMA shrink_memory").close()
                    with observe(db, owner, frame_type, monkeypatch) as recorder:
                        verify_history_import_primary(recorder, key, imports, record)
                    samples.append(
                        sample(
                            dict(
                                dims,
                                metadata_octets=max(
                                    dims["metadata_octets"], recorder.maximum_metadata
                                ),
                            ),
                            recorder.metrics,
                        )
                    )
                raw = db.execute(
                    "SELECT value_json FROM discovery_history_set_records "
                    "WHERE build_id=? AND admission_sha256=? AND records_sha256=? AND record_key=?",
                    (key.build_id, key.admission_sha256, imports.authority.records_sha256, record),
                ).fetchone()[0]
                imported = MemberHistoryImport.from_mapping(require_canonical_json(raw.encode()))
                changed = replace(
                    imported,
                    input_state=dict(
                        imported.input_state,
                        object_id="urn:uuid:ffffffff-ffff-4fff-8fff-ffffffffffff",
                    ),
                )
                db.execute(
                    "UPDATE discovery_history_set_records SET value_json=? "
                    "WHERE build_id=? AND admission_sha256=? AND records_sha256=? AND record_key=?",
                    (
                        canonical_json_bytes(changed.to_mapping()).decode(),
                        key.build_id,
                        key.admission_sha256,
                        imports.authority.records_sha256,
                        record,
                    ),
                ).close()
                with observe(db, owner, frame_type, monkeypatch) as recorder:
                    with pytest.raises(ProvenanceValidationError, match="source primary"):
                        verify_history_import_primary(recorder, key, imports, record)
                samples.append(
                    sample(
                        dict(
                            dims,
                            metadata_octets=max(dims["metadata_octets"], recorder.maximum_metadata),
                        ),
                        recorder.metrics,
                    )
                )
    record_property("growth_samples", samples)
    record_property(
        "import_primary_conditions",
        "Actual retained Native primary support, real authenticated source binding "
        "proof and original import incidence. Independent unrelated retained rows "
        "and full worker/control widths; evicted and warm current reads and faulted metadata "
        "rejection. Every query requires an indexed plan; no source replay, Native "
        "body call, structure read or mutation occurs in the measured point boundary.",
    )
