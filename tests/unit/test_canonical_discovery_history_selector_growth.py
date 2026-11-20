from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import BOUND_HISTORY_EXTENT, RETAINED_HISTORY_EXTENT
from riverhog_canonical_json import format_scalar, parse_scalar
from riverhog_core import canonical_discovery_history_selectors as owner
from riverhog_core import canonical_discovery_selector_work as selectors
from riverhog_core.canonical_discovery_selectors import Selector

from tests.support.canonical_discovery_work import observe_metadata, write
from tests.support.catalog_work import octets
from tests.unit.test_canonical_discovery_history_selectors import (
    APPLY,
    KINDS,
    PREPARE,
    graph,
    intern,
    work,
)
from tests.unit.test_canonical_discovery_history_selectors import case as case
from tests.unit.test_canonical_discovery_history_selectors import (
    original_histories as original_histories,
)
from tests.unit.test_canonical_discovery_selector_growth import retain
from tests.unit.test_canonical_discovery_selector_work import progress
from tests.unit.test_journal_validation_workspace_reference_growth import sample


@contextmanager
def observe(case, monkeypatch):
    loads, same_source, same_coordinates = (
        json.loads,
        owner._same_source,
        selectors._equal_coordinates,
    )
    changed = case.db.total_changes
    with observe_metadata(case.db, monkeypatch) as recorder:
        recorder.metrics.update(decoded_octets=0, compared_octets=0, source_payload_octets=0)

        def decoded(value, *args, **kwargs):
            recorder.metrics["decoded_octets"] += len(
                value if isinstance(value, bytes) else value.encode()
            )
            return loads(value, *args, **kwargs)

        def encoded_scalar(kind, value):
            result = format_scalar(kind, value)
            recorder.metrics["encoded_octets"] += len(result.encode())
            return result

        def decoded_scalar(kind, value):
            recorder.metrics["decoded_octets"] += len(value.encode())
            return parse_scalar(kind, value)

        def compared(left, right, *, original):
            recorder.metrics["compared_octets"] += len(left.encode()) + len(right.encode())
            return original(left, right)

        with monkeypatch.context() as patch:
            patch.setattr(json, "loads", decoded)
            patch.setattr(
                owner,
                "_same_source",
                lambda left, right: compared(left, right, original=same_source),
            )
            patch.setattr(
                selectors,
                "_equal_coordinates",
                lambda left, right: compared(left, right, original=same_coordinates),
            )
            for module in tuple(sys.modules.values()):
                if module is not None and getattr(module, "__name__", "").startswith(
                    (
                        "riverhog_core.canonical_discovery",
                        "riverhog_archive_contracts",
                        "riverhog_canonical_json",
                    )
                ):
                    if getattr(module, "format_scalar", None) is format_scalar:
                        patch.setattr(module, "format_scalar", encoded_scalar)
                    if getattr(module, "parse_scalar", None) is parse_scalar:
                        patch.setattr(module, "parse_scalar", decoded_scalar)
            yield recorder
        recorder.metrics["changed_rows"] = case.db.total_changes - changed
    for sql, parameters in recorder.statements:
        if sql.lstrip().startswith("SELECT"):
            plan = case.db.execute("EXPLAIN QUERY PLAN " + sql, parameters).fetchall()
            assert not any("SCAN " in row[3] or "TEMP B-TREE" in row[3] for row in plan)


def dimensions(case, *controls):
    rows, width = 0, max(map(octets, (case.key, *controls)))
    tables = [
        row[0]
        for row in case.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'discovery_%'"
        )
    ]
    for table in tables:
        for row in case.db.execute('SELECT * FROM "' + table + '"'):
            rows += 1
            width = max(width, octets(row))
    return {"retained_rows": rows, "metadata_octets": width}


def seeds(case):
    for descriptor in case.descriptors:
        for extent in (BOUND_HISTORY_EXTENT, RETAINED_HISTORY_EXTENT):
            yield Selector("history-documentary", history_sha256=descriptor.identity, extent=extent)
        yield Selector("import-reachable", history_sha256=descriptor.identity)


def pending(case):
    from riverhog_canonical_json import require_canonical_json

    result = []
    for (raw,) in case.db.execute(
        "SELECT coordinates_json FROM discovery_selectors WHERE build_id=? "
        "AND admission_sha256=? AND complete=0",
        (case.key.build_id, case.key.admission_sha256),
    ):
        selector = Selector.from_mapping(require_canonical_json(raw.encode()))
        if selector.kind in KINDS:
            result.append(progress(case, selector))
    return result


def exercise(case, tmp_path, monkeypatch, record_property, operation):
    samples, kinds, skipped, absent = [], set(), False, False
    for backlog in (1, 17, 1025, 8193):
        path = tmp_path / f"history-{backlog}.sqlite3"
        db = sqlite3.connect(path, isolation_level=None)
        db.execute("PRAGMA foreign_keys=ON").close()
        case.db.backup(db)
        selected = SimpleNamespace(**vars(case))
        selected.db, selected.path = db, path
        try:
            retain(selected, backlog)
            for seed in seeds(selected):
                intern(selected, seed)
            compiler = work(selected)
            while True:
                remaining = pending(selected)
                if not remaining:
                    break
                current = remaining[0]
                kinds.add(current.selector.kind)
                inputs = dimensions(selected, current, compiler._archive_binding)
                if operation is PREPARE:
                    with observe(selected, monkeypatch) as recorder:
                        prepared = PREPARE(recorder, selected.key, compiler, current)
                    values = dict(
                        inputs,
                        metadata_octets=max(
                            inputs["metadata_octets"],
                            *(len(r.exact_json.encode()) for r in prepared.sources),
                        ),
                    )
                    samples.append(sample(operation, values, recorder.metrics))
                else:
                    prepared = PREPARE(db, selected.key, compiler, current)
                    values = dimensions(selected, prepared, compiler._archive_binding)
                    db.execute("BEGIN IMMEDIATE").close()
                    with observe(selected, monkeypatch) as recorder:
                        APPLY(recorder, selected.key, compiler, prepared)
                    after = dimensions(selected, prepared, compiler._archive_binding)
                    dimensions_max = {name: max(values[name], after[name]) for name in values}
                    samples.append(sample(operation, dimensions_max, recorder.metrics))
                    db.rollback()
                    assert progress(selected, current.selector) == current
                for _retry in range(2):
                    with write(db):
                        if operation is APPLY:
                            with observe(selected, monkeypatch) as recorder:
                                APPLY(recorder, selected.key, compiler, prepared)
                        else:
                            APPLY(db, selected.key, compiler, prepared)
                    if operation is APPLY:
                        after = dimensions(selected, prepared, compiler._archive_binding)
                        dimensions_max = {name: max(values[name], after[name]) for name in values}
                        samples.append(sample(operation, dimensions_max, recorder.metrics))
                skipped |= not prepared.page.result.complete and prepared.page.output is None
                absent |= prepared.page.result.complete and current.version == 0
                # An actual cold Core reopen between every source-normal atom.
                db.close()
                db = selected.db = sqlite3.connect(path, isolation_level=None)
                selected.db.execute("PRAGMA foreign_keys=ON").close()
                compiler = work(selected)
            nodes, edges = graph(selected)
            assert sum(spec.kind == "root-set-documentary" for spec in nodes.values()) == 2
            assert sum(parent.kind == "root-set-documentary" for parent, _ in edges) == 3
        finally:
            selected.db.close()
    assert kinds == KINDS and skipped and absent
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-shared-history-selector-prepare/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_history_selectors.prepare_history_selector",
)
def test_real_history_set_joins_capture_bounded_original_receipts(
    case, tmp_path, monkeypatch, record_property
):
    exercise(case, tmp_path, monkeypatch, record_property, PREPARE)


@pytest.mark.growth_witness(
    "riverhog-shared-history-selector-apply/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_history_selectors.apply_history_selector",
)
def test_real_source_receipt_retry_and_rollback_have_bounded_indexed_cost(
    case, tmp_path, monkeypatch, record_property
):
    exercise(case, tmp_path, monkeypatch, record_property, APPLY)
