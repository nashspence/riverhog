from __future__ import annotations

import hashlib
import sqlite3
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core import canonical_discovery_selector_work as owner
from riverhog_core.canonical_discovery_selectors import ImportedLateGuard, Selector

from tests.support.canonical_discovery_work import observe_metadata, write
from tests.support.catalog_work import octets
from tests.unit.test_canonical_discovery_selector_work import (
    APPLY,
    BUDGET,
    HISTORY,
    INTERN,
    NEXT,
    PREPARE,
    SNAPSHOT,
    STATE,
    intern,
    primary,
    progress,
    uid,
)
from tests.unit.test_canonical_discovery_selector_work import case as case
from tests.unit.test_journal_validation_workspace_reference_growth import sample


@contextmanager
def observe(case, monkeypatch):
    decode, encode_scalar, decode_scalar, equal = (
        owner.require_canonical_json,
        owner.format_scalar,
        owner.parse_scalar,
        owner._equal_coordinates,
    )
    changed = case.db.total_changes
    with observe_metadata(case.db, monkeypatch) as recorder:
        recorder.metrics.update(decoded_octets=0, compared_octets=0, source_payload_octets=0)

        def decoded(value):
            recorder.metrics["decoded_octets"] += len(value)
            return decode(value)

        def encoded_scalar(kind, value):
            result = encode_scalar(kind, value)
            recorder.metrics["encoded_octets"] += len(result.encode())
            return result

        def decoded_scalar(kind, value):
            recorder.metrics["decoded_octets"] += len(value.encode())
            return decode_scalar(kind, value)

        def compared(left, right):
            recorder.metrics["compared_octets"] += len(left.encode()) + len(right.encode())
            return equal(left, right)

        with monkeypatch.context() as patch:
            patch.setattr(owner, "require_canonical_json", decoded)
            patch.setattr(owner, "format_scalar", encoded_scalar)
            patch.setattr(owner, "parse_scalar", decoded_scalar)
            patch.setattr(owner, "_equal_coordinates", compared)
            yield recorder
        recorder.metrics["changed_rows"] = case.db.total_changes - changed
    for sql, parameters in recorder.statements:
        if sql.lstrip().startswith("SELECT"):
            plan = case.db.execute("EXPLAIN QUERY PLAN " + sql, parameters).fetchall()
            assert not any("SCAN " in row[3] or "TEMP B-TREE" in row[3] for row in plan)


def retain(case, count):
    with write(case.db):
        for number in range(count):
            spec = Selector(
                "history-documentary",
                history_sha256=f"{number:064x}",
                extent="complete-retained-history",
            )
            raw = canonical_json_bytes(spec.to_mapping()).decode()
            identity = hashlib.sha256(raw.encode()).hexdigest()
            case.db.execute(
                "INSERT INTO discovery_selectors VALUES (?,?,?,?,?,'1','[]',1,'null')",
                (case.key.build_id, case.key.admission_sha256, identity, case.key.worker_id, raw),
            ).close()


def dimensions(case, *inputs):
    rows, width = 0, max(map(octets, (case.key, *inputs)))
    for table in (
        "discovery_archive_tasks",
        "discovery_history_primary_bindings",
        "discovery_selectors",
        "discovery_selector_unary",
        "discovery_selector_imported_late",
    ):
        for row in case.db.execute("SELECT * FROM " + table):
            rows += 1
            width = max(width, octets(row))
    return {"retained_rows": rows, "metadata_octets": width}


def actual_sample(operation, inputs, case, recorder):
    after = dimensions(case)
    values = {name: max(value, after[name]) for name, value in inputs.items()}
    return sample(operation, values, recorder.metrics)


def exercise(case, tmp_path, monkeypatch, record_property, operation):
    samples = []
    for count in (1, 17, 1025, 8193):
        for guarded in (False, True):
            path = tmp_path / f"{count}-{guarded}.sqlite3"
            db = sqlite3.connect(path, isolation_level=None)
            case.db.backup(db)
            selected = SimpleNamespace(db=db, key=case.key, path=path)
            try:
                retain(selected, count)
                guard = ImportedLateGuard(HISTORY, STATE)
                parent = (
                    guard.operands[0]
                    if guarded
                    else Selector("state", **SNAPSHOT, object_id=uid(4), scope="member")
                )
                output = (
                    guard
                    if guarded
                    else Selector("own", **SNAPSHOT, object_id=uid(4), scope="member")
                )
                if guarded:
                    primary(selected)
                if operation is INTERN:
                    inputs = dimensions(selected, parent.to_mapping())
                    for _ in range(3):
                        with write(db), observe(selected, monkeypatch) as recorder:
                            INTERN(recorder, selected.key, parent, octet_budget=BUDGET)
                        samples.append(actual_sample(operation, inputs, selected, recorder))
                    with write(db), observe(selected, monkeypatch) as recorder:
                        with pytest.raises(owner.SelectorCapacity):
                            INTERN(recorder, selected.key, parent, octet_budget=16)
                    samples.append(actual_sample(operation, inputs, selected, recorder))
                    continue
                intern(selected, parent)
                before = progress(selected, parent)
                work = owner.SelectorWork(selected.key, octet_budget=BUDGET)
                inputs = dimensions(selected, parent.to_mapping(), before, output)
                if operation is PREPARE:
                    for terminal in (False, True):
                        with observe(selected, monkeypatch) as recorder:
                            PREPARE(
                                work,
                                before,
                                position=("next", "x" * count),
                                output=output,
                                complete=terminal,
                            )
                        values = dict(
                            inputs, metadata_octets=max(inputs["metadata_octets"], count + 32)
                        )
                        samples.append(actual_sample(operation, values, selected, recorder))
                    continue
                if operation is APPLY:
                    page = PREPARE(work, before, position=("next",), output=output)
                    db.execute("BEGIN IMMEDIATE").close()
                    with observe(selected, monkeypatch) as recorder:
                        APPLY(recorder, selected.key, work, page)
                    samples.append(actual_sample(operation, inputs, selected, recorder))
                    db.rollback()
                    assert progress(selected, parent) == before
                    for _ in range(3):
                        with write(db), observe(selected, monkeypatch) as recorder:
                            APPLY(recorder, selected.key, work, page)
                        samples.append(actual_sample(operation, inputs, selected, recorder))
                    continue
                # Read actual pending controls with and without retained incidence receipts.
                page = PREPARE(work, before, position=("next",), output=output)
                for committed in (False, True):
                    if committed:
                        with write(db):
                            APPLY(db, selected.key, work, page)
                        children = guard.operands[1:] if guarded else (output,)
                        for child in children:
                            child_work = owner.SelectorWork(selected.key, octet_budget=BUDGET)
                            child_page = PREPARE(
                                child_work,
                                progress(selected, child),
                                position=("end",),
                                complete=True,
                            )
                            with write(db):
                                APPLY(db, selected.key, child_work, child_page)
                    for cold in (False, True):
                        if cold:
                            db.close()
                            db = selected.db = sqlite3.connect(path, isolation_level=None)
                        current_inputs = dimensions(selected, parent.to_mapping(), before, output)
                        with observe(selected, monkeypatch) as recorder:
                            assert NEXT(recorder, selected.key, octet_budget=BUDGET) is not None
                        samples.append(actual_sample(operation, current_inputs, selected, recorder))
                with write(db):
                    db.execute("UPDATE discovery_selectors SET complete=1").close()
                with observe(selected, monkeypatch) as recorder:
                    assert NEXT(recorder, selected.key, octet_budget=BUDGET) is None
                samples.append(actual_sample(operation, current_inputs, selected, recorder))
            finally:
                selected.db.close()
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-fixed-selector-registration/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_selector_work.intern_selector",
)
def test_registration_compares_exact_coordinates_without_backlog_work(
    case, tmp_path, monkeypatch, record_property
):
    exercise(case, tmp_path, monkeypatch, record_property, INTERN)


@pytest.mark.growth_witness(
    "riverhog-fixed-selector-pending/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_selector_work.next_selector",
)
def test_pending_seek_and_receipts_do_not_scan_completed_work(
    case, tmp_path, monkeypatch, record_property
):
    exercise(case, tmp_path, monkeypatch, record_property, NEXT)


@pytest.mark.growth_witness(
    "riverhog-fixed-selector-preparation/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_selector_work.prepare_selector_page",
)
def test_preparation_detaches_one_fixed_incidence_and_complete_cursor(
    case, tmp_path, monkeypatch, record_property
):
    exercise(case, tmp_path, monkeypatch, record_property, PREPARE)


@pytest.mark.growth_witness(
    "riverhog-fixed-selector-commit/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_selector_work.apply_selector_page",
)
def test_atomic_commit_rollback_and_retry_have_indexed_incidence_cost(
    case, tmp_path, monkeypatch, record_property
):
    exercise(case, tmp_path, monkeypatch, record_property, APPLY)
