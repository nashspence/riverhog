from __future__ import annotations

import os
import sqlite3
from contextlib import closing, contextmanager

import pytest
from riverhog_core import canonical_discovery_archive_framing as framing
from riverhog_core.canonical_discovery_archive_source import ArchiveJournalSource
from riverhog_core.journal_validation_workspace import ValidationWorkspaceCustody
from riverhog_core.journal_validation_workspace_references import JournalWorkspaceReferences
from riverhog_core.services import discovery_journal_result as owner
from riverhog_core.services.discovery_validation_owner import (
    DiscoveryValidationOwner,
    DiscoveryValidationWork,
)
from riverhog_growth_contracts import growth_binding
from riverhog_provenance import JournalResultOpening, JournalSummary
from sqlalchemy import event, insert, update
from sqlalchemy.orm import Session

from tests.support.catalog_work import octets
from tests.unit.test_discovery_journal_result import open_result, prepared, quantum
from tests.unit.test_discovery_validation_owner_growth import Recorder, Slots, dimensions
from tests.unit.test_journal_validation_workspace_reference_growth import sample
from tests.unit.test_upload_journal_result import Job
from tests.unit.test_upload_journal_result_admission_growth import Rows, seed

PREPARE = owner.DiscoverySealedJournalOwner.prepare.__func__
OPEN = owner.DiscoverySealedJournalOwner.open
BORROW = owner.DiscoverySealedJournalOwner.borrow
CLOSE = owner.DiscoverySealedJournalOwner.close
CURRENT = owner.DiscoverySealedJournalSource.verify_current
DISPATCH = DiscoveryValidationWork.sealed_journal


@contextmanager
def observe(operation, monkeypatch):
    metrics = {bound.metric: 0 for bound in growth_binding(operation).declaration.bounds}
    depth = 0

    def wrap(function, metric):
        def measured(*args, **kwargs):
            nonlocal depth
            if depth == 0 and metric in metrics:
                metrics[metric] += 1
            depth += 1
            try:
                return function(*args, **kwargs)
            finally:
                depth -= 1

        return measured

    with monkeypatch.context() as patch:
        for module, name, metric in (
            (ArchiveJournalSource, "verify_current", "archive_checks"),
            (ValidationWorkspaceCustody, "verify_current", "custody_checks"),
            (Session, "begin", "catalog_transactions"),
            (owner, "_build_collection_current", "build_queries"),
            (owner, "prepare_upload_journal_result", "admission_preparations"),
            (owner.SealedJournalResultAdmission, "verify_current", "admission_probes"),
            (owner, "retain_validation_workspace", "reference_retains"),
            (owner, "lease_journal_validation_workspace", "lease_contexts"),
            (owner.DiscoverySealedJournalSource, "__init__", "source_constructions"),
            (owner, "open_retained_journal_result", "native_opens"),
            (owner.DiscoverySealedJournalSource, "verify_current", "source_checks"),
            (JournalSummary, "borrow", "native_borrows"),
            (JournalSummary, "close", "native_closes"),
            (JournalResultOpening, "close", "opening_closes"),
            (owner.ExitStack, "close", "context_closes"),
            (JournalWorkspaceReferences, "close", "reference_group_closes"),
            (owner.DiscoverySealedJournalOwner, "open", "child_opens"),
            (owner.DiscoverySealedJournalOwner, "close", "child_closes"),
            (owner.DiscoverySealedJournalOwner, "borrow", "child_borrows"),
        ):
            patch.setattr(module, name, wrap(getattr(module, name), metric))
        prepare = owner.DiscoverySealedJournalOwner.prepare.__func__
        patch.setattr(
            owner.DiscoverySealedJournalOwner,
            "prepare",
            classmethod(wrap(prepare, "preparations")),
        )
        anchor = JournalSummary.anchor
        patch.setattr(JournalSummary, "anchor", property(wrap(anchor.fget, "anchor_reads")))
        yield metrics


def exercise(tmp_path, monkeypatch, record_property, operation):
    samples = []
    for retained in (1, 17, 1025):
        with prepared(tmp_path / str(retained)) as case:
            seed(case.factory, retained)
            with quantum(case) as (work, source):
                values = {"retained_journals": retained + 1}
                if operation is PREPARE:
                    for _ in range(2):
                        with observe(operation, monkeypatch) as metrics:
                            selected = PREPARE(
                                owner.DiscoverySealedJournalOwner, source, 1, case.catalog
                            )
                        assert selected.result is None and selected.opening is None
                        assert case.engine.pool.checkedout() == 0
                        samples.append(sample(operation, values, metrics))
                elif operation is OPEN:
                    selected = owner.DiscoverySealedJournalOwner.prepare(source, 1, case.catalog)
                    work._slot.sealed = selected
                    with observe(operation, monkeypatch) as metrics:
                        result = OPEN(selected, case.native_root, case.catalog)
                    assert result is not None and metrics["native_opens"] == 1
                    samples.append(sample(operation, values, metrics))
                elif operation is DISPATCH:
                    for _ in range(2):
                        with observe(operation, monkeypatch) as metrics:
                            result = DISPATCH(work, source, collection_id=1)
                        result.close()
                        samples.append(sample(operation, values, metrics))
                    assert metrics["preparations"] == metrics["child_opens"] == 0
                else:
                    open_result(work, source).close()
                    selected = work._slot.sealed
                    if operation is CURRENT:
                        with observe(operation, monkeypatch) as metrics:
                            assert CURRENT(selected.native_source)
                        samples.append(sample(operation, values, metrics))
                        with case.factory() as session, session.begin():
                            session.execute(
                                update(Job)
                                .where(Job.workspace_id == case.job.workspace_id)
                                .values(version="f" * 64)
                            )
                        with observe(operation, monkeypatch) as metrics:
                            assert not CURRENT(selected.native_source)
                        samples.append(sample(operation, values, metrics))
                    else:
                        for _ in range(2):
                            with observe(operation, monkeypatch) as metrics:
                                result = operation(selected)
                            if operation is BORROW:
                                result.close()
                            samples.append(sample(operation, values, metrics))
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-discovery-sealed-result-prepare/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.discovery_journal_result.DiscoverySealedJournalOwner.prepare",
)
def test_detached_prepare_ports(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, PREPARE)


@pytest.mark.growth_witness(
    "riverhog-discovery-sealed-result-open/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.discovery_journal_result.DiscoverySealedJournalOwner.open",
)
def test_actual_readonly_open_ports(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, OPEN)


@pytest.mark.growth_witness(
    "riverhog-discovery-sealed-result-borrow/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.discovery_journal_result.DiscoverySealedJournalOwner.borrow",
)
def test_current_independent_borrow_ports(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, BORROW)


@pytest.mark.growth_witness(
    "riverhog-discovery-sealed-result-close/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.discovery_journal_result.DiscoverySealedJournalOwner.close",
)
def test_actual_close_and_retry_ports(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, CLOSE)


@pytest.mark.growth_witness(
    "riverhog-discovery-sealed-result-current/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.discovery_journal_result.DiscoverySealedJournalSource.verify_current",
)
def test_live_original_source_and_catalog_fence_ports(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, CURRENT)


@pytest.mark.growth_witness(
    "riverhog-discovery-sealed-result-dispatch/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.discovery_validation_owner.DiscoveryValidationWork.sealed_journal",
)
def test_actual_selected_slot_and_warm_retry_ports(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, DISPATCH)


@pytest.mark.growth_witness(
    "riverhog-discovery-sealed-build-collection-current/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.discovery_journal_result._build_collection_current",
)
def test_indexed_build_collection_probe_width_rows_and_vm(tmp_path, monkeypatch, record_property):
    operation = owner._build_collection_current
    samples = []
    for retained in (1, 17, 1025):
        with prepared(tmp_path / str(retained)) as case:
            with case.factory() as session, session.begin():
                row = session.get(owner.Generation, case.build_id)
                values = {
                    column.key: getattr(row, column.key)
                    for column in owner.Generation.__table__.columns
                }
                session.execute(
                    insert(owner.Generation),
                    [
                        dict(values, build_id="independent-build-" + str(index))
                        for index in range(retained)
                    ],
                )
            with quantum(case) as (_work, source):
                for collection_id in (1, 2, 1):
                    with case.factory() as session, session.begin():
                        metrics = {
                            bound.metric: 0
                            for bound in growth_binding(operation).declaration.bounds
                        }
                        driver = session.connection().connection.driver_connection
                        results, statements = [], []
                        execute = session.execute

                        def tick(_metrics=metrics):
                            _metrics["sqlite_vm_instructions"] += 1
                            return 0

                        def before(
                            _conn,
                            _cursor,
                            statement,
                            parameters,
                            _context,
                            _many,
                            _metrics=metrics,
                            _statements=statements,
                        ):
                            _metrics["sql_statements"] += 1
                            _metrics["parameter_octets"] += octets(parameters)
                            _statements.append((statement, parameters))

                        def observed(
                            *args, _execute=execute, _metrics=metrics, _results=results, **kwargs
                        ):
                            result = _execute(*args, **kwargs)
                            wrapped = Rows(result, _metrics)
                            _results.append(wrapped)
                            return wrapped

                        with monkeypatch.context() as patch:
                            patch.setattr(session, "execute", observed)
                            event.listen(case.engine, "before_cursor_execute", before)
                            driver.set_progress_handler(tick, 1)
                            try:
                                assert operation(session, source, collection_id) is (
                                    collection_id == 1
                                )
                            finally:
                                driver.set_progress_handler(None, 0)
                                event.remove(case.engine, "before_cursor_execute", before)
                        metrics["open_cursors"] = sum(not result.base.closed for result in results)
                        for statement, parameters in statements:
                            plans = driver.execute(
                                "EXPLAIN QUERY PLAN " + statement, parameters
                            ).fetchall()
                            assert not any(
                                "SCAN " in row[3] or "TEMP B-TREE" in row[3] for row in plans
                            )
                        samples.append(
                            sample(
                                operation,
                                {
                                    "catalog_builds": retained + 1,
                                    "control_octets": octets(
                                        (case.build_id, collection_id, case.build_id)
                                    ),
                                },
                                metrics,
                            )
                        )
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-discovery-sealed-composed-owner-close/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.discovery_validation_owner.DiscoveryValidationOwner.close",
)
@pytest.mark.parametrize("warm", [False, True])
def test_actual_sealed_and_warm_cleanup_in_composed_owner(
    tmp_path, monkeypatch, record_property, warm
):
    samples = []
    for retained in (1, 17, 1025):
        with prepared(tmp_path / str(retained)) as case:
            seed(case.factory, retained)
            case.pool.close()
            case.pool = DiscoveryValidationOwner(
                case.factory, case.root, journal_root=case.native_root
            )
            recorder = Recorder()
            case.readers = [case.reader]
            case.pool._slots = Slots(recorder)
            with recorder.connect_only():
                with quantum(case) as (work, source):
                    open_result(work, source).close()
                    helpers = [work._slot.sealed]
                    native_paths = [case.native_root / case.job.workspace_id / "snapshot.sqlite3"]
                    if warm:
                        task = framing.ArchiveJournalFramingWork(source)
                        with work.metadata.transaction() as (db, key):
                            framing.begin_archive_framing(db, key, task)
                        while True:
                            with work.metadata.read() as (db, key):
                                page = framing.prepare_archive_framing(
                                    db, key, task, byte_budget=65536, frame_budget=64
                                )
                            if page is None:
                                break
                            with work.metadata.transaction() as (db, key):
                                framing.apply_archive_framing(db, key, task, page)
                        assert work.journal(task) is not None
                        helpers.append(work._slot.journal)
                        native_paths.append(work._slot.journal.directory / "snapshot.sqlite3")
                    for helper in helpers:
                        store = (
                            helper.result._store
                            if isinstance(helper, owner.DiscoverySealedJournalOwner)
                            else helper.session._store
                        )
                        callback = store._storage.on_close

                        def release(_callback=callback, _recorder=recorder):
                            if _recorder.active:
                                _recorder.metrics["native_release_callbacks"] += 1
                            return _callback()

                        store._storage.on_close = release

                        class Stream:
                            def __init__(self, actual):
                                self.actual = actual

                            def close(self, _recorder=recorder):
                                if _recorder.active:
                                    _recorder.metrics["stream_close_calls"] += 1
                                return self.actual.close()

                        for reference in (helper.references.parent, helper.references.child):
                            reference._stream = Stream(reference._stream)
                values = dimensions(case, retained=retained, width=17)
                # Count the real application catalog and actual original Native
                # databases, which are independently located in this fixture.
                values["retained_catalog_rows"] = values["retained_native_rows"] = 0
                paths = [("catalog", case.engine.url.database)]
                paths.extend(("native", str(path)) for path in native_paths)
                for kind, path in paths:
                    values["path_octets"] = max(values["path_octets"], len(os.fsencode(path)))
                    with closing(sqlite3.connect(path)) as db:
                        for (table,) in db.execute(
                            "SELECT name FROM sqlite_master WHERE type='table'"
                        ):
                            for row in db.execute('SELECT * FROM "' + table + '"'):
                                values["retained_" + kind + "_rows"] += 1
                                values["control_octets"] = max(
                                    values["control_octets"], octets(row)
                                )
                with recorder.observe(case, cold=False):
                    case.pool.close()
                assert recorder.metrics["native_closed_connections"] == 1 + warm
                assert recorder.metrics["stream_close_calls"] == 2 * (1 + warm)
                samples.append(recorder.sample(type(case.pool).close, values))
    record_property("growth_samples", samples)
