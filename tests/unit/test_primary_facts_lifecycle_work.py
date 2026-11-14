"""Measured source-owned catalog, namespace, disposal and receiving work."""

from __future__ import annotations

import builtins
import fcntl
import hashlib
import io
import os
import sqlite3
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import asdict, replace
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core import primary_facts_workspace as workspace_owner
from riverhog_core.catalog_primary_facts_models import PrimaryFactsJobRecord as Row
from riverhog_core.primary_facts_reclamation import PrimaryFactsRetirementTurn
from riverhog_core.primary_facts_workspace import PrimaryFactsWorkspace, capture_root_identity
from riverhog_core.services import primary_facts_jobs as jobs
from riverhog_core.services.upload_primary_facts_retirement import UploadPrimaryFactsRetirement
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_protocol.errors import Conflict
from sqlalchemy import event, select

from tests.support.catalog_work import octets
from tests.unit.test_archive_upload_execution import _joined
from tests.unit.test_primary_facts_lifecycle import (
    OWNER,
    RESTART,
    advance,
    current,
    finish,
    finish_scheduled,
    lifecycle_case,
    pinned,
    populate,
    registered,
    retired,
    scheduled,
)


def sample(operation, dimensions, metrics):
    binding = growth_binding(operation)
    projection = project_bindings((binding,))
    result = {
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
    validate_execution_samples(projection, binding.declaration.id, [result])
    return result


def rows(case):
    with case.factory() as session:
        return [jobs.PrimaryFactsJob.read(row) for row in session.scalars(select(Row))]


@contextmanager
def catalog_work(case, monkeypatch):
    """Count actual logical SQL/fetches and VM work, keeping pool setup outside."""
    measured = defaultdict(int)
    active = False
    statements = []
    engine = case.engine
    engine.dispose()
    dbapi = engine.dialect.dbapi
    connect = dbapi.connect

    def returned(cursor, row):
        if active and getattr(cursor, "owned_query", False) and row is not None:
            measured["returned_rows"] += 1
            measured["returned_metadata_octets"] += octets(row)
        return row

    class Cursor(sqlite3.Cursor):
        owned_query = False

        def fetchone(self):
            return returned(self, super().fetchone())

        def fetchmany(self, *args):
            return [returned(self, row) for row in super().fetchmany(*args)]

        def fetchall(self):
            return [returned(self, row) for row in super().fetchall()]

        def __next__(self):
            return returned(self, super().__next__())

    class Connection(sqlite3.Connection):
        def cursor(self, *args, **kwargs):
            return super().cursor(*args, **{**kwargs, "factory": Cursor})

    def connected(*args, **kwargs):
        connection = connect(*args, **{**kwargs, "factory": Connection})

        def stepped():
            if active:
                measured["sqlite_vm_instructions"] += 1
            return 0

        connection.set_progress_handler(stepped, 1)
        return connection

    def statement(_connection, cursor, sql, parameters, _context, many):
        assert not many
        if active:
            cursor.owned_query = True
            measured["sql_statements"] += 1
            measured["bound_parameter_octets"] += octets(parameters)
            statements.append((sql, parameters))

    event.listen(engine, "before_cursor_execute", statement)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(dbapi, "connect", connected)
            with engine.connect():
                pass
            active = True
            try:
                yield measured
            finally:
                active = False
    finally:
        event.remove(engine, "before_cursor_execute", statement)
    for sql, parameters in statements:
        if "primary_facts_jobs" not in sql or sql.lstrip().upper().startswith("INSERT"):
            continue
        with engine.connect() as connection:
            plan = " ".join(
                row[3]
                for row in connection.exec_driver_sql("EXPLAIN QUERY PLAN " + sql, parameters)
            )
        assert "SEARCH" in plan and "SCAN" not in plan and "USE TEMP" not in plan
    assert engine.pool.checkedout() == 0


def measured_catalog(case, monkeypatch, operation, invoke, *, controls):
    before = rows(case)
    with catalog_work(case, monkeypatch) as measured:
        result = invoke()
    after = rows(case)
    result_row = (result,) if isinstance(result, jobs.PrimaryFactsJob) else ()
    dimensions = {
        "retained_rows": max(len(before), len(after)),
        "metadata_row_octets": max(
            [octets(controls), *(octets(asdict(row)) for row in (*before, *after, *result_row))]
        ),
        "control_octets": octets(controls),
    }
    return result, sample(operation, dimensions, measured)


def catalog_samples(tmp_path, monkeypatch, mode):
    samples = []
    for unrelated, width in ((0, 0), (17, 0), (0, 4096)):
        with lifecycle_case(tmp_path / f"{unrelated}-{width}") as case:
            for ordinal in range(unrelated):
                registered(case, collection_id=ordinal + 2)
            controls = {
                "collection_id": 1,
                "construction": "opaque construction " + "x" * width,
                "epoch": "e" * 64,
                "owner_id": OWNER,
                "root_identity": case.root_identity,
            }
            if mode == "register":

                def invoke(controls=controls):
                    with case.factory() as session, session.begin():
                        return jobs.register(session, **controls)

                result, observed = measured_catalog(
                    case, monkeypatch, jobs.register, invoke, controls=controls
                )
                assert (
                    result.state == "allocated" and not (case.root / result.workspace_id).exists()
                )
                samples.append(observed)
                continue
            with case.factory() as session, session.begin():
                job = jobs.register(session, **controls)
            if mode in {"transition", "finish"}:
                with case.factory() as session, session.begin():
                    job = jobs.retire(session, job)
                    if mode == "finish":
                        job = jobs.transition(session, job, "memo")
                        job = jobs.transition(session, job, "directory")
            operation = getattr(jobs, mode)
            arguments = {
                "get": (job.workspace_id,),
                "find_pending": (job.collection_id,),
                "current": (job,),
                "pin": (job, '{"directory":["1","2","16832","3"],"lease":["1","4","33152","3"]}'),
                "retire": (job,),
                "transition": (job, "memo"),
                "finish": (job,),
            }[mode]

            def invoke(arguments=arguments, operation=operation):
                with case.factory() as session, session.begin():
                    return operation(session, *arguments)

            result, observed = measured_catalog(
                case,
                monkeypatch,
                operation,
                invoke,
                controls=(asdict(job), arguments[1:] if len(arguments) > 1 else ()),
            )
            assert result is not None and result is not False
            samples.append(observed)
            if mode in {"get", "find_pending", "current"}:
                absent = {
                    "get": ("f" * 32,),
                    "find_pending": (1000000,),
                    "current": (replace(job, version="f" * 32),),
                }[mode]
                result, observed = measured_catalog(
                    case,
                    monkeypatch,
                    operation,
                    lambda absent=absent: invoke(absent),
                    controls=asdict(job),
                )
                assert result is None or result is False
                samples.append(observed)
            else:
                result, observed = measured_catalog(
                    case, monkeypatch, operation, invoke, controls=asdict(job)
                )
                assert result is None or result is False
                samples.append(observed)
    return samples


@pytest.mark.growth_witness(
    "riverhog-primary-lifecycle-register-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.primary_facts_jobs.register",
)
def test_registration_work_precedes_namespace_effects(tmp_path, monkeypatch, record_property):
    record_property("growth_samples", catalog_samples(tmp_path, monkeypatch, "register"))


@pytest.mark.growth_witness(
    "riverhog-primary-lifecycle-get-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.primary_facts_jobs.get",
)
def test_exact_job_lookup_work(tmp_path, monkeypatch, record_property):
    record_property("growth_samples", catalog_samples(tmp_path, monkeypatch, "get"))


@pytest.mark.growth_witness(
    "riverhog-primary-lifecycle-pending-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.primary_facts_jobs.find_pending",
)
def test_unfinished_collection_lookup_work(tmp_path, monkeypatch, record_property):
    record_property("growth_samples", catalog_samples(tmp_path, monkeypatch, "find_pending"))


@pytest.mark.growth_witness(
    "riverhog-primary-lifecycle-current-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.primary_facts_jobs.current",
)
def test_exact_current_catalog_fence_work(tmp_path, monkeypatch, record_property):
    record_property("growth_samples", catalog_samples(tmp_path, monkeypatch, "current"))


@pytest.mark.growth_witness(
    "riverhog-primary-lifecycle-pin-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.primary_facts_jobs.pin",
)
def test_namespace_pin_compare_and_swap_work(tmp_path, monkeypatch, record_property):
    record_property("growth_samples", catalog_samples(tmp_path, monkeypatch, "pin"))


@pytest.mark.growth_witness(
    "riverhog-primary-lifecycle-retire-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.primary_facts_jobs.retire",
)
def test_retirement_selection_compare_and_swap_work(tmp_path, monkeypatch, record_property):
    record_property("growth_samples", catalog_samples(tmp_path, monkeypatch, "retire"))


@pytest.mark.growth_witness(
    "riverhog-primary-lifecycle-transition-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.primary_facts_jobs.transition",
)
def test_monotonic_phase_transition_work(tmp_path, monkeypatch, record_property):
    record_property("growth_samples", catalog_samples(tmp_path, monkeypatch, "transition"))


@pytest.mark.growth_witness(
    "riverhog-primary-lifecycle-finish-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.primary_facts_jobs.finish",
)
def test_final_catalog_pointer_receiving_work(tmp_path, monkeypatch, record_property):
    record_property("growth_samples", catalog_samples(tmp_path, monkeypatch, "finish"))


@pytest.mark.growth_witness(
    "riverhog-primary-lifecycle-discovery-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_primary_facts_retirement.UploadPrimaryFactsRetirement._select",
)
def test_discovery_seeks_bounded_candidates_independently_of_unrelated_rows(
    tmp_path, monkeypatch, record_property
):
    samples = []
    for unrelated in (0, 17):
        with lifecycle_case(tmp_path / str(unrelated)) as case, scheduled(case) as (cleanup, _, _):
            selected = registered(case)
            for ordinal in range(unrelated):
                registered(case, collection_id=ordinal + 2)
            seen = set()
            for _ in range(unrelated + 2):
                result, observed = measured_catalog(
                    case,
                    monkeypatch,
                    UploadPrimaryFactsRetirement._select,
                    lambda: cleanup._select(()),
                    controls=(cleanup._after, cleanup._phase),
                )
                assert result is not None
                seen.add(result.workspace_id)
                samples.append(observed)
            assert selected.workspace_id in seen and len(seen) == unrelated + 1
    record_property("growth_samples", samples)


def retirement_catalog_samples(tmp_path, monkeypatch, mode):
    samples = []
    for unrelated, width in ((0, 0), (17, 0), (0, 4096)):
        with lifecycle_case(tmp_path / f"{unrelated}-{width}") as case:
            for ordinal in range(unrelated):
                registered(case, collection_id=ordinal + 2)
            with case.factory() as session, session.begin():
                job = jobs.register(
                    session,
                    collection_id=1,
                    construction="opaque construction " + "x" * width,
                    epoch="e" * 64,
                    owner_id=OWNER,
                    root_identity=case.root_identity,
                )
                if mode in {"transition", "finish"}:
                    job = jobs.retire(session, job)
                    if mode == "finish":
                        job = jobs.transition(session, job, "memo")
                        job = jobs.transition(session, job, "directory")
            with scheduled(case) as (cleanup, _, _):
                operation = getattr(UploadPrimaryFactsRetirement, mode)
                failure = "exact namespace custody refused " + "b" * width
                arguments = {
                    "current": (job,),
                    "transition": (job, "memo"),
                    "finish": (job,),
                    "blocked": (job, failure),
                }[mode]

                def invoke(arguments=arguments, operation=operation):
                    return operation(cleanup, *arguments)

                result, observed = measured_catalog(
                    case, monkeypatch, operation, invoke, controls=(asdict(job), arguments[1:])
                )
                assert result is not None and result is not False
                samples.append(observed)
                if mode == "current":
                    arguments = (replace(job, version="f" * 32),)

                    def repeated(arguments=arguments, invoke=invoke):
                        return invoke(arguments)

                elif mode == "transition":

                    def repeated(arguments=arguments, operation=operation):
                        with pytest.raises(Conflict, match="identity changed"):
                            operation(cleanup, *arguments)
                        return False

                else:
                    repeated = invoke
                result, observed = measured_catalog(
                    case, monkeypatch, operation, repeated, controls=(asdict(job), arguments[1:])
                )
                # An absent final pointer reconciles the original commit's lost
                # acknowledgement; a repeated blocker changes no catalog value.
                assert result is (mode == "finish")
                samples.append(observed)
                if mode == "blocked":
                    received = current(case, job.workspace_id)
                    assert received.failure == failure and received.version == job.version
    return samples


@pytest.mark.growth_witness(
    "riverhog-primary-retirement-current-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_primary_facts_retirement.UploadPrimaryFactsRetirement.current",
)
def test_physical_current_callback_catalog_work(tmp_path, monkeypatch, record_property):
    record_property("growth_samples", retirement_catalog_samples(tmp_path, monkeypatch, "current"))


@pytest.mark.growth_witness(
    "riverhog-primary-retirement-transition-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_primary_facts_retirement.UploadPrimaryFactsRetirement.transition",
)
def test_physical_phase_callback_catalog_work(tmp_path, monkeypatch, record_property):
    record_property(
        "growth_samples", retirement_catalog_samples(tmp_path, monkeypatch, "transition")
    )


@pytest.mark.growth_witness(
    "riverhog-primary-retirement-finish-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_primary_facts_retirement.UploadPrimaryFactsRetirement.finish",
)
def test_physical_final_callback_catalog_work(tmp_path, monkeypatch, record_property):
    record_property("growth_samples", retirement_catalog_samples(tmp_path, monkeypatch, "finish"))


@pytest.mark.growth_witness(
    "riverhog-primary-retirement-blocked-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_primary_facts_retirement.UploadPrimaryFactsRetirement.blocked",
)
def test_exact_blocker_callback_catalog_work(tmp_path, monkeypatch, record_property):
    record_property("growth_samples", retirement_catalog_samples(tmp_path, monkeypatch, "blocked"))


@pytest.mark.growth_witness(
    "riverhog-primary-retirement-coordination-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_primary_facts_retirement.UploadPrimaryFactsRetirement.advance",
)
def test_actual_cleanup_scheduling_has_explicit_candidate_and_receiving_admissions(
    tmp_path, monkeypatch, record_property
):
    samples = []
    for unrelated, limit in ((0, 1), (17, 1), (0, 3)):
        with lifecycle_case(tmp_path / f"{unrelated}-{limit}") as case:
            job, workspace = pinned(case)
            populate(workspace.path, sidecars=False)
            workspace.close()
            job = retired(case, job)
            for ordinal in range(unrelated):
                registered(case, collection_id=ordinal + 2, owner_id=RESTART)
            with scheduled(case) as (cleanup, execution, _):
                submissions = 0
                for admit in (True, False, True, False):
                    measured = defaultdict(int)
                    with monkeypatch.context() as patch:
                        selected = cleanup._select

                        def select_candidate(*args, _measured=measured, _selected=selected):
                            _measured["selection_calls"] += 1
                            return _selected(*args)

                        patch.setattr(cleanup, "_select", select_candidate)
                        for name, metric in (
                            ("collect", "collect_limit"),
                            ("resume_ready", "resume_limit"),
                            ("submit", "submit_calls"),
                        ):
                            operation = getattr(cleanup._registry, name)

                            def counted(
                                *args,
                                _metric=metric,
                                _operation=operation,
                                _measured=measured,
                                **kwargs,
                            ):
                                _measured[_metric] += (
                                    1 if _metric == "submit_calls" else kwargs["limit"]
                                )
                                result = _operation(*args, **kwargs)
                                if _metric == "submit_calls":
                                    _measured["new_submissions"] += result
                                return result

                            patch.setattr(cleanup._registry, name, counted)
                        measured["bookkeeping_events"] = cleanup.advance(
                            limit=limit, octet_budget=1, admit=admit
                        )
                    if not admit:
                        assert measured["selection_calls"] == measured["submit_calls"] == 0
                    submissions += measured["new_submissions"]
                    samples.append(
                        sample(UploadPrimaryFactsRetirement.advance, {"limit": limit}, measured)
                    )
                    _joined(execution)
                assert submissions > 0 and (workspace.path / "corpus.sqlite3").exists()
                finish_scheduled(case, cleanup, execution, [job.workspace_id])
    record_property("growth_samples", samples)


@contextmanager
def physical_work(monkeypatch, original=None):
    """Observe direct syscalls plus the actual retained native owners' work."""
    metrics = defaultdict(int)
    active = True
    if isinstance(original, PrimaryFactsWorkspace):
        resources = [record.owner for record in original.lease.records]
    elif isinstance(original, PrimaryFactsRetirementTurn):
        resources = [
            original._root.owner,
            original._coordinator.owner,
            original._directory.owner,
            original._lease.owner,
            original._files._directory,
            original._files._file,
        ]
    else:
        resources = []
    before = [resource.metrics for resource in resources]
    fsencode, fstat, stat = os.fsencode, os.fstat, os.stat
    loads, dumps = workspace_owner.json.loads, workspace_owner.json.dumps

    def callback(operation, metric):
        def invoke(*args, **kwargs):
            nonlocal active
            previous = active
            if active:
                metrics[metric] += 1
            active = False
            try:
                return operation(*args, **kwargs)
            finally:
                active = previous

        return invoke

    def encoded(value):
        result = fsencode(value)
        if active:
            metrics["path_octets"] += len(result)
        return result

    def decoded(value, *args, **kwargs):
        if active:
            metrics["json_decoded_octets"] += len(
                value.encode() if isinstance(value, str) else value
            )
        return loads(value, *args, **kwargs)

    def dumped(*args, **kwargs):
        value = dumps(*args, **kwargs)
        if active:
            metrics["json_encoded_octets"] += len(value.encode())
        return value

    with monkeypatch.context() as patch:
        for name, metric in (
            ("stat", "stat_calls"),
            ("fstat", "stat_calls"),
            ("getuid", "uid_calls"),
            ("mkdir", "mkdir_calls"),
            ("fsync", "fsync_calls"),
            ("ftruncate", "truncate_calls"),
            ("unlink", "unlink_calls"),
            ("rmdir", "rmdir_calls"),
            ("open", "open_calls"),
            ("close", "close_calls"),
        ):
            operation = getattr(os, name)

            def counted(*args, _name=name, _metric=metric, _operation=operation, **kwargs):
                removed = 0
                if active:
                    metrics[_metric] += 1
                    if _name in {"stat", "mkdir", "unlink", "rmdir", "open"}:
                        metrics["path_octets"] += len(fsencode(args[0]))
                    if _name == "ftruncate":
                        removed = fstat(args[0]).st_size - args[1]
                    elif _name == "unlink":
                        removed = stat(
                            args[0], dir_fd=kwargs.get("dir_fd"), follow_symlinks=False
                        ).st_size
                result = _operation(*args, **kwargs)
                if active:
                    metrics["reclaimed_extent_octets"] += removed
                return result

            patch.setattr(os, name, counted)
        flock = fcntl.flock

        def locked(*args, **kwargs):
            if active:
                metrics["flock_calls"] += 1
                assert args[1] == fcntl.LOCK_UN or args[1] & fcntl.LOCK_NB
            return flock(*args, **kwargs)

        patch.setattr(fcntl, "flock", locked)
        patch.setattr(os, "fsencode", encoded)
        patch.setattr(workspace_owner, "json", SimpleNamespace(loads=decoded, dumps=dumped))
        for module, name in (
            (os, "listdir"),
            (os, "scandir"),
            (os, "read"),
            (os, "pread"),
            (builtins, "open"),
            (io, "open"),
            (sqlite3, "connect"),
        ):
            operation = getattr(module, name)

            def forbidden(*args, _name=name, _operation=operation, **kwargs):
                if active:
                    pytest.fail(f"primary lifecycle performed forbidden physical {_name}")
                return _operation(*args, **kwargs)

            patch.setattr(module, name, forbidden)
        if isinstance(original, PrimaryFactsRetirementTurn):
            for name in ("current", "transition", "finish"):
                patch.setattr(
                    original.cleanup,
                    name,
                    callback(getattr(original.cleanup, name), name + "_calls"),
                )
        try:
            yield SimpleNamespace(metrics=metrics, callback=callback)
        finally:
            active = False
    for resource, previous in zip(resources, before, strict=True):
        latest = resource.metrics
        for name, metric in (
            ("open_calls", "open_calls"),
            ("close_calls", "close_calls"),
            ("fstat_calls", "stat_calls"),
            ("dup_calls", "dup_calls"),
            ("control_octets", "native_control_octets"),
        ):
            metrics[metric] += latest[name] - previous[name]


def measured_physical(
    case, monkeypatch, operation, original, invoke, *, budget=None, controls=None
):
    controls = {} if controls is None else controls
    with physical_work(monkeypatch, original) as measured:
        result = invoke(measured)
    dimensions = {
        "root_path_octets": len(os.fsencode(case.root)),
        "control_octets": octets(controls),
    }
    if budget is not None:
        dimensions["octet_budget"] = budget
    return result, sample(operation, dimensions, measured.metrics)


def workspace_samples(tmp_path, monkeypatch, mode):
    samples = []
    operation = {
        "root": capture_root_identity,
        "open": PrimaryFactsWorkspace.open,
        "current": PrimaryFactsWorkspace.require_current,
        "close": PrimaryFactsWorkspace.close,
    }[mode]
    for siblings, extent in ((0, 17), (17, 17), (0, 65537)):
        with lifecycle_case(tmp_path / f"{siblings}-{extent}") as case:
            for ordinal in range(siblings):
                (case.root / f"unrelated-{ordinal}").mkdir(mode=0o700)
            if mode == "root":
                result, observed = measured_physical(
                    case,
                    monkeypatch,
                    operation,
                    None,
                    lambda _measured: operation(case.root),
                )
                assert result == case.root_identity
            elif mode == "open":
                job = registered(case)
                original = PrimaryFactsWorkspace(case.root, job)
                case.originals.append(original)
                result, observed = measured_physical(
                    case,
                    monkeypatch,
                    operation,
                    original,
                    lambda measured, original=original, job=job: original.open(
                        measured.callback(lambda: case.cleanup.current(job), "current_calls")
                    ),
                    controls=asdict(job),
                )
                assert result is True
            else:
                job, original = pinned(case)
                populate(original.path, extent=extent)
                result, observed = measured_physical(
                    case,
                    monkeypatch,
                    operation,
                    original,
                    lambda _measured, original=original, job=job: (
                        original.require_current(job) if mode == "current" else original.close()
                    ),
                    controls=asdict(job),
                )
                assert result is None
                if mode == "close":
                    assert original.received and original.path.exists()
            samples.append(observed)
    return samples


@pytest.mark.growth_witness(
    "riverhog-primary-root-identity-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.primary_facts_workspace.capture_root_identity",
)
def test_original_root_identity_work(tmp_path, monkeypatch, record_property):
    record_property("growth_samples", workspace_samples(tmp_path, monkeypatch, "root"))


@pytest.mark.growth_witness(
    "riverhog-primary-workspace-open-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.primary_facts_workspace.PrimaryFactsWorkspace.open",
)
def test_original_workspace_creation_work(tmp_path, monkeypatch, record_property):
    record_property("growth_samples", workspace_samples(tmp_path, monkeypatch, "open"))


@pytest.mark.growth_witness(
    "riverhog-primary-workspace-current-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.primary_facts_workspace.PrimaryFactsWorkspace.require_current",
)
def test_live_workspace_currentness_work(tmp_path, monkeypatch, record_property):
    record_property("growth_samples", workspace_samples(tmp_path, monkeypatch, "current"))


@pytest.mark.growth_witness(
    "riverhog-primary-workspace-close-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.primary_facts_workspace.PrimaryFactsWorkspace.close",
)
def test_original_workspace_close_work(tmp_path, monkeypatch, record_property):
    record_property("growth_samples", workspace_samples(tmp_path, monkeypatch, "close"))


@pytest.mark.growth_witness(
    "riverhog-primary-retirement-physical-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.primary_facts_reclamation.PrimaryFactsRetirementTurn.advance",
)
def test_exact_sidecar_extents_and_final_namespace_have_measured_work(
    tmp_path, monkeypatch, record_property
):
    samples = []
    for siblings, extent, budget in ((0, 17, 1), (17, 17, 1), (0, 65537, 1), (0, 65537, 65536)):
        with lifecycle_case(tmp_path / f"{siblings}-{extent}-{budget}") as case:
            for ordinal in range(siblings):
                (case.root / f"unrelated-{ordinal}").mkdir(mode=0o700)
            job, workspace = pinned(case)
            populate(workspace.path, extent=extent, sidecars=True)
            workspace.close()
            job = retired(case, job)
            for phase in ("corpus", "memo", "directory"):
                while current(case, job.workspace_id).state != phase:
                    advance(case, current(case, job.workspace_id))
                selected = current(case, job.workspace_id)
                original = PrimaryFactsRetirementTurn(case.cleanup, selected)
                case.originals.append(original)
                result, observed = measured_physical(
                    case,
                    monkeypatch,
                    PrimaryFactsRetirementTurn.advance,
                    original,
                    lambda _measured, original=original, budget=budget: original.advance(
                        octet_budget=budget
                    ),
                    budget=budget,
                    controls=asdict(selected),
                )
                assert original.received and result.advanced
                assert result.removed_octets == observed["metrics"]["reclaimed_extent_octets"]
                samples.append(observed)
            assert current(case, job.workspace_id) is None and not workspace.path.exists()
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-primary-retirement-close-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.primary_facts_reclamation.PrimaryFactsRetirementTurn.close",
)
def test_original_retirement_close_never_repeats_disposal(tmp_path, monkeypatch, record_property):
    samples = []
    for extent in (17, 65537):
        with lifecycle_case(tmp_path / str(extent)) as case:
            job, workspace = pinned(case)
            populate(workspace.path, extent=extent, sidecars=False)
            workspace.close()
            job = retired(case, job)
            original = PrimaryFactsRetirementTurn(case.cleanup, job)
            case.originals.append(original)
            sync = os.fsync

            def failed(descriptor, workspace=workspace, extent=extent, sync=sync):
                if (workspace.path / "corpus.sqlite3").stat().st_size == extent - 1:
                    raise OSError(28, "injected original synchronization refusal")
                return sync(descriptor)

            with monkeypatch.context() as patch:
                patch.setattr(os, "fsync", failed)
                with pytest.raises(Exception, match="capacity|synchronization"):
                    original.advance(octet_budget=1)
            for _ in range(2):
                _, observed = measured_physical(
                    case,
                    monkeypatch,
                    PrimaryFactsRetirementTurn.close,
                    original,
                    lambda _measured, original=original: original.close(),
                    controls=asdict(job),
                )
                samples.append(observed)
                assert original.received
                assert (workspace.path / "corpus.sqlite3").stat().st_size == extent - 1
            finish(case, job.workspace_id)
    record_property("growth_samples", samples)
