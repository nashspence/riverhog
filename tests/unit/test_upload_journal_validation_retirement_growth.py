from __future__ import annotations

import builtins
import errno
import fcntl
import hashlib
import io
import os
import sqlite3
from collections import defaultdict
from contextlib import ExitStack, contextmanager
from dataclasses import asdict, replace

import pytest
import riverhog_provenance.journal_reclamation as native_reclamation
import workspace_reclamation
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.journal_validation_workspace import LostJournalValidationWorkspace
from riverhog_core.services import upload_journal_validation_retirement as owner
from riverhog_core.services.journal_retirement_registry import JournalRetirementTurn
from riverhog_core.services.physical_execution import PhysicalWorkspaceExecutionKey
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import JournalValidationCapacity

from tests.support.journal_retirement import cleanups as cleanups
from tests.support.retirement_namespace import observe_retained_namespace
from tests.support.workspace_reclamation import ReclamationMetrics, execution_sample
from tests.unit import test_journal_validation_workspace_growth as workspaces
from tests.unit import test_upload_journal_validation_retirement_catalog_growth as catalog
from tests.unit.test_archive_upload_execution import owned as owned
from tests.unit.test_journal_validation_retirement_custody_growth import held
from tests.unit.test_upload_journal_source_growth import octets
from tests.unit.test_upload_journal_validation_jobs_growth import Recorder as CatalogRecorder
from tests.unit.test_upload_journal_validation_jobs_growth import snapshot

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX retirement composition backend")
ROOT = owner.UploadJournalValidationRetirement._advance
CHILDREN = dict(owner._retirement_unit_children())
DATA = tuple("snapshot.sqlite3" + suffix for suffix in ("-journal", "-wal", "-shm", ""))
ALIASES = {
    "metadata_calls": "stat_calls",
    "owner_identity_calls": "uid_calls",
    "sync_calls": "fsync_calls",
    "payload_read_octets": "payload_octets",
    "enumeration_calls": "directory_enumerations",
}


class EvidenceFailure(RuntimeError):
    pass


def sample(operation, inputs, metrics, delegates=None):
    binding = growth_binding(operation)
    projection = project_bindings((binding,))
    if operation is workspace_reclamation.reclaim_retired_files:
        result = execution_sample(binding, projection, inputs, metrics, delegates or {})
        validate_execution_samples(projection, binding.declaration.id, [result])
        return result
    result = {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical_json_bytes(projection["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": inputs,
        "metrics": {
            bound.metric: metrics.get(bound.metric, 0) for bound in binding.declaration.bounds
        },
        "delegates": {} if delegates is None else delegates,
    }
    try:
        validate_execution_samples(projection, binding.declaration.id, [result])
    except ValueError as error:
        raise EvidenceFailure(
            "actual retirement composition violates its source-owned bounds"
        ) from error
    return result


class FilesystemRecorder:
    GROUPS = {
        "open": "open_calls",
        "close": "close_calls",
        "stat": "stat_calls",
        "fstat": "stat_calls",
        "getuid": "uid_calls",
        "mkdir": "mkdir_calls",
        "fsync": "fsync_calls",
        "unlink": "unlink_calls",
        "rmdir": "rmdir_calls",
        "ftruncate": "truncate_calls",
    }

    def __init__(self, fault):
        self.metrics, self.live = defaultdict(int), set()
        self.active, self.scope, self.fault = False, None, fault
        self.invocations = defaultdict(int)
        self.attempts = []

    @contextmanager
    def paused(self):
        active, self.active = self.active, False
        try:
            yield
        finally:
            self.active = active

    @contextmanager
    def observe(self):
        actual_flock, actual_fstat, actual_stat = fcntl.flock, os.fstat, os.stat
        actual_fsencode, actual_compare = os.fsencode, workspace_reclamation._compare_names
        actual_directory_encoder = workspace_reclamation._encode_directory
        encoding_directory = False
        with pytest.MonkeyPatch.context() as patch, observe_retained_namespace(self):
            for name, metric in self.GROUPS.items():
                actual = getattr(os, name)

                def invoke(*args, _name=name, _metric=metric, _actual=actual, **kwargs):
                    active, previous = self.active, 0
                    if active:
                        self.metrics[_metric] += 1
                        self.invocations[self.scope, _name] += 1
                        if _name in {"open", "stat", "mkdir", "unlink", "rmdir"}:
                            self.metrics["path_octets"] += len(actual_fsencode(args[0]))
                        if _name == "open":
                            assert not args[1] & (os.O_CREAT | os.O_TRUNC | os.O_APPEND)
                            assert args[1] & os.O_NOFOLLOW
                        if _name == "close":
                            assert args[0] in self.live
                        if self.fault == (self.scope, _name, self.invocations[self.scope, _name]):
                            raise OSError(
                                errno.ENOSPC, ("injected retirement synchronization capacity")
                            )
                        if _name == "ftruncate":
                            previous = actual_fstat(args[0]).st_size
                        if _name == "unlink" and self.scope == "reclaim":
                            previous = actual_stat(args[0], follow_symlinks=False, **kwargs).st_size
                    result = _actual(*args, **kwargs)
                    if active:
                        if _name == "open":
                            assert result not in self.live
                            self.live.add(result)
                        if _name == "close":
                            self.live.remove(args[0])
                        if _name == "ftruncate":
                            self.metrics["reclaimed_extent_octets"] += (
                                previous - actual_fstat(args[0]).st_size
                            )
                        if _name == "unlink" and self.scope == "reclaim":
                            self.metrics["reclaimed_extent_octets"] += previous
                    return result

                patch.setattr(os, name, invoke)

            def flock(descriptor, flags):
                if self.active:
                    self.metrics["flock_calls"] += 1
                    assert descriptor in self.live
                    assert flags == fcntl.LOCK_UN or flags & fcntl.LOCK_NB
                return actual_flock(descriptor, flags)

            def fsencode(name):
                result = actual_fsencode(name)
                if self.active and self.scope == "reclaim":
                    metric = (
                        "encoded_directory_octets" if encoding_directory else "encoded_name_octets"
                    )
                    self.metrics[metric] += len(result)
                return result

            def compare(left, right, **kwargs):
                if self.active:
                    self.metrics["name_order_comparisons"] += 1
                    self.metrics["name_comparison_octets"] += len(left) + len(right)
                return actual_compare(left, right, **kwargs)

            def encode_directory(directory):
                nonlocal encoding_directory
                previous, encoding_directory = encoding_directory, True
                try:
                    return actual_directory_encoder(directory)
                finally:
                    encoding_directory = previous

            def native_observed(actual):
                binding = growth_binding(actual)

                def observed(receiver, *args, **kwargs):
                    originals = [
                        resource.metrics for resource in (receiver._directory, receiver._file)
                    ]
                    beginning = dict(self.metrics)
                    try:
                        return actual(receiver, *args, **kwargs)
                    finally:
                        for resource, before in zip(
                            (receiver._directory, receiver._file), originals, strict=True
                        ):
                            after = resource.metrics
                            for name, metric in (
                                ("open_calls", "open_calls"),
                                ("close_calls", "close_calls"),
                                ("fstat_calls", "stat_calls"),
                                ("dup_calls", "dup_calls"),
                                ("control_octets", "native_control_octets"),
                            ):
                                self.metrics[metric] += after[name] - before[name]
                        if actual.__name__ == "reclaim":
                            observed_metrics = differences(self.metrics, beginning)
                            self.attempts.append(
                                {
                                    "binding": binding,
                                    "names": len(kwargs["names"]),
                                    "directory_octets": len(receiver._path),
                                    "octet_budget": kwargs["octet_budget"],
                                    "metrics": {
                                        bound.metric: observed_metrics.get(
                                            ALIASES.get(bound.metric, bound.metric), 0
                                        )
                                        for bound in binding.declaration.bounds
                                    },
                                }
                            )

                return observed

            patch.setattr(workspace_reclamation, "_encode_directory", encode_directory)
            for name in ("reclaim", "close"):
                patch.setattr(
                    workspace_reclamation.FileReclamationOwner,
                    name,
                    native_observed(getattr(workspace_reclamation.FileReclamationOwner, name)),
                )
            patch.setattr(os, "fsencode", fsencode)
            patch.setattr(workspace_reclamation, "_compare_names", compare)
            patch.setattr(fcntl, "flock", flock)
            for module, name, metric in (
                *(
                    (os, name, "payload_octets")
                    for name in ("read", "pread", "readv", "write", "pwrite", "writev", "fdopen")
                ),
                (builtins, "open", "payload_octets"),
                (io, "open", "payload_octets"),
                (os, "listdir", "directory_enumerations"),
                (os, "scandir", "directory_enumerations"),
                (sqlite3, "connect", "sqlite_connections"),
            ):
                actual = getattr(module, name)

                def forbidden(*args, _actual=actual, _metric=metric, **kwargs):
                    if self.active:
                        self.metrics[_metric] += 1
                        raise EvidenceFailure(
                            "retirement interpreted native payload or enumerated storage"
                        )
                    return _actual(*args, **kwargs)

                patch.setattr(module, name, forbidden)
            yield
        assert not self.live


def differences(after, before):
    return {key: after.get(key, 0) - before.get(key, 0) for key in set(after) | set(before)}


def totals(recorder):
    result = dict(recorder.metrics)
    result["changed_rows"] = sum(db.total_changes for db in recorder.native)
    result["connection_statements"] = result["driver_statements"] - result["sql_statements"]
    result["source_payload_octets"] = 0
    return result


def measure(
    case, current, job, budget, *, turn, cold=False, error=None, fault=None, observed_error=None
):
    before = snapshot(case.engine)
    row_width = max(octets(row) for rows in before.values() for row in rows)
    control = tuple(asdict(job).values())
    dimensions = {
        "octet_budget": budget,
        "retained_rows": sum(map(len, before.values())),
        "metadata_row_octets": max(row_width, octets(control)),
        "control_octets": octets(control),
        "root_path_octets": len(os.fsencode(current.root)),
        "workspace_identity_octets": len(job.workspace_id.encode()),
    }
    sql, fs = CatalogRecorder(), FilesystemRecorder(fault)
    children = {tag: [] for tag in CHILDREN}

    def record(tag, inputs, metrics, delegates=None):
        if tag in {"reclaim", "receive"}:
            metrics = {
                bound.metric: metrics.get(ALIASES.get(bound.metric, bound.metric), 0)
                for bound in growth_binding(CHILDREN[tag]).declaration.bounds
            }
        children[tag].append(sample(CHILDREN[tag], inputs, metrics, delegates))

    def transition(selected, target, state):
        arguments = tuple(asdict(target).values())
        inputs = {
            "retained_rows": dimensions["retained_rows"],
            "metadata_row_octets": max(row_width, octets((*arguments, state))),
            "control_octets": octets((arguments, state)),
            "excluded_jobs": 0,
        }
        beginning = totals(sql)
        try:
            return CHILDREN["transition"](selected, target, state)
        finally:
            record("transition", inputs, differences(totals(sql), beginning))

    @contextmanager
    def lease(root, identity, *, handles):
        inputs = {
            "root_path_octets": len(os.fsencode(root)),
            "workspace_identity_octets": len(identity.encode()),
        }
        previous, fs.scope = fs.scope, "lease"
        beginning, body = dict(fs.metrics), defaultdict(int)
        receive_start = len(children["receive"])
        try:
            with CHILDREN["lease"](root, identity, handles=handles) as owned:
                body_start = dict(fs.metrics)
                fs.scope = previous
                try:
                    yield owned
                finally:
                    body = differences(fs.metrics, body_start)
                    fs.scope = "lease"
        finally:
            metrics = differences(fs.metrics, beginning)
            metrics = differences(metrics, body)
            for received in children["receive"][receive_start:]:
                metrics = differences(
                    metrics,
                    {ALIASES.get(name, name): value for name, value in received["metrics"].items()},
                )
            fs.scope = previous
            record("lease", inputs, metrics)

    def directory(root, identity, *, handles):
        inputs = {
            "root_path_octets": len(os.fsencode(root)),
            "workspace_identity_octets": len(identity.encode()),
        }
        previous, fs.scope = fs.scope, "directory"
        beginning = dict(fs.metrics)
        try:
            return CHILDREN["directory"](root, identity, handles=handles)
        finally:
            fs.scope = previous
            record("directory", inputs, differences(fs.metrics, beginning))

    receive_inputs = None

    def reclaim(path, *, receiver, octet_budget):
        nonlocal receive_inputs
        with fs.paused():
            active, sql.active = sql.active, False
            try:
                assert catalog.read(case, job.workspace_id).state == "retiring"
                with held(current.root / ".coordinator"):
                    pass
                with pytest.raises(BlockingIOError), held(path / ".lease", shared=True):
                    pass
            finally:
                sql.active = active
            extents = [item.stat().st_size for name in DATA if (item := path / name).exists()]
        inputs = {
            "directory_octets": len(os.fsencode(path)),
            "retained_octets": sum(extents),
            "retained_files": len(extents),
            "octet_budget": octet_budget,
        }
        previous, fs.scope = fs.scope, "reclaim"
        beginning = dict(fs.metrics)
        files_operation = native_reclamation.reclaim_retired_files
        file_children = []

        def files(directory, *, receiver, names, octet_budget):
            nonlocal receive_inputs
            with fs.paused():
                encoded = tuple(os.fsencode(name) for name in names)
            child_inputs = {
                "candidate_files": len(names),
                "name_octets": sum(map(len, encoded)),
                "maximum_name_octets": max(map(len, encoded), default=0),
                **inputs,
                "octet_budget": octet_budget,
            }
            receive_inputs = child_inputs
            child_beginning = dict(fs.metrics)
            attempt_start = len(fs.attempts)
            try:
                return files_operation(
                    directory, receiver=receiver, names=names, octet_budget=octet_budget
                )
            finally:
                actual = differences(fs.metrics, child_beginning)
                child_metrics = ReclamationMetrics(
                    {
                        bound.metric: actual.get(ALIASES.get(bound.metric, bound.metric), 0)
                        for bound in growth_binding(files_operation).declaration.bounds
                    }
                )
                child_metrics.attempts = fs.attempts[attempt_start:]
                file_children.append(sample(files_operation, child_inputs, child_metrics))

        try:
            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(native_reclamation, "reclaim_retired_files", files)
                return CHILDREN["reclaim"](path, receiver=receiver, octet_budget=octet_budget)
        finally:
            fs.scope = previous
            record(
                "reclaim",
                inputs,
                differences(fs.metrics, beginning),
                {"files": file_children},
            )

    caught, result = None, None
    with fs.observe(), pytest.MonkeyPatch.context() as patch:
        actual_close = workspace_reclamation.FileReclamationOwner.close

        def receive(receiver):
            assert receive_inputs is not None and receiver is turn._files
            beginning = dict(fs.metrics)
            try:
                return actual_close(receiver)
            finally:
                record("receive", receive_inputs, differences(fs.metrics, beginning))

        patch.setattr(workspace_reclamation.FileReclamationOwner, "close", receive)
        patch.setattr(owner.UploadJournalValidationRetirement, "_transition", transition)
        patch.setattr(owner, "lease_retiring_journal_workspace", lease)
        patch.setattr(owner, "remove_retired_journal_workspace_directory", directory)
        patch.setattr(owner, "reclaim_retired_journal_workspace", reclaim)
        with sql.observe(case.engine, cold=cold):
            fs.active = True
            try:
                result = ROOT(current, job, budget, turn=turn)
            except (
                JournalValidationCapacity,
                LostJournalValidationWorkspace,
                RuntimeError,
            ) as failure:
                if isinstance(failure, EvidenceFailure):
                    raise
                caught = failure
            finally:
                fs.active = False
    if error is None:
        assert caught is None
    else:
        assert isinstance(caught, error)
    if observed_error is not None:
        observed_error.append(caught)
    after = snapshot(case.engine)
    assert before[catalog.jobs.Journal.__tablename__] == after[catalog.jobs.Journal.__tablename__]
    assert tuple(
        row for row in before[catalog.Job.__tablename__] if row[0] != job.workspace_id
    ) == tuple(row for row in after[catalog.Job.__tablename__] if row[0] != job.workspace_id)
    assert case.engine.pool.checkedout() == 0
    metrics = {**sql.metrics, **fs.metrics}
    own_sql = dict(sql.metrics)
    for child in children["transition"]:
        own_sql = differences(own_sql, child["metrics"])
    delete_calls = own_sql["sql_statements"]
    dimensions["delete_calls"] = delete_calls
    metrics.update({"own_" + key: value for key, value in own_sql.items()})
    metrics["delete_calls"] = metrics["delete_calls_limit"] = delete_calls
    metrics["custody_branches"] = len(children["lease"]) + len(children["directory"])
    metrics["catalog_branches"] = len(children["transition"]) + delete_calls
    metrics["reclaim_requires_lease"] = len(children["reclaim"])
    metrics["native_admission_octets"] = sum(
        child["dimensions"]["octet_budget"] for child in children["reclaim"]
    )
    for tag, operation in CHILDREN.items():
        dimensions[tag + "_calls"] = len(children[tag])
        metrics[tag + "_calls"] = metrics[tag + "_calls_limit"] = len(children[tag])
        for item in growth_binding(operation).declaration.dimensions:
            dimensions[tag + "_" + item.name] = (
                children[tag][0]["dimensions"][item.name] if children[tag] else 0
            )
        for bound in growth_binding(operation).declaration.bounds:
            metrics[tag + "_" + bound.metric] = sum(
                child["metrics"][bound.metric] for child in children[tag]
            )
    return result, sample(ROOT, dimensions, metrics, children)


def measure_in_worker(case, original, job, budget, **kwargs):
    current = original.cleanup
    turn = JournalRetirementTurn(current, job)
    key = PhysicalWorkspaceExecutionKey(job.workspace_id, job.version)
    target = "native-journal:" + str(current.root / job.workspace_id)

    def work():
        try:
            return measure(case, current, job, budget, turn=turn, **kwargs)
        finally:
            turn.close()

    assert (
        original.pool.try_submit_workspace(
            key, target_incarnation=target, work=work, owner=turn, identity=turn
        )
        == "submitted"
    )
    slot = original.pool._slots[key]
    slot.worker.join(timeout=30)
    assert not slot.worker.is_alive()
    (completed,) = original.pool.collect_workspace(owner=turn, limit=1)
    assert completed.key == key and completed.target_incarnation == target
    assert completed.identity is turn and completed.terminal
    result = completed.result()
    assert turn.received and not original.pool.has_unreceived_work()
    return result


def unit_samples(root, samples, cleanups, *, large=True):
    with catalog.build_cases(root / "catalogs", large=large) as cases:
        for index, case in enumerate(cases):
            for cold in (False, True):
                for kind in (
                    "opening",
                    "missing-directory",
                    "empty",
                    "truncate",
                    "zero",
                    "four-files",
                    "busy-writer",
                    "busy-reader",
                    "directory",
                    "directory-missing",
                    "unclassified",
                    "directory-data",
                    "stale-delete",
                    "stale-transition",
                    "linked",
                    "reclaim-capacity",
                    "directory-capacity",
                ):
                    state = (
                        "directory"
                        if kind.startswith("directory") or kind in {"unclassified", "stale-delete"}
                        else "opening"
                        if kind in {"opening", "stale-transition", "linked"}
                        else "retiring"
                    )
                    fields = {**case.selected, "state": state}
                    if kind == "linked":
                        fields.update(collection_id=1, journal_id=catalog.JOURNAL)
                    base, case.base = (
                        case.base,
                        [
                            row
                            for row in case.base
                            if kind != "linked" or row["journal_id"] != catalog.JOURNAL
                        ],
                    )
                    catalog.reset(case, fields)
                    case.base = base
                    layout = workspaces.setup(
                        root / "files" / f"{index}-{cold}-{kind}",
                        depth=16 if index == 2 else 1,
                        siblings=1025 if index == 2 and kind == "truncate" else 0,
                        selected=kind not in {"missing-directory", "directory-missing"},
                    )
                    directory = layout.root / catalog.ONE
                    extent = 1048577 if index == 3 else 65537
                    if kind in {"truncate", "reclaim-capacity", "directory-data"}:
                        workspaces.private_file(directory / DATA[-1], b"p" * extent)
                    if kind == "zero":
                        workspaces.private_file(directory / DATA[-1])
                    if kind == "four-files":
                        for name in DATA:
                            workspaces.private_file(directory / name, b"p" * 17)
                    if kind == "unclassified":
                        workspaces.private_file(
                            directory / "unclassified", b"retain unrelated bytes"
                        )
                    case.root = layout.root
                    original = cleanups(case)
                    argument = catalog.read(case)
                    if kind in {"stale-delete", "stale-transition"}:
                        argument = replace(argument, version="1" * 64)
                    failure = {
                        "unclassified": LostJournalValidationWorkspace,
                        "directory-data": LostJournalValidationWorkspace,
                        "linked": RuntimeError,
                        "reclaim-capacity": JournalValidationCapacity,
                        "directory-capacity": JournalValidationCapacity,
                    }.get(kind)
                    fault = (
                        ("reclaim", "fsync", 1)
                        if kind == "reclaim-capacity"
                        else ("directory", "fsync", 1)
                        if kind == "directory-capacity"
                        else None
                    )
                    with ExitStack() as resources:
                        if kind in {"busy-writer", "busy-reader"}:
                            resources.enter_context(
                                held(directory / ".lease", shared=kind == "busy-reader")
                            )
                        result, observed = measure_in_worker(
                            case, original, argument, 65536, cold=cold, error=failure, fault=fault
                        )
                    if failure is None:
                        assert result is (
                            kind
                            not in {
                                "busy-writer",
                                "busy-reader",
                                "stale-delete",
                                "stale-transition",
                            }
                        )
                    selected = catalog.read(case)
                    if kind in {"directory", "directory-missing"}:
                        assert selected is None and not directory.exists()
                    elif kind in {"missing-directory", "empty", "zero"}:
                        assert selected.state == "directory"
                    elif kind == "opening":
                        assert selected.state == "retiring"
                    elif kind in {"truncate", "reclaim-capacity"}:
                        assert (directory / DATA[-1]).read_bytes() == b"p" * (extent - 65536)
                        assert selected.state == "retiring"
                    elif kind == "unclassified":
                        assert (
                            directory / "unclassified"
                        ).read_bytes() == b"retain unrelated bytes"
                    if kind in {"directory-capacity", "stale-delete"}:
                        # Retry reconciles lease unlink or an already removed exact directory.
                        result, retry = measure_in_worker(
                            case, original, catalog.read(case), 65536, cold=cold
                        )
                        assert result is True and catalog.read(case) is None
                        samples.append(retry)
                    samples.append(observed)


def history_samples(root, samples, cleanups, *, large=True):
    with catalog.build_cases(root / "catalogs", large=large) as cases:
        for index, case in enumerate(cases):
            for cold, budget, extents in (
                (False, 7, (17, 0, 29)),
                (True, 65536, (0, 17, 65537, 131073)),
            ):
                catalog.reset(case, {**case.selected, "state": "active"})
                layout = workspaces.setup(root / "files" / f"{index}-{cold}")
                for name, size in zip(DATA, extents, strict=False):
                    workspaces.private_file(layout.root / catalog.ONE / name, b"p" * size)
                case.root = layout.root
                original = cleanups(case)
                expected_units = 2 + sum(max(1, (size + budget - 1) // budget) for size in extents)
                reclaimed = 0
                for number in range(expected_units):
                    argument = catalog.read(case)
                    assert argument is not None
                    result, observed = measure_in_worker(
                        case, original, argument, budget, cold=cold
                    )
                    assert result is True
                    reclaimed += observed["metrics"]["reclaimed_extent_octets"]
                    samples.append(observed)
                    if number < expected_units - 1:
                        assert catalog.read(case) is not None
                assert reclaimed == sum(extents)
                assert catalog.read(case) is None and not (layout.root / catalog.ONE).exists()
                assert (layout.root / ".coordinator").is_file()


@pytest.mark.growth_witness(
    "riverhog-validation-retirement-unit-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_retirement.UploadJournalValidationRetirement._advance",
)
def test_actual_retirement_unit_composes_catalog_custody_extents_and_directory(
    tmp_path, record_property, cleanups
):
    samples = []
    record_property("growth_samples", samples)
    unit_samples(tmp_path / "branches", samples, cleanups)
    history_samples(tmp_path / "histories", samples, cleanups)
