from __future__ import annotations

import builtins
import errno
import fcntl
import hashlib
import io
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core import journal_validation_retirement as owner
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples

from tests.unit import test_journal_validation_workspace_growth as basis

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX retirement custody backend")
ONE = basis.ONE
DATA = tuple("snapshot.sqlite3" + suffix for suffix in ("-journal", "-wal", "-shm", ""))


class EvidenceFailure(RuntimeError):
    pass


class Recorder:
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

    def __init__(self, operation, root, identity, fault):
        self.operation, self.root, self.identity, self.fault = operation, root, identity, fault
        self.active = False
        self.live = set()
        self.invocations = {}
        self.metrics = {bound.metric: 0 for bound in growth_binding(operation).declaration.bounds}

    @contextmanager
    def paused(self):
        active, self.active = self.active, False
        try:
            yield
        finally:
            self.active = active

    @contextmanager
    def observe(self):
        actual_flock = fcntl.flock
        with pytest.MonkeyPatch.context() as patch:
            for name, metric in self.GROUPS.items():
                actual = getattr(os, name)

                def invoke(*args, _name=name, _metric=metric, _actual=actual, **kwargs):
                    active = self.active
                    if active:
                        self.metrics[_metric] += 1
                        self.invocations[_name] = self.invocations.get(_name, 0) + 1
                        if _name in {"open", "stat", "mkdir", "unlink", "rmdir"}:
                            self.metrics["path_octets"] += len(os.fsencode(args[0]))
                        if _name == "open":
                            assert not args[1] & (os.O_CREAT | os.O_TRUNC | os.O_APPEND)
                            assert args[1] & os.O_NOFOLLOW
                        if _name == "close":
                            assert args[0] in self.live
                        if (
                            self.fault is not None
                            and (_name, self.invocations[_name]) == self.fault[:2]
                        ):
                            raise OSError(self.fault[2], "injected retirement syscall failure")
                    result = _actual(*args, **kwargs)
                    if active and _name == "open":
                        assert result not in self.live
                        self.live.add(result)
                    if active and _name == "close":
                        self.live.remove(args[0])
                    return result

                patch.setattr(os, name, invoke)

            def flock(descriptor, flags):
                if self.active:
                    self.metrics["flock_calls"] += 1
                    assert descriptor in self.live
                    assert flags == fcntl.LOCK_UN or flags & fcntl.LOCK_NB
                return actual_flock(descriptor, flags)

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
                        raise EvidenceFailure("retirement custody crossed its metadata-only scope")
                    return _actual(*args, **kwargs)

                patch.setattr(module, name, forbidden)
            self.active = True
            try:
                yield
            finally:
                self.active = False
        assert not self.live


def measure(
    root, *, lease, identity=ONE, expected=None, error=None, body_error=None, fault=None, body=None
):
    operation = (
        owner.lease_retiring_journal_workspace
        if lease
        else owner.remove_retired_journal_workspace_directory
    )
    recorder = Recorder(operation, root, identity, fault)
    caught, returned = None, None
    with recorder.observe():
        try:
            if lease:
                with operation(root, identity) as returned:
                    with recorder.paused():
                        if body is not None:
                            body(returned)
                        if body_error is not None:
                            raise body_error
            else:
                returned = operation(root, identity)
        except BaseException as failure:
            caught = failure
    if error is None:
        assert caught is None
        assert returned == expected
    else:
        assert isinstance(caught, error)
        if body_error is not None:
            assert caught is body_error
    binding = growth_binding(operation)
    projection = project_bindings((binding,))
    sample = {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical_json_bytes(projection["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": {
            "root_path_octets": len(os.fsencode(root)),
            "workspace_identity_octets": len(identity.encode()),
        },
        "metrics": recorder.metrics,
        "delegates": {},
    }
    validate_execution_samples(projection, binding.declaration.id, [sample])
    return sample


@contextmanager
def held(path, *, shared=False):
    descriptor = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
    try:
        fcntl.flock(descriptor, (fcntl.LOCK_SH if shared else fcntl.LOCK_EX) | fcntl.LOCK_NB)
        yield
    finally:
        os.close(descriptor)


def body_custody(case):
    def verify(returned):
        assert returned == owner.RetiringJournalWorkspace(case.root / ONE)
        # Caller work is outside the leaf's metrics. Root coordination is free;
        # the exact selected lease still excludes a second independent reader.
        with held(case.root / ".coordinator"):
            pass
        with pytest.raises(BlockingIOError), held(case.root / ONE / ".lease", shared=True):
            pass

    return verify


def custody_samples(root, *, lease, samples, large=True):
    specifications = [(1, 0, 0)]
    if large:
        specifications.extend(((16, 1025, 0), (1, 0, 1024 * 1024)))
    for ordinal, (depth, siblings, payload) in enumerate(specifications):
        kinds = (
            "clean",
            "missing-directory",
            "missing-lease",
            "missing-coordinator",
            "coordinator-busy",
            "lease-busy",
            "reader-busy",
            "root-mode",
            "lease-mode",
            "lease-link",
            "lease-symlink",
            "invalid-identity",
            "relative-root",
            "capacity",
            "quota",
            "permission",
        )
        if lease:
            kinds += ("body-oserror", "body-valueerror")
        else:
            kinds += ("unclassified", "sync-interruption", *DATA)
        for kind in kinds:
            case = basis.setup(
                root / f"{ordinal}-{kind}",
                depth=depth,
                siblings=siblings,
                payload=payload if lease else 0,
                selected=kind != "missing-directory",
                lease=kind != "missing-lease",
                coordinator=kind != "missing-coordinator",
            )
            identity, selected_root = ONE, case.root
            fault, failure, body_error = None, None, None
            expected = owner.RetiringJournalWorkspace(case.root / ONE) if lease else True
            if kind == "missing-directory":
                expected = owner.RetiringJournalWorkspace(None) if lease else True
            elif kind == "missing-lease":
                failure = owner.LostJournalValidationWorkspace if lease else None
            elif kind == "missing-coordinator":
                failure = owner.LostJournalValidationWorkspace
            elif kind in {"root-mode", "lease-mode"}:
                (case.root if kind == "root-mode" else case.root / ONE / ".lease").chmod(
                    0o750 if kind == "root-mode" else 0o644
                )
                failure = owner.LostJournalValidationWorkspace
            elif kind == "lease-link":
                target = case.root / "other-lease-link"
                os.link(case.root / ONE / ".lease", target)
                case.paths.append(target)
                failure = owner.LostJournalValidationWorkspace
            elif kind == "lease-symlink":
                target = case.root / "original-lease"
                (case.root / ONE / ".lease").rename(target)
                (case.root / ONE / ".lease").symlink_to(target)
                case.paths.append(target)
                failure = owner.LostJournalValidationWorkspace
            elif kind == "invalid-identity":
                identity, failure = "not-an-owned-workspace", ValueError
            elif kind == "relative-root":
                selected_root, failure = Path("relative-retirement-root"), ValueError
            elif kind in {"capacity", "quota", "permission"}:
                number = {
                    "capacity": errno.ENOSPC,
                    "quota": errno.EDQUOT,
                    "permission": errno.EACCES,
                }[kind]
                fault = ("open", 1, number)
                failure = (
                    PermissionError if kind == "permission" else owner.JournalValidationCapacity
                )
            elif kind.startswith("body-"):
                body_error = (
                    OSError(errno.ENOENT, "caller body failure")
                    if kind == "body-oserror"
                    else ValueError("caller body failure")
                )
                failure = type(body_error)
            elif kind == "unclassified":
                unknown = case.root / ONE / "unclassified"
                basis.private_file(unknown, b"preserve unclassified payload")
                case.paths.append(unknown)
                failure = owner.LostJournalValidationWorkspace
            elif kind == "sync-interruption":
                fault, failure = ("fsync", 1, errno.ENOSPC), owner.JournalValidationCapacity
            elif kind in DATA:
                basis.private_file(case.root / ONE / kind, b"x" * payload)
                if case.root / ONE / kind not in case.paths:
                    case.paths.append(case.root / ONE / kind)
                failure = owner.LostJournalValidationWorkspace
            before = basis.snapshot(case)
            if kind in {"coordinator-busy", "lease-busy", "reader-busy"}:
                lock = case.root / (
                    ".coordinator" if kind == "coordinator-busy" else ONE + "/.lease"
                )
                with held(lock, shared=kind == "reader-busy"):
                    sample = measure(
                        selected_root,
                        lease=lease,
                        identity=identity,
                        expected=None if lease else False,
                    )
                assert basis.snapshot(case) == before
            else:
                sample = measure(
                    selected_root,
                    lease=lease,
                    identity=identity,
                    expected=expected,
                    error=failure,
                    fault=fault,
                    body_error=body_error,
                    body=body_custody(case)
                    if lease and kind in {"clean", "body-oserror", "body-valueerror"}
                    else None,
                )
                if (
                    lease
                    or failure is not None
                    and kind not in {"unclassified", "sync-interruption"}
                ):
                    assert basis.snapshot(case) == before
                if not lease and failure is None:
                    assert not (case.root / ONE).exists()
                    assert (case.root / ".coordinator").is_file()
                if kind == "unclassified":
                    assert unknown.read_bytes() == b"preserve unclassified payload"
                    assert not (case.root / ONE / ".lease").exists()
                    unknown.unlink()
                    samples.append(measure(case.root, lease=False, expected=True))
                if kind == "sync-interruption":
                    assert not (case.root / ONE / ".lease").exists()
                    samples.append(measure(case.root, lease=False, expected=True))
            samples.append(sample)


@pytest.mark.growth_witness(
    "riverhog-retired-workspace-custody-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.journal_validation_retirement.lease_retiring_journal_workspace",
)
def test_actual_retired_workspace_custody_is_bounded_and_exclusive(tmp_path, record_property):
    samples = []
    record_property("growth_samples", samples)
    custody_samples(tmp_path, lease=True, samples=samples)


@pytest.mark.growth_witness(
    "riverhog-retired-workspace-directory-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.journal_validation_retirement.remove_retired_journal_workspace_directory",
)
def test_actual_retired_workspace_directory_removal_preserves_unclassified_data(
    tmp_path, record_property
):
    samples = []
    record_property("growth_samples", samples)
    custody_samples(tmp_path, lease=False, samples=samples)
