from __future__ import annotations

import builtins
import errno
import fcntl
import hashlib
import io
import os
import sqlite3
import stat
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core import journal_validation_workspace as owner
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX workspace lease backend")

ONE, TWO = "a" * 32, "b" * 32


class Recorder:
    GROUPS = {
        "open": "open_calls",
        "close": "close_calls",
        "stat": "stat_calls",
        "fstat": "stat_calls",
        "getuid": "uid_calls",
        "mkdir": "mkdir_calls",
        "fsync": "fsync_calls",
    }

    def __init__(self):
        self.active = False
        self.live = set()
        self.metrics = {
            name: 0
            for name in {
                *self.GROUPS.values(),
                "flock_calls",
                "path_octets",
                "creation_callbacks",
                "payload_octets",
                "directory_enumerations",
            }
        }

    def callback(self, operation):
        def invoke():
            self.metrics["creation_callbacks"] += 1
            active, self.active = self.active, False
            try:
                return operation()
            finally:
                self.active = active

        return invoke

    @contextmanager
    def observe(self):
        real_flock = fcntl.flock
        with pytest.MonkeyPatch.context() as patch:
            for name, metric in self.GROUPS.items():
                actual = getattr(os, name)

                def invoke(*args, _name=name, _metric=metric, _actual=actual, **kwargs):
                    active = self.active
                    if active:
                        self.metrics[_metric] += 1
                        if _name in {"open", "mkdir", "stat"}:
                            assert not isinstance(args[0], int)
                            self.metrics["path_octets"] += len(os.fsencode(args[0]))
                        if _name == "mkdir":
                            assert (args[1] if len(args) > 1 else kwargs["mode"]) == 0o700
                        if _name == "close":
                            assert args[0] in self.live
                    if active and _name == "open":
                        assert not args[1] & (os.O_TRUNC | os.O_APPEND)
                        if Path(os.fsdecode(args[0])) != self.root.parent:
                            assert args[1] & os.O_NOFOLLOW
                        if args[1] & os.O_CREAT:
                            assert (args[2] if len(args) > 2 else kwargs["mode"]) == 0o600
                    result = _actual(*args, **kwargs)
                    if active and _name == "open":
                        assert result not in self.live
                        self.live.add(result)
                    if active and _name == "close":
                        self.live.remove(args[0])
                    return result

                patch.setattr(os, name, invoke)
            for name in (
                "read",
                "pread",
                "readv",
                "listdir",
                "scandir",
                "unlink",
                "rmdir",
                "rename",
                "replace",
            ):
                actual = getattr(os, name)

                def forbidden(*args, _name=name, _actual=actual, **kwargs):
                    if self.active:
                        raise AssertionError(
                            "workspace custody read payload or enumerated a directory"
                        )
                    return _actual(*args, **kwargs)

                patch.setattr(os, name, forbidden)

            for module, name in (
                (builtins, "open"),
                (io, "open"),
                (os, "fdopen"),
                (sqlite3, "connect"),
            ):
                actual = getattr(module, name)

                def forbidden_open(*args, _actual=actual, **kwargs):
                    if self.active:
                        raise AssertionError(
                            "workspace custody opened payload or interpreted a database"
                        )
                    return _actual(*args, **kwargs)

                patch.setattr(module, name, forbidden_open)

            def flock(descriptor, flags):
                if self.active:
                    self.metrics["flock_calls"] += 1
                    assert flags == fcntl.LOCK_UN or flags & fcntl.LOCK_NB
                    assert descriptor in self.live
                return real_flock(descriptor, flags)

            patch.setattr(fcntl, "flock", flock)
            self.active = True
            try:
                yield
            finally:
                self.active = False
        assert not self.live


def private_file(path, payload=b""):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        if payload:
            with os.fdopen(os.dup(descriptor), "wb") as stream:
                stream.write(payload)
    finally:
        os.close(descriptor)


def setup(root, *, depth=1, siblings=0, payload=0, selected=True, lease=True, coordinator=True):
    for number in range(depth - 1):
        root = root / f"d{number}"
    root.mkdir(mode=0o700, parents=True)
    paths = [
        root,
        root / ".coordinator",
        root / ONE,
        root / ONE / ".lease",
        root / ONE / "snapshot.sqlite3",
    ]
    if coordinator:
        private_file(root / ".coordinator")
    if selected:
        (root / ONE).mkdir(mode=0o700)
        if lease:
            private_file(root / ONE / ".lease")
        if payload:
            private_file(root / ONE / "snapshot.sqlite3", b"x" * payload)
    for number in range(siblings):
        path = root / f"{number:032x}"
        path.mkdir(mode=0o700)
        paths.append(path)
    return SimpleNamespace(root=root, paths=paths)


def snapshot(case):
    records = []
    for path in case.paths:
        try:
            value = path.lstat()
        except FileNotFoundError:
            records.append(None)
            continue
        digest = (
            hashlib.sha256(path.read_bytes()).hexdigest()
            if path.name == "snapshot.sqlite3" and stat.S_ISREG(value.st_mode)
            else None
        )
        records.append(
            (
                value.st_dev,
                value.st_ino,
                value.st_mode,
                value.st_nlink,
                value.st_size,
                value.st_mtime_ns,
                digest,
            )
        )
    return tuple(records)


def measure(
    root,
    *,
    identity=ONE,
    create=False,
    shared=False,
    callback=None,
    expected_callback=0,
    expected=None,
    error=None,
    body_error=None,
):
    recorder = Recorder()
    recorder.root = root
    value, caught = None, None
    operation = owner.lease_journal_validation_workspace
    binding = growth_binding(operation)
    projection = project_bindings((binding,))
    inputs = {
        "root_path_octets": len(os.fsencode(root)),
        "workspace_identity_octets": len(identity.encode()),
    }
    with recorder.observe():
        try:
            with operation(
                root,
                identity,
                create=create,
                shared=shared,
                creation_current=None if callback is None else recorder.callback(callback),
            ) as value:
                if body_error is not None:
                    raise body_error
        except BaseException as failure:
            caught = failure
    if error is None:
        assert caught is None
        assert value == expected
    else:
        assert isinstance(caught, error)
        if body_error is not None:
            assert caught is body_error
    assert recorder.metrics["creation_callbacks"] == expected_callback
    sample = {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical_json_bytes(projection["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": inputs,
        "metrics": recorder.metrics,
        "delegates": {},
    }
    validate_execution_samples(projection, binding.declaration.id, [sample])
    return sample


def lease_samples(root, *, samples, large=True):
    specifications = [(1, 0, 0), (17, 0, 0), (1, 17, 0)]
    if large:
        specifications.extend(
            ((65, 0, 0), (1, 129, 0), (1, 1025, 0), (1, 0, 65537), (1, 0, 1024 * 1024))
        )
    root.mkdir(parents=True)
    for index, (depth, siblings, payload) in enumerate(specifications):
        case = setup(root / ("é-" + str(index)), depth=depth, siblings=siblings, payload=payload)
        before = snapshot(case)
        for create, shared in ((False, False), (True, False), (False, True)):
            samples.append(
                measure(
                    case.root,
                    create=create,
                    shared=shared,
                    callback=lambda: True,
                    expected_callback=int(create),
                    expected=case.root / ONE,
                )
            )
            assert snapshot(case) == before
        with owner.lease_journal_validation_workspace(case.root, ONE) as held:
            assert held == case.root / ONE
            for shared in (False, True):
                samples.append(measure(case.root, shared=shared, expected=None))
            assert snapshot(case) == before
        with owner.lease_journal_validation_workspace(case.root, ONE, shared=True):
            with owner.lease_journal_validation_workspace(case.root, ONE, shared=True):
                samples.append(measure(case.root, shared=False, expected=None))
                samples.append(measure(case.root, shared=True, expected=case.root / ONE))
        with owner.lease_journal_validation_workspace(case.root, TWO, create=True):
            samples.append(measure(case.root, expected=case.root / ONE))
        coordinator = os.open(case.root / ".coordinator", os.O_RDWR)
        try:
            fcntl.flock(coordinator, fcntl.LOCK_EX | fcntl.LOCK_NB)
            samples.append(
                measure(
                    case.root,
                    create=True,
                    callback=lambda: True,
                    expected_callback=0,
                    expected=None,
                )
            )
        finally:
            os.close(coordinator)
        # A failure in the caller body retains its own meaning after exact release.
        failure = OSError(errno.ENOSPC, "caller body failure")
        samples.append(measure(case.root, error=OSError, body_error=failure))
    new = root / "new"
    samples.append(
        measure(new, create=True, callback=lambda: True, expected_callback=1, expected=new / ONE)
    )
    assert stat.S_IMODE(new.stat().st_mode) == 0o700
    assert stat.S_IMODE((new / ONE / ".lease").stat().st_mode) == 0o600
    samples.append(measure(root / "missing", error=owner.LostJournalValidationWorkspace))
    for name, options in (
        ("stale", {"selected": False}),
        ("empty", {"lease": False}),
        ("lost", {"lease": False, "payload": 65537}),
        ("coordinator", {"coordinator": False}),
        ("absent", {"selected": False}),
    ):
        case = setup(root / name, **options)
        before = snapshot(case)
        if name == "stale":
            samples.append(
                measure(
                    case.root,
                    create=True,
                    callback=lambda: False,
                    expected_callback=1,
                    expected=None,
                )
            )
        elif name == "empty":
            samples.append(
                measure(
                    case.root,
                    create=True,
                    callback=lambda: True,
                    expected_callback=1,
                    expected=case.root / ONE,
                )
            )
            assert (case.root / ONE / ".lease").is_file()
            continue
        elif name == "lost":
            samples.append(
                measure(
                    case.root,
                    create=True,
                    callback=lambda: True,
                    expected_callback=1,
                    error=owner.LostJournalValidationWorkspace,
                )
            )
        else:
            samples.append(measure(case.root, error=owner.LostJournalValidationWorkspace))
        assert snapshot(case) == before
    for fault in ("mode", "symlink", "hardlink", "directory"):
        case = setup(root / ("foreign-" + fault))
        selected = case.root / ONE / ".lease"
        if fault == "mode":
            selected.chmod(0o644)
        elif fault == "symlink":
            selected.unlink()
            target = case.root / "owned-foreign-target"
            private_file(target)
            selected.symlink_to(target)
            case.paths.append(target)
        elif fault == "hardlink":
            target = case.root / "owned-foreign-target"
            os.link(selected, target)
            case.paths.append(target)
        else:
            case.root.chmod(0o755)
        before = snapshot(case)
        samples.append(measure(case.root, error=owner.LostJournalValidationWorkspace))
        assert snapshot(case) == before
    for kwargs, error in (
        ({"identity": "a" * 65537}, ValueError),
        ({"create": 1}, TypeError),
        ({"shared": "yes"}, TypeError),
        ({"create": True, "shared": True}, ValueError),
    ):
        samples.append(measure(new, **kwargs, error=error))
    samples.append(measure(Path("relative"), error=ValueError))


@pytest.mark.growth_witness(
    "riverhog-validation-workspace-lease-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.journal_validation_workspace.lease_journal_validation_workspace",
)
def test_actual_workspace_lease_bounds_complete_enter_exit_and_kernel_custody(
    tmp_path, record_property
):
    samples = []
    record_property("growth_samples", samples)
    lease_samples(tmp_path / "leases", samples=samples)
