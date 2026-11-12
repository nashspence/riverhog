"""Installed cross-platform file tests; POSIX directory cases are explicit."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from retained_resource import Borrow, Resource

FAULT_EXTENSION = None


def encoded(path):
    return os.fsencode(path) if os.name == "posix" else str(path)


def fd_of(owner):
    borrow = Borrow(owner)
    borrow.acquire()
    result = borrow.fileno()
    borrow.close()
    return result


class ReceivingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="retained-construction-")
        self.root = Path(self.tmp.name)
        if os.name == "posix":
            self.root.chmod(0o700)

    def tearDown(self):
        self.tmp.cleanup()

    def file(self, name="member"):
        path = self.root / name
        path.write_bytes(b"original bytes")
        if os.name == "posix":
            path.chmod(0o600)
        return path

    def owner(self, path=None):
        r = Resource("file")
        r.acquire(encoded(path or self.file()))
        self.addCleanup(r.close)
        return r

    def test_non_ascii_native_path(self):
        path = self.root / "native-\u00e9-\U0001f432"
        r = Resource("file")
        r.acquire(encoded(path), writable=True, create=True, exclusive=True)
        borrower = Borrow(r)
        borrower.acquire()
        os.write(borrower.fileno(), b"exact bytes")
        borrower.close()
        r.close()
        self.assertEqual(path.read_bytes(), b"exact bytes")

    def test_exclusive_existing_never_truncates(self):
        path = self.file("snapshot.sqlite3")
        r = Resource("file")
        with self.assertRaises(FileExistsError):
            r.acquire(encoded(path), writable=True, create=True, exclusive=True)
        self.assertEqual(path.read_bytes(), b"original bytes")
        self.assertEqual(r.state, "absent")
        r.close()
        self.assertTrue(r.received)

    def test_before_open_retry_has_no_first_effect(self):
        path = self.root / "new"
        r = Resource("file")
        r._test_fault("before_open")
        with self.assertRaises(RuntimeError):
            r.acquire(encoded(path), writable=True, create=True, exclusive=True)
        self.assertFalse(path.exists())
        self.assertEqual(r.state, "new")
        r.acquire(encoded(path), writable=True, create=True, exclusive=True)
        r.close()
        self.assertEqual(r.metrics["open_calls"], 1)

    def test_after_open_reply_loss_retains_original(self):
        r = Resource("file")
        r._test_fault("after_open")
        with self.assertRaises(RuntimeError):
            r.acquire(encoded(self.file()))
        self.assertEqual(r.state, "live")
        self.assertEqual(os.read(fd_of(r), 14), b"original bytes")
        with self.assertRaises(RuntimeError):
            r.acquire(encoded(self.file("other")))
        r.close()
        self.assertEqual(r.metrics["open_calls"], 1)

    def test_close_refusal_retains_original(self):
        r = self.owner()
        fd = fd_of(r)
        r._test_fault("before_close")
        with self.assertRaises(RuntimeError):
            r.close()
        os.fstat(fd)
        self.assertFalse(r.received)
        self.assertEqual(r.metrics["close_calls"], 0)
        r.close()
        self.assertTrue(r.received)

    def reuse_after_reply_loss(self, same_inode):
        path = self.file()
        r = Resource("file")
        r.acquire(encoded(path))
        old = fd_of(r)
        r._test_fault("after_close")
        with self.assertRaises(RuntimeError):
            r.close()
        self.assertTrue(r.received)
        target = path if same_inode else self.file("unrelated")
        new = os.open(target, os.O_RDONLY)
        if new != old:
            os.dup2(new, old)
            os.close(new)
        try:
            original = os.fstat(old)
            for _ in range(20):
                r.close()
            self.assertEqual(os.fstat(old), original)
            self.assertEqual(os.read(old, 14), b"original bytes")
            self.assertEqual(r.metrics["close_calls"], 1)
        finally:
            os.close(old)

    def test_lost_close_reply_unrelated_slot_reuse(self):
        self.reuse_after_reply_loss(False)

    def test_lost_close_reply_same_inode_reopen(self):
        self.reuse_after_reply_loss(True)

    def test_signal_after_acquire_preserves_owner(self):
        r = Resource("file")
        r._test_fault("signal_after_open")
        with self.assertRaises(KeyboardInterrupt):
            r.acquire(encoded(self.file()))
        self.assertEqual(r.state, "live")
        r.close()

    def test_signal_after_close_retries_no_syscall(self):
        r = self.owner()
        r._test_fault("signal_after_close")
        with self.assertRaises(KeyboardInterrupt):
            r.close()
        self.assertTrue(r.received)
        r.close()
        self.assertEqual(r.metrics["close_calls"], 1)

    def test_rejected_post_open_retains_owner(self):
        r = Resource("file")
        r._test_fault("stat_refusal")
        with self.assertRaises(OSError):
            r.acquire(encoded(self.file()))
        self.assertEqual(r.state, "rejected")
        self.assertFalse(r.received)
        r.close()
        self.assertTrue(r.received)

    def test_active_borrow_prevents_close(self):
        r = self.owner()
        b = Borrow(r)
        b.acquire()
        with self.assertRaises(BlockingIOError):
            r.close()
        b.close()
        r.close()

    def test_invalid_path_refuses_before_dispatch(self):
        r = Resource("file")
        with self.assertRaises(RuntimeError):
            r.acquire(b"relative" if os.name == "posix" else "relative")
        self.assertEqual(r.metrics["open_calls"], 0)
        r.close()

    def test_read_only_reopen_preserves_bytes(self):
        path = self.file()
        r = self.owner(path)
        with self.assertRaises(OSError):
            os.write(fd_of(r), b"wrong")
        r.close()
        self.assertEqual(path.read_bytes(), b"original bytes")

    def unknown_child(self, fault):
        # Isolate intentionally unrecoverable resources; child exit is test
        # cleanup, never a positive in-process receiving receipt.
        code = """
import os,sys,tempfile,pathlib
import importlib.util
spec=importlib.util.spec_from_file_location("_construction_fault._native",sys.argv[3])
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
Resource,Borrow=module.Resource,module.Borrow
if True:
 p=pathlib.Path(sys.argv[2])/'f';p.write_bytes(b'x')
 r=Resource('file'); name=os.fsencode(p) if os.name=='posix' else str(p)
 fault=sys.argv[1]
 if fault=='unknown_open':r._test_fault(fault)
 try:r.acquire(name)
 except RuntimeError:pass
 if fault!='unknown_open':
  r._test_fault(fault)
  try:r.close()
  except OSError:pass
 assert r.state=='unknown' and not r.received
 count=r.metrics['close_calls']
 for _ in range(5):
  try:r.close()
  except RuntimeError:pass
  else:raise AssertionError('unknown accepted')
 assert r.metrics['close_calls']==count and not r.received
 # Windows open files cannot be removed here; exit owns probe-only cleanup.
 os._exit(0)
"""
        directory = self.root / fault
        directory.mkdir()
        child = subprocess.run(
            [sys.executable, "-c", code, fault, str(directory), str(FAULT_EXTENSION)],
            capture_output=True,
            text=True,
        )
        self.assertEqual(child.returncode, 0, child.stderr)

    def test_unknown_open_is_never_reopened(self):
        self.unknown_child("unknown_open")

    def test_unknown_close_before_effect_is_never_retried(self):
        self.unknown_child("unknown_close_open")

    def test_unknown_close_after_effect_is_never_retried(self):
        self.unknown_child("unknown_close_closed")

    @unittest.skipUnless(
        os.name == "posix", "directory backend is existing POSIX-only corpus profile"
    )
    def test_directory_close_reply_loss(self):
        r = Resource("directory")
        r.acquire(os.fsencode(self.root), private_owned=True)
        fd = fd_of(r)
        r._test_fault("after_close")
        with self.assertRaises(RuntimeError):
            r.close()
        other = os.open(self.file(), os.O_RDONLY)
        if other != fd:
            os.dup2(other, fd)
            os.close(other)
        try:
            r.close()
            os.fstat(fd)
        finally:
            os.close(fd)

    @unittest.skipUnless(os.name == "posix", "POSIX current UID/private mode check")
    def test_wrong_private_mode_is_retained_rejected(self):
        p = self.file()
        p.chmod(0o644)
        r = Resource("file")
        with self.assertRaises(RuntimeError):
            r.acquire(os.fsencode(p), private_owned=True)
        self.assertEqual(r.state, "rejected")
        r.close()
        self.assertTrue(r.received)


def test_sync_failure_keeps_original_resource_for_receiving(tmp_path, fault_native):
    import pytest

    native, _ = fault_native
    path = tmp_path / "sync"
    path.write_bytes(b"owned")
    target = encoded(tmp_path if os.name == "posix" else path)
    for fault in ("before_sync", "after_sync"):
        resource = native.Resource("directory" if os.name == "posix" else "file")
        resource.acquire(target, writable=os.name == "nt")
        resource._test_fault(fault)
        with pytest.raises(RuntimeError):
            resource.sync()
        assert resource.state == "live" and not resource.received
        assert resource.metrics["sync_calls"] == int(fault == "after_sync")
        resource.close()
        assert resource.received


def test_actual_sync_growth_is_independent_of_population_and_extent(tmp_path, record_property):
    from retained_resource.growth import sync
    from riverhog_growth_contracts import (
        growth_binding,
        project_bindings,
        validate_execution_samples,
    )

    from tests.support.workspace_reclamation import execution_sample

    binding = growth_binding(sync)
    projection = project_bindings((binding,))
    samples = []
    for population in (0, 17, 257):
        root = tmp_path / str(population)
        root.mkdir(mode=0o700)
        for item in range(population):
            (root / str(item)).touch()
        path = root / "owned"
        for extent in (0, 17, 65537, 8388608):
            with path.open("wb") as stream:
                stream.truncate(extent)
            kinds = ("file", "directory") if os.name == "posix" else ("file",)
            for kind in kinds:
                resource = Resource(kind)
                target = encoded(path if kind == "file" else root)
                resource.acquire(target, writable=kind == "file")
                before = resource.metrics
                sync(resource)
                after = resource.metrics
                metrics = {
                    bound.metric: after[bound.metric] - before[bound.metric]
                    for bound in binding.declaration.bounds
                }
                width = (
                    len(target) if isinstance(target, bytes) else len(target.encode("utf-16-le"))
                )
                sample = execution_sample(
                    binding,
                    projection,
                    {"unrelated_files": population, "path_octets": width},
                    metrics,
                    {},
                )
                validate_execution_samples(projection, binding.declaration.id, [sample])
                samples.append(sample)
                resource.close()
    record_property("growth_samples", samples)
