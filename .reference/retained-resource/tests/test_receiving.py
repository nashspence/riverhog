"""Installed cross-platform file tests; POSIX directory cases are explicit."""
from __future__ import annotations
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest

from retained_resource import Borrow, Resource
from retained_resource.construction import Construction


def encoded(path):
    return os.fsencode(path) if os.name == "posix" else str(path)


def fd_of(owner):
    borrow = Borrow(owner)
    borrow.acquire()
    result = borrow.fileno()
    borrow.close()
    return result


class Scope:
    """Test driver for an INDEPENDENT receiving owner, not a SQLite proof."""
    def __init__(self):
        self.held = False
        self.calls = 0
        self.unknown = False
    def connect(self, *_args, **_kw):
        self.calls += 1
        self.held = True
        return self
    def close(self):
        if not self.unknown:
            self.held = False
    @property
    def received(self):
        return not self.held and not self.unknown


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
    def test_passive_constructor_has_no_effect(self):
        scope = Scope()
        frame = Construction(scope, resource_limit=2)
        self.assertEqual(frame.creator.metrics["open_calls"], 0)
        self.assertEqual(scope.calls, 0)
        self.assertTrue(frame.close_step())
    def test_capacity_before_effect(self):
        with self.assertRaises(ValueError):
            Construction(Scope(), resource_limit=0)
        self.assertEqual(list(self.root.iterdir()), [])
    def test_exclusive_creator(self):
        path = self.root / "snapshot.sqlite3"
        frame = Construction(Scope(), resource_limit=2)
        frame.enter()
        try:
            frame.create_exclusive(path)
            frame.sync_directory(self.root)
            frame.ready()
        finally:
            frame.leave()
        self.assertEqual(path.read_bytes(), b"")
        self.assertEqual(frame.creator.metrics["open_calls"], 1)
        self.assertEqual(frame.creator.metrics["close_calls"], 1)
        self.assertTrue(frame.close_step())
    def test_non_ascii_native_path(self):
        path=self.root / "native-\u00e9-\U0001f432"
        r=Resource("file")
        r.acquire(encoded(path),writable=True,exclusive=True)
        borrower=Borrow(r);borrower.acquire()
        os.write(borrower.fileno(),b"exact bytes");borrower.close()
        r.close();self.assertEqual(path.read_bytes(),b"exact bytes")
    def test_exclusive_existing_never_truncates(self):
        path = self.file("snapshot.sqlite3")
        r = Resource("file")
        with self.assertRaises(FileExistsError):
            r.acquire(encoded(path), writable=True, exclusive=True)
        self.assertEqual(path.read_bytes(), b"original bytes")
        self.assertEqual(r.state, "absent")
        r.close()
        self.assertTrue(r.received)
    def test_before_open_retry_has_no_first_effect(self):
        path = self.root / "new"
        r = Resource("file")
        r._test_fault("before_open")
        with self.assertRaises(RuntimeError):
            r.acquire(encoded(path), writable=True, exclusive=True)
        self.assertFalse(path.exists())
        self.assertEqual(r.state, "new")
        r.acquire(encoded(path), writable=True, exclusive=True)
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
    def test_unknown_scopes_keep_capacity_independent_work_advances(self):
        first, second = Scope(), Scope()
        a, b = Construction(first, resource_limit=2), Construction(second, resource_limit=2)
        first.held = first.unknown = True
        self.assertFalse(a.close_step())
        b.enter()
        try:
            b.create_exclusive(self.root / "independent")
            b.ready()
        finally:
            b.leave()
        self.assertTrue(b.close_step())
        self.assertFalse(a.received)
        self.assertEqual(first.calls, 0)
    def test_constructor_busy_is_not_worker_join(self):
        frame = Construction(Scope(), resource_limit=2)
        entered, finish = threading.Event(), threading.Event()
        def worker():
            frame.enter()
            entered.set()
            finish.wait(10)
            frame.leave()
        t = threading.Thread(target=worker)
        t.start()
        self.assertTrue(entered.wait(10))
        try:
            with self.assertRaises(BlockingIOError):
                frame.close_step()
        finally:
            finish.set()
            t.join(10)
        self.assertFalse(t.is_alive())
        self.assertTrue(frame.close_step())
    def unknown_child(self, fault):
        # Isolate intentionally unrecoverable resources; child exit is test
        # cleanup, never a positive in-process receiving receipt.
        code = '''
import os,sys,tempfile,pathlib
from retained_resource import Resource,Borrow
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
'''
        directory = self.root / fault
        directory.mkdir()
        child = subprocess.run([sys.executable, "-c", code, fault, str(directory)], capture_output=True, text=True)
        self.assertEqual(child.returncode, 0, child.stderr)
    def test_unknown_open_is_never_reopened(self):
        self.unknown_child("unknown_open")
    def test_unknown_close_before_effect_is_never_retried(self):
        self.unknown_child("unknown_close_open")
    def test_unknown_close_after_effect_is_never_retried(self):
        self.unknown_child("unknown_close_closed")
    @unittest.skipUnless(os.name == "posix", "directory backend is existing POSIX-only corpus profile")
    def test_directory_close_reply_loss(self):
        r = Resource("directory")
        r.acquire(os.fsencode(self.root), private_owned=True)
        fd = fd_of(r)
        r._test_fault("after_close")
        with self.assertRaises(RuntimeError):
            r.close()
        other = os.open(self.file(), os.O_RDONLY)
        if other != fd:
            os.dup2(other, fd); os.close(other)
        try:
            r.close(); os.fstat(fd)
        finally:
            os.close(fd)
    @unittest.skipUnless(os.name == "posix", "directory sync is not a Windows Native operation")
    def test_sync_failure_does_not_certify_constructor(self):
        frame = Construction(Scope(), resource_limit=2)
        frame.enter()
        try:
            frame.directory._test_fault("before_sync")
            with self.assertRaises(RuntimeError):
                frame.sync_directory(self.root)
        finally:
            frame.leave()
        self.assertEqual(frame.phase, "failed-retained")
        self.assertTrue(frame.close_step())
        self.assertNotEqual(frame.phase, "ready")
    @unittest.skipUnless(os.name == "posix", "POSIX current UID/private mode check")
    def test_wrong_private_mode_is_retained_rejected(self):
        p = self.file();p.chmod(0o644)
        r=Resource("file")
        with self.assertRaises(RuntimeError):r.acquire(os.fsencode(p), private_owned=True)
        self.assertEqual(r.state,"rejected");r.close();self.assertTrue(r.received)

if __name__ == "__main__":
    unittest.main()
