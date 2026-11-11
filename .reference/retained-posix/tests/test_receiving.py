"""Actual descriptor witnesses. Identity probes here are test oracles, never receiving."""
import errno
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import unittest

from retained_posix import Borrow, Group, Resource
from retained_posix.extent import ExtentTurn


class Receiving(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.path = self.root / 'data'
        self.path.write_bytes(b'abcdefghijk')
        self.path.chmod(0o600)

    def tearDown(self):
        self.temp.cleanup()

    def open(self, kind='file'):
        r=Resource(kind)
        r.acquire(os.fsencode(self.path if kind=='file' else self.root))
        return r

    def fd(self, r):
        b=Borrow(r); b.acquire()
        try: return b.fileno()
        finally: b.close()

    def test_constructor_has_no_effect_and_new_receives(self):
        r=Resource('file')
        self.assertEqual(r.metrics['open_calls'],0)
        self.assertFalse(r.received)
        r.close(); r.close()
        self.assertTrue(r.received)
        self.assertEqual(r.metrics['close_calls'],0)

    def test_missing_open_is_definite_no_resource(self):
        r=Resource('file')
        with self.assertRaises(FileNotFoundError): r.acquire(os.fsencode(self.root/'absent'))
        self.assertEqual(r.state,'absent')
        r.close(); self.assertTrue(r.received)
        self.assertEqual(r.metrics['close_calls'],0)

    def test_before_open_refusal_has_no_effect(self):
        for kind in ('file','directory'):
            r=Resource(kind); r._test_fault('before_open')
            with self.assertRaises(RuntimeError): r.acquire(os.fsencode(self.path if kind=='file' else self.root))
            self.assertEqual((r.state,r.metrics['open_calls']),('new',0))
            r.close(); self.assertTrue(r.received)

    def test_lost_acquisition_reply_retains_exact_owner(self):
        for kind in ('file','directory'):
            r=Resource(kind); r._test_fault('after_open')
            with self.assertRaises(RuntimeError): r.acquire(os.fsencode(self.path if kind=='file' else self.root))
            fd=self.fd(r)
            self.assertEqual(r.state,'live')
            os.fstat(fd)
            with self.assertRaises(RuntimeError): r.acquire(os.fsencode(self.path))
            r.close(); self.assertTrue(r.received)
            self.assertEqual(r.metrics['open_calls'],1)

    def test_kind_failure_retains_descriptor_until_close(self):
        r=Resource('file')
        with self.assertRaises(RuntimeError): r.acquire(os.fsencode(self.root))
        self.assertEqual(r.state,'rejected')
        r.close(); self.assertEqual(r.metrics['close_calls'],1)

    def test_post_open_metadata_failure_retains_descriptor(self):
        r=Resource('file'); r._test_fault('stat_refusal')
        with self.assertRaises(OSError): r.acquire(os.fsencode(self.path))
        self.assertEqual(r.state,'rejected')
        r.close(); self.assertEqual(r.metrics['close_calls'],1)

    def test_nofollow_and_cloexec(self):
        r=Resource('file'); link=self.root/'link'; link.symlink_to(self.path)
        with self.assertRaises(OSError): r.acquire(os.fsencode(link))
        r.close()
        r=self.open(); self.assertFalse(os.get_inheritable(self.fd(r))); r.close()

    def test_before_close_refusal_keeps_file_and_directory_open(self):
        for kind in ('file','directory'):
            r=self.open(kind); fd=self.fd(r); r._test_fault('before_close')
            with self.assertRaises(RuntimeError): r.close()
            self.assertFalse(r.received); self.assertEqual(r.state,'live')
            os.fstat(fd)
            self.assertEqual(r.metrics['close_calls'],0)
            r.close(); self.assertTrue(r.received)
            self.assertEqual(r.metrics['close_calls'],1)

    def reuse(self, old, path):
        fresh=os.open(path,os.O_RDONLY|os.O_CLOEXEC)
        if fresh!=old:
            os.dup2(fresh,old,inheritable=False); os.close(fresh)
        return old

    def test_after_close_lost_reply_reuse_unrelated_file(self):
        other=self.root/'unrelated'; other.write_bytes(b'original unrelated contents')
        for kind in ('file','directory'):
            r=self.open(kind); old=self.fd(r); r._test_fault('after_close')
            with self.assertRaises(RuntimeError): r.close()
            self.assertTrue(r.received)
            new=self.reuse(old,other)
            try:
                for _ in range(32): r.close()
                self.assertEqual(os.read(new,100),other.read_bytes())
                self.assertEqual(r.metrics['close_calls'],1)
            finally: os.close(new)

    def test_after_close_reply_loss_reopen_same_inode(self):
        for kind in ('file','directory'):
            path=self.path if kind=='file' else self.root
            r=self.open(kind); old=self.fd(r); first=os.fstat(old)
            r._test_fault('after_close')
            with self.assertRaises(RuntimeError): r.close()
            fd=self.reuse(old,path)
            try:
                self.assertEqual((first.st_dev,first.st_ino),(os.fstat(fd).st_dev,os.fstat(fd).st_ino))
                r.close(); os.fstat(fd)
            finally: os.close(fd)

    def test_unknown_close_even_when_actually_closed_never_retries(self):
        r=self.open(); old=self.fd(r); r._test_fault('unknown_close_closed')
        with self.assertRaises(InterruptedError): r.close()
        self.assertEqual(r.state,'unknown'); self.assertFalse(r.received)
        new=self.reuse(old,self.path)
        try:
            for _ in range(4):
                with self.assertRaises(RuntimeError): r.close()
            os.fstat(new); self.assertEqual(r.metrics['close_calls'],1)
        finally: os.close(new)

    def test_genuinely_unknown_open_and_close_stay_charged_in_child(self):
        code=r'''
import json,os,sys
from retained_posix import Resource,Borrow
root=Resource('directory'); root.acquire(os.fsencode(sys.argv[1]))
r=Resource('file')
if sys.argv[2]=='acquire':
 r._test_fault('unknown_open')
 try:r.acquire(b'data',parent=root)
 except RuntimeError:pass
else:
 r.acquire(b'data',parent=root)
 r._test_fault('unknown_close_open')
 try:r.close()
 except OSError:pass
assert r.state=='unknown' and not r.received
for _ in range(5):
 try:r.close()
 except RuntimeError:pass
 else:raise AssertionError('false receiving')
try:root.close()
except BlockingIOError:pass
else:raise AssertionError('lost parent')
assert root.metrics['children']==1
print(json.dumps({'unknown':True,'charged_parent':True,'close_calls':r.metrics['close_calls']}))
# Exit only ends this test process; it is not a product receiving receipt.
'''
        for phase in ('acquire','close'):
            run=subprocess.run([sys.executable,'-c',code,str(self.root),phase],capture_output=True,text=True,check=True)
            self.assertEqual(json.loads(run.stdout)['close_calls'],0)

    def test_external_close_is_contract_breach_not_receiving(self):
        r=self.open(); fd=self.fd(r); os.close(fd)
        with self.assertRaises(OSError) as c: r.close()
        self.assertEqual(c.exception.errno,errno.EBADF)
        self.assertFalse(r.received); self.assertEqual(r.state,'unknown')
        # This deliberate breach has no surviving fd, but does not manufacture proof.

    def test_signal_after_open_and_close_has_committed_native_state(self):
        for kind in ('file','directory'):
            r=Resource(kind); r._test_fault('signal_after_open')
            with self.assertRaises(KeyboardInterrupt):
                r.acquire(os.fsencode(self.path if kind=='file' else self.root))
            self.assertEqual(r.state,'live')
            old=self.fd(r); r._test_fault('signal_after_close')
            with self.assertRaises(KeyboardInterrupt): r.close()
            self.assertTrue(r.received)
            fd=self.reuse(old,self.path)
            try:r.close(); os.fstat(fd)
            finally:os.close(fd)

    def test_child_dependency_prevents_parent_close(self):
        p=self.open('directory'); c=Resource('file'); c.acquire(b'data',parent=p)
        with self.assertRaises(BlockingIOError): p.close()
        c._test_fault('before_close')
        with self.assertRaises(RuntimeError): c.close()
        with self.assertRaises(BlockingIOError): p.close()
        c.close(); p.close(); self.assertTrue(p.received)

    def test_borrow_retention_and_actual_worker_join_are_separate(self):
        r=self.open(); b=Borrow(r); b.acquire()
        started=threading.Event(); exit_allowed=threading.Event()
        def work():
            os.fstat(b.fileno()); started.set(); exit_allowed.wait(5); os.fstat(b.fileno())
        worker=threading.Thread(target=work); worker.start(); self.assertTrue(started.wait(5))
        with self.assertRaises(BlockingIOError): r.close()
        independent=self.open('directory'); independent.close(); self.assertTrue(independent.received)
        exit_allowed.set(); worker.join(5); self.assertFalse(worker.is_alive())
        with self.assertRaises(BlockingIOError): r.close()  # join alone did not receive b
        b.close(); b.close(); r.close(); self.assertTrue(r.received)

    def test_group_does_not_receive_an_active_borrow(self):
        g=Group(2); r=g.resource('file'); r.acquire(os.fsencode(self.path))
        b=g.borrow(r); b.acquire()
        with self.assertRaises(BlockingIOError): g.close_step(2)
        self.assertEqual(g.retained,2); os.fstat(b.fileno())
        b.close(); self.assertTrue(g.close_step(2))

    def test_duplicate_uses_original_open_resource_not_name(self):
        r=self.open(); copy=Resource('file'); copy._test_fault('after_open')
        with self.assertRaises(RuntimeError): copy.duplicate(r)
        old_stat=os.fstat(self.fd(r)); self.path.unlink(); self.path.write_bytes(b'replacement')
        r.close(); fd=self.fd(copy)
        self.assertEqual((os.fstat(fd).st_dev,os.fstat(fd).st_ino),(old_stat.st_dev,old_stat.st_ino))
        copy.close(); self.assertEqual(copy.metrics['dup_calls'],1)

    def test_duplicate_retains_original_parent_dependency(self):
        parent=self.open('directory'); source=Resource('file'); source.acquire(b'data',parent=parent)
        copy=Resource('file'); copy.duplicate(source); source.close()
        with self.assertRaises(BlockingIOError): parent.close()
        copy.close(); parent.close(); self.assertTrue(parent.received)

    def test_group_retains_uncertain_child_and_all_parent_custody(self):
        g=Group(2); p=g.resource('directory'); p.acquire(os.fsencode(self.root))
        c=g.resource('file'); c.acquire(b'data',parent=p); c._test_fault('before_close')
        with self.assertRaises(RuntimeError): g.close_step(2)
        self.assertEqual(g.retained,2); self.assertFalse(p.received)
        self.assertTrue(g.close_step(2)); self.assertTrue(p.received)

    def test_bounded_close_steps_and_admission(self):
        g=Group(17)
        resources=[g.resource('file') for _ in range(17)]
        for r in resources:r.acquire(os.fsencode(self.path))
        with self.assertRaises(RuntimeError):g.resource('file')
        for count in (13,9,5,1):
            self.assertFalse(g.close_step(4)); self.assertEqual(g.retained,count)
        self.assertTrue(g.close_step(4))
        self.assertEqual(sum(r.metrics['close_calls'] for r in resources),17)

    def test_extent_data_close_refusal_retains_owner_after_advance_raises(self):
        t=ExtentTurn(self.root,names=('data',),octet_budget=3)
        t.file_owner._test_fault('before_close')
        with self.assertRaises(RuntimeError):t.advance()
        self.assertEqual(self.path.read_bytes(),b'abcdefgh')
        self.assertEqual(t.file_owner.state,'live'); self.assertFalse(t.received)
        self.assertFalse(t.dir_owner.received)
        os.fstat(self.fd(t.file_owner))
        self.assertTrue(t.close()); self.assertTrue(t.received)
        self.assertEqual(t.file_owner.metrics['close_calls'],1)
        self.assertEqual(self.path.read_bytes(),b'abcdefgh') # close did not repeat truncate

    def test_extent_directory_reply_loss_preserves_unrelated_reused_fd(self):
        t=ExtentTurn(self.root,names=('data',),octet_budget=3)
        t.dir_owner._test_fault('after_close')
        # Native fd is obtained with a transient test borrow before directory close.
        original=t._files; observed=[]
        def files(fd): observed.append(fd); return original(fd)
        t._files=files
        with self.assertRaises(RuntimeError):t.advance()
        self.assertTrue(t.dir_owner.received); self.assertFalse(t.received)
        fd=self.reuse(observed[0],self.path)
        try:
            self.assertTrue(t.close()); os.fstat(fd)
            self.assertEqual(t.dir_owner.metrics['close_calls'],1)
        finally:os.close(fd)

    def test_extent_receiving_does_not_claim_sync_success(self):
        from unittest.mock import patch
        t=ExtentTurn(self.root,names=('data',),octet_budget=3)
        with patch('os.fsync',side_effect=OSError(errno.EIO,'injected sync refusal')):
            with self.assertRaises(OSError):t.advance()
        self.assertTrue(t.received); self.assertIsNone(t.progress)
        self.assertEqual(self.path.stat().st_size,8)

    def test_extent_exact_names_budget_and_unclassified_file(self):
        unknown=self.root/'.lease'; unknown.write_bytes(b'parent'); unknown.chmod(0o600)
        t=ExtentTurn(self.root,names=('data',),octet_budget=11)
        result=t.advance(); self.assertTrue(t.received)
        self.assertEqual((result.removed_octets,result.removed_files,result.remaining_files),(11,1,0))
        self.assertEqual(unknown.read_bytes(),b'parent')

    def test_source_owned_native_growth_independent_of_extents_and_retries(self):
        for size in (0,17,65537,8*1024*1024):
            with self.path.open('r+b') as f:f.truncate(size)
            for kind in ('file','directory'):
                r=self.open(kind); before=r.metrics
                for _ in range(129):self.assertFalse(r.received); self.assertEqual(r.state,'live')
                r.close()
                for _ in range(129):r.close()
                m=r.metrics
                self.assertEqual((m['open_calls'],m['fstat_calls'],m['close_calls']),(1,1,1))
                self.assertEqual(m['owner_storage_bytes'],before['owner_storage_bytes'])


if __name__=='__main__': unittest.main(verbosity=2)
