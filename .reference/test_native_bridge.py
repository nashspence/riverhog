"""Actual unchanged JournalStore/Session/Core code with the reference origin bridge."""
import os
import sqlite3
import subprocess
import sys
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from retained_resource import Borrow
from retained_resource.construction import Construction
from native_bridge import DedicatedSqliteScope, native_bridge, native_route, core_bridge
from riverhog_provenance.journal_store import JournalStore


class NativeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        if os.name == "posix":self.root.chmod(0o700)
    def tearDown(self):self.tmp.cleanup()
    def frame(self):
        return Construction(DedicatedSqliteScope(cursor_limit=128, verify_process=lambda: None), resource_limit=2)
    def open(self, frame, **kw):
        with native_route(frame):return JournalStore.open_retained(self.root, **kw)
    def test_create_readonly_preserves_actual_native_format(self):
        with native_bridge():
            a=self.frame();s=self.open(a,create=True);s.close();self.assertTrue(a.close_step())
            before=(self.root/'snapshot.sqlite3').read_bytes()
            b=self.frame();s=self.open(b,read_only=True)
            with self.assertRaisesRegex(sqlite3.OperationalError, 'readonly'):
                s.db.execute('CREATE TABLE forbidden_readonly_write(x INTEGER)')
            s.close();self.assertTrue(b.close_step())
            self.assertEqual((self.root/'snapshot.sqlite3').read_bytes(),before)
    def test_creator_before_close_refusal_retained_before_native_callback(self):
        with native_bridge():
            a=self.frame();a.creator._test_fault('before_close');handles=[]
            with self.assertRaises(RuntimeError):self.open(a,create=True,on_storage_open=handles.append)
            self.assertEqual(handles,[]);self.assertEqual(a.creator.state,'live')
            b=Borrow(a.creator);b.acquire();fd=b.fileno();b.close();os.fstat(fd)
            self.assertFalse(a.received);self.assertTrue(a.close_step())
            self.assertEqual(a.creator.metrics['close_calls'],1)
    def test_unreturned_factory_connection_remains_scoped(self):
        with native_bridge():
            a=self.frame();original=a.scope.connect
            def lose(*args,**kw):
                original(*args,**kw)
                raise InterruptedError('after factory acquisition, before return')
            a.scope.connect=lose
            with self.assertRaises(InterruptedError):self.open(a,create=True)
            self.assertEqual(a.scope.admission.status.connections,1)
            self.assertIsNone(a.storage)
            self.assertTrue(a.close_step())
    def test_unreturned_storage_constructor_keeps_original_connection(self):
        from riverhog_provenance import journal_store
        with native_bridge():
            a=self.frame()
            with patch.object(journal_store, '_SnapshotStorage', side_effect=MemoryError('storage bookkeeping interruption')):
                with self.assertRaises(MemoryError):self.open(a,create=True)
            self.assertIsNone(a.storage)
            self.assertEqual(a.scope.admission.status.connections,1)
            self.assertTrue(a.close_step())
    def test_schema_rejection_keeps_original_constructor(self):
        with native_bridge():
            a=self.frame();handles=[]
            with patch.object(JournalStore,'_initialize_schema',side_effect=InterruptedError('schema rejection')):
                with self.assertRaises(InterruptedError):self.open(a,create=True,on_storage_open=handles.append)
            self.assertEqual(len(handles),1);self.assertIs(a.storage.store,handles[0])
            self.assertTrue(a.close_step())
    def test_callback_interruption_cannot_hide_original_storage(self):
        with native_bridge():
            a=self.frame()
            def refuses(_):raise InterruptedError('callback rejected after first retention')
            with self.assertRaises(InterruptedError):self.open(a,create=True,on_storage_open=refuses)
            self.assertIsNotNone(a.storage);self.assertTrue(a.close_step())
    def test_final_reader_controls_connection_receiving(self):
        with native_bridge():
            a=self.frame();s=self.open(a,create=True);s.freeze();reader=s.borrow()
            self.assertFalse(a.close_step())
            self.assertEqual(a.scope.admission.status.connections,1)
            reader.close();self.assertTrue(a.close_step())
    def test_exclusive_existing_bytes_preserved(self):
        with native_bridge():
            p=self.root/'snapshot.sqlite3';p.write_bytes(b'preexisting confirmed bytes')
            a=self.frame()
            with self.assertRaises(FileExistsError):self.open(a,create=True)
            self.assertEqual(p.read_bytes(),b'preexisting confirmed bytes')
            self.assertTrue(a.close_step())
    def test_actual_core_slot_receives_before_parent_release(self):
        if os.name != 'posix':self.skipTest('Core workspace lease is POSIX; Windows JournalStore tests above remain required')
        from tests.unit.test_journal_capture import retained
        from tests.unit.test_journal_summary_consumers import source
        from tests.unit.test_staged_journal_range import JOURNAL
        from tests.unit.test_upload_journal_certificates import ready
        from riverhog_core.services import collection_uploads as owner_source
        from riverhog_core.services.upload_journal_validation_owner import UploadJournalValidationOwner
        frames=[]
        def factory():
            frame=self.frame();frames.append(frame);frame.creator._test_fault('before_close');return frame
        with retained(self.root,source(journal_id=JOURNAL)) as (_,db):
            work,_=ready(db);owner=UploadJournalValidationOwner(db,self.root/'native')
            with core_bridge(factory):
                try:
                    with self.assertRaises(BlockingIOError):owner.advance(work,catalog=owner_source.admission_provenance_catalog())
                    self.assertEqual(len(frames),1)
                    self.assertTrue(frames[0].received)
                    self.assertEqual(frames[0].creator.metrics['close_calls'],1)
                    self.assertEqual(owner.active_keys(),())
                finally:owner.close()

    def test_actual_core_unknown_keeps_slot_and_lease(self):
        if os.name != "posix":
            self.skipTest("Core lease is POSIX; Windows Native constructor tests still execute")
        code = r"""
import os,sys
from pathlib import Path
from native_bridge import DedicatedSqliteScope,core_bridge,native_bridge,native_route
from retained_resource.construction import Construction
from riverhog_provenance.journal_store import JournalStore
from tests.unit.test_journal_capture import retained
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_staged_journal_range import JOURNAL
from tests.unit.test_upload_journal_certificates import ready
from riverhog_core.services import collection_uploads as original
from riverhog_core.services.upload_journal_validation_owner import UploadJournalValidationOwner
from riverhog_core.journal_validation_workspace import lease_journal_validation_workspace
root=Path(sys.argv[1]);frames=[]
def factory():
 f=Construction(DedicatedSqliteScope(cursor_limit=128,verify_process=lambda:None),resource_limit=2)
 f.creator._test_fault('unknown_close_open');frames.append(f);return f
with retained(root,source(journal_id=JOURNAL)) as (_,db):
 work,_=ready(db);owner=UploadJournalValidationOwner(db,root/'native')
 with core_bridge(factory):
  try:owner.advance(work,catalog=original.admission_provenance_catalog())
  except (RuntimeError,OSError):pass
  else:raise AssertionError('unknown construction returned success')
  assert len(frames)==1 and not frames[0].received and frames[0].creator.state=='unknown'
  assert owner.active_keys()
  files=list((root/'native').glob('*/snapshot.sqlite3'));assert len(files)==1
  with lease_journal_validation_workspace(root/'native',files[0].parent.name) as claimed:
   assert claimed is None,'unknown Native creator released its parent lease'
  for _ in range(3):
   try:owner.close()
   except RuntimeError:pass
   else:raise AssertionError('unknown resource was force-received')
  assert owner.active_keys() and not frames[0].received
  independent=root/'other';independent.mkdir(mode=0o700)
  f=Construction(DedicatedSqliteScope(cursor_limit=128,verify_process=lambda:None),resource_limit=2)
  with native_route(f):store=JournalStore.open_retained(independent,create=True)
  store.close();assert f.close_step()
  assert not frames[0].received
  print('ACTUAL_CORE_UNKNOWN_RETAINED; INDEPENDENT_NATIVE_RECEIVED',flush=True)
  # Only fixture process cleanup: never a product receiving acknowledgement.
  os._exit(0)
"""
        run = subprocess.run([sys.executable,"-c",code,str(self.root)],capture_output=True,text=True)
        self.assertEqual(run.returncode,0,run.stdout+run.stderr)
        self.assertIn("ACTUAL_CORE_UNKNOWN_RETAINED",run.stdout)

if __name__=='__main__':unittest.main()
