"""Reference-only resource tests, including real SQLite VFS refusal on Linux.

Run from the repository root: python .reference/probe.py
Uses the audited repository's *test* VFS delegate. This is not native Windows,
locked-product qualification, a complete resource wrapper or a power-loss test.
"""
from __future__ import annotations
import ctypes as c
import importlib.util
import json
import platform
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
import _sqlite3
from envelope import Exhausted, Gate, I64_MAX, Limits, control_bound, payload_may_commit

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("reference_vfs_fixture", ROOT / "tests/support/sqlite_vfs_faults.py")
fixture = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = fixture
spec.loader.exec_module(fixture)
OBSERVATIONS = []

class ProbeVfs(fixture.FaultVfs):
    """Only xWrite policing is prototyped. Other production interception is specified."""
    def __init__(self, lib, base):
        self.gate = self.recovery = None
        self.refusals = []
        self.recovering = False
        super().__init__(lib, base)

    def _file_callback(self, name):
        original = super()._file_callback(name)
        if name != "write":
            return original
        def write(file, body, amount, offset):
            kind = self.files[c.cast(file, c.c_void_p).value]
            gate = self.recovery if self.recovering else self.gate
            if gate is not None:
                try:
                    gate.write(kind, amount, offset)
                except Exhausted as exc:
                    self.refusals.append((kind, amount, offset, str(exc)))
                    # Arm reserved recovery before SQLite sees the first error:
                    # rollback may be internal to this very sqlite3_step/COMMIT.
                    self.recovering = True
                    return sqlite3.SQLITE_FULL
            return original(file, body, amount, offset)
        return write

    def arm_limits(self, *, pages=128, journal=1024*1024, database=4*1024*1024):
        self.gate = Gate(Limits(4096, pages, database, journal, 16*1024*1024, 10000))
        self.recovery = Gate(Limits(4096, 1024, database, 1024*1024, 16*1024*1024, 10000))
        self.recovering = False
        self.refusals.clear()


def native_library():
    if sys.platform != "linux":
        raise RuntimeError("this reference probe intentionally requires Linux")
    lib = c.CDLL(_sqlite3.__file__)
    lib.sqlite3_sourceid.restype = c.c_char_p
    with sqlite3.connect(":memory:") as conn:
        identity = conn.execute("SELECT sqlite_source_id()").fetchone()[0]
    if lib.sqlite3_sourceid().decode() != identity:
        raise RuntimeError("different SQLite engine")
    lib.sqlite3_vfs_find.argtypes = [c.c_char_p]
    lib.sqlite3_vfs_find.restype = fixture._PV
    lib.sqlite3_vfs_register.argtypes = [fixture._PV, c.c_int]
    lib.sqlite3_vfs_register.restype = c.c_int
    lib.sqlite3_vfs_unregister.argtypes = [fixture._PV]
    lib.sqlite3_vfs_unregister.restype = c.c_int
    return lib, identity


def setup_connection(path, shim):
    db = sqlite3.connect(path.as_uri() + "?vfs=" + shim.name, uri=True, isolation_level=None)
    for key, value in (("page_size",4096),("auto_vacuum",2),("journal_mode","PERSIST"),
                       ("synchronous","EXTRA"),("cache_spill",0),("mmap_size",0),
                       ("temp_store",2),("journal_size_limit",1048576)):
        db.execute(f"PRAGMA {key}={value}").close()
    db.executescript("CREATE TABLE controls(id INTEGER PRIMARY KEY, body BLOB NOT NULL);"
                     "CREATE TABLE cells(id INTEGER PRIMARY KEY, k TEXT UNIQUE, body BLOB);"
                     "INSERT INTO controls VALUES(1,zeroblob(262153));")
    return db


class Arithmetic(unittest.TestCase):
    def test_control_bound_increases_and_accounts_sector_peers(self):
        small = control_bound((8192+9,16))
        big = control_bound((262144+9,16))
        wide = control_bound((262144+9,16),sector=8192)
        self.assertGreater(big[0], small[0]); self.assertEqual(wide[0],2*big[0])
        OBSERVATIONS.append({"fixed_control_8KiB":small,"fixed_control_256KiB":big,"wide_sector":wide})

    def test_prefix_gate_counts_crossings_rewrites_and_refuses_atomically(self):
        gate=Gate(Limits(4096,2,16384,10000,20000,20))
        gate.write("database",2,4095)
        gate.write("database",1,0)
        before=(set(gate.pages),gate.writes,gate.calls)
        with self.assertRaises(Exhausted):gate.write("database",1,8192)
        self.assertEqual(before,(gate.pages,gate.writes,gate.calls))
        self.assertEqual(len(gate.pages),2)

    def test_extent_overflow_and_other_files_fail_before_counting(self):
        gate=Gate(Limits(4096,2,16384,10000,20000,20))
        for kind,n,offset in (("journal",1,10000),("database",1,16384),("other",1,0),
                               ("journal",2,I64_MAX),("database",True,0)):
            with self.assertRaises(Exhausted):gate.write(kind,n,offset)
        self.assertEqual(gate.calls,0)

    def test_repeated_bytes_and_calls_are_independent_limits(self):
        for limit,expected in ((Limits(4096,2,16384,10000,3,20),3),
                               (Limits(4096,2,16384,10000,20000,2),2)):
            gate=Gate(limit)
            for _ in range(expected):gate.write("database",1,0)
            with self.assertRaises(Exhausted):gate.write("database",1,0)

    def test_database_reserve_cannot_be_borrowed_by_payload(self):
        self.assertTrue(payload_may_commit(backed_pages=100,end_pages=100,free_pages=10,reserve_floor=10))
        self.assertFalse(payload_may_commit(backed_pages=100,end_pages=100,free_pages=9,reserve_floor=10))
        self.assertFalse(payload_may_commit(backed_pages=100,end_pages=101,free_pages=10,reserve_floor=10))


class RealSqlite(unittest.TestCase):
    def scenario(self, *, pages=128, journal=1048576, operation="control"):
        lib,_=native_library()
        with tempfile.TemporaryDirectory() as directory, ProbeVfs(lib,lib.sqlite3_vfs_find(b"unix")) as shim:
            db=setup_connection(Path(directory)/"probe.db",shim)
            original=db.execute("SELECT body FROM controls").fetchone()[0]
            # Bootstrap deliberately predates the tested quota. Production cannot bypass it.
            shim.arm_limits(pages=pages,journal=journal)
            error=None
            try:
                db.execute("BEGIN IMMEDIATE")
                if operation=="control":
                    with db.blobopen("controls","body",1) as blob:blob.write(b"x"*262153)
                else:
                    for i in range(64):db.execute("INSERT INTO cells VALUES(?,?,?)",(i,f"key-{i}",b"a"*4096))
                db.execute("COMMIT")
            except sqlite3.Error as exc:
                error=getattr(exc,"sqlite_errorcode",None)
                if db.in_transaction:db.rollback()
            record={"scenario":operation,"page_cap":pages,"journal_cap":journal,"error":error,
                    "forward_pages":len(shim.gate.pages),"forward_journal_peak":shim.gate.journal_peak,
                    "refusals":shim.refusals,"rollback_pages":len(shim.recovery.pages)}
            OBSERVATIONS.append(record)
            self.assertLessEqual(len(shim.gate.pages),pages)
            self.assertLessEqual(shim.gate.journal_peak,journal)
            if error is not None:
                self.assertEqual(db.execute("SELECT body FROM controls").fetchone()[0],original)
                self.assertEqual(db.execute("SELECT count(*) FROM cells").fetchone()[0],0)
            self.assertEqual(db.execute("PRAGMA integrity_check").fetchone()[0],"ok")
            db.close()
            return record

    def test_successful_fixed_control_below_derived_envelope(self):
        q,j=control_bound((262153,16))
        r=self.scenario(pages=q,journal=j)
        self.assertIsNone(r["error"])

    def test_database_write_refusal_rolls_back_after_partial_commit(self):
        r=self.scenario(pages=2)
        self.assertEqual(r["error"],sqlite3.SQLITE_FULL)
        self.assertEqual(r["forward_pages"],2)
        self.assertGreater(r["rollback_pages"],0)

    def test_journal_extent_refusal_preserves_original(self):
        r=self.scenario(journal=8192)
        self.assertEqual(r["error"],sqlite3.SQLITE_FULL)
        self.assertEqual(r["forward_pages"],0)

    def test_structural_many_row_probe_is_rejected_not_inferred_bounded(self):
        r=self.scenario(pages=3,operation="structural")
        self.assertEqual(r["error"],sqlite3.SQLITE_FULL)

    def test_application_owned_sqlite_heap_cap_rejects_large_allocation(self):
        code='''import sqlite3,json
c=sqlite3.connect(":memory:")
c.execute("create table t(x)");c.execute("insert into t values(7)");c.commit()
limit=c.execute("PRAGMA hard_heap_limit=1048576").fetchone()[0]
try:
 c.execute("SELECT randomblob(16777216)").fetchone()
except MemoryError:
 print(json.dumps({"hard_heap_bytes":limit,"rejected":True,"original":c.execute("select x from t").fetchone()[0]}))
else:
 raise SystemExit("missing hard-heap refusal")
'''
        result=subprocess.run([sys.executable,"-c",code],capture_output=True,text=True,check=True)
        evidence=json.loads(result.stdout)
        self.assertTrue(evidence["rejected"]);self.assertEqual(evidence["original"],7)
        OBSERVATIONS.append(evidence)

if __name__=="__main__":
    start=time.perf_counter()
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__]))
    _,sourceid=native_library()
    report={"scope":"reference arithmetic and Linux actual-SQLite xWrite refusal only; not production/Windows qualification",
            "python":platform.python_version(),"platform":platform.platform(),"sqlite":sqlite3.sqlite_version,
            "sqlite_source_id":sourceid,"audited_source":"f41bae6e1456d4ae16952de293320cbe4a6ce460",
            "tests":result.testsRun,"failures":len(result.failures),"errors":len(result.errors),
            "elapsed_seconds":time.perf_counter()-start,"observations":OBSERVATIONS}
    Path(__file__).with_name("PROBE_RESULT.json").write_text(json.dumps(report,indent=2)+"\n")
    raise SystemExit(not result.wasSuccessful())
