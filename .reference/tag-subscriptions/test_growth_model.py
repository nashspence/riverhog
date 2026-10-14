"""Actual reference-body counts, saturation and mutation-sensitive growth checks."""
from __future__ import annotations

import hashlib
import io
import itertools
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
for package in ("riverhog-growth-contracts", "riverhog-canonical-json"):
    sys.path.insert(0, str(ROOT / "packages" / package / "src"))

from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from growth_model import CapacityPressure, Measure, OperationalModel, bounded_bytes
from model import AuthorityError, ModelStore, StaleProposal
from test_model import SOURCE, VIEW, OTHER_VIEW, upsert, departure

SAMPLES: list[dict[str, object]] = []


def record(operation, dimensions, observed, case):
    binding = growth_binding(operation)
    projection = project_bindings((binding,))
    declaration = projection["declarations"][binding.declaration.id]
    sample = {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(canonical_json_bytes(declaration)).hexdigest(),
        "dimensions": dimensions, "metrics": observed.as_dict(), "delegates": {},
    }
    validate_execution_samples(projection, binding.declaration.id, [sample])
    SAMPLES.append({"case": case, "sample": sample})
    return projection, sample


class QueryGrowthTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.store = OperationalModel(self.db)

    def tearDown(self):
        self.db.close()

    def seed(self, n, ready=()):
        self.db.executemany("INSERT INTO work(id,state,due,turn) VALUES(?,'complete',0,?)",
                            ((i,i) for i in range(1,n+1)))
        self.db.executemany("UPDATE work SET state='ready' WHERE id=?", ((i,) for i in ready))
        self.db.commit()

    def test_sparse_due_lookup_as_completed_root_grows(self):
        for n in (17,257,4097,65537):
            with self.subTest(n=n):
                self.db.execute("DELETE FROM work")
                self.seed(n, (n,))
                with Measure(self.db) as observed:
                    rows = self.store.select_due(1, limit=7)
                self.assertEqual([r['id'] for r in rows], [n])
                record(OperationalModel.select_due, {"rows": n,"limit":7}, observed,
                       f"sparse-due-{n}")

    def test_no_due_result_does_not_scan_completed_rows(self):
        self.seed(65537)
        with Measure(self.db) as observed:
            self.assertEqual(self.store.select_due(1,limit=1), [])
        record(OperationalModel.select_due, {"rows":65537,"limit":1}, observed,"idle-root")
        self.assertLess(observed.vm_instructions, 100)

    def test_future_due_queue_uses_ordered_index(self):
        self.seed(4097, range(1,4098))
        self.db.execute("UPDATE work SET due=1000");self.db.commit()
        with Measure(self.db) as observed:
            self.assertEqual(self.store.select_due(10,limit=7), [])
        record(OperationalModel.select_due, {"rows":4097,"limit":7}, observed,"future-only")

    def test_dropping_index_is_detected_by_growth_contract(self):
        self.seed(65537,(65537,))
        self.db.execute("DROP INDEX work_due");self.db.commit()
        with Measure(self.db) as observed:
            self.store.select_due(1,limit=7)
        with self.assertRaisesRegex(ValueError,"violate"):
            record(OperationalModel.select_due,{"rows":65537,"limit":7},observed,"must-fail")

    def test_deep_keyset_page_does_not_revisit_prefix(self):
        self.seed(65537)
        with Measure(self.db) as observed:
            rows=self.store.select_page(65000,65537,limit=7)
        self.assertEqual([r['id'] for r in rows],list(range(65001,65008)))
        record(OperationalModel.select_page,{"rows":65537,"limit":7},observed,"deep-keyset")

    def test_offset_mutation_exceeds_indexed_page_contract(self):
        self.seed(65537)
        with Measure(self.db) as observed:
            self.db.execute("SELECT id,state,due FROM work ORDER BY id LIMIT 7 OFFSET 65000").fetchall()
        with self.assertRaisesRegex(ValueError,"violate"):
            record(OperationalModel.select_page,{"rows":65537,"limit":7},observed,"must-fail")

    def test_full_scan_has_explicit_linear_total_and_fixed_horizon(self):
        self.seed(1001)
        after,seen,decoded=0,[],0
        while True:
            with Measure(self.db) as observed:
                rows=self.store.select_page(after,1001,limit=37)
            record(OperationalModel.select_page,{"rows":1001,"limit":37},observed,
                   f"full-traversal-after-{after}")
            decoded+=observed.returned_rows
            if not rows:break
            seen.extend(row['id'] for row in rows);after=rows[-1]['id']
        self.assertEqual(seen,list(range(1,1002)))
        self.assertEqual(decoded,1001)

    def test_payload_column_is_never_read_by_due_or_status(self):
        self.seed(4097,(4097,))
        def authorize(action, table, column, *_):
            if action==sqlite3.SQLITE_READ and table=='work' and column=='payload':
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK
        self.db.set_authorizer(authorize)
        try:
            self.assertEqual(self.store.select_due(1,limit=1)[0]['id'],4097)
            with Measure(self.db) as observed:self.store.status()
            self.assertEqual(observed.returned_rows,1)
            self.assertEqual(observed.statements,1)
        finally:self.db.set_authorizer(None)

    def test_bad_carrier_rejected_before_database_access(self):
        for bad in (0,101,True,1.5):
            with Measure(self.db) as observed, self.assertRaises(ValueError):
                self.store.select_due(1,limit=bad)
            self.assertEqual(observed.statements,0)


class LifecycleGrowthTests(unittest.TestCase):
    def setUp(self):
        self.db=sqlite3.connect(':memory:');self.store=OperationalModel(self.db)
    def tearDown(self):self.db.close()

    def test_capacity_pressure_rolls_back_whole_cursor_acceptance(self):
        self.store.admit(0,[1,2],capacity=2)
        before=self.store.status()
        with self.assertRaises(CapacityPressure):self.store.admit(1,[3,4],capacity=2)
        self.assertEqual(self.store.status(),before)
        self.assertEqual([r['id'] for r in self.store.select_due(1)], [1,2])
        self.store.finish_work(1)
        self.store.admit(1,[3],capacity=2)
        self.assertEqual(self.store.status()['pending'],2)
        self.assertEqual(self.store.status()['serial'],2)

    def test_duplicate_obligation_does_not_consume_capacity(self):
        self.store.admit(0,[1],capacity=1)
        self.store.admit(1,[1],capacity=1)
        self.assertEqual(self.store.status()['pending'],1)

    def test_one_failed_work_item_does_not_starve_next(self):
        self.store.admit(0,[1,2,3],capacity=3)
        self.store.defer(1,now=1,delay=10)
        self.assertEqual([r['id'] for r in self.store.select_due(1)], [2,3])
        self.store.finish_work(2);self.store.finish_work(3)
        self.assertEqual(self.store.select_due(10),[])
        self.assertEqual(self.store.select_due(11)[0]['id'],1)

    def test_parent_fanout_is_not_hidden_behind_one_delete(self):
        self.db.execute("INSERT INTO owners VALUES(1)")
        self.db.executemany("INSERT INTO garbage VALUES(1,?)",((i,) for i in range(4097)))
        self.db.commit()
        with Measure(self.db) as observed:
            self.assertEqual(self.store.prune_children(1,limit=1),1)
        self.assertEqual(observed.mutated_rows,1)
        record(OperationalModel.prune_children,{"rows":4097,"limit":1},observed,"bounded-high-fanout-cleanup")
        self.assertEqual(observed.returned_rows,1)
        self.assertIsNotNone(self.db.execute("SELECT 1 FROM owners WHERE id=1").fetchone())
        with self.assertRaises(sqlite3.IntegrityError):self.db.execute("DELETE FROM owners WHERE id=1")
        self.db.rollback()

    def test_cleanup_restarts_and_charges_parent_to_same_budget(self):
        self.db.execute("INSERT INTO owners VALUES(1)")
        self.db.executemany("INSERT INTO garbage VALUES(1,?)",((i,) for i in range(103)))
        self.db.commit();deleted=0
        while True:
            with Measure(self.db) as observed:count=self.store.prune_children(1,limit=7)
            self.assertLessEqual(count,7);self.assertEqual(observed.mutated_rows,count)
            record(OperationalModel.prune_children,{"rows":max(0,103-deleted),"limit":7},
                   observed,f"child-cleanup-after-{deleted}")
            deleted+=count
            if not count:break
        self.assertEqual(deleted,104)

    def test_retention_caps_actual_count_and_summary_octets(self):
        for i in range(1001):self.store.retain_run(i,i,b'x'*1024,keep=17)
        self.assertEqual(self.store.status()['history_count'],17)
        count,octets=self.db.execute("SELECT COUNT(*),SUM(length(summary)) FROM runs").fetchone()
        self.assertEqual((count,octets),(17,17*1024))
        with self.assertRaises(ValueError):self.store.retain_run(2000,2000,b'x'*1025,keep=17)
        self.assertEqual(self.store.status()['history_count'],17)

    def test_retention_age_cleanup_is_bounded(self):
        for i in range(31):self.store.retain_run(i,i,b'{}',keep=31)
        with Measure(self.db) as observed:self.assertEqual(self.store.prune_runs(100,limit=3),3)
        self.assertEqual(observed.mutated_rows,4) # three summaries plus bounded counter
        self.assertEqual(self.store.status()['history_count'],28)

    def test_lowering_retention_cannot_trigger_unbounded_cleanup(self):
        for i in range(31):self.store.retain_run(i,i,b'{}',keep=31)
        with self.assertRaises(CapacityPressure):self.store.retain_run(32,32,b'{}',keep=1)
        self.assertEqual(self.store.status()['history_count'],31)

    def test_retention_does_not_remove_live_obligations(self):
        self.store.admit(0,[1,2],capacity=2)
        for i in range(30):self.store.retain_run(i,i,b'{}',keep=3)
        self.store.prune_runs(100,limit=100)
        self.assertEqual(self.store.status()['pending'],2)

    def test_scratch_retry_reuses_reservation(self):
        for _ in range(1000):self.store.reserve_scratch(1,123,capacity=200)
        self.assertEqual(self.store.status()['scratch_octets'],123)
        with self.assertRaises(CapacityPressure):self.store.reserve_scratch(2,78,capacity=200)
        self.assertEqual(self.store.status()['scratch_octets'],123)
        with self.assertRaises(ValueError):self.store.reserve_scratch(1,124,capacity=200)

    def test_state_survives_reopen_after_capacity_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'queue.db';db=sqlite3.connect(p);store=OperationalModel(db)
            store.admit(0,[1],capacity=1)
            with self.assertRaises(CapacityPressure):store.admit(1,[2],capacity=1)
            db.close();db=sqlite3.connect(p)
            try:self.assertEqual(db.execute("SELECT serial,pending FROM bookkeeping").fetchone(),(1,1))
            finally:db.close()


class RetainedGenerationTests(unittest.TestCase):
    def test_rebaseline_retains_one_observation_per_distinct_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            store=ModelStore.initialize(Path(tmp)/'state.db',source=SOURCE,view=VIEW,tag='camera')
            try:
                for generation in range(100):
                    if generation:
                        store.rebaseline(store.position(),source=SOURCE,view=VIEW,cursor=f'g{generation}')
                    store.accept(store.position(),tuple(upsert(str(i)) for i in range(1,98)),
                                 source=SOURCE,view=VIEW,next_cursor='catchup',next_phase='catchup')
                self.assertEqual(store.db.execute('SELECT COUNT(*) FROM observed').fetchone()[0],97)
                self.assertEqual(store.db.execute('SELECT COUNT(*) FROM identity').fetchone()[0],97)
                self.assertEqual(store.db.execute('SELECT COUNT(*) FROM intents').fetchone()[0],0)
            finally:store.close()

    def test_cross_generation_does_not_reuse_old_match_or_forget_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            store=ModelStore.initialize(Path(tmp)/'state.db',source=SOURCE,view=VIEW,tag='camera')
            try:
                store.accept(store.position(),(upsert(),),source=SOURCE,view=VIEW,
                             next_cursor='old',next_phase='catchup')
                stale=store.position()
                store.rebaseline(stale,source=SOURCE,view=OTHER_VIEW,cursor='new',backfill=True)
                with self.assertRaises(StaleProposal):
                    store.accept(stale,(),source=SOURCE,view=VIEW,next_cursor='stale',next_phase='following')
                with self.assertRaises(AuthorityError):
                    store.accept(store.position(),(upsert(root='f'*64),),source=SOURCE,view=OTHER_VIEW,
                                 next_cursor='bad',next_phase='catchup')
                store.accept(store.position(),(upsert(),),source=SOURCE,view=OTHER_VIEW,
                             next_cursor='good',next_phase='catchup')
                self.assertEqual(len(store.intents()),1)
            finally:store.close()

    def test_infinite_page_is_rejected_after_bounded_lookahead(self):
        with tempfile.TemporaryDirectory() as tmp:
            store=ModelStore.initialize(Path(tmp)/'state.db',source=SOURCE,view=VIEW,tag='camera')
            pulled=0
            def observations():
                nonlocal pulled
                for i in itertools.count(1):
                    pulled+=1;yield upsert(str(i))
            try:
                before=store.position()
                with self.assertRaises(ValueError):store.accept(before,observations(),source=SOURCE,view=VIEW,
                                                               next_cursor='bad',next_phase='catchup')
                self.assertEqual(pulled,101);self.assertEqual(store.position(),before)
            finally:store.close()


class TransferTurnTests(unittest.TestCase):
    def test_large_object_advances_with_exact_byte_budgets(self):
        data=bytes(range(251))*1001;stream=io.BytesIO(data);out=bytearray();counts=[]
        while stream.tell()<len(data):
            done=bounded_bytes(stream.read,out.extend,budget=4097,chunk=1000)
            counts.append(done);self.assertLessEqual(done,4097)
        self.assertEqual(bytes(out),data);self.assertEqual(sum(counts),len(data))

    def test_no_overread_to_infer_completion_at_exact_budget(self):
        calls=[]
        def read(n):calls.append(n);return b'x'*n
        self.assertEqual(bounded_bytes(read,lambda _:None,budget=7,chunk=4),7)
        self.assertEqual(calls,[4,3])

    def test_bad_source_extent_is_rejected_before_consumption(self):
        consumed=[]
        with self.assertRaises(ValueError):bounded_bytes(lambda n:b'x'*(n+1),consumed.append,budget=7,chunk=4)
        self.assertEqual(consumed,[])
