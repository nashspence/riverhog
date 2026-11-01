"""Executable reference vectors: planner parity and SQLite visibility, not service qualification.

Run from the reference checkout:
  python .reference/probe.py
In this dependency-limited research environment only:
  RIVERHOG_REFERENCE_ASCII_ONLY=1 python .reference/probe.py
The latter installs a fail-on-use sentinel for a missing rfc8785 dependency.
No serializer is substituted: tested plans use the maintained serializer's
ASCII-key/safe-integer fast path. Any call to the missing slow path fails.
"""
from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import sqlite3
import subprocess
import sys
import tempfile
import time
import types
import unittest
from dataclasses import asdict

ROOT = Path(__file__).resolve().parents[1]
for source in [ROOT / 'riverhog/src', *sorted(ROOT.glob('packages/*/src'))]:
    sys.path.insert(0, str(source))
SENTINEL = False
if os.getenv('RIVERHOG_REFERENCE_ASCII_ONLY') == '1':
    try:
        import rfc8785
    except ModuleNotFoundError:
        m = types.ModuleType('rfc8785')
        class Unavailable(Exception):
            pass
        def unavailable(*args, **kwargs):
            raise Unavailable('unqualified RFC8785 slow path requested')
        m.CanonicalizationError = Unavailable
        m.dump = m.dumps = unavailable
        sys.modules['rfc8785'] = m
        SENTINEL = True

from bounded_emission import Frontier, advance, coverage, plan_bytes
from riverhog_core.collection_plan import CollectionVolumePolicy
from riverhog_core.domain.archive import ArchiveArtifact
from riverhog_core.incremental_plan import (
    OrderedArchiveArtifact, advance_incremental_volume_plan,
    incremental_volume_planner_checkpoint_bytes,
    new_incremental_volume_planner, parse_incremental_volume_planner_checkpoint,
)

UNIT = 5 * 1024 * 1024
POLICY = CollectionVolumePolicy(pack_source_bytes=10, pack_artifacts=2,
    pack_member_bytes=8, pack_part_plaintext_bytes=UNIT,
    raw_volume_plaintext_bytes=UNIT, raw_part_plaintext_bytes=UNIT)

def item(order, size):
    return OrderedArchiveArtifact(order, ArchiveArtifact(f'{order+1:064x}', size,
        hashlib.sha256(f'fixture:{order}:{size}'.encode()).hexdigest()))

def encoded(state):
    return json.dumps({'cp':incremental_volume_planner_checkpoint_bytes(state.checkpoint).decode(),
        'member':state.member, 'raw_offset':state.raw_offset, 'done':state.done},sort_keys=True)

def decoded(text):
    x=json.loads(text)
    return Frontier(parse_incremental_volume_planner_checkpoint(x['cp']),
        x['member'],x['raw_offset'],x['done'])

SAMPLES=[]

class PublicationModel:
    """One-construction SQL witness. Auth/leases and crash durability are NOT implemented.

    A single existing worker would call stage/verify/publish; APIs only admit/read.
    This model uses BEGIN IMMEDIATE. Commit-trigger tests exercise logical
    rollback, not killed-process, disk failure or actual service integration.
    """
    def __init__(self, path):
        self.path=path
        self.db=sqlite3.connect(path,isolation_level=None)
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS head(id INTEGER PRIMARY KEY CHECK(id=1), cp TEXT NOT NULL,
            pending TEXT, epoch INTEGER NOT NULL, accepted INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS operation(id TEXT PRIMARY KEY, request TEXT NOT NULL,
            state TEXT NOT NULL, base TEXT NOT NULL, frontier TEXT NOT NULL,
            verifier TEXT NOT NULL, start_seq INTEGER NOT NULL, end_seq INTEGER,
            stage_chain TEXT NOT NULL, verify_chain TEXT NOT NULL, receipt TEXT);
        CREATE TABLE IF NOT EXISTS member(id TEXT PRIMARY KEY, ord INTEGER UNIQUE,
            bytes INTEGER, digest TEXT, op TEXT);
        CREATE TABLE IF NOT EXISTS volume(seq INTEGER PRIMARY KEY, op TEXT, plan BLOB NOT NULL);
        CREATE TABLE IF NOT EXISTS edge(seq INTEGER, member TEXT, PRIMARY KEY(seq,member));
        CREATE INDEX IF NOT EXISTS operation_volumes ON volume(op,seq);
        ''')
        cp=incremental_volume_planner_checkpoint_bytes(new_incremental_volume_planner(policy=POLICY)).decode()
        self.db.execute('INSERT OR IGNORE INTO head VALUES(1,?,NULL,0,0)',(cp,))
    def close(self):self.db.close()
    def _tx(self, fn):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            value=fn();self.db.commit();return value
        except BaseException:
            self.db.rollback();raise
    def admit(self, inputs):
        request=json.dumps([asdict(x) for x in inputs],sort_keys=True,separators=(',',':'))
        identity=hashlib.sha256(b'reference-registration\0'+request.encode()).hexdigest()
        def write():
            old=self.db.execute('SELECT request FROM operation WHERE id=?',(identity,)).fetchone()
            if old:
                assert old[0]==request
                return identity
            cp,pending,epoch,count=self.db.execute('SELECT cp,pending,epoch,accepted FROM head').fetchone()
            if pending: raise ValueError('registration busy')
            checkpoint=parse_incremental_volume_planner_checkpoint(cp)
            if checkpoint.closed:raise ValueError('closed')
            if inputs and inputs[0].order!=checkpoint.next_artifact_order:raise ValueError('order')
            for x in inputs:
                a=x.artifact
                self.db.execute('INSERT INTO member VALUES(?,?,?,?,?)',
                    (a.artifact_id,x.order,a.bytes,a.sha256,identity))
            frontier=encoded(Frontier(checkpoint))
            self.db.execute('INSERT INTO operation VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                (identity,request,'staging',cp,frontier,frontier,checkpoint.next_sequence,None,'0'*64,'0'*64,None))
            self.db.execute('UPDATE head SET pending=? WHERE id=1',(identity,))
            return identity
        return self._tx(write)
    def _load(self, identity):
        self.db.row_factory=sqlite3.Row
        row=self.db.execute('SELECT * FROM operation WHERE id=?',(identity,)).fetchone()
        inputs=tuple(OrderedArchiveArtifact(x['order'],ArchiveArtifact(**x['artifact']))
            for x in json.loads(row['request']))
        return row,inputs
    @staticmethod
    def _chain(previous, plan):
        body=plan_bytes(plan)
        data=(bytes.fromhex(previous)+len(body).to_bytes(8,'big')+body+
              json.dumps(coverage(plan),separators=(',',':')).encode())
        return hashlib.sha256(data).hexdigest()
    def stage(self, identity, epoch, fail=False):
        def write():
            head=self.db.execute('SELECT pending,epoch FROM head').fetchone()
            if tuple(head)!=(identity,epoch):raise ValueError('stale owner')
            row,inputs=self._load(identity)
            if row['state']!='staging':raise ValueError('not staging')
            state,plans=advance(decoded(row['frontier']),inputs,max_plans=3,max_steps=4,max_edges=16,max_bytes=16000)
            chain=row['stage_chain']
            for plan in plans:
                self.db.execute('INSERT INTO volume VALUES(?,?,?)',(plan.sequence,identity,plan_bytes(plan)))
                for mid in coverage(plan):self.db.execute('INSERT INTO edge VALUES(?,?)',(plan.sequence,mid))
                chain=self._chain(chain,plan)
            if fail:raise OSError('injected before frontier commit')
            self.db.execute('UPDATE operation SET frontier=?,state=?,end_seq=?,stage_chain=? WHERE id=?',
                (encoded(state),'verifying' if state.done else 'staging',state.checkpoint.next_sequence,chain,identity))
            return len(plans)
        return self._tx(write)
    def verify(self, identity, epoch):
        def write():
            if tuple(self.db.execute('SELECT pending,epoch FROM head').fetchone())!=(identity,epoch):raise ValueError('stale owner')
            row,inputs=self._load(identity)
            if row['state']!='verifying':raise ValueError('not verifying')
            state,plans=advance(decoded(row['verifier']),inputs,max_plans=3,max_steps=4,max_edges=16,max_bytes=16000)
            chain=row['verify_chain']
            for plan in plans:
                stored=self.db.execute('SELECT plan FROM volume WHERE seq=? AND op=?',(plan.sequence,identity)).fetchone()
                edges=tuple(x[0] for x in self.db.execute('SELECT member FROM edge WHERE seq=? ORDER BY member',(plan.sequence,)))
                if stored is None or bytes(stored[0])!=plan_bytes(plan) or edges!=tuple(sorted(coverage(plan))):
                    raise ValueError('staged plan/coverage mismatch')
                chain=self._chain(chain,plan)
            if state.done:
                if chain!=row['stage_chain'] or encoded(state)!=row['frontier']:raise ValueError('frontier mismatch')
                if self.db.execute('SELECT 1 FROM volume WHERE op=? AND seq>=? LIMIT 1',(identity,row['end_seq'])).fetchone():raise ValueError('extra plan')
            self.db.execute('UPDATE operation SET verifier=?,verify_chain=?,state=? WHERE id=?',
                (encoded(state),chain,'ready' if state.done else 'verifying',identity))
        self._tx(write)
    def publish(self, identity, epoch, fail=False):
        def write():
            row,_=self._load(identity)
            if row['state']=='published':return row['receipt']
            cp,pending,held,count=tuple(self.db.execute('SELECT cp,pending,epoch,accepted FROM head').fetchone())
            if held!=epoch or pending!=identity or cp!=row['base'] or row['state']!='ready':raise ValueError('publication fence')
            end=decoded(row['frontier']).checkpoint
            receipt=json.dumps({'operation':identity,'first_sequence':row['start_seq'],
                'next_sequence':row['end_seq'],'next_order':end.next_artifact_order},sort_keys=True)
            self.db.execute('UPDATE head SET cp=?,accepted=?,pending=NULL WHERE id=1',
                (incremental_volume_planner_checkpoint_bytes(end).decode(),end.next_artifact_order))
            if fail:raise OSError('injected before atomic marker')
            self.db.execute("UPDATE operation SET state='published',receipt=? WHERE id=?",(receipt,identity))
            return receipt
        return self._tx(write)
    def visible(self):
        return self.db.execute('SELECT COUNT(*) FROM member WHERE ord<(SELECT accepted FROM head)').fetchone()[0]
    def visible_plans(self):
        cp=parse_incremental_volume_planner_checkpoint(self.db.execute('SELECT cp FROM head').fetchone()[0])
        return [bytes(x[0]) for x in self.db.execute('SELECT plan FROM volume WHERE seq<? ORDER BY seq',(cp.next_sequence,))]
    def finish(self, identity, epoch=0):
        while True:
            row,_=self._load(identity)
            if row['state']=='staging': self.stage(identity,epoch)
            elif row['state']=='verifying':self.verify(identity,epoch)
            else:break
    def abort(self, identity):
        # Models an already-authorized owning cancellation/disposition, not a new API.
        def write():
            row,_=self._load(identity)
            if row['state']=='published':raise ValueError('cannot unpublish registration')
            if self.db.execute('SELECT pending FROM head').fetchone()[0]!=identity:raise ValueError('wrong pending')
            self.db.execute('UPDATE head SET epoch=epoch+1')
            self.db.execute("UPDATE operation SET state='aborting' WHERE id=?",(identity,))
        self._tx(write)
    def cleanup(self, identity):
        def write():
            row,_=self._load(identity)
            if row['state']!='aborting':raise ValueError('not aborting')
            seqs=[x[0] for x in self.db.execute('SELECT seq FROM volume WHERE op=? ORDER BY seq LIMIT 3',(identity,))]
            for seq in seqs:
                self.db.execute('DELETE FROM edge WHERE seq=?',(seq,))
                self.db.execute('DELETE FROM volume WHERE seq=?',(seq,))
            if not seqs:
                self.db.execute('DELETE FROM member WHERE op=?',(identity,))
                self.db.execute("UPDATE operation SET state='aborted' WHERE id=?",(identity,))
                self.db.execute('UPDATE head SET pending=NULL WHERE pending=?',(identity,))
            return len(seqs)
        return self._tx(write)
    def page(self, identity, after=-1, limit=3):
        row,_=self._load(identity)
        if row['state']!='published':raise ValueError('no published volume page')
        rows=list(self.db.execute('SELECT seq,plan FROM volume WHERE op=? AND seq>? AND seq>=? AND seq<? ORDER BY seq LIMIT ?',
            (identity,after,row['start_seq'],row['end_seq'],limit+1)))
        chosen=rows[:limit]
        return [bytes(x[1]) for x in chosen], chosen[-1][0] if len(rows)>limit else None

class Vectors(unittest.TestCase):
    def test_differential_raw_frontiers(self):
        for count in [1,16,17,129,1000]:
            for quantum in [1,3,16]:
                inputs=(item(0,count*UNIT-1),)
                base=new_incremental_volume_planner(policy=POLICY)
                expected=advance_incremental_volume_plan(base,inputs)
                state=Frontier(base);plans=[];calls=0;peak=0
                while not state.done:
                    state,new=advance(state,inputs,max_plans=quantum,max_steps=quantum,max_edges=16,max_bytes=100000)
                    state=decoded(encoded(state));calls+=1;peak=max(peak,len(new));plans.extend(new)
                self.assertEqual([plan_bytes(x) for x in plans],[plan_bytes(x) for x in expected.volumes])
                self.assertEqual(state.checkpoint,expected.checkpoint)
                self.assertLessEqual(peak,quantum)
                SAMPLES.append({'raw_volumes':count,'quantum':quantum,'calls':calls,'peak_plans':peak})
    def test_mixed_pack_raw_and_close_parity(self):
        rng=random.Random(964)
        for n in range(1,81):
            inputs=tuple(item(i,rng.choice([0,1,3,7,8,UNIT,2*UNIT+5])) for i in range(n))
            base=new_incremental_volume_planner(policy=POLICY)
            expected=advance_incremental_volume_plan(base,inputs,final=True)
            state=Frontier(base);plans=[]
            while not state.done:
                state,delta=advance(state,inputs,max_plans=2,max_steps=3,max_edges=16,max_bytes=16000,final=True)
                plans.extend(delta);state=decoded(encoded(state))
            self.assertEqual(state.checkpoint,expected.checkpoint)
            self.assertEqual([plan_bytes(x) for x in plans],[plan_bytes(x) for x in expected.volumes])
    def test_previous_pending_pack_is_flushed_once(self):
        base=advance_incremental_volume_plan(new_incremental_volume_planner(policy=POLICY),(item(0,3),)).checkpoint
        inputs=(item(1,17*UNIT),)
        expected=advance_incremental_volume_plan(base,inputs)
        state=Frontier(base);plans=[]
        while not state.done:
            state,delta=advance(state,inputs,max_plans=1,max_steps=1,max_edges=16,max_bytes=16000)
            plans.extend(delta)
        self.assertEqual([plan_bytes(x) for x in plans],[plan_bytes(x) for x in expected.volumes])
    def test_explicit_byte_admission_no_omission(self):
        base=new_incremental_volume_planner(policy=POLICY);inputs=(item(0,3*UNIT),)
        with self.assertRaises(ValueError):advance(Frontier(base),inputs,max_plans=1,max_steps=1,max_edges=16,max_bytes=1)
        _,sample=advance(Frontier(base),inputs,max_plans=1,max_steps=1,max_edges=16,max_bytes=16000)
        grant=max(len(plan_bytes(x)) for x in advance_incremental_volume_plan(base,inputs).volumes)
        state,one=advance(Frontier(base),inputs,max_plans=3,max_steps=3,max_edges=16,max_bytes=grant)
        self.assertEqual(len(one),1);self.assertEqual(state.raw_offset,UNIT)
    def model(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
        model=PublicationModel(str(Path(temp.name)/'model.sqlite3'));self.addCleanup(model.close)
        return model
    def test_visibility_atomicity_cold_and_lost_reply(self):
        m=self.model();inputs=(item(0,17*UNIT),);identity=m.admit(inputs)
        self.assertEqual(m.admit(inputs),identity)
        while m._load(identity)[0]['state']=='staging':
            m.stage(identity,0);self.assertEqual(m.visible(),0);self.assertEqual(m.visible_plans(),[])
        m.close();m.db=sqlite3.connect(m.path,isolation_level=None)
        m.finish(identity)
        self.assertEqual(m.visible(),0)
        with self.assertRaises(OSError):m.publish(identity,0,fail=True)
        self.assertEqual(m.visible(),0)
        ack=m.publish(identity,0)
        self.assertEqual(m.visible(),1);self.assertEqual(len(m.visible_plans()),17)
        self.assertEqual(m.publish(identity,0),ack);self.assertEqual(m.admit(inputs),identity)
    def test_failed_quantum_does_not_advance_or_duplicate(self):
        m=self.model();identity=m.admit((item(0,17*UNIT),));before=m._load(identity)[0]['frontier']
        with self.assertRaises(OSError):m.stage(identity,0,fail=True)
        self.assertEqual(m._load(identity)[0]['frontier'],before)
        self.assertEqual(m.db.execute('SELECT COUNT(*) FROM volume').fetchone()[0],0)
        m.finish(identity);m.publish(identity,0)
        self.assertEqual(len(m.visible_plans()),17)
    def test_no_concurrent_append_or_stale_epoch(self):
        m=self.model();identity=m.admit((item(0,17*UNIT),))
        with self.assertRaises(ValueError):m.admit((item(0,18*UNIT),))
        m.db.execute('UPDATE head SET epoch=1')
        with self.assertRaises(ValueError):m.stage(identity,0)
        m.finish(identity,1);m.publish(identity,1)
        self.assertEqual(m.visible(),1)
    def test_missing_or_changed_staged_coverage_blocks_publish(self):
        for change in ['DELETE FROM edge WHERE seq=1',"UPDATE volume SET plan=x'00' WHERE seq=1"]:
            m=self.model();identity=m.admit((item(0,4*UNIT),))
            while m._load(identity)[0]['state']=='staging':m.stage(identity,0)
            m.db.execute(change)
            with self.assertRaises(ValueError):m.finish(identity)
            with self.assertRaises(ValueError):m.publish(identity,0)
            self.assertEqual(m.visible(),0)
    def test_pages_pin_original_range_across_later_append(self):
        m=self.model();a=m.admit((item(0,17*UNIT),));m.finish(a);m.publish(a,0)
        first,token=m.page(a);self.assertEqual(len(first),3)
        b=m.admit((item(1,5*UNIT),));m.finish(b);m.publish(b,0)
        got=first
        while token is not None:
            page,token=m.page(a,token);got+=page
        self.assertEqual(len(got),17);self.assertEqual(len(m.visible_plans()),22)
    def test_abort_fences_publish_and_cleanup_preserves_earlier_work(self):
        m=self.model();a=m.admit((item(0,2*UNIT),));m.finish(a);ack=m.publish(a,0)
        old=m.visible_plans();b=m.admit((item(1,17*UNIT),));m.stage(b,0);m.abort(b)
        with self.assertRaises(ValueError):m.publish(b,0)
        with self.assertRaises(ValueError):m.stage(b,0)
        self.assertEqual(m.visible_plans(),old)
        m.close();m.db=sqlite3.connect(m.path,isolation_level=None)
        while m.cleanup(b):pass
        self.assertEqual(m.visible(),1);self.assertEqual(m.visible_plans(),old)
        self.assertEqual(m.publish(a,1),ack)
        self.assertEqual(m._load(b)[0]['state'],'aborted')
    def test_pending_page_not_success(self):
        m=self.model();identity=m.admit((item(0,17*UNIT),));m.stage(identity,0)
        with self.assertRaises(ValueError):m.page(identity)
    def test_all_small_batch_has_zero_emission_but_accepted_members(self):
        m=self.model();identity=m.admit((item(0,1),item(1,2)));m.finish(identity)
        ack=json.loads(m.publish(identity,0));self.assertEqual(m.visible(),2)
        self.assertEqual(ack['first_sequence'],ack['next_sequence'])

if __name__=='__main__':
    start=time.perf_counter()
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Vectors))
    record={'kind':'reference-only; not service or custody qualification',
        'source':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
        'python':platform.python_version(),'sqlite':sqlite3.sqlite_version,
        'missing_rfc8785_fail_on_use_sentinel':SENTINEL,
        'tests':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),
        'elapsed_seconds':time.perf_counter()-start,'samples':SAMPLES}
    Path(__file__).with_name('PROBE_RESULT.json').write_text(json.dumps(record,indent=2)+'\n')
    raise SystemExit(not result.wasSuccessful())
