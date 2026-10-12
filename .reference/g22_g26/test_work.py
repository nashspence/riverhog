from __future__ import annotations

import ast
import hashlib
import json
import os
from pathlib import Path
import random
import sqlite3
from types import SimpleNamespace

import pytest
from jsonschema import Draft202012Validator

from model import Crash, Conflict, DagWork, FactWork, FRAGMENT, db, leaf, node, frame
from sha256_state import Sha256


@pytest.mark.parametrize('size',[0,1,55,56,63,64,65,119,120,127,128,129,1024,8193])
def test_exact_sha_resume_and_nonmutating_digest(size):
    raw=random.Random(size).randbytes(size)
    h=Sha256()
    for i in range(0,size,17):
        h.update(raw[i:i+17]); old=h.export()
        assert h.digest()==hashlib.sha256(raw[:i+17]).digest()
        assert h.export()==old
        h=Sha256.restore(old)
    assert h.digest()==hashlib.sha256(raw).digest()
    assert len(h.export())<=108


@pytest.mark.parametrize('seed',range(10))
def test_sha_random_restart_splits(seed):
    rng=random.Random(seed); raw=rng.randbytes(8192+seed)
    h=Sha256();i=0
    while i<len(raw):
        n=rng.randint(1,257);h.update(raw[i:i+n]);i+=n;h=Sha256.restore(h.export())
    assert h.digest()==hashlib.sha256(raw).digest()


@pytest.mark.parametrize('raw',[b'',b'x'*45,b'S256\x01'+b'\0'*41,b'S256\x02'+b'\0'*40])
def test_bad_checkpoint_shapes_fail(raw):
    with pytest.raises(ValueError): Sha256.restore(raw)


def run_dag(path,budget=7,epoch=1):
    units=0
    while True:
        obj=DagWork(path)
        if obj.status()[0] in ('done','cyclic'):return obj.status(),units
        assert obj.step(budget,epoch=epoch)<=budget
        units+=1
        assert units<100000


@pytest.mark.parametrize('shape',['reverse','star','sparse','diamond'])
@pytest.mark.parametrize('budget',[1,7,31])
def test_kahn_high_degree_and_restart(tmp_path,shape,budget):
    if shape=='reverse': edges=[(i+1,i) for i in range(96)]
    elif shape=='star':edges=[(0,i+1) for i in range(96)]
    elif shape=='sparse':edges=[(i*10000,(i+1)*10000) for i in range(96)]
    else:edges=[(0,i) for i in range(1,49)]+[(i,99) for i in range(1,49)]
    path=tmp_path/'dag';g=DagWork(path)
    for i in range(0,len(edges),17):g.add(edges[i:i+17],17)
    g.add(edges[:2],2) # graph duplicates do not change indegrees
    g.seal();result,units=run_dag(path,budget)
    assert result[0]=='done';assert result[1]==result[2];assert result[3]==len(set(edges))
    with db(path) as c: ranks=dict(c.execute('SELECT id,n FROM rank'))
    assert all(ranks[a]<ranks[b] for a,b in edges)


def test_kahn_crash_and_stale_worker(tmp_path):
    p=tmp_path/'dag';g=DagWork(p);g.add([(0,1),(1,2)],2);g.seal();before=g.status()
    with pytest.raises(Crash):g.step(5,crash=True)
    assert g.status()==before
    epoch=g.takeover()
    with pytest.raises(Conflict):g.step(1,epoch=1)
    assert run_dag(p,epoch=epoch)[0][0]=='done'


@pytest.mark.parametrize('edges',[[(1,1)],[(0,1),(1,0)],[(0,1),(1,2),(2,1)]])
def test_cyclic_certificate_never_claims_valid(tmp_path,edges):
    p=tmp_path/'dag';g=DagWork(p);g.add(edges,10);g.seal()
    assert run_dag(p)[0][0]=='cyclic'
    # A cyclic speculative union must invoke native exact-prefix replay, not
    # report final graph acceptance or select its own semantic-error precedence.


def pieces(values):
    for p,raw in enumerate(values):
        if not raw:yield (p,0,b'',0)
        for offset in range(0,len(raw),FRAGMENT):
            yield p,offset//FRAGMENT,raw[offset:offset+FRAGMENT],len(raw)


def fill(work,records,budget=31):
    batch=[]
    for r in records:
        batch.append(r)
        if len(batch)==budget:
            work.stage(batch,row_budget=budget,byte_budget=FRAGMENT*budget);batch=[]
    if batch:work.stage(batch,row_budget=budget,byte_budget=FRAGMENT*budget)


def finish(path,budget=7):
    count=0
    while True:
        b=FactWork(path)
        if b.status()[0]=='done':return b.root(),count
        assert b.step(budget)<=budget;count+=1
        assert count<1000000


def oracle_mth(leaves):
    if not leaves:return hashlib.sha256(b'').digest()
    if len(leaves)==1:return leaves[0]
    k=1<<((len(leaves)-1).bit_length()-1)
    return node(oracle_mth(leaves[:k]),oracle_mth(leaves[k:]))


@pytest.mark.parametrize('budget',[1,7,31])
@pytest.mark.parametrize('values',[[],[b''],[b'x'],[b'x',b'x'],[b'a'*256,b'b'*257,b'']])
def test_merkle_root_independent_of_steps_and_restart(tmp_path,budget,values):
    path=tmp_path/'facts';b=FactWork(path,len(values));recs=list(pieces(values))
    fill(b,recs,budget);b.seal();root,_=finish(path,budget)
    want=oracle_mth([leaf((p,part),raw) for p,part,raw,_ in recs])
    want=hashlib.sha256(b'g26-model-dataset/v1\0'+frame(str(len(values)).encode())+frame(str(len(recs)).encode())+want).digest()
    assert root==want


def test_record_order_not_transport_order_changes_identity(tmp_path):
    values=[b'a'*300,b'b',b'c'*600]
    a=FactWork(tmp_path/'a',3);b=FactWork(tmp_path/'b',3)
    records=list(pieces(values));fill(a,records)
    fill(b,[r for p in [2,0,1] for r in records if r[0]==p])
    a.seal();b.seal();assert finish(tmp_path/'a')[0]==finish(tmp_path/'b',1)[0]


def test_partial_retry_and_hash_carry_crash(tmp_path):
    path=tmp_path/'facts';b=FactWork(path,1);recs=list(pieces([b'xyz'*700]))
    b.stage(recs[:2],row_budget=2,byte_budget=512)
    before=b.status()
    with pytest.raises(Crash):b.stage(recs[2:4],row_budget=2,byte_budget=512,crash=True)
    assert b.status()==before
    fill(b,recs);b.seal();b.step(1);before=b.status()
    with pytest.raises(Crash):b.step(7,crash=True)
    assert b.status()==before
    assert finish(path,1)[0]


def test_gap_conflicting_retry_and_source_terminal(tmp_path):
    b=FactWork(tmp_path/'facts',2)
    r=list(pieces([b'a'*600]))
    with pytest.raises(Conflict):b.stage([r[1]],row_budget=1,byte_budget=256)
    b.stage(r[:1],row_budget=1,byte_budget=256)
    with pytest.raises(Conflict):b.stage([(0,0,b'b'*256,600)],row_budget=1,byte_budget=256)
    with pytest.raises(Conflict):b.seal()
    fill(b,r)
    with pytest.raises(Conflict):b.seal() # second posting is still missing
    b.stage([(1,0,b'',0)],row_budget=1,byte_budget=1);b.seal()
    with pytest.raises(Conflict):b.stage([(1,0,b'',0)],row_budget=1,byte_budget=1)


def test_above_prior_posting_and_chunk_limits(tmp_path):
    b=FactWork(tmp_path/'postings',20001)
    fill(b,((i,0,b'x',1) for i in range(20001)),127);b.seal()
    assert b.status()[1]==20001
    big=FactWork(tmp_path/'chunks',1)
    fill(big,((0,i,b'x'*FRAGMENT,10001*FRAGMENT) for i in range(10001)),127)
    big.seal();assert big.status()[1]==1
    # Full root hashing is separately exercised by the increasing-size probe.


def test_connection_release_between_work_units(tmp_path):
    p=tmp_path/'dag';g=DagWork(p);g.add([(0,i) for i in range(1,100)],100);g.seal()
    c=sqlite3.connect(p);c.execute('PRAGMA journal_mode=WAL');c.close()
    while g.status()[0] not in ('done','cyclic'):
        g.step(7)
        c=sqlite3.connect(p,timeout=0)
        c.execute('BEGIN EXCLUSIVE');c.execute('COMMIT')
        assert c.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0]==0;c.close()


def test_source_anchored_extent_negative_control():
    repo=Path(os.environ['RIVERHOG_AUDITED'])
    path=repo/'riverhog/src/riverhog_core/canonical_discovery_index.py'
    tree=ast.parse(path.read_text())
    constants={n.targets[0].id:ast.literal_eval(n.value) for n in tree.body
               if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name)
               and n.targets[0].id.startswith('_MAX_')}
    assert constants['_MAX_POSTINGS_PER_ASSERTION']==20000
    assert constants['_MAX_POSTINGS_PER_PAGE']==50000
    assert constants['_MAX_TEXT_CHUNKS_PER_POSTING']==10000
    fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='stage_assertion_page')
    # Execute exactly the native guards through per-assertion rejection, not the
    # SQL/production function; dependencies past this prefix are out of scope.
    prefix=[]
    for n in fn.body:
        if isinstance(n,ast.For):
            loop=ast.For(target=n.target,iter=n.iter,body=[n.body[0]],orelse=[])
            prefix.append(loop);break
        prefix.append(n)
    fn.body=prefix
    module=ast.fix_missing_locations(ast.Module(body=[fn],type_ignores=[]))
    env={'Session':object,'Sequence':list,'IndexedAssertion':object,
         '_require_pending':lambda *a:None,'_MAX_ROWS_PER_BATCH':32,
         '_MAX_POSTINGS_PER_PAGE':50000,'_MAX_POSTINGS_PER_ASSERTION':20000,
         'IndexResourceLimit':Conflict}
    exec(compile(module,str(path),'exec'),env)
    row=SimpleNamespace(row_key='one',postings=range(20001))
    with pytest.raises(Conflict,match='assertion exceeds'):
        env['stage_assertion_page'](None,build_id='fixture',rows=[row])
    schema_path=repo/'packages/riverhog-provenance-contracts/src/riverhog_provenance_contracts/schemas/riverhog-provenance-v1-journal-entry.schema.json'
    schema=json.loads(schema_path.read_text());schema['$ref']='#/$defs/json'
    value=['x']*20001
    Draft202012Validator(schema).validate(value)
    assert len(json.dumps(value,separators=(',',':')).encode())<8*1024*1024
    # This proves a native generic-value/derived-guard mismatch, NOT a complete
    # bound journal/profile/custody fixture reaching the production endpoint.
