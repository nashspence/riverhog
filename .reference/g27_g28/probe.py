"""Execute source-anchored work diagnostics; no end-to-end timing qualification."""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import patch

from sqlalchemy import Column,Integer,Text,MetaData,Table,create_engine,select
from sqlalchemy.dialects import sqlite
from riverhog_core.browse import keyset_statement
import riverhog_core.canonical_discovery_extraction as extraction
from riverhog_core.canonical_discovery_search import _read_identity
from riverhog_protocol.artifact_discovery import ArtifactDiscoveryRequest
from reference import seek,clause_matches_once,ProjectionStore
from test_reference import row,clause,v

SOURCE="152d095038df0c0d0ed642cf265d47b11c65a754"
HASHES={
 'riverhog/src/riverhog_core/browse.py':'1c4afdbfae8e96d9c8b9a53b1d4137a233a91e17dc813af088f7c4b881b82162',
 'riverhog/src/riverhog_core/canonical_discovery_extraction.py':'b06dba0a7abcc09cf45b8d5960f9ea3ad00903c91b36654476b2eb46c291a4e9',
 'riverhog/src/riverhog_core/canonical_discovery_search.py':'a9896b90c7e9f88f51e34708950783a7aff4b055077f62705c721baf3a4ceea2',
 'packages/riverhog-protocol/src/riverhog_protocol/artifact_discovery.py':'09dae874bd41a4c886282492239e6773ca7a9abada2508191b22103e679e64e3',
}


def paging(n, *, layout="member", all_tied=False):
    meta=MetaData()
    t=Table('item',meta,Column('k',Integer,nullable=False),Column('id',Integer if layout=='rowid' else Text,primary_key=True))
    engine=create_engine('sqlite://')
    out={'rows':n,'layout':layout,'all_leading_keys_tied':all_tied,'page_size':25,'runs':{}}
    rows=[{'k':0 if all_tied else i//4,'id':i if layout=='rowid' else f'{i:064x}'} for i in range(1,n+1)]
    try:
        meta.create_all(engine)
        with engine.begin() as con:
            con.exec_driver_sql('CREATE INDEX by_key ON item(k,id)')
            con.execute(t.insert(),rows)
        raw=engine.raw_connection()
        try:
            db=raw.driver_connection
            for mode in ('native-or','nonnull-tuple'):
                instructions=[0]; pos=None; result=[]; pages=0; plans=[]
                def tick(): instructions[0]+=1;return 0
                while True:
                    args=dict(columns=(t.c.k,t.c.id),position=pos,order='asc',page_size=25)
                    stmt=keyset_statement(select(t),**args) if mode=='native-or' else seek(select(t),nonnull=True,**args)
                    sql=str(stmt.compile(dialect=sqlite.dialect(),compile_kwargs={'literal_binds':True}))
                    if pages in (0,1,n//50):
                        plans.append({'page':pages,'sql':sql,'plan':db.execute('EXPLAIN QUERY PLAN '+sql).fetchall()})
                    db.set_progress_handler(tick,1)
                    batch=db.execute(sql).fetchall()
                    db.set_progress_handler(None,0)
                    pages+=1;result.extend(batch[:25])
                    if len(batch)<=25:break
                    pos=batch[24]
                assert result==[(r['k'],r['id']) for r in rows]
                out['runs'][mode]={'sqlite_vm_instructions':instructions[0],'pages':pages,'returned':len(result),'plans':plans}
        finally:raw.close()
    finally:engine.dispose()
    return out


class CountedPointer(str):
    checks=0
    def startswith(self,*args,**kwargs):
        type(self).checks+=1
        return super().startswith(*args,**kwargs)


def profiles(n):
    r=row([{'v':'absent-haystack'} for _ in range(n)]); c=clause(v('needle'))
    native_postings=list(extraction.assertion_postings(r))
    counted=[extraction.ScalarPosting(CountedPointer(p.pointer),p.value,p.representation,p.source_value_sha256)
             for p in native_postings]
    CountedPointer.checks=0
    with patch.object(extraction,'assertion_postings',return_value=iter(counted)):
        assert extraction.assertion_clause_matches(r,c,assertion_state='effective') is None
    counts={}
    assert clause_matches_once(r,c,assertion_state='effective',counts=counts) is None
    return {'sections':n,'postings':len(native_postings),'native_startswith_calls':CountedPointer.checks,
            'candidate':counts,'result':'no-match; complete predicate evaluation'}


def identities(n):
    class Session:
        count=0
        def execute(self,statement):return self
        def yield_per(self,size):
            for i in range(1,n+1):
                self.count+=1
                yield SimpleNamespace(collection_id=i,archive_root_sha256='a'*64,generation_id=None,
                                      tag_revision=0,description_revision=0)
    class Selected:
        selected_columns=SimpleNamespace(collection_id='id')
        def order_by(self,unused):return self
    session=Session(); request=ArtifactDiscoveryRequest(page_size=50); digests=set()
    pages=(n+49)//50
    for _ in range(pages):
        digests.add(_read_identity(session,Selected(),request=request,principal=None,source_identity='model-source'))
    assert len(digests)==1
    model=ProjectionStore()
    try:
        for i in range(1,n+1):model.put_catalog(i)
        a,r=model.start('k','query','principal')
        model.append('k',a,[(i,'a','support') for i in range(1,n+1)],max_rows=n)
        model.seal('k',a,terminal=True,expected_count=n)
        pos=(0,'');returned=0
        while True:
            page=model.page('k','query','principal',authorized=True,expected_read=r,after=pos,size=50)
            returned+=len(page['items'])
            if page['complete']:break
            pos=page['next']
        assert returned==n
        return {'selected_collections':n,'pages':pages,'native_read_identity_rows':session.count,
                'projection_model_manifest_rows':model.counts['manifest_rows'],
                'projection_model_page_authorizations':model.counts['validations'],
                'note':'native digest uses controlled source-header fixtures; projection hash is model-only'}
    finally:model.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--repo',type=Path,required=True)
    args=parser.parse_args()
    sha=subprocess.check_output(['git','-C',str(args.repo),'rev-parse','HEAD'],text=True).strip()
    if sha!=SOURCE:raise SystemExit('source changed: reconcile rather than relabel evidence')
    for path,digest in HASHES.items():
        if hashlib.sha256((args.repo/path).read_bytes()).hexdigest()!=digest:raise SystemExit('source hash differs: '+path)
    for mod in (extraction,):
        if not Path(mod.__file__).resolve().is_relative_to(args.repo.resolve()):
            raise SystemExit('wrong imported source checkout')
    result={'kind':'source-anchored SQL/native-helper plus separate SQLite projection model',
            'source':sha,'python':sys.version,'sqlite':sqlite3.sqlite_version,
            'paging':[paging(n,all_tied=tied) for tied in (False,True) for n in (128,512,1024,2048)],
            'unqualified_rowid_negative':[paging(n,layout='rowid',all_tied=True) for n in (128,512,1024)],
            'profiles':[profiles(n) for n in (16,32,64,128,256,512)],
            'identities':[identities(n) for n in (128,512,1024,2048)],
            'not_qualification':['PostgreSQL plans','HTTP/current authorization integration','full image lifecycle','wall-time targets']}
    # Preserve exact SQL/plans without repeating identical witnesses in each run.
    catalog={}
    for run_group in result['paging']+result['unqualified_rowid_negative']:
        for run in run_group['runs'].values():
            witnesses=[]
            for plan in run['plans']:
                entry={key:value for key,value in plan.items() if key!='page'}
                ident=hashlib.sha256(json.dumps(entry,sort_keys=True).encode()).hexdigest()[:16]
                if ident in catalog and catalog[ident]!=entry:
                    raise RuntimeError('plan witness digest collision')
                catalog[ident]=entry
                witnesses.append({'page':plan['page'],'plan_id':ident})
            run['plans']=witnesses
    result['plan_catalog']=catalog
    print(json.dumps(result,separators=(',',':')))

if __name__=='__main__':main()
