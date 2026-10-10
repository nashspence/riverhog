#!/usr/bin/env python3
"""Source-anchored operation-count diagnostics, not whole-repository qualification.

Load the exact checked AST bodies for measured algorithms. External wire/model
validation is outside these tests; already-indexed/selected records are supplied.
Only standard library facilities are required. No repository runtime is changed.
"""
from __future__ import annotations
import ast
import base64
import binascii
import collections
import collections.abc
import contextlib
import hashlib
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import types
import typing
import weakref
import uuid
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

ROOT=Path(sys.argv[1] if __name__ == '__main__' and len(sys.argv)>1 else os.environ.get('RIVERHOG_AUDIT_ROOT', str(Path(__file__).parent/'clone'))).resolve()
SHA='8425e672c60285a549ca4d5aaba15ecbd99e5bf3'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()==SHA
PROV='packages/riverhog-provenance/src/riverhog_provenance/'
JSTORE=PROV+'journal_store.py'
CORPUS=PROV+'corpus.py'
HISTORY=PROV+'history.py'
PAIR='some-implementations/stove0/observers/filename-prefix-sidecars/src/a_stove0_filename_prefix_sidecar_observer/pairing.py'
OBS='some-implementations/stove0/application/server/src/stove0_core/accepted_observations.py'
RECOVERY='some-implementations/riverhog/recovery/src/a_riverhog_recovery_tool/_provenance_reader.py'
SOURCES={}
class ProvenanceValidationError(ValueError):pass

def canonical_ascii(x):
    # Diagnostics use only ASCII string keys/values, safe ints, booleans and null.
    # This equals JCS for that restricted domain; it is not a replacement codec.
    return json.dumps(x,sort_keys=True,separators=(',',':'),ensure_ascii=True).encode()

def tree(path):
    src=(ROOT/path).read_text()
    SOURCES[path]={'sha256':hashlib.sha256(src.encode()).hexdigest(),
                  'git_blob':subprocess.check_output(['git','hash-object',path],cwd=ROOT,text=True).strip()}
    return ast.parse(src,filename=path)

def ns():
    d={'__name__':'source_probe','__builtins__':__builtins__}
    exec('from __future__ import annotations\nfrom typing import *\nfrom collections.abc import *\nfrom dataclasses import dataclass\nfrom tempfile import TemporaryDirectory, mkdtemp\nfrom pathlib import Path\n',d)
    d.update(vars(types)); d.update({'hashlib':hashlib,'json':json,'sqlite3':sqlite3,
        'weakref':weakref,'shutil':shutil,'contextlib':contextlib,'sys':sys,
        're':re,'base64':base64,'binascii':binascii,
        'canonical_json_bytes':canonical_ascii,'canonical_document':canonical_ascii,
        'require_canonical_json':json.loads,'ProvenanceValidationError':ProvenanceValidationError,
        '_Validator':object,'fingerprint':lambda x:(x['bytes'],x['sha256']),
        'scratch_directory':lambda **kw:tempfile.TemporaryDirectory(**kw)})
    return d

def load_nodes(path,names,extra=None):
    t=tree(path); selected=[n for n in t.body if getattr(n,'name',None) in names]
    assert set(getattr(n,'name',None) for n in selected)==set(names)
    d=ns(); d.update(extra or {})
    # Required by dataclasses resolving deferred annotations.
    mod=types.ModuleType('source_probe'); mod.__dict__.update(d);sys.modules['source_probe']=mod
    block=ast.Module(body=[ast.ImportFrom(module='__future__',names=[ast.alias(name='annotations')],level=0),*selected],type_ignores=[])
    ast.fix_missing_locations(block);exec(compile(block,str(ROOT/path),'exec'),mod.__dict__)
    return mod.__dict__

def method_node(path,cls,name):
    c=next(n for n in tree(path).body if isinstance(n,ast.ClassDef) and n.name==cls)
    return next(n for n in c.body if isinstance(n,ast.FunctionDef) and n.name==name)

def method_sql(path,cls,name,contains):
    matches=[n.value for n in ast.walk(method_node(path,cls,name)) if isinstance(n,ast.Constant)
             and isinstance(n.value,str) and contains in n.value]
    assert len(matches)==1,(path,name,contains,matches)
    return matches[0]

def ddl(path,cls):
    for n in ast.walk(method_node(path,cls,'__init__')):
        if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='executescript':
            return ast.literal_eval(n.args[0])
    raise AssertionError('no schema')

def measure_sql(db,fn):
    steps=0
    def tick():
        nonlocal steps
        steps+=100
        return 0
    db.set_progress_handler(tick,100)
    start=time.perf_counter();fn();elapsed=time.perf_counter()-start
    db.set_progress_handler(None,0)
    return {'sqlite_vm_steps_rounded_down_100':steps,'seconds':round(elapsed,6)}

def make_db(schema):
    db=sqlite3.connect(':memory:');db.executescript(schema);return db

J=load_nodes(JSTORE,['_cleanup_snapshot','_cleanup_reader','_key','_Map','_Set','_Findings','_Edges','_StoredValidator','JournalStore'])
C=load_nodes(CORPUS,['CanonicalCorpusValidator'])

def touch_reset(n,fixed=False):
    db=make_db(ddl(JSTORE,'JournalStore'))
    query=method_sql(JSTORE,'JournalStore','accept','UPDATE assertions SET touched = 0')
    if fixed:db.execute('CREATE INDEX probe_touched ON assertions(touched)')
    plan=[r[3] for r in db.execute('EXPLAIN QUERY PLAN '+query)]
    def work():
        for i in range(n):
            db.execute(query)
            db.execute('INSERT INTO assertions VALUES(?,?,?,?,?,?,?,?,?,?)',
                       (f'a{i:08}',f'o{i:08}','entities','artifact',None,None,str(i),b'{}',1,1))
    result=measure_sql(db,work);result.update(n=n,plan=plan,
        row_count=db.execute('SELECT count(*) FROM assertions').fetchone()[0],
        touched=db.execute('SELECT count(*) FROM assertions WHERE touched=1').fetchone()[0])
    assert result['row_count']==n and result['touched']==1
    db.close();return result

D=load_nodes(HISTORY,['MemberHistoryClosure'],extra={
  'MemberHistoryBinding':SimpleNamespace(from_mapping=lambda v:v),
  'HistoryJournalAnchor':SimpleNamespace(from_mapping=lambda v:v),
  '_external_references':lambda v:iter(()),
})

def pending_queue(n,fixed=False,which='histories'):
    db=make_db(ddl(HISTORY,'MemberHistoryClosure'))
    for i in range(n):
        if which=='histories':db.execute('INSERT INTO histories VALUES(?,?,?,0)',(f'h{i:08}','retained',b'{}'))
        else:db.execute('INSERT INTO snapshots VALUES(?,?,0)',(f's{i:08}',b'{}'))
    if fixed:
        db.execute('CREATE INDEX pending_h ON histories(identity,extent) WHERE done=0')
        db.execute('CREATE INDEX pending_s ON snapshots(identity) WHERE done=0')
    q=method_sql(HISTORY,'MemberHistoryClosure','_drain',f'FROM {which} WHERE done = 0')
    plan=[r[3] for r in db.execute('EXPLAIN QUERY PLAN '+q)]
    obj=object.__new__(D['MemberHistoryClosure']);obj._db=db
    visited=[]
    obj._visit_history=lambda *args:visited.append('history')
    def summary_at(*args):
        visited.append('snapshot')
        return SimpleNamespace(frames=[SimpleNamespace(document={'body':{'assertions':{},'journal':{}}})])
    obj.summary_at=summary_at
    result=measure_sql(db,obj._drain);result.update(n=n,plan=plan,visited=len(visited))
    assert len(visited)==n and db.execute(f'SELECT count(*) FROM {which} WHERE done=0').fetchone()[0]==0
    db.close();return result

def corpus_graph(n,shape='chain'):
    with C['CanonicalCorpusValidator']() as c:
        edges=[(f'n{i:08}',f'n{i+1:08}') for i in range(n)] if shape=='chain' else [(f's{i:08}',f't{i:08}') for i in range(n)]
        c._db.executemany('INSERT INTO edges VALUES(?,?,?)',[('derivation',*e) for e in edges])
        result=measure_sql(c._db,c.validate);result.update(n=n,shape=shape)
        return result

def incremental_cycle(n):
    db=make_db(ddl(JSTORE,'JournalStore'))
    e=J['_Edges'](SimpleNamespace(db=db),'derivation')
    result=measure_sql(db,lambda:[e.append((f'n{i:08}',f'n{i+1:08}')) for i in reversed(range(n))])
    result.update(n=n,edges=db.execute('SELECT count(*) FROM edges').fetchone()[0])
    assert result['edges']==n
    try:e.append((f'n{n:08}',f'n{0:08}'))
    except ProvenanceValidationError:result['rejects_cycle']=True
    else:raise AssertionError('missed cycle')
    db.close();return result

def lifecycle_uses(n):
    db=make_db(ddl(JSTORE,'JournalStore'))
    for i in range(n):
        value={'id':f'u{i:08}','type':'usage','state':{'scope':'local','object_id':'s','object_type':'state'},'activity_id':f'act{i:08}'}
        db.execute('INSERT INTO assertions VALUES(?,?,?,?,?,?,?,?,?,?)',
                   (f'a{i:08}',value['id'],'relations','usage','s',None,'0',canonical_ascii(value),1,1))
    v=object.__new__(J['_StoredValidator']);v.store=SimpleNamespace(db=db)
    decoded=0
    old_json=J['json']
    def count_decode(value):
        nonlocal decoded
        decoded+=1
        return old_json.loads(value)
    J['json']=SimpleNamespace(loads=count_decode)
    try:
        result=measure_sql(db,lambda:v.finish({}, {}, (), ()))
    finally:
        J['json']=old_json
    assert decoded==n+n*n
    result.update(n=n,decoded_rows=decoded,related_rows_decoded=decoded-n)
    db.close();return result

P=load_nodes(PAIR,['UnsupportedLocator','LocatorEvidence','SourceStatus','FilenameCandidate','_ParsedName','_Selected','_name_bytes','split_locator','_view_id','_unique_support','_select','_stem','_comparable','compare_filenames'],extra={
    'SOURCE_NAMING_VIEW_SCHEME':'https://nashspence.github.io/riverhog/v1/provenance/identifiers/source-naming-view',
    'require_canonical_uuid_urn':load_nodes('packages/riverhog-provenance-contracts/src/riverhog_provenance_contracts/binding.py',['require_canonical_uuid_urn'],{'uuid':uuid})['require_canonical_uuid_urn'],
})
original_comparable=P['_comparable']
def filename_pairs(n):
    facts={};primary=[];sidecars=[]
    for prefix,extension,ids in [('p','.wav',primary),('s','.wav.xmp',sidecars)]:
        for i in range(n):
            id=f'{prefix}{i:08}';ids.append(id)
            facts[id]=(P['LocatorEvidence'](id,{'kind':'filesystem_path','form':'absolute','syntax':'posix','name':{'kind':'text','text':f'/media/{i:08}{extension}'}}, {'journal_id':'shared','context_id':'same'},({'scheme':P['SOURCE_NAMING_VIEW_SCHEME'],'scope':'global','value':{'kind':'text','text':'urn:uuid:11111111-1111-4111-8111-111111111111'}},),{'id':'context'},{'id':'locator-'+id}),)
    count=0
    def counted(a,b):
        nonlocal count
        count+=1
        return original_comparable(a,b)
    P['_comparable']=counted
    start=time.perf_counter();statuses,pairs=P['compare_filenames'](facts,primary_ids=primary,sidecar_ids=sidecars,sidecar_suffixes=('.xmp',));elapsed=time.perf_counter()-start
    assert len(statuses)==2*n and len(pairs)==n and all(s.status=='usable' for s in statuses)
    assert {(p.primary_id[1:],p.sidecar_id[1:]) for p in pairs}=={(f'{i:08}',f'{i:08}') for i in range(n)}
    return {'n':n,'members':2*n,'comparisons':count,'candidate_rows':len(pairs),'seconds':round(elapsed,6)}

def observer_subjects(n):
    # Execute precisely the selected request-port validation loop. The preceding
    # request/model validators are outside this benchmark, with a unique valid set.
    fn=method_node(OBS,'AcceptedObservationStore','register_request')
    loop=next(x for x in fn.body if isinstance(x,ast.For) and isinstance(x.iter,ast.Call) and ast.unparse(x.iter)=='ports.items()')
    counts={'yields':0}; values=[SimpleNamespace(id=f's{i:08}') for i in range(n)]
    class Subjects:
        def __iter__(self):
            for v in values:
                counts['yields']+=1;yield v
    lookup={v.id:v for v in values}
    env={'self':SimpleNamespace(selections=SimpleNamespace(member=lambda _,id:lookup[id])),
         'question':SimpleNamespace(subject_ports={'subjects':SimpleNamespace(selection_sha256='exact-selection')}),
         'request':SimpleNamespace(subjects=Subjects()),'ports':{'subjects':tuple(lookup)}}
    module=ast.fix_missing_locations(ast.Module(body=[loop],type_ignores=[]))
    start=time.perf_counter();exec(compile(module,str(ROOT/OBS),'exec'),env);elapsed=time.perf_counter()-start
    assert counts['yields']==n*(n+1)//2
    return {'n':n,'subject_visits':counts['yields'],'seconds':round(elapsed,6)}

# The recovery fixture feeds already-parsed, ordered exact descriptors. The
# original scan/range methods still check contiguity, root count/digest and payload
# digests. Object decoding/encryption is not benchmarked or claimed here.
@dataclass
class Terminal:
    sequence:int; archive_generation:str='generation';artifact_set_sha256:str='set'
    def to_json_bytes(self):return canonical_ascii({'seq':self.sequence,'terminal':True})
@dataclass
class Volume:
    sequence:int;journal_id:str;payload:object;journal_offset:int=0;journal_bytes:int=4;journal_sha256:str='';archive_generation:str='generation';artifact_set_sha256:str='set'
    def to_json_bytes(self):return canonical_ascii({'seq':self.sequence,'journal':self.journal_id,'sha256':self.journal_sha256})
DOMAIN=b'probe-descriptor-commitment\0'
def update_digest(digest,doc):digest.update(doc.to_json_bytes())
R=load_nodes(RECOVERY,['ProvenanceArchiveReadError','ProvenanceArchiveSummary','CanonicalProvenanceArchiveReader','_read_bounded'],extra={
 'PROVENANCE_SEQUENCE_DOMAIN':DOMAIN,'ProvenanceTerminalDocument':Terminal,
 'update_provenance_commitment':update_digest,
})

def recovery_journals(n):
    payload=b'data';sha=hashlib.sha256(payload).hexdigest()
    docs=[Volume(i,f'j{i:08}',SimpleNamespace(kind='journal',bytes=4,path=f'p{i:08}',sha256=sha),journal_sha256=sha) for i in range(n)]
    docs.append(Terminal(n))
    digest=hashlib.sha256(DOMAIN)
    for doc in docs:update_digest(digest,doc)
    root=SimpleNamespace(archive_generation='generation',artifact_set_sha256='set',ordered_volume_sha256=digest.hexdigest(),binding_count=0,journal_count=n)
    counts={'descriptors':0,'payload_reads':0,'root_reads':0}
    def read(path):counts['payload_reads']+=1;yield payload
    reader=R['CanonicalProvenanceArchiveReader'](read,expected_root_sha256='root',archive_generation='generation',artifact_set_sha256='set')
    def get_root():counts['root_reads']+=1;return root
    def descriptors():
        for doc in docs:counts['descriptors']+=1;yield doc
    reader._root=get_root;reader._descriptors=descriptors
    start=time.perf_counter()
    for i in range(n):assert b''.join(reader.iter_journal_range(f'j{i:08}'))==payload
    elapsed=time.perf_counter()-start
    assert counts['descriptors']==2*n*(n+1) and counts['payload_reads']==n
    return {'n':n,**counts,'seconds':round(elapsed,6)}

def negatives():
    with C['CanonicalCorpusValidator']() as c:
        c._db.executemany('INSERT INTO edges VALUES(?,?,?)',[('derivation','a','b'),('derivation','b','a')])
        try:c.validate()
        except ProvenanceValidationError:pass
        else:raise AssertionError('corpus cycle accepted')
    return {'corpus_cycle_rejected':True}

def run():
    result={'audited_sha':SHA,'python':sys.version,'sqlite':sqlite3.sqlite_version,
      'measurement':'source-anchored native internal algorithm probes; not functional qualification',
      'cases':{}}
    cases=[('journal_touch_reset',touch_reset),('journal_touch_reset_with_full_index',lambda n:touch_reset(n,True)),
           ('history_pending_queue',pending_queue),('history_pending_queue_with_partial_index',lambda n:pending_queue(n,True)),
           ('snapshot_pending_queue',lambda n:pending_queue(n,False,'snapshots')),
           ('corpus_chain',corpus_graph),('corpus_disjoint_edges',lambda n:corpus_graph(n,'disjoint')),
           ('incremental_reverse_chain',incremental_cycle),('lifecycle_shared_state',lifecycle_uses),
           ('filename_pairing',filename_pairs),('observer_subject_lookup',observer_subjects),('recovery_journal_ranges',recovery_journals)]
    for name,fn in cases:
        result['cases'][name]=[fn(n) for n in (128,256,512)]
        print(name,result['cases'][name],flush=True)
    result['negative_controls']=negatives();result['source_hashes']=SOURCES
    out=Path(__file__).parent/'results.json';out.write_text(json.dumps(result,indent=2)+'\n');print('WROTE',out)
if __name__=='__main__':run()
