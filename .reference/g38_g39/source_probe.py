"""Execute the exact native delivery method's query path on controlled fixtures.

Uses real SQLAlchemy/SQLite and the source AST. Typed deserialization and the final
physical-question constructor are instrumented fixture adapters, not native
semantic validation. No result is a valid-wire observation or production benchmark.
"""
from __future__ import annotations
import ast
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace as NS
from sqlalchemy import Column, MetaData, String, Table, Text, create_engine, select

PATH = 'some-implementations/stove0/application/server/src/stove0_core/compiled_observation_delivery.py'


def native_delivery(repo: Path, n: int, partitioning: str) -> dict:
    if n < 1 or partitioning not in {'whole-scope', 'independent-subjects'}:
        raise ValueError('fixture')
    source = (repo / PATH).read_bytes()
    tree = ast.parse(source)
    klass = next(x for x in tree.body if isinstance(x, ast.ClassDef)
                 and x.name == 'CompiledObservationDelivery')
    method = next(x for x in klass.body if isinstance(x, ast.FunctionDef) and x.name == 'request')
    module = ast.Module(body=[ast.ImportFrom(module='__future__',
                         names=[ast.alias(name='annotations')], level=0), method],type_ignores=[])
    module = ast.fix_missing_locations(module)
    engine = create_engine('sqlite://')
    metadata = MetaData()
    members = Table('members',metadata,Column('selection_sha256',String,primary_key=True),
                    Column('artifact_id',String,primary_key=True),Column('document_json',Text))
    physical = Table('physical',metadata,Column('request_id',String),Column('request_json',Text),
                     Column('question_sha256',String),Column('evidence_json',Text))
    coverage = Table('coverage',metadata,Column('question_sha256',String),Column('subject_id',String))
    metadata.create_all(engine)
    with engine.begin() as db:
        db.execute(members.insert(), [dict(selection_sha256='selection',artifact_id=f'{i:064x}',
                    document_json=json.dumps({'id':f'{i:064x}','sha256':'e'*64})) for i in range(n)])
    counts={'subjects_decoded':0,'subject_json_bytes':0}
    class Subject:
        @classmethod
        def model_validate_json(cls, doc):
            counts['subjects_decoded']+=1
            counts['subject_json_bytes']+=len(doc.encode())
            return NS(**json.loads(doc))
    descriptor=NS(support_for=lambda _:NS(preferred_subject_batch_size=100))
    state=NS(engine=engine,planning_key=lambda x:x,compiled_planning=NS(members=members),
             accepted_observations=NS(tables={'physical':physical,'coverage':coverage},
                                     register_request=lambda *args:None))
    resource=NS(interface=NS(partitioning=partitioning,inputs={}))
    closure=NS(interface=lambda **kwargs:resource)
    planner=NS(_definition=lambda _: (None,closure),
               observer_binding=lambda *args:('provider',descriptor),
               observation_execution_timeout_seconds=300)
    scope=NS(selection_sha256='selection')
    question=NS(interface=NS(id='i',sha256='x'),scope=scope,question_sha256='q',
                subject_ports={'subjects':scope},observer_contract=NS(id='contract'))
    namespace={'select':select,'WorkArtifactSubject':Subject,
               'physical_question':lambda **kwargs:NS(subjects=kwargs['subjects']),
               'EvidencePort':type('UnusedEvidencePort',(),{})}
    exec(compile(module,str(repo/PATH),'exec'),namespace)
    try:
        result,_=namespace['request'](NS(planner=planner,state=state),None,question)
        return {'input_subjects':n,'partitioning':partitioning,
                'returned_subjects':len(result.subjects),**counts,
                'source_sha256':hashlib.sha256(source).hexdigest(),
                'coverage':'native AST query path; typed/physical seal boundaries instrumented'}
    finally:
        engine.dispose()


def native_unresolved_taint(repo: Path, n: int) -> dict:
    """Actual relation-loop AST; controlled already-resolved fixtures, not acceptance."""
    from enum import Enum
    path=repo/'some-implementations/stove0/packages/protocol/src/stove0_protocol/observation_views.py'
    source=path.read_text(); tree=ast.parse(source)
    method=next(x for x in tree.body if isinstance(x,ast.FunctionDef) and x.name=='project_interface')
    loops=[x for x in ast.walk(method) if isinstance(x,ast.For) and isinstance(x.iter,ast.Call)
           and isinstance(x.iter.func,ast.Name) and x.iter.func.id=='_records']
    loop=max(loops,key=lambda x:x.lineno)
    class Truth(Enum):
        TRUE=1; FALSE=0
    class Count(dict):
        checks=0
        def __getitem__(self,key):
            self.checks+=1
            return super().__getitem__(key)
    statuses=Count({str(i):'complete' for i in range(n)})
    namespace={'view':NS(records=None,where=True,require=True,
                        primary=NS(at='p'),associated=NS(at='a')),
               'facts':None,'_records':lambda *_:({'p':'unresolved','a':'unresolved'} for _ in range(n)),
               'evaluate_row':lambda *_:Truth.TRUE,'Truth':Truth,
               'read_pointer':lambda row,key:row[key],'MISSING':object(),
               'SubjectEndpoint':type('SubjectEndpoint',(),{}),
               'lookups':{'primary':{},'associated':{}},
               'canonical_json_bytes':lambda value:json.dumps(value).encode(),
               'covered':tuple(statuses),'statuses':statuses,'edges':set(),'records':[]}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[loop],type_ignores=[])),str(path),'exec'),namespace)
    return {'subjects':n,'unresolved_relevant_claims':n,'native_status_lookups':statuses.checks,
            'source_sha256':hashlib.sha256(source.encode()).hexdigest(),
            'coverage':'native relation-loop AST with controlled pointer/predicate fixtures'}
