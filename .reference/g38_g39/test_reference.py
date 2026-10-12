"""Focused diagnostic tests, not full observer/profile/runtime qualification."""
from dataclasses import replace
import hashlib
from pathlib import Path
import sqlite3
import sys
import pytest
sys.path.insert(0, str(Path(__file__).parent))
from reference import Store, Grant, request_id, job_id, validate_delivery


def fixture(tmp_path, n=20):
    store = Store(tmp_path / 'work.db')
    store.begin_work('j')
    for i in range(n):
        s = f'{i:064x}'
        store.asked_row('j', s, f'root/member/{i}')
        store.answer_row('j', s, f'root/member/{i}', 'complete')
    return store


def accept(store, limit=7):
    store.seal_stage('j', independently_validated=True)
    work = []
    while True:
        n = store.check_step('j', 1, limit)
        work.append(n)
        assert store.connections == 0
        if not n:
            break
    store.publish('j', 1, current_authority=True)
    return work


@pytest.mark.parametrize('size', [0, 1, 63, 64, 65, 255, 256, 257, 1025, 10001])
def test_exact_segmented_transfer_restart(tmp_path, size):
    data = bytes(i % 251 for i in range(size))
    digest = hashlib.sha256(data).hexdigest()
    grant = Grant('j', 'principal', 3)
    for window in (17, 64, 256):
        path = tmp_path / f'{window}.db'
        store = Store(path)
        store.reserve(grant, digest, size)
        for offset in range(0, size, window):
            chunk = data[offset:offset+window]
            store.put(grant, digest, offset, chunk)
            assert store.put(grant, digest, offset, chunk) == offset + len(chunk)
            store = Store(path)  # actual connection/file reuse, not live hash object
        store.finish(grant, digest)
        assert store.connections == 0
        if data:
            assert store.chunk(grant, digest, 0) == data[:window]


@pytest.mark.parametrize('kind', ['gap', 'overrun', 'budget', 'zero', 'negative', 'bool'])
def test_invalid_chunk(tmp_path, kind):
    store = Store(tmp_path / 'a.db')
    grant = Grant('j', 'p', 1)
    digest = hashlib.sha256(b'abc').hexdigest()
    store.reserve(grant, digest, 3)
    offset, data = {'gap':(1,b'a'), 'overrun':(0,b'abcd'), 'budget':(0,b'x'*257),
                    'zero':(0,b''), 'negative':(-1,b'a'), 'bool':(True,b'a')}[kind]
    with pytest.raises(ValueError):
        store.put(grant, digest, offset, data)
    assert store.connections == 0


def test_forged_terminal_and_corruption(tmp_path):
    store = Store(tmp_path / 'a.db'); grant = Grant('j', 'p', 1)
    digest = hashlib.sha256(b'abc').hexdigest()
    store.reserve(grant, digest, 3)
    store.put(grant, digest, 0, b'x')
    with pytest.raises(ValueError): store.finish(grant, digest)
    with pytest.raises(ValueError): store.chunk(grant, digest, 0)
    store.put(grant, digest, 1, b'bc')
    with pytest.raises(ValueError): store.finish(grant, digest)


def test_conflicting_retry_and_rollback(tmp_path):
    store = Store(tmp_path / 'a.db'); grant = Grant('j', 'p', 1)
    digest = hashlib.sha256(b'abc').hexdigest(); store.reserve(grant, digest, 3)
    def fail(): raise RuntimeError('crash model')
    with pytest.raises(RuntimeError): store.put(grant, digest, 0, b'abc', before_commit=fail)
    store.put(grant, digest, 0, b'abc')
    with pytest.raises(ValueError): store.put(grant, digest, 0, b'xyz')
    store.finish(grant, digest)


@pytest.mark.parametrize('mutation', ['job', 'principal', 'fence', 'live'])
def test_digest_does_not_grant_authority(tmp_path, mutation):
    store = Store(tmp_path / 'a.db'); grant = Grant('j', 'p', 1)
    digest = hashlib.sha256(b'abc').hexdigest(); store.reserve(grant, digest, 3)
    store.put(grant, digest, 0, b'abc'); store.finish(grant, digest)
    altered = replace(grant, **{mutation:{'job':'other','principal':'other',
                                          'fence':2,'live':False}[mutation]})
    with pytest.raises(PermissionError): store.chunk(altered, digest, 0)
    with pytest.raises(PermissionError): store.put(altered, digest, 0, b'abc')


def test_rebinding_length_and_authority(tmp_path):
    store = Store(tmp_path / 'a.db'); grant = Grant('j', 'p', 1)
    store.reserve(grant, '0'*64, 9)
    with pytest.raises(ValueError): store.reserve(grant, '0'*64, 10)
    with pytest.raises(PermissionError): store.reserve(replace(grant,fence=2), '0'*64, 9)


@pytest.mark.parametrize('n', [1, 17, 100, 101, 512, 2048])
def test_complete_quanta_and_constant_status(tmp_path, n):
    store = fixture(tmp_path, n)
    status_size = len(store.status('j'))
    work = accept(store, limit=17)
    assert sum(work) == n and max(work) <= 17
    assert status_size < 80 and len(store.status('j')) < 80
    with store.db() as db:
        assert db.execute('SELECT visited FROM work').fetchone() == (n,)
    rows, after = [], ''
    while page := store.view('j', after, 31, current_authority=True):
        rows.extend(page); after = page[-1][0]
    assert len(rows) == n


@pytest.mark.parametrize('kind', ['missing', 'extra', 'identity'])
def test_same_count_is_not_coverage(tmp_path, kind):
    store = fixture(tmp_path, 30)
    with store.db() as db:
        if kind == 'missing': db.execute('DELETE FROM answer WHERE subject=?', (f'{29:064x}',))
        if kind == 'extra': db.execute("INSERT INTO answer VALUES('j',?,'x','complete')", (f'{40:064x}',))
        if kind == 'identity': db.execute("UPDATE answer SET identity='wrong-root' WHERE subject=?", (f'{29:064x}',))
    store.seal_stage('j', independently_validated=True)
    with pytest.raises(ValueError):
        while store.check_step('j', 1, 7): pass
    with pytest.raises(ValueError): store.publish('j', 1, current_authority=True)
    with pytest.raises(ValueError): store.view('j', '', 5, current_authority=True)


def test_duplicate_subject_and_status(tmp_path):
    store = fixture(tmp_path, 1)
    with pytest.raises(sqlite3.IntegrityError): store.answer_row('j', '0'*64, 'x', 'complete')
    with pytest.raises(ValueError): store.answer_row('j', '1'*64, 'x', 'unknown')


def test_no_acceptance_on_provider_say_so(tmp_path):
    store = fixture(tmp_path, 5)
    store.seal_stage('j', independently_validated=False)
    while store.check_step('j', 1, 2): pass
    with pytest.raises(ValueError): store.publish('j', 1, current_authority=True)


def test_connection_return_and_stale_epoch(tmp_path):
    store = fixture(tmp_path, 20); store.seal_stage('j', independently_validated=True)
    def inspect():
        assert store.connections == 0
        assert store.takeover('j') == 2
    with pytest.raises(ValueError): store.check_step('j', 1, 7, detached=inspect)
    with store.db() as db: assert db.execute('SELECT visited FROM work').fetchone() == (0,)
    while store.check_step('j', 2, 7): pass
    store.publish('j', 2, current_authority=True)


def test_authority_change_and_completion_precedence(tmp_path):
    store = fixture(tmp_path, 1)
    store.seal_stage('j', independently_validated=True)
    while store.check_step('j', 1, 1): pass
    with pytest.raises(PermissionError): store.publish('j', 1, current_authority=False)
    store.publish('j', 1, current_authority=True)
    store.cancel('j')
    assert b'accepted' in store.status('j')
    with pytest.raises(PermissionError): store.view('j', '', 10, current_authority=False)


def test_whole_scope_global_taint_not_early_negative(tmp_path):
    store = fixture(tmp_path, 500)
    for i in range(500): store.relation('j', i, None, None, False)
    with store.db() as db:
        assert db.execute('SELECT taint FROM work').fetchone() == (1,)
        assert db.execute("SELECT count(*) FROM answer WHERE status='unsupported'").fetchone() == (0,)
    with pytest.raises(ValueError): store.view('j', '', 10, current_authority=True)
    accept(store)
    assert {r[2] for r in store.view('j', '', 500, current_authority=True)} == {'unsupported'}


def test_taint_before_status_and_ambiguous_preserved(tmp_path):
    store = Store(tmp_path / 'a.db'); store.begin_work('j')
    store.asked_row('j', 'a', 'x'); store.asked_row('j', 'b', 'y')
    store.relation('j', 0, None, 'a', False)
    store.answer_row('j', 'a', 'x', 'complete')
    store.answer_row('j', 'b', 'y', 'ambiguous')
    accept(store)
    assert [x[2] for x in store.view('j', '', 10, current_authority=True)] == ['unsupported','ambiguous']


def test_relations_cross_page_sources_and_duplicates(tmp_path):
    store = fixture(tmp_path, 20)
    a,b=f'{0:064x}',f'{19:064x}'
    store.relation('j',0,a,b,True)
    store.relation('j',0,a,b,True)
    store.relation('j',1,a,b,True)
    with store.db() as db:
        assert db.execute('SELECT count(*) FROM edge').fetchone() == (1,)
        assert db.execute('SELECT count(*) FROM source_relation').fetchone() == (2,)
    with pytest.raises(ValueError): store.relation('j',0,b,a,True)
    with pytest.raises(ValueError): store.relation('j',3,a,a,True)
    with pytest.raises(ValueError): store.relation('j',3,a,'unknown',True)
    accept(store)


def test_no_unbounded_delete_at_cancellation(tmp_path):
    store = fixture(tmp_path, 100)
    store.cancel('j')
    with pytest.raises(ValueError): store.publish('j', 1, current_authority=True)
    counts=[]
    while n:=store.gc_step('j',17): counts.append(n)
    assert sum(counts)==200 and max(counts)<=17


def test_wal_checkpoint_between_checks(tmp_path):
    store = fixture(tmp_path,40); store.seal_stage('j',independently_validated=True)
    while store.check_step('j',1,3):
        assert store.connections==0
        db=sqlite3.connect(store.path)
        assert db.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0]==0
        db.close()


def test_question_and_runtime_identity_separation():
    req=request_id('q','scope','ports','provider')
    assert req==request_id('q','scope','ports','provider')
    assert req!=request_id('other-task','scope','ports','provider')
    assert job_id(req,'claim',1)!=job_id(req,'claim',2)
    assert job_id(req,'claim',1)!=job_id(req,'other',1)
    # Transfer chunk size/token are absent from semantic identity inputs.
    with pytest.raises(ValueError): job_id(req,'claim',True)


def test_whole_scope_is_not_physical_batching():
    q={'a':'root1/member1','b':'root2/member1'}
    validate_delivery(q,q,whole_scope=True)
    with pytest.raises(ValueError): validate_delivery(q,{'a':q['a']},whole_scope=True)
    validate_delivery(q,{'a':q['a']},whole_scope=False)
    with pytest.raises(ValueError): validate_delivery(q,{'a':q['b']},whole_scope=False)
    with pytest.raises(ValueError): validate_delivery(q,{},whole_scope=True)


@pytest.mark.parametrize('n', [17, 101, 512])
def test_native_whole_scope_query_is_not_bounded_by_batch_hint(n):
    import os
    from source_probe import native_delivery
    repo = Path(os.environ['RIVERHOG_AUDITED'])
    whole = native_delivery(repo,n,'whole-scope')
    independent = native_delivery(repo,n,'independent-subjects')
    assert whole['subjects_decoded'] == whole['returned_subjects'] == n
    assert independent['subjects_decoded'] == min(n,100)


def test_proposed_minimum_side_effect_rules_are_in_native_source():
    import ast, os
    repo=Path(os.environ['RIVERHOG_AUDITED'])
    p=repo/'some-implementations/stove0/packages/protocol/src/stove0_protocol/observation_views.py'
    text=p.read_text(); tree=ast.parse(text)
    node=next(x for x in tree.body if isinstance(x,ast.FunctionDef) and x.name=='project_interface')
    block=ast.get_source_segment(text,node)
    assert 'associated is not None else covered' in block
    assert 'edges.add((primary, associated))' in block
    assert 'records.append(row)' in block
    assert 'if statuses[subject] == "complete"' in block


@pytest.mark.parametrize('n', [16, 64, 256])
def test_native_unresolved_relation_cost(n):
    import os
    from source_probe import native_unresolved_taint
    result=native_unresolved_taint(Path(os.environ['RIVERHOG_AUDITED']),n)
    assert result['native_status_lookups']==n*n
