"""Executable diagnostic witnesses, not production G26/G28 qualification."""
from __future__ import annotations

import random
import sqlite3

import pytest

from model import InvalidView, LostAttempt, NotReady, Store, contains_chunks


def run(store, query='q', *, budget=7, change=None, quota=10**9):
    now, visits = 0, 0
    while True:
        epoch = store.claim(query, now=now)
        ticket = store.ticket(query, epoch, now=now)
        unit = store.compute(ticket, budget=budget)
        assert unit.visits <= budget
        assert not store.db.in_transaction
        visits += unit.visits
        if change:
            change(now)
        state = store.commit(unit, now=now, quota_bytes=quota)
        assert not store.db.in_transaction
        if state != 'SCAN':
            return state, now, visits
        now += 1
        assert now < 10000


def all_pages(store, query='q', **kw):
    rows, after = [], -1
    while True:
        page, complete = store.page(query, now=0, after=after, size=3, **kw)
        rows.extend(page)
        if complete:
            return rows
        after = page[-1][0]


@pytest.mark.parametrize('width', [6, 12, 63])
def test_admission_and_path_copy_are_bounded(tmp_path, width):
    s = Store(tmp_path/'db', width=width)
    for key in range(32):
        before = dict(s.counters)
        s.put(key, str(key))
        assert s.counters['nodes_created'] - before['nodes_created'] <= width + 1
        assert s.counters['nodes_read'] - before['nodes_read'] <= width
    before = dict(s.counters)
    root = s.start('q')
    assert s.counters['nodes_read'] == before['nodes_read']
    assert s.counters['admission_node_reads'] == 0
    s.put(0, 'new')
    assert s.start('q') == root  # admission replay is not a fresh snapshot
    run(s)
    assert all_pages(s) == [(k, str(k)) for k in range(32)]
    assert s.max_stack <= width + 1
    s.close()


@pytest.mark.parametrize('budget', [1, 7, 31])
def test_continuous_mutations_do_not_restart_or_mix_query(tmp_path, budget):
    s = Store(tmp_path/'db', width=8)
    for k in range(48):
        s.put(k, f'original-{k}')
    old = s.start('q')
    def mutate(i):
        s.put(i % 48, f'changed-{i}')
        s.put(100, f'new-{i}')
        s.reap(limit=11)
    state, _, visits = run(s, budget=budget, change=mutate)
    assert state == 'READY'
    assert all_pages(s) == [(k, f'original-{k}') for k in range(48)]
    assert s.db.execute("SELECT root FROM query WHERE id='q'").fetchone()[0] == old
    assert visits <= 2 * 48 + 8
    s.close()


def test_real_sqlite_wal_can_checkpoint_between_work(tmp_path):
    path = tmp_path/'db'
    s, writer = Store(path, width=8), Store(path, width=8)
    for k in range(16):
        s.put(k, 'old')
    s.start('q')
    observations = []
    epoch = s.claim('q', now=0)
    t = s.ticket('q', epoch, now=0)
    def while_computing(_):
        assert not s.db.in_transaction
        writer.put(200, 'concurrent')
        observations.append(tuple(writer.db.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()))
    unit = s.compute(t, budget=20, observer=while_computing)
    s.commit(unit, now=0)
    assert observations and all(o[0] == 0 and o[1] == 0 for o in observations)
    run(s)
    assert all_pages(s) == [(k, 'old') for k in range(16)]
    writer.close(); s.close()


def test_reopen_resumes_persisted_stack_and_root(tmp_path):
    path = tmp_path/'db'
    s = Store(path, width=8)
    for k in range(60): s.put(k, str(k))
    s.start('q')
    epoch = s.claim('q', now=0)
    unit = s.compute(s.ticket('q', epoch, now=0), budget=23)
    assert s.commit(unit, now=0) == 'SCAN'
    previous = s.db.execute("SELECT version,hits FROM query WHERE id='q'").fetchone()
    s.close()
    s = Store(path, width=8)
    s.put(1, 'replacement')
    run(s)
    assert all_pages(s) == [(k, str(k)) for k in range(60)]
    assert s.db.execute("SELECT version FROM query WHERE id='q'").fetchone()[0] > previous[0]
    s.close()


def test_crash_before_checkpoint_replays_only_one_unit(tmp_path):
    s = Store(tmp_path/'db', width=8)
    for k in range(20): s.put(k, str(k))
    s.start('q')
    epoch = s.claim('q', now=0, lease=2)
    t = s.ticket('q', epoch, now=0)
    a = s.compute(t, budget=9)
    with pytest.raises(RuntimeError): s.commit(a, now=0, fail=True)
    assert s.db.execute('SELECT COUNT(*) FROM result').fetchone()[0] == 0
    epoch2 = s.claim('q', now=2)
    b = s.compute(s.ticket('q', epoch2, now=2), budget=9)
    assert a.rows == b.rows and a.stack == b.stack
    with pytest.raises(LostAttempt): s.commit(a, now=2)
    s.commit(b, now=2)
    run(s)
    assert all_pages(s) == [(k, str(k)) for k in range(20)]
    s.close()


def test_double_commit_fenced(tmp_path):
    s = Store(tmp_path/'db', width=8); s.put(2, 'x'); s.start('q')
    epoch = s.claim('q', now=0)
    u = s.compute(s.ticket('q', epoch, now=0), budget=4)
    s.commit(u, now=0)
    with pytest.raises(LostAttempt): s.commit(u, now=0)
    s.close()


def test_cancel_during_compute_cannot_publish(tmp_path):
    s = Store(tmp_path/'db', width=8); s.put(2, 'x'); s.start('q')
    epoch = s.claim('q', now=0)
    u = s.compute(s.ticket('q', epoch, now=0), budget=20)
    s.terminate('q')
    with pytest.raises(LostAttempt): s.commit(u, now=0)
    with pytest.raises(InvalidView): s.page('q', now=0)
    s.close()


def test_capacity_is_explicit_and_resumable_not_truncated(tmp_path):
    s = Store(tmp_path/'db', width=8)
    for k in range(50): s.put(k, 'x'*20)
    s.start('q')
    state, _, _ = run(s, budget=11, quota=150)
    assert state == 'BLOCKED'
    with pytest.raises(NotReady): s.page('q', now=0)
    old = tuple(s.db.execute("SELECT version,hits,root FROM query WHERE id='q'").fetchone())
    s.resume_capacity('q')
    run(s, budget=11, quota=10000)
    assert len(all_pages(s)) == 50
    assert s.db.execute("SELECT root FROM query WHERE id='q'").fetchone()[0] == old[2]
    s.close()


@pytest.mark.parametrize('change', ['principal', 'authority', 'source', 'expiry', 'live'])
def test_current_authority_and_source_are_not_snapshot_permissions(tmp_path, change):
    s = Store(tmp_path/'db', width=8); s.put(2, 'x'); s.start('q', expires=50)
    run(s)
    kw = {'now':0}
    if change == 'principal': kw['principal'] = 'other'
    elif change == 'authority': kw['authority'] = 'new-grants'
    elif change == 'source': s.reset_source('new-source')
    elif change == 'expiry': kw['now'] = 50
    elif change == 'live': kw['allow'] = lambda _: False
    with pytest.raises(InvalidView): s.page('q', **kw)
    s.close()


def test_deletion_retains_source_but_live_denial_does_not_skip(tmp_path):
    s = Store(tmp_path/'db', width=8)
    s.put(1, 'one'); s.put(2, 'two'); s.start('q'); s.put(1, None)
    while s.db.execute('SELECT COUNT(*) FROM reap').fetchone()[0]: s.reap(limit=4)
    run(s)
    with pytest.raises(InvalidView): s.page('q', now=0, allow=lambda k:k!=1)
    assert all_pages(s) == [(1,'one'), (2,'two')]
    s.close()


def test_gc_is_bounded_and_cannot_delete_active_pin(tmp_path):
    s = Store(tmp_path/'db', width=8)
    for k in range(20): s.put(k, str(k))
    root = s.start('q')
    for k in range(20): s.put(k, None)
    while s.db.execute('SELECT COUNT(*) FROM reap').fetchone()[0]:
        assert s.reap(limit=3) <= 3
    assert s.db.execute('SELECT refs FROM node WHERE hash=?', (root,)).fetchone()[0] == 1
    run(s)
    assert len(all_pages(s)) == 20
    s.terminate('q')
    while s.db.execute('SELECT COUNT(*) FROM reap').fetchone()[0]: assert s.reap(limit=3) <= 3
    assert s.db.execute('SELECT COUNT(*) FROM node').fetchone()[0] == 0
    while s.db.execute('SELECT COUNT(*) FROM result').fetchone()[0]:
        assert s.cleanup_results('q', limit=2) <= 2
    s.close()


def test_reuse_zero_ref_node_survives_reap_queue(tmp_path):
    s = Store(tmp_path/'db', width=8)
    s.put(1, 'one'); before = s.db.execute('SELECT root FROM current_view').fetchone()[0]
    s.put(1, None); s.put(1, 'one')
    while s.db.execute('SELECT COUNT(*) FROM reap').fetchone()[0]: s.reap(limit=3)
    assert s.db.execute('SELECT root FROM current_view').fetchone()[0] == before
    s.start('q'); run(s); assert all_pages(s) == [(1, 'one')]
    s.close()


def test_mutation_rollback_preserves_exact_source(tmp_path):
    s = Store(tmp_path/'db', width=8); s.put(1,'one')
    before = s.db.execute('SELECT root FROM current_view').fetchone()[0]
    with pytest.raises(RuntimeError): s.put(1,'bad',fail=True)
    assert s.db.execute('SELECT root FROM current_view').fetchone()[0] == before
    s.start('q'); run(s); assert all_pages(s) == [(1,'one')]
    s.close()


@pytest.mark.parametrize('empty_source', [False, True])
def test_empty_is_only_published_after_terminal_traversal(tmp_path, empty_source):
    s = Store(tmp_path/'db', width=8)
    if not empty_source:
        for k in range(40): s.put(k,'no match')
    s.start('q',predicate='absent')
    with pytest.raises(NotReady): s.page('q',now=0)
    run(s,budget=2)
    assert s.page('q',now=0) == ([],True)
    s.close()


def test_identity_and_chain_do_not_depend_on_quantum_size(tmp_path):
    s = Store(tmp_path/'db',width=8)
    for k in range(32): s.put(k,str(k))
    roots=[];chains=[]
    for i,b in enumerate([1,9,40]):
        q=f'q{i}'; roots.append(s.start(q)); run(s,q,budget=b)
        chains.append(s.db.execute('SELECT chain FROM query WHERE id=?',(q,)).fetchone()[0])
    assert len(set(roots))==1 and len(set(chains))==1
    s.close()


@pytest.mark.parametrize('seed', range(8))
def test_random_snapshot_mutation_oracle(tmp_path, seed):
    rng=random.Random(seed);s=Store(tmp_path/'db',width=8);expected={}
    for _ in range(100):
        key=rng.randrange(100);value=None if rng.randrange(4)==0 else str(rng.randrange(1000))
        s.put(key,value)
        if value is None: expected.pop(key,None)
        else: expected[key]=value
    s.start('q')
    for _ in range(100): s.put(rng.randrange(100),str(rng.randrange(1000)))
    run(s,budget=13)
    assert all_pages(s)==sorted(expected.items())
    s.close()


@pytest.mark.parametrize('text,needle,fold', [
    ('xxneedlezz','needle',False), ('aaaaaaab','aaaab',False),
    ('ÄaBCz','Abc',True), ('Äß','ä',True), ('xx%_\\zz','%_\\',False),
    ('你好world','好w',False), ('abc','abcd',False), ('','x',False),
    ('aaaaa','ba',False), ('aAÀ','aaà',True),
])
def test_streaming_literal_across_every_chunk_boundary(text,needle,fold):
    ascii_lower=lambda s: ''.join(chr(ord(c)+32) if 'A'<=c<='Z' else c for c in s)
    expected=(ascii_lower(needle) in ascii_lower(text)) if fold else needle in text
    for cut in range(len(text)+1):
        assert contains_chunks(iter([text[:cut],text[cut:]]),needle,ascii_fold=fold)==expected
    assert contains_chunks(iter(text),needle,ascii_fold=fold)==expected


def test_corrupt_immutable_source_fails_closed(tmp_path):
    s = Store(tmp_path/'db', width=8); s.put(1,'one'); s.start('q')
    s.db.execute("UPDATE node SET value='corrupt' WHERE kind='leaf'")
    e=s.claim('q',now=0)
    with pytest.raises(InvalidView): s.compute(s.ticket('q',e,now=0),budget=20)
    with pytest.raises(NotReady): s.page('q',now=0)
    s.close()


def test_sparse_keys_do_not_claim_dense_key_bound(tmp_path):
    s=Store(tmp_path/'db');keys=[0,(1<<62)-1,1<<62,(1<<63)-1]
    for k in keys:s.put(k,'equal payload')
    s.start('q');_,_,visits=run(s,budget=5)
    assert 2*len(keys)+s.width < visits <= (s.width+1)*len(keys)
    assert all_pages(s)==[(k,'equal payload') for k in keys]
    assert s.max_stack<=s.width+1
    s.close()


def test_long_snapshot_negative_control_blocks_truncation(tmp_path):
    path=tmp_path/'db';s=Store(path,width=8);w=Store(path,width=8)
    s.put(1,'one');w.db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    s.db.execute('BEGIN');s.db.execute('SELECT * FROM current_view').fetchall()
    w.put(2,'two');w.db.execute('PRAGMA busy_timeout=0')
    held=tuple(w.db.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone())
    assert held[0]==1
    s.db.rollback()
    assert tuple(w.db.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone())==(0,0,0)
    w.close();s.close()


def test_large_streamed_literal_uses_successive_chunks():
    def chunks():
        for _ in range(1000):yield 'a'*1024
        yield 'NE';yield 'EDLE'
    assert contains_chunks(chunks(),'needle',ascii_fold=True)
    assert not contains_chunks(iter(['Ä']*1000),'ä',ascii_fold=True)


def test_expired_pin_released_only_by_explicit_reaper_state(tmp_path):
    s=Store(tmp_path/'db',width=8);s.put(1,'x');root=s.start('q',expires=2)
    s.put(1,None)
    with pytest.raises(InvalidView):s.claim('q',now=2)
    assert s.db.execute('SELECT refs FROM node WHERE hash=?',(root,)).fetchone()[0]>0
    s.terminate('q',reason='EXPIRED')
    while s.db.execute('SELECT COUNT(*) FROM reap').fetchone()[0]:s.reap(limit=2)
    assert s.db.execute('SELECT COUNT(*) FROM node').fetchone()[0]==0
    s.close()
