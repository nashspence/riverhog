from __future__ import annotations

import random
import sqlite3
from itertools import product

import pytest

from factored_index import FactorIndex

SOURCE = {'archive_root_sha256': 'a'*64, 'extraction': 'prototype-fixed-scopes/v1'}


def populated(db, n=1, a=3):
    db.exact('journals', ('j', '1'*64), ('id',))
    for i in range(a):
        db.exact('assertions', ('j', f'a{i}', i, None, b'{}'), ('journal', 'assertion'))
    db.exact('snapshots', ('s', 'j', a, b'{}'), ('id',))
    shared = db.node('snapshot', 's'); db.leaf(shared, 's')
    for i in range(n):
        root = db.node('member', str(i)); db.edge(root, shared)
        db.root(str(i), {'id': str(i)}, [root])
    return shared


@pytest.mark.parametrize('n', [16, 32, 64, 128, 256])
def test_factor_build_hash_and_common_search_avoid_cartesian_expansion(n, monkeypatch):
    with FactorIndex() as db:
        populated(db, n, n)
        sizes = db.sizes(); db.seal(sizes, SOURCE)
        assert sizes['assertions'] == n and sizes['leaves'] == 1 and sizes['emits'] == 0
        assert sum(sizes.values()) == db.counts['hash_records'] == 5*n+4
        assert db.candidates([('s', 'a0', 'recorded-history')]) == {str(i) for i in range(n)}
        assert db.counts['reverse_node_visits'] == n+1
        assert db.logical('0') == {('s', f'a{i}', 'recorded-history') for i in range(n)}
        def forbid_expansion(*args):
            raise AssertionError('search attempted full member/assertion expansion')
        monkeypatch.setattr(db, 'logical', forbid_expansion)
        supports = db.first_support_unguarded([((i,), 's', f'a{i}', 'recorded-history') for i in reversed(range(n))])
        assert supports == {str(i): (0,) for i in range(n)}
        assert db.counts['first_support_nodes'] == n+1
        assert db.counts['first_support_edges'] == n


def test_guard_requires_both_inputs_for_the_same_member():
    with FactorIndex() as db:
        populated(db, 0)
        left, right, target = [db.node('signal', i) for i in range(3)]
        db.gate(left, right, target); db.gate(right, left, target)
        db.emit(target, 's', 'a0', 'input-history')
        db.root('left', {}, [left]); db.root('right', {}, [right]); db.root('both', {}, [left, right])
        db.seal(db.sizes(), SOURCE)
        wanted = {('s', 'a0', 'input-history')}
        assert db.candidates(wanted) == {'left', 'right', 'both'}
        assert not db.logical('left') & wanted
        assert not db.logical('right') & wanted
        assert db.logical('both') & wanted == wanted
        # OR-only fast path must not fabricate support through a guard.
        assert db.first_support_unguarded([((0,), *next(iter(wanted)))]) == {}


@pytest.mark.parametrize('seed', range(12))
def test_fixedpoint_matches_independent_set_oracle(seed):
    rng = random.Random(seed)
    with FactorIndex() as db:
        nodes = [db.node('n', i) for i in range(20)]
        edges = {(rng.randrange(20), rng.randrange(20)) for _ in range(30)}
        gates = {(rng.randrange(20), rng.randrange(20), rng.randrange(20)) for _ in range(20)}
        for a, b in edges: db.edge(nodes[a], nodes[b])
        for a, b, c in gates: db.gate(nodes[a], nodes[b], nodes[c])
        start = set(rng.sample(range(20), 3)); db.root('m', {}, [nodes[i] for i in start])
        db.seal(db.sizes(), SOURCE)
        expected = set(start)
        while True:
            new = expected | {b for a, b in edges if a in expected} | {c for a, b, c in gates if a in expected and b in expected}
            if new == expected: break
            expected = new
        assert db.reachable('m') == {nodes[i] for i in expected}
        assert db.counts['query_node_visits'] == len(expected)
        assert db.counts['query_gate_incidents'] <= 2*len(gates)


def test_diamond_uses_nodes_not_exponentially_many_paths():
    with FactorIndex() as db:
        prior = [db.node('root')]; db.root('m', {}, prior)
        for level in range(24):
            layer = [db.node('layer', level, side) for side in ('l', 'r')]
            for a, b in product(prior, layer): db.edge(a, b)
            prior = layer
        db.seal(db.sizes(), SOURCE); db.reachable('m')
        assert db.counts['query_node_visits'] == 49
        assert db.counts['query_edge_visits'] == 94


def test_digest_deterministic_across_order_retry_and_reopen(tmp_path):
    digests = []
    for reverse in (False, True):
        path = tmp_path/str(reverse)
        with FactorIndex(path) as db:
            for i in sorted(range(20), reverse=reverse): db.root(str(i), {'i': str(i)}, [db.node('m', i)])
            sizes = db.sizes()
            for i in range(20): db.root(str(i), {'i': str(i)}, [db.node('m', i)])
            assert db.sizes() == sizes
            digests.append(db.seal(sizes, SOURCE))
            with pytest.raises(ValueError, match='immutable'): db.node('late')
        with FactorIndex(path) as db:
            assert len(db.reachable('1')) == 1
    assert digests[0] == digests[1]


def test_missing_changed_incomplete_and_out_of_prefix_rows_rejected():
    with FactorIndex() as db:
        populated(db)
        with pytest.raises(sqlite3.IntegrityError): db.edge('unknown', 'other')
        with pytest.raises(ValueError, match='changed'): db.exact('assertions', ('j', 'a0', 0, None, b'{"x":1}'), ('journal', 'assertion'))
        with pytest.raises(ValueError, match='unsealed'): db.logical('0')
        expected = db.sizes(); expected['members'] += 1
        with pytest.raises(ValueError, match='incomplete'): db.seal(expected, SOURCE)
        db.emit(db.node('bad'), 's', 'absent', 'member')
        with pytest.raises(ValueError, match='supported'): db.seal(db.sizes(), SOURCE)


def test_growing_prefixes_store_assertions_once():
    with FactorIndex() as db:
        db.exact('journals', ('j', 'f'*64), ('id',)); n = 128
        for i in range(n):
            db.exact('assertions', ('j', f'a{i}', i, None, b'{}'), ('journal', 'assertion'))
            db.exact('snapshots', (f's{i}', 'j', i, b'{}'), ('id',))
            node = db.node('snap', i); db.leaf(node, f's{i}'); db.root(str(i), {}, [node])
        db.seal(db.sizes(), SOURCE)
        assert db.sizes()['assertions'] == n
        assert db.counts['hash_records'] == 6*n+1
        assert len(db.logical('127')) == 128 and len(db.logical('0')) == 1


def test_minimum_support_propagation_compares_all_paths_not_first_discovered():
    with FactorIndex() as db:
        populated(db, 0)
        nodes = [db.node('node', i) for i in range(5)]
        db.root('member', {}, [nodes[0]])
        for a, b in ((0,1), (0,2), (1,3), (2,4), (4,2)):
            db.edge(nodes[a], nodes[b])
        db.emit(nodes[3], 's', 'a0', 'member')
        db.emit(nodes[4], 's', 'a1', 'member')
        db.seal(db.sizes(), SOURCE)
        assert db.first_support_unguarded([((4,), 's','a0','member'), ((2,), 's','a1','member')]) == {'member': (2,)}
        assert db.counts['first_support_nodes'] == 5
