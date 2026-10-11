"""Executable G26 relational model, not a production persistence adapter.

One instance is an isolated build. Production keys additionally include build_id.
The only gate is an AND of two reached signals. No generic user expression language.
Materializing helpers exist for differential tests, not the production search path.
"""
from __future__ import annotations

import hashlib
import heapq
import sqlite3
from collections import Counter
from collections.abc import Iterable
from pathlib import Path

from riverhog_canonical_json import canonical_json_bytes

SCOPES = frozenset({'member', 'input-history', 'collection', 'recorded-history'})
TABLES = ('nodes', 'edges', 'gates', 'journals', 'assertions', 'snapshots', 'leaves', 'emits', 'members', 'roots')


class FactorIndex:
    def __init__(self, path: str | Path = ':memory:') -> None:
        self.db = sqlite3.connect(path)
        self.db.executescript('''
        PRAGMA foreign_keys=ON; PRAGMA cache_size=-512; PRAGMA temp_store=FILE;
        CREATE TABLE IF NOT EXISTS nodes(id TEXT PRIMARY KEY, coordinate BLOB NOT NULL);
        CREATE TABLE IF NOT EXISTS edges(source TEXT REFERENCES nodes(id), target TEXT REFERENCES nodes(id), PRIMARY KEY(source,target));
        CREATE INDEX IF NOT EXISTS edge_reverse ON edges(target,source);
        CREATE TABLE IF NOT EXISTS gates(left_id TEXT REFERENCES nodes(id), right_id TEXT REFERENCES nodes(id), target TEXT REFERENCES nodes(id), PRIMARY KEY(left_id,right_id,target));
        CREATE INDEX IF NOT EXISTS gate_right ON gates(right_id,left_id,target);
        CREATE INDEX IF NOT EXISTS gate_reverse ON gates(target,left_id,right_id);
        CREATE TABLE IF NOT EXISTS journals(id TEXT PRIMARY KEY, identity TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS assertions(journal TEXT REFERENCES journals(id), assertion TEXT, born INTEGER NOT NULL, retired INTEGER, raw BLOB NOT NULL, PRIMARY KEY(journal,assertion), CHECK(born>=0), CHECK(retired IS NULL OR retired>born));
        CREATE INDEX IF NOT EXISTS assertions_range ON assertions(journal,born,assertion);
        CREATE TABLE IF NOT EXISTS snapshots(id TEXT PRIMARY KEY, journal TEXT REFERENCES journals(id), sequence INTEGER NOT NULL, anchor BLOB NOT NULL);
        CREATE INDEX IF NOT EXISTS snapshot_range ON snapshots(journal,sequence,id);
        CREATE TABLE IF NOT EXISTS leaves(node TEXT REFERENCES nodes(id), snapshot TEXT REFERENCES snapshots(id), scope TEXT NOT NULL, PRIMARY KEY(node,snapshot,scope));
        CREATE INDEX IF NOT EXISTS leaves_reverse ON leaves(snapshot,node,scope);
        CREATE TABLE IF NOT EXISTS emits(node TEXT REFERENCES nodes(id), snapshot TEXT REFERENCES snapshots(id), assertion TEXT NOT NULL, scope TEXT NOT NULL, PRIMARY KEY(node,snapshot,assertion,scope));
        CREATE INDEX IF NOT EXISTS emits_reverse ON emits(snapshot,assertion,scope,node);
        CREATE TABLE IF NOT EXISTS members(id TEXT PRIMARY KEY, binding BLOB NOT NULL);
        CREATE TABLE IF NOT EXISTS roots(member TEXT REFERENCES members(id), node TEXT REFERENCES nodes(id), PRIMARY KEY(member,node));
        CREATE INDEX IF NOT EXISTS roots_reverse ON roots(node,member);
        CREATE TABLE IF NOT EXISTS seal(singleton INTEGER PRIMARY KEY CHECK(singleton=1), manifest BLOB NOT NULL, digest TEXT NOT NULL);
        ''')
        self.counts: Counter[str] = Counter()

    def close(self) -> None:
        self.db.close()

    def __enter__(self) -> FactorIndex:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def exact(self, table: str, values: tuple[object, ...], keys: tuple[str, ...]) -> None:
        if self.db.execute('SELECT 1 FROM seal').fetchone():
            raise ValueError('sealed build is immutable')
        if table not in TABLES:
            raise ValueError('unknown fixed table')
        columns = [r[1] for r in self.db.execute('PRAGMA table_info(' + table + ')')]
        if not set(keys) <= set(columns):
            raise ValueError('unknown key')
        selected = tuple(values[columns.index(c)] for c in keys)
        old = self.db.execute('SELECT * FROM '+table+' WHERE '+' AND '.join(c+'=?' for c in keys), selected).fetchone()
        if old is not None and old != values:
            raise ValueError('changed exact staged record: ' + table)
        if old is None:
            self.db.execute('INSERT INTO '+table+' VALUES ('+','.join('?' for _ in values)+')', values)
            self.counts['inserted_'+table] += 1

    def node(self, kind: str, *parts: object) -> str:
        parts = tuple(str(v) if isinstance(v, str) else v for v in parts)
        coordinate = canonical_json_bytes({'kind': kind, 'parts': list(parts)})
        key = hashlib.sha256(coordinate).hexdigest()
        self.exact('nodes', (key, coordinate), ('id',))
        return key

    def edge(self, source: str, target: str) -> None:
        self.exact('edges', (source, target), ('source', 'target'))

    def gate(self, left: str, right: str, target: str) -> None:
        left, right = sorted((left, right))
        if left == right:
            self.edge(left, target)
        else:
            self.exact('gates', (left, right, target), ('left_id', 'right_id', 'target'))

    def emit(self, node: str, snapshot: str, assertion: str, scope: str) -> None:
        if scope not in SCOPES:
            raise ValueError('invalid scope')
        self.exact('emits', (node, snapshot, assertion, scope), ('node','snapshot','assertion','scope'))

    def leaf(self, node: str, snapshot: str, scope: str = 'recorded-history') -> None:
        if scope not in SCOPES:
            raise ValueError('invalid scope')
        self.exact('leaves', (node, snapshot, scope), ('node','snapshot','scope'))

    def root(self, member: str, binding: dict[str, object], nodes: Iterable[str]) -> None:
        self.exact('members', (str(member), canonical_json_bytes(binding)), ('id',))
        for node in nodes:
            self.exact('roots', (str(member), node), ('member','node'))

    def sizes(self) -> dict[str, int]:
        return {t: self.db.execute('SELECT count(*) FROM '+t).fetchone()[0] for t in TABLES}

    def seal(self, expected: dict[str, int], source: dict[str, object]) -> str:
        """Counts are a model control, NOT production proof of source completeness."""
        if self.db.execute('SELECT 1 FROM seal').fetchone():
            raise ValueError('sealed build is immutable')
        if self.sizes() != expected:
            raise ValueError('incomplete source manifest')
        if self.db.execute('''SELECT 1 FROM emits e JOIN snapshots s ON s.id=e.snapshot
          LEFT JOIN assertions a ON a.journal=s.journal AND a.assertion=e.assertion
          WHERE a.assertion IS NULL OR a.born>s.sequence LIMIT 1''').fetchone():
            raise ValueError('emission not supported by exact snapshot')
        if self.db.execute('PRAGMA foreign_key_check').fetchone():
            raise ValueError('unresolved reference')
        digest = hashlib.sha256(b'riverhog-canonical-discovery-factor-dataset/v1\n')
        data = canonical_json_bytes(source)
        digest.update(len(data).to_bytes(8, 'big')); digest.update(data)
        self.counts['hash_records'] = self.counts['hash_bytes'] = 0
        for table in TABLES:
            info = list(self.db.execute('PRAGMA table_info('+table+')'))
            keys = [r[1] for r in sorted(info, key=lambda r:r[5]) if r[5]]
            for row in self.db.execute('SELECT * FROM '+table+' ORDER BY '+','.join(keys)):
                values = [{'hex': v.hex()} if isinstance(v, bytes) else str(v) if type(v) is int else v for v in row]
                data = canonical_json_bytes({'table': table, 'row': values})
                digest.update(len(data).to_bytes(8, 'big')); digest.update(data)
                self.counts['hash_records'] += 1; self.counts['hash_bytes'] += len(data)
        value = digest.hexdigest()
        self.db.execute('INSERT INTO seal VALUES (1,?,?)', (canonical_json_bytes({'expected':expected,'source':source}),value))
        self.db.commit()
        return value

    def _readable(self) -> None:
        if self.db.execute('SELECT 1 FROM seal').fetchone() is None:
            raise ValueError('unsealed build')

    def reachable(self, member: str) -> set[str]:
        """Disk-backed least fixed point. Final Python set is test convenience."""
        self._readable()
        if not self.db.execute('SELECT 1 FROM members WHERE id=?', (str(member),)).fetchone():
            raise KeyError(member)
        self.db.executescript('''DROP TABLE IF EXISTS temp.seen; DROP TABLE IF EXISTS temp.todo;
          CREATE TEMP TABLE seen(id TEXT PRIMARY KEY); CREATE TEMP TABLE todo(id TEXT PRIMARY KEY);''')
        def add(node: str) -> None:
            if self.db.execute('INSERT OR IGNORE INTO seen VALUES (?)', (node,)).rowcount:
                self.db.execute('INSERT INTO todo VALUES (?)', (node,))
        for (node,) in self.db.execute('SELECT node FROM roots WHERE member=?', (str(member),)):
            add(node)
        while row := self.db.execute('SELECT id FROM todo LIMIT 1').fetchone():
            node = row[0]; self.db.execute('DELETE FROM todo WHERE id=?', (node,))
            self.counts['query_node_visits'] += 1
            for (target,) in self.db.execute('SELECT target FROM edges WHERE source=?', (node,)):
                self.counts['query_edge_visits'] += 1; add(target)
            for other, target in self.db.execute('''SELECT right_id,target FROM gates WHERE left_id=?
              UNION ALL SELECT left_id,target FROM gates WHERE right_id=?''', (node,node)):
                self.counts['query_gate_incidents'] += 1
                if self.db.execute('SELECT 1 FROM seen WHERE id=?', (other,)).fetchone():
                    add(target)
        return {r[0] for r in self.db.execute('SELECT id FROM seen')}

    def logical(self, member: str) -> set[tuple[str, str, str]]:
        """Test-only flat expansion; never use for production build/seal/count."""
        self.reachable(member)
        return set(self.db.execute('''SELECT e.snapshot,e.assertion,e.scope FROM emits e JOIN seen n ON n.id=e.node
          UNION SELECT l.snapshot,a.assertion,l.scope FROM leaves l JOIN seen n ON n.id=l.node
          JOIN snapshots s ON s.id=l.snapshot JOIN assertions a ON a.journal=s.journal AND a.born<=s.sequence'''))

    def _seed_nodes(self, facts: Iterable[tuple[str,str,str]]) -> Iterable[str]:
        for snapshot, assertion, scope in facts:
            yield from (r[0] for r in self.db.execute('''SELECT node FROM emits WHERE snapshot=? AND assertion=? AND scope=?
              UNION SELECT l.node FROM leaves l JOIN snapshots s ON s.id=l.snapshot
              JOIN assertions a ON a.journal=s.journal AND a.born<=s.sequence
              WHERE l.snapshot=? AND a.assertion=? AND l.scope=?''', (snapshot,assertion,scope,snapshot,assertion,scope)))

    def candidates(self, facts: Iterable[tuple[str,str,str]]) -> set[str]:
        """Reverse OR relaxation: guarded matches are candidates, not acceptance."""
        self._readable()
        self.db.executescript('''DROP TABLE IF EXISTS temp.reverse_seen; DROP TABLE IF EXISTS temp.reverse_todo;
          CREATE TEMP TABLE reverse_seen(id TEXT PRIMARY KEY); CREATE TEMP TABLE reverse_todo(id TEXT PRIMARY KEY);''')
        def add(node: str) -> None:
            if self.db.execute('INSERT OR IGNORE INTO reverse_seen VALUES (?)', (node,)).rowcount:
                self.db.execute('INSERT INTO reverse_todo VALUES (?)', (node,))
        for node in self._seed_nodes(facts): add(node)
        while row := self.db.execute('SELECT id FROM reverse_todo LIMIT 1').fetchone():
            node = row[0]; self.db.execute('DELETE FROM reverse_todo WHERE id=?', (node,))
            self.counts['reverse_node_visits'] += 1
            for (parent,) in self.db.execute('SELECT source FROM edges WHERE target=?', (node,)):
                self.counts['reverse_edge_visits'] += 1; add(parent)
            for left, right in self.db.execute('SELECT left_id,right_id FROM gates WHERE target=?', (node,)):
                add(left); add(right)
        return {r[0] for r in self.db.execute('SELECT DISTINCT member FROM roots JOIN reverse_seen ON id=node')}

    def first_support_unguarded(self, facts: Iterable[tuple[tuple, str, str, str]]) -> dict[str, tuple]:
        """Best-first reverse propagation for exact OR-only proof, without N*A rows.

        Priority keys come from the maintained support ordering; input facts must
        already pass the SAME assertion/profile predicate. Python heap/dict are a
        small-fixture model; production uses bounded disk-backed priority state.
        Guarded paths are intentionally not proved by this method.
        """
        self._readable()
        heap = []
        for key, snap, assertion, scope in facts:
            for node in self._seed_nodes([(snap,assertion,scope)]):
                heapq.heappush(heap, (key,node))
        settled = {}; result = {}
        while heap:
            key, node = heapq.heappop(heap)
            if node in settled: continue
            settled[node] = key; self.counts['first_support_nodes'] += 1
            for (member,) in self.db.execute('SELECT member FROM roots WHERE node=?', (node,)):
                result.setdefault(member,key)
            for (parent,) in self.db.execute('SELECT source FROM edges WHERE target=?', (node,)):
                self.counts['first_support_edges'] += 1
                if parent not in settled: heapq.heappush(heap, (key,parent))
        return result
