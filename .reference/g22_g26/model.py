"""SQLite work-step diagnostics, NOT a production validator/index or wire codec.

Fixture integers and byte records stand in for authenticated native source facts.
Every public operation opens/closes its connection; each tick commits <=budget
logical transitions. SQL/index costs and production owner integration are separate.
"""
from __future__ import annotations

import hashlib
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

FRAGMENT = 256  # Diagnostic representation constant, not a product limit/default.


@contextmanager
def db(path: Path) -> Iterator[sqlite3.Connection]:
    connection = sqlite3.connect(path)
    try:
        connection.execute('PRAGMA foreign_keys=ON')
        connection.execute('PRAGMA synchronous=FULL')
        with connection:
            yield connection
    finally:
        connection.close()


class Conflict(ValueError):
    pass


class Crash(RuntimeError):
    pass


class DagWork:
    def __init__(self, path: Path) -> None:
        self.path = path
        with db(path) as c:
            c.executescript('''
            CREATE TABLE IF NOT EXISTS vertex(id INTEGER PRIMARY KEY, degree INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS edge(src INTEGER,dst INTEGER,PRIMARY KEY(src,dst));
            CREATE TABLE IF NOT EXISTS ready(id INTEGER PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS rank(id INTEGER PRIMARY KEY,n INTEGER UNIQUE);
            CREATE TABLE IF NOT EXISTS work(id INTEGER PRIMARY KEY,phase TEXT,cursor INTEGER,
                active INTEGER,last_dst INTEGER,vertices INTEGER,ranked INTEGER,visits INTEGER,
                epoch INTEGER,version INTEGER);
            INSERT OR IGNORE INTO work VALUES(1,'input',-1,NULL,-1,0,0,0,1,0);
            ''')

    def add(self, edges: list[tuple[int,int]], budget: int) -> None:
        if not 0 < len(edges) <= budget:
            raise ValueError('bounded input required')
        with db(self.path) as c:
            if c.execute('SELECT phase FROM work').fetchone()[0] != 'input':
                raise Conflict('input sealed')
            for a,b in edges:
                if min(a,b) < 0:
                    raise ValueError('diagnostic nonnegative vertex IDs')
                for v in (a,b):
                    n = c.execute('INSERT OR IGNORE INTO vertex VALUES(?,0)',(v,)).rowcount
                    c.execute('UPDATE work SET vertices=vertices+?',(n,))
                if c.execute('INSERT OR IGNORE INTO edge VALUES(?,?)',(a,b)).rowcount:
                    c.execute('UPDATE vertex SET degree=degree+1 WHERE id=?',(b,))

    def seal(self) -> None:
        with db(self.path) as c:
            c.execute("UPDATE work SET phase='seed',version=version+1 WHERE phase='input'")

    def takeover(self) -> int:
        with db(self.path) as c:
            c.execute('UPDATE work SET epoch=epoch+1,version=version+1')
            return c.execute('SELECT epoch FROM work').fetchone()[0]

    def status(self) -> tuple:
        with db(self.path) as c:
            return c.execute('SELECT phase,vertices,ranked,visits,epoch,version FROM work').fetchone()

    def step(self, budget: int, *, epoch: int = 1, crash: bool = False) -> int:
        if budget < 1:
            raise ValueError('positive budget required')
        steps = 0
        with db(self.path) as c:
            if c.execute('SELECT epoch FROM work').fetchone()[0] != epoch:
                raise Conflict('stale worker')
            while steps < budget:
                phase,cur,active,last,vertices,ranked = c.execute(
                    'SELECT phase,cursor,active,last_dst,vertices,ranked FROM work').fetchone()
                if phase in ('input','done','cyclic'):
                    break
                if phase == 'seed':
                    row = c.execute('SELECT id,degree FROM vertex WHERE id>? ORDER BY id LIMIT 1',(cur,)).fetchone()
                    if row is None:
                        c.execute("UPDATE work SET phase='rank'")
                    else:
                        v,degree=row
                        if degree == 0:
                            c.execute('INSERT INTO ready VALUES(?)',(v,))
                        c.execute('UPDATE work SET cursor=?',(v,))
                elif active is None:
                    row=c.execute('SELECT id FROM ready ORDER BY id LIMIT 1').fetchone()
                    if row is None:
                        c.execute('UPDATE work SET phase=?',('done' if ranked == vertices else 'cyclic',))
                    else:
                        v=row[0]
                        c.execute('DELETE FROM ready WHERE id=?',(v,))
                        c.execute('INSERT INTO rank VALUES(?,?)',(v,ranked))
                        c.execute('UPDATE work SET active=?,last_dst=-1,ranked=ranked+1',(v,))
                else:
                    row=c.execute('SELECT dst FROM edge WHERE src=? AND dst>? ORDER BY dst LIMIT 1',(active,last)).fetchone()
                    if row is None:
                        c.execute('UPDATE work SET active=NULL')
                    else:
                        v=row[0]
                        c.execute('UPDATE vertex SET degree=degree-1 WHERE id=?',(v,))
                        if c.execute('SELECT degree FROM vertex WHERE id=?',(v,)).fetchone()[0] == 0:
                            c.execute('INSERT INTO ready VALUES(?)',(v,))
                        c.execute('UPDATE work SET last_dst=?,visits=visits+1',(v,))
                steps+=1
            c.execute('UPDATE work SET version=version+1')
            if crash:
                raise Crash('rollback before checkpoint commit')
        return steps


def frame(raw: bytes) -> bytes:
    return str(len(raw)).encode('ascii') + b':' + raw


def leaf(key: tuple[int,int], raw: bytes) -> bytes:
    encoded = frame(str(key[0]).encode())+frame(str(key[1]).encode())+frame(raw)
    return hashlib.sha256(b'\x00'+encoded).digest()


def node(left: bytes, right: bytes) -> bytes:
    return hashlib.sha256(b'\x01'+left+right).digest()


class FactWork:
    def __init__(self,path: Path, postings: int | None = None) -> None:
        self.path=path
        with db(path) as c:
            c.executescript('''
            CREATE TABLE IF NOT EXISTS posting(id INTEGER PRIMARY KEY,total INTEGER,next_part INTEGER,received INTEGER);
            CREATE TABLE IF NOT EXISTS fragment(posting INTEGER,part INTEGER,raw BLOB,
                PRIMARY KEY(posting,part),FOREIGN KEY(posting) REFERENCES posting(id));
            CREATE TABLE IF NOT EXISTS frontier(level INTEGER PRIMARY KEY,digest BLOB);
            CREATE TABLE IF NOT EXISTS work(id INTEGER PRIMARY KEY,expected INTEGER,completed INTEGER,
                phase TEXT,p INTEGER,part INTEGER,count INTEGER,carry BLOB,level INTEGER,folded BLOB,
                epoch INTEGER,version INTEGER);
            ''')
            if postings is not None:
                if postings < 0:
                    raise ValueError('negative posting count')
                c.execute("INSERT INTO work VALUES(1,?,0,'input',-1,-1,0,NULL,0,NULL,1,0)",(postings,))

    def stage(self, rows: list[tuple[int,int,bytes,int]], *, row_budget: int,
              byte_budget: int, epoch: int = 1, crash: bool = False) -> None:
        if not rows or len(rows)>row_budget or sum(len(r[2]) for r in rows)>byte_budget:
            raise ValueError('work-unit budget exceeded, split and resume')
        with db(self.path) as c:
            phase,expected,current=c.execute('SELECT phase,expected,epoch FROM work').fetchone()
            if phase!='input' or epoch!=current:
                raise Conflict('closed input or stale worker')
            for p,part,raw,total in rows:
                if type(raw) is not bytes or not 0 <= p < expected or total < 0 or part < 0:
                    raise ValueError('invalid diagnostic fragment')
                parts=max(1,(total+FRAGMENT-1)//FRAGMENT)
                want=0 if total==0 else min(FRAGMENT,total-part*FRAGMENT)
                if part>=parts or len(raw)!=want:
                    raise ValueError('noncanonical fragment boundary')
                head=c.execute('SELECT total,next_part,received FROM posting WHERE id=?',(p,)).fetchone()
                if head is None:
                    if part!=0:
                        raise Conflict('missing posting prefix')
                    c.execute('INSERT INTO posting VALUES(?,?,0,0)',(p,total)); head=(total,0,0)
                if head[0]!=total:
                    raise Conflict('changed source header')
                if part < head[1]:
                    old=c.execute('SELECT raw FROM fragment WHERE posting=? AND part=?',(p,part)).fetchone()
                    if old is None or old[0]!=raw:
                        raise Conflict('changed retry')
                    continue
                if part != head[1]:
                    raise Conflict('fragment gap')
                c.execute('INSERT INTO fragment VALUES(?,?,?)',(p,part,raw))
                c.execute('UPDATE posting SET next_part=next_part+1,received=received+? WHERE id=?',(len(raw),p))
                if part+1 == parts:
                    if head[2]+len(raw)!=total:
                        raise Conflict('incomplete posting')
                    c.execute('UPDATE work SET completed=completed+1')
            c.execute('UPDATE work SET version=version+1')
            if crash:
                raise Crash('rolled back fragments and watermark')

    def seal(self) -> None:
        with db(self.path) as c:
            expected,completed,phase=c.execute('SELECT expected,completed,phase FROM work').fetchone()
            if expected!=completed or phase!='input':
                raise Conflict('source terminal incomplete')
            c.execute("UPDATE work SET phase='hash',version=version+1")

    def step(self,budget: int, *, epoch: int = 1,crash: bool = False) -> int:
        if budget<1:
            raise ValueError('positive budget required')
        n=0
        with db(self.path) as c:
            if c.execute('SELECT epoch FROM work').fetchone()[0]!=epoch:
                raise Conflict('stale worker')
            while n<budget:
                phase,p,part,carry,level,folded=c.execute('SELECT phase,p,part,carry,level,folded FROM work').fetchone()
                if phase in ('input','done'):
                    break
                if carry is not None:
                    old=c.execute('SELECT digest FROM frontier WHERE level=?',(level,)).fetchone()
                    if old is None:
                        c.execute('INSERT INTO frontier VALUES(?,?)',(level,carry))
                        c.execute('UPDATE work SET carry=NULL,level=0')
                    else:
                        c.execute('DELETE FROM frontier WHERE level=?',(level,))
                        c.execute('UPDATE work SET carry=?,level=level+1',(node(old[0],carry),))
                elif phase=='hash':
                    # Disjoint prefix seeks avoid SQLite's partial tuple-seek trap.
                    row=c.execute('SELECT posting,part,raw FROM fragment WHERE posting=? AND part>? ORDER BY part LIMIT 1',(p,part)).fetchone()
                    if row is None:
                        row=c.execute('SELECT posting,part,raw FROM fragment WHERE posting>? ORDER BY posting,part LIMIT 1',(p,)).fetchone()
                    if row is None:
                        c.execute("UPDATE work SET phase='fold'")
                    else:
                        p,part,raw=row
                        c.execute('UPDATE work SET p=?,part=?,carry=?,level=0,count=count+1',(p,part,leaf((p,part),raw)))
                else:
                    row=c.execute('SELECT level,digest FROM frontier ORDER BY level LIMIT 1').fetchone()
                    if row is None:
                        c.execute("UPDATE work SET phase='done',folded=?",(folded if folded is not None else hashlib.sha256(b'').digest(),))
                    else:
                        level,digest=row
                        c.execute('DELETE FROM frontier WHERE level=?',(level,))
                        c.execute('UPDATE work SET folded=?',(digest if folded is None else node(digest,folded),))
                n+=1
            c.execute('UPDATE work SET version=version+1')
            if crash:
                raise Crash('rolled back Merkle carry/checkpoint')
        return n

    def status(self) -> tuple:
        with db(self.path) as c:
            return c.execute('SELECT phase,completed,count,carry,level,folded,version FROM work').fetchone()

    def root(self) -> bytes:
        with db(self.path) as c:
            phase,n,root,expected=c.execute('SELECT phase,count,folded,expected FROM work').fetchone()
            if phase!='done':
                raise Conflict('not complete')
            return hashlib.sha256(b'g26-model-dataset/v1\0'+frame(str(expected).encode())+
                                  frame(str(n).encode())+root).digest()
