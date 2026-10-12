"""Native semantic differential tests plus explicitly separate lifecycle model."""
from __future__ import annotations

import random
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, create_engine, select
from riverhog_core.browse import keyset_statement
from riverhog_core.canonical_discovery_extraction import assertion_clause_matches, assertion_postings
from riverhog_protocol.artifact_discovery import AssertionClause
from riverhog_protocol.errors import BadRequest
from reference import ProjectionStore, ReadChanged, Unavailable, LostLease, RebuildRequired, seek, clause_matches_once

PIN = {"contract_id": "urn:example:profile", "contract_sha256": "a" * 64, "schema_id": "metadata"}


def clause(*values, profile=PIN, **kw):
    return AssertionClause.model_validate({"profile": profile, "values": list(values), **kw})


def v(value, operator="equals", **kw):
    return {"value": value, "operator": operator, **kw}


def row(values, kind="observation"):
    return {"type": kind, "profiles": [{"profile": PIN, "data": item} for item in values]}


@pytest.mark.parametrize("order", ["asc", "desc"])
@pytest.mark.parametrize("nullable", [False, True])
@pytest.mark.parametrize("size", [1, 7, 100])
def test_seek_native_order_equivalence(order, nullable, size):
    engine = create_engine("sqlite://")
    try:
        meta = MetaData()
        t = Table("items", meta, Column("k", Integer, nullable=nullable), Column("id", Integer, primary_key=True))
        meta.create_all(engine)
        rows = [{"k": None if nullable and i % 5 == 0 else i % 9, "id": i} for i in range(1, 124)]
        with engine.begin() as db:
            db.execute(t.insert(), rows)
        with engine.connect() as db:
            expected = sorted([(x["k"], x["id"]) for x in rows],
                              key=lambda x: ((x[0] is None, x[0] if x[0] is not None else 0), x[1]),
                              reverse=order == "desc")
            pos, result = None, []
            while True:
                args = dict(columns=(t.c.k, t.c.id), position=pos, order=order, page_size=size)
                original = db.execute(keyset_statement(select(t), **args)).all()
                candidate = db.execute(seek(select(t), nonnull=not nullable, **args)).all()
                assert original == candidate
                result.extend(map(tuple, candidate[:size]))
                if len(candidate) <= size:
                    break
                pos = tuple(candidate[size - 1])
            assert result == expected
    finally:
        engine.dispose()


@pytest.mark.parametrize("kwargs", [{"page_size": 0}, {"order": "invalid"}, {"position": (1,2)}])
def test_seek_invalid_input_native_error(kwargs):
    t = Table("t", MetaData(), Column("id", Integer, primary_key=True))
    args = dict(columns=(t.c.id,), position=None, order="asc", page_size=3)
    args.update(kwargs)
    with pytest.raises(BadRequest):
        seek(select(t), nonnull=True, **args)


@pytest.mark.parametrize("state", ["effective", "retracted"])
@pytest.mark.parametrize("profile", [PIN, None])
def test_matcher_seeded_native_differential(state, profile):
    rng = random.Random(9642728)
    scalars = [None, False, True, 0, 1, 3, "AbC", "abc", "Ä", "ä", "x%_y", "a/b", "~"]
    for _ in range(120):
        r = row([{f"key/{j}~": rng.choice(scalars) for j in range(rng.randrange(1,6))}
                 for _ in range(rng.randrange(0,15))])
        values = []
        for _ in range(rng.randrange(1,5)):
            value = rng.choice(scalars[1:])
            values.append(v(value, text_mode=rng.choice(["exact", "ascii-fold"]) if type(value) is str else "exact"))
        c = clause(*values, profile=profile, assertion_state=rng.choice(["effective", "retracted", "any"]))
        assert clause_matches_once(r,c,assertion_state=state) == assertion_clause_matches(r,c,assertion_state=state)


def test_predicates_cannot_cross_profile_sections():
    r = row([{"a":"left"},{"b":"right"}])
    c = clause(v("left"),v("right"))
    assert clause_matches_once(r,c,assertion_state="effective") is None
    assert clause_matches_once(r,clause(v("left"),v("right"),profile=None),assertion_state="effective") is not None


def test_declared_order_not_first_completed_group():
    r = {"type":"activity", "details":[{"profile":PIN,"data":{"x":"yes"}}],
         "configuration":{"profile":PIN,"data":{"x":"yes"}}}
    c = clause(v("yes"))
    got = clause_matches_once(r,c,assertion_state="effective")
    assert got == assertion_clause_matches(r,c,assertion_state="effective")
    assert got[0].pointer == "/details/0/data/x"  # configuration is traversed first


@pytest.mark.parametrize("predicate", [v("AbC", "contains"),v("abc", "contains",text_mode="ascii-fold"),
    v("a%_b", "contains"),v("ä", "contains"),v(True),v(1),v("AbC",pointer="/profiles/0/data/a~1b~0")])
def test_literal_type_pointer_native_parity(predicate):
    r = row([{"a/b~":"AbC a%_b Ä", "flag":True, "number":1}])
    c = clause(predicate)
    assert clause_matches_once(r,c,assertion_state="effective") == assertion_clause_matches(r,c,assertion_state="effective")


def test_one_posting_pass_no_profile_by_posting_product():
    r = row([{"v":"not-the-needle"} for _ in range(512)])
    c = clause(v("absent"))
    counts = {}
    assert clause_matches_once(r,c,assertion_state="effective",counts=counts) is None
    assert counts["sections"] == 512
    assert counts["postings"] == len(tuple(assertion_postings(r)))
    assert counts["predicate_calls"] == 512


@pytest.fixture
def store(tmp_path):
    s = ProjectionStore(str(tmp_path / "query.sqlite3"))
    s.put_catalog(1); s.put_catalog(2)
    try:
        yield s
    finally:
        s.close()


def prepared(s, *, selected=None, rows=5, expires=1000):
    a,r = s.start("k","q","p",selected=selected,expires=expires)
    items = [(1, f"a{i:04}", "support") for i in range(rows)]
    s.append("k",a,items,max_rows=100)
    s.seal("k",a,selected=selected,terminal=True,expected_count=rows)
    return a,r,items


def page(s,r,**kw):
    return s.page("k","q","p",authorized=True,expected_read=r,**kw)


def test_complete_projection_pages_no_manifest_rescan(store):
    a,r,expected = prepared(store)
    before = store.counts["manifest_rows"]
    result=[]; pos=(0,"")
    while True:
        p=page(store,r,after=pos)
        result.extend(p["items"])
        if p["complete"]:
            assert p["next"] is None
            break
        pos=p["next"]
    assert result == expected
    assert store.counts["manifest_rows"] == before


def test_unrelated_mutation_revalidates_without_false_conflict(store):
    _,r,_=prepared(store,selected=(1,))
    store.put_catalog(2,tag=9)
    before=store.counts["manifest_rows"]
    with pytest.raises(RebuildRequired):
        page(store,r,selected=(1,))
    assert store.counts["manifest_rows"] == before+1
    _,new_read,_=prepared(store,selected=(1,))
    assert new_read==r  # internal rebuild, not a new public stale-cursor error
    before=store.counts["manifest_rows"]
    assert page(store,r,selected=(1,))["items"]
    assert store.counts["manifest_rows"]==before


@pytest.mark.parametrize("change", ["tag", "generation", "insert", "delete", "source"])
def test_relevant_mutation_conflicts(store,change):
    _,r,_=prepared(store)
    if change=="tag": store.put_catalog(1,tag=7)
    if change=="generation": store.put_catalog(1,generation="new")
    if change=="insert": store.put_catalog(3)
    if change=="delete":
        with store.db: store.db.execute("DELETE FROM catalog WHERE id=1")
    if change=="source":
        with store.db: store.db.execute("UPDATE fence SET source='new-source' WHERE id=1")
    with pytest.raises(ReadChanged): page(store,r)


def test_authorization_checked_on_unchanged_cache_hit(store):
    _,r,_=prepared(store)
    with pytest.raises(PermissionError):
        store.page("k","q","p",authorized=False,expected_read=r)
    with pytest.raises(ReadChanged):
        store.page("k","q","other-key",authorized=True,expected_read=r)


def test_incomplete_zero_and_capacity_never_look_complete(store):
    a,r=store.start("k","q","p")
    with pytest.raises(Unavailable): page(store,r)
    with pytest.raises(Unavailable): store.seal("k",a,terminal=False,expected_count=0)
    with pytest.raises(Unavailable): store.append("k",a,[(1,"x","s"),(1,"y","s")],max_rows=1)
    assert store.db.execute("SELECT count(*) FROM result").fetchone()[0]==0
    store.seal("k",a,terminal=True,expected_count=0)
    assert page(store,r)=={"items":[],"next":None,"complete":True}


def test_exact_replay_and_old_lease_rejected(store):
    a,r=store.start("k","q","p")
    store.append("k",a,[(1,"x","s")],max_rows=2)
    store.append("k",a,[(1,"x","s")],max_rows=2)
    with pytest.raises(ValueError): store.append("k",a,[(1,"x","different")],max_rows=2)
    b,r=store.start("k","q","p",expected_read=r)
    assert b>a
    with pytest.raises(LostLease): store.append("k",a,[(1,"y","s")],max_rows=2)
    with pytest.raises(LostLease): store.seal("k",a,terminal=True,expected_count=1)


def test_build_mutation_rejected_before_ready(store):
    a,r=store.start("k","q","p")
    store.put_catalog(1,tag=99)
    with pytest.raises(ReadChanged): store.seal("k",a,terminal=True,expected_count=0)
    with pytest.raises(Unavailable): page(store,r)


def test_rollback_restores_fence_and_catalog_together(store):
    before=store.stamp()
    store.db.execute("BEGIN")
    store.db.execute("UPDATE catalog SET tag_revision=77 WHERE id=1")
    assert store.stamp()[1]>before[1]
    store.db.rollback()
    assert store.stamp()==before
    assert store.db.execute("SELECT tag_revision FROM catalog WHERE id=1").fetchone()[0]==0


def test_reopen_expiry_and_rebuild(store,tmp_path):
    _,r,expected=prepared(store,expires=10)
    other=ProjectionStore(str(tmp_path/"query.sqlite3"))
    try:
        assert page(other,r)["items"]==expected[:2]
        with pytest.raises(Unavailable): page(other,r,now=11)
        a,r2=other.start("k","q","p",expected_read=r,expires=20)
        assert r2==r
        other.append("k",a,expected,max_rows=100)
        other.seal("k",a,terminal=True,expected_count=len(expected))
        assert page(other,r,now=11)["items"]==expected[:2]
    finally:
        other.close()


@pytest.mark.parametrize("operator,value", [("bytes-equals","QUJD"),("equals","ABC"),("contains","BC")])
def test_declared_bytes_representations(operator,value):
    r = {"type":"delivery_association", "slot":{"kind":"bytes","bytes":{"data":"QUJD"},"encoding":"utf-8"}}
    c = clause(v(value,operator,pointer="/slot"),profile=None)
    got = clause_matches_once(r,c,assertion_state="effective")
    assert got == assertion_clause_matches(r,c,assertion_state="effective")
    assert got is not None


@pytest.mark.parametrize("order", ["asc", "desc"])
@pytest.mark.parametrize("n", [512, 2048])
def test_nonnull_text_tie_key_complete_seek_work(order, n):
    """All leading keys equal: only a full tuple seek passes this work control."""
    from sqlalchemy import Text
    from sqlalchemy.dialects import sqlite
    engine = create_engine("sqlite://")
    try:
        meta = MetaData()
        t = Table("tied", meta, Column("k", Integer, nullable=False),
                  Column("id", Text, primary_key=True))
        meta.create_all(engine)
        with engine.begin() as con:
            con.exec_driver_sql("CREATE INDEX tied_key ON tied(k,id)")
            con.execute(t.insert(), [{"k":0,"id":f"{i:064x}"} for i in range(1,n+1)])
        with engine.connect() as con:
            db=con.connection.driver_connection
            work=[0]; result=[]; pos=None
            def tick():
                work[0]+=1
                return 0
            try:
                while True:
                    statement=seek(select(t),columns=(t.c.k,t.c.id),position=pos,
                                   order=order,page_size=25,nonnull=True)
                    sql=str(statement.compile(dialect=sqlite.dialect(),compile_kwargs={"literal_binds":True}))
                    assert "NULLS" not in sql
                    db.set_progress_handler(tick,1)
                    batch=db.execute(sql).fetchall()
                    db.set_progress_handler(None,0)
                    result.extend(batch[:25])
                    if len(batch)<=25: break
                    pos=batch[24]
            finally:
                db.set_progress_handler(None,0)
            expected=[(0,f"{i:064x}") for i in range(1,n+1)]
            assert result==sorted(expected,reverse=order=="desc")
            assert work[0] <= 80*n+1000  # SQLite component envelope, not a universal backend guarantee
    finally:
        engine.dispose()


@pytest.mark.parametrize("order", ["asc", "desc"])
@pytest.mark.parametrize("n", [512, 2048])
def test_rowid_prefix_partition_seek_work(order, n):
    """Equality-prefix partitions avoid the observed rowid tuple partial seek."""
    from sqlalchemy.dialects import sqlite
    from reference import seek_partitions
    engine=create_engine("sqlite://")
    try:
        meta=MetaData()
        t=Table("rowid_ties",meta,Column("k",Integer,nullable=False),Column("id",Integer,primary_key=True))
        meta.create_all(engine)
        with engine.begin() as con:
            con.exec_driver_sql("CREATE INDEX rowid_key ON rowid_ties(k,id)")
            con.execute(t.insert(),[{"k":i//512,"id":i} for i in range(1,n+1)])
        with engine.connect() as con:
            db=con.connection.driver_connection
            work=[0];result=[];pos=None
            def tick():
                work[0]+=1
                return 0
            try:
                while True:
                    batch=[]
                    for statement in seek_partitions(select(t),columns=(t.c.k,t.c.id),position=pos,
                                                      order=order,page_size=25,nonnull=True):
                        sql=str(statement.compile(dialect=sqlite.dialect(),compile_kwargs={"literal_binds":True}))
                        db.set_progress_handler(tick,1)
                        batch.extend(db.execute(sql).fetchall())
                        db.set_progress_handler(None,0)
                        if len(batch)>=26: break
                    batch=batch[:26]
                    result.extend(batch[:25])
                    if len(batch)<=25: break
                    pos=batch[24]
            finally:
                db.set_progress_handler(None,0)
            assert result==sorted([(i//512,i) for i in range(1,n+1)],reverse=order=="desc")
            assert work[0]<=120*n+1000
    finally:
        engine.dispose()
