from __future__ import annotations

import hashlib

import pytest
from riverhog_core.services import collection_uploads as source_owner
from riverhog_core.services import upload_journal_certificates as owner
from riverhog_core.services import upload_journal_replay as replay
from riverhog_provenance import JournalValidationCapacity
from riverhog_provenance.graph_certificates import InvalidJournalGraphCertificate
from sqlalchemy import delete, event, select, update

from tests.unit.test_journal_capture import retained
from tests.unit.test_journal_epoch_rank import finish as finish_rank
from tests.unit.test_journal_epoch_rank import prepared
from tests.unit.test_journal_epoch_union import edge, raw_edges, uid
from tests.unit.test_journal_topology_capture import entry
from tests.unit.test_staged_journal_range import JOURNAL


def ready(factory):
    work = prepared(factory)
    finish_rank(factory, work)
    catalog = source_owner.admission_provenance_catalog()
    assert replay.prepare_upload_journal_replay(factory, work, catalog=catalog)
    return work, owner.UploadJournalGraphCertificates(factory, work, catalog=catalog)


def proof(source, raw, *, kind="derivation", target=None, **options):
    if target is None:
        target = uid(100)
    return source.edge(
        frame_ordinal=0,
        frame_sha256=hashlib.sha256(raw[1:-1]).hexdigest(),
        kind=kind,
        source=uid(0),
        target=target,
        byte_budget=4096,
        **options,
    )


@pytest.mark.parametrize("kind", ["derivation", "specialization"])
def test_native_rank_point_returns_detached_source_bound_evidence(tmp_path, kind):
    raw = raw_edges([edge(n, kind=kind) for n in range(17)])
    with retained(tmp_path, raw) as (engine, factory):
        work, source = ready(factory)
        queries = []

        def observed(_connection, _cursor, sql, _parameters, _context, _many):
            queries.append(sql)

        event.listen(engine, "before_cursor_execute", observed)
        try:
            result = proof(source, raw, kind=kind)
        finally:
            event.remove(engine, "before_cursor_execute", observed)
        assert result is not None
        assert result.source_rank < result.target_rank < result.vertex_count == 18
        assert result.arc_count == 17 and result.epoch == 0
        assert source.binding.source == replay._binding(
            work, source_owner.admission_provenance_catalog()
        )
        assert engine.pool.checkedout() == 0
        assert len(queries) == 1
        assert not any(".content" in sql for sql in queries)


@pytest.mark.parametrize("fault", ["arc", "rank", "order", "frame", "source", "manifest"])
def test_missing_changed_or_replaced_rank_state_fails_closed(tmp_path, fault):
    raw = raw_edges([edge(n) for n in range(3)])
    with retained(tmp_path, raw) as (engine, factory):
        _work, source = ready(factory)
        with factory() as session, session.begin():
            if fault == "arc":
                session.execute(delete(owner.Arc).where(owner.Arc.target_id == uid(100)))
            elif fault == "rank":
                session.execute(delete(owner.Ranked).where(owner.Ranked.vertex_id == uid(100)))
            elif fault == "order":
                rank = session.scalar(
                    select(owner.Ranked.rank).where(owner.Ranked.vertex_id == uid(0))
                )
                session.execute(
                    update(owner.Ranked).where(owner.Ranked.vertex_id == uid(100)).values(rank=rank)
                )
            elif fault == "frame":
                session.execute(update(owner.Frame).values(epoch_json_sha256="0" * 64))
            elif fault == "source":
                session.execute(update(owner.Journal).values(state="failed"))
            else:
                scan = session.get(owner.Scan, (1, JOURNAL))
                scan.version += 1
        with pytest.raises(InvalidJournalGraphCertificate):
            proof(source, raw)
        assert engine.pool.checkedout() == 0


def test_completed_cyclic_union_selects_native_online_fallback(tmp_path):
    raw = raw_edges([edge(0, 0, 100), edge(1, 100, 0)])
    with retained(tmp_path, raw) as (engine, factory):
        _work, source = ready(factory)
        assert proof(source, raw) is None
        assert engine.pool.checkedout() == 0


def test_speculative_deferred_input_does_not_create_a_graph_rejection(tmp_path):
    raw = raw_edges([edge(0)]) + b"later malformed framing"
    with retained(tmp_path, raw) as (engine, factory):
        _work, source = ready(factory)
        assert (
            source.edge(
                frame_ordinal=0,
                frame_sha256=hashlib.sha256(entry([edge(0)], kind="journal_init")).hexdigest(),
                kind="derivation",
                source=uid(0),
                target=uid(100),
                byte_budget=4096,
            )
            is None
        )
        assert engine.pool.checkedout() == 0


def test_byte_capacity_rejects_before_opening_a_rank_reader(tmp_path, monkeypatch):
    raw = raw_edges([edge(0)])
    with retained(tmp_path, raw) as (engine, factory):
        _work, source = ready(factory)

        def forbidden(*_args, **_kwargs):
            pytest.fail("capacity rejection opened a rank query")

        monkeypatch.setattr(source, "_probe", forbidden)
        with pytest.raises(JournalValidationCapacity):
            source.edge(
                frame_ordinal=0,
                frame_sha256="0" * 64,
                kind="derivation",
                source=uid(0),
                target=uid(100),
                byte_budget=1,
            )
        assert engine.pool.checkedout() == 0
