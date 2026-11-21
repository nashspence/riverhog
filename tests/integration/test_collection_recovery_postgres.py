from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord
from riverhog_protocol.errors import Conflict
from sqlalchemy import delete, text

from tests.integration.test_immutable_archive_preparation_concurrency import (
    database_url as database_url,
)
from tests.unit.test_collection_recovery_accounting import (
    NS,
    _close_gate,
    _read,
    _receive,
    _reserve,
    _root_case,
    _select,
)

pytestmark = pytest.mark.integration


def test_postgres_root_cas_competitors_cannot_spend_the_same_snapshot(database_url, tmp_path):
    with _root_case(tmp_path, database_url=database_url) as case:
        barrier = Barrier(2)

        def attempt(domain):
            barrier.wait(timeout=10)
            try:
                return _reserve(case, domain=domain, revision=0)[0].reason
            except Conflict:
                return "stale-root"

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(attempt, ("native", "corpus")))
        assert sorted(results) == ["checkpoint-admitted", "stale-root"]
        root = _read(case)
        assert root.revision == root.next_grant_sequence == 1
        assert root.accounting.reserved_ns == 10 * NS
        assert root.accounting.held_count_slots == 1


@pytest.mark.parametrize("after_root_write", [False, True])
def test_postgres_domain_selector_and_root_cost_count_share_one_commit(
    database_url, tmp_path, monkeypatch, after_root_write
):
    with _root_case(tmp_path, database_url=database_url) as case:
        _, grant, before = _reserve(case)
        original = case.recovery._save

        def interrupted(*args):
            if after_root_write:
                original(*args)
            raise RuntimeError("selection interrupted")

        with monkeypatch.context() as patch:
            patch.setattr(case.recovery, "_save", interrupted)
            with pytest.raises(RuntimeError, match="interrupted"):
                _select(case, grant)
        assert _read(case) == before
        with session_scope(case.factory) as session:
            assert session.scalar(text("SELECT count(*) FROM recovery_domain_fixture")) == 0
        selected = _select(case, grant)
        assert selected.accounting.successful_checkpoints == 1
        assert selected.accounting.charged_ns == 8 * NS
        assert selected.accounting.reserved_ns == 2 * NS


def test_postgres_original_receiving_survives_gate_close_and_source_retirement(
    database_url, tmp_path
):
    with _root_case(tmp_path, database_url=database_url) as case:
        _, grant, _ = _reserve(case)
        selected = _select(case, grant)
        _close_gate(case)
        with session_scope(case.factory) as session:
            session.execute(
                delete(CollectionUploadRecord).where(
                    CollectionUploadRecord.collection_id == case.root_id
                )
            )
        assert _select(case, grant, revision=0) == selected
        status, received = _receive(case, grant, actual=11 * NS, outcome="published")
        assert status == "received"
        assert received.grants == ()
        assert received.accounting.charged_ns == 11 * NS
        assert received.accounting.successful_checkpoints == 1
        assert _receive(case, grant, actual=11 * NS, outcome="published", revision=0) == (
            "already-received",
            received,
        )
        with session_scope(case.factory) as session:
            assert (
                session.scalar(text("SELECT generation FROM recovery_domain_fixture"))
                == "generation-a"
            )
