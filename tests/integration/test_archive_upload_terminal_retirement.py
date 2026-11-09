from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import AppKeyRecord, CollectionUploadRecord
from riverhog_core.services.archive_upload_terminal_result import ArchiveUploadTerminalResults

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_upload_terminal_adjudication import _decision
from tests.unit.test_archive_upload_terminal_authority import _discard_case
from tests.unit.test_archive_upload_terminal_retirement import (
    _bounded_retirement,
    _drain,
    _failed_forward,
    _ready_original,
    _retained,
    _retire,
    _unstarted_input,
)
from tests.unit.test_archive_write_admission import _case

pytestmark = pytest.mark.integration


def test_postgres_retirement_respects_one_bounded_original_allowance(database_url, tmp_path):
    with _case(tmp_path, database_url=database_url) as case:
        _bounded_retirement(case)


@pytest.mark.parametrize("unknown", (False, True))
def test_postgres_exact_original_receiving_owns_unstarted_input_withdrawal(
    database_url, tmp_path, monkeypatch, unknown
):
    with _case(tmp_path, database_url=database_url) as case:
        _unstarted_input(case, monkeypatch, unknown=unknown)


def test_postgres_concurrent_retirement_releases_each_original_once(database_url, tmp_path):
    with _case(tmp_path, database_url=database_url) as case:
        _, operation = _ready_original(case, "metadata")
        simultaneous = Barrier(2)

        def guard(session):
            simultaneous.wait(timeout=10)

        with ThreadPoolExecutor(max_workers=2) as workers:
            jobs = [workers.submit(_retire, case, operation, guard) for _ in range(2)]
            results = [job.result(timeout=20) for job in jobs]
        assert sum(r.retired for r in results) == 1
        assert all(
            r.pending_reason in {None, "catalog-busy", "retained-originals"} for r in results
        )
        assert _retained(case) == 0


@pytest.mark.parametrize("resource", ("key", "parent"))
def test_postgres_retirement_holds_current_authority_and_original_parent(
    database_url, tmp_path, resource
):
    with _discard_case(tmp_path, database_url=database_url) as case:
        registry = _failed_forward(case, "metadata")
        with session_scope(case.factory) as session:
            session.get(CollectionUploadRecord, case.collection_id).state = "orphaned"
        guard = case.service._terminal_write_guard(case.collection_id, case.operation)
        ArchiveUploadTerminalResults(case.factory).prepare(
            case.collection_id,
            case.operation,
            {"status": "discarded", "collection_id": str(case.collection_id)},
            guard=guard,
        )
        _drain(case, registry, case.operation)
        assert _decision(case, case.operation, guard).phase == "cleanup-ready"
        model, identity = (
            (AppKeyRecord, case.principal.key_id)
            if resource == "key"
            else (CollectionUploadRecord, case.collection_id)
        )
        with ThreadPoolExecutor(max_workers=1) as workers:
            with session_scope(case.factory) as session:
                session.get(model, identity, with_for_update=True)
                result = workers.submit(_retire, case, case.operation, guard).result(timeout=5)
        assert result.phase is None and result.pending_reason == "catalog-busy"
        assert _retained(case) == 1
        assert _retire(case, case.operation, guard).retired == 1
