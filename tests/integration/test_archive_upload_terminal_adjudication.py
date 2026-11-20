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
from tests.unit.test_archive_upload_terminal_adjudication import (
    _decision,
    _original_receiving,
    _phase,
    _result,
)
from tests.unit.test_archive_upload_terminal_authority import _discard_case
from tests.unit.test_archive_write_admission import _case, _selected

pytestmark = pytest.mark.integration


def test_postgres_global_decision_requires_all_original_receiving(database_url, tmp_path):
    with _case(tmp_path, database_url=database_url) as case:
        _original_receiving(case)


def test_postgres_global_decision_is_single_selection_under_concurrent_attempts(
    database_url, tmp_path
):
    with _case(tmp_path, database_url=database_url) as case:
        operation = _selected(case)
        _result(case, operation)
        simultaneous = Barrier(2)

        def guard(session):
            simultaneous.wait(timeout=10)

        with ThreadPoolExecutor(max_workers=2) as workers:
            results = [
                f.result(timeout=20)
                for f in [workers.submit(_decision, case, operation, guard) for _ in range(2)]
            ]
        assert any(r.phase == "cleanup-ready" for r in results)
        assert all(
            r.phase == "cleanup-ready" or r.phase is None and r.pending_reason == "catalog-busy"
            for r in results
        )
        assert _phase(case, operation)[:2] == ("cleanup-ready", "discarding")


@pytest.mark.parametrize("resource", ("key", "parent"))
def test_postgres_adjudication_retains_current_authority_and_defers_busy_original_rows(
    database_url, tmp_path, resource
):
    with _discard_case(tmp_path, database_url=database_url) as case:
        with session_scope(case.factory) as session:
            session.get(CollectionUploadRecord, case.collection_id).state = "orphaned"
        guard = case.service._terminal_write_guard(case.collection_id, case.operation)
        ArchiveUploadTerminalResults(case.factory).prepare(
            case.collection_id,
            case.operation,
            {"status": "discarded", "collection_id": str(case.collection_id)},
            guard=guard,
        )
        model, identity = (
            (AppKeyRecord, case.principal.key_id)
            if resource == "key"
            else (CollectionUploadRecord, case.collection_id)
        )
        with ThreadPoolExecutor(max_workers=1) as workers:
            with session_scope(case.factory) as session:
                session.get(model, identity, with_for_update=True)
                decision = workers.submit(_decision, case, case.operation, guard).result(timeout=5)
                assert decision.phase is None and decision.pending_reason == "catalog-busy"
        assert _phase(case, case.operation)[:2] == ("draining", "orphaned")
        assert _decision(case, case.operation, guard).phase == "cleanup-ready"
