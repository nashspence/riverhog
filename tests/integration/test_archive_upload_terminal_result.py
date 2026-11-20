from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest
from riverhog_core.catalog_db import catalog_write_contention
from riverhog_core.services.archive_upload_terminal_result import ArchiveUploadTerminalResults
from riverhog_protocol.errors import Conflict
from sqlalchemy.exc import OperationalError

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_upload_terminal_result import _lost_reply, _select, _state
from tests.unit.test_archive_write_admission import _case

pytestmark = pytest.mark.integration


def test_postgres_lost_original_result_commit_reply(database_url, tmp_path):
    with _case(tmp_path, database_url=database_url) as case:
        _lost_reply(case)


def test_postgres_competing_original_results_cannot_replace_the_winner(database_url, tmp_path):
    with _case(tmp_path, database_url=database_url) as case:
        operation = _select(case)
        simultaneous = Barrier(2)

        def prepare(number):
            def guard(session):
                simultaneous.wait(timeout=10)

            try:
                return ArchiveUploadTerminalResults(case.factory).prepare(
                    case.collection_id, operation, {"files": number}, guard=guard
                )
            except Conflict:
                return None
            except OperationalError as exc:
                assert catalog_write_contention(exc)
                return None

        with ThreadPoolExecutor(max_workers=2) as workers:
            results = [f.result(timeout=20) for f in [workers.submit(prepare, n) for n in (13, 29)]]
        received = [value for value in results if value is not None]
        assert len(received) == 1
        assert _state(case, operation) == ("draining", 4096, received[0], None)


@pytest.mark.parametrize("collection_id", (2**53 + 1, 2**63 - 1))
def test_postgres_terminal_control_preserves_large_exact_identity_and_reservation(
    database_url, tmp_path, collection_id
):
    from tests.unit.test_archive_upload_terminal_result import _large_scalar_result

    with _case(tmp_path, database_url=database_url) as case:
        _large_scalar_result(case, collection_id, 2**60)
