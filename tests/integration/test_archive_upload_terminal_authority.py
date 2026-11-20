from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from riverhog_core.app_permissions import CATALOG_READ, COLLECTIONS_DELETE, ApplicationAccess
from riverhog_core.catalog_db import catalog_write_contention, session_scope
from riverhog_core.catalog_models import AppKeyRecord, CollectionUploadRecord
from riverhog_protocol.errors import NotFound
from sqlalchemy import event, text
from sqlalchemy.exc import OperationalError

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_app_keys import BOOTSTRAP
from tests.unit.test_archive_upload_terminal_authority import _discard_case, _observe, _unchanged

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("warm", (False, True))
@pytest.mark.parametrize("resource", ("key", "parent"))
def test_postgres_cold_terminal_authority_defers_contended_original_locks(
    database_url, tmp_path, monkeypatch, warm, resource
):
    with _discard_case(tmp_path, database_url=database_url) as case:
        guard = case.service._terminal_write_guard(case.collection_id, case.operation)
        if warm:
            assert _observe(case, guard).records.operation_id == case.operation
        probes = []
        original = case.service._archive_finalizers.local_terminal_status

        def probe(*args, **kwargs):
            probes.append(True)
            return original(*args, **kwargs)

        monkeypatch.setattr(case.service._archive_finalizers, "local_terminal_status", probe)
        model, identity = (
            (AppKeyRecord, case.principal.key_id)
            if resource == "key"
            else (CollectionUploadRecord, case.collection_id)
        )
        with ThreadPoolExecutor(max_workers=1) as workers:
            with session_scope(case.factory) as holder:
                holder.get(model, identity, with_for_update=True)
                attempt = workers.submit(_observe, case, guard)
                with pytest.raises(OperationalError) as failed:
                    attempt.result(timeout=5)
                assert catalog_write_contention(failed.value) and not probes
        _unchanged(case)
        assert _observe(case, guard).records.operation_id == case.operation
        assert probes == [True]


@pytest.mark.parametrize("warm", (False, True))
def test_postgres_terminal_permission_reload_retains_the_key_against_grant_mutation(
    database_url, tmp_path, warm
):
    with _discard_case(tmp_path, database_url=database_url) as case:
        guard = case.service._terminal_write_guard(case.collection_id, case.operation)
        if warm:
            assert _observe(case, guard).records.operation_id == case.operation
        lane = threading.local()
        waiting = threading.Event()
        pids = {}
        engine = case.factory.kw["bind"]

        def capture(connection, cursor, statement, parameters, context, executemany):
            if (
                getattr(lane, "mutation", False)
                and "FROM app_keys" in statement
                and "FOR UPDATE" in statement
            ):
                cursor.execute("SELECT pg_backend_pid()")
                pids["mutation"] = cursor.fetchone()[0]
                waiting.set()

        def mutate():
            lane.mutation = True
            return case.keys.replace_access(
                app=case.principal.id,
                key_id=case.principal.key_id,
                access=(ApplicationAccess(CATALOG_READ),),
                grantor=BOOTSTRAP,
            )

        def blocked():
            deadline = time.monotonic() + 5
            with engine.connect() as observer:
                while time.monotonic() < deadline:
                    if pids["terminal"] in observer.scalar(
                        text("SELECT pg_blocking_pids(:pid)"), {"pid": pids["mutation"]}
                    ):
                        return True
                    time.sleep(0.01)
            return False

        event.listen(engine, "before_cursor_execute", capture)
        try:
            with ThreadPoolExecutor(max_workers=1) as workers:
                with session_scope(case.factory) as session:
                    observed = case.service._archive_terminal_observation.observe(
                        session, case.collection_id, case.operation, guard=guard
                    )
                    assert observed.records.operation_id == case.operation
                    pids["terminal"] = session.scalar(text("SELECT pg_backend_pid()"))
                    attempt = workers.submit(mutate)
                    assert waiting.wait(5) and blocked() and not attempt.done()
                attempt.result(timeout=10)
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        with pytest.raises(NotFound):
            _observe(case, guard)
        case.keys.replace_access(
            app=case.principal.id,
            key_id=case.principal.key_id,
            access=(ApplicationAccess(COLLECTIONS_DELETE),),
            grantor=BOOTSTRAP,
        )
        assert _observe(case, guard).records.operation_id == case.operation
        _unchanged(case)
