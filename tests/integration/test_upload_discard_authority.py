from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from riverhog_core.catalog_db import catalog_write_contention, session_scope
from riverhog_core.catalog_models import AppKeyRecord, CollectionUploadRecord
from riverhog_core.catalog_workflow_models import CollectionProcessingClaimRecord
from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy import event, text
from sqlalchemy.exc import OperationalError

from tests.integration.test_collection_deletion_concurrency import WORKFLOW_PRINCIPAL
from tests.integration.test_upload_terminal_claim_authority import _control, _selected_output
from tests.integration.test_upload_terminal_claim_authority import database_url as database_url
from tests.unit.test_upload_discard_authority import _operator

pytestmark = pytest.mark.integration


def _observe(case, guard):
    with session_scope(case.factory) as session:
        return case.service._archive_terminal_observation.observe(
            session, case.collection_id, case.operation, guard=guard
        )


@pytest.mark.parametrize("resource", ("key", "claim", "parent"))
def test_postgres_discard_guard_defers_each_contended_lock_before_local_probes(
    database_url, monkeypatch, resource
):
    with _selected_output(database_url) as case:
        _, principal = _operator(case)
        guard = case.service._discard_write_guard(case.collection_id, principal)
        before = _control(case)
        probes = []
        original = case.service._archive_finalizers.local_terminal_status

        def probe(*args, **kwargs):
            probes.append(True)
            return original(*args, **kwargs)

        monkeypatch.setattr(case.service._archive_finalizers, "local_terminal_status", probe)
        model, identity = {
            "key": (AppKeyRecord, principal.key_id),
            "claim": (CollectionProcessingClaimRecord, case.claim_id),
            "parent": (CollectionUploadRecord, case.collection_id),
        }[resource]
        with ThreadPoolExecutor(max_workers=1) as workers:
            with session_scope(case.factory) as holder:
                holder.get(model, identity, with_for_update=True)
                attempt = workers.submit(_observe, case, guard)
                with pytest.raises(OperationalError) as failed:
                    attempt.result(timeout=5)
                assert catalog_write_contention(failed.value) and not probes
        assert _control(case) == before
        assert _observe(case, guard).records.operation_id == case.operation
        assert probes == [True]


@pytest.mark.parametrize("mutation", ("renew", "revoke"))
def test_postgres_discard_observation_retains_key_and_claim_against_real_mutations(
    database_url, mutation
):
    with _selected_output(database_url) as case:
        keys, principal = _operator(case)
        guard = case.service._discard_write_guard(case.collection_id, principal)
        before = _control(case)
        lane = threading.local()
        waiting = threading.Event()
        pids = {}
        engine = (case.workflows._session_factory if mutation == "renew" else case.factory).kw[
            "bind"
        ]
        table = "collection_processing_claims" if mutation == "renew" else "app_keys"

        def capture(connection, cursor, statement, parameters, context, executemany):
            if (
                getattr(lane, "mutation", False)
                and "FROM " + table in statement
                and "FOR UPDATE" in statement
            ):
                cursor.execute("SELECT pg_backend_pid()")
                pids["mutation"] = cursor.fetchone()[0]
                waiting.set()

        def mutate():
            lane.mutation = True
            if mutation == "renew":
                return case.workflows.renew_claim(
                    case.claim_id, fence=1, lease_seconds=600, principal=WORKFLOW_PRINCIPAL
                )
            return keys.revoke(app=principal.id, key_id=principal.key_id)

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
        assert _control(case) == before
        error, message = (
            (Conflict, "claim remains active")
            if mutation == "renew"
            else (NotFound, "no longer active")
        )
        with pytest.raises(error, match=message):
            _observe(case, guard)
        assert _control(case) == before


@pytest.mark.parametrize("field", ("initiated_by_principal_id", "archive_storage_prefix"))
def test_postgres_discard_guard_rechecks_original_after_its_nonlocking_creator_lookup(
    database_url, monkeypatch, field
):
    with _selected_output(database_url) as case:
        _, principal = _operator(case)
        guard = case.service._discard_write_guard(case.collection_id, principal)
        before = _control(case)
        engine = case.factory.kw["bind"]
        changed = []

        def capture(connection, cursor, statement, parameters, context, executemany):
            if not changed and "FROM collection_uploads" in statement and "FOR UPDATE" in statement:
                changed.append(True)
                with session_scope(case.factory) as mutation:
                    upload = mutation.get(
                        CollectionUploadRecord, case.collection_id, with_for_update=True
                    )
                    setattr(
                        upload, field, "replacement-producer" if field.endswith("id") else "new/"
                    )

        def forbidden(*args, **kwargs):
            pytest.fail("changed original construction reached local terminal probes")

        monkeypatch.setattr(case.service._archive_finalizers, "local_terminal_status", forbidden)
        event.listen(engine, "before_cursor_execute", capture)
        try:
            with pytest.raises(Conflict, match="original creator|construction identity"):
                _observe(case, guard)
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        assert changed == [True] and _control(case) == before
