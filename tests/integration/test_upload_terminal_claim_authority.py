from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord
from riverhog_core.catalog_db import (
    catalog_write_contention,
    dispose_session_factory,
    session_scope,
)
from riverhog_core.catalog_workflow_models import CollectionProcessingClaimRecord
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_core.services.collection_upload_authority import retain_inactive_upload_claim
from riverhog_core.services.collection_workflows import SqlAlchemyCollectionWorkflowService
from riverhog_protocol.errors import Conflict
from sqlalchemy import event, select, text
from sqlalchemy.exc import OperationalError

from tests.integration.test_collection_deletion_concurrency import (
    EXECUTION_ID,
    WORKFLOW_PRINCIPAL,
    _issue_capability,
    _seal_workflow_claim,
    _seed,
    _upload_service,
    _workflow_artifact,
    _workflow_claim,
)
from tests.integration.test_collection_deletion_concurrency import (
    database_url as database_url,
)

pytestmark = pytest.mark.integration


@contextmanager
def _selected_output(database_url):
    _seed(database_url)
    workflows = SqlAlchemyCollectionWorkflowService(
        RuntimeConfig.for_testing(database_url=database_url)
    )
    root, claim = _workflow_claim(workflows)
    claim_id = str(claim["id"])
    _seal_workflow_claim(workflows, claim_id)
    capability = _issue_capability(
        workflows,
        claim_id,
        audience="fixture.target/v1",
        actions=("read-inputs", "write-output"),
        artifacts=(_workflow_artifact(root),),
    )
    principal = workflows.authenticate_capability(str(capability["token"]))
    assert principal is not None
    service = _upload_service(database_url)
    try:
        collection_id = int(
            service.create_or_resume(
                idempotency_key=EXECUTION_ID,
                ingest_source="processing:" + EXECUTION_ID,
                archive_store=None,
                initiator=principal,
                event_context=None,
            )["collection_id"]
        )
        with session_scope(service._session_factory) as session:
            session.get(
                CollectionProcessingClaimRecord, claim_id
            ).expires_at = "2000-01-01T00:00:00Z"

        def guard(session):
            assert retain_inactive_upload_claim(session, principal.id, nowait=True) == claim_id

        operation = ArchiveWriteAdmission(service._session_factory).select_terminal(
            collection_id,
            kind="discard",
            request={"challenge": "selected-output-scope"},
            principal_id="deletion-operator",
            grants=service._config.archive_write,
            guard=guard,
            validate_selection=lambda *args: None,
        )
        yield SimpleNamespace(
            service=service,
            workflows=workflows,
            claim_id=claim_id,
            collection_id=collection_id,
            operation=operation,
            guard=guard,
            factory=service._session_factory,
        )
    finally:
        service.close()
        dispose_session_factory(service._session_factory)
        dispose_session_factory(workflows._session_factory)


def _control(case):
    with session_scope(case.factory) as session:
        row = session.get(ArchiveWriteControlRecord, case.collection_id)
        return row.construction_json, row.gate, row.epoch, row.terminal_id


def _observe(case):
    with session_scope(case.factory) as session:
        return case.service._archive_terminal_observation.observe(
            session, case.collection_id, case.operation, guard=case.guard
        )


def test_postgres_terminal_claim_fence_interlocks_renewal_without_prohibiting_it(database_url):
    with _selected_output(database_url) as case:
        before = _control(case)
        lane = threading.local()
        waiting = threading.Event()
        pids = {}
        engine = case.workflows._session_factory.kw["bind"]

        def capture(connection, cursor, statement, parameters, context, executemany):
            if (
                getattr(lane, "renewal", False)
                and "FROM collection_processing_claims" in statement
                and "FOR UPDATE" in statement
            ):
                cursor.execute("SELECT pg_backend_pid()")
                pids["renewal"] = cursor.fetchone()[0]
                waiting.set()

        def renew():
            lane.renewal = True
            return case.workflows.renew_claim(
                case.claim_id, fence=1, lease_seconds=600, principal=WORKFLOW_PRINCIPAL
            )

        def blocked():
            deadline = time.monotonic() + 5
            with engine.connect() as observer:
                while time.monotonic() < deadline:
                    if pids["terminal"] in observer.scalar(
                        text("SELECT pg_blocking_pids(:pid)"), {"pid": pids["renewal"]}
                    ):
                        return True
                    time.sleep(0.01)
            return False

        event.listen(engine, "before_cursor_execute", capture)
        try:
            with ThreadPoolExecutor(max_workers=1) as workers:
                with session_scope(case.factory) as session:
                    observed = case.service._archive_terminal_observation.observe(
                        session, case.collection_id, case.operation, guard=case.guard
                    )
                    assert observed.records.operation_id == case.operation
                    pids["terminal"] = session.scalar(text("SELECT pg_backend_pid()"))
                    attempt = workers.submit(renew)
                    assert waiting.wait(5) and blocked() and not attempt.done()
                renewed = attempt.result(timeout=10)
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        assert renewed["state"] == "active" and renewed["fence"] == "1"
        assert _control(case) == before
        with pytest.raises(Conflict, match="claim remains active"):
            _observe(case)
        assert _control(case) == before


def test_postgres_contended_output_claim_defers_before_terminal_local_probes(
    database_url, monkeypatch
):
    with _selected_output(database_url) as case:
        probes = []
        original = case.service._archive_finalizers.local_terminal_status

        def probe(*args, **kwargs):
            probes.append(True)
            return original(*args, **kwargs)

        monkeypatch.setattr(case.service._archive_finalizers, "local_terminal_status", probe)
        with ThreadPoolExecutor(max_workers=1) as workers:
            with session_scope(case.factory) as holder:
                holder.scalar(
                    select(CollectionProcessingClaimRecord.id)
                    .where(CollectionProcessingClaimRecord.id == case.claim_id)
                    .with_for_update()
                )
                attempt = workers.submit(_observe, case)
                with pytest.raises(OperationalError) as failed:
                    attempt.result(timeout=5)
                assert catalog_write_contention(failed.value) and not probes
        assert _observe(case).records.operation_id == case.operation
        assert probes == [True]
