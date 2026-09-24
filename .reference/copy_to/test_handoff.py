from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
from typing import Any

import pytest
from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from handoff import (
    AccessDenied, Actor, IdentityConflict, InvalidRequest, JobReceipt,
    TerminalHandoffError, after_seconds, intents, metadata, operations,
)
from harness import (
    ACTOR, BASE_IDENTITY, NOW, Harness, collections, events, grants, jobs, keys,
)

HERE = Path(__file__).resolve().parent


@pytest.fixture
def h(tmp_path: Path):
    instance = Harness(tmp_path / "catalog.sqlite")
    yield instance
    instance.close()


def count(h: Harness, table: Any) -> int:
    with h.sessions.begin() as session:
        return session.scalar(select(func.count()).select_from(table))


def raw_intent(h: Harness, destination: str = "copy-a") -> dict[str, Any]:
    with h.sessions.begin() as session:
        return dict(session.execute(select(intents).where(intents.c.collection_id == 1, intents.c.destination_store == destination)).mappings().one())


def run_worker(h: Harness, phase: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, str(HERE / "crash_worker.py"), h.path, phase], capture_output=True, text=True, timeout=30, check=False)


def test_acceptance_durable_before_payload_and_no_early_jobs(h: Harness):
    with h.sessions.begin() as session:
        first = h.accept(session, copy_to=["copy-b", "copy-a", "copy-a"])
    h.close()
    restarted = Harness(h.path, create=False)
    try:
        assert restarted.snapshot()["copy_to"] == ["copy-a", "copy-b"]
        assert all(row["state"] == "accepted" for row in first["copies"])
        assert count(restarted, jobs) == 0
        assert restarted.service.process_due(restarted.sessions)["progressed"] == 0
    finally:
        restarted.close()


def test_acceptance_rollback_leaves_no_partial_intents(h: Harness):
    with pytest.raises(RuntimeError), h.sessions.begin() as session:
        h.accept(session, copy_to=["copy-a", "copy-b"])
        raise RuntimeError("upload creation failed")
    assert count(h, operations) == count(h, intents) == 0


@pytest.mark.parametrize("copy_to", ["copy-a", [""], [" copy-a"], ["copy-a "], [False], [1], ["archive"], ["missing"], ["copy-a", "missing"], ["copy-a"] * 65])
def test_invalid_destinations_reject_entire_acceptance(h: Harness, copy_to: Any):
    with pytest.raises(InvalidRequest), h.sessions.begin() as session:
        h.accept(session, copy_to=copy_to)
    assert count(h, operations) == count(h, intents) == count(h, jobs) == 0


@pytest.mark.parametrize("cache", [0, 1, "false", [], {}])
def test_cache_choice_rejects_non_booleans(h: Harness, cache: Any):
    with pytest.raises(InvalidRequest), h.sessions.begin() as session:
        h.accept(session, use_cache=cache)


@pytest.mark.parametrize("permission", ["collections:create", "archives:manage"])
def test_both_permissions_required_at_acceptance(h: Harness, permission: str):
    with h.sessions.begin() as session:
        session.execute(delete(grants).where(grants.c.permission == permission))
    with pytest.raises(AccessDenied), h.sessions.begin() as session:
        h.accept(session)
    assert count(h, operations) == 0


def test_no_copy_intent_needs_only_creation_authority(h: Harness):
    with h.sessions.begin() as session:
        session.execute(delete(grants).where(grants.c.permission == "archives:manage"))
        h.accept(session, copy_to=[])
        h.publish(session)
    assert count(h, intents) == count(h, jobs) == 0


@pytest.mark.parametrize("actor", [Actor("wrong-app", ACTOR.key_id), Actor(ACTOR.app, "unknown-key"), Actor(ACTOR.app, ""), Actor("", ACTOR.key_id)])
def test_no_identity_or_unattributed_bypass(h: Harness, actor: Actor):
    with pytest.raises(AccessDenied), h.sessions.begin() as session:
        h.accept(session, actor=actor)


@pytest.mark.parametrize("field", ["revoked_at", "expires_at"])
def test_inactive_key_rejected_at_acceptance(h: Harness, field: str):
    with h.sessions.begin() as session:
        session.execute(update(keys).values(**{field: NOW}))
    with pytest.raises(AccessDenied), h.sessions.begin() as session:
        h.accept(session)


def test_tag_scoped_authority_and_current_tags_at_handoff(h: Harness):
    with h.sessions.begin() as session:
        session.execute(update(grants).values(resource="tag:test-tag"))
        h.accept(session)
        h.publish(session)
        session.execute(update(collections).values(tags_json='["different-tag"]'))
    assert h.service.process_due(h.sessions)["progressed"] == 1
    assert raw_intent(h)["failure_code"] == "authorization_denied"
    assert count(h, jobs) == 0


def test_tag_creation_scope_is_conservative_for_all_requested_tags(h: Harness):
    with h.sessions.begin() as session:
        session.execute(update(grants).values(resource="tag:test-tag"))
    with pytest.raises(AccessDenied), h.sessions.begin() as session:
        h.accept(session, tags=["test-tag", "outside-scope"])


@pytest.mark.parametrize("change", ["revoked", "expired", "grant-removed", "key-deleted", "app-changed"])
def test_fresh_authority_rechecked_after_publication(h: Harness, change: str):
    h.ready()
    with h.sessions.begin() as session:
        if change == "revoked":
            session.execute(update(keys).values(revoked_at=NOW))
        elif change == "expired":
            session.execute(update(keys).values(expires_at=NOW))
        elif change == "grant-removed":
            session.execute(delete(grants).where(grants.c.permission == "archives:manage"))
        elif change == "app-changed":
            session.execute(update(keys).values(app="different-app"))
        else:
            session.execute(delete(grants))
            session.execute(delete(keys))
    assert h.service.process_due(h.sessions)["progressed"] == 1
    assert raw_intent(h)["state"] == "failed"
    assert raw_intent(h)["failure_code"] == "authorization_denied"
    assert count(h, jobs) == count(h, events) == 0


def test_revocation_after_handoff_does_not_reassign_or_restart_job(h: Harness):
    h.ready()
    h.service.process_due(h.sessions)
    with h.sessions.begin() as session:
        session.execute(update(keys).values(revoked_at=NOW))
    assert h.service.process_due(h.sessions)["progressed"] == 0
    assert count(h, jobs) == count(h, events) == 1
    with pytest.raises(AccessDenied):
        h.snapshot()


def test_canonical_idempotency_order_and_duplicates(h: Harness):
    with h.sessions.begin() as session:
        first = h.accept(session, copy_to=["copy-b", "copy-a"])
    with h.sessions.begin() as session:
        again = h.accept(session, copy_to=["copy-a", "copy-a", "copy-b"])
    assert first["creation_identity"] == again["creation_identity"]
    assert count(h, operations) == 1
    assert count(h, intents) == 2


@pytest.mark.parametrize("changed", [
    {"base_identity": "b" * 64}, {"event_context": {"trace": "changed"}},
    {"use_cache": False}, {"copy_to": []}, {"copy_to": ["copy-b"]},
    {"archive_store": "new-default"}, {"tags": ["different-tag"]},
])
def test_changed_semantics_conflict_without_mutation(h: Harness, changed: dict[str, Any]):
    with h.sessions.begin() as session:
        original = h.accept(session)
    with pytest.raises(IdentityConflict), h.sessions.begin() as session:
        h.accept(session, **changed)
    assert h.snapshot()["creation_identity"] == original["creation_identity"]
    assert count(h, intents) == 1


def test_another_active_key_cannot_reattribute_accepted_intent(h: Harness):
    with h.sessions.begin() as session:
        h.accept(session)
        session.execute(insert(keys).values(id="0000000000000002", app=ACTOR.app))
        session.execute(insert(grants), [{"key_id": "0000000000000002", "permission": p, "resource": "*"} for p in ("collections:create", "archives:manage")])
    with pytest.raises(IdentityConflict), h.sessions.begin() as session:
        h.accept(session, actor=Actor(ACTOR.app, "0000000000000002"))


def test_omitted_choices_on_resume_ignore_changed_defaults(h: Harness):
    with h.sessions.begin() as session:
        first = h.accept(session)
    h.registry.default = "new-default"
    h.registry.cache_default = False
    with h.sessions.begin() as session:
        resumed = h.accept(session, copy_to=None)
        h.publish(session)
    assert resumed["creation_identity"] == first["creation_identity"]
    assert resumed["archive_store"] == "archive" and resumed["use_cache"] is True
    assert h.service.process_due(h.sessions)["progressed"] == 1
    assert h.snapshot()["copies"][0]["job_observation"]["source_store"] == "archive"


@pytest.mark.parametrize("store", ["archive", "copy-a"])
@pytest.mark.parametrize("change", ["removed", "remapped"])
def test_binding_config_drift_is_visible_terminal_failure(h: Harness, store: str, change: str):
    h.ready()
    if change == "removed":
        del h.registry.stores[store]
    else:
        h.registry.stores[store] = "different-backend-binding"
    assert h.service.process_due(h.sessions)["progressed"] == 1
    assert raw_intent(h)["failure_code"] == "configuration_changed"
    assert count(h, jobs) == 0
    h.registry.stores[store] = "test-binding:" + store
    assert h.service.process_due(h.sessions)["progressed"] == 0  # no silent resurrection


def test_publication_and_pending_continuation_share_transaction(h: Harness):
    with h.sessions.begin() as session:
        h.accept(session)
    with pytest.raises(RuntimeError), h.sessions.begin() as session:
        h.publish(session)
        raise RuntimeError("finalization abort")
    assert count(h, collections) == 0
    assert raw_intent(h)["state"] == "accepted"
    with h.sessions.begin() as session:
        h.publish(session)
    assert raw_intent(h)["state"] == "pending"


def test_publication_requires_matching_owner_and_archive(h: Harness):
    with h.sessions.begin() as session:
        h.accept(session)
        session.execute(insert(collections).values(id=1, owner="someone-else", source="archive", tags_json="[]", published="1"))
    with pytest.raises(TerminalHandoffError), h.sessions.begin() as session:
        h.service.publish(session, 1)
    assert raw_intent(h)["state"] == "accepted"


def test_cancel_before_publication_is_durable_and_final(h: Harness):
    with h.sessions.begin() as session:
        h.accept(session)
        h.service.cancel(session, 1, ACTOR)
        h.service.cancel(session, 1, ACTOR)
    assert raw_intent(h)["state"] == "canceled"
    assert h.service.process_due(h.sessions)["progressed"] == 0
    with pytest.raises(IdentityConflict), h.sessions.begin() as session:
        h.accept(session)
    with pytest.raises(IdentityConflict), h.sessions.begin() as session:
        h.publish(session)
    assert count(h, jobs) == 0


def test_post_publication_upload_cancel_cannot_cancel_jobs(h: Harness):
    h.ready()
    with pytest.raises(IdentityConflict), h.sessions.begin() as session:
        h.service.cancel(session, 1, ACTOR)
    assert raw_intent(h)["state"] == "pending"


@pytest.mark.parametrize("state", ["requested", "waiting", "checking", "copying", "canceling", "completed", "failed", "canceled"])
def test_repeated_finalization_never_resubmits_ordinary_job(h: Harness, state: str):
    h.ready()
    h.service.process_due(h.sessions)
    with h.sessions.begin() as session:
        session.execute(update(jobs).values(state=state))
        h.publish(session)
        h.accept(session)
    h.service.process_due(h.sessions)
    snapshot = h.snapshot()["copies"][0]
    assert snapshot["job_observation"]["state"] == state
    assert snapshot["job"]["created"] is True
    assert count(h, jobs) == count(h, events) == 1


def test_existing_job_keeps_other_initiator_and_terminal_state(h: Harness):
    h.ready()
    with h.sessions.begin() as session:
        session.execute(insert(jobs).values(collection_id=1, destination_store="copy-a", source_store="archive", state="canceled",
            initiated_by_app="ordinary-caller", initiated_by_key_id="ordinary-key", event_context_json="{}"))
    h.service.process_due(h.sessions)
    observation = h.snapshot()["copies"][0]
    assert observation["job"]["created"] is False
    assert observation["job_observation"]["state"] == "canceled"
    assert observation["job_observation"]["initiated_by_app"] == "ordinary-caller"
    assert count(h, events) == 0


def test_missing_job_after_receipt_is_observable_not_recreated(h: Harness):
    h.ready()
    h.service.process_due(h.sessions)
    with h.sessions.begin() as session:
        session.execute(delete(jobs))
    h.service.process_due(h.sessions)
    assert h.snapshot()["copies"][0]["job_observation"] is None
    assert raw_intent(h)["state"] == "handed_off"
    assert count(h, jobs) == 0


def test_generated_job_and_event_preserve_original_attribution(h: Harness):
    h.ready()
    h.service.process_due(h.sessions)
    with h.sessions.begin() as session:
        job = session.execute(select(jobs)).mappings().one()
        event = session.execute(select(events)).mappings().one()
    assert (job["initiated_by_app"], job["initiated_by_key_id"]) == (ACTOR.app, ACTOR.key_id)
    assert (event["app"], event["key_id"]) == (ACTOR.app, ACTOR.key_id)
    assert json.loads(event["context_json"]) == json.loads(job["event_context_json"]) == {"trace": "test-trace"}


def test_partial_multi_destination_handoff_resumes_on_restart(h: Harness):
    h.ready(copy_to=["copy-a", "copy-b"])
    assert h.service.process_due(h.sessions, limit=1)["progressed"] == 1
    h.close()
    restarted = Harness(h.path, create=False)
    try:
        assert restarted.service.process_due(restarted.sessions)["progressed"] == 1
        assert count(restarted, jobs) == count(restarted, events) == 2
    finally:
        restarted.close()


def test_terminal_error_after_job_write_rolls_back_savepoint(h: Harness):
    h.ready()
    def fail(phase: str):
        if phase == "after_job":
            raise TerminalHandoffError("source_unavailable")
    h.gateway.fault = fail
    h.service.process_due(h.sessions)
    assert raw_intent(h)["failure_code"] == "source_unavailable"
    assert count(h, jobs) == count(h, events) == 0


def test_transient_failure_rolls_back_and_retries_with_bounded_safe_evidence(h: Harness):
    h.ready(copy_to=["copy-a", "copy-b"])
    failures = 0
    def fail(phase: str):
        nonlocal failures
        if phase == "after_event" and failures == 0:
            failures += 1
            raise RuntimeError("secret-token-must-not-be-persisted")
    h.gateway.fault = fail
    result = h.service.process_due(h.sessions)
    assert result == {"progressed": 1, "retry_scheduled": 1}
    row = raw_intent(h)
    assert row["state"] == "pending" and row["attempts"] == 1
    assert row["failure_code"] == "retryable_handoff_error"
    assert "secret" not in json.dumps(row)
    assert count(h, jobs) == count(h, events) == 1
    assert h.service.process_due(h.sessions)["progressed"] == 0
    h.now = after_seconds(NOW, 2)
    assert h.service.process_due(h.sessions)["progressed"] == 1
    assert count(h, jobs) == count(h, events) == 2
    assert raw_intent(h)["failure_code"] is None


def test_unrelated_gateway_receipt_rolls_back_everything(h: Harness):
    h.ready()
    ensure = h.gateway.ensure
    def wrong(*args: Any, **kwargs: Any):
        ensure(*args, **kwargs)
        return JobReceipt(99, "copy-a", True)
    h.gateway.ensure = wrong
    assert h.service.process_due(h.sessions)["retry_scheduled"] == 1
    assert count(h, jobs) == count(h, events) == 0
    assert raw_intent(h)["job_receipt_json"] is None


@pytest.mark.parametrize("phase", ["before_accept_commit", "after_accept_commit"])
def test_sigkill_acceptance_restart(h: Harness, phase: str):
    result = run_worker(h, phase)
    assert result.returncode == -signal.SIGKILL, result.stderr
    assert count(h, operations) == (0 if phase == "before_accept_commit" else 1)
    with h.sessions.begin() as session:
        h.accept(session)
        h.publish(session)
    h.service.process_due(h.sessions)
    assert count(h, jobs) == count(h, events) == 1


@pytest.mark.parametrize("phase", ["before_publish_commit", "after_publish_commit"])
def test_sigkill_publication_restart(h: Harness, phase: str):
    with h.sessions.begin() as session:
        h.accept(session)
    result = run_worker(h, phase)
    assert result.returncode == -signal.SIGKILL, result.stderr
    assert count(h, collections) == (0 if phase == "before_publish_commit" else 1)
    assert raw_intent(h)["state"] == ("accepted" if phase == "before_publish_commit" else "pending")
    with h.sessions.begin() as session:
        h.publish(session)
    h.service.process_due(h.sessions)
    assert count(h, jobs) == count(h, events) == 1


@pytest.mark.parametrize("phase", ["after_job", "after_event", "before_handoff_commit", "after_handoff_commit"])
def test_sigkill_job_event_receipt_are_all_or_nothing(h: Harness, phase: str):
    h.ready()
    result = run_worker(h, phase)
    assert result.returncode == -signal.SIGKILL, result.stderr
    committed = phase == "after_handoff_commit"
    assert count(h, jobs) == count(h, events) == int(committed)
    assert raw_intent(h)["state"] == ("handed_off" if committed else "pending")
    restarted = Harness(h.path, create=False)
    try:
        restarted.service.process_due(restarted.sessions)
        assert count(restarted, jobs) == count(restarted, events) == 1
        assert raw_intent(restarted)["state"] == "handed_off"
    finally:
        restarted.close()


def test_competing_process_sweepers_create_one_job_and_event(h: Harness):
    h.ready()
    processes = [subprocess.Popen([sys.executable, str(HERE / "crash_worker.py"), h.path, "normal"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for _ in range(4)]
    for process in processes:
        _, error = process.communicate(timeout=30)
        assert process.returncode == 0, error
    assert count(h, jobs) == count(h, events) == 1
    assert raw_intent(h)["attempts"] == 1


def test_postgresql_ddl_compiles_and_has_receipt_constraint():
    statements = "\n".join(str(CreateTable(table).compile(dialect=postgresql.dialect())) for table in metadata.sorted_tables)
    assert "ck_upload_copy_intent_receipt" in statements
    assert "ON DELETE RESTRICT" in statements
    assert "BIGINT" in statements


@pytest.mark.skipif(not os.environ.get("COPY_TO_TEST_POSTGRES_URL"), reason="PostgreSQL runtime/driver not available; set COPY_TO_TEST_POSTGRES_URL for a disposable DB")
def test_postgresql_concurrent_handoff(tmp_path: Path):
    # Run only against an empty disposable DB. This tests the reference schema,
    # NOT Riverhog's production schema. No runtime schema upgrades are proposed.
    from concurrent.futures import ThreadPoolExecutor
    pg = Harness(tmp_path / "unused", database_url=os.environ["COPY_TO_TEST_POSTGRES_URL"])
    try:
        pg.ready()
        with ThreadPoolExecutor(max_workers=4) as executor:
            list(executor.map(lambda _: pg.service.process_due(pg.sessions), range(4)))
        assert count(pg, jobs) == count(pg, events) == 1
    finally:
        pg.close()


def test_idempotent_finalized_response_rechecks_current_archive_scope(h: Harness):
    with h.sessions.begin() as session:
        session.execute(update(grants).values(resource="tag:test-tag"))
    h.ready()
    h.service.process_due(h.sessions)
    with h.sessions.begin() as session:
        session.execute(update(collections).values(tags_json='["new-tag"]'))
    with pytest.raises(AccessDenied), h.sessions.begin() as session:
        h.accept(session)


def test_no_copy_finalized_observation_does_not_add_archive_permission(h: Harness):
    with h.sessions.begin() as session:
        session.execute(delete(grants).where(grants.c.permission == "archives:manage"))
    h.ready(copy_to=[])
    assert h.snapshot()["copies"] == []


def test_operator_can_observe_revocation_failure_without_reattributing_work(h: Harness):
    h.ready()
    operator = Actor("test-operator", "0000000000000002")
    with h.sessions.begin() as session:
        session.execute(update(keys).values(revoked_at=NOW))
        session.execute(insert(keys).values(id=operator.key_id, app=operator.app))
        session.execute(insert(grants).values(key_id=operator.key_id, permission="archives:manage", resource="collection:1"))
    h.service.process_due(h.sessions)
    with h.sessions.begin() as session:
        result = h.service.inspect_published(session, 1, operator)
    assert result["copies"][0]["failure_code"] == "authorization_denied"
    assert result["initiated_by_key_id"] == ACTOR.key_id
    assert count(h, jobs) == 0


def test_operator_inspection_rejects_wrong_scope_and_unpublished_state(h: Harness):
    with h.sessions.begin() as session:
        h.accept(session)
    with pytest.raises(AccessDenied), h.sessions.begin() as session:
        h.service.inspect_published(session, 1, ACTOR)
    with h.sessions.begin() as session:
        h.publish(session)
        session.execute(delete(grants).where(grants.c.permission == "archives:manage"))
    with pytest.raises(AccessDenied), h.sessions.begin() as session:
        h.service.inspect_published(session, 1, ACTOR)
