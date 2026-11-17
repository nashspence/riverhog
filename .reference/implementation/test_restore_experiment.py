"""Real AWS policy and Core owners, with explicitly controlled effect seams."""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
from a_riverhog_aws_store.provider import AwsDeepArchiveReadPreparation
from restore_experiment import RestoreGuard, RestoreIdentityChanged
from riverhog_core.catalog_models import RetrievalJobRecord
from riverhog_core.services import retrieval as retrieval_module
from sqlalchemy import select
from state_schema import read_snapshot

from tests.support.runtime_config import write_test_runtime_config
from tests.unit.test_retrieval_service import (
    MemoryRetrievalCache,
    RecordingDownloadAllowance,
    _seed_collection,
)

IDENTITY = {
    "root": "retrieval-original-1",
    "consent": "selected-plan-etag-1",
    "source_incarnation": "fixture-incarnation-1",
    "object_path": "archives/object.age",
    "revision": "provider-version-1",
    "policy_identity": "opaque-adapter-policy-1",
}


def current() -> None:
    """Fixture says current; production needs current key/scope/incarnation CAS."""


class RestoreClient:
    def __init__(self) -> None:
        self.head: dict[str, Any] = {"StorageClass": "DEEP_ARCHIVE"}
        self.requests: list[dict[str, Any]] = []
        self.lose_reply = False

    def head_object(self, **request: Any) -> dict[str, Any]:
        assert request == {
            "Bucket": "fixture-bucket",
            "Key": IDENTITY["object_path"],
            "VersionId": IDENTITY["revision"],
        }
        return dict(self.head)

    def restore_object(self, **request: Any) -> None:
        self.requests.append(request)
        self.head["Restore"] = 'ongoing-request="true"'
        if self.lose_reply:
            raise OSError("reference: provider accepted request; response lost")


def provider_calls(client: RestoreClient):
    policy = AwsDeepArchiveReadPreparation(days=3, tier="Bulk")
    arguments = {
        "client": client,
        "bucket": "fixture-bucket",
        "objects": ((IDENTITY["object_path"], IDENTITY["revision"]),),
    }
    return (
        lambda: policy.prepare(**arguments).state,
        lambda: policy.status(**arguments).state,
    )


def record(name: str, value: dict[str, object]) -> None:
    if directory := os.environ.get("REFERENCE_999_RESTORE_EVIDENCE"):
        (Path(directory) / f"restore_{name}.json").write_text(
            json.dumps(value, indent=2, sort_keys=True) + "\n"
        )


def test_actual_policy_repeats_after_lost_reply_and_expired_window() -> None:
    client = RestoreClient()
    prepare, _ = provider_calls(client)
    client.lose_reply = True
    with pytest.raises(OSError):
        prepare()
    # Deterministic later-window observation, not a claim about AWS consistency.
    client.head = {"StorageClass": "DEEP_ARCHIVE"}
    client.lose_reply = False
    assert prepare() == "requested"
    assert len(client.requests) == 2
    assert client.requests[0] == client.requests[1]
    record("current_policy", {"restore_requests": client.requests, "count": 2})


@pytest.mark.parametrize("lost_at", ["provider_reply", "intent_commit_reply"])
def test_original_survives_restart_without_repeating_entry(tmp_path: Path, lost_at: str) -> None:
    path = tmp_path / "original.sqlite3"
    client = RestoreClient()
    prepare, status = provider_calls(client)
    client.lose_reply = True
    owner = RestoreGuard(path)
    owner.accept("call-1", IDENTITY)
    with pytest.raises(OSError):
        owner.enter(
            "call-1",
            current=current,
            invoke=prepare,
            lose_commit_reply=lost_at == "intent_commit_reply",
        )
    owner.close()
    client.head = {"StorageClass": "DEEP_ARCHIVE"}
    client.lose_reply = False
    resumed = RestoreGuard(path)
    try:
        resumed.accept("call-1", IDENTITY)
        assert not resumed.enter("call-1", current=current, invoke=prepare)
        assert resumed.poll("call-1", current=current, status=status) == "expired"
        assert resumed.state("call-1") == "attention_required"
        assert not resumed.enter("call-1", current=current, invoke=prepare)
        expected = 0 if lost_at == "intent_commit_reply" else 1
        assert len(client.requests) == expected
        record(lost_at, {"provider_entries": expected, "state": resumed.state("call-1")})
    finally:
        resumed.close()


def test_repeated_waits_create_zero_progress_writes(tmp_path: Path) -> None:
    client = RestoreClient()
    prepare, status = provider_calls(client)
    owner = RestoreGuard(tmp_path / "original.sqlite3")
    try:
        owner.accept("call-1", IDENTITY)
        assert owner.enter("call-1", current=current, invoke=prepare)
        before = owner.update_count, owner.commit_count
        for _ in range(64):
            assert owner.poll("call-1", current=current, status=status) == "requested"
        assert (owner.update_count, owner.commit_count) == before
        assert len(client.requests) == 1
        record("waiting", {"polls": 64, "updates": 0, "commits": 0, "provider_entries": 1})
    finally:
        owner.close()


def test_two_reopened_owners_cannot_repeat_the_original(tmp_path: Path) -> None:
    path = tmp_path / "original.sqlite3"
    first, second = RestoreGuard(path), RestoreGuard(path)
    client = RestoreClient()
    prepare, _ = provider_calls(client)
    try:
        first.accept("call-1", IDENTITY)
        second.accept("call-1", IDENTITY)
        assert first.enter("call-1", current=current, invoke=prepare)
        assert not second.enter("call-1", current=current, invoke=prepare)
        assert len(client.requests) == 1
    finally:
        first.close()
        second.close()


@pytest.mark.parametrize("field", ["consent", "source_incarnation", "revision", "policy_identity"])
def test_original_cannot_change_identity(tmp_path: Path, field: str) -> None:
    owner = RestoreGuard(tmp_path / "original.sqlite3")
    try:
        owner.accept("call-1", IDENTITY)
        with pytest.raises(RestoreIdentityChanged):
            owner.accept("call-1", IDENTITY | {field: "changed"})
        assert owner.state("call-1") == "prepared"
    finally:
        owner.close()


def test_current_authority_required_at_entry_and_result(tmp_path: Path) -> None:
    owner = RestoreGuard(tmp_path / "original.sqlite3")
    client = RestoreClient()
    prepare, status = provider_calls(client)

    def revoked() -> None:
        raise PermissionError("fixture authority changed")

    try:
        owner.accept("call-1", IDENTITY)
        with pytest.raises(PermissionError):
            owner.enter("call-1", current=revoked, invoke=prepare)
        assert not client.requests
        assert owner.enter("call-1", current=current, invoke=prepare)
        with pytest.raises(PermissionError):
            owner.poll("call-1", current=revoked, status=status)
        assert owner.state("call-1") == "requested"
    finally:
        owner.close()


def test_actual_core_repeated_claim_and_post_commit_allowance_release(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RIVERHOG_CONFIG", str(write_test_runtime_config(tmp_path)))
    allowance = RecordingDownloadAllowance()
    artifact = "a" * 64
    service, collection, _, _ = _seed_collection(
        tmp_path,
        {artifact: b"small actual archive"},
        read_mode="restore_required",
        cache=MemoryRetrievalCache(),
        allowance=allowance,
    )
    plan = service.plan(((collection, artifact),), idempotency_key="reference-original")
    original_scope = retrieval_module.session_scope
    lose_reply = True

    @contextmanager
    def after_commit(factory):
        nonlocal lose_reply
        with original_scope(factory) as session:
            yield session
        if lose_reply:
            lose_reply = False
            raise OSError("reference: catalog commit completed; reply lost")

    with monkeypatch.context() as scoped:
        scoped.setattr(retrieval_module, "session_scope", after_commit)
        with pytest.raises(OSError, match="catalog commit"):
            service.create(
                principal_id="reader",
                key_id="reader-key",
                plan_id=str(plan["id"]),
                plan_etag=str(plan["etag"]),
            )
    with read_snapshot(service._session_factory) as session:
        original = session.scalar(
            select(RetrievalJobRecord).where(RetrievalJobRecord.plan_id == str(plan["id"]))
        )
        assert original is not None
        job_id = original.id
    assert allowance.released == [job_id]
    retried = service.create(
        principal_id="reader",
        key_id="reader-key",
        plan_id=str(plan["id"]),
        plan_etag=str(plan["etag"]),
    )
    assert retried["id"] == job_id
    assert len(allowance.reservations) == 1
    # A second actual owner claim can select prepare without receiving the first.
    claim1 = service._claim_restore_step(job_id)
    claim2 = service._claim_restore_step(job_id)
    assert claim1 is not None and claim2 is not None
    assert claim1[0] == claim2[0] == "prepare"
    assert claim1[2] == claim2[2]
    record(
        "core_current",
        {
            "catalog_job_survived_lost_commit_reply": True,
            "allowance_released_despite_surviving_job": True,
            "original_job_reused_without_second_reservation": True,
            "sequential_unreceived_claim_actions": [claim1[0], claim2[0]],
            "scope": (
                "actual Core/SQLite/archive fixture; no concurrent process or live provider claim"
            ),
        },
    )
