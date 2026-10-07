"""Departure effect target transport and idempotent withdrawal witness."""

from __future__ import annotations

import threading
import time
from pathlib import Path

import httpx
import pytest
from riverhog_protocol import CatalogSyncDescriptor
from stove0_extension_support import ExecutionOwner
from stove0_target_client import DepartureEffectClient, TargetProtocolError
from stove0_target_protocol import (
    DepartureEffectIntent,
    DepartureEffectIntentPayload,
    DepartureEffectReceipt,
    DepartureEffectReceiptPayload,
    DepartureEffectStatus,
    DepartureEffectTargetDescriptor,
    DepartureEffectTargetDescriptorPayload,
)
from stove0_target_support import (
    DepartureEffectHttpBinding,
    DepartureExecutionSession,
    PersistentDepartureEffectService,
)


def _target_descriptor() -> DepartureEffectTargetDescriptor:
    return DepartureEffectTargetDescriptor.seal(
        DepartureEffectTargetDescriptorPayload(
            implementation_id="fixture.index/v1",
            implementation_version="1.0.0",
            source_revision="fixture-source",
            image_id="sha256:" + "8" * 64,
            scope_identity="9" * 64,
        )
    )


def _intent() -> DepartureEffectIntent:
    return DepartureEffectIntent.seal(
        DepartureEffectIntentPayload(
            policy_id="withdraw-index",
            policy_revision=1,
            policy_sha256="1" * 64,
            target_registration_id="index",
            target_identity=_target_descriptor().target_identity,
            source_identity="2" * 64,
            authorization_view_identity="3" * 64,
            last_collection=CatalogSyncDescriptor(
                collection_id="17",
                archive_root_sha256="4" * 64,
                artifact_set_identity="5" * 64,
                description=None,
                description_revision=0,
                description_identity="6" * 64,
                tag_revision=1,
                tag_set_identity="7" * 64,
                revision="9",
            ),
            departure_cause="visibility_lost",
            departure_revision="10",
        )
    )


class _Withdrawal:
    def __init__(self) -> None:
        self.effects: set[str] = set()
        self.receipts: dict[str, DepartureEffectReceipt] = {}

    def execute(self, session: DepartureExecutionSession) -> DepartureEffectReceipt:
        intent = session.intent
        self.effects.add(intent.departure_id)
        return self.receipts.setdefault(intent.departure_id, _receipt(intent))


def _receipt(intent: DepartureEffectIntent) -> DepartureEffectReceipt:
    return DepartureEffectReceipt.seal(
        DepartureEffectReceiptPayload(
            departure_id=intent.departure_id,
            target_identity=intent.target_identity,
            result={"index_action": "withdrawn"},
        )
    )


def _eventually(service: PersistentDepartureEffectService, state: str) -> DepartureEffectStatus:
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        status = service.get_departure_effect(_intent().departure_id)
        if status.state == state:
            return status
        time.sleep(0.005)
    raise AssertionError(f"departure status did not reach {state}: {status}")


def test_departure_binding_delivers_polls_and_preserves_exact_completion(tmp_path: Path) -> None:
    intent = _intent()
    target = _Withdrawal()
    service = PersistentDepartureEffectService(
        descriptor=_target_descriptor(),
        state_root=tmp_path,
        execute=target.execute,
    )
    try:
        binding = DepartureEffectHttpBinding(service)
        path = f"/v1/departure-effects/{intent.departure_id}"
        body = intent.model_dump_json().encode("utf-8")
        assert (
            DepartureEffectTargetDescriptor.model_validate_json(
                binding.handle("GET", "/v1/departure-target").body
            )
            == service.descriptor()
        )
        first = binding.handle("PUT", path, body)
        assert first.status == 200
        assert (
            DepartureEffectStatus.model_validate_json(first.body).departure_id
            == intent.departure_id
        )
        completed = _eventually(service, "completed")
        replay = DepartureEffectStatus.model_validate_json(binding.handle("PUT", path, body).body)
        polled = DepartureEffectStatus.model_validate_json(binding.handle("GET", path).body)
        canceled = DepartureEffectStatus.model_validate_json(
            binding.handle("POST", path + "/cancel", body).body
        )
        assert replay == polled == canceled == completed
        assert completed.receipt == target.receipts[intent.departure_id]
        assert target.effects == {intent.departure_id}
        assert binding.handle("PUT", f"/v1/departure-effects/{'0' * 64}", body).status == 400
        assert binding.handle("PUT", path, b"{}").status == 400
        wrong = DepartureEffectIntent.seal(
            DepartureEffectIntentPayload.model_validate(
                {
                    **intent.model_dump(mode="json", exclude={"departure_id"}),
                    "target_identity": "9" * 64,
                }
            )
        )
        assert (
            binding.handle(
                "PUT",
                f"/v1/departure-effects/{wrong.departure_id}",
                wrong.model_dump_json().encode(),
            ).status
            == 409
        )
    finally:
        service.close()


def test_departure_client_lost_response_retries_same_effect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    intent = _intent()
    target = _Withdrawal()
    service = PersistentDepartureEffectService(
        descriptor=_target_descriptor(), state_root=tmp_path, execute=target.execute
    )
    try:
        binding = DepartureEffectHttpBinding(service)
        real_client = httpx.Client
        replies = 0

        def respond(request: httpx.Request) -> httpx.Response:
            nonlocal replies
            assert request.headers["Authorization"] == "Bearer fixture-token"
            result = binding.handle(request.method, request.url.path, request.content)
            replies += 1
            if replies == 2:
                raise httpx.ReadTimeout("response was lost")
            return httpx.Response(result.status, content=result.body, headers=dict(result.headers))

        monkeypatch.setattr(
            httpx, "Client", lambda **_kwargs: real_client(transport=httpx.MockTransport(respond))
        )
        client = DepartureEffectClient("https://index.example", token="fixture-token")
        with pytest.raises(TargetProtocolError) as error:
            client.put_effect(intent)
        assert error.value.failure_kind == "transport"
        completed = _eventually(service, "completed")
        assert client.put_effect(intent) == client.status(intent.departure_id) == completed
        assert client.cancel(intent) == completed
        assert target.effects == {intent.departure_id}
    finally:
        service.close()


class _Withheld:
    def __init__(self) -> None:
        self.owners: set[ExecutionOwner] = set()

    def probe(self, owner: ExecutionOwner, *, deadline: float):
        self.owners.add(owner)
        return None

    def withdraw(self, owner: ExecutionOwner, *, deadline: float) -> None:
        self.owners.discard(owner)


def test_queued_departure_has_no_payload_and_cancel_fences_restart(tmp_path: Path) -> None:
    target = _Withdrawal()
    admission = _Withheld()
    service = PersistentDepartureEffectService(
        descriptor=_target_descriptor(),
        state_root=tmp_path,
        execute=target.execute,
        execution_admission=admission,
        admission_retry_seconds=0.01,
    )
    intent = _intent()
    try:
        assert service.put_departure_effect(intent).state == "queued"
        deadline = time.monotonic() + 1
        while not admission.owners and time.monotonic() < deadline:
            time.sleep(0.005)
        assert admission.owners
        assert service._dispatch.payload_count == 0
        assert not target.effects
        assert service.cancel_departure_effect(intent).state == "canceled"
        assert service.put_departure_effect(intent).state == "canceled"
    finally:
        service.close()
    assert not admission.owners
    resumed = PersistentDepartureEffectService(
        descriptor=_target_descriptor(), state_root=tmp_path, execute=target.execute
    )
    try:
        assert resumed.put_departure_effect(intent).state == "canceled"
        assert not target.effects
    finally:
        resumed.close()


def test_departure_cancel_before_first_put_is_durable(tmp_path: Path) -> None:
    target = _Withdrawal()
    service = PersistentDepartureEffectService(
        descriptor=_target_descriptor(), state_root=tmp_path, execute=target.execute
    )
    assert service.cancel_departure_effect(_intent()).state == "canceled"
    service.close()
    # Exercise the crash window between cancellation fence and accepted state.
    for suffix in ("accepted", "status"):
        (tmp_path / f"{_intent().departure_id}.{suffix}.json").unlink()
    resumed = PersistentDepartureEffectService(
        descriptor=_target_descriptor(), state_root=tmp_path, execute=target.execute
    )
    try:
        assert resumed.put_departure_effect(_intent()).state == "canceled"
        assert not target.effects
    finally:
        resumed.close()


def test_started_uncertain_departure_never_replays_and_reconciles_exact_receipt(
    tmp_path: Path,
) -> None:
    calls = 0

    def uncertain(session: DepartureExecutionSession) -> DepartureEffectReceipt:
        nonlocal calls
        calls += 1
        raise RuntimeError("transport lost after an external commit")

    service = PersistentDepartureEffectService(
        descriptor=_target_descriptor(), state_root=tmp_path, execute=uncertain
    )
    service.put_departure_effect(_intent())
    assert _eventually(service, "interrupted").receipt is None
    assert service.put_departure_effect(_intent()).state == "interrupted"
    service.close()
    resumed = PersistentDepartureEffectService(
        descriptor=_target_descriptor(),
        state_root=tmp_path,
        execute=uncertain,
        reconcile=lambda intent, deadline: _receipt(intent),
    )
    try:
        assert resumed.put_departure_effect(_intent()).receipt == _receipt(_intent())
        assert calls == 1
        assert resumed.get_departure_effect(_intent().departure_id).receipt == _receipt(_intent())
        assert resumed.cancel_departure_effect(_intent()).state == "completed"
    finally:
        resumed.close()


@pytest.mark.parametrize("action", ["put", "get", "cancel"])
def test_normal_departure_control_recovers_a_receipt_without_execution_admission(tmp_path, action):
    calls = 0

    def uncertain(session):
        nonlocal calls
        calls += 1
        raise RuntimeError("external commit response was lost")

    service = PersistentDepartureEffectService(
        descriptor=_target_descriptor(), state_root=tmp_path, execute=uncertain
    )
    service.put_departure_effect(_intent())
    _eventually(service, "interrupted")
    service.close()
    admission = _Withheld()
    reads = []

    def read_receipt(intent, deadline):
        assert not resumed._lock._is_owned()
        assert 0 < deadline - time.monotonic() <= 0.1
        reads.append(intent.departure_id)
        return _receipt(intent)

    resumed = PersistentDepartureEffectService(
        descriptor=_target_descriptor(),
        state_root=tmp_path,
        execute=uncertain,
        reconcile=read_receipt,
        execution_admission=admission,
        reconciliation_seconds=0.05,
    )
    try:
        if action == "put":
            status = resumed.put_departure_effect(_intent())
        elif action == "get":
            status = resumed.get_departure_effect(_intent().departure_id)
        else:
            status = resumed.cancel_departure_effect(_intent())
        assert status.receipt == _receipt(_intent())
        assert calls == 1 and reads == [_intent().departure_id]
        assert not admission.owners and resumed._dispatch.payload_count == 0
    finally:
        resumed.close()


def test_slow_departure_receipt_lookup_retains_one_contact_and_accepts_late_completion(tmp_path):
    def uncertain(session):
        raise RuntimeError("external commit response was lost")

    service = PersistentDepartureEffectService(
        descriptor=_target_descriptor(), state_root=tmp_path, execute=uncertain
    )
    service.put_departure_effect(_intent())
    _eventually(service, "interrupted")
    service.close()
    entered, release = threading.Event(), threading.Event()
    reads = []

    def read_receipt(intent, deadline):
        reads.append(intent.departure_id)
        entered.set()
        assert release.wait(3)
        return _receipt(intent)

    resumed = PersistentDepartureEffectService(
        descriptor=_target_descriptor(),
        state_root=tmp_path,
        execute=uncertain,
        reconcile=read_receipt,
        reconciliation_seconds=0.05,
    )
    try:
        start = time.monotonic()
        assert resumed.get_departure_effect(_intent().departure_id).state == "interrupted"
        assert entered.is_set() and time.monotonic() - start < 0.5
        assert resumed.put_departure_effect(_intent()).state == "interrupted"
        assert resumed.cancel_departure_effect(_intent()).state == "interrupted"
        assert resumed.get_departure_effect(_intent().departure_id).state == "interrupted"
        assert reads == [_intent().departure_id]
        assert resumed.descriptor() == _target_descriptor()
        assert resumed._dispatch.payload_count == 0
        release.set()
        assert _eventually(resumed, "completed").receipt == _receipt(_intent())
    finally:
        release.set()
        resumed.close()


def test_departure_reconciliation_rejects_a_mutated_receipt(tmp_path):
    def uncertain(session):
        raise RuntimeError("external commit response was lost")

    service = PersistentDepartureEffectService(
        descriptor=_target_descriptor(), state_root=tmp_path, execute=uncertain
    )
    service.put_departure_effect(_intent())
    _eventually(service, "interrupted")
    service.close()
    receipt = _receipt(_intent())
    receipt.result["index_action"] = "unbound mutation"
    resumed = PersistentDepartureEffectService(
        descriptor=_target_descriptor(),
        state_root=tmp_path,
        execute=uncertain,
        reconcile=lambda *_: receipt,
    )
    try:
        with pytest.raises(ValueError):
            resumed.get_departure_effect(_intent().departure_id)
        assert resumed._status(_intent().departure_id).state == "interrupted"
    finally:
        resumed.close()


def test_durable_effect_completion_wins_late_cancel_and_cleanup_error(tmp_path: Path) -> None:
    completed = threading.Event()
    release = threading.Event()

    def execute(session: DepartureExecutionSession) -> DepartureEffectReceipt:
        session.record_completed(_receipt(session.intent))
        completed.set()
        assert release.wait(3)
        raise RuntimeError("cleanup failed after the exact effect receipt")

    service = PersistentDepartureEffectService(
        descriptor=_target_descriptor(), state_root=tmp_path, execute=execute
    )
    try:
        service.put_departure_effect(_intent())
        assert completed.wait(2)
        assert service.cancel_departure_effect(_intent()).receipt == _receipt(_intent())
        release.set()
        assert _eventually(service, "completed").receipt == _receipt(_intent())
    finally:
        release.set()
        service.close()
