"""Departure effect target transport and idempotent withdrawal witness."""

from __future__ import annotations

import httpx
import pytest
from riverhog_protocol import CatalogSyncDescriptor
from stove0_target_client import DepartureEffectClient, TargetProtocolError
from stove0_target_protocol import (
    DepartureEffectIntent,
    DepartureEffectIntentPayload,
    DepartureEffectReceipt,
    DepartureEffectReceiptPayload,
    DepartureEffectTargetDescriptor,
    DepartureEffectTargetDescriptorPayload,
)
from stove0_target_support import DepartureEffectHttpBinding


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
                content_identity="5" * 64,
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


class _WithdrawalTarget:
    def __init__(self) -> None:
        self.effects: set[str] = set()
        self.receipts: dict[str, DepartureEffectReceipt] = {}

    def descriptor(self) -> DepartureEffectTargetDescriptor:
        return _target_descriptor()

    def put_departure_effect(self, intent: DepartureEffectIntent) -> DepartureEffectReceipt:
        self.effects.add(intent.departure_id)
        return self.receipts.setdefault(
            intent.departure_id,
            DepartureEffectReceipt.seal(
                DepartureEffectReceiptPayload(
                    departure_id=intent.departure_id,
                    target_identity=self.descriptor().target_identity,
                    result={"index_action": "withdrawn"},
                )
            ),
        )


def test_departure_target_binding_accepts_only_exact_sealed_intent() -> None:
    intent = _intent()
    target = _WithdrawalTarget()
    binding = DepartureEffectHttpBinding(target)
    path = f"/v1/departure-effects/{intent.departure_id}"
    body = intent.model_dump_json().encode("utf-8")
    assert (
        DepartureEffectTargetDescriptor.model_validate_json(
            binding.handle("GET", "/v1/departure-target").body
        )
        == target.descriptor()
    )
    first = binding.handle("PUT", path, body)
    replay = binding.handle("PUT", path, body)
    assert first.status == replay.status == 200
    assert first.body == replay.body
    assert target.effects == {intent.departure_id}
    assert binding.handle("PUT", f"/v1/departure-effects/{'0' * 64}", body).status == 400
    assert binding.handle("PUT", path, b"{}").status == 400
    wrong_target = DepartureEffectIntent.seal(
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
            f"/v1/departure-effects/{wrong_target.departure_id}",
            wrong_target.model_dump_json().encode("utf-8"),
        ).status
        == 409
    )
    assert target.effects == {intent.departure_id}


def test_departure_client_retries_same_identity_after_lost_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    intent = _intent()
    target = _WithdrawalTarget()
    binding = DepartureEffectHttpBinding(target)
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
        httpx,
        "Client",
        lambda **_kwargs: real_client(transport=httpx.MockTransport(respond)),
    )
    client = DepartureEffectClient("https://index.example", token="fixture-token")
    with pytest.raises(TargetProtocolError) as error:
        client.put_effect(intent)
    assert error.value.failure_kind == "transport"
    receipt = client.put_effect(intent)
    assert receipt == target.receipts[intent.departure_id]
    assert replies == 4
    assert target.effects == {intent.departure_id}
