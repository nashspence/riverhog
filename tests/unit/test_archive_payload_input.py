from __future__ import annotations

import gc
import hashlib
import threading
import weakref
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_core.archive_input_grants import ArchiveInputGrants
from riverhog_core.services.archive_payload_input import (
    ArchivePayloadInput,
    ArchivePayloadInputs,
    ArchivePayloadRequest,
)
from riverhog_core.services.archive_upload_execution import ArchiveUploadExecutionKey
from riverhog_protocol.errors import BadRequest, Conflict

from tests.unit.test_archive_upload_invocations import _receive
from tests.unit.test_archive_upload_invocations import owned as owned
from tests.unit.test_collection_uploads import _CREATOR


def _request(**values):
    return replace(
        ArchivePayloadRequest(
            ArchiveUploadExecutionKey(1, "a" * 64),
            "target-a",
            "epoch-a",
            "volume-a",
            0,
            "b" * 64,
            4,
            _CREATOR,
        ),
        **values,
    )


def _body(pool=None, request=None, content=b"data"):
    pool = pool or ArchivePayloadInputs(ArchiveInputGrants())
    request = request or _request(plaintext_bytes=len(content))
    value = pool.reserve(request)
    assert isinstance(value, ArchivePayloadInput)
    value.append(content)
    value.seal()
    return value


def _submit(registry, payload, *, create, advance):
    return registry.submit(
        payload.request.key,
        target_incarnation=payload.request.target_incarnation,
        request_sha256=payload.request_sha256,
        create=create,
        advance=advance,
        payload=payload,
    )


class _Actor:
    def __init__(self, payload):
        self.payload = payload
        payload.bind_actor(self)
        self.has_pending = False

    def advance(self):
        value = self.payload.content()
        self.payload.consumed(self)
        return hashlib.sha256(value).hexdigest()

    def drain_terminal(self, operation_id):
        self.payload.consumed(self)
        return "closed"


def test_ingress_reserves_both_assembly_and_final_copy_before_allocation(monkeypatch):
    import riverhog_core.services.archive_payload_input as source

    pool = ArchivePayloadInputs(
        ArchiveInputGrants(
            inputs=4, inputs_per_target=2, plaintext_bytes=16, plaintext_bytes_per_target=8
        )
    )
    monkeypatch.setattr(
        source, "bytearray", lambda size: pytest.fail("allocated before body read"), raising=False
    )
    first = pool.reserve(_request())
    assert isinstance(first, ArchivePayloadInput) and first._buffer is None
    assert pool._bytes == 8 and pool._targets == {"target-a": (1, 8)}
    assert pool.reserve(_request()) == "target-input-capacity"
    second = pool.reserve(_request(target_incarnation="target-b"))
    assert isinstance(second, ArchivePayloadInput)
    assert pool.reserve(_request(target_incarnation="target-c")) == "input-capacity"
    first.discard_unretained()
    second.discard_unretained()
    assert pool._bytes == 0 and not pool._inputs and not pool._targets


@pytest.mark.parametrize("size", (0, 64 * 1024, 128 * 1024, 256 * 1024))
def test_stream_hashes_each_input_byte_once_and_keeps_only_scalar_identity_after_release(
    size, monkeypatch
):
    import riverhog_core.services.archive_payload_input as source

    hashes = []
    actual_hash = hashlib.sha256

    class CountedHash:
        def __init__(self, content=b""):
            self.original = actual_hash(content)
            self.bytes = len(content)
            hashes.append(self)

        def update(self, content):
            self.bytes += len(content)
            self.original.update(content)

        def hexdigest(self):
            return self.original.hexdigest()

    monkeypatch.setattr(source, "hashlib", SimpleNamespace(sha256=CountedHash))
    pool = ArchivePayloadInputs(ArchiveInputGrants())
    value = pool.reserve(_request(plaintext_bytes=size))
    assert isinstance(value, ArchivePayloadInput)
    content = b"x" * size
    for start in range(0, size, 16 * 1024):
        value.append(content[start : start + 16 * 1024])
    value.seal()
    assert hashes[0].bytes == size and len(hashes) == 2 and value._hash is None
    assert value.payload_sha256 == actual_hash(content).hexdigest()
    assert value.content() is value.content() and value.content() == content
    assert pool._bytes == size
    identity = value.request_sha256
    value.discard_unretained()
    assert value.released and value.request_sha256 == identity
    assert value.payload_sha256 == actual_hash(content).hexdigest()
    assert not pool._inputs and pool._bytes == 0
    with pytest.raises(Conflict, match="unavailable"):
        value.content()


@pytest.mark.parametrize(
    "change",
    (
        {"key": ArchiveUploadExecutionKey(2, "a" * 64)},
        {"key": ArchiveUploadExecutionKey(1, "c" * 64)},
        {"target_incarnation": "target-b"},
        {"epoch": "epoch-b"},
        {"volume_id": "volume-b"},
        {"unit": 1},
        {"plan_sha256": "c" * 64},
        {"plaintext_bytes": 3},
        {"principal": replace(_CREATOR, key_id="other-key")},
        {"principal": replace(_CREATOR, authorization_view_identity="changed-view")},
        {"principal": replace(_CREATOR, access=frozenset())},
        {"principal": replace(_CREATOR, artifact_scope_capability_id="other-scope")},
        {"principal": replace(_CREATOR, unrestricted_delegation=True)},
    ),
)
def test_semantic_commitment_binds_original_arguments_and_authorization(change):
    request = _request()
    assert request.commitment("d" * 64) != replace(request, **change).commitment("d" * 64)
    assert request.commitment("d" * 64) != request.commitment("e" * 64)


def test_empty_bodies_still_spend_input_slots_and_partial_invalid_bodies_release_only_themselves():
    pool = ArchivePayloadInputs(ArchiveInputGrants(inputs=2, inputs_per_target=1))
    zero = _body(pool, _request(plaintext_bytes=0), content=b"")
    assert pool.reserve(_request(plaintext_bytes=0)) == "target-input-capacity"
    partial = pool.reserve(_request(target_incarnation="target-b"))
    assert isinstance(partial, ArchivePayloadInput)
    partial.append(b"da")
    with pytest.raises(BadRequest):
        partial.seal()
    with pytest.raises(BadRequest):
        partial.append(b"overflow")
    partial.discard_unretained()
    assert partial._hash is None
    assert len(pool._inputs) == 1 and pool._bytes == 0
    zero.discard_unretained()
    assert not pool._inputs


def test_input_release_requires_actual_original_worker_join(owned, monkeypatch):
    import riverhog_core.services.archive_upload_execution as source

    registry, execution, retention = owned.make()
    entered, release = threading.Event(), owned.event()

    class HeldExit(threading.Thread):
        def run(self):
            super().run()
            entered.set()
            assert release.wait(timeout=10)

    monkeypatch.setattr(source, "Thread", HeldExit)
    payload = _body()
    assert (
        _submit(
            registry, payload, create=lambda: _Actor(payload), advance=lambda owner: owner.advance()
        )
        == "submitted"
    )
    assert entered.wait(timeout=10)
    assert not payload.released and payload._pool._bytes == 4
    assert registry.collect(limit=1) == ()
    assert not payload.released
    release.set()
    (outcome,) = _receive(registry, execution)
    assert outcome.result() == hashlib.sha256(b"data").hexdigest()
    assert payload.released and payload._pool._bytes == 0
    assert registry.retire_idle(outcome) and not retention._reservations


def test_retry_discards_only_new_unused_body_and_keeps_original_callback_and_input(owned):
    registry, execution, _ = owned.make()
    first = _body()
    actor = _Actor(first)
    calls = []

    def advance(owner):
        calls.append(owner)
        if len(calls) == 1:
            raise OSError("known preparation acknowledgement loss")
        return owner.advance()

    assert _submit(registry, first, create=lambda: actor, advance=advance) == "submitted"
    (failed,) = _receive(registry, execution)
    with pytest.raises(OSError):
        failed.result()
    assert not first.released
    new = _body()
    assert new.request_sha256 == first.request_sha256
    assert (
        _submit(
            registry,
            new,
            create=lambda: pytest.fail("reconstructed owner"),
            advance=lambda owner: pytest.fail("replaced original callback"),
        )
        == "submitted"
    )
    assert new.released and not first.released
    (success,) = _receive(registry, execution)
    assert success.result() == hashlib.sha256(b"data").hexdigest()
    assert first.released and calls == [actor, actor]
    assert registry.retire_idle(success)


@pytest.mark.parametrize("started", (False, True))
def test_worker_start_acknowledgement_preserves_exact_input_or_positive_nonstart_withdrawal(
    owned, monkeypatch, started
):
    import riverhog_core.services.archive_upload_execution as source

    registry, execution, retention = owned.make()
    payload = _body()
    actual = threading.Thread

    class StartAcknowledgement(actual):
        def start(self):
            if started:
                super().start()
            raise OSError("worker startup acknowledgement")

    monkeypatch.setattr(source, "Thread", StartAcknowledgement)
    with pytest.raises(OSError, match="startup acknowledgement"):
        _submit(
            registry, payload, create=lambda: _Actor(payload), advance=lambda owner: owner.advance()
        )
    assert payload.released is not started
    if started:
        (received,) = _receive(registry, execution)
        assert received.result() == hashlib.sha256(b"data").hexdigest()
        assert payload.released and registry.retire_idle(received)
    assert not retention._reservations and not payload._pool._inputs


def test_unknown_constructor_keeps_input_but_joined_receiving_retires_exception_frame_copies(owned):
    registry, execution, _ = owned.make()
    payload = _body()
    refs, errors = [], []

    class Local:
        def __init__(self):
            self.content = payload.content()

    def create():
        local = Local()
        refs.append(weakref.ref(local))
        try:
            raise OSError("original inner failure")
        except OSError as cause:
            error = RuntimeError("unknown original constructor")
            errors.append(error)
            raise error from cause

    assert _submit(registry, payload, create=create, advance=lambda owner: None) == "submitted"
    worker = execution._slots[payload.request.key.collection_id].worker
    worker.join(timeout=10)
    assert not worker.is_alive() and refs[0]() is not None
    (received,) = registry.collect(limit=1)
    gc.collect()
    assert refs[0]() is None
    with pytest.raises(RuntimeError) as raised:
        received.result()
    assert raised.value is errors[0] and isinstance(raised.value.__cause__, OSError)
    assert not payload.released and payload.content() == b"data"


def test_invalid_submission_and_capacity_refusal_withdraw_only_the_new_input(owned):
    registry, execution, retention = owned.make(forward_workers=1)
    release = owned.event()

    entered = threading.Event()

    def hold(owner):
        entered.set()
        return release.wait(timeout=10)

    assert (
        registry.submit(
            ArchiveUploadExecutionKey(2, "a" * 64),
            target_incarnation="target-b",
            request_sha256="b" * 64,
            create=lambda: SimpleNamespace(has_pending=False),
            advance=hold,
        )
        == "submitted"
    )
    assert entered.wait(timeout=10)
    payload = _body()
    assert (
        _submit(
            registry,
            payload,
            create=lambda: pytest.fail("over capacity"),
            advance=lambda owner: None,
        )
        == "capacity"
    )
    assert payload.released and len(retention._reservations) == 1
    invalid = _body()
    with pytest.raises(Conflict):
        registry.submit(
            invalid.request.key,
            target_incarnation="other",
            request_sha256=invalid.request_sha256,
            payload=invalid,
            create=lambda: pytest.fail("invalid identity"),
            advance=lambda owner: None,
        )
    assert invalid.released
    release.set()
    (done,) = _receive(registry, execution)
    assert registry.retire_idle(done)


def test_another_registry_cannot_withdraw_the_original_input(owned):
    first, execution, _ = owned.make()
    other, _, _ = owned.make()
    payload = _body()
    entered, release = threading.Event(), owned.event()

    def hold(owner):
        entered.set()
        assert release.wait(timeout=10)
        return owner.advance()

    assert _submit(first, payload, create=lambda: _Actor(payload), advance=hold) == "submitted"
    assert entered.wait(timeout=10)
    with pytest.raises(Conflict):
        _submit(
            other,
            payload,
            create=lambda: pytest.fail("foreign registry"),
            advance=lambda owner: None,
        )
    assert not payload.released
    release.set()
    (done,) = _receive(first, execution)
    assert payload.released and first.retire_idle(done)


def test_unretained_or_foreign_service_input_cannot_start_actual_payload_work():
    value = _body()
    actor = _Actor(value)
    with pytest.raises(Conflict, match="retained original"):
        value.require_actor(actor)
    with pytest.raises(Conflict, match="another service"):
        value.require_pool(ArchivePayloadInputs(ArchiveInputGrants()))
    value.require_pool(value._pool)
    value.discard_unretained()
    with pytest.raises(Conflict, match="retained original"):
        value.require_actor(actor)


def test_only_a_positive_original_pool_reservation_can_allocate_ingress_bytes(monkeypatch):
    import riverhog_core.services.archive_payload_input as source

    pool = ArchivePayloadInputs(ArchiveInputGrants())
    fabricated = ArchivePayloadInput(pool, _request(), 8)
    monkeypatch.setattr(
        source,
        "bytearray",
        lambda size: pytest.fail("allocated without a reservation"),
        raising=False,
    )
    with pytest.raises(Conflict, match="original reservation"):
        fabricated.append(b"data")
    assert pool._bytes == 0 and not pool._inputs
