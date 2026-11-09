from __future__ import annotations

import threading
from contextlib import contextmanager
from dataclasses import replace

import pytest
from riverhog_core.services import collection_uploads as service_module
from riverhog_core.services.archive_payload_input import ArchivePayloadInput, ArchivePayloadInputs
from riverhog_core.services.physical_execution import PhysicalExecutionKey
from riverhog_protocol.errors import Conflict, NotFound

from tests.unit.test_archive_payload_unit import _service_actor
from tests.unit.test_archive_upload_invocations import _receive
from tests.unit.test_collection_uploads import _CREATOR


def _reserve(f, **changes):
    arguments = dict(
        collection_id=f.case.collection_id,
        volume_id=f.case.object_id,
        unit=0,
        plan_sha256=f.case.plan_sha256,
        principal=_CREATOR,
    )
    arguments.update(changes)
    return f.service._reserve_payload_input(**arguments)


def _terminal(f, *, validate=lambda session, upload: None):
    return f.admission.select_terminal(
        f.case.collection_id,
        kind="cancel",
        request={"fixture": f.request},
        principal_id=_CREATOR.id,
        key_id=_CREATOR.key_id,
        grants=f.service._config.archive_write,
        guard=f.service._payload_write_guard(f.case.collection_id, _CREATOR),
        validate_selection=validate,
    )


def _body(f):
    body = _reserve(f)
    assert isinstance(body, ArchivePayloadInput)
    middle = len(f.case.content) // 2
    body.append(f.case.content[:middle])
    body.append(f.case.content[middle:])
    body.seal()
    return body


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("mode", ("direct", "owned-http"))
def test_selected_ingress_transfers_exact_input_to_actual_provider_execution(
    tmp_path, monkeypatch, kind, mode
):
    with _service_actor(tmp_path, monkeypatch, kind, mode) as f:
        f.payload.discard_unretained()
        body = _body(f)
        assert body.request.key == f.key
        assert body.request.target_incarnation == f.target
        assert body.request.principal is _CREATOR
        assert body.request.plaintext_bytes == len(f.case.content)
        assert f.service._archive_inputs._bytes == len(f.case.content)
        assert not f.seen
        assert f.service._submit_payload_input(body) == "submitted"
        (done,) = _receive(f.registry, f.execution)
        assert done.result()["state"] == "committed"
        assert body.released and not f.service._archive_inputs._inputs
        assert [v.operation.kind for v in f.seen] == ["lookup", "begin", "part", "part", "complete"]
        before = list(f.seen)
        duplicate = _body(f)
        assert duplicate.request_sha256 == body.request_sha256
        assert f.service._submit_payload_input(duplicate) == "retained"
        assert duplicate.released and f.seen == before
        assert f.registry.completed(body.request_sha256).result()["state"] == "committed"
        assert f.registry.retire_idle(done)


@pytest.mark.parametrize("committed", (False, True))
@pytest.mark.parametrize("mode", ("direct", "owned-http"))
def test_ingress_retry_keeps_original_preparation_and_discards_only_new_body(
    tmp_path, monkeypatch, committed, mode
):
    _ingress_original_retry(tmp_path, monkeypatch, committed, mode)


def _ingress_original_retry(tmp_path, monkeypatch, committed, mode, *, database_url=None):
    with _service_actor(tmp_path, monkeypatch, "segment", mode, database_url=database_url) as f:
        f.payload.discard_unretained()
        create = f.service._payload_unit_invocation
        originals = []

        def lose_ack(**arguments):
            owner = create(**arguments)
            originals.append(owner)
            prepare = owner._admission.prepare
            attempts = []

            def save(*args, **kwargs):
                attempts.append(None)
                if len(attempts) == 1:
                    if committed:
                        prepare(*args, **kwargs)
                    raise OSError("injected ingress preparation acknowledgement")
                return prepare(*args, **kwargs)

            monkeypatch.setattr(owner._admission, "prepare", save)
            return owner

        monkeypatch.setattr(f.service, "_payload_unit_invocation", lose_ack)
        body = _body(f)
        assert f.service._submit_payload_input(body) == "submitted"
        (failed,) = _receive(f.registry, f.execution)
        with pytest.raises(OSError, match="ingress preparation"):
            failed.result()
        candidate = originals[0]._candidate
        assert candidate is not None and not body.released and not f.seen
        monkeypatch.setattr(
            f.service,
            "_payload_unit_invocation",
            lambda **kwargs: pytest.fail("reconstructed original ingress actor"),
        )
        incoming = _body(f)
        assert incoming.request_sha256 == body.request_sha256
        assert f.service._submit_payload_input(incoming) == "submitted"
        assert incoming.released and not body.released
        (done,) = _receive(f.registry, f.execution)
        assert done.result()["state"] == "committed"
        assert originals[0]._candidate is candidate and len(originals) == 1
        assert body.released and not f.service._archive_inputs._inputs
        assert f.registry.retire_idle(done)


@pytest.mark.parametrize(
    "change",
    (
        {"volume_id": "missing"},
        {"unit": -1},
        {"unit": 99},
        {"unit": True},
        {"plan_sha256": "0" * 64},
        {"principal": replace(_CREATOR, id="different-owner")},
    ),
)
def test_invalid_current_selection_reserves_no_input(tmp_path, monkeypatch, change):
    with _service_actor(tmp_path, monkeypatch, "segment", "direct") as f:
        f.payload.discard_unretained()
        with pytest.raises((Conflict, NotFound)):
            _reserve(f, **change)
        assert not f.service._archive_inputs._inputs and not f.seen


def test_selected_ingress_spends_configured_capacity_before_assembly(tmp_path, monkeypatch):
    with _service_actor(tmp_path, monkeypatch, "segment", "direct") as f:
        f.payload.discard_unretained()
        amount = len(f.case.content)
        grants = replace(
            f.service._config.archive_input,
            inputs=1,
            inputs_per_target=1,
            plaintext_bytes=2 * amount,
            plaintext_bytes_per_target=2 * amount,
        )
        f.service._archive_inputs = ArchivePayloadInputs(grants)
        first = _reserve(f)
        assert isinstance(first, ArchivePayloadInput)
        assert first._buffer is None and f.service._archive_inputs._bytes == 2 * amount
        assert _reserve(f) == "input-capacity"
        first.append(f.case.content[:1])
        first.discard_unretained()
        assert not f.service._archive_inputs._inputs and not f.seen
        second = _reserve(f)
        assert isinstance(second, ArchivePayloadInput)
        second.discard_unretained()


def test_terminal_selected_during_body_assembly_cannot_start_forward_provider_work(
    tmp_path, monkeypatch
):
    _terminal_during_assembly(tmp_path, monkeypatch)


def _terminal_during_assembly(tmp_path, monkeypatch, *, database_url=None):
    with _service_actor(tmp_path, monkeypatch, "segment", "direct", database_url=database_url) as f:
        f.payload.discard_unretained()
        body = _reserve(f)
        assert isinstance(body, ArchivePayloadInput)
        body.append(f.case.content[:1])
        operation = _terminal(f)
        assert (
            f.registry.fence_terminal(f.key, target_incarnation=f.target, operation_id=operation)
            == "untracked"
        )
        assert f.service._archive_inputs.status(f.key) == "retained"
        with pytest.raises(Conflict, match="closed"):
            _reserve(f)
        assert len(f.service._archive_inputs._inputs) == 1
        body.append(f.case.content[1:])
        body.seal()
        assert f.service._submit_payload_input(body) == "submitted"
        (failed,) = _receive(f.registry, f.execution)
        with pytest.raises(Conflict, match="closed"):
            failed.result()
        assert not f.seen and not body.released
        assert (
            f.registry.submit_terminal(f.key, target_incarnation=f.target, operation_id=operation)
            == "submitted"
        )
        (drained,) = _receive(f.registry, f.execution)
        assert drained.result() == "closed" and body.released and not f.seen


def test_ingress_submission_runs_constructor_and_provider_off_the_coordinator(
    tmp_path, monkeypatch
):
    with _service_actor(tmp_path, monkeypatch, "segment", "direct") as f:
        f.payload.discard_unretained()
        body = _body(f)
        entered, release = threading.Event(), threading.Event()
        coordinator = threading.get_ident()
        create = f.service._payload_unit_invocation

        def held(**kwargs):
            assert threading.get_ident() != coordinator
            entered.set()
            assert release.wait(10)
            return create(**kwargs)

        monkeypatch.setattr(f.service, "_payload_unit_invocation", held)
        try:
            assert f.service._submit_payload_input(body) == "submitted"
            assert entered.wait(10)
            assert not f.seen and not body.released
        finally:
            release.set()
        (done,) = _receive(f.registry, f.execution)
        assert done.result()["state"] == "committed" and body.released
        assert f.registry.retire_idle(done)


def test_input_installation_is_interlocked_with_actual_terminal_selection(tmp_path, monkeypatch):
    _input_selection_interlock(tmp_path, monkeypatch)


def _input_selection_interlock(tmp_path, monkeypatch, *, database_url=None):
    with _service_actor(tmp_path, monkeypatch, "segment", "direct", database_url=database_url) as f:
        f.payload.discard_unretained()
        pool = f.service._archive_inputs
        reserve = pool.reserve
        entered, release, terminal_started, terminal_done = (threading.Event() for _ in range(4))
        bodies, operations, observed, errors = [], [], [], []

        def hold(request):
            entered.set()
            assert release.wait(10)
            return reserve(request)

        def assemble():
            try:
                bodies.append(_reserve(f))
            except BaseException as error:
                errors.append(error)

        def select():
            terminal_started.set()
            try:
                operations.append(
                    _terminal(
                        f, validate=lambda session, upload: observed.append(pool.status(f.key))
                    )
                )
            except BaseException as error:
                errors.append(error)
            finally:
                terminal_done.set()

        monkeypatch.setattr(pool, "reserve", hold)
        assembly = threading.Thread(target=assemble)
        terminal = threading.Thread(target=select)
        assembly.start()
        try:
            assert entered.wait(10)
            terminal.start()
            assert terminal_started.wait(10)
            assert not terminal_done.wait(0.1)
        finally:
            release.set()
            assembly.join(10)
            if terminal.ident is not None:
                terminal.join(10)
        assert not assembly.is_alive() and not terminal.is_alive() and not errors
        assert len(bodies) == len(operations) == 1 and observed == ["retained"]
        bodies[0].discard_unretained()
        assert pool.status(f.key) == "untracked" and not f.seen


@pytest.mark.parametrize("committed", (False, True))
def test_lost_selection_transaction_acknowledgement_withdraws_only_unused_input(
    tmp_path, monkeypatch, committed
):
    with _service_actor(tmp_path, monkeypatch, "segment", "direct") as f:
        original = f.payload
        actual = service_module.session_scope

        @contextmanager
        def transaction(factory):
            with actual(factory) as session:
                yield session
                if not committed:
                    raise OSError("injected ingress selection rollback")
            if committed:
                raise OSError("injected ingress selection reply")

        with monkeypatch.context() as patch:
            patch.setattr(service_module, "session_scope", transaction)
            with pytest.raises(OSError, match="ingress selection"):
                _reserve(f)
        pool = f.service._archive_inputs
        assert pool.status(f.key) == "retained"
        assert list(pool._inputs.values()) == [original] and not original.released
        original.discard_unretained()
        assert pool.status(f.key) == "untracked" and not f.seen


def test_input_status_is_nonblocking_and_scoped_to_exact_construction(tmp_path, monkeypatch):
    with _service_actor(tmp_path, monkeypatch, "segment", "direct") as f:
        pool = f.service._archive_inputs
        original = f.payload
        other = PhysicalExecutionKey(f.key.collection_id, "0" * 64)
        assert pool.status(f.key) == "retained" and pool.status(other) == "untracked"
        second = _reserve(f)
        assert isinstance(second, ArchivePayloadInput)
        original.discard_unretained()
        assert pool.status(f.key) == "retained"
        with pool._lock:
            assert pool.status(f.key) == "busy"
        second.discard_unretained()
        assert pool.status(f.key) == "untracked"
