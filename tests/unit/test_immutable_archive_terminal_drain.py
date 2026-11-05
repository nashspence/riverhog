from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_core.services.immutable_archive_receiving import drain_original_immutable_report
from riverhog_protocol.errors import Conflict

from tests.unit.test_archive_initial_description_checkpoint import (
    _description_source,
    _publish,
    _received,
    _snapshot,
)
from tests.unit.test_archive_provenance_journal_writer import _lose_ack
from tests.unit.test_archive_volume_metadata_receiving import _count


def _terminal(f):
    return ArchiveWriteAdmission(f.case.factory).select_terminal(
        f.case.collection_id,
        kind="cancel",
        request={"fixture": "immutable-terminal"},
        principal_id="uploader",
        grants=f.grants,
        guard=lambda session: None,
        validate_selection=lambda *args: None,
    )


def _assert_stopped(f, operation):
    assert f.actor.drain_terminal(operation) == "closed"
    assert not f.actor.has_pending and _count(f.case) == 0
    with pytest.raises(Conflict, match="another terminal request"):
        f.actor.drain_terminal("f" * 64)


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_idle_immutable_actor_stops_before_any_future_provider_dispatch(
    tmp_path, monkeypatch, mode
):
    with _description_source(tmp_path, monkeypatch, mode=mode) as f:
        operation = _terminal(f)
        before = _snapshot(f)
        _assert_stopped(f, operation)
        with pytest.raises(Conflict, match="stopped for its terminal"):
            _publish(f)
        assert _snapshot(f) == before and _received(f) is None
        assert {k: len(v) for k, v in f.calls.items()} == f.before_calls
        with session_scope(f.case.factory) as session:
            session.delete(session.get(Upload, f.case.collection_id))
        # The exact local closure acknowledgement precedes later owner lookup.
        assert f.actor.drain_terminal(operation) == "closed"


def test_immutable_actor_requires_exact_terminal_selection_before_installing_fence(
    tmp_path, monkeypatch
):
    with _description_source(tmp_path, monkeypatch) as f:
        with pytest.raises(Conflict, match="not current"):
            f.actor.drain_terminal("f" * 64)
        assert f.actor._terminal_operation_id is None
        assert _publish(f) == _received(f)


def _busy(f, monkeypatch):
    started, release = threading.Event(), threading.Event()
    execute = f.ports.execute_write

    def held(invocation, *, content):
        started.set()
        assert release.wait(10)
        return execute(invocation, content=content)

    with monkeypatch.context() as patch:
        patch.setattr(f.ports, "execute_write", held)
        with ThreadPoolExecutor(max_workers=2) as pool:
            forward = pool.submit(_publish, f)
            try:
                assert started.wait(10)
                operation = _terminal(f)
                drain = pool.submit(f.actor.drain_terminal, operation)
                assert drain.result(timeout=5) == "busy" and not forward.done()
            finally:
                release.set()
            physical = forward.result(timeout=10)
    before = _snapshot(f)
    _assert_stopped(f, operation)
    assert _received(f) == physical and _snapshot(f) == before
    assert len(f.calls["execute"]) == f.before_calls["execute"] + 1


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_running_immutable_dispatch_is_observed_without_waiting_or_declaring_closure(
    tmp_path, monkeypatch, mode
):
    with _description_source(tmp_path, monkeypatch, mode=mode) as f:
        _busy(f, monkeypatch)


def _unused(f, monkeypatch):
    def interrupted(*args, **kwargs):
        raise OSError("injected observer interruption")

    def unacknowledged(*args, **kwargs):
        raise OSError("injected unused bookkeeping interruption")

    with monkeypatch.context() as patch:
        patch.setattr(f.ports, "observe_immutable_object", interrupted)
        patch.setattr(f.actor._admission, "withdraw", unacknowledged)
        with pytest.raises(OSError, match="observer interruption"):
            _publish(f)
    assert f.actor.has_pending and f.actor._admitted is not None
    assert _count(f.case) == 1 and _received(f) is None
    return _terminal(f)


@pytest.mark.parametrize("lost_reply", (False, True))
def test_terminal_immutable_drain_withdraws_only_the_original_unused_live_ticket(
    tmp_path, monkeypatch, lost_reply
):
    with _description_source(tmp_path, monkeypatch) as f:
        operation = _unused(f, monkeypatch)
        before = _snapshot(f)
        if lost_reply:
            withdraw = f.actor._admission.withdraw

            def lost(*args, **kwargs):
                withdraw(*args, **kwargs)
                raise OSError("injected withdrawal reply loss")

            with monkeypatch.context() as patch:
                patch.setattr(f.actor._admission, "withdraw", lost)
                with pytest.raises(OSError, match="withdrawal reply loss"):
                    f.actor.drain_terminal(operation)
        _assert_stopped(f, operation)
        with pytest.raises(Conflict, match="stopped for its terminal"):
            _publish(f)
        assert _snapshot(f) == before and _received(f) is None
        assert {k: len(v) for k, v in f.calls.items()} == f.before_calls


@pytest.mark.parametrize("committed", (False, True))
def test_unconfirmed_immutable_admission_cannot_supply_terminal_closure(
    tmp_path, monkeypatch, committed
):
    with _description_source(tmp_path, monkeypatch) as f:
        arm = f.actor._admission.arm

        def lost(*args, **kwargs):
            if committed:
                arm(*args, **kwargs)
            raise OSError("injected admission acknowledgement loss")

        with monkeypatch.context() as patch:
            patch.setattr(f.actor._admission, "arm", lost)
            with pytest.raises(OSError, match="admission acknowledgement loss"):
                _publish(f)
        operation = _terminal(f)
        assert f.actor.drain_terminal(operation) == "pending" and f.actor.has_pending
        assert _count(f.case) == int(committed)
        assert {k: len(v) for k, v in f.calls.items()} == f.before_calls
        with pytest.raises(Conflict, match="stopped for its terminal"):
            _publish(f)


@pytest.mark.parametrize("phase", ("report", "retirement"))
@pytest.mark.parametrize("committed", (False, True))
def test_terminal_immutable_drain_receives_original_results_before_current_forward_rights(
    tmp_path, monkeypatch, phase, committed
):
    with _description_source(tmp_path, monkeypatch) as f:
        with _lose_ack(monkeypatch, phase, committed):
            with pytest.raises(OSError, match="injected journal writer"):
                _publish(f)
        response = f.actor._response_json
        operation = _terminal(f)
        with session_scope(f.case.factory) as session:
            upload = session.get(Upload, f.case.collection_id)
            upload.state = upload.archive_phase = "orphaned"
            upload.description = "changed current selection"
            upload.final_authority_json = None
        before = _snapshot(f)
        _assert_stopped(f, operation)
        assert f.actor._response_json == response and _received(f) is not None
        assert _snapshot(f) == before and _count(f.case) == 0


def _unknown(f, monkeypatch, returned):
    execute = f.ports.execute_write

    def uncertain(invocation, *, content):
        response = execute(invocation, content=content)
        if not returned:
            raise OSError("injected provider reply loss")
        return response.model_copy(update={"execution": "unknown"})

    with monkeypatch.context() as patch:
        patch.setattr(f.ports, "execute_write", uncertain)
        with pytest.raises((Conflict, OSError)):
            _publish(f)
    operation = _terminal(f)
    if returned:
        with pytest.raises(Conflict, match="execution remains unknown"):
            f.actor.drain_terminal(operation)
        assert _received(f) is not None
    else:
        assert f.actor.drain_terminal(operation) == "pending"
    assert f.actor.has_pending and _count(f.case) == 1
    assert len(f.calls["execute"]) == f.before_calls["execute"] + 1


@pytest.mark.parametrize("returned", (False, True))
@pytest.mark.parametrize("mode", ("direct", "http"))
def test_terminal_immutable_drain_preserves_unknown_execution_even_with_positive_data(
    tmp_path, monkeypatch, returned, mode
):
    with _description_source(tmp_path, monkeypatch, mode=mode) as f:
        _unknown(f, monkeypatch, returned)


def test_cold_receiver_and_terminal_actor_drain_share_original_facts(tmp_path, monkeypatch):
    with _description_source(tmp_path, monkeypatch) as f:
        with _lose_ack(monkeypatch, "report", True):
            with pytest.raises(OSError, match="injected journal writer"):
                _publish(f)
        operation = _terminal(f)
        assert drain_original_immutable_report(f.case.factory, f.case.collection_id)
        _assert_stopped(f, operation)
        assert _received(f) is not None


def test_terminal_drain_keeps_an_original_compatible_observation_without_new_io(
    tmp_path, monkeypatch
):
    from riverhog_age import encrypt_age_scrypt

    with _description_source(tmp_path, monkeypatch) as f:
        publication = f.source.publication
        original = f.physical.put_immutable_object(
            object_path=publication.object_path,
            content=encrypt_age_scrypt(publication.plaintext, "fixture", log_n=1),
            content_type=publication.content_type,
            required_identity_assertions=publication.required_identity_assertions,
            placement_policy="immediate_default",
        )

        def interrupted(*args, **kwargs):
            raise OSError("injected observation withdrawal interruption")

        with monkeypatch.context() as patch:
            patch.setattr(f.actor._admission, "withdraw", interrupted)
            with pytest.raises(OSError, match="observation withdrawal interruption"):
                _publish(f)
        assert f.actor._observed_json is not None and _received(f) is None
        calls = {k: len(v) for k, v in f.calls.items()}
        _assert_stopped(f, _terminal(f))
        assert _received(f).stored_sha256 == original.stored_sha256
        assert {k: len(v) for k, v in f.calls.items()} == calls
