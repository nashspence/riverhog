from __future__ import annotations

import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from riverhog_age import CHUNK_SIZE
from riverhog_core.catalog_archive_write_models import ArchiveUploadTerminalRecord
from riverhog_core.catalog_db import session_scope
from riverhog_core.provider_write_dispatch import ProviderWriteAdmissionRefused
from riverhog_core.upload_continuation import UploadContinuation, upload_step
from riverhog_protocol.errors import Conflict
from riverhog_storage_adapter_protocol import unknown_write_execution

from tests.unit.test_archive_finalizer_registry import _scope
from tests.unit.test_archive_upload_invocations import _receive
from tests.unit.test_archive_upload_writes import _actor, _open, _stored
from tests.unit.test_collection_uploads import _CREATOR


def _unit(actor, checkpoint):
    arguments = {
        "plan": actor.arguments["plan"],
        "checkpoint": checkpoint,
        "unit_number": 0 if actor.kind == "pack" else 1,
    }
    arguments["payload_chunks" if actor.kind == "pack" else "plaintext"] = (
        (actor.case.content,) if actor.kind == "pack" else actor.case.content
    )
    return actor.uploader.retain_unit(**arguments)


def _checkpoint_loss(actor, monkeypatch, *, committed, after=1):
    original = actor.admission.apply_reports
    calls = []

    def lose(reports, *, apply_result):
        calls.append(reports)
        if len(calls) == after:
            if committed:
                original(reports, apply_result=apply_result)
            raise OSError("injected original checkpoint acknowledgement loss")
        return original(reports, apply_result=apply_result)

    monkeypatch.setattr(actor.admission, "apply_reports", lose)
    return calls


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("committed", (False, True))
def test_retained_open_retries_original_report_without_another_begin(
    tmp_path, monkeypatch, kind, mode, committed
):
    with _actor(tmp_path, monkeypatch, kind, mode=mode) as actor:
        # The completed-state no-effect report precedes the begin checkpoint.
        reports = _checkpoint_loss(actor, monkeypatch, committed=committed, after=2)
        original = actor.uploader.retain_open(**actor.arguments)
        try:
            with pytest.raises(OSError, match="acknowledgement loss"):
                original.run()
            calls = list(actor.seen)
            assert [call.operation.kind for call in calls] == ["lookup", "begin"]
            opened = original.run()
            assert original.run() is opened
            assert not original.has_pending and not actor.journal.has_pending
            assert actor.seen == calls
            assert _stored(actor) == (opened.to_json(), [])
            assert reports[1] == reports[2]
        finally:
            assert original.close()


def _retained_unit_retry(
    tmp_path, monkeypatch, kind, mode, committed, *, database_url=None, checkpoint_at=1, chunks=1
):
    with _actor(
        tmp_path,
        monkeypatch,
        kind,
        mode=mode,
        database_url=database_url,
        content=b"x" * CHUNK_SIZE * chunks,
        unit_plaintext_bytes=CHUNK_SIZE * chunks,
    ) as actor:
        opened = _open(actor)
        prepared = []
        module = __import__(
            "riverhog_core.pack_upload" if kind == "pack" else "riverhog_core.raw_upload",
            fromlist=["prepare_age_part"],
        )
        prepare = module.prepare_age_part

        def once(**arguments):
            prepared.append(arguments)
            return prepare(**arguments)

        monkeypatch.setattr(module, "prepare_age_part", once)
        reports = _checkpoint_loss(actor, monkeypatch, committed=committed, after=checkpoint_at)
        original = _unit(actor, opened)
        assert not prepared
        try:
            # Stop after exactly one real segment; no Python stack unwinds.
            assert not original.advance()
            first = [call for call in actor.seen if call.operation.kind == "part"]
            assert len(first) == 1 and original.has_pending
            budget = actor.uploader._byte_budget
            assert budget.available < budget.capacity
            with pytest.raises(OSError, match="acknowledgement loss"):
                original.run()
            parts = [call for call in actor.seen if call.operation.kind == "part"]
            assert len(parts) >= 2
            assert len(prepared) == 1 and budget.available == budget.capacity
            before_retry = list(actor.seen)
            completed = original.run()
            assert completed.completed is not None
            assert len(prepared) == 1
            assert len(parts) == (completed.completed.bytes + 65535) // 65536
            assert len({call.operation.request.number for call in parts}) == len(parts)
            assert actor.seen[: len(before_retry)] == before_retry
            assert [call for call in actor.seen if call.operation.kind == "part"] == parts
            assert len({call.call.call_id for call in actor.seen}) == len(actor.seen)
            assert reports[checkpoint_at - 1] == reports[checkpoint_at]
            assert not original.has_pending and not actor.journal.has_pending
            assert _stored(actor) == (completed.to_json(), [])
            assert original.run() is completed
        finally:
            assert original.close()


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("committed", (False, True))
@pytest.mark.parametrize("checkpoint_at", (1, 2))
def test_retained_unit_keeps_preparation_segments_and_checkpoint_after_interruption(
    tmp_path, monkeypatch, kind, mode, committed, checkpoint_at
):
    _retained_unit_retry(tmp_path, monkeypatch, kind, mode, committed, checkpoint_at=checkpoint_at)


@pytest.mark.parametrize("chunks", (1, 2, 4))
def test_continuation_work_tracks_bytes_without_reencrypting_or_resending_prefix(
    tmp_path, monkeypatch, chunks
):
    _retained_unit_retry(tmp_path, monkeypatch, "segment", "direct", True, chunks=chunks)


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("mode", ("direct", "owned-http"))
def test_unknown_second_segment_cannot_replay_either_original_segment(
    tmp_path, monkeypatch, kind, mode
):
    with _actor(tmp_path, monkeypatch, kind, mode=mode, content=b"x" * CHUNK_SIZE) as actor:
        opened = _open(actor)
        execute = actor.executor.execute_write
        calls = []

        def unknown(invocation, **arguments):
            response = execute(invocation, **arguments)
            calls.append(invocation)
            if invocation.operation.kind == "part" and len(calls) == 2:
                return unknown_write_execution(invocation)
            return response

        monkeypatch.setattr(actor.executor, "execute_write", unknown)
        original = _unit(actor, opened)
        budget = actor.uploader._byte_budget
        try:
            with pytest.raises(Conflict, match="unknown"):
                original.run()
            before = list(actor.seen)
            assert len(calls) == 2 and actor.journal.has_pending
            assert budget.available < budget.capacity
            for _ in range(3):
                with pytest.raises(Conflict, match="outcome is unresolved"):
                    original.run()
            assert actor.seen == before and len(calls) == 2
        finally:
            assert original.close()
        assert budget.available == budget.capacity
        assert actor.journal.has_pending


@pytest.mark.parametrize("kind", ("pack", "segment"))
def test_known_second_segment_refusal_resumes_the_exact_next_action(tmp_path, monkeypatch, kind):
    with _actor(tmp_path, monkeypatch, kind, content=b"x" * CHUNK_SIZE) as actor:
        opened = _open(actor)
        execute = actor.journal.execute_write_operation
        refused = []
        writes = 0

        def refuse(operation, *, content=b""):
            nonlocal writes
            if operation.kind == "part":
                writes += 1
                if writes == 2:
                    refused.append((operation, content))
                    raise ProviderWriteAdmissionRefused("injected capacity refusal")
                if writes == 3:
                    assert (operation, content) == refused[0]
            return execute(operation, content=content)

        monkeypatch.setattr(actor.journal, "execute_write_operation", refuse)
        original = _unit(actor, opened)
        try:
            with pytest.raises(ProviderWriteAdmissionRefused):
                original.run()
            first = [call for call in actor.seen if call.operation.kind == "part"]
            assert len(first) == 1
            completed = original.run()
            assert completed.completed is not None
            parts = [call for call in actor.seen if call.operation.kind == "part"]
            assert len(parts) >= 2 and parts[0] is first[0]
            assert len({call.operation.request.number for call in parts}) == len(parts)
        finally:
            assert original.close()


def test_close_cannot_release_a_prepared_input_while_its_action_runs():
    entered, release = threading.Event(), threading.Event()
    closed = []

    def action():
        entered.set()
        assert release.wait(5)

    def steps():
        try:
            yield from upload_step(action)
        finally:
            closed.append(True)

    original = UploadContinuation(steps())
    with ThreadPoolExecutor(max_workers=1) as worker:
        running = worker.submit(original.advance)
        try:
            assert entered.wait(5)
            assert not original.close() and not closed
            with pytest.raises(Conflict, match="is running"):
                original.advance()
        finally:
            release.set()
            assert running.result(timeout=5) is False
    assert original.close() and closed == [True]


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("terminal", (False, True))
def test_service_registry_retains_the_exact_logical_unit_after_checkpoint_failure(
    tmp_path, monkeypatch, kind, terminal
):
    with _actor(tmp_path, monkeypatch, kind, content=b"x" * CHUNK_SIZE) as actor:
        opened = _open(actor)
        _checkpoint_loss(actor, monkeypatch, committed=True)
        original = _unit(actor, opened)
        registry = actor.case.service._archive_invocations
        execution = actor.case.service._archive_execution
        key, target = _scope(actor.case)
        request = hashlib.sha256(b"exact original unit").hexdigest()
        created = []

        class Owner:
            @property
            def has_pending(self):
                return original.has_pending or actor.journal.has_pending

            def drain_terminal(self, operation_id):
                assert original.close()
                return actor.journal.drain_terminal(operation_id)

        def create():
            value = Owner()
            created.append(value)
            return value

        def advance(owner):
            assert owner is created[0]
            return original.run()

        try:
            assert (
                registry.submit(
                    key,
                    target_incarnation=target,
                    request_sha256=request,
                    create=create,
                    advance=advance,
                )
                == "submitted"
            )
            (first,) = _receive(registry, execution)
            with pytest.raises(OSError, match="acknowledgement loss"):
                first.result()
            before = list(actor.seen)
            if terminal:
                operation = actor.admission.select_terminal(
                    actor.case.collection_id,
                    kind="cancel",
                    request={"original": request},
                    principal_id=_CREATOR.id,
                    grants=actor.case.service._config.archive_write,
                    guard=actor.case.service._payload_write_guard(
                        actor.case.collection_id, _CREATOR
                    ),
                    validate_selection=lambda session, upload: None,
                )
                assert (
                    registry.fence_terminal(key, target_incarnation=target, operation_id=operation)
                    == "fenced"
                )
                assert (
                    registry.submit_terminal(key, target_incarnation=target, operation_id=operation)
                    == "submitted"
                )
                (received,) = _receive(registry, execution)
                assert received.execution.terminal and received.result() == "closed"
                with pytest.raises(Conflict, match="is closed"):
                    original.run()
                assert actor.seen == before and not actor.journal.has_pending
                with session_scope(actor.case.factory) as session:
                    assert session.get(ArchiveUploadTerminalRecord, operation).phase == "draining"
                return
            assert (
                registry.submit(
                    key,
                    target_incarnation=target,
                    request_sha256=request,
                    create=lambda: pytest.fail("reconstructed original unit"),
                    advance=lambda owner: pytest.fail("replaced original continuation"),
                )
                == "submitted"
            )
            (second,) = _receive(registry, execution)
            assert second.result().completed is not None
            assert actor.seen[: len(before)] == before
            assert len(created) == 1 and registry.retire_idle(second)
        finally:
            assert original.close()


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("mode", ("direct", "owned-http"))
def test_checkpoint_receiving_cannot_start_provider_reconciliation_or_completion(
    tmp_path, monkeypatch, kind, mode
):
    with _actor(tmp_path, monkeypatch, kind, mode=mode, content=b"x" * CHUNK_SIZE) as actor:
        opened = _open(actor)
        original = _unit(actor, opened)
        try:
            # Two physical segments, followed by their original unit checkpoint.
            for _ in range(3):
                assert not original.advance()
            calls = list(actor.seen)
            assert [v.operation.kind for v in calls] == ["lookup", "begin", "part", "part"]
            assert not actor.journal.has_pending
            monkeypatch.setattr(
                actor.provider,
                "list_segments",
                lambda **kwargs: pytest.fail(
                    "terminal receiving started fresh provider reconciliation"
                ),
            )
            assert original.receive_checkpoint() is None
            assert actor.seen == calls
            with pytest.raises(Conflict, match="no completed result"):
                original.result()
        finally:
            assert original.close()
