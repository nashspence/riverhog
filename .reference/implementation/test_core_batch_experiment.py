"""Causal faults in the actual retained effect/checkpoint owners."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest
from core_batch_experiment import CatalogTrace, run_case, stored
from riverhog_core.provider_write_dispatch import ArmedProviderWrite, ProviderWriteReport
from riverhog_protocol.errors import Conflict

from tests.unit.test_archive_upload_writes import _actor, _open, _unit
from tests.unit.test_collection_uploads import _CREATOR


@pytest.mark.parametrize("kind", ("segment", "pack"))
@pytest.mark.parametrize("size", (65536, 262144))
def test_same_logical_checkpoint_has_one_batch_commit_and_all_original_effects(
    tmp_path: Any, kind: Any, size: int
) -> None:
    individual = run_case(
        tmp_path / "individual", mode="individual", kind=kind, plaintext_bytes=size
    )
    batch = run_case(tmp_path / "batch", mode="batch", kind=kind, plaintext_bytes=size)
    assert individual["provider_calls"] == batch["provider_calls"]
    assert individual["provider_request_bytes"] == batch["provider_request_bytes"]
    assert individual["format_result_apply_calls"] == batch["format_result_apply_calls"]
    count = batch["provider_calls"]["part"]
    assert count > 1
    one = individual["transactions"]["by_stage"]
    many = batch["transactions"]["by_stage"]
    assert one["result_part"]["successful_session_commits"] == count
    assert many["result_part"]["successful_session_commits"] == 1
    assert one["result_part"]["early_writer_attempts"] == count
    assert many["result_part"]["early_writer_attempts"] == 1
    for stage in ("effect_admission", "report_receiving_and_retirement"):
        assert one[stage] == many[stage]
    for result in (individual, batch):
        assert result["retained_effects_after_original_receiving"] == 0
        assert result["final_checkpoint_completed"]
        assert result["transactions"]["physical_sync_calls"] is None
        assert all(
            group["effects"] == group["distinct_call_ids"]
            and group["distinct_checkpoint_hashes"] == 1
            for group in result["logical_report_batches"]
        )


@pytest.mark.parametrize("committed", (False, True))
def test_original_batch_survives_rollback_or_lost_reply_without_provider_replay(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, committed: bool
) -> None:
    with _actor(tmp_path, monkeypatch, "segment", content=b"x" * 65536) as actor:
        opened = _open(actor)
        original = actor.admission.apply_reports
        received = []
        applies = []

        def interrupt(reports: tuple[Any, ...], *, apply_result: Any) -> None:
            received.append(reports)

            def apply(session: Any, *args: Any) -> None:
                applies.append(1)
                apply_result(session, *args)
                session.flush()
                if not committed:
                    raise OSError("injected known result rollback")

            original(reports, apply_result=apply)
            raise OSError("injected result commit reply loss")

        with CatalogTrace() as trace, monkeypatch.context() as patch:
            patch.setattr(actor.admission, "apply_reports", interrupt)
            with trace.phase("first_attempt"), pytest.raises(OSError, match="injected"):
                _unit(actor, opened)
            point = trace.snapshot()["by_stage"]["first_attempt"]
        checkpoint, effects = stored(actor)
        assert len(received) == 1 and len(received[0]) > 1
        pending = tuple(actor.journal._pending)
        original_input = actor.journal._report_input
        assert original_input is not None and len(pending) == len(received[0])
        assert actor.journal.has_pending and applies == [1]
        if committed:
            assert checkpoint == original_input[0]
            assert all(state == "settled" and not unresolved for _, state, unresolved in effects)
            assert point["rollback_calls"] == 0
        else:
            assert checkpoint == opened.to_json()
            assert all(state == "armed" and unresolved for _, state, unresolved in effects)
            assert point["rollback_calls"] >= 1
        before = tuple(actor.seen)
        with pytest.raises(Conflict, match="report changed"):
            actor.journal._report(original_input[0] + " ", kind="segment")
        assert tuple(actor.journal._pending) == pending
        replay_applies = []

        def retry(reports: tuple[Any, ...], *, apply_result: Any) -> None:
            assert reports == received[0]

            def apply(*args: Any) -> None:
                replay_applies.append(1)
                apply_result(*args)

            original(reports, apply_result=apply)

        monkeypatch.setattr(actor.admission, "apply_reports", retry)
        actor.journal._report(original_input[0], kind="segment")
        assert replay_applies == ([] if committed else [1])
        assert tuple(actor.seen) == before
        assert stored(actor) == (original_input[0], [])
        assert not actor.journal.has_pending
        for ticket, _ in pending:
            with pytest.raises(Conflict, match="already used"):
                ticket.dispatch(lambda: pytest.fail("original provider dispatch replayed"))


@pytest.mark.parametrize("change", ("duplicate", "volume", "closure-request", "closure-executor"))
def test_mixed_or_changed_report_batch_cannot_commit_any_checkpoint(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    with _actor(tmp_path, monkeypatch, "segment", content=b"x" * 65536) as actor:
        opened = _open(actor)
        original = actor.admission.apply_reports
        captured = []

        def hold(reports: tuple[Any, ...], **kwargs: Any) -> None:
            captured.append(reports)
            raise OSError("retain exact original batch before its result transaction")

        with monkeypatch.context() as patch:
            patch.setattr(actor.admission, "apply_reports", hold)
            with pytest.raises(OSError, match="retain exact"):
                _unit(actor, opened)
        reports = list(captured[0])
        original_input = actor.journal._report_input
        assert original_input is not None and len(reports) > 1
        if change == "duplicate":
            reports[1] = reports[0]
        elif change == "volume":
            old = reports[1].ticket
            foreign = ArmedProviderWrite(
                call_id=old.call_id,
                collection_id=old.collection_id,
                object_id="different-volume",
                nonce=old._nonce,
                request_sha256=old.request_sha256,
                executor_id=old.executor_id,
                target_incarnation=old.target_incarnation,
            )
            reports[1] = replace(reports[1], ticket=foreign)
        else:
            evidence = reports[1].closure
            assert evidence is not None
            evidence = replace(
                evidence,
                **{"request_sha256" if change == "closure-request" else "executor_id": "f" * 64},
            )
            reports[1] = replace(reports[1], closure=evidence)
        with pytest.raises((ValueError, Conflict)):
            original(
                tuple(reports),
                apply_result=lambda *args: pytest.fail("invalid batch applied a checkpoint"),
            )
        checkpoint, effects = stored(actor)
        assert checkpoint == opened.to_json()
        assert all(state == "armed" and unresolved for _, state, unresolved in effects)
        before = tuple(actor.seen)
        actor.journal._report(original_input[0], kind="segment")
        assert tuple(actor.seen) == before
        assert stored(actor) == (original_input[0], [])


def test_positive_unknown_segment_stays_charged_and_blocks_completion(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _actor(tmp_path, monkeypatch, "segment", content=b"x" * 65536) as actor:
        _open(actor)
        execute = actor.executor.execute_write
        original = actor.admission.apply_reports
        captured = []
        selected = []

        def unknown_first_part(invocation: Any, *, content: bytes = b"") -> Any:
            response = execute(invocation, content=content)
            if invocation.operation.kind == "part" and not selected:
                selected.append(invocation.call.call_id)
                return response.model_copy(update={"execution": "unknown"})
            return response

        def capture(reports: tuple[ProviderWriteReport, ...], *, apply_result: Any) -> None:
            captured.append(reports)
            original(reports, apply_result=apply_result)

        monkeypatch.setattr(actor.executor, "execute_write", unknown_first_part)
        monkeypatch.setattr(actor.admission, "apply_reports", capture)
        from riverhog_core.raw_upload import RawUploadCheckpoint

        checkpoint, _ = stored(actor)
        assert checkpoint is not None
        with pytest.raises(Conflict, match="already unresolved"):
            _unit(actor, RawUploadCheckpoint.from_json(checkpoint))
        checkpoint, effects = stored(actor)
        assert len(effects) == 1 and effects[0] == (selected[0], "uncertain", True)
        assert actor.journal.has_pending
        assert actor.admission.unresolved(actor.case.collection_id, limit=1) == tuple(selected)
        assert [item for group in captured for item in group if item.closure is None]
        assert "complete" not in [invocation.operation.kind for invocation in actor.seen]
        assert (
            checkpoint is not None and RawUploadCheckpoint.from_json(checkpoint).completed is None
        )


def test_terminal_cutover_accepts_old_exact_results_and_fences_next_effect(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _actor(tmp_path, monkeypatch, "segment", content=b"x" * 65536) as actor:
        opened = _open(actor)
        original = actor.admission.apply_reports
        selected = []

        def close_gate_then_receive(reports: tuple[Any, ...], *, apply_result: Any) -> None:
            selected.append(
                actor.admission.select_terminal(
                    actor.case.collection_id,
                    kind="cancel",
                    request={"selection": "after-provider-before-original-batch-report"},
                    principal_id=_CREATOR.id,
                    key_id=_CREATOR.key_id,
                    grants=actor.case.service._config.archive_write,
                    guard=actor.case.service._payload_write_guard(
                        actor.case.collection_id, _CREATOR
                    ),
                    validate_selection=lambda *args: None,
                )
            )
            original(reports, apply_result=apply_result)

        monkeypatch.setattr(actor.admission, "apply_reports", close_gate_then_receive)
        with pytest.raises(Conflict, match="admission is closed"):
            _unit(actor, opened)
        checkpoint, effects = stored(actor)
        assert selected and checkpoint != opened.to_json() and effects == []
        assert "complete" not in [invocation.operation.kind for invocation in actor.seen]
        assert actor.admission.unresolved(actor.case.collection_id, limit=1) == ()
        with pytest.raises(Conflict, match="admission is closed"):
            actor.admission.bind_epoch(
                actor.case.collection_id,
                guard=actor.case.service._payload_write_guard(actor.case.collection_id, _CREATOR),
            )
