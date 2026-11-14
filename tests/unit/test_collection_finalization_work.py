from __future__ import annotations

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionRecord
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services.collection_uploads import _FinalizationClaim

from tests.unit.finalization_claim_fixtures import _different_claim
from tests.unit.test_archive_finalizer_registry import _receive
from tests.unit.test_archive_provenance_journal_writer import _lose_ack
from tests.unit.test_archive_volume_metadata_receiving import _count
from tests.unit.test_collection_finalization_plan import _metadata_case, _until
from tests.unit.test_finalization_claim_readiness import _claim_snapshot


def _completed(case):
    service = case.service
    (result,) = _receive(service._archive_finalizers, service._archive_execution)
    return result.result()


def _forbidden(*args, **kwargs):
    pytest.fail("an original pipeline or workflow reply was replaced")


def test_actual_application_workers_finish_one_claim_before_selecting_the_next(
    tmp_path, monkeypatch
):
    with _metadata_case(tmp_path, monkeypatch, count=1) as (case, claim):
        claims = set()
        for _ in range(256):
            assert claim.claim_id not in claims
            claims.add(claim.claim_id)
            assert case.service._submit_finalization(claim) == "submitted"
            turn = _completed(case)
            assert turn.selection == "collection-finalization" and turn.progressed
            with session_scope(case.factory) as session:
                if session.get(Upload, case.collection_id) is None:
                    assert session.get(CollectionRecord, case.collection_id).is_published
                    break
                upload = session.get(Upload, case.collection_id)
                assert upload.archive_claim_id is None
                assert upload.archive_phase == "finalization_queued"
            claim = case.service._claim_due_finalization()
            assert claim is not None
        else:
            pytest.fail("the worker-driven metadata plan did not publish")
        context = case.service._archive_finalizers._contexts[case.collection_id]
        assert not context.owner.has_pending
        assert case.service._archive_finalizers.retire_idle(
            context.key, target_incarnation=context.target
        )
        assert _count(case) == 0 and len(claims) > 13


def test_original_pipeline_feedback_survives_replacement_claim_and_lost_reply(
    tmp_path, monkeypatch
):
    with _metadata_case(tmp_path, monkeypatch, count=1) as (case, claim):
        actual = case.service._requeue_finalization_step
        received = []

        def lost(selected):
            received.append(selected)
            actual(selected)
            if len(received) == 1:
                raise OSError("injected application workflow reply loss")

        monkeypatch.setattr(case.service, "_requeue_finalization_step", lost)
        assert case.service._submit_finalization(claim) == "submitted"
        with pytest.raises(OSError, match="workflow reply loss"):
            _completed(case)
        context = case.service._archive_finalizers._contexts[case.collection_id]
        retained = context.owner
        original = retained._original
        assert retained.has_pending and not original.has_pending
        replacement = case.service._claim_due_finalization()
        assert replacement is not None and replacement.claim_id != claim.claim_id
        snapshot, calls = _claim_snapshot(case), list(case.calls)
        with monkeypatch.context() as patch:
            patch.setattr(case.service, "_finalization_owner", _forbidden)
            patch.setattr(original, "advance", _forbidden)
            assert case.service._submit_finalization(replacement) == "submitted"
            assert _completed(case).progressed
        assert context.owner is retained and not retained.has_pending
        assert received == [claim, claim]
        assert _claim_snapshot(case) == snapshot and case.calls == calls


@pytest.mark.parametrize("committed", (False, True))
def test_failed_original_metadata_worker_keeps_claim_until_actual_receiving_and_feedback(
    tmp_path, monkeypatch, committed
):
    with _metadata_case(tmp_path, monkeypatch, count=1) as (case, claim):
        _until(case, claim, lambda upload: upload.archive_tree_sha256 is not None)
        with _lose_ack(monkeypatch, "report", committed):
            assert case.service._submit_finalization(claim) == "submitted"
            with pytest.raises(OSError):
                _completed(case)
        context = case.service._archive_finalizers._contexts[case.collection_id]
        retained, pipeline = context.owner, context.owner._original
        original = pipeline._original
        assert retained.has_pending and pipeline.has_pending and original.has_pending
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            assert upload.archive_claim_id == claim.claim_id
            assert upload.archive_phase == "finalizing" and upload.archive_next_attempt_at is None
        calls = list(case.calls)
        different = _FinalizationClaim(case.collection_id, _different_claim(claim))
        with monkeypatch.context() as patch:
            patch.setattr(case.service, "_finalization_owner", _forbidden)
            assert case.service._submit_finalization(different) == "submitted"
            turn = _completed(case)
        assert turn.progressed and context.owner is retained
        assert not retained.has_pending and pipeline._original is original
        assert case.calls == calls and _count(case) == 0
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            assert upload.archive_phase == "finalization_queued" and upload.archive_claim_id is None
