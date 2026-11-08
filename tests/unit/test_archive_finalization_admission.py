from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services.archive_finalization_admission import OriginalFinalizationAdmission
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission, _writer
from sqlalchemy import select
from sqlalchemy.exc import OperationalError

from tests.unit.finalization_claim_fixtures import _different_claim
from tests.unit.test_archive_finalizer_registry import _receive
from tests.unit.test_collection_finalization_plan import _metadata_case
from tests.unit.test_finalization_claim_readiness import _claim_snapshot


class _Ready(threading.Event):
    def __init__(self):
        super().__init__()
        self.waiting = threading.Event()

    def wait(self, timeout=None):
        self.waiting.set()
        return super().wait(timeout)


def _owner(case, claim, ready):
    source = OriginalFinalizationAdmission(
        case.factory, collection_id=case.collection_id, claim_id=claim.claim_id, ready=ready
    )
    owner = case.service._finalization_owner(claim, admission=source)
    constructors = []

    def observe(plan):
        def select_source():
            create = plan.select()
            if create is None:
                return None

            def construct():
                assert ready.is_set() and source._admitted is True
                constructors.append(threading.get_ident())
                return create()

            return construct

        return select_source

    owner._stages = tuple(replace(plan, select=observe(plan)) for plan in owner._stages)
    return source, owner, constructors


@pytest.mark.parametrize("committed", (False, True))
def test_original_claim_handoff_resolves_rollback_and_lost_commit_ack_before_children(
    tmp_path, monkeypatch, committed, database_url=None
):
    with _metadata_case(tmp_path, monkeypatch, count=1, database_url=database_url) as (case, claim):
        proposed = replace(claim, claim_id=_different_claim(claim))
        ready = _Ready()
        source, owner, constructors = _owner(case, proposed, ready)
        before = _claim_snapshot(case)
        with ThreadPoolExecutor(max_workers=1) as workers:
            try:
                with pytest.raises(OSError, match="claim transaction acknowledgement"):
                    with session_scope(case.factory) as session:
                        _writer(session)
                        session.scalar(
                            select(Upload)
                            .where(Upload.collection_id == case.collection_id)
                            .with_for_update()
                        ).archive_claim_id = proposed.claim_id
                        future = workers.submit(owner.advance)
                        assert ready.waiting.wait(5)
                        assert constructors == [] and case.calls == []
                        if not committed:
                            raise OSError("claim transaction acknowledgement")
                    raise OSError("claim transaction acknowledgement")
            finally:
                ready.set()
            assert future.result(timeout=10) is committed
        assert source._admitted is committed and not owner.has_pending
        if committed:
            assert constructors and all(value != threading.get_ident() for value in constructors)
            assert _claim_snapshot(case)[6] == proposed.claim_id
        else:
            assert constructors == [] and _claim_snapshot(case) == before
        assert case.calls == []


@pytest.mark.parametrize("change", ("state", "phase", "claim"))
def test_unadmitted_original_never_reopens_if_a_later_marker_matches(tmp_path, monkeypatch, change):
    with _metadata_case(tmp_path, monkeypatch, count=1) as (case, claim):
        ready = _Ready()
        ready.set()
        source, owner, constructors = _owner(case, claim, ready)
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            if change == "state":
                upload.state = "orphaned"
            elif change == "phase":
                upload.archive_phase = "retry_wait"
            else:
                upload.archive_claim_id = _different_claim(claim)
        assert owner.advance() is False and source._admitted is False
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            upload.state = upload.archive_phase = "finalizing"
            upload.archive_claim_id = claim.claim_id
        assert owner.advance() is False and case.calls == [] and constructors == []


def test_terminal_before_claim_release_prevents_every_semantic_constructor(tmp_path, monkeypatch):
    with _metadata_case(tmp_path, monkeypatch, count=1) as (case, claim):
        ready = _Ready()
        source, owner, constructors = _owner(case, claim, ready)
        with ThreadPoolExecutor(max_workers=1) as workers:
            future = workers.submit(owner.advance)
            try:
                assert ready.waiting.wait(5)
                operation = ArchiveWriteAdmission(case.factory).select_terminal(
                    case.collection_id,
                    kind="cancel",
                    request={"fixture": "claim-handoff"},
                    principal_id="uploader",
                    key_id=None,
                    grants=case.service._config.archive_write,
                    guard=lambda session: None,
                    validate_selection=lambda *args: None,
                )
                assert operation and source._admitted is None and case.calls == []
            finally:
                ready.set()
            assert future.result(timeout=10) is False
        assert source._admitted is False and not owner.has_pending and case.calls == []
        assert constructors == []


def test_contended_claim_observation_stays_in_the_same_retained_pipeline(
    tmp_path, monkeypatch, database_url=None
):
    with _metadata_case(tmp_path, monkeypatch, count=1, database_url=database_url) as (case, claim):
        ready = _Ready()
        source, owner, constructors = _owner(case, claim, ready)
        service = case.service
        received = []
        assert (
            service._archive_finalizer_work._submit(
                case.collection_id,
                "claim-handoff",
                lambda: owner,
                lambda original: original.advance(),
                received.append,
            )
            == "submitted"
        )
        try:
            assert ready.waiting.wait(5)
            with session_scope(case.factory) as session:
                _writer(session)
                session.scalar(
                    select(Upload.collection_id)
                    .where(Upload.collection_id == case.collection_id)
                    .with_for_update()
                )
                ready.set()
                (failed,) = _receive(service._archive_finalizers, service._archive_execution)
                with pytest.raises(OperationalError):
                    failed.result()
        finally:
            ready.set()
        context = service._archive_finalizers._contexts[case.collection_id]
        assert context.owner._original is owner and context.owner.has_pending
        assert source._admitted is None and not owner._construction_failure and received == []
        assert constructors == []
        assert (
            service._archive_finalizers.resume(context.key, target_incarnation=context.target)
            == "submitted"
        )
        (completed,) = _receive(service._archive_finalizers, service._archive_execution)
        assert completed.result().progressed and received == [True]
        assert source._admitted is True and not context.owner.has_pending and constructors
