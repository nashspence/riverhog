from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord
from riverhog_core.catalog_db import dispose_session_factory, session_scope
from riverhog_core.catalog_models import CollectionUploadRecord
from riverhog_core.catalog_workflow_models import CollectionProcessingClaimRecord
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission, _writer
from riverhog_core.services.collection_upload_authority import retain_inactive_upload_claim
from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy import select

from tests.unit.test_collection_registration_delegation import _delegated
from tests.unit.test_collection_registration_operations import _close


@contextmanager
def _output(tmp_path):
    service, keys, workflows, principal, collection_id = _delegated(tmp_path)
    try:
        owner = ArchiveWriteAdmission(service._session_factory)
        owner.bind_epoch(collection_id, guard=lambda session: None)
        with session_scope(service._session_factory) as session:
            claim_id = session.scalar(
                select(CollectionProcessingClaimRecord.id).where(
                    CollectionProcessingClaimRecord.execution_id
                    == principal.id.removeprefix("processing:")
                )
            )
        yield SimpleNamespace(
            service=service,
            principal=principal,
            collection_id=collection_id,
            claim_id=claim_id,
            factory=service._session_factory,
            owner=owner,
        )
    finally:
        dispose_session_factory(workflows._session_factory)
        _close(service, keys)


def _guard(case):
    def guard(session):
        assert not any(
            isinstance(row, CollectionUploadRecord) for row in session.identity_map.values()
        )
        assert (
            retain_inactive_upload_claim(session, case.principal.id, nowait=True) == case.claim_id
        )

    return guard


def _select(case):
    return case.owner.select_terminal(
        case.collection_id,
        kind="discard",
        request={"challenge": "original-discard-scope"},
        principal_id="deletion-operator",
        grants=case.service._config.archive_write,
        guard=_guard(case),
        validate_selection=lambda session, upload: None,
    )


def test_live_output_claim_blocks_terminal_selection_before_gate_closure(tmp_path):
    with _output(tmp_path) as case:
        with session_scope(case.factory) as session:
            control = session.get(ArchiveWriteControlRecord, case.collection_id)
            original = (control.gate, control.epoch, control.terminal_id)
        with pytest.raises(Conflict, match="claim remains active"):
            _select(case)
        with session_scope(case.factory) as session:
            control = session.get(ArchiveWriteControlRecord, case.collection_id)
            assert (control.gate, control.epoch, control.terminal_id) == original


@pytest.mark.parametrize("inactive", ("expired", "settled"))
def test_inactive_original_claim_is_retained_through_selected_terminal_observation(
    tmp_path, inactive
):
    with _output(tmp_path) as case:
        with session_scope(case.factory) as session:
            claim = session.get(CollectionProcessingClaimRecord, case.claim_id)
            if inactive == "expired":
                claim.expires_at = "2000-01-01T00:00:00Z"
            else:
                claim.state = "settled"
        operation = _select(case)
        with session_scope(case.factory) as session:
            observed = case.service._archive_terminal_observation.observe(
                session, case.collection_id, operation, guard=_guard(case)
            )
            assert observed.records.operation_id == operation
            assert observed.execution == observed.inputs == "untracked"
        with session_scope(case.factory) as session:
            control = session.get(ArchiveWriteControlRecord, case.collection_id)
            claim = session.get(CollectionProcessingClaimRecord, case.claim_id)
            assert control.gate == "closed" and control.terminal_id == operation
            assert claim.state == ("active" if inactive == "expired" else "settled")


def test_absent_original_processing_claim_does_not_establish_inactivity(tmp_path):
    with _output(tmp_path) as case:
        with session_scope(case.factory) as session:
            _writer(session)
            with pytest.raises(NotFound, match="output claim is unavailable"):
                retain_inactive_upload_claim(session, "processing:" + "f" * 64, nowait=True)


def test_ordinary_creator_needs_no_processing_claim_row(tmp_path):
    with _output(tmp_path) as case:
        with session_scope(case.factory) as session:
            _writer(session)
            assert retain_inactive_upload_claim(session, "ordinary-creator", nowait=True) is None
