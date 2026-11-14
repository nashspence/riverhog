from __future__ import annotations

import json

import pytest
from riverhog_core.catalog_archive_write_models import ArchiveUploadTerminalRecord
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import AppKeyRecord
from riverhog_core.catalog_workflow_models import CollectionProcessingClaimRecord
from riverhog_core.services.archive_upload_drain import lock_terminal_selection
from riverhog_core.services.collection_upload_authority import retain_upload_key
from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy import func, select

from tests.unit.test_app_keys import BOOTSTRAP
from tests.unit.test_archive_upload_terminal_tags import _teardown_upload
from tests.unit.test_archive_write_admission import GRANTS, _case, _selected
from tests.unit.test_upload_discard_authority import _operator, _select
from tests.unit.test_upload_terminal_claim_authority import _output


def _context(case, operation):
    with session_scope(case.factory) as session:
        _, terminal = lock_terminal_selection(
            session, case.collection_id, operation, require_upload=False
        )
        return terminal.key_id, json.loads(terminal.request_json)


def _raw_retry(case, principal):
    # Retain a current key for raw selection replay; this grants no cleanup.
    return case.owner.select_terminal(
        case.collection_id,
        kind="discard",
        request={"challenge": "original-discard-scope"},
        principal_id=principal.id,
        key_id=principal.key_id,
        grants=GRANTS,
        guard=lambda session: retain_upload_key(session, principal),
        validate_selection=lambda *args: pytest.fail("exact retry selected fresh work"),
    )


def test_terminal_retains_selecting_key_and_replays_after_credential_rotation(tmp_path):
    with _case(tmp_path) as case:
        keys, principal = _operator(case)
        guard = case.service._discard_write_guard(case.collection_id, principal)
        operation = _select(case, principal, guard)
        key, request = _context(case, operation)
        assert key == principal.key_id and request["key_id"] == key
        keys.rotate(app=principal.id, key_id=principal.key_id, grantor=BOOTSTRAP)
        assert _select(case, principal, guard) == operation


def test_another_key_for_the_same_application_cannot_borrow_an_exact_terminal_retry(tmp_path):
    with _case(tmp_path) as case:
        _, principal = _operator(case)
        _, other = _operator(case)
        assert principal.id == other.id and principal.key_id != other.key_id
        operation = _select(
            case, principal, case.service._discard_write_guard(case.collection_id, principal)
        )
        original = _context(case, operation)
        with pytest.raises(Conflict, match="different archive upload terminal request"):
            _select(case, other, case.service._discard_write_guard(case.collection_id, other))
        assert _context(case, operation) == original
        with session_scope(case.factory) as session:
            assert (
                session.scalar(select(func.count()).select_from(ArchiveUploadTerminalRecord)) == 1
            )


@pytest.mark.parametrize("change", ("revoke", "delete"))
def test_original_key_context_survives_upload_and_key_teardown_without_reauthorizing(
    tmp_path, change
):
    with _case(tmp_path) as case:
        keys, principal = _operator(case)
        operation = _select(
            case, principal, case.service._discard_write_guard(case.collection_id, principal)
        )
        original = _context(case, operation)
        _teardown_upload(case)
        assert _raw_retry(case, principal) == operation
        if change == "revoke":
            keys.revoke(app=principal.id, key_id=principal.key_id)
        else:
            with session_scope(case.factory) as session:
                session.delete(session.get(AppKeyRecord, principal.key_id))
        with pytest.raises(NotFound, match="application key"):
            _raw_retry(case, principal)
        assert _context(case, operation) == original


def test_selected_key_row_drift_is_rejected_by_replay_and_locked_terminal_consumers(tmp_path):
    with _case(tmp_path) as case:
        _, principal = _operator(case)
        guard = case.service._discard_write_guard(case.collection_id, principal)
        operation = _select(case, principal, guard)
        with session_scope(case.factory) as session:
            session.get(ArchiveUploadTerminalRecord, operation).key_id = "f" * 16
        with pytest.raises(Conflict, match="preimage changed"):
            _select(case, principal, guard)
        with pytest.raises(Conflict, match="original selection changed"):
            _context(case, operation)


def test_keyless_selector_is_an_explicit_original_canonical_context(tmp_path):
    with _case(tmp_path) as case:
        operation = _selected(case)
        key, request = _context(case, operation)
        assert key is None and request["key_id"] is None
        assert set(request) == {"construction", "kind", "principal_id", "key_id", "request"}


@pytest.mark.parametrize("key_id", ("", False, 0, []))
def test_invalid_selecting_key_identity_is_rejected_before_authority_or_parent_work(
    tmp_path, key_id
):
    with _case(tmp_path) as case:

        def forbidden(*args):
            pytest.fail("invalid selecting key reached authority or catalog work")

        with pytest.raises(ValueError, match="selecting key identity"):
            case.owner.select_terminal(
                case.collection_id,
                kind="discard",
                request={"selected": "original"},
                principal_id="deletion-operator",
                key_id=key_id,
                grants=GRANTS,
                guard=forbidden,
                validate_selection=forbidden,
            )


def test_discard_selects_the_operator_key_independently_of_the_original_output_key(tmp_path):
    with _output(tmp_path) as case:
        _, principal = _operator(case)
        assert principal.key_id != case.principal.key_id
        with session_scope(case.factory) as session:
            session.get(
                CollectionProcessingClaimRecord, case.claim_id
            ).expires_at = "2000-01-01T00:00:00Z"
        operation = _select(
            case, principal, case.service._discard_write_guard(case.collection_id, principal)
        )
        key, request = _context(case, operation)
        assert key == principal.key_id and request["key_id"] == principal.key_id


def test_terminal_preserves_the_delegated_owners_opaque_key_reference(tmp_path):
    with _output(tmp_path) as case:
        operation = case.owner.select_terminal(
            case.collection_id,
            kind="cancel",
            request={"selected": "original-output"},
            principal_id=case.principal.id,
            key_id=case.principal.key_id,
            grants=GRANTS,
            guard=case.service._payload_write_guard(case.collection_id, case.principal),
            validate_selection=lambda *args: None,
        )
        key, request = _context(case, operation)
        assert key == case.principal.key_id and request["key_id"] == key
