from __future__ import annotations

import json
from dataclasses import replace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.catalog_archive_write_models import ArchiveUploadTerminalRecord as Terminal
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord as Control
from riverhog_core.catalog_db import session_scope
from riverhog_core.services.archive_upload_terminal_control import (
    require_prepared_terminal_result,
    terminal_control_bytes,
)
from riverhog_core.services.archive_upload_terminal_result import ArchiveUploadTerminalResults
from riverhog_core.services.archive_upload_terminal_tags import (
    load_terminal_tag_selector,
    upload_terminal_tag_selector,
)
from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy import event
from sqlalchemy.exc import InvalidRequestError

from tests.unit.test_archive_upload_cleanup import _inventory_arm, _ready
from tests.unit.test_archive_upload_terminal_tags import _teardown_upload
from tests.unit.test_archive_write_admission import GRANTS, _case, _guard
from tests.unit.test_collection_uploads import _CREATOR


def _select(case, capacity=4096):
    return case.owner.select_terminal(
        case.collection_id,
        kind="cancel",
        request={"selected": "exact-original"},
        principal_id=_CREATOR.id,
        key_id=_CREATOR.key_id,
        grants=replace(GRANTS, terminal_bytes=capacity),
        guard=_guard,
        validate_selection=lambda *args: None,
    )


def _state(case, operation):
    with session_scope(case.factory) as session:
        row = session.get(Terminal, operation)
        return row.phase, row.control_capacity_bytes, row.original_result_json, row.result_json


def _prepare(case, operation, result):
    return ArchiveUploadTerminalResults(case.factory).prepare(
        case.collection_id, operation, result, guard=_guard
    )


def test_selection_reserves_complete_original_header_and_tag_reference(tmp_path):
    from riverhog_core.catalog_models import CollectionUploadRecord

    with _case(tmp_path) as case:
        with session_scope(case.factory) as session:
            control = session.get(Control, case.collection_id)
            original = (control.gate, control.epoch, control.terminal_id)
            request = {
                "construction": json.loads(control.construction_json),
                "kind": "cancel",
                "principal_id": _CREATOR.id,
                "key_id": _CREATOR.key_id,
                "request": {"selected": "exact-original"},
            }
            selector = upload_terminal_tag_selector(
                session.get(CollectionUploadRecord, case.collection_id)
            )
            request_only = len(canonical_json_bytes(request)) + len(
                canonical_json_bytes(selector.payload())
            )
        with pytest.raises(Conflict, match="byte grant"):
            _select(case, request_only)
        with session_scope(case.factory) as session:
            control = session.get(Control, case.collection_id)
            assert (control.gate, control.epoch, control.terminal_id) == original
        operation = _select(case)
        assert _state(case, operation) == ("draining", 4096, None, None)


def test_exact_last_byte_fits_and_oversize_cannot_start_cleanup(tmp_path):
    with _case(tmp_path) as case:
        operation = _select(case)
        with session_scope(case.factory) as session:
            row = session.get(Terminal, operation)
            selector = load_terminal_tag_selector(session, operation)
            row.original_result_json = canonical_json_bytes({"payload": ""}).decode()
            room = row.control_capacity_bytes - terminal_control_bytes(row, selector)
            row.original_result_json = None
        assert room > 0
        with pytest.raises(Conflict, match="byte grant"):
            _prepare(case, operation, {"payload": "x" * (room + 1)})
        assert _state(case, operation) == ("draining", 4096, None, None)
        expected = canonical_json_bytes({"payload": "x" * room}).decode()
        assert _prepare(case, operation, {"payload": "x" * room}) == expected
        with session_scope(case.factory) as session:
            row = session.get(Terminal, operation)
            selector = load_terminal_tag_selector(session, operation)
            assert terminal_control_bytes(row, selector) == row.control_capacity_bytes
            assert require_prepared_terminal_result(row, selector) == expected
        assert _state(case, operation) == ("draining", 4096, expected, None)


def _lost_reply(case):
    operation = _select(case)
    result = {"collection_id": str(case.collection_id), "state": "canceled", "files": 17}

    def lost(session):
        raise RuntimeError("original result commit reply lost")

    event.listen(case.factory, "after_commit", lost, once=True)
    with pytest.raises(InvalidRequestError, match="committed") as caught:
        _prepare(case, operation, result)
    assert isinstance(caught.value.__context__, RuntimeError)
    assert "original result commit reply lost" in str(caught.value.__context__)
    frozen = canonical_json_bytes(result).decode()
    assert _state(case, operation) == ("draining", 4096, frozen, None)
    assert _prepare(case, operation, result) == frozen
    with pytest.raises(Conflict, match="original result differs"):
        _prepare(case, operation, {**result, "files": 0})
    assert _state(case, operation) == ("draining", 4096, frozen, None)
    return operation, frozen


def test_lost_commit_reply_receives_only_the_same_original_result(tmp_path):
    with _case(tmp_path) as case:
        _lost_reply(case)


@pytest.mark.parametrize("phase", ("cleanup-ready", "cleaning"))
def test_prepared_result_is_not_confirmation_and_replays_during_partial_cleanup(tmp_path, phase):
    with _case(tmp_path) as case:
        operation = _select(case)
        result = {"files": 29, "bytes": 761, "state": "canceled"}
        encoded = _prepare(case, operation, result)
        with session_scope(case.factory) as session:
            session.get(Terminal, operation).phase = phase
        assert _prepare(case, operation, result) == encoded
        assert _state(case, operation) == (phase, 4096, encoded, None)


def test_lowered_current_grant_does_not_replace_original_receiving_space(tmp_path):
    with _case(tmp_path) as case:
        operation = _select(case)
        result = {"message": "original result with exact counts"}
        assert (
            case.owner.select_terminal(
                case.collection_id,
                kind="cancel",
                request={"selected": "exact-original"},
                principal_id=_CREATOR.id,
                key_id=_CREATOR.key_id,
                grants=replace(GRANTS, terminal_bytes=1),
                guard=_guard,
                validate_selection=lambda *args: pytest.fail("exact retry selected fresh work"),
            )
            == operation
        )
        encoded = _prepare(case, operation, result)
        assert _state(case, operation) == ("draining", 4096, encoded, None)


def test_cleanup_ready_flag_without_original_result_grants_no_provider_admission(tmp_path):
    with _case(tmp_path) as case:
        ready = _ready(case, ready=False)
        with session_scope(case.factory) as session:
            session.get(Terminal, ready.operation).phase = "cleanup-ready"
        with pytest.raises(Conflict, match="original result reservation"):
            _inventory_arm(ready)
        with pytest.raises(Conflict, match="must precede cleanup"):
            _prepare(case, ready.operation, {"state": "canceled"})


@pytest.mark.parametrize("change", ("missing", "noncanonical", "array", "oversize", "confirmed"))
def test_changed_original_result_or_reservation_blocks_new_cleanup(tmp_path, change):
    with _case(tmp_path) as case:
        ready = _ready(case)
        with session_scope(case.factory) as session:
            row = session.get(Terminal, ready.operation)
            if change == "missing":
                row.original_result_json = None
            elif change == "noncanonical":
                row.original_result_json = '{ "files": 3 }'
            elif change == "array":
                row.original_result_json = "[]"
            elif change == "oversize":
                row.control_capacity_bytes = 1
            else:
                row.result_json = '{"state":"canceled"}'
        with pytest.raises(Conflict, match="original result|byte grant"):
            _inventory_arm(ready)


def test_original_result_survives_parent_retirement_without_becoming_confirmed(tmp_path):
    with _case(tmp_path) as case:
        operation, frozen = _lost_reply(case)
        _teardown_upload(case)
        assert _state(case, operation) == ("draining", 4096, frozen, None)
        with pytest.raises(NotFound):
            _prepare(case, operation, json.loads(frozen))
        assert _state(case, operation) == ("draining", 4096, frozen, None)


def test_result_rejection_before_parent_locks_preserves_selected_state(tmp_path):
    with _case(tmp_path) as case:
        operation = _select(case)

        def revoked(session):
            raise NotFound("current terminal authority is unavailable")

        with pytest.raises(NotFound, match="current terminal authority"):
            ArchiveUploadTerminalResults(case.factory).prepare(
                case.collection_id, operation, {"state": "canceled"}, guard=revoked
            )
        assert _state(case, operation) == ("draining", 4096, None, None)


@pytest.mark.parametrize("phase", ("refused-sealed", "needs-reconfirmation"))
def test_refusal_cannot_receive_a_proposed_success_as_confirmation(tmp_path, phase):
    with _case(tmp_path) as case:
        operation = _select(case)
        with session_scope(case.factory) as session:
            session.get(Terminal, operation).phase = phase
        with pytest.raises(Conflict, match="this phase"):
            _prepare(case, operation, {"state": "canceled"})
        assert _state(case, operation) == (phase, 4096, None, None)


def _large_scalar_result(case, collection_id, capacity):
    from types import SimpleNamespace

    from tests.unit.test_finalization_readiness_growth import clone_upload

    clone_upload(case.factory, collection_id)
    selected = SimpleNamespace(**{**vars(case), "collection_id": collection_id})
    operation = _select(selected, capacity)
    result = {"state": "canceled", "collection_id": str(collection_id), "files": 1}
    encoded = _prepare(selected, operation, result)
    assert _state(selected, operation) == ("draining", capacity, encoded, None)
    assert _prepare(selected, operation, result) == encoded
    with session_scope(case.factory) as session:
        terminal = session.get(Terminal, operation)
        assert terminal.collection_id == collection_id
        assert terminal.control_capacity_bytes == capacity
        assert (
            require_prepared_terminal_result(
                terminal, load_terminal_tag_selector(session, operation)
            )
            == encoded
        )
    return operation


@pytest.mark.parametrize("collection_id", (2**53 - 1, 2**53, 2**53 + 1, 2**60, 2**63 - 1))
@pytest.mark.parametrize("capacity", (4096, 2**60))
def test_full_terminal_reservation_preserves_large_exact_catalog_scalars(
    tmp_path, collection_id, capacity
):
    with _case(tmp_path) as case:
        _large_scalar_result(case, collection_id, capacity)
