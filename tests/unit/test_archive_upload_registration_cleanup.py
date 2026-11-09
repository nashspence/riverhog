from __future__ import annotations

from contextlib import contextmanager
from functools import partial
from types import SimpleNamespace

import pytest
from riverhog_core.app_permissions import COLLECTIONS_CREATE, COLLECTIONS_DELETE, ApplicationAccess
from riverhog_core.catalog_archive_write_models import ArchiveUploadTerminalRecord as Terminal
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.catalog_registration_models import CollectionRegistrationRecord as Registration
from riverhog_core.services import archive_upload_registration_cleanup as module
from riverhog_core.services.archive_upload_registration_cleanup import (
    ArchiveUploadRegistrationCleanup,
)
from riverhog_core.services.archive_upload_terminal_result import ArchiveUploadTerminalResults
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_core.services.collection_registration import CollectionRegistrationOwner
from riverhog_protocol.errors import Conflict, NotFound

from tests.unit import test_collection_registration_operations as witness
from tests.unit.test_app_keys import BOOTSTRAP
from tests.unit.test_archive_upload_cleanup_turn import _bind, _complete
from tests.unit.test_collection_registration_cleanup import CLEANUP, _snapshot, _staged
from tests.unit.test_collection_registration_operations import _close
from tests.unit.test_upload_cancel_authority import _select


@contextmanager
def _prepared(tmp_path, monkeypatch, *, database_url=None, provider_ready=True):
    if database_url is not None:
        monkeypatch.setattr(
            witness,
            "_service_with_archive_objects",
            partial(witness._service_with_archive_objects, database_url=database_url),
        )
    service, keys, principal, created, cid, registration, prior = _staged(tmp_path)
    try:
        case = SimpleNamespace(
            service=service,
            factory=service._session_factory,
            config=service._config,
            keys=keys,
            principal=principal,
            collection_id=cid,
            owner=ArchiveWriteAdmission(service._session_factory),
        )
        with session_scope(case.factory) as session:
            case.incarnation = session.get(Upload, cid).archive_incarnation_id
        case.operation = _select(case, principal, service._cancel_write_guard(cid, principal))
        guard = service._terminal_write_guard(cid, case.operation)
        original = ArchiveUploadTerminalResults(case.factory).prepare(
            cid,
            case.operation,
            {"state": "canceled", "collection_id": str(cid)},
            guard=guard,
        )
        assert service._terminal_turn(cid, case.operation).advance(limit=1).originals_retired
        ready = _bind(case)
        ready.target.items = ()
        if provider_ready:
            _complete(ready)
        owner = ArchiveUploadRegistrationCleanup(case.factory, ready.owner)
        yield SimpleNamespace(
            case=case,
            ready=ready,
            owner=owner,
            registration=registration,
            prior=prior,
            original=original,
            guard=guard,
        )
    finally:
        _close(service, keys)


def _advance(ready, *, owner=None):
    return (owner or ready.owner).advance(
        ready.case.collection_id,
        ready.case.operation,
        guard=ready.guard,
        limits=CLEANUP,
    )


def _state(ready):
    return _snapshot(ready.case.service, ready.registration)


def _complete_registration(ready):
    previous = _state(ready)
    for _ in range(40):
        # Reconstructing this SQL-only owner proves that its cursor and selected
        # disposition belong to retained state rather than process-local hints.
        owner = ArchiveUploadRegistrationCleanup(ready.case.factory, ready.ready.owner)
        result = _advance(ready, owner=owner)
        current = _state(ready)
        for name, count in current["counts"].items():
            assert 0 <= previous["counts"][name] - count <= 2
        assert current["checkpoint"] == previous["checkpoint"]
        assert current["accepted_members"] == previous["accepted_members"]
        assert current["accepted_sequence"] == previous["accepted_sequence"]
        with session_scope(ready.case.factory) as session:
            registration = session.get(Registration, ready.registration)
            terminal = session.get(Terminal, ready.case.operation)
            assert registration.due_at is None
            assert terminal.original_result_json == ready.original
            assert terminal.result_json is None
        assert CollectionRegistrationOwner(ready.case.service).process_due() == 0
        if result.phase == "disposed":
            break
        previous = current
    else:
        pytest.fail("current-authorized hidden registration cleanup did not finish")
    assert current["active"] is None and set(current["counts"].values()) == {0}
    assert _advance(ready).registration_id is None
    with session_scope(ready.case.factory) as session:
        assert session.get(Upload, ready.case.collection_id) is not None
        assert session.get(Terminal, ready.case.operation).result_json is None


def test_native_registration_cleanup_cold_resumes_bounded_windows_and_keeps_original_result(
    tmp_path, monkeypatch
):
    with _prepared(tmp_path, monkeypatch) as ready:
        _complete_registration(ready)


def test_hidden_disposal_cannot_start_before_positive_archive_inventory_proof(
    tmp_path, monkeypatch
):
    with _prepared(tmp_path, monkeypatch, provider_ready=False) as ready:
        before = _state(ready)
        with pytest.raises(Conflict, match="retained inventory proof"):
            _advance(ready)
        assert _state(ready) == before


def _revoked(ready):
    assert _advance(ready).phase == "cleaning"
    before = _state(ready)
    case = ready.case
    case.keys.replace_access(
        app=case.principal.id,
        key_id=case.principal.key_id,
        access=(ApplicationAccess(COLLECTIONS_DELETE),),
        grantor=BOOTSTRAP,
    )
    with pytest.raises(NotFound):
        _advance(ready)
    assert _state(ready) == before
    case.keys.replace_access(
        app=case.principal.id,
        key_id=case.principal.key_id,
        access=(ApplicationAccess(COLLECTIONS_CREATE),),
        grantor=BOOTSTRAP,
    )
    _complete_registration(ready)


def test_current_terminal_revocation_pauses_destructive_registration_quanta(tmp_path, monkeypatch):
    with _prepared(tmp_path, monkeypatch) as ready:
        _revoked(ready)


def _independent(ready):
    assert _advance(ready).phase == "cleaning"
    before = _state(ready)
    owner = CollectionRegistrationOwner(ready.case.service)
    with pytest.raises(Conflict, match="current upload terminal"):
        owner.advance(ready.case.collection_id, ready.registration, limits=CLEANUP)
    assert _state(ready) == before
    assert owner.process_due() == 0
    _complete_registration(ready)


def test_independent_registration_executor_cannot_borrow_native_terminal_authority(
    tmp_path, monkeypatch
):
    with _prepared(tmp_path, monkeypatch) as ready:
        _independent(ready)


def _lost(ready, monkeypatch, committed):
    if committed:
        actual = module.session_scope

        @contextmanager
        def lose(factory):
            with actual(factory) as session:
                yield session
            raise OSError("registration cleanup commit reply lost")

        patch_name, replacement = "session_scope", lose
    else:
        actual = module.advance_registration_cleanup

        def lose(*args, **kwargs):
            actual(*args, **kwargs)
            raise OSError("registration cleanup rolled back")

        patch_name, replacement = "advance_registration_cleanup", lose
    before = _state(ready)
    with monkeypatch.context() as patch:
        patch.setattr(module, patch_name, replacement)
        with pytest.raises(OSError, match="registration cleanup"):
            _advance(ready)
    assert (_state(ready) == before) is not committed
    _complete_registration(ready)


@pytest.mark.parametrize("committed", (False, True))
def test_failed_registration_cleanup_reply_preserves_the_same_bounded_cursor(
    tmp_path, monkeypatch, committed
):
    with _prepared(tmp_path, monkeypatch) as ready:
        _lost(ready, monkeypatch, committed)
