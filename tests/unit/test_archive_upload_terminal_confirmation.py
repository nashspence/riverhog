from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace

import pytest
from riverhog_core.app_permissions import (
    CATALOG_READ,
    COLLECTIONS_CREATE,
    COLLECTIONS_DELETE,
    ApplicationAccess,
)
from riverhog_core.catalog_archive_write_models import (
    ArchiveUploadTerminalRecord as Terminal,
)
from riverhog_core.catalog_archive_write_models import (
    ArchiveWriteControlRecord as Control,
)
from riverhog_core.catalog_archive_write_models import (
    ArchiveWritePreparationRecord as Preparation,
)
from riverhog_core.catalog_archive_write_models import (
    TerminalCleanupCapacityRecord as Capacity,
)
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.catalog_object_cleanup_models import ObjectCleanupPageRecord as Page
from riverhog_core.incremental_emission import VolumeEmissionCapacity
from riverhog_core.services import archive_upload_terminal_confirmation as module
from riverhog_core.services.archive_upload_catalog_cleanup import ArchiveUploadCatalogCleanup
from riverhog_core.services.archive_upload_checkpoint_cleanup import ArchiveUploadCheckpointCleanup
from riverhog_core.services.archive_upload_copy_intent_cleanup import ArchiveUploadCopyIntentCleanup
from riverhog_core.services.archive_upload_projection_cleanup import ArchiveUploadProjectionCleanup
from riverhog_core.services.archive_upload_registration_cleanup import (
    ArchiveUploadRegistrationCleanup,
)
from riverhog_core.services.archive_upload_terminal_confirmation import (
    ArchiveUploadTerminalConfirmation,
)
from riverhog_core.services.archive_upload_terminal_tags import load_terminal_tag_selector
from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy import event, select

from tests.unit.test_app_keys import BOOTSTRAP
from tests.unit.test_archive_upload_projection_cleanup import LIMITS, _projection


def _owner(ready):
    case = ready.case
    return ArchiveUploadTerminalConfirmation(
        case.factory, ready.ready.owner, case.service._archive_terminal_observation
    )


def _confirm(ready, *, limits=LIMITS):
    return _owner(ready).confirm(
        ready.case.collection_id,
        ready.case.operation,
        guard=ready.guard,
        replay_guard=ready.guard.read,
        limits=limits,
    )


def _confirmed(ready):
    return _owner(ready).confirmed(
        ready.case.collection_id,
        ready.case.operation,
        guard=ready.guard.read,
    )


def _clean(ready, *, skip=None):
    case = ready.case
    for name, owner in (
        ("registration", ArchiveUploadRegistrationCleanup(case.factory, ready.ready.owner)),
        ("catalog", ArchiveUploadCatalogCleanup(case.factory, ready.ready.owner)),
        ("checkpoints", ArchiveUploadCheckpointCleanup(case.factory, ready.ready.owner)),
        ("copies", ArchiveUploadCopyIntentCleanup(case.factory, ready.ready.owner)),
        ("projection", ArchiveUploadProjectionCleanup(case.factory, ready.ready.owner)),
    ):
        if name == skip:
            continue
        for _ in range(256):
            progress = owner.advance(
                case.collection_id, case.operation, guard=ready.guard, limits=LIMITS
            )
            complete = (
                progress.registration_id is None or progress.phase == "disposed"
                if name == "registration"
                else progress.upload_children_exhausted
                if name == "catalog"
                else progress.exhausted
            )
            if complete:
                break
            assert getattr(progress, "pending_reason", None) is None
        else:
            pytest.fail("fixture cleanup did not finish its bounded windows")
    assert _confirmed(ready) is None


def _receipts(ready):
    case = ready.case
    with session_scope(case.factory) as session:

        def row(model, identity):
            value = session.get(model, identity)
            return (
                None
                if value is None
                else {
                    column.name: getattr(value, column.name) for column in model.__table__.columns
                }
            )

        return {
            "control": row(Control, case.collection_id),
            "page": row(Page, case.operation),
            "capacity": row(Capacity, case.operation),
            "tags": load_terminal_tag_selector(session, case.operation),
        }


def _prepared(ready, *, phase="cleaning"):
    with session_scope(ready.case.factory) as session:
        terminal = session.get(Terminal, ready.case.operation)
        assert terminal.phase == phase and terminal.original_result_json == ready.original
        assert (
            terminal.result_json is None
            and session.get(Upload, ready.case.collection_id) is not None
        )


def _finished(ready):
    assert _confirm(ready) == ready.original
    assert _confirmed(ready) == ready.original
    assert _confirm(ready) == ready.original
    with session_scope(ready.case.factory) as session:
        terminal = session.get(Terminal, ready.case.operation)
        assert terminal.phase == "done" and terminal.original_result_json is None
        assert terminal.result_json == ready.original
        assert terminal.execution_next_attempt_at is None
        assert session.get(Upload, ready.case.collection_id) is None


def test_confirmation_transfers_exact_original_bytes_and_keeps_receipts_after_upload_removal(
    tmp_path,
):
    with _projection(tmp_path, indexed=True) as ready:
        _clean(ready)
        receipts = _receipts(ready)
        _finished(ready)
        assert _receipts(ready) == receipts


def _lost(ready, monkeypatch, committed):
    actual = module.session_scope

    @contextmanager
    def lose(factory):
        with actual(factory) as session:
            yield session
            if not committed:
                raise OSError("confirmation rollback")
        raise OSError("confirmation commit reply lost")

    before = _receipts(ready)
    with monkeypatch.context() as patch:
        patch.setattr(module, "session_scope", lose)
        with pytest.raises(OSError, match="confirmation"):
            _confirm(ready)
    if committed:
        assert _confirmed(ready) == ready.original
    else:
        _prepared(ready)
        assert _confirmed(ready) is None
    _finished(ready)
    assert _receipts(ready) == before


@pytest.mark.parametrize("committed", (False, True))
def test_atomic_confirmation_resumes_sql_rollback_or_lost_reply_without_new_result(
    tmp_path, monkeypatch, committed
):
    with _projection(tmp_path, indexed=True) as ready:
        _clean(ready)
        _lost(ready, monkeypatch, committed)


def _rights(ready):
    case = ready.case
    case.keys.replace_access(
        app=case.principal.id,
        key_id=case.principal.key_id,
        access=(ApplicationAccess(CATALOG_READ),),
        grantor=BOOTSTRAP,
    )
    with pytest.raises(NotFound):
        _confirm(ready)
    _prepared(ready)
    case.keys.replace_access(
        app=case.principal.id,
        key_id=case.principal.key_id,
        access=(ApplicationAccess(COLLECTIONS_DELETE),),
        grantor=BOOTSTRAP,
    )
    _finished(ready)
    for access, allowed in (
        ((ApplicationAccess(COLLECTIONS_CREATE),), False),
        ((ApplicationAccess(CATALOG_READ),), False),
        ((ApplicationAccess(COLLECTIONS_DELETE),), True),
    ):
        case.keys.replace_access(
            app=case.principal.id, key_id=case.principal.key_id, access=access, grantor=BOOTSTRAP
        )
        if allowed:
            assert _confirmed(ready) == _confirm(ready) == ready.original
        else:
            with pytest.raises(NotFound):
                _confirmed(ready)
            with pytest.raises(NotFound):
                _confirm(ready)


def test_confirmation_and_done_replay_retain_current_authority_without_a_parent(tmp_path):
    with _projection(tmp_path, indexed=True) as ready:
        _clean(ready)
        _rights(ready)


@pytest.mark.parametrize("skip", ("catalog", "projection"))
def test_no_confirmed_result_is_inferred_from_partial_catalog_cleanup(tmp_path, skip):
    with _projection(tmp_path, indexed=True) as ready:
        _clean(ready, skip=skip)
        before = _receipts(ready)
        with pytest.raises(Conflict, match="required cleanup|projection"):
            _confirm(ready)
        _prepared(ready)
        assert _receipts(ready) == before


def test_confirmation_requires_positive_original_archive_inventory_not_only_empty_tables(tmp_path):
    with _projection(tmp_path, provider_ready=False) as ready:
        before = _receipts(ready)
        with pytest.raises(Conflict, match="inventory proof"):
            _confirm(ready)
        _prepared(ready, phase="cleanup-ready")
        assert _receipts(ready) == before


def test_original_preparation_still_blocks_final_confirmation_after_other_cleanup(tmp_path):
    with _projection(tmp_path, indexed=True) as ready:
        _clean(ready)
        with session_scope(ready.case.factory) as session:
            session.add(
                Preparation(
                    collection_id=ready.case.collection_id,
                    object_id="a" * 64,
                    identity_json="{}",
                    preparation_json="original-opaque-encryption-state",
                )
            )
        with pytest.raises(Conflict, match="required cleanup"):
            _confirm(ready)
        _prepared(ready)


@pytest.mark.parametrize("which", ("finalizers", "invocations", "inputs", "execution"))
def test_busy_actual_resource_probe_cannot_be_promoted_to_terminal_completion(tmp_path, which):
    with _projection(tmp_path, indexed=True) as ready:
        _clean(ready)
        case = ready.case
        owners = {
            "finalizers": case.service._archive_finalizers,
            "invocations": case.service._archive_invocations,
            "inputs": case.service._archive_inputs,
            "execution": case.service._archive_execution,
        }
        with owners[which]._lock:
            with pytest.raises(Conflict, match="resource custody"):
                _confirm(ready)
        _prepared(ready)
        _finished(ready)


@pytest.mark.parametrize("dimension", ("metadata_bytes", "input_transitions"))
def test_atomic_confirmation_refuses_a_small_grant_before_parent_or_result_changes(
    tmp_path, dimension
):
    with _projection(tmp_path, indexed=True) as ready:
        _clean(ready)
        with pytest.raises(VolumeEmissionCapacity) as refused:
            _confirm(ready, limits=replace(LIMITS, **{dimension: 1}))
        assert refused.value.dimension == dimension and refused.value.required > 1
        _prepared(ready)
        _finished(ready)


def test_complete_opaque_upload_metadata_is_charged_to_the_final_atomic_quantum(tmp_path):
    with _projection(tmp_path, indexed=True) as ready:
        _clean(ready)
        with session_scope(ready.case.factory) as session:
            session.get(Upload, ready.case.collection_id).archive_failure = "opaque-" + "x" * 20001
        with pytest.raises(VolumeEmissionCapacity) as refused:
            _confirm(ready)
        assert refused.value.dimension == "metadata_bytes" and refused.value.required > 20001
        _prepared(ready)
        assert _confirm(ready, limits=replace(LIMITS, metadata_bytes=50000)) == ready.original
        _finished(ready)


def test_linked_discovery_cannot_be_bypassed_by_final_confirmation(tmp_path):
    with _projection(tmp_path, indexed=True, linked=True) as ready:
        before = ready.discovery.marker.read_bytes()
        with pytest.raises(Conflict, match="required cleanup|projection"):
            _confirm(ready)
        _prepared(ready)
        assert ready.discovery.marker.read_bytes() == before


def _history(ready):
    from riverhog_core.catalog_models import CollectionUploadCopyIntentRecord as Intent
    from riverhog_core.catalog_registration_models import (
        CollectionRegistrationRecord as Registration,
    )

    from tests.unit.test_archive_upload_copy_intent_cleanup import DESTINATIONS

    with session_scope(ready.case.factory) as session:
        before = {
            model.__tablename__: [
                dict(row) for row in session.execute(select(model.__table__)).mappings()
            ]
            for model in (Intent, Registration)
        }
    assert len(before[Intent.__tablename__]) == len(DESTINATIONS)
    _finished(ready)
    with session_scope(ready.case.factory) as session:
        after = {
            model.__tablename__: [
                dict(row) for row in session.execute(select(model.__table__)).mappings()
            ]
            for model in (Intent, Registration)
        }
    assert after == before


def test_cancel_confirmation_retains_hidden_registration_history_and_copy_receipts(
    tmp_path, monkeypatch
):
    from tests.unit.test_archive_upload_copy_intent_cleanup import DESTINATIONS
    from tests.unit.test_archive_upload_registration_cleanup import _prepared as registration

    with registration(tmp_path, monkeypatch, copy_to=DESTINATIONS) as ready:
        _clean(ready)
        _history(ready)


def test_done_replay_uses_fresh_retained_key_generation_without_bearer_authentication(
    tmp_path, monkeypatch
):
    from riverhog_core.services.app_keys import SqlAlchemyAppKeyService

    with _projection(tmp_path) as ready:
        _clean(ready)
        _finished(ready)
        ready.case.keys.rotate(
            app=ready.case.principal.id, key_id=ready.case.principal.key_id, grantor=BOOTSTRAP
        )

        def forbidden(*args, **kwargs):
            pytest.fail("completed terminal replay authenticated a bearer")

        monkeypatch.setattr(SqlAlchemyAppKeyService, "authenticate", forbidden)
        cold = ready.case.service._terminal_write_guard(
            ready.case.collection_id, ready.case.operation
        )
        assert (
            _owner(ready).confirmed(ready.case.collection_id, ready.case.operation, guard=cold.read)
            == ready.original
        )


def test_confirmed_replay_never_repeats_metadata_mutations_or_provider_work(tmp_path):
    with _projection(tmp_path, indexed=True) as ready:
        _clean(ready)
        _finished(ready)
        mutations = []
        engine = ready.case.factory.kw["bind"]

        def observed(connection, cursor, statement, parameters, context, executemany):
            if context.isinsert or context.isupdate or context.isdelete:
                mutations.append(statement)

        calls = len(ready.ready.target.calls)
        event.listen(engine, "before_cursor_execute", observed)
        try:
            for _ in range(3):
                assert _confirm(ready) == ready.original
        finally:
            event.remove(engine, "before_cursor_execute", observed)
        assert mutations == [] and len(ready.ready.target.calls) == calls


def test_new_confirmation_uses_disposal_authority_without_borrowing_readback_rights(tmp_path):
    with _projection(tmp_path, indexed=True) as ready:
        _clean(ready)

        def forbidden(session):
            pytest.fail("new confirmation required an additional readback grant")

        assert (
            _owner(ready).confirm(
                ready.case.collection_id,
                ready.case.operation,
                guard=ready.guard,
                replay_guard=forbidden,
                limits=LIMITS,
            )
            == ready.original
        )
        _finished(ready)


def _replay_without_disposal(ready):
    def ineligible(session):
        raise Conflict("a new orphan disposition is unavailable")

    assert (
        _owner(ready).confirm(
            ready.case.collection_id,
            ready.case.operation,
            guard=ineligible,
            replay_guard=ready.guard.read,
            limits=LIMITS,
        )
        == ready.original
    )


def test_confirmed_replay_uses_current_read_scope_without_new_disposal_eligibility(tmp_path):
    with _projection(tmp_path, indexed=True) as ready:
        _clean(ready)
        _finished(ready)
        _replay_without_disposal(ready)


def test_read_scope_cannot_confirm_an_incomplete_disposal(tmp_path):
    with _projection(tmp_path, indexed=True) as ready:
        _clean(ready)

        def ineligible(session):
            raise Conflict("original disposal rights are unavailable")

        with pytest.raises(Conflict, match="original disposal rights"):
            _owner(ready).confirm(
                ready.case.collection_id,
                ready.case.operation,
                guard=ineligible,
                replay_guard=ready.guard.read,
                limits=LIMITS,
            )
        _prepared(ready)
