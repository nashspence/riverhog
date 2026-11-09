from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace

import pytest
from riverhog_core.app_permissions import COLLECTIONS_CREATE, COLLECTIONS_DELETE, ApplicationAccess
from riverhog_core.catalog_archive_write_models import (
    ArchiveUploadTerminalRecord as Terminal,
)
from riverhog_core.catalog_archive_write_models import (
    ArchiveWriteEffectRecord as Effect,
)
from riverhog_core.catalog_archive_write_models import (
    ArchiveWritePreparationRecord as Preparation,
)
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.catalog_object_cleanup_models import ObjectCleanupPageRecord as Page
from riverhog_core.incremental_emission import VolumeEmissionCapacity
from riverhog_core.services import archive_upload_checkpoint_cleanup as module
from riverhog_core.services.archive_upload_checkpoint_cleanup import ArchiveUploadCheckpointCleanup
from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy import select

from tests.unit.test_app_keys import BOOTSTRAP
from tests.unit.test_archive_upload_registration_cleanup import (
    _complete_registration,
    _prepared,
)
from tests.unit.test_collection_registration_cleanup import CLEANUP

LIMITS = replace(CLEANUP, input_transitions=2)


def _seed(ready, count=17, *, width=17):
    with session_scope(ready.case.factory) as session:
        session.add_all(
            Preparation(
                collection_id=ready.case.collection_id,
                object_id=f"{n:064x}",
                identity_json='{"original":"identity"}',
                preparation_json="p" * width,
            )
            for n in range(count)
        )


def _rows(ready):
    with session_scope(ready.case.factory) as session:
        return [
            tuple(row)
            for row in session.execute(
                select(Preparation.__table__)
                .where(Preparation.collection_id == ready.case.collection_id)
                .order_by(Preparation.object_id)
            )
        ]


def _advance(ready, *, limits=LIMITS):
    return ArchiveUploadCheckpointCleanup(ready.case.factory, ready.ready.owner).advance(
        ready.case.collection_id, ready.case.operation, guard=ready.guard, limits=limits
    )


def _finish(ready):
    previous = _rows(ready)
    for _ in range(len(previous) + 2):
        progress = _advance(ready)
        current = _rows(ready)
        assert len(previous) - len(current) == progress.removed
        assert progress.removed <= LIMITS.input_transitions
        assert progress.metadata_bytes <= LIMITS.metadata_bytes
        if progress.exhausted:
            break
        previous = current
    else:
        pytest.fail("original preparation retirement did not finish")
    assert current == []
    with session_scope(ready.case.factory) as session:
        assert session.get(Upload, ready.case.collection_id) is not None
        terminal = session.get(Terminal, ready.case.operation)
        assert terminal.original_result_json == ready.original and terminal.result_json is None
        assert terminal.phase == "cleaning"
        assert session.get(Page, ready.case.operation) is not None


@pytest.mark.parametrize("count", (1, 17, 65))
def test_original_preparations_cold_resume_bounded_rows_without_parent_confirmation(
    tmp_path, monkeypatch, count
):
    with _prepared(tmp_path, monkeypatch) as ready:
        _complete_registration(ready)
        _seed(ready, count)
        _finish(ready)


@pytest.mark.parametrize("provider_ready", (False, True))
def test_checkpoint_evidence_survives_until_positive_archive_and_registration_proof(
    tmp_path, monkeypatch, provider_ready
):
    with _prepared(tmp_path, monkeypatch, provider_ready=provider_ready) as ready:
        _seed(ready)
        before = _rows(ready)
        with pytest.raises(Conflict, match="inventory proof|hidden registration"):
            _advance(ready)
        assert _rows(ready) == before


def test_full_original_preparation_bytes_refuse_insufficient_metadata_grant(tmp_path, monkeypatch):
    with _prepared(tmp_path, monkeypatch) as ready:
        _complete_registration(ready)
        _seed(ready, 1, width=10001)
        before = _rows(ready)
        with pytest.raises(VolumeEmissionCapacity) as refusal:
            _advance(ready)
        assert refusal.value.required > 10001 and _rows(ready) == before
        assert _advance(ready, limits=replace(LIMITS, metadata_bytes=20000)).removed == 1
        _finish(ready)


def _revoked(ready):
    assert _advance(ready).removed == 2
    before = _rows(ready)
    case = ready.case
    case.keys.replace_access(
        app=case.principal.id,
        key_id=case.principal.key_id,
        access=(ApplicationAccess(COLLECTIONS_DELETE),),
        grantor=BOOTSTRAP,
    )
    with pytest.raises(NotFound):
        _advance(ready)
    assert _rows(ready) == before
    case.keys.replace_access(
        app=case.principal.id,
        key_id=case.principal.key_id,
        access=(ApplicationAccess(COLLECTIONS_CREATE),),
        grantor=BOOTSTRAP,
    )
    _finish(ready)


def test_original_checkpoint_windows_require_current_terminal_rights(tmp_path, monkeypatch):
    with _prepared(tmp_path, monkeypatch) as ready:
        _complete_registration(ready)
        _seed(ready)
        _revoked(ready)


def _lost(ready, monkeypatch, committed):
    actual = module.session_scope

    @contextmanager
    def lose(factory):
        with actual(factory) as session:
            yield session
            if not committed:
                raise OSError("original checkpoint retirement rollback")
        raise OSError("original checkpoint retirement commit reply lost")

    before = _rows(ready)
    with monkeypatch.context() as patch:
        patch.setattr(module, "session_scope", lose)
        with pytest.raises(OSError, match="original checkpoint retirement"):
            _advance(ready)
    assert (_rows(ready) == before) is not committed
    _finish(ready)


@pytest.mark.parametrize("committed", (False, True))
def test_original_checkpoint_windows_retry_after_sql_interruption(tmp_path, monkeypatch, committed):
    with _prepared(tmp_path, monkeypatch) as ready:
        _complete_registration(ready)
        _seed(ready)
        _lost(ready, monkeypatch, committed)


def _retained_effect(ready):
    _seed(ready, 1)
    with session_scope(ready.case.factory) as session:
        session.add(
            Effect(
                id="a" * 64,
                collection_id=ready.case.collection_id,
                object_id=f"{0:064x}",
                sequence="0",
                epoch="e" * 64,
                executor_id="e" * 64,
                dispatch_sha256="d" * 64,
                request_sha256="b" * 64,
                request_json="{}",
                resource="begin",
                kind="begin",
                state="settled",
                unresolved=False,
                control_bytes=1,
                result_capacity_bytes=1,
                result_json="{}",
            )
        )
    before = _rows(ready)
    with pytest.raises(Conflict, match="original effect receiving"):
        _advance(ready)
    assert _rows(ready) == before


def test_even_closed_but_unreceived_original_effect_blocks_preparation_retirement(
    tmp_path, monkeypatch
):
    with _prepared(tmp_path, monkeypatch) as ready:
        _complete_registration(ready)
        _retained_effect(ready)
