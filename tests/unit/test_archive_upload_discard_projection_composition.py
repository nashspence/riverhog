from __future__ import annotations

import threading
from types import SimpleNamespace

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services import archive_upload_terminal_registry as registry_module
from riverhog_core.services.archive_upload_discard_result_projection import (
    ArchiveUploadDiscardResultProjection,
    decode_discard_terminal_result,
)
from riverhog_core.services.archive_upload_terminal_turn import ArchiveUploadTerminalTurn
from riverhog_core.services.physical_execution import PhysicalExecutionKey

from tests.unit.test_archive_upload_discard_result_projection import _discard, _state
from tests.unit.test_archive_upload_projection_cleanup import LIMITS
from tests.unit.test_archive_upload_terminal_completion import _quantum


def _actor(ready, monkeypatch):
    case = ready.case
    registry = case.service._archive_terminals
    projection = ArchiveUploadDiscardResultProjection(
        case.factory, case.service._archive_terminal_observation
    )
    created = []

    def create(collection_id, operation_id):
        created.append(threading.current_thread().name)
        return ArchiveUploadTerminalTurn(
            case.factory,
            collection_id,
            operation_id,
            guard=ready.guard,
            adjudication=case.service._archive_terminal_adjudication,
            retirement=case.service._archive_terminal_retirement,
            result_projection=projection,
            result_projection_limits=LIMITS,
        )

    monkeypatch.setattr(registry, "_create", create)
    clock = [100.0]
    monkeypatch.setattr(registry_module, "monotonic", lambda: clock[0])
    return SimpleNamespace(
        ready=ready,
        registry=registry,
        created=created,
        projection=projection,
        key=PhysicalExecutionKey(case.collection_id, case.operation),
        clock=clock,
        original_context=None,
    )


def _compose(ready, monkeypatch, *, revoked=False, committed=None):
    from contextlib import contextmanager

    from riverhog_core.app_permissions import (
        COLLECTIONS_CREATE,
        COLLECTIONS_DELETE,
        ApplicationAccess,
    )
    from riverhog_core.services import archive_upload_discard_result_projection as projection_module
    from riverhog_protocol.errors import NotFound

    from tests.unit.test_app_keys import BOOTSTRAP

    actor = _actor(ready, monkeypatch)
    if committed is not None:
        actual = projection_module.session_scope

        @contextmanager
        def lose(factory):
            with actual(factory) as session:
                yield session
                if not committed:
                    raise OSError("original actor projection rollback")
            raise OSError("original actor projection commit reply lost")

        with monkeypatch.context() as patch:
            patch.setattr(projection_module, "session_scope", lose)
            with pytest.raises(OSError, match="original actor projection"):
                for _ in range(32):
                    _quantum(actor)
        assert (_state(ready)[1] is not None) is committed
    for _ in range(32):
        _quantum(actor)
        if _state(ready)[1] is not None:
            break
    else:
        pytest.fail("original actor did not retain a partial result projection")
    original = actor.original_context
    assert actor.created == ["physical-execution"]
    assert ready.case.service._archive_retention._counts == {False: 0, True: 1}
    if revoked:
        before = _state(ready)
        case = ready.case
        case.keys.replace_access(
            app=case.principal.id,
            key_id=case.principal.key_id,
            access=(ApplicationAccess(COLLECTIONS_CREATE),),
            grantor=BOOTSTRAP,
        )
        with pytest.raises(NotFound):
            _quantum(actor)
        assert _state(ready) == before and actor.original_context is original
        case.keys.replace_access(
            app=case.principal.id,
            key_id=case.principal.key_id,
            access=(ApplicationAccess(COLLECTIONS_DELETE),),
            grantor=BOOTSTRAP,
        )
    for _ in range(ready.count + 100):
        progress = _quantum(actor)
        assert not progress.execution_stopped
        if progress.phase == "cleanup-ready" and progress.originals_retired:
            break
    else:
        pytest.fail("original result did not enter fresh current adjudication")
    phase, partial, encoded, result = _state(ready)
    assert phase == "cleanup-ready" and partial is None and result is None
    assert decode_discard_terminal_result(encoded) == ready.expected
    assert not progress.archive_inventory_exhausted
    assert actor.created == ["physical-execution"] and actor.original_context is original
    assert ready.case.service._archive_retention._counts == {False: 0, True: 1}
    with session_scope(ready.case.factory) as session:
        assert session.get(Upload, ready.case.collection_id) is not None


@pytest.mark.parametrize(
    "revoked,committed", ((False, None), (True, None), (False, False), (False, True))
)
def test_same_original_actor_prepares_discard_result_without_confirming_or_releasing_custody(
    tmp_path, monkeypatch, revoked, committed
):
    with _discard(tmp_path) as ready:
        _compose(ready, monkeypatch, revoked=revoked, committed=committed)


@pytest.mark.parametrize("with_owner", (False, True))
def test_result_projection_requires_paired_original_owner_and_explicit_quantum(
    tmp_path, with_owner
):
    with _discard(tmp_path) as ready:
        projection = ArchiveUploadDiscardResultProjection(
            ready.case.factory, ready.case.service._archive_terminal_observation
        )
        with pytest.raises(ValueError, match="owner and bounded limits"):
            ArchiveUploadTerminalTurn(
                ready.case.factory,
                ready.case.collection_id,
                ready.case.operation,
                guard=ready.guard,
                adjudication=ready.case.service._archive_terminal_adjudication,
                retirement=ready.case.service._archive_terminal_retirement,
                result_projection=projection if with_owner else None,
                result_projection_limits=None if with_owner else LIMITS,
            )
