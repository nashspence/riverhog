from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, replace

import pytest
from riverhog_core.canonical_discovery_corpus_jobs import DiscoveryJob
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_discovery_validation_models import DiscoveryValidationJobRecord as Job
from riverhog_core.incremental_emission import VolumeEmissionCapacity
from riverhog_core.journal_validation_workspace import (
    LostJournalValidationWorkspace,
    lease_journal_validation_workspace,
)
from riverhog_core.services import archive_upload_projection_cleanup as module
from riverhog_core.services.archive_upload_projection_cleanup import ArchiveUploadProjectionCleanup
from riverhog_protocol.errors import Conflict

from tests.unit.test_archive_upload_projection_cleanup import (
    LIMITS,
    _count,
    _finish,
    _projection,
)


def _advance(ready, *, limits=LIMITS):
    return ArchiveUploadProjectionCleanup(
        ready.case.factory, ready.ready.owner, discovery_root=ready.discovery.root
    ).advance(ready.case.collection_id, ready.case.operation, guard=ready.guard, limits=limits)


def _job(ready):
    with session_scope(ready.case.factory) as session:
        return DiscoveryJob.read(session.get(Job, ready.discovery.original.workspace_id))


def _busy(ready):
    with lease_journal_validation_workspace(
        ready.discovery.root, ready.discovery.original.workspace_id, create=False
    ) as directory:
        assert directory is None


def _retire(ready):
    before = _count(ready)
    held = ready.discovery
    assert _advance(ready).pending_reason == "discovery-custody"
    assert _job(ready) == held.original and _count(ready) == before
    held.custody.close()
    progress = _advance(ready)
    assert progress.discovery_retired and progress.removed == 0 and not progress.exhausted
    assert progress.pending_reason is None and progress.metadata_bytes > 0
    retired = _job(ready)
    expected = asdict(held.original)
    expected.update(
        build_id=None,
        state="retiring",
        version=retired.version,
        reclamation_phase="children" if expected["storage_state"] == "confirmed" else "metadata",
    )
    assert asdict(retired) == expected and retired.version != held.original.version
    assert held.marker.read_bytes() == b"original independently leased bytes"
    # Catalog detachment does not claim independent file reclamation.
    with lease_journal_validation_workspace(held.root, retired.workspace_id) as directory:
        assert directory == held.marker.parent
    _finish(ready)
    assert _job(ready) == retired
    assert held.marker.read_bytes() == b"original independently leased bytes"


def test_exact_discovery_detaches_only_after_original_lease_closes_and_keeps_its_receipt(tmp_path):
    with _projection(tmp_path, indexed=True, linked=True) as ready:
        _retire(ready)


def _lost(ready, monkeypatch, *, committed):
    ready.discovery.custody.close()
    actual = module.session_scope

    @contextmanager
    def lose(factory):
        with actual(factory) as session:
            yield session
            # Physical custody must still exclude every receiver at commit.
            _busy(ready)
            if not committed:
                raise OSError("discovery retirement rollback")
        _busy(ready)
        raise OSError("discovery retirement commit reply lost")

    with monkeypatch.context() as patch:
        patch.setattr(module, "session_scope", lose)
        with pytest.raises(OSError, match="discovery retirement"):
            _advance(ready)
    if committed:
        retired = _job(ready)
        assert retired.build_id is None and retired.state == "retiring"
        assert not _advance(ready).discovery_retired
    else:
        assert _job(ready) == ready.discovery.original
        assert _advance(ready).discovery_retired
    assert ready.discovery.marker.read_bytes() == b"original independently leased bytes"
    _finish(ready)


@pytest.mark.parametrize("committed", (False, True))
def test_discovery_detachment_holds_the_lease_through_rollback_and_lost_commit_reply(
    tmp_path, monkeypatch, committed
):
    with _projection(tmp_path, indexed=True, linked=True) as ready:
        _lost(ready, monkeypatch, committed=committed)


@pytest.mark.parametrize("missing", ("root", "coordinator", "directory", "lease"))
def test_missing_discovery_custody_never_creates_files_or_detaches_the_job(tmp_path, missing):
    with _projection(tmp_path, indexed=True, linked=True) as ready:
        ready.discovery.custody.close()
        root, marker = ready.discovery.root, ready.discovery.marker
        if missing == "root":
            root.rename(root.with_name("retained-original-root"))
        elif missing == "directory":
            marker.parent.rename(root / ("1" * 32))
        else:
            (
                root / ".coordinator" if missing == "coordinator" else marker.parent / ".lease"
            ).unlink()
        before = _count(ready)
        with pytest.raises(LostJournalValidationWorkspace):
            _advance(ready)
        assert _job(ready) == ready.discovery.original and _count(ready) == before
        if missing == "root":
            assert not root.exists()
        elif missing == "directory":
            assert not marker.parent.exists()
        else:
            assert not (
                root / ".coordinator" if missing == "coordinator" else marker.parent / ".lease"
            ).exists()


@pytest.mark.parametrize(
    "field",
    (
        "archive_generation",
        "archive_root_sha256",
        "provenance_identity",
        "artifact_set_identity",
        "build_epoch",
        "policy_sha256",
    ),
)
def test_discovery_detachment_refuses_a_different_original_projection_before_custody(
    tmp_path, monkeypatch, field
):
    with _projection(tmp_path, indexed=True, linked=True) as ready:
        with session_scope(ready.case.factory) as session:
            job = session.get(Job, ready.discovery.original.workspace_id)
            setattr(job, field, 2 if field == "build_epoch" else "0" * 64)
        before = _job(ready), _count(ready)

        def forbidden(*args, **kwargs):
            raise AssertionError("different original must not request filesystem custody")

        monkeypatch.setattr(module, "lease_journal_validation_workspace", forbidden)
        with pytest.raises(Conflict, match="original projection"):
            _advance(ready)
        assert (_job(ready), _count(ready)) == before


def test_discovery_detachment_charges_the_complete_opaque_job_before_requesting_custody(
    tmp_path, monkeypatch
):
    with _projection(tmp_path, indexed=True, linked=True) as ready:
        with session_scope(ready.case.factory) as session:
            session.get(Job, ready.discovery.original.workspace_id).failure = "x" * 30000
        before = _job(ready), _count(ready)

        def forbidden(*args, **kwargs):
            raise AssertionError("unadmitted metadata must not request filesystem custody")

        with monkeypatch.context() as patch:
            patch.setattr(module, "lease_journal_validation_workspace", forbidden)
            with pytest.raises(VolumeEmissionCapacity) as failure:
                _advance(ready, limits=replace(LIMITS, metadata_bytes=20000))
        assert failure.value.required > 30000 and (_job(ready), _count(ready)) == before
        ready.discovery.custody.close()
        assert _advance(ready, limits=replace(LIMITS, metadata_bytes=40000)).discovery_retired


def _revoked(ready):
    from riverhog_core.app_permissions import (
        COLLECTIONS_CREATE,
        COLLECTIONS_DELETE,
        ApplicationAccess,
    )
    from riverhog_protocol.errors import NotFound

    from tests.unit.test_app_keys import BOOTSTRAP

    ready.discovery.custody.close()
    case = ready.case
    case.keys.replace_access(
        app=case.principal.id,
        key_id=case.principal.key_id,
        access=(ApplicationAccess(COLLECTIONS_CREATE),),
        grantor=BOOTSTRAP,
    )
    before = _count(ready)
    with pytest.raises(NotFound):
        _advance(ready)
    assert _job(ready) == ready.discovery.original and _count(ready) == before
    case.keys.replace_access(
        app=case.principal.id,
        key_id=case.principal.key_id,
        access=(ApplicationAccess(COLLECTIONS_DELETE),),
        grantor=BOOTSTRAP,
    )
    assert _advance(ready).discovery_retired
    _finish(ready)


def test_discovery_detachment_rechecks_current_terminal_rights_after_physical_release(tmp_path):
    with _projection(tmp_path, indexed=True, linked=True) as ready:
        _revoked(ready)


def _composed(ready, monkeypatch, *, committed=None):
    from tests.unit.test_archive_upload_terminal_completion import _actor, _drive, _quantum
    from tests.unit.test_archive_upload_terminal_confirmation import _confirmed

    actor = _actor(ready, monkeypatch, discovery_root=ready.discovery.root)
    for _ in range(32):
        progress = _quantum(actor)
        assert not progress.execution_stopped
        if progress.pending_reason == "discovery-custody":
            break
    else:
        pytest.fail("original actor did not reach exact busy discovery custody")
    assert _job(ready) == ready.discovery.original
    assert _count(ready) > 0 and _confirmed(ready) is None
    assert ready.case.service._archive_retention._counts == {False: 0, True: 1}
    context = actor.original_context
    ready.discovery.custody.close()
    if committed is not None:
        actual = module.session_scope

        @contextmanager
        def lose(factory):
            with actual(factory) as session:
                yield session
                _busy(ready)
                if not committed:
                    raise OSError("original discovery detachment rollback")
            _busy(ready)
            raise OSError("original discovery detachment commit reply lost")

        with monkeypatch.context() as patch:
            patch.setattr(module, "session_scope", lose)
            with pytest.raises(OSError, match="original discovery detachment"):
                for _ in range(32):
                    progress = _quantum(actor)
                    assert not progress.execution_stopped
        assert (_job(ready).build_id is None) is committed
        assert ready.case.service._archive_retention._counts == {False: 0, True: 1}
    _drive(actor)
    assert actor.original_context is context and actor.created == ["physical-execution"]
    assert _confirmed(ready) == ready.original
    retired = _job(ready)
    expected = asdict(ready.discovery.original)
    expected.update(
        build_id=None,
        state="retiring",
        version=retired.version,
        reclamation_phase="children" if expected["storage_state"] == "confirmed" else "metadata",
    )
    assert asdict(retired) == expected
    assert ready.discovery.marker.read_bytes() == b"original independently leased bytes"
    with lease_journal_validation_workspace(
        ready.discovery.root, retired.workspace_id, create=False
    ) as directory:
        assert directory == ready.discovery.marker.parent


@pytest.mark.parametrize("committed", (None, False, True))
def test_same_original_actor_receives_completion_after_exact_discovery_detachment(
    tmp_path, monkeypatch, committed
):
    with _projection(tmp_path, indexed=True, linked=True) as ready:
        _composed(ready, monkeypatch, committed=committed)
