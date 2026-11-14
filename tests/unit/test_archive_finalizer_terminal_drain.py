from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from functools import partial
from types import SimpleNamespace

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_core.services.immutable_archive_admission import ImmutableArchiveAdmission
from riverhog_core.services.immutable_archive_writes import ImmutableArchiveWrites
from riverhog_protocol.errors import Conflict

from tests.unit import test_archive_authority_checkpoint as root_source
from tests.unit import test_archive_authority_finalizer as root_owner
from tests.unit import test_archive_initial_description_checkpoint as description_source
from tests.unit import test_archive_initial_description_finalizer as description_owner
from tests.unit import test_archive_initial_tag_checkpoint as tag_source
from tests.unit import test_archive_initial_tag_finalizer as tag_owner
from tests.unit import test_archive_provenance_authority_checkpoint as authority_source
from tests.unit import test_archive_provenance_authority_finalizer as authority_owner
from tests.unit import test_archive_provenance_journal_checkpoint as journal_source
from tests.unit import test_archive_provenance_journal_writer as journal_owner
from tests.unit import test_archive_provenance_structure_checkpoint as structure_source
from tests.unit import test_archive_provenance_structure_writer as structure_owner
from tests.unit import test_archive_provenance_volume_checkpoint as volume_source
from tests.unit import test_archive_provenance_volume_finalizer as volume_owner
from tests.unit import test_archive_volume_metadata_finalizer as archive_volume_owner
from tests.unit.test_archive_provenance_journal_writer import _lose_ack
from tests.unit.test_archive_volume_metadata_receiving import _count, _metadata_source

_KINDS = (
    "archive-volume",
    "archive-terminal",
    "description",
    "tag-node",
    "provenance-authority",
    "root",
    "recovery",
    "structure",
    "journal",
    "volume-bindings",
    "volume-journal",
    "volume-metadata",
)


@contextmanager
def _original(tmp_path, monkeypatch, kind, mode="direct", *, database_url=None):
    args = {"mode": mode, "database_url": database_url}
    prior = 0
    if kind in {"archive-volume", "archive-terminal"}:
        context = _metadata_source(tmp_path, monkeypatch, **args)
        make = archive_volume_owner._owner
        prior = 2 if kind == "archive-terminal" else 0
    elif kind == "description":
        context = description_source._description_source(tmp_path, monkeypatch, **args)
        make = description_owner._owner
    elif kind == "tag-node":
        context = tag_source._tag_source(tmp_path, monkeypatch, role="node", **args)
        make = tag_owner._owner
    elif kind == "provenance-authority":
        context = authority_source._authority_source(tmp_path, monkeypatch, **args)
        make = authority_owner._owner
    elif kind in {"root", "recovery"}:
        context = root_source._authority_source(tmp_path, monkeypatch, **args)
        make = root_owner._owner
        prior = int(kind == "recovery")
    elif kind == "structure":
        context = structure_source._structure_source(tmp_path, monkeypatch, **args)
        make = structure_owner._owner
    elif kind == "journal":
        context = journal_source._journal_source(tmp_path, monkeypatch, **args)
        make = journal_owner._owner
    else:
        assert kind in {"volume-bindings", "volume-journal", "volume-metadata"}
        context = volume_source._volume_source(
            tmp_path,
            monkeypatch,
            kind="journal" if kind == "volume-journal" else "bindings",
            **args,
        )
        make = volume_owner._owner
        prior = int(kind == "volume-metadata")
    with context as fixture:
        original = make(fixture)
        owner = getattr(original, "owner", original)
        if kind == "structure":
            advance = partial(structure_owner._advance, owner, fixture)
        elif kind == "journal":
            advance = partial(journal_owner._advance, owner, offset=8)
        else:
            advance = owner.advance
        for _ in range(prior):
            assert advance()
        with session_scope(fixture.case.factory) as session:
            upload = session.get(Upload, fixture.case.collection_id)
            ports = owner._archive_stores.require_incarnation(
                upload.archive_store, upload.archive_incarnation_id
            ).require_immutable_writes()
        yield SimpleNamespace(
            fixture=fixture,
            owner=owner,
            advance=advance,
            case=fixture.case,
            ports=ports,
            grants=owner._config.archive_write,
            kind=kind,
        )


def _terminal(original):
    return ArchiveWriteAdmission(original.case.factory).select_terminal(
        original.case.collection_id,
        kind="cancel",
        request={"fixture": "retained-finalizer-terminal"},
        principal_id="uploader",
        key_id=None,
        grants=original.grants,
        guard=lambda session: None,
        validate_selection=lambda *args: None,
    )


def _inputs(owner):
    return {
        name: getattr(owner, name)
        for name in (
            "_source",
            "_actor",
            "_passphrase",
            "_scrypt_log_n",
            "_phase",
            "_selected",
            "_volume",
        )
        if hasattr(owner, name)
    }


def _same_inputs(owner, inputs):
    assert all(getattr(owner, name) is value for name, value in inputs.items())


def _stopped(original, operation):
    assert original.owner.drain_terminal(operation) == "closed"
    with pytest.raises(Conflict, match="stopped for its terminal"):
        original.advance()
    with pytest.raises(Conflict, match="another terminal request"):
        original.owner.drain_terminal("f" * 64)


@pytest.mark.parametrize("kind", _KINDS)
@pytest.mark.parametrize("mode", ("direct", "http"))
def test_idle_finalizer_requires_exact_selection_and_never_selects_after_terminal(
    tmp_path, monkeypatch, kind, mode
):
    with _original(tmp_path, monkeypatch, kind, mode) as original:
        with pytest.raises(Conflict, match="not current"):
            original.owner.drain_terminal("f" * 64)
        assert original.owner._terminal._operation_id is None
        operation = _terminal(original)
        original.owner._config = original.owner._archive_stores = None
        _stopped(original, operation)
        assert not original.owner.has_pending and _count(original.case) == 0
        with session_scope(original.case.factory) as session:
            session.delete(session.get(Upload, original.case.collection_id))
        assert original.owner.drain_terminal(operation) == "closed"


def _late_result(original, monkeypatch, phase="report", committed=True):
    with _lose_ack(monkeypatch, phase, committed):
        with pytest.raises(OSError, match="injected journal writer"):
            original.advance()
    actor = original.owner._actor
    assert actor is not None and actor._response_json is not None
    inputs = _inputs(original.owner)
    response = actor._response_json
    operation = _terminal(original)
    original.owner._config = original.owner._archive_stores = None
    with session_scope(original.case.factory) as session:
        upload = session.get(Upload, original.case.collection_id)
        upload.state = upload.archive_phase = "orphaned"
    # A committed retirement can already be recovered from original receiving
    # proof. Other paths still retry the retained report/retirement transaction.
    if phase == "retirement" and committed:
        assert original.owner.drain_terminal(operation) == "closed"
    else:
        with _lose_ack(monkeypatch, phase, committed):
            with pytest.raises(OSError, match="injected journal writer"):
                original.owner.drain_terminal(operation)
    _same_inputs(original.owner, inputs)
    _stopped(original, operation)
    _same_inputs(original.owner, inputs)
    assert actor._response_json == response and not actor.has_pending
    assert original.owner.has_pending and _count(original.case) == 0
    if original.kind == "journal":
        # Original receiving preserves the canonical range at zero. The
        # separately selected repeated range is not advanced by drainage.
        assert journal_owner._association(original.fixture) is not None
        assert journal_owner._association(original.fixture, offset=8) is None
        assert inputs["_selected"].offset == 8


@pytest.mark.parametrize("kind", _KINDS)
@pytest.mark.parametrize("phase", ("report", "retirement"))
@pytest.mark.parametrize("committed", (False, True))
def test_finalizer_drains_original_results_through_lost_ack_without_releasing_inputs(
    tmp_path, monkeypatch, kind, phase, committed
):
    with _original(tmp_path, monkeypatch, kind) as original:
        _late_result(original, monkeypatch, phase, committed)


def _unknown(original, monkeypatch, returned):
    execute = original.ports.execute_write

    def uncertain(invocation, *, content):
        response = execute(invocation, content=content)
        if not returned:
            raise OSError("injected provider reply loss")
        return response.model_copy(update={"execution": "unknown"})

    with monkeypatch.context() as patch:
        patch.setattr(original.ports, "execute_write", uncertain)
        with pytest.raises((Conflict, OSError)):
            original.advance()
    actor = original.owner._actor
    inputs = _inputs(original.owner)
    operation = _terminal(original)
    if returned:
        with pytest.raises(Conflict, match="execution remains unknown"):
            original.owner.drain_terminal(operation)
    else:
        assert original.owner.drain_terminal(operation) == "pending"
    _same_inputs(original.owner, inputs)
    assert original.owner.has_pending and actor.has_pending and _count(original.case) == 1
    with pytest.raises(Conflict, match="stopped for its terminal"):
        original.advance()


@pytest.mark.parametrize("kind", _KINDS)
@pytest.mark.parametrize("returned", (False, True))
def test_finalizer_keeps_unknown_execution_and_all_original_resources(
    tmp_path, monkeypatch, kind, returned
):
    with _original(tmp_path, monkeypatch, kind) as original:
        _unknown(original, monkeypatch, returned)


@pytest.mark.parametrize("kind", _KINDS)
@pytest.mark.parametrize("committed", (False, True))
def test_finalizer_unconfirmed_admission_stays_pending_even_without_a_catalog_effect(
    tmp_path, monkeypatch, kind, committed
):
    with _original(tmp_path, monkeypatch, kind) as original:
        arm = ImmutableArchiveAdmission.arm

        def lost(*args, **kwargs):
            if committed:
                arm(*args, **kwargs)
            raise OSError("injected admission acknowledgement loss")

        with monkeypatch.context() as patch:
            patch.setattr(ImmutableArchiveAdmission, "arm", lost)
            with pytest.raises(OSError, match="admission acknowledgement loss"):
                original.advance()
        inputs = _inputs(original.owner)
        operation = _terminal(original)
        assert original.owner.drain_terminal(operation) == "pending"
        _same_inputs(original.owner, inputs)
        assert original.owner.has_pending and _count(original.case) == int(committed)
        with pytest.raises(Conflict, match="stopped for its terminal"):
            original.advance()


def _busy(original, monkeypatch, before_admission):
    started, release = threading.Event(), threading.Event()
    if before_admission:
        owner, method = ImmutableArchiveWrites, "put_immutable_object"
    else:
        owner, method = original.ports, "execute_write"
    execute = getattr(owner, method)

    def held(*args, **kwargs):
        started.set()
        assert release.wait(15)
        return execute(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(owner, method, held)
        with ThreadPoolExecutor(max_workers=2) as pool:
            forward = pool.submit(original.advance)
            try:
                assert started.wait(15)
                inputs = _inputs(original.owner)
                assert original.owner.has_pending
                operation = _terminal(original)
                drain = pool.submit(original.owner.drain_terminal, operation)
                assert drain.result(timeout=5) == "busy" and not forward.done()
                _same_inputs(original.owner, inputs)
            finally:
                release.set()
            if before_admission:
                with pytest.raises(Conflict):
                    forward.result(timeout=15)
                _same_inputs(original.owner, inputs)
            else:
                try:
                    forward.result(timeout=15)
                except Conflict:
                    # An original association can still be pending after the
                    # physical report drained; terminal does not advance it.
                    pass
    _stopped(original, operation)
    assert _count(original.case) == 0


@pytest.mark.parametrize("kind", _KINDS)
@pytest.mark.parametrize("before_admission", (False, True))
def test_finalizer_drain_does_not_wait_for_live_preparation_or_provider_work(
    tmp_path, monkeypatch, kind, before_admission
):
    with _original(tmp_path, monkeypatch, kind) as original:
        _busy(original, monkeypatch, before_admission)


@pytest.mark.parametrize(
    "kind", ("description", "tag-node", "provenance-authority", "volume-bindings")
)
def test_terminal_fence_retains_unacknowledged_logical_progress_without_advancing_it(
    tmp_path, monkeypatch, kind
):
    with _original(tmp_path, monkeypatch, kind) as original:
        for _ in range(2 if kind == "volume-bindings" else 1):
            assert original.advance()
        fixture = original.fixture
        source = (
            fixture.authority
            if kind == "provenance-authority"
            else fixture.metadata
            if kind == "volume-bindings"
            else fixture.source
        )
        method = "commit_volume" if kind == "volume-bindings" else "commit_progress"
        calls = 0

        def interrupted(*args, **kwargs):
            nonlocal calls
            calls += 1
            raise OSError("injected original logical progress")

        with monkeypatch.context() as patch:
            patch.setattr(type(source), method, interrupted)
            with pytest.raises(OSError, match="original logical progress"):
                original.advance()
            assert original.owner.has_pending and original.owner._actor is None
            inputs = _inputs(original.owner)
            operation = _terminal(original)
            _stopped(original, operation)
            _same_inputs(original.owner, inputs)
            assert original.owner.has_pending and calls == 1
            assert _count(original.case) == 0
