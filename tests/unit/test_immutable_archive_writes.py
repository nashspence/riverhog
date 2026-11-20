from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from riverhog_age import encrypt_age_scrypt
from riverhog_core.archive_recovery_descriptor import ArchiveRecoveryDescriptorPublisher
from riverhog_core.archive_root import ArchiveRootPublisher
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services import immutable_archive_admission as admission_module
from riverhog_core.services.archive_authority_checkpoint import ArchiveRootSource
from riverhog_core.services.immutable_archive_writes import ImmutableArchiveWrites
from riverhog_protocol.errors import Conflict

from tests.unit.test_archive_authority_checkpoint import (
    _authority_source,
    _snapshot,
    _source,
    _write,
)
from tests.unit.test_archive_volume_metadata_receiving import _count


def _revision(fixture):
    with session_scope(fixture.case.factory) as session:
        return int(
            session.get(ArchiveWriteControlRecord, fixture.case.collection_id).custody_revision
        )


def _actor(fixture, source):
    calls = {"observe": [], "execute": []}

    class Ports:
        def observe_immutable_object(self, request):
            calls["observe"].append(request)
            return fixture.physical.observe_immutable_object(request)

        def execute_write(self, invocation, *, content):
            calls["execute"].append((invocation, content))
            return fixture.physical.execute_write(invocation, content=content)

    ports = Ports()
    return SimpleNamespace(
        fixture=fixture,
        source=source,
        ports=ports,
        calls=calls,
        actor=ImmutableArchiveWrites(
            fixture.case.factory,
            source=source,
            epoch=fixture.epoch,
            grants=fixture.grants,
            executor=ports,
            observer=ports,
        ),
    )


def _publish(original):
    # Source owners resume original results before constructing fresh format inputs.
    received = original.actor.resume_original()
    if received is not None:
        return original.source.publication.sealed_receipt(received)
    if isinstance(original.source, ArchiveRootSource):
        return ArchiveRootPublisher(
            object_store=original.actor,
            passphrase="fixture",
            scrypt_log_n=1,
        ).publish_root_manifest(
            archive_storage_prefix=original.source.publication.archive_storage_prefix,
            manifest=original.source.publication.plaintext,
        )
    source = original.source
    with session_scope(original.fixture.case.factory) as session:
        root = source.root_source.received(session)
    return ArchiveRecoveryDescriptorPublisher(object_store=original.actor).publish(
        archive_storage_prefix=source.publication.archive_storage_prefix,
        root=root,
        encryption=source.publication.descriptor.encryption,
    )


def _prior_root(fixture, kind):
    if kind == "recovery":
        source = _source(fixture, "root")
        owner, admitted, _ = _write(fixture, source)
        owner.acknowledge_report(admitted, verify_receipt=source.verify_prepared_receipt)


@pytest.mark.parametrize("mode", ["direct", "http"])
@pytest.mark.parametrize("kind", ["root", "recovery"])
def test_actual_shared_actor_retains_one_original_authority_invocation(
    tmp_path, monkeypatch, mode, kind
):
    with _authority_source(tmp_path, monkeypatch, mode) as fixture:
        _prior_root(fixture, kind)
        original = _actor(fixture, _source(fixture, kind))
        revision = _revision(fixture)
        first = _publish(original)
        before = _snapshot(fixture)
        assert _publish(original) == first and _snapshot(fixture) == before
        assert _revision(fixture) == revision + 1
        assert len(original.calls["observe"]) == len(original.calls["execute"]) == 1
        assert not original.actor.has_pending and _count(fixture.case) == 0
        assert (
            original.actor.flush().stored_sha256 == first.stored_sha256
            if kind == "root"
            else (original.actor.flush().stored_sha256 == first.sha256)
        )


@pytest.mark.parametrize("kind", ["root", "recovery"])
@pytest.mark.parametrize("phase", ["report", "retirement"])
@pytest.mark.parametrize("committed", [False, True])
def test_actual_authority_actor_replays_original_result_before_any_new_publisher_work(
    tmp_path, monkeypatch, kind, phase, committed
):
    with _authority_source(tmp_path, monkeypatch) as fixture:
        _prior_root(fixture, kind)
        original = _actor(fixture, _source(fixture, kind))
        revision = _revision(fixture)
        transaction = admission_module.session_scope
        position = 2 if phase == "report" else 3
        calls = 0

        @contextmanager
        def lost(factory):
            nonlocal calls
            calls += 1
            with transaction(factory) as session:
                yield session
                if calls == position and not committed:
                    raise OSError("injected original authority rollback")
            if calls == position and committed:
                raise OSError("injected original authority commit reply loss")

        with monkeypatch.context() as patch:
            patch.setattr(admission_module, "session_scope", lost)
            with pytest.raises(OSError, match="injected original authority"):
                _publish(original)
        assert original.actor.has_pending
        original_response = original.actor._response_json
        assert original_response is not None
        with session_scope(fixture.case.factory) as session:
            upload = session.get(Upload, fixture.case.collection_id)
            upload.state = upload.archive_phase = "orphaned"

        def forbidden(*args, **kwargs):
            raise AssertionError("original authority result must resume before new publisher work")

        with monkeypatch.context() as patch:
            patch.setattr(ArchiveRootPublisher, "publish_root_manifest", forbidden)
            patch.setattr(ArchiveRecoveryDescriptorPublisher, "publish", forbidden)
            result = _publish(original)
            assert _publish(original) == result
        assert original.actor._response_json == original_response
        assert _revision(fixture) == revision + 1
        assert len(original.calls["observe"]) == len(original.calls["execute"]) == 1
        assert not original.actor.has_pending and _count(fixture.case) == 0


@pytest.mark.parametrize("kind", ["root", "recovery"])
def test_actual_unknown_authority_result_stays_owned_and_cannot_advance_fresh_work(
    tmp_path, monkeypatch, kind
):
    with _authority_source(tmp_path, monkeypatch) as fixture:
        _prior_root(fixture, kind)
        source = _source(fixture, kind)
        original = _actor(fixture, source)
        execute = original.ports.execute_write

        def unknown(invocation, *, content):
            return execute(invocation, content=content).model_copy(update={"execution": "unknown"})

        monkeypatch.setattr(original.ports, "execute_write", unknown)
        with pytest.raises(Conflict, match="execution remains unknown"):
            _publish(original)
        assert original.actor.has_pending and _count(fixture.case) == 1
        with pytest.raises(Conflict, match="execution remains unknown"):
            _publish(original)
        assert len(original.calls["observe"]) == len(original.calls["execute"]) == 1
        cold = _actor(fixture, source)
        refusal = (
            "unresolved provider writes" if kind == "root" else "finalizer claim is not current"
        )
        with pytest.raises(Conflict, match=refusal):
            _publish(cold)
        assert cold.calls == {"observe": [], "execute": []} and _count(fixture.case) == 1
        if kind == "root":
            with session_scope(fixture.case.factory) as session:
                recovery = _source(fixture, "recovery")
                with pytest.raises(Conflict, match="unresolved provider writes"):
                    recovery.guard(session)


@pytest.mark.parametrize("mode", ["direct", "http"])
def test_actual_existing_root_observation_uses_original_unused_capability(
    tmp_path, monkeypatch, mode
):
    with _authority_source(tmp_path, monkeypatch, mode) as fixture:
        source = _source(fixture, "root")
        revision = _revision(fixture)
        physical = fixture.physical.put_immutable_object(
            object_path=source.publication.object_path,
            content=encrypt_age_scrypt(source.publication.plaintext, "fixture", log_n=1),
            content_type=source.publication.request(b"opaque").content_type,
            required_identity_assertions=source.publication.required_identity_assertions,
            placement_policy="immediate_default",
        )
        original = _actor(fixture, source)
        result = _publish(original)
        assert result.stored_sha256 == physical.stored_sha256
        assert result.stored_sha256 != original.actor._prepared.operation.request.stored_sha256
        assert len(original.calls["observe"]) == 1 and not original.calls["execute"]
        assert not original.actor.has_pending and _count(fixture.case) == 0
        assert _revision(fixture) == revision + 1
        assert _publish(original) == result and _revision(fixture) == revision + 1
        recovery = _source(fixture, "recovery")
        assert recovery.publication.descriptor.root.stored_sha256 == physical.stored_sha256


def _original_pair(fixture, monkeypatch):
    for kind in ("root", "recovery"):
        source = _source(fixture, kind)
        original = _actor(fixture, source)
        revision = _revision(fixture)
        report = original.actor._admission.report

        def lost(*args, _original_report=report, **kwargs):
            _original_report(*args, **kwargs)
            raise OSError("injected original report reply loss")

        with monkeypatch.context() as patch:
            patch.setattr(original.actor._admission, "report", lost)
            with pytest.raises(OSError, match="original report reply loss"):
                _publish(original)
        assert original.actor.has_pending
        receipt = _publish(original)
        assert not original.actor.has_pending and _count(fixture.case) == 0
        assert len(original.calls["observe"]) == len(original.calls["execute"]) == 1
        with session_scope(fixture.case.factory) as session:
            source.verify_prepared_receipt(
                session, original.actor._prepared, original.actor._receipt()
            )
        if kind == "root":
            assert _snapshot(fixture)[0] is not None and _snapshot(fixture)[1] is None
        else:
            assert _snapshot(fixture)[1] is not None
        assert receipt is not None
        assert _revision(fixture) == revision + 1
        assert _publish(original) == receipt and _revision(fixture) == revision + 1
