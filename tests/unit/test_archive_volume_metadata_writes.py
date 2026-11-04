from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from riverhog_age import encrypt_age_scrypt
from riverhog_core.archive_root import ArchiveRootPublisher
from riverhog_core.catalog_archive_write_models import ArchiveWriteEffectRecord
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services import immutable_archive_admission as admission_module
from riverhog_core.services import immutable_archive_preparation as preparation_module
from riverhog_core.services.archive_volume_metadata_checkpoint import ArchiveVolumeMetadataSource
from riverhog_core.services.archive_volume_metadata_writes import ArchiveVolumeMetadataWrites
from riverhog_protocol.errors import Conflict
from sqlalchemy import select

from tests.unit.test_archive_volume_metadata_checkpoint import _snapshot
from tests.unit.test_archive_volume_metadata_receiving import _count, _metadata_source
from tests.unit.test_finalization_claim_readiness import _fresh_coordinator


def _actor(fixture):
    case = fixture.case
    with session_scope(case.factory) as session:
        source = ArchiveVolumeMetadataSource.load(session, case.collection_id, 0)
    calls = {"observe": [], "execute": []}

    def committed():
        with session_scope(case.factory) as session:
            if session.get_bind().dialect.name == "sqlite":
                session.connection(execution_options={"sqlite_immediate_writer": True})
            upload = session.scalar(
                select(Upload)
                .where(Upload.collection_id == case.collection_id)
                .with_for_update(nowait=True)
            )
            assert upload.lease_expires_at is None
            effect = session.scalar(
                select(ArchiveWriteEffectRecord).where(
                    ArchiveWriteEffectRecord.collection_id == case.collection_id
                )
            )
            assert effect is not None and effect.state == "armed" and effect.unresolved
            assert effect.object_id != source.object_id and effect.resource == "put"

    class Ports:
        def observe_immutable_object(self, request):
            committed()
            calls["observe"].append(request)
            return fixture.physical.observe_immutable_object(request)

        def execute_write(self, invocation, *, content):
            committed()
            calls["execute"].append((invocation, content))
            return fixture.physical.execute_write(invocation, content=content)

    ports = Ports()
    actor = ArchiveVolumeMetadataWrites(
        case.factory,
        source=source,
        epoch=fixture.epoch,
        grants=fixture.grants,
        executor=ports,
        observer=ports,
    )
    return SimpleNamespace(case=case, source=source, actor=actor, calls=calls)


def _publish(original):
    # Publisher objects may be recreated. The original live actor stays owned.
    return ArchiveRootPublisher(
        object_store=original.actor, passphrase="fixture", scrypt_log_n=1
    ).publish_volume_metadata(
        archive_storage_prefix=original.source.publication.archive_storage_prefix,
        document=original.source.document,
    )


@pytest.mark.parametrize("kind", ["pack", "segment"])
@pytest.mark.parametrize("mode", ["direct", "http"])
def test_actual_publisher_uses_one_original_admitted_metadata_actor(
    tmp_path, monkeypatch, kind, mode
):
    with _metadata_source(tmp_path, monkeypatch, kind, mode) as fixture:
        original = _actor(fixture)
        first = _publish(original)
        before = _snapshot(original.case)
        assert _publish(original) == first and _snapshot(original.case) == before
        assert len(original.calls["observe"]) == len(original.calls["execute"]) == 1
        assert _count(original.case) == 0 and not original.actor.has_pending
        invocation, content = original.calls["execute"][0]
        assert content == original.actor._prepared.content
        assert first.stored_sha256 == invocation.operation.request.stored_sha256
        assert original.actor.flush().stored_sha256 == first.stored_sha256


@pytest.mark.parametrize("phase", ["preparation", "admission", "report", "retirement"])
@pytest.mark.parametrize("committed", [False, True])
def test_publisher_retry_retains_original_bytes_response_and_retirement_proof(
    tmp_path, monkeypatch, phase, committed
):
    with _metadata_source(tmp_path, monkeypatch) as fixture:
        original = _actor(fixture)
        module = preparation_module if phase == "preparation" else admission_module
        transaction = module.session_scope
        position = {"preparation": 1, "admission": 1, "report": 2, "retirement": 3}[phase]
        calls = 0

        @contextmanager
        def lost(factory):
            nonlocal calls
            calls += 1
            selected = calls == position
            with transaction(factory) as session:
                yield session
                if selected and not committed:
                    raise OSError("injected original metadata rollback")
            if selected and committed:
                raise OSError("injected original metadata commit reply loss")

        with monkeypatch.context() as patch:
            patch.setattr(module, "session_scope", lost)
            with pytest.raises(OSError, match="injected original metadata"):
                _publish(original)
        if phase == "admission" and committed:
            # An uncertain commit created no live capability. Cold preparation
            # cannot recover one, and retry preserves its unstarted charge.
            assert original.actor._admitted is None and original.actor.has_pending
            with pytest.raises(Conflict, match="admission remains unconfirmed"):
                original.actor.flush()
            with pytest.raises(Conflict, match="unresolved provider writes"):
                _publish(original)
            assert original.calls == {"observe": [], "execute": []}
            assert _count(original.case) == 1
            return
        first_content = original.calls["execute"][0][1] if original.calls["execute"] else None
        published = _publish(original)
        assert _publish(original) == published
        assert len(original.calls["observe"]) == len(original.calls["execute"]) == 1
        if first_content is not None:
            assert original.calls["execute"][0][1] == first_content
        assert _snapshot(original.case)[0] == 1 and _count(original.case) == 0


@pytest.mark.parametrize("phase", ["report", "retirement"])
@pytest.mark.parametrize("committed", [False, True])
def test_observed_original_ciphertext_keeps_unused_proof_across_reply_loss(
    tmp_path, monkeypatch, phase, committed
):
    with _metadata_source(tmp_path, monkeypatch) as fixture:
        original = _actor(fixture)
        publication = original.source.publication
        ciphertext = encrypt_age_scrypt(publication.plaintext, "fixture", log_n=1)
        request = publication.request(ciphertext)
        retained = fixture.physical.put_immutable_object(
            object_path=request.object_path,
            content=ciphertext,
            content_type=request.content_type,
            required_identity_assertions=request.required_identity_assertions,
            placement_policy=request.placement_policy,
        )
        transaction = admission_module.session_scope
        position = 2 if phase == "report" else 3
        calls = 0

        @contextmanager
        def lost(factory):
            nonlocal calls
            calls += 1
            selected = calls == position
            with transaction(factory) as session:
                yield session
                if selected and not committed:
                    raise OSError("injected observed metadata rollback")
            if selected and committed:
                raise OSError("injected observed metadata commit reply loss")

        with monkeypatch.context() as patch:
            patch.setattr(admission_module, "session_scope", lost)
            with pytest.raises(OSError, match="injected observed metadata"):
                _publish(original)
        published = _publish(original)
        assert published.stored_sha256 == retained.stored_sha256
        assert published.stored_sha256 != original.actor._prepared.operation.request.stored_sha256
        assert len(original.calls["observe"]) == 1 and original.calls["execute"] == []
        assert _count(original.case) == 0 and _snapshot(original.case)[0] == 1


def test_observer_interrupt_retains_unused_proof_when_its_settlement_fails(tmp_path, monkeypatch):
    with _metadata_source(tmp_path, monkeypatch) as fixture:
        original = _actor(fixture)
        interruption = KeyboardInterrupt("injected observer interruption")

        def interrupted(request):
            raise interruption

        def refused(proof):
            raise OSError("injected unused-proof settlement failure")

        with monkeypatch.context() as patch:
            patch.setattr(original.actor._observer, "observe_immutable_object", interrupted)
            patch.setattr(original.actor._admission, "settle_withdrawal", refused)
            with pytest.raises(KeyboardInterrupt) as error:
                _publish(original)
            assert error.value is interruption
        assert original.actor._withdrawal is not None
        assert original.actor.has_pending and _count(original.case) == 1
        assert original.calls["execute"] == []
        published = _publish(original)
        assert published.stored_bytes > 0 and _count(original.case) == 0
        assert len(original.calls["execute"]) == 1


def test_started_provider_interrupt_is_not_replaced_by_failed_bookkeeping_or_redispatched(
    tmp_path, monkeypatch
):
    with _metadata_source(tmp_path, monkeypatch) as fixture:
        original = _actor(fixture)
        interruption = KeyboardInterrupt("injected provider interruption")
        attempts = []

        def interrupted(invocation, *, content):
            attempts.append(invocation)
            fixture.physical.execute_write(invocation, content=content)
            raise interruption

        def refused(admitted):
            raise OSError("injected unknown bookkeeping failure")

        with monkeypatch.context() as patch:
            patch.setattr(original.actor._executor, "execute_write", interrupted)
            patch.setattr(original.actor._admission, "record_unknown", refused)
            with pytest.raises(KeyboardInterrupt) as error:
                _publish(original)
            assert error.value is interruption
        assert (
            fixture.physical.observe_immutable_object(original.actor._prepared.operation.request)
            is not None
        )
        with pytest.raises(Conflict, match="execution remains unknown"):
            _publish(original)
        assert len(attempts) == 1 and len(original.calls["observe"]) == 1
        assert _count(original.case) == 1 and _snapshot(original.case)[0] == 0
        assert original.actor.has_pending and original.actor._withdrawal is None


def test_positive_unknown_receipt_never_becomes_execution_closure_on_publisher_retry(
    tmp_path, monkeypatch
):
    with _metadata_source(tmp_path, monkeypatch) as fixture:
        original = _actor(fixture)
        execute = original.actor._executor.execute_write

        def unknown(invocation, *, content):
            return execute(invocation, content=content).model_copy(update={"execution": "unknown"})

        monkeypatch.setattr(original.actor._executor, "execute_write", unknown)
        for _ in range(3):
            with pytest.raises(Conflict, match="execution remains unknown"):
                _publish(original)
        assert len(original.calls["observe"]) == len(original.calls["execute"]) == 1
        assert _count(original.case) == 1 and _snapshot(original.case)[0] == 1
        assert original.actor.has_pending
        assert not _fresh_coordinator(original.case)._drain_archive_immutable_report(
            original.case.collection_id
        )


def test_live_publisher_accepts_actual_cold_receiving_proof_after_lost_report_reply(
    tmp_path, monkeypatch
):
    with _metadata_source(tmp_path, monkeypatch) as fixture:
        original = _actor(fixture)
        report = original.actor._admission.report

        def lost(*args, **kwargs):
            report(*args, **kwargs)
            raise OSError("injected report reply loss before cold drain")

        with monkeypatch.context() as patch:
            patch.setattr(original.actor._admission, "report", lost)
            with pytest.raises(OSError, match="before cold drain"):
                _publish(original)
        assert original.actor.has_pending and _count(original.case) == 1
        assert _fresh_coordinator(original.case)._drain_archive_immutable_report(
            original.case.collection_id
        )
        before = _snapshot(original.case)
        published = _publish(original)
        assert published.stored_bytes > 0
        assert _snapshot(original.case) == before and _count(original.case) == 0
        assert len(original.calls["execute"]) == len(original.calls["observe"]) == 1


def test_original_unused_proof_survives_lost_settlement_reply_and_cold_retirement(
    tmp_path, monkeypatch
):
    with _metadata_source(tmp_path, monkeypatch) as fixture:
        original = _actor(fixture)
        interruption = KeyboardInterrupt("injected interrupted observation")
        settle = original.actor._admission.settle_withdrawal

        def interrupted(request):
            raise interruption

        def lost(proof):
            settle(proof)
            raise OSError("injected unused settlement reply loss")

        with monkeypatch.context() as patch:
            patch.setattr(original.actor._observer, "observe_immutable_object", interrupted)
            patch.setattr(original.actor._admission, "settle_withdrawal", lost)
            with pytest.raises(KeyboardInterrupt) as error:
                _publish(original)
            assert error.value is interruption
        assert original.actor._withdrawal is not None and _count(original.case) == 1
        assert _fresh_coordinator(original.case)._drain_archive_immutable_report(
            original.case.collection_id
        )
        assert _count(original.case) == 0 and _snapshot(original.case)[0] == 0
        assert _publish(original).stored_bytes > 0
        assert len(original.calls["execute"]) == 1 and _count(original.case) == 0


def test_original_actor_receives_closed_result_after_terminal_and_forward_state_loss(
    tmp_path, monkeypatch
):
    from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission

    with _metadata_source(tmp_path, monkeypatch) as fixture:
        original = _actor(fixture)
        execute = original.actor._executor.execute_write

        def terminal_guard(session):
            # The fixture's independent terminal owner retains the original
            # construction. It must be able to fence unresolved forward work.
            original.source._parent(session)

        def revoked(invocation, *, content):
            response = execute(invocation, content=content)
            ArchiveWriteAdmission(original.case.factory).select_terminal(
                original.case.collection_id,
                kind="discard",
                request={"fixture": "original-metadata"},
                principal_id="fixture-operator",
                grants=fixture.grants,
                guard=terminal_guard,
                validate_selection=lambda *args: None,
            )
            with session_scope(original.case.factory) as session:
                upload = session.get(Upload, original.case.collection_id)
                upload.state = upload.archive_phase = "orphaned"
            return response

        monkeypatch.setattr(original.actor._executor, "execute_write", revoked)
        published = _publish(original)
        with session_scope(original.case.factory) as session:
            with pytest.raises(Conflict, match="claim is not current"):
                original.source.guard(session)
            original.source.verify_prepared_receipt(
                session, original.actor._prepared, original.actor._receipt()
            )
        assert _publish(original) == published and _count(original.case) == 0
        assert len(original.calls["execute"]) == 1


def test_missing_receiving_parent_preserves_original_actor_result_and_call_charge(
    tmp_path, monkeypatch
):
    from riverhog_protocol.errors import NotFound

    with _metadata_source(tmp_path, monkeypatch) as fixture:
        original = _actor(fixture)
        report = original.actor._admission.report

        def lost(*args, **kwargs):
            report(*args, **kwargs)
            raise OSError("injected receiving reply loss")

        with monkeypatch.context() as patch:
            patch.setattr(original.actor._admission, "report", lost)
            with pytest.raises(OSError):
                _publish(original)
        response = original.actor._response_json
        with session_scope(original.case.factory) as session:
            session.delete(session.get(Upload, original.case.collection_id))
        with pytest.raises(NotFound):
            _publish(original)
        assert _count(original.case) == 1 and original.actor.has_pending
        assert original.actor._response_json == response
        assert len(original.calls["execute"]) == len(original.calls["observe"]) == 1
