from __future__ import annotations

import hashlib
import json
import threading
import uuid
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from a_riverhog_filesystem_store import FilesystemStorageAdapter, FilesystemStorageAdapterConfig
from a_riverhog_filesystem_store import incarnation as filesystem_incarnation
from riverhog_age import CHUNK_SIZE
from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.catalog_archive_write_models import (
    ArchiveUploadTerminalRecord,
    ArchiveWriteControlRecord,
    ArchiveWriteEffectRecord,
    ArchiveWritePreparationRecord,
)
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord, CollectionUploadRecord
from riverhog_core.services.archive_payload_input import (
    ArchivePayloadInput,
    ArchivePayloadRequest,
)
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_core.stores.storage_adapter_archive_objects import (
    StorageAdapterArchiveResumableObjectStore,
)
from riverhog_protocol.errors import Conflict
from sqlalchemy import select

from tests.unit.test_archive_finalizer_registry import _scope
from tests.unit.test_archive_upload_invocations import _receive
from tests.unit.test_archive_write_execution import _executor
from tests.unit.test_collection_upload_preparation import _case
from tests.unit.test_collection_uploads import _CREATOR


def _payload(service, key, target, volume, plan, content, *, principal=_CREATOR, unit=0):
    epoch = ArchiveWriteAdmission(service._session_factory).bind_epoch(
        key.collection_id, guard=service._payload_write_guard(key.collection_id, principal)
    )
    payload = service._archive_inputs.reserve(
        ArchivePayloadRequest(key, target, epoch, volume, unit, plan, len(content), principal)
    )
    assert isinstance(payload, ArchivePayloadInput)
    payload.append(content)
    payload.seal()
    return payload


@contextmanager
def _service_actor(
    tmp_path, monkeypatch, kind, mode, *, database_url=None, custody_mode="producer-retained"
):
    with _case(
        tmp_path,
        kind,
        content=b"x" * CHUNK_SIZE,
        database_url=database_url,
        custody_mode=custody_mode,
    ) as case:
        service = case.service
        with session_scope(case.factory) as session:
            upload = session.get(CollectionUploadRecord, case.collection_id)
            target, store = upload.archive_incarnation_id, upload.archive_store
        root = tmp_path / "provider"
        root.mkdir()
        with monkeypatch.context() as patch:
            patch.setattr(filesystem_incarnation, "uuid4", lambda: uuid.UUID(target))
            assert filesystem_incarnation.provision_storage_root(root) == target
        with (
            FilesystemStorageAdapter(
                FilesystemStorageAdapterConfig(
                    root=root,
                    segment_bytes=64 * 1024,
                    read_chunk_bytes=64 * 1024,
                    minimum_free_bytes=0,
                )
            ) as adapter,
            _executor(adapter, mode) as transport,
        ):
            provider = StorageAdapterArchiveResumableObjectStore(transport)
            seen, observed_preparations = [], []

            class Executor:
                def execute_write(self, invocation, *, content=b""):
                    with session_scope(case.factory) as session:
                        if session.get_bind().dialect.name == "sqlite":
                            session.connection(execution_options={"sqlite_immediate_writer": True})
                        effect = session.get(ArchiveWriteEffectRecord, invocation.call.call_id)
                        assert effect is not None and effect.state == "armed"
                        upload = session.scalar(
                            select(CollectionUploadRecord)
                            .where(CollectionUploadRecord.collection_id == effect.collection_id)
                            .with_for_update(nowait=True)
                        )
                        assert upload is not None
                        preparation = session.get(
                            ArchiveWritePreparationRecord, (effect.collection_id, effect.object_id)
                        )
                        assert preparation is not None
                        observed_preparations.append(
                            hashlib.sha256(preparation.preparation_json.encode()).hexdigest()
                        )
                    seen.append(invocation)
                    return provider.execute_write(invocation, content=content)

            executor = Executor()
            binding = service._archive_stores.require(store)
            service._archive_stores = ArchiveStoreRegistry(
                {store: replace(binding, resumable_objects=provider, resumable_writes=executor)}
            )
            admission = ArchiveWriteAdmission(case.factory)
            admission.bind_epoch(
                case.collection_id, guard=service._payload_write_guard(case.collection_id, _CREATOR)
            )
            key, target = _scope(case)
            payload = _payload(service, key, target, case.object_id, case.plan_sha256, case.content)
            request = payload.request_sha256
            registry, execution = service._archive_invocations, service._archive_execution
            originals = []

            def create():
                owner = service._payload_unit_invocation(
                    key=key,
                    target_incarnation=target,
                    volume_id=case.object_id,
                    unit=0,
                    plan_sha256=case.plan_sha256,
                    payload=payload,
                    principal=_CREATOR,
                )
                originals.append(owner)
                return owner

            def submit(factory=create, *, incoming=payload):
                return registry.submit(
                    key,
                    target_incarnation=target,
                    request_sha256=incoming.request_sha256,
                    payload=incoming,
                    create=factory,
                    advance=lambda owner: owner.advance(),
                )

            yield SimpleNamespace(
                case=case,
                service=service,
                executor=executor,
                seen=seen,
                preparations=observed_preparations,
                admission=admission,
                key=key,
                target=target,
                request=request,
                registry=registry,
                execution=execution,
                create=create,
                submit=submit,
                originals=originals,
                payload=payload,
                adapter=adapter,
            )


def _select_terminal(f):
    operation = f.admission.select_terminal(
        f.case.collection_id,
        kind="cancel",
        request={"fixture": f.request},
        principal_id=_CREATOR.id,
        key_id=_CREATOR.key_id,
        grants=f.service._config.archive_write,
        guard=f.service._payload_write_guard(f.case.collection_id, _CREATOR),
        validate_selection=lambda session, upload: None,
    )
    assert (
        f.registry.fence_terminal(f.key, target_incarnation=f.target, operation_id=operation)
        == "fenced"
    )
    return operation


def _stored(f):
    with session_scope(f.case.factory) as session:
        volume = session.get(
            CollectionArchiveObjectUploadRecord, (f.case.collection_id, f.case.object_id)
        )
        return volume.checkpoint_json, volume.sealed_receipt_json, volume.uploaded_units


def _preparation_retry(tmp_path, monkeypatch, kind, mode, committed, *, database_url=None):
    with _service_actor(tmp_path, monkeypatch, kind, mode, database_url=database_url) as f:
        calls = []

        def create():
            owner = f.create()
            original = owner._admission.prepare

            def prepare(*args, **kwargs):
                calls.append(
                    hashlib.sha256(
                        json.dumps(kwargs["preparation"], sort_keys=True).encode()
                    ).hexdigest()
                )
                assert owner._candidate is not None and owner._writes is not None
                if len(calls) == 1:
                    if committed:
                        original(*args, **kwargs)
                    raise OSError("injected original preparation acknowledgement loss")
                return original(*args, **kwargs)

            monkeypatch.setattr(owner._admission, "prepare", prepare)
            return owner

        monkeypatch.setattr(
            f.service,
            "_schedule_finalization_if_ready",
            lambda *args: pytest.fail("payload execution cannot activate an unowned finalizer"),
        )
        assert f.submit(create) == "submitted"
        (first,) = _receive(f.registry, f.execution)
        with pytest.raises(OSError, match="preparation acknowledgement"):
            first.result()
        owner = f.originals[0]
        candidate = owner._candidate
        uploader = owner._pack or owner._raw
        assert candidate is not None and uploader is not None and not f.seen
        assert owner.has_pending and not f.registry.retire_idle(first)
        monkeypatch.setattr(
            uploader,
            "prepare",
            lambda **kwargs: pytest.fail("recreated original encryption preparation"),
        )
        incoming = _payload(
            f.service, f.key, f.target, f.case.object_id, f.case.plan_sha256, f.case.content
        )
        assert incoming.request_sha256 == f.payload.request_sha256
        assert (
            f.submit(lambda: pytest.fail("reconstructed original payload unit"), incoming=incoming)
            == "submitted"
        )
        assert incoming.released and not f.payload.released

        (second,) = _receive(f.registry, f.execution)
        assert second.result()["state"] == "committed"
        assert (
            owner._candidate is candidate
            and owner._preparation.to_json_bytes() == candidate.to_json_bytes()
        )
        assert len(f.originals) == 1 and len(set(calls)) == 1
        assert len(set(f.preparations)) == 1
        assert [v.operation.kind for v in f.seen] == ["lookup", "begin", "part", "part", "complete"]
        assert f.payload.released and not f.service._archive_inputs._inputs
        assert _stored(f)[1] is not None and not owner.has_pending
        assert f.registry.retire_idle(second)


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("committed", (False, True))
def test_actual_service_payload_retains_candidate_before_preparation_acknowledgement(
    tmp_path, monkeypatch, kind, mode, committed
):
    _preparation_retry(tmp_path, monkeypatch, kind, mode, committed)


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("phase", ("begin", "unit", "complete", "receipt"))
@pytest.mark.parametrize("committed", (False, True))
@pytest.mark.parametrize("terminal", (False, True))
def test_actual_service_payload_resumes_each_original_checkpoint_and_receipt(
    tmp_path, monkeypatch, kind, mode, phase, committed, terminal
):
    with _service_actor(tmp_path, monkeypatch, kind, mode) as f:
        reports = []
        selected = {"begin": 2, "unit": 3, "complete": 4}.get(phase)

        def create():
            owner = f.create()
            original = owner._admission.apply_reports

            def apply(values, *, apply_result):
                reports.append(values)
                if len(reports) == selected:
                    if committed:
                        original(values, apply_result=apply_result)
                    raise OSError("injected original checkpoint acknowledgement loss")
                return original(values, apply_result=apply_result)

            monkeypatch.setattr(owner._admission, "apply_reports", apply)
            return owner

        if phase == "receipt":
            save = f.service._record_sealed_volume
            saved = []

            def record(collection_id, receipt):
                saved.append(receipt)
                if len(saved) == 1:
                    if committed:
                        save(collection_id, receipt)
                    raise OSError("injected original checkpoint acknowledgement loss")
                return save(collection_id, receipt)

            monkeypatch.setattr(f.service, "_record_sealed_volume", record)

        assert f.submit(create) == "submitted"
        (first,) = _receive(f.registry, f.execution)
        with pytest.raises(OSError, match="checkpoint acknowledgement"):
            first.result()
        owner = f.originals[0]
        original_open, original_unit = owner._opening, owner._uploading
        before = list(f.seen)
        if terminal:
            operation = _select_terminal(f)
            from tests.unit.test_archive_upload_drain import _inspect

            before_receiving = _inspect(f.case, operation)
            expected_revision = str(int(phase == "receipt" or phase == "complete" and committed))
            assert before_receiving.custody_revision == expected_revision
            assert before_receiving.selected_custody_revision == expected_revision
            assert before_receiving.payload_sealed is (
                phase == "receipt" or phase == "complete" and committed
            )
            assert (
                f.registry.submit_terminal(
                    f.key, target_incarnation=f.target, operation_id=operation
                )
                == "submitted"
            )
            (received,) = _receive(f.registry, f.execution)
            assert received.result() == "closed" and received.execution.terminal
            assert f.seen == before
            checkpoint, sealed, _ = _stored(f)
            assert checkpoint is not None
            assert (sealed is not None) is (phase in {"complete", "receipt"})
            after_receiving = _inspect(f.case, operation)
            assert after_receiving.payload_sealed is (phase in {"complete", "receipt"})
            assert after_receiving.custody_revision == str(int(phase in {"complete", "receipt"}))
            assert after_receiving.selected_custody_revision == expected_revision
            assert owner._input.released and not owner.has_pending
            with session_scope(f.case.factory) as session:
                assert session.get(ArchiveUploadTerminalRecord, operation).phase == "draining"
            return
        if phase in {"complete", "receipt"}:
            monkeypatch.setattr(
                f.service._archive_stores,
                "require_incarnation",
                lambda *args: pytest.fail("original receiving checked fresh provider health"),
            )
        assert f.submit(lambda: pytest.fail("reconstructed original payload unit")) == "submitted"
        (second,) = _receive(f.registry, f.execution)
        assert second.result()["state"] == "committed"
        assert f.seen[: len(before)] == before
        assert owner._opening is original_open
        if original_unit is not None:
            assert owner._uploading is original_unit
        assert [v.operation.kind for v in f.seen] == ["lookup", "begin", "part", "part", "complete"]
        assert len({v.call.call_id for v in f.seen}) == len(f.seen)
        assert _stored(f)[1] is not None
        with session_scope(f.case.factory) as session:
            assert (
                session.get(ArchiveWriteControlRecord, f.key.collection_id).custody_revision == "1"
            )
        assert f.registry.retire_idle(second)


def _independent_unit(f, kind):
    from riverhog_protocol import CollectionUploadRawDigestBatchDocument
    from riverhog_protocol.raw_ingress import ordered_raw_part_commitment

    content = b"independent content"
    digest = hashlib.sha256(content).hexdigest()
    opened = f.service.create_or_resume(
        idempotency_key="independent payload constructor",
        ingest_source="fixture",
        archive_store=None,
        use_cache=False,
        initiator=_CREATOR,
        event_context=None,
    )
    collection_id = int(opened["collection_id"])
    member = {"artifact_id": "3" * 64, "bytes": str(len(content)), "sha256": digest}
    if kind == "segment":
        count, commitment = ordered_raw_part_commitment((digest,))
        member["raw_parts"] = {
            "part_plaintext_bytes": str(CHUNK_SIZE),
            "part_count": str(count),
            "ordered_sha256": commitment,
        }
    members = [member]
    if kind == "pack":
        members.append(
            {"artifact_id": "4" * 64, "bytes": "0", "sha256": hashlib.sha256(b"").hexdigest()}
        )
    f.service.register_artifacts(collection_id, members)
    if kind == "segment":
        f.service.register_raw_part_digests(
            collection_id,
            CollectionUploadRawDigestBatchDocument(
                artifact_id="3" * 64,
                first_part="0",
                sha256s=[digest],
            ),
        )
    volume = f.service.list_volumes(collection_id)["volumes"][0]
    assert volume["kind"] == kind
    f.admission.bind_epoch(
        collection_id, guard=f.service._payload_write_guard(collection_id, _CREATOR)
    )
    key, target = _scope(SimpleNamespace(factory=f.case.factory, collection_id=collection_id))
    payload = _payload(f.service, key, target, volume["volume_id"], volume["plan_sha256"], content)
    request = payload.request_sha256
    assert (
        f.registry.submit(
            key,
            target_incarnation=target,
            request_sha256=request,
            payload=payload,
            create=lambda: f.service._payload_unit_invocation(
                key=key,
                target_incarnation=target,
                volume_id=volume["volume_id"],
                unit=0,
                plan_sha256=volume["plan_sha256"],
                payload=payload,
                principal=_CREATOR,
            ),
            advance=lambda owner: owner.advance(),
        )
        == "submitted"
    )
    worker = f.execution._slots[collection_id].worker
    worker.join(timeout=10)
    assert not worker.is_alive()
    (received,) = f.registry.collect(limit=2)
    assert received.request_sha256 == request
    assert received.result()["state"] == "committed"
    assert f.registry.retire_idle(received)
    assert f.execution.status(f.key) == "running"


def _held_terminal(
    tmp_path, monkeypatch, kind, mode, phase, *, database_url=None, independent=False
):
    with _service_actor(tmp_path, monkeypatch, kind, mode, database_url=database_url) as f:
        entered, release = threading.Event(), threading.Event()
        execute = f.executor.execute_write
        factory_name = "_pack_uploader" if kind == "pack" else "_raw_uploader"
        uploader_factory = getattr(f.service, factory_name)

        def hold_key(*args, **kwargs):
            if args[0]._executor._writes.scope[0] == f.case.collection_id:
                assert f.registry._invocations[f.request].owner is f.originals[0]
                assert f.originals[0]._archive is not None
                entered.set()
                assert release.wait(10)
            return uploader_factory(*args, **kwargs)

        def hold(invocation, **kwargs):
            if invocation.operation.kind == phase:
                entered.set()
                assert release.wait(10)
            return execute(invocation, **kwargs)

        if phase == "key":
            monkeypatch.setattr(f.service, factory_name, hold_key)
        else:
            monkeypatch.setattr(f.executor, "execute_write", hold)
        try:
            assert f.submit() == "submitted"
            assert entered.wait(5)
            if independent:
                _independent_unit(f, kind)
            owner = f.originals[0]
            assert owner.has_pending and not f.payload.released
            operation = _select_terminal(f)
            assert f.registry.collect(limit=1) == ()
            assert (
                f.registry.submit_terminal(
                    f.key, target_incarnation=f.target, operation_id=operation
                )
                == "busy"
            )
            with pytest.raises(Conflict, match="fenced"):
                f.submit()
            release.set()
            (forward,) = _receive(f.registry, f.execution)
            if phase == "complete":
                assert forward.result()["state"] == "committed"
                assert _stored(f)[1] is not None
            else:
                with pytest.raises(Conflict):
                    forward.result()
            before = list(f.seen)
            assert (
                f.registry.submit_terminal(
                    f.key, target_incarnation=f.target, operation_id=operation
                )
                == "submitted"
            )
            (terminal,) = _receive(f.registry, f.execution)
            assert terminal.execution.terminal and terminal.result() == "closed"
            assert f.seen == before and owner._input.released
            with pytest.raises(Conflict, match="fenced"):
                owner.advance()
            with session_scope(f.case.factory) as session:
                assert session.get(ArchiveUploadTerminalRecord, operation).phase == "draining"
                assert session.get(CollectionUploadRecord, f.case.collection_id) is not None
                assert (
                    session.get(
                        CollectionArchiveObjectUploadRecord,
                        (f.case.collection_id, f.case.object_id),
                    )
                    is not None
                )
            if phase == "part":
                checkpoint, sealed, uploaded = _stored(f)
                assert checkpoint is not None and sealed is None and uploaded == 0
                assert [v.operation.kind for v in f.seen] == ["lookup", "begin", "part"]
        finally:
            release.set()
            f.registry.close()


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("phase", ("key", "begin", "part", "complete"))
def test_actual_service_unit_remains_owned_through_fence_original_reply_and_receiving(
    tmp_path, monkeypatch, kind, mode, phase
):
    _held_terminal(tmp_path, monkeypatch, kind, mode, phase)


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("unknown", (False, True))
def test_actual_service_unit_keeps_the_selected_original_cache_receiver(
    tmp_path, monkeypatch, kind, mode, unknown
):
    from riverhog_storage_adapter_protocol import unknown_write_execution

    from tests.unit.test_mirrored_archive_upload_writes import _writing

    with _writing(tmp_path, monkeypatch, kind, mode=mode) as joint:
        actor = joint.actor
        service = actor.case.service
        with session_scope(actor.case.factory) as session:
            upload = session.get(CollectionUploadRecord, actor.case.collection_id)
            name = upload.archive_store
        binding = service._archive_stores.require(name)
        service._archive_stores = ArchiveStoreRegistry(
            {
                name: replace(
                    binding, resumable_objects=actor.provider, resumable_writes=actor.executor
                )
            }
        )
        cache_calls = []
        execute = joint.candidate.execute_write

        def cache(invocation, **kwargs):
            response = execute(invocation, **kwargs)
            cache_calls.append(invocation)
            if unknown and invocation.operation.kind == "part":
                return unknown_write_execution(invocation)
            return response

        monkeypatch.setattr(joint.candidate, "execute_write", cache)
        key, target = _scope(actor.case)
        built = []

        def mirror(archive, guard):
            writer = _service_cache_writer(joint, archive, guard)
            built.append(writer)
            return writer

        payload = _payload(
            service, key, target, actor.case.object_id, actor.case.plan_sha256, actor.case.content
        )
        owner = service._payload_unit_invocation(
            key=key,
            target_incarnation=target,
            volume_id=actor.case.object_id,
            unit=0,
            plan_sha256=actor.case.plan_sha256,
            payload=payload,
            principal=_CREATOR,
            mirror=service._payload_cache_family(
                payload,
                create=mirror,
                grants=joint.writer._grants,
                validate_population=joint.writer._validate_population,
            ),
        )
        registry, execution = service._archive_invocations, service._archive_execution
        request = payload.request_sha256
        assert (
            registry.submit(
                key,
                target_incarnation=target,
                request_sha256=request,
                payload=payload,
                create=lambda: owner,
                advance=lambda original: original.advance(),
            )
            == "submitted"
        )
        (received,) = _receive(registry, execution)
        assert received.result()["state"] == "committed"
        assert len(built) == 1
        begin = built[0]._begin.cache_begin_call_id
        assert begin is not None
        assert payload.released and service._archive_inputs._bytes == 0
        assert owner.has_pending and not registry.retire_idle(received)
        before = list(actor.seen), list(cache_calls)
        assert (
            registry.submit(
                key,
                target_incarnation=target,
                request_sha256=request,
                create=lambda: pytest.fail("reconstructed optional cache owner"),
                advance=lambda original: pytest.fail("restarted completed payload unit"),
            )
            == "retained"
        )
        assert (actor.seen, cache_calls) == before
        assert built[0]._begin.cache_begin_call_id == begin
        if unknown:
            assert not service._archive_cache_families.retire(payload)
        else:
            assert service._archive_cache_families.retire(payload)
            assert not owner.has_pending and registry.retire_idle(received)


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("mode", ("direct", "owned-http"))
def test_independent_actual_payload_progresses_while_another_key_constructor_is_held(
    tmp_path, monkeypatch, kind, mode
):
    _held_terminal(tmp_path, monkeypatch, kind, mode, "key", independent=True)


@pytest.mark.parametrize("outcome", ("raises", "missing", "foreign", "refused"))
def test_uncertain_cache_constructor_keeps_original_archive_and_retention_charge(
    tmp_path, monkeypatch, outcome
):
    from tests.unit.test_mirrored_archive_upload_writes import _writing

    with _writing(tmp_path, monkeypatch, "segment") as joint:
        actor = joint.actor
        service = actor.case.service
        with session_scope(actor.case.factory) as session:
            name = session.get(CollectionUploadRecord, actor.case.collection_id).archive_store
        service._archive_stores = ArchiveStoreRegistry(
            {
                name: replace(
                    service._archive_stores.require(name),
                    resumable_objects=actor.provider,
                    resumable_writes=actor.executor,
                )
            }
        )
        key, target = _scope(actor.case)
        entered = []

        def mirror(archive, guard):
            entered.append(archive)
            if outcome == "raises":
                raise OSError("injected original cache constructor acknowledgement loss")
            if outcome == "refused":
                from riverhog_core.services.archive_cache_families import CacheFamilyNotAdmitted

                raise CacheFamilyNotAdmitted("constructor entered before refusal")
            return None if outcome == "missing" else joint.writer

        payload = _payload(
            service, key, target, actor.case.object_id, actor.case.plan_sha256, actor.case.content
        )
        owner = service._payload_unit_invocation(
            key=key,
            target_incarnation=target,
            volume_id=actor.case.object_id,
            unit=0,
            plan_sha256=actor.case.plan_sha256,
            payload=payload,
            principal=_CREATOR,
            mirror=service._payload_cache_family(
                payload,
                create=mirror,
                grants=joint.writer._grants,
                validate_population=joint.writer._validate_population,
            ),
        )
        registry, execution = service._archive_invocations, service._archive_execution
        request = payload.request_sha256

        def submit():
            return registry.submit(
                key,
                target_incarnation=target,
                request_sha256=request,
                payload=payload,
                create=lambda: owner,
                advance=lambda original: original.advance(),
            )

        assert submit() == "submitted"
        (first,) = _receive(registry, execution)
        with pytest.raises((OSError, Conflict)):
            first.result()
        assert owner.has_pending and owner._archive is entered[0]
        assert not registry.retire_idle(first) and not actor.seen
        assert submit() == "submitted"
        (second,) = _receive(registry, execution)
        with pytest.raises(Conflict, match="constructor outcome is unresolved"):
            second.result()
        assert len(entered) == 1 and not actor.seen
        operation = actor.admission.select_terminal(
            actor.case.collection_id,
            kind="cancel",
            request={"fixture": request},
            principal_id=_CREATOR.id,
            key_id=_CREATOR.key_id,
            grants=service._config.archive_write,
            guard=service._payload_write_guard(actor.case.collection_id, _CREATOR),
            validate_selection=lambda session, upload: None,
        )
        assert (
            registry.submit_terminal(
                key,
                target_incarnation=target,
                operation_id=operation,
            )
            == "submitted"
        )
        (received,) = _receive(registry, execution)
        assert received.result() == "pending"
        assert owner.has_pending and registry._invocations[request].owner is owner
        assert not actor.seen


def _service_cache_writer(joint, archive, guard):
    from riverhog_core.services.mirrored_archive_begin import MirroredArchiveBegin
    from riverhog_core.services.mirrored_archive_upload_writes import MirroredArchiveUploadWrites

    actor, original = joint.actor, joint.mirror
    with session_scope(actor.case.factory) as session:
        name = session.get(CollectionUploadRecord, actor.case.collection_id).archive_store
    begin = MirroredArchiveBegin(
        actor.case.factory,
        archive=archive,
        cache=original._cache,
        candidate=joint.candidate,
        registration=original._registration,
        owner=original._owner,
        principal=_CREATOR,
        grants=original._grants,
        guard=guard,
        validate_population=original._validate_population,
    )

    def validate(session, upload, object_id, expected_bytes):
        volume = session.get(
            CollectionArchiveObjectUploadRecord,
            (actor.case.collection_id, object_id),
            with_for_update=True,
        )
        assert volume is not None and upload.collection_id == actor.case.collection_id
        assert begin.cache_session is not None
        assert expected_bytes == begin.cache_session.expected_bytes

    return MirroredArchiveUploadWrites(
        archive=archive,
        begin=begin,
        cache=joint.writes,
        candidate=joint.candidate,
        source_store=name,
        owner=original._owner,
        principal=_CREATOR,
        grants=joint.writer._grants,
        guard=guard,
        validate_population=validate,
    )
