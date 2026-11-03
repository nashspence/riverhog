from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_age import encrypt_age_scrypt
from riverhog_core.archive_manifest import build_collection_archive_terminal_document
from riverhog_core.archive_root import ArchiveRootPublisher
from riverhog_core.catalog_archive_write_models import (
    ArchiveWriteControlRecord,
    ArchiveWritePreparationRecord,
)
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord, StorageIncarnationRecord
from riverhog_core.services import immutable_archive_preparation as preparation_module
from riverhog_core.services.immutable_archive_preparation import ImmutableArchivePreparation
from riverhog_core.stores.storage_adapter_archive_objects import (
    StorageAdapterImmutableArchiveObjectStore,
)
from riverhog_protocol.errors import Conflict, ServiceUnavailable
from riverhog_storage_adapter_protocol import PutObjectOperation, SmallObjectWriteRequest
from sqlalchemy import event

from tests.unit.test_archive_upload_writes import _actor
from tests.unit.test_archive_write_execution import _executor
from tests.unit.test_collection_uploads import _CREATOR


@contextmanager
def _metadata(tmp_path, monkeypatch, *, database_url=None):
    with _actor(tmp_path, monkeypatch, "pack", database_url=database_url) as actor:
        with session_scope(actor.case.factory) as session:
            upload = session.get(CollectionUploadRecord, actor.case.collection_id)
            prefix, generation = upload.archive_storage_prefix, upload.archive_generation
            source_store = upload.archive_store
        plaintext = b"original immutable archive authority"
        content = encrypt_age_scrypt(plaintext, "fixture", log_n=1)
        operation = PutObjectOperation(
            request=SmallObjectWriteRequest(
                object_path=prefix + "/metadata/fixture.age",
                content_type="application/vnd.riverhog-fixture.age",
                required_identity_assertions={
                    "riverhog-plaintext-bytes": str(len(plaintext)),
                    "riverhog-plaintext-sha256": hashlib.sha256(plaintext).hexdigest(),
                },
                placement_policy="immediate_default",
                mode="create_only",
                stored_bytes=len(content),
                stored_sha256=hashlib.sha256(content).hexdigest(),
            )
        )

        def validate(session, upload, original):
            assert upload.collection_id == actor.case.collection_id
            assert original.request.object_path.startswith(prefix + "/metadata/")
            assert "riverhog-plaintext-sha256" in original.request.required_identity_assertions

        yield SimpleNamespace(
            actor=actor,
            owner=ImmutableArchivePreparation(actor.case.factory),
            operation=operation,
            content=content,
            plaintext=plaintext,
            prefix=prefix,
            generation=generation,
            validate=validate,
            source_store=source_store,
            guard=actor.case.service._payload_write_guard(actor.case.collection_id, _CREATOR),
            grants=actor.case.service._config.archive_write,
            epoch=actor.journal.scope[2],
        )


def _prepare(case, *, operation=None, content=None, **changes):
    arguments = dict(
        operation=operation or case.operation,
        content=case.content if content is None else content,
        epoch=case.epoch,
        grants=case.grants,
        guard=case.guard,
        validate_metadata=case.validate,
    )
    arguments.update(changes)
    return case.owner.prepare(case.actor.case.collection_id, **arguments)


def _stored(case):
    with session_scope(case.actor.case.factory) as session:
        record = session.get(
            ArchiveWritePreparationRecord,
            (
                case.actor.case.collection_id,
                case.owner.object_id(case.operation.request.object_path),
            ),
        )
        return None if record is None else (record.identity_json, record.preparation_json)


def _replacement(case):
    content = encrypt_age_scrypt(case.plaintext, "fixture", log_n=1)
    request = case.operation.request.model_copy(
        update={
            "stored_bytes": len(content),
            "stored_sha256": hashlib.sha256(content).hexdigest(),
        }
    )
    return case.operation.model_copy(update={"request": request}), content


def _assert_publisher(tmp_path, monkeypatch, mode, *, database_url=None):
    with (
        _metadata(tmp_path, monkeypatch, database_url=database_url) as case,
        _executor(case.actor.adapter, mode) as transport,
    ):
        physical = StorageAdapterImmutableArchiveObjectStore(transport)
        incoming, sent = [], []

        class PreparedStore:
            def put_immutable_object(
                self,
                *,
                object_path,
                content,
                content_type,
                required_identity_assertions,
                placement_policy,
            ):
                incoming.append(content)
                operation = PutObjectOperation(
                    request=SmallObjectWriteRequest(
                        object_path=object_path,
                        content_type=content_type,
                        required_identity_assertions=required_identity_assertions,
                        placement_policy=placement_policy,
                        mode="create_only",
                        stored_bytes=len(content),
                        stored_sha256=hashlib.sha256(content).hexdigest(),
                    )
                )
                prepared = _prepare(case, operation=operation, content=content)
                original = prepared.operation.request
                body = prepared.content
                with session_scope(case.actor.case.factory) as session:
                    assert (
                        session.get(
                            ArchiveWritePreparationRecord,
                            (prepared.collection_id, prepared.object_id),
                        ).preparation_json
                        == prepared.preparation_json
                    )
                sent.append(body)
                return physical.put_immutable_object(
                    object_path=original.object_path,
                    content=body,
                    content_type=original.content_type,
                    required_identity_assertions=original.required_identity_assertions,
                    placement_policy=original.placement_policy,
                )

        publisher = ArchiveRootPublisher(
            object_store=PreparedStore(), passphrase="fixture", scrypt_log_n=1
        )
        document = build_collection_archive_terminal_document(
            archive_generation=case.generation, artifact_set_sha256="a" * 64, sequence=1
        )
        first = publisher.publish_terminal_metadata(
            archive_storage_prefix=case.prefix, document=document
        )
        second = publisher.publish_terminal_metadata(
            archive_storage_prefix=case.prefix, document=document
        )
        assert first == second and incoming[0] != incoming[1]
        assert sent == [incoming[0], incoming[0]]
        cold = ImmutableArchivePreparation(case.actor.case.factory)
        retained = cold.load(case.actor.case.collection_id, first.object_path, epoch=case.epoch)
        assert retained is not None and retained.content == incoming[0]
        assert retained.operation.request.stored_sha256 == first.stored_sha256


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
def test_real_encrypted_metadata_publisher_reuses_original_prepared_ciphertext(
    tmp_path, monkeypatch, mode
):
    _assert_publisher(tmp_path, monkeypatch, mode)


@pytest.mark.parametrize("committed", (False, True))
def test_preparation_commit_and_reply_loss_preserve_original_bytes_before_any_dispatch(
    tmp_path, monkeypatch, committed
):
    with _metadata(tmp_path, monkeypatch) as case:
        transaction = preparation_module.session_scope
        prepare, attempts = case.owner.prepare, []

        @contextmanager
        def rollback(factory):
            with transaction(factory) as session:
                yield session
                if not committed and len(attempts) == 1:
                    raise OSError("injected metadata preparation reply loss")

        def lost(*args, **kwargs):
            attempts.append(kwargs["content"])
            result = prepare(*args, **kwargs)
            if committed and len(attempts) == 1:
                raise OSError("injected metadata preparation reply loss")
            return result

        monkeypatch.setattr(preparation_module, "session_scope", rollback)
        monkeypatch.setattr(case.owner, "prepare", lost)
        with pytest.raises(OSError, match="reply loss"):
            _prepare(case)
        held = _stored(case)
        assert (held is not None) is committed
        operation, replacement = _replacement(case)
        prepared = _prepare(case, operation=operation, content=replacement)
        assert prepared.content == (case.content if committed else replacement)
        assert _stored(case)[1] == prepared.preparation_json
        assert case.actor.seen == []


@pytest.mark.parametrize("changed", ("plaintext", "type", "placement", "mode"))
def test_preparation_rejects_changed_immutable_identity_without_replacing_original(
    tmp_path, monkeypatch, changed
):
    with _metadata(tmp_path, monkeypatch) as case:
        original = _prepare(case)
        request = case.operation.request
        if changed == "plaintext":
            request = request.model_copy(
                update={
                    "required_identity_assertions": {
                        **request.required_identity_assertions,
                        "riverhog-plaintext-sha256": "f" * 64,
                    }
                }
            )
        elif changed == "type":
            request = request.model_copy(update={"content_type": "application/octet-stream"})
        elif changed == "placement":
            request = request.model_copy(update={"placement_policy": "archive_default"})
        else:
            request = request.model_copy(
                update={"mode": "replace_current", "expected_current_stored_sha256": "e" * 64}
            )
        before = _stored(case)
        with pytest.raises(Conflict):
            _prepare(case, operation=case.operation.model_copy(update={"request": request}))
        assert _stored(case) == before and original.content == case.content


def test_metadata_without_complete_plaintext_identity_keeps_exact_stored_identity(
    tmp_path, monkeypatch
):
    with _metadata(tmp_path, monkeypatch) as case:
        request = case.operation.request.model_copy(update={"required_identity_assertions": {}})
        operation = case.operation.model_copy(update={"request": request})

        def validate(session, upload, original):
            return None

        _prepare(case, operation=operation, validate_metadata=validate)
        replacement_operation, replacement = _replacement(case)
        changed = replacement_operation.model_copy(
            update={
                "request": replacement_operation.request.model_copy(
                    update={"required_identity_assertions": {}}
                )
            }
        )
        before = _stored(case)
        with pytest.raises(Conflict, match="metadata identity"):
            _prepare(case, operation=changed, content=replacement, validate_metadata=validate)
        assert _stored(case) == before


@pytest.mark.parametrize("changed", ("grant", "ciphertext", "guard", "target", "epoch", "path"))
def test_refused_preparation_does_not_create_original_metadata_or_call_a_provider(
    tmp_path, monkeypatch, changed
):
    with _metadata(tmp_path, monkeypatch) as case:
        options = {}
        if changed == "grant":
            options["grants"] = replace(case.grants, preparation_bytes=1)
        elif changed == "ciphertext":
            options["content"] = case.content + b"changed"
        elif changed == "guard":

            def refused(session):
                raise Conflict("injected current metadata owner refusal")

            options["guard"] = refused
        elif changed == "target":
            with session_scope(case.actor.case.factory) as session:
                session.get(
                    StorageIncarnationRecord, ("archive", case.source_store)
                ).state = "disabled"
        elif changed == "epoch":
            options["epoch"] = "e" * 64
        else:
            options["operation"] = case.operation.model_copy(
                update={
                    "request": case.operation.request.model_copy(
                        update={"object_path": "outside/metadata.age"}
                    )
                }
            )
        with pytest.raises((Conflict, ServiceUnavailable)):
            _prepare(case, **options)
        assert _stored(case) is None and case.actor.seen == []


def test_original_preparation_survives_parent_retirement_without_new_dispatch_authority(
    tmp_path, monkeypatch
):
    with _metadata(tmp_path, monkeypatch) as case:
        prepared = _prepare(case)
        case.actor.admission.select_terminal(
            case.actor.case.collection_id,
            kind="cancel",
            request={"fixture": "metadata"},
            principal_id=_CREATOR.id,
            grants=case.grants,
            guard=case.guard,
            validate_selection=lambda *args: None,
        )
        with pytest.raises(Conflict, match="admission is closed"):
            _prepare(case)
        with session_scope(case.actor.case.factory) as session:
            selected_epoch = session.get(
                ArchiveWriteControlRecord, case.actor.case.collection_id
            ).epoch
            assert selected_epoch != case.epoch
        with pytest.raises(Conflict, match="original preparation epoch"):
            case.owner.load(
                prepared.collection_id, prepared.operation.request.object_path, epoch=selected_epoch
            )
        with session_scope(case.actor.case.factory) as session:
            session.delete(session.get(CollectionUploadRecord, case.actor.case.collection_id))
        retained = ImmutableArchivePreparation(case.actor.case.factory).load(
            prepared.collection_id, prepared.operation.request.object_path, epoch=case.epoch
        )
        assert retained == prepared and retained.content == case.content
        assert case.actor.seen == []


@pytest.mark.parametrize("count", (1, 8, 32))
def test_many_metadata_preparations_use_fixed_per_object_grants_and_point_reads(
    tmp_path, monkeypatch, count
):
    with _metadata(tmp_path, monkeypatch) as case:
        factory = case.actor.case.factory
        engine = factory.kw["bind"]
        reads = []

        def observe(connection, cursor, statement, parameters, context, executemany):
            if "FROM archive_write_preparations" in statement:
                reads.append(statement)

        event.listen(engine, "before_cursor_execute", observe)
        try:
            for number in range(count):
                operation = case.operation.model_copy(
                    update={
                        "request": case.operation.request.model_copy(
                            update={
                                "object_path": case.prefix + "/metadata/" + str(number) + ".age"
                            }
                        )
                    }
                )
                first = _prepare(case, operation=operation)
                assert _prepare(case, operation=operation) == first
        finally:
            event.remove(engine, "before_cursor_execute", observe)
        assert len(reads) == 2 * count
        assert all(
            "archive_write_preparations.collection_id =" in query
            and "archive_write_preparations.object_id =" in query
            for query in reads
        )


def test_metadata_preparation_snapshots_mutable_caller_and_validator_inputs(tmp_path, monkeypatch):
    with _metadata(tmp_path, monkeypatch) as case:
        original = dict(case.operation.request.required_identity_assertions)

        def guard(session):
            case.guard(session)
            case.operation.request.required_identity_assertions["riverhog-plaintext-sha256"] = (
                "e" * 64
            )

        def validate(session, upload, operation):
            case.validate(session, upload, operation)
            operation.request.required_identity_assertions["riverhog-plaintext-sha256"] = "f" * 64

        prepared = _prepare(case, guard=guard, validate_metadata=validate)
        assert prepared.operation.request.required_identity_assertions == original
        assert prepared.content == case.content


def test_replay_retains_original_grant_but_requires_current_owner_permission(tmp_path, monkeypatch):
    with _metadata(tmp_path, monkeypatch) as case:
        original = _prepare(case)
        assert _prepare(case, grants=replace(case.grants, preparation_bytes=1)) == original
        before = _stored(case)

        def revoked(session):
            raise Conflict("injected revoked metadata owner")

        with pytest.raises(Conflict, match="revoked metadata owner"):
            _prepare(case, guard=revoked)
        assert _stored(case) == before


def test_cold_preparation_refuses_changed_opaque_bytes_without_repair(tmp_path, monkeypatch):
    with _metadata(tmp_path, monkeypatch) as case:
        original = _prepare(case)
        with session_scope(case.actor.case.factory) as session:
            record = session.get(
                ArchiveWritePreparationRecord, (original.collection_id, original.object_id)
            )
            value = json.loads(record.preparation_json)
            value["content_base64"] = "Y2hhbmdlZA=="
            record.preparation_json = json.dumps(value)
        before = _stored(case)
        with pytest.raises(Conflict, match="ciphertext commitment"):
            case.owner.load(
                original.collection_id, case.operation.request.object_path, epoch=case.epoch
            )
        assert _stored(case) == before and case.actor.seen == []
