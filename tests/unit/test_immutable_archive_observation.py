from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import replace

import pytest
from riverhog_core.archive_manifest import build_collection_archive_terminal_document
from riverhog_core.archive_root import ArchiveRootPublisher
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord
from riverhog_core.ports.archive_objects import (
    ArchiveObjectIdentityConflict,
    ImmutableObjectReceipt,
)
from riverhog_core.services import immutable_archive_admission as admission_module
from riverhog_core.services.immutable_archive_admission import ImmutableArchiveAdmission
from riverhog_core.stores.storage_adapter_archive_objects import (
    StorageAdapterImmutableArchiveObjectStore,
)
from riverhog_protocol.errors import Conflict
from riverhog_storage_adapter_protocol import PutObjectOperation, SmallObjectWriteRequest

from tests.unit.test_archive_write_execution import _executor
from tests.unit.test_immutable_archive_admission import (
    _arm,
    _checkpoint,
    _effect,
    _select,
    _verify_checkpoint,
)
from tests.unit.test_immutable_archive_preparation import _metadata, _prepare, _replacement


def _seed(case, physical):
    operation, content = _replacement(case)
    request = operation.request
    receipt = physical.put_immutable_object(
        object_path=request.object_path,
        content=content,
        content_type=request.content_type,
        required_identity_assertions=request.required_identity_assertions,
        placement_policy=request.placement_policy,
    )
    return receipt


def _assert_observed_existing(tmp_path, monkeypatch, mode, *, database_url=None):
    with (
        _metadata(tmp_path, monkeypatch, database_url=database_url) as case,
        _executor(case.actor.adapter, mode) as transport,
    ):
        owner = ImmutableArchiveAdmission(case.actor.case.factory)
        physical = StorageAdapterImmutableArchiveObjectStore(transport)
        seeded = _seed(case, physical)
        prepared = _prepare(case)
        assert prepared.operation.request.stored_sha256 != seeded.stored_sha256
        admitted = _arm(case, owner, prepared)
        observed = physical.observe_immutable_object(prepared.operation.request)
        assert observed is not None and observed.stored_sha256 == seeded.stored_sha256
        assert _effect(case, admitted).state == "armed" and _effect(case, admitted).unresolved
        # The terminal still sees the original unused invocation while its
        # positive object observation has not been received.
        _select(case)
        proof = owner.withdraw(admitted)
        owner.accept_existing(admitted, proof, observed, apply_receipt=_checkpoint)
        owner.accept_existing(
            admitted, proof, observed, apply_receipt=lambda *args: pytest.fail("duplicate apply")
        )
        effect = _effect(case, admitted)
        encoded = json.loads(effect.result)
        assert encoded["dispatch"] == "never-started" and "call" not in encoded
        assert encoded["observed_object"]["stored_sha256"] == seeded.stored_sha256
        assert not effect.unresolved and effect.observation is None
        owner.acknowledge_report(admitted, verify_receipt=_verify_checkpoint)
        assert _effect(case, admitted) is None
        with pytest.raises(Conflict, match="used live actor"):
            owner.execute(admitted, physical)


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_existing_randomized_ciphertext_is_object_authority_and_unused_dispatch(
    tmp_path, monkeypatch, mode
):
    _assert_observed_existing(tmp_path, monkeypatch, mode)


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_actual_encrypted_publisher_replays_preexisting_cipher_without_write_closure(
    tmp_path, monkeypatch, mode
):
    with (
        _metadata(tmp_path, monkeypatch) as case,
        _executor(case.actor.adapter, mode) as transport,
    ):
        physical = StorageAdapterImmutableArchiveObjectStore(transport)
        owner = ImmutableArchiveAdmission(case.actor.case.factory)
        document = build_collection_archive_terminal_document(
            archive_generation=case.generation, artifact_set_sha256="a" * 64, sequence=1
        )
        seeded = ArchiveRootPublisher(
            object_store=physical, passphrase="fixture", scrypt_log_n=1
        ).publish_terminal_metadata(archive_storage_prefix=case.prefix, document=document)
        outcomes = []

        class ObservedStore:
            def put_immutable_object(
                self,
                *,
                object_path,
                content,
                content_type,
                required_identity_assertions,
                placement_policy,
            ):
                import hashlib

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
                prepared = _prepare(
                    case,
                    operation=operation,
                    content=content,
                    validate_metadata=lambda session, upload, original: None,
                )
                admitted = _arm(
                    case,
                    owner,
                    prepared,
                    validate_metadata=lambda session, upload, original: None,
                )
                observed = physical.observe_immutable_object(prepared.operation.request)
                assert observed is not None
                assert observed.stored_sha256 != prepared.operation.request.stored_sha256
                proof = owner.withdraw(admitted)
                owner.accept_existing(admitted, proof, observed, apply_receipt=_checkpoint)
                outcomes.append(_effect(case, admitted).result)
                owner.acknowledge_report(admitted, verify_receipt=_verify_checkpoint)
                return ImmutableObjectReceipt(
                    object_path=observed.object_path,
                    revision=observed.revision,
                    entity_token=observed.entity_token,
                    stored_bytes=observed.stored_bytes,
                    stored_sha256=observed.stored_sha256,
                    completed_at=observed.completed_at,
                )

        publisher = ArchiveRootPublisher(
            object_store=ObservedStore(), passphrase="fixture", scrypt_log_n=1
        )
        first = publisher.publish_terminal_metadata(
            archive_storage_prefix=case.prefix, document=document
        )
        second = publisher.publish_terminal_metadata(
            archive_storage_prefix=case.prefix, document=document
        )
        assert first == second == seeded
        assert len(outcomes) == 2
        assert all(json.loads(outcome)["dispatch"] == "never-started" for outcome in outcomes)


def test_absent_observation_does_not_settle_or_replace_original_put(tmp_path, monkeypatch):
    with _metadata(tmp_path, monkeypatch) as case:
        owner = ImmutableArchiveAdmission(case.actor.case.factory)
        physical = StorageAdapterImmutableArchiveObjectStore(case.actor.adapter)
        admitted = _arm(case, owner)
        assert physical.observe_immutable_object(admitted.prepared.operation.request) is None
        assert _effect(case, admitted).unresolved
        response = owner.execute(admitted, physical)
        owner.report(admitted, response, apply_receipt=_checkpoint)
        owner.acknowledge_report(admitted, verify_receipt=_verify_checkpoint)


@pytest.mark.parametrize("committed", (False, True))
def test_existing_object_acceptance_preserves_original_unused_proof_across_reply_loss(
    tmp_path, monkeypatch, committed
):
    with _metadata(tmp_path, monkeypatch) as case:
        owner = ImmutableArchiveAdmission(case.actor.case.factory)
        physical = StorageAdapterImmutableArchiveObjectStore(case.actor.adapter)
        _seed(case, physical)
        admitted = _arm(case, owner)
        observed = physical.observe_immutable_object(admitted.prepared.operation.request)
        assert observed is not None
        proof = owner.withdraw(admitted)
        transaction = admission_module.session_scope
        applications = []

        def apply(session, prepared, receipt):
            applications.append(receipt)
            _checkpoint(session, prepared, receipt)

        @contextmanager
        def lost(factory):
            with transaction(factory) as session:
                yield session
                if not committed:
                    raise OSError("injected observation acknowledgement loss")
            if committed:
                raise OSError("injected observation acknowledgement loss")

        with monkeypatch.context() as patch:
            patch.setattr(admission_module, "session_scope", lost)
            with pytest.raises(OSError, match="acknowledgement loss"):
                owner.accept_existing(admitted, proof, observed, apply_receipt=apply)
        assert _effect(case, admitted).unresolved is not committed
        owner.accept_existing(admitted, proof, observed, apply_receipt=apply)
        assert len(applications) == (1 if committed else 2)
        owner.acknowledge_report(admitted, verify_receipt=_verify_checkpoint)


def test_existing_object_cannot_close_a_started_unknown_original_put(tmp_path, monkeypatch):
    with _metadata(tmp_path, monkeypatch) as case:
        owner = ImmutableArchiveAdmission(case.actor.case.factory)
        physical = StorageAdapterImmutableArchiveObjectStore(case.actor.adapter)
        admitted = _arm(case, owner)
        response = owner.execute(admitted, physical)
        owner.report(
            admitted,
            response.model_copy(update={"execution": "unknown"}),
            apply_receipt=_checkpoint,
        )
        observed = physical.observe_immutable_object(admitted.prepared.operation.request)
        assert observed is not None
        with pytest.raises(Conflict, match="used live actor"):
            owner.withdraw(admitted)
        assert _effect(case, admitted).unresolved
        with pytest.raises(Conflict, match="retain its reservation"):
            owner.acknowledge_report(admitted, verify_receipt=_verify_checkpoint)


@pytest.mark.parametrize(
    "change", ("path", "type", "assertions", "placement", "stored", "incomplete-plaintext")
)
def test_existing_object_acceptance_requires_exact_archive_identity(tmp_path, monkeypatch, change):
    with _metadata(tmp_path, monkeypatch) as case:
        owner = ImmutableArchiveAdmission(case.actor.case.factory)
        physical = StorageAdapterImmutableArchiveObjectStore(case.actor.adapter)
        if change in {"stored", "incomplete-plaintext"}:
            assertions = {"riverhog-format": "exact/v1"}
            if change == "incomplete-plaintext":
                assertions["riverhog-plaintext-sha256"] = "a" * 64
            request = case.operation.request.model_copy(
                update={"required_identity_assertions": assertions}
            )
            operation = case.operation.model_copy(update={"request": request})
            prepared = _prepare(
                case,
                operation=operation,
                validate_metadata=lambda session, upload, original: None,
            )
            admitted = _arm(
                case,
                owner,
                prepared,
                validate_metadata=lambda session, upload, original: None,
            )
            response = owner.execute(admitted, physical)
            # Use another unused original call for an observation that cannot
            # satisfy its exact stored-byte identity.
            owner.report(admitted, response, apply_receipt=_checkpoint)
            owner.acknowledge_report(admitted, verify_receipt=_verify_checkpoint)
            admitted = _arm(
                case,
                owner,
                prepared,
                validate_metadata=lambda session, upload, original: None,
            )
            observed = physical.observe_immutable_object(prepared.operation.request)
            changed = observed.model_copy(update={"stored_sha256": "f" * 64})
        else:
            _seed(case, physical)
            admitted = _arm(case, owner)
            observed = physical.observe_immutable_object(admitted.prepared.operation.request)
            updates = {
                "path": {"object_path": case.prefix + "/metadata/other.age"},
                "type": {"verified_content_type": "application/other"},
                "assertions": {"verified_identity_assertions": {"riverhog-format": "other"}},
                "placement": {"verified_placement_policy": "archive_default"},
            }
            changed = observed.model_copy(update=updates[change])
        proof = owner.withdraw(admitted)
        with pytest.raises(ArchiveObjectIdentityConflict):
            owner.accept_existing(
                admitted, proof, changed, apply_receipt=lambda *args: pytest.fail("wrong identity")
            )
        assert _effect(case, admitted).unresolved
        owner.settle_withdrawal(proof)
        owner.acknowledge_report(admitted)


def test_existing_receipt_without_receiver_keeps_its_original_report(tmp_path, monkeypatch):
    with _metadata(tmp_path, monkeypatch) as case:
        owner = ImmutableArchiveAdmission(case.actor.case.factory)
        physical = StorageAdapterImmutableArchiveObjectStore(case.actor.adapter)
        _seed(case, physical)
        admitted = _arm(case, owner)
        observed = physical.observe_immutable_object(admitted.prepared.operation.request)
        proof = owner.withdraw(admitted)
        _select(case)
        with session_scope(case.actor.case.factory) as session:
            session.delete(session.get(CollectionUploadRecord, admitted.prepared.collection_id))
        owner.accept_existing(admitted, proof, observed, apply_receipt=lambda *args: None)
        with pytest.raises(Conflict, match="durable receiving owner"):
            owner.acknowledge_report(admitted, verify_receipt=_verify_checkpoint)
        assert _effect(case, admitted).result is not None


def test_changed_observation_and_oversize_result_preserve_original_control(tmp_path, monkeypatch):
    with _metadata(tmp_path, monkeypatch) as case:
        owner = ImmutableArchiveAdmission(case.actor.case.factory)
        physical = StorageAdapterImmutableArchiveObjectStore(case.actor.adapter)
        _seed(case, physical)
        admitted = _arm(case, owner, grants=replace(case.grants, result_bytes=100))
        observed = physical.observe_immutable_object(admitted.prepared.operation.request)
        proof = owner.withdraw(admitted)
        with pytest.raises(Conflict, match="reserved byte grant"):
            owner.accept_existing(admitted, proof, observed, apply_receipt=_checkpoint)
        assert _effect(case, admitted).unresolved
        owner.settle_withdrawal(proof)
        owner.acknowledge_report(admitted)

    with _metadata(tmp_path / "changed", monkeypatch) as case:
        owner = ImmutableArchiveAdmission(case.actor.case.factory)
        physical = StorageAdapterImmutableArchiveObjectStore(case.actor.adapter)
        _seed(case, physical)
        admitted = _arm(case, owner)
        observed = physical.observe_immutable_object(admitted.prepared.operation.request)
        proof = owner.withdraw(admitted)
        owner.accept_existing(admitted, proof, observed, apply_receipt=_checkpoint)
        with pytest.raises(Conflict, match="conflicts with its outcome"):
            owner.accept_existing(
                admitted,
                proof,
                observed.model_copy(update={"revision": "changed"}),
                apply_receipt=lambda *args: pytest.fail("changed receipt"),
            )
        owner.acknowledge_report(admitted, verify_receipt=_verify_checkpoint)
