from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import replace

import pytest
from riverhog_core import incremental_emission
from riverhog_core.app_permissions import ARCHIVES_MANAGE, COLLECTIONS_CREATE, ApplicationAccess
from riverhog_core.catalog_db import dispose_session_factory, session_scope
from riverhog_core.catalog_models import (
    CollectionArchiveObjectUploadRecord,
    CollectionUploadArtifactRecord,
    CollectionUploadArtifactVolumeRecord,
    CollectionUploadRecord,
)
from riverhog_core.catalog_registration_models import (
    CollectionRegistrationRecord,
    CollectionRegistrationVolumeRecord,
)
from riverhog_core.collection_plan import CollectionVolumePolicy
from riverhog_core.domain.archive import ArchiveArtifact, PackVolumePlan
from riverhog_core.incremental_emission import (
    VolumeEmissionCapacity,
    VolumeEmissionLimits,
    parse_volume_emission_frontier,
    volume_emission_metadata,
)
from riverhog_core.incremental_plan import (
    OrderedArchiveArtifact,
    advance_incremental_volume_plan,
    incremental_volume_planner_checkpoint_bytes,
    parse_incremental_volume_planner_checkpoint,
)
from riverhog_core.services import collection_uploads as uploads
from riverhog_core.services.app_keys import SqlAlchemyAppKeyService
from riverhog_core.services.collection_registration import CollectionRegistrationOwner
from riverhog_protocol import CollectionUploadRawDigestBatchDocument
from riverhog_protocol.errors import Conflict, NotFound
from riverhog_protocol.raw_ingress import ordered_raw_part_commitment
from sqlalchemy import func, select

from tests.unit.test_app_keys import BOOTSTRAP
from tests.unit.test_collection_uploads import _service_with_archive_objects

PART = 5 * 1024 * 1024
LIMITS = VolumeEmissionLimits(3, 16, 100000, 4)


def _member(identity="2", volumes=17):
    count, commitment = ordered_raw_part_commitment(("a" * 64,) * volumes)
    return {
        "artifact_id": identity * 64,
        "bytes": str(volumes * PART - 1),
        "sha256": hashlib.sha256(b"metadata parity fixture").hexdigest(),
        "raw_parts": {
            "part_plaintext_bytes": str(PART),
            "part_count": str(count),
            "ordered_sha256": commitment,
        },
    }


def _open(tmp_path, *, custody_mode="producer-retained", copy_to=()):
    policy = CollectionVolumePolicy(
        pack_member_bytes=8,
        raw_volume_plaintext_bytes=PART,
        raw_part_plaintext_bytes=PART,
    )
    service, config, resumable, roots = _service_with_archive_objects(tmp_path, policy=policy)
    if copy_to:
        from tests.unit.storage_incarnation_fixtures import (
            fixture_storage_incarnation_id,
            seed_storage_incarnation,
        )

        binding = service._archive_stores.require("archive")
        with session_scope(service._session_factory) as session:
            for name in copy_to:
                seed_storage_incarnation(session, "archive", name)
                service._archive_stores.admit(
                    name,
                    replace(
                        binding, incarnation_id=fixture_storage_incarnation_id("archive", name)
                    ),
                    probe=lambda: None,
                )
    keys = SqlAlchemyAppKeyService(config)
    created = keys.create(
        app="registration-writer",
        access=(ApplicationAccess(COLLECTIONS_CREATE),)
        + ((ApplicationAccess(ARCHIVES_MANAGE),) if copy_to else ()),
        grantor=BOOTSTRAP,
    )
    principal = keys.authenticate(str(created["token"]))
    assert principal is not None
    opened = service.create_or_resume(
        idempotency_key="bounded-registration",
        ingest_source="fixture",
        archive_store=None,
        initiator=principal,
        event_context=None,
        custody_mode=custody_mode,
        copy_to=copy_to,
    )
    return service, keys, principal, created, int(opened["collection_id"])


def _read(service, operation_id):
    with session_scope(service._session_factory) as session:
        operation = session.get(CollectionRegistrationRecord, operation_id)
        return {
            "phase": operation.phase,
            "stage": operation.stage_frontier_json,
            "verify": operation.verify_frontier_json,
            "epoch": operation.execution_epoch,
            "ack": operation.acknowledgement_json,
        }


def _until(service, collection_id, operation_id, phase):
    owner = CollectionRegistrationOwner(service)
    for _ in range(100):
        current = _read(service, operation_id)
        if current["phase"] == phase:
            return
        owner.advance(collection_id, operation_id, limits=LIMITS)
    pytest.fail("bounded registration did not reach its expected phase")


def _close(service, keys):
    service.close()
    dispose_session_factory(service._session_factory)
    dispose_session_factory(keys._session_factory)


@pytest.mark.parametrize("prior_pending_pack", [False, True])
def test_service_registration_reopens_hidden_quantum_and_publishes_one_exact_acknowledgement(
    tmp_path, monkeypatch, prior_pending_pack
):
    service, keys, principal, created, collection_id = _open(tmp_path)
    zero = {"artifact_id": "1" * 64, "bytes": "0", "sha256": hashlib.sha256(b"").hexdigest()}
    if prior_pending_pack:
        service.register_artifacts(collection_id, [zero])
    with session_scope(service._session_factory) as session:
        upload = session.get(CollectionUploadRecord, collection_id)
        original = upload.planner_checkpoint_json
    base = parse_incremental_volume_planner_checkpoint(original)
    member = _member()
    expected = advance_incremental_volume_plan(
        base,
        (
            OrderedArchiveArtifact(
                base.next_artifact_order,
                ArchiveArtifact(member["artifact_id"], int(member["bytes"]), member["sha256"]),
            ),
        ),
    )
    raw_calls = 0
    constructor = incremental_emission.RawVolumePlan

    def construct(*args, **kwargs):
        nonlocal raw_calls
        raw_calls += 1
        return constructor(*args, **kwargs)

    monkeypatch.setattr(incremental_emission, "RawVolumePlan", construct)
    owner = CollectionRegistrationOwner(service)
    pending = owner.admit(collection_id, [member], principal=principal)
    operation_id = pending["operation_id"]
    assert pending["phase"] == "staging"
    admitted = _read(service, operation_id)
    assert owner.admit(collection_id, [member], principal=principal) == pending
    assert _read(service, operation_id)["stage"] == admitted["stage"]
    owner.advance(collection_id, operation_id, limits=LIMITS)
    with session_scope(service._session_factory) as session:
        session.info["include_unpublished_registration"] = True
        assert (
            session.scalar(
                select(func.count())
                .select_from(CollectionArchiveObjectUploadRecord)
                .where(CollectionArchiveObjectUploadRecord.registration_id == operation_id)
            )
            == 3
        )
    stores, config = service._archive_stores, service._config
    _close(service, keys)
    service = uploads.SqlAlchemyCollectionUploadService(config, stores, policy=base.policy)
    keys = SqlAlchemyAppKeyService(config)
    principal = keys.authenticate(str(created["token"]))
    try:
        owner = CollectionRegistrationOwner(service)
        with session_scope(service._session_factory) as session:
            upload = session.get(CollectionUploadRecord, collection_id)
            assert upload.planner_checkpoint_json == original
            assert upload.artifact_count == int(prior_pending_pack)
            assert upload.planner_next_sequence == 0
            assert (
                session.get(CollectionUploadArtifactRecord, (collection_id, member["artifact_id"]))
                is None
            )
        assert service.list_volumes(collection_id)["volumes"] == []
        work = service.acquire_work(collection_id, limit=3)
        assert work["work"] == [] and work["planning_complete"] is False
        with pytest.raises(Conflict, match="registration is pending"):
            service.complete(collection_id)
        phases = Counter()
        for _ in range(100):
            before = _read(service, operation_id)
            if before["phase"] == "published":
                break
            phases[before["phase"]] += 1
            owner.advance(collection_id, operation_id, limits=LIMITS)
            after = _read(service, operation_id)
            stage = parse_volume_emission_frontier(after["stage"])
            prior_stage = parse_volume_emission_frontier(before["stage"])
            assert stage.checkpoint.next_sequence - prior_stage.checkpoint.next_sequence <= 3
            if after["phase"] != "published":
                assert service.list_volumes(collection_id)["volumes"] == []
                with session_scope(service._session_factory) as session:
                    upload = session.get(CollectionUploadRecord, collection_id)
                    assert upload.planner_checkpoint_json == original
                    assert upload.artifact_count == int(prior_pending_pack)
        else:
            pytest.fail("registration did not publish")
        assert all(phases[phase] for phase in ("staging", "verifying", "auditing", "ready"))
        assert raw_calls == 34
        acknowledgement = json.loads(_read(service, operation_id)["ack"])
        assert owner.admit(collection_id, [member], principal=principal) == acknowledgement
        assert owner.advance(collection_id, operation_id, limits=LIMITS) == acknowledgement
        with session_scope(service._session_factory) as session:
            upload = session.get(CollectionUploadRecord, collection_id)
            assert upload.active_registration_id is None
            assert (
                upload.planner_checkpoint_json
                == incremental_volume_planner_checkpoint_bytes(expected.checkpoint).decode()
            )
            assert upload.artifact_count == 1 + int(prior_pending_pack)
            assert upload.artifact_bytes == int(member["bytes"])
            for plan in expected.volumes:
                row = session.get(
                    CollectionArchiveObjectUploadRecord, (collection_id, plan.volume_id)
                )
                body, members, _ = volume_emission_metadata(plan)
                assert row.plan_json.encode() == body
                assert row.sequence == plan.sequence
                assert row.plan_sha256 == (
                    plan.plan_sha256
                    if isinstance(plan, PackVolumePlan)
                    else hashlib.sha256(body).hexdigest()
                )
                assert set(
                    session.scalars(
                        select(CollectionUploadArtifactVolumeRecord.artifact_id).where(
                            CollectionUploadArtifactVolumeRecord.collection_id == collection_id,
                            CollectionUploadArtifactVolumeRecord.object_id == plan.volume_id,
                        )
                    )
                ) == set(members)
            summaries = list(
                session.scalars(
                    select(CollectionRegistrationVolumeRecord)
                    .where(CollectionRegistrationVolumeRecord.registration_id == operation_id)
                    .order_by(CollectionRegistrationVolumeRecord.sequence)
                )
            )
            assert [(s.sequence, s.object_id) for s in summaries] == [
                (p.sequence, p.volume_id) for p in expected.volumes
            ]
        later = owner.admit(collection_id, [zero], principal=principal)
        _until(service, collection_id, later["operation_id"], "published")
        service.register_raw_part_digests(
            collection_id,
            CollectionUploadRawDigestBatchDocument(
                artifact_id=member["artifact_id"], first_part="0", sha256s=["a" * 64] * 17
            ),
        )
        service.complete(collection_id)
        assert owner.admit(collection_id, [member], principal=principal) == acknowledgement
        assert owner.status(collection_id, operation_id, principal=principal) == acknowledgement
        page = owner.list_volumes(collection_id, operation_id, principal=principal, page_size=3)
        assert len(page["volumes"]) == 3
        assert not page["exhausted"] and page["next_page_token"] is not None
        assert raw_calls == 34
    finally:
        _close(service, keys)


def test_quantum_commit_failure_rolls_back_rows_chains_and_cursor(tmp_path, monkeypatch):
    service, keys, principal, created, collection_id = _open(tmp_path)
    owner = CollectionRegistrationOwner(service)
    operation_id = owner.admit(collection_id, [_member()], principal=principal)["operation_id"]
    before = _read(service, operation_id)
    persist = uploads._persist_plan_batch

    def interrupted(*args, **kwargs):
        persist(*args, **kwargs)
        raise RuntimeError("injected quantum interruption")

    try:
        with monkeypatch.context() as patch:
            patch.setattr(uploads, "_persist_plan_batch", interrupted)
            with pytest.raises(RuntimeError, match="injected quantum interruption"):
                owner.advance(collection_id, operation_id, limits=LIMITS)
        assert _read(service, operation_id) == before
        with session_scope(service._session_factory) as session:
            session.info["include_unpublished_registration"] = True
            assert (
                session.scalar(
                    select(func.count())
                    .select_from(CollectionArchiveObjectUploadRecord)
                    .where(CollectionArchiveObjectUploadRecord.registration_id == operation_id)
                )
                == 0
            )
        owner.advance(collection_id, operation_id, limits=LIMITS)
        assert (
            parse_volume_emission_frontier(
                _read(service, operation_id)["stage"]
            ).checkpoint.next_sequence
            == 3
        )
    finally:
        _close(service, keys)


@pytest.mark.parametrize("change", ["plan", "work", "coverage", "summary", "member"])
def test_independent_verification_rejects_changed_staged_metadata(tmp_path, change):
    service, keys, principal, created, collection_id = _open(tmp_path)
    owner = CollectionRegistrationOwner(service)
    member = _member()
    operation_id = owner.admit(collection_id, [member], principal=principal)["operation_id"]
    try:
        _until(service, collection_id, operation_id, "verifying")
        with session_scope(service._session_factory) as session:
            session.info["include_unpublished_registration"] = True
            row = session.scalar(
                select(CollectionArchiveObjectUploadRecord)
                .where(CollectionArchiveObjectUploadRecord.registration_id == operation_id)
                .order_by(CollectionArchiveObjectUploadRecord.sequence)
                .limit(1)
            )
            if change == "plan":
                row.plan_json = "{}"
            elif change == "work":
                row.source_first_part += 1
            elif change == "coverage":
                edge = session.get(
                    CollectionUploadArtifactVolumeRecord,
                    (collection_id, member["artifact_id"], row.object_id),
                )
                session.delete(edge)
            elif change == "summary":
                summary = session.get(
                    CollectionRegistrationVolumeRecord, (operation_id, row.sequence)
                )
                summary.object_id = "segment-" + "f" * 64
            else:
                session.get(
                    CollectionUploadArtifactRecord, (collection_id, member["artifact_id"])
                ).sha256 = "f" * 64
        with pytest.raises(RuntimeError, match="registration"):
            _until(service, collection_id, operation_id, "ready")
        with session_scope(service._session_factory) as session:
            upload = session.get(CollectionUploadRecord, collection_id)
            assert upload.artifact_count == upload.planner_next_sequence == 0
            assert (
                session.get(CollectionRegistrationRecord, operation_id).acknowledgement_json is None
            )
    finally:
        _close(service, keys)


@pytest.mark.parametrize("at_phase", ["staging", "ready"])
def test_registration_rechecks_revocation_and_never_renews_lease_in_background(tmp_path, at_phase):
    service, keys, principal, created, collection_id = _open(
        tmp_path, custody_mode="custody-transfer"
    )
    owner = CollectionRegistrationOwner(service)
    operation_id = owner.admit(collection_id, [_member()], principal=principal)["operation_id"]
    try:
        if at_phase == "ready":
            _until(service, collection_id, operation_id, "ready")
        before = _read(service, operation_id)
        with session_scope(service._session_factory) as session:
            lease = session.get(CollectionUploadRecord, collection_id).lease_expires_at
        keys.revoke(app=principal.id, key_id=principal.key_id)
        with pytest.raises(NotFound, match="no longer active"):
            owner.advance(collection_id, operation_id, limits=LIMITS)
        assert _read(service, operation_id) == before
        with session_scope(service._session_factory) as session:
            assert session.get(CollectionUploadRecord, collection_id).lease_expires_at == lease
    finally:
        _close(service, keys)


def test_expired_lease_and_insufficient_quantum_preserve_exact_registration_cursor(tmp_path):
    service, keys, principal, created, collection_id = _open(
        tmp_path, custody_mode="custody-transfer"
    )
    owner = CollectionRegistrationOwner(service)
    operation_id = owner.admit(collection_id, [_member()], principal=principal)["operation_id"]
    before = _read(service, operation_id)
    try:
        with pytest.raises(VolumeEmissionCapacity):
            owner.advance(collection_id, operation_id, limits=VolumeEmissionLimits(3, 16, 1, 4))
        assert _read(service, operation_id) == before
        owner.advance(collection_id, operation_id, limits=LIMITS)
        after = _read(service, operation_id)
        with session_scope(service._session_factory) as session:
            session.get(
                CollectionUploadRecord, collection_id
            ).lease_expires_at = "2000-01-01T00:00:00.000000000Z"
        with pytest.raises(Conflict, match="lease is not active"):
            owner.advance(collection_id, operation_id, limits=LIMITS)
        assert _read(service, operation_id) == after
    finally:
        _close(service, keys)


def test_extra_owned_coverage_is_rejected_by_bounded_audit_without_publishing(tmp_path):
    service, keys, principal, created, collection_id = _open(tmp_path)
    zero = {"artifact_id": "1" * 64, "bytes": "0", "sha256": hashlib.sha256(b"").hexdigest()}
    service.register_artifacts(collection_id, [zero, _member("3", 1)])
    owner = CollectionRegistrationOwner(service)
    member = _member()
    operation_id = owner.admit(collection_id, [member], principal=principal)["operation_id"]
    try:
        _until(service, collection_id, operation_id, "verifying")
        with session_scope(service._session_factory) as session:
            session.info["include_unpublished_registration"] = True
            old_plan = session.scalar(
                select(CollectionArchiveObjectUploadRecord.object_id)
                .where(
                    CollectionArchiveObjectUploadRecord.collection_id == collection_id,
                    CollectionArchiveObjectUploadRecord.registration_id.is_(None),
                    CollectionArchiveObjectUploadRecord.kind == "pack",
                )
                .limit(1)
            )
            assert old_plan is not None
            session.add(
                CollectionUploadArtifactVolumeRecord(
                    collection_id=collection_id,
                    artifact_id=member["artifact_id"],
                    object_id=old_plan,
                    registration_id=operation_id,
                )
            )
        _until(service, collection_id, operation_id, "auditing")
        with pytest.raises(RuntimeError, match="extra staged coverage"):
            _until(service, collection_id, operation_id, "ready")
        with session_scope(service._session_factory) as session:
            upload = session.get(CollectionUploadRecord, collection_id)
            assert upload.artifact_count == 2
            assert upload.planner_next_sequence == 2
            assert (
                session.get(CollectionRegistrationRecord, operation_id).acknowledgement_json is None
            )
    finally:
        _close(service, keys)


def test_changed_grants_require_authorized_resume_and_fence_old_execution_epoch(tmp_path):
    service, keys, principal, created, collection_id = _open(tmp_path)
    owner = CollectionRegistrationOwner(service)
    member = _member()
    operation_id = owner.admit(collection_id, [member], principal=principal)["operation_id"]
    try:
        owner.advance(collection_id, operation_id, limits=LIMITS)
        before = _read(service, operation_id)
        keys.replace_access(
            app=principal.id,
            key_id=principal.key_id,
            access=(ApplicationAccess(COLLECTIONS_CREATE),),
            grantor=BOOTSTRAP,
        )
        with pytest.raises(NotFound, match="authorization changed"):
            owner.advance(collection_id, operation_id, limits=LIMITS)
        assert _read(service, operation_id) == before
        current = keys.authenticate(str(created["token"]))
        resumed = owner.admit(collection_id, [member], principal=current)
        assert resumed["operation_id"] == operation_id
        after = _read(service, operation_id)
        assert after["stage"] == before["stage"]
        assert after["epoch"] != before["epoch"]
        with pytest.raises(Conflict, match="execution changed"):
            owner.advance(
                collection_id, operation_id, limits=LIMITS, expected_epoch=before["epoch"]
            )
        owner.advance(collection_id, operation_id, limits=LIMITS, expected_epoch=after["epoch"])
    finally:
        _close(service, keys)


def test_request_order_and_existing_outcomes_are_immutable_across_later_operations(tmp_path):
    service, keys, principal, created, collection_id = _open(tmp_path)
    owner = CollectionRegistrationOwner(service)
    batch = [
        {"artifact_id": identity * 64, "bytes": "0", "sha256": hashlib.sha256(b"").hexdigest()}
        for identity in ("1", "2")
    ]
    try:
        first = owner.admit(collection_id, batch, principal=principal)
        with pytest.raises(Conflict, match="registration is pending"):
            owner.admit(collection_id, list(reversed(batch)), principal=principal)
        _until(service, collection_id, first["operation_id"], "published")
        acknowledgement = owner.admit(collection_id, batch, principal=principal)
        assert all(item["new"] for item in acknowledgement["artifacts"])
        assert acknowledgement["emitted"] == {"first": "0" * 64, "next": "0" * 64}
        other = owner.admit(collection_id, list(reversed(batch)), principal=principal)
        assert other["operation_id"] != first["operation_id"]
        _until(service, collection_id, other["operation_id"], "published")
        other_ack = owner.admit(collection_id, list(reversed(batch)), principal=principal)
        assert not any(item["new"] for item in other_ack["artifacts"])
        assert owner.admit(collection_id, batch, principal=principal) == acknowledgement
    finally:
        _close(service, keys)


@pytest.mark.parametrize("volume_count", [1, 16, 17, 129, 1000])
def test_first_service_quantum_does_not_construct_a_growing_raw_prefix(
    tmp_path, monkeypatch, volume_count
):
    service, keys, principal, created, collection_id = _open(tmp_path)
    owner = CollectionRegistrationOwner(service)
    calls = 0
    constructor = incremental_emission.RawVolumePlan

    def construct(*args, **kwargs):
        nonlocal calls
        calls += 1
        return constructor(*args, **kwargs)

    monkeypatch.setattr(incremental_emission, "RawVolumePlan", construct)
    try:
        operation_id = owner.admit(
            collection_id, [_member(volumes=volume_count)], principal=principal
        )["operation_id"]
        owner.advance(collection_id, operation_id, limits=LIMITS)
        assert calls == min(3, volume_count)
        frontier = parse_volume_emission_frontier(_read(service, operation_id)["stage"])
        assert frontier.checkpoint.next_sequence == min(3, volume_count)
        assert service.list_volumes(collection_id)["volumes"] == []
    finally:
        _close(service, keys)


def test_admission_refuses_an_oversized_batch_before_materializing_it(tmp_path):
    from riverhog_protocol.errors import BadRequest
    from riverhog_protocol.transport import COLLECTION_UPLOAD_ARTIFACT_BATCH_MAX

    class OversizedBatch:
        def __len__(self):
            return COLLECTION_UPLOAD_ARTIFACT_BATCH_MAX + 1

        def __iter__(self):
            raise AssertionError("oversized request was materialized")

    service, keys, principal, created, collection_id = _open(tmp_path)
    try:
        with pytest.raises(BadRequest, match="artifact batch"):
            CollectionRegistrationOwner(service).admit(
                collection_id, OversizedBatch(), principal=principal
            )
    finally:
        _close(service, keys)
