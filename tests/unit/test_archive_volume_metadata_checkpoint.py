from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest
from riverhog_archive_contracts import update_archive_sequence_commitment
from riverhog_core.archive_manifest import (
    build_collection_archive_terminal_document,
    build_collection_archive_volume_document,
)
from riverhog_core.archive_root import ArchiveRootPublisher
from riverhog_core.archive_volume_receipts import parse_sealed_pack, parse_sealed_raw
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord as Volume
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.pack_volume import parse_pack_volume_plan
from riverhog_core.services.archive_volume_metadata_checkpoint import ArchiveVolumeMetadataSource
from riverhog_protocol.errors import Conflict, NotFound
from riverhog_storage_adapter_protocol import PutObjectOperation
from sqlalchemy import select

from tests.unit.test_archive_root import MemoryImmutableStore
from tests.unit.test_collection_upload_preparation import _case
from tests.unit.test_finalization_claim_readiness import _seal


def _source(case, sequence, *, terminal=False):
    with session_scope(case.factory) as session:
        upload = session.get(Upload, case.collection_id)
        upload.archive_tree_sha256 = "b" * 64
        if terminal:
            record = None
            document = build_collection_archive_terminal_document(
                archive_generation=upload.archive_generation,
                artifact_set_sha256=upload.archive_tree_sha256,
                sequence=sequence,
            )
        else:
            record = session.scalar(
                select(Volume).where(
                    Volume.collection_id == case.collection_id, Volume.sequence == sequence
                )
            )
            if record.kind == "pack":
                plan = parse_pack_volume_plan(record.plan_json)
                receipt = parse_sealed_pack(record.sealed_receipt_json)
            else:
                plan = None
                receipt = parse_sealed_raw(record.sealed_receipt_json)
            document = build_collection_archive_volume_document(
                archive_generation=upload.archive_generation,
                artifact_set_sha256=upload.archive_tree_sha256,
                plan=plan,
                receipt=receipt,
            )
        source = ArchiveVolumeMetadataSource.capture(
            upload=upload,
            record=record,
            document=document,
            total_volumes=upload.planner_next_sequence,
        )
        return source


def _publish(source):
    publisher = ArchiveRootPublisher(
        object_store=MemoryImmutableStore(), passphrase="fixture", scrypt_log_n=1
    )
    if source.object_id is None:
        return publisher.publish_terminal_metadata(
            archive_storage_prefix=source.publication.archive_storage_prefix,
            document=source.document,
        )
    return publisher.publish_volume_metadata(
        archive_storage_prefix=source.publication.archive_storage_prefix,
        document=source.document,
    )


def _snapshot(case):
    with session_scope(case.factory) as session:
        upload = session.get(Upload, case.collection_id)
        volume = session.get(Volume, (case.collection_id, case.object_id))
        return (
            upload.archive_volume_next_sequence,
            upload.archive_volume_hash_state,
            upload.archive_terminal_receipt_json,
            upload.archive_ordered_volume_sha256,
            volume.metadata_receipt_json,
        )


@pytest.mark.parametrize("kind", ["pack", "segment"])
def test_actual_receipt_is_idempotent_through_fresh_receiving_owners(tmp_path, kind):
    with _case(tmp_path, kind, artifact_count=2) as case:
        _seal(case)
        source = _source(case, 0)
        published = _publish(source)
        with session_scope(case.factory) as session:
            source.apply_receipt(session, published)
            source.verify_receipt(session, published)
            source.apply_receipt(session, published)
        before = _snapshot(case)
        assert before[0] == 1 and before[1] is not None and before[4] is not None
        cold = _source(case, 0)
        with session_scope(case.factory) as session:
            cold.apply_receipt(session, published)
            cold.verify_receipt(session, published)
        assert _snapshot(case) == before


@pytest.mark.parametrize("kind", ["pack", "segment"])
def test_terminal_receipt_replay_does_not_append_the_commitment_twice(tmp_path, kind):
    with _case(tmp_path, kind, artifact_count=2) as case:
        _seal(case)
        expected = hashlib.sha256()
        for sequence in range(2):
            source = _source(case, sequence)
            update_archive_sequence_commitment(expected, source.document)
            with session_scope(case.factory) as session:
                source.apply_receipt(session, _publish(source))
        source = _source(case, 2, terminal=True)
        published = _publish(source)
        update_archive_sequence_commitment(expected, source.document)
        with session_scope(case.factory) as session:
            source.apply_receipt(session, published)
            source.verify_receipt(session, published)
            source.apply_receipt(session, published)
        before = _snapshot(case)
        assert before[0] == 2 and before[1] is None and before[2] is not None
        assert before[3] == expected.hexdigest()
        cold = _source(case, 2, terminal=True)
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            upload.state = upload.archive_phase = "orphaned"
        with session_scope(case.factory) as session:
            with pytest.raises(Conflict, match="claim is not current"):
                cold.guard(session)
            cold.apply_receipt(session, published)
            cold.verify_receipt(session, published)
        assert _snapshot(case) == before


@pytest.mark.parametrize("committed", [False, True])
def test_receiving_rollback_and_lost_commit_reply_preserve_once_only_progress(tmp_path, committed):
    with _case(tmp_path, "segment", artifact_count=2) as case:
        _seal(case)
        source = _source(case, 0)
        published = _publish(source)
        with pytest.raises(OSError, match="injected"):
            with session_scope(case.factory) as session:
                source.apply_receipt(session, published)
                if not committed:
                    raise OSError("injected receiving rollback")
            raise OSError("injected lost receiving commit reply")
        assert _snapshot(case)[0] == int(committed)
        with session_scope(case.factory) as session:
            source.apply_receipt(session, published)
            source.verify_receipt(session, published)
        assert _snapshot(case)[0] == 1


@pytest.mark.parametrize(
    "field",
    [
        "sequence",
        "relative_path",
        "plaintext_bytes",
        "plaintext_sha256",
        "revision",
        "stored_bytes",
        "stored_sha256",
        "completed_at",
    ],
)
def test_durable_receiving_proof_requires_the_original_projection(tmp_path, field):
    with _case(tmp_path, "segment", artifact_count=2) as case:
        _seal(case)
        source = _source(case, 0)
        published = _publish(source)
        with session_scope(case.factory) as session:
            source.apply_receipt(session, published)
        before = _snapshot(case)
        old = getattr(published, field)
        changed = replace(published, **{field: old + 1 if isinstance(old, int) else "changed"})
        for method in (source.apply_receipt, source.verify_receipt):
            with pytest.raises(Conflict):
                with session_scope(case.factory) as session:
                    method(session, changed)
        assert _snapshot(case) == before


@pytest.mark.parametrize("field", ["tree", "prefix", "plan", "seal", "planned_count"])
def test_receiving_rechecks_exact_source_after_original_capture(tmp_path, field):
    with _case(tmp_path, "segment", artifact_count=2) as case:
        _seal(case)
        source = _source(case, 0)
        published = _publish(source)
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            volume = session.get(Volume, (case.collection_id, case.object_id))
            if field == "tree":
                upload.archive_tree_sha256 = "c" * 64
            elif field == "prefix":
                upload.archive_storage_prefix += "/changed"
            elif field == "planned_count":
                upload.planner_next_sequence += 1
            elif field == "plan":
                volume.plan_json = "{}"
            else:
                volume.sealed_receipt_json = "{}"
        before = _snapshot(case)
        with pytest.raises(Conflict, match="changed"):
            with session_scope(case.factory) as session:
                source.apply_receipt(session, published)
        assert _snapshot(case) == before


def test_accepted_finalizer_authority_is_separate_from_receiving_facts(tmp_path):
    with _case(tmp_path, "segment", artifact_count=2) as case:
        _seal(case)
        source = _source(case, 0)
        with session_scope(case.factory) as session:
            with pytest.raises(Conflict, match="claim is not current"):
                source.guard(session)
        assert case.service._claim_due_finalization() == case.collection_id
        operation = PutObjectOperation(request=source.publication.request(b"opaque ciphertext"))
        with session_scope(case.factory) as session:
            source.guard(session)
            source.validate_metadata(session, session.get(Upload, case.collection_id), operation)
            assert session.get(Upload, case.collection_id).lease_expires_at is None
            with pytest.raises(Conflict, match="not durably received"):
                source.verify_receipt(session, _publish(source))
        published = _publish(source)
        with session_scope(case.factory) as session:
            source.apply_receipt(session, published)
            source.verify_receipt(session, published)
            with pytest.raises(Conflict, match="claim is not current"):
                source.guard(session)


def test_parent_absence_is_not_positive_receiving_acknowledgement(tmp_path):
    with _case(tmp_path, "segment", artifact_count=2) as case:
        _seal(case)
        source = _source(case, 0)
        published = _publish(source)
        with session_scope(case.factory) as session:
            session.delete(session.get(Upload, case.collection_id))
        for method in (source.apply_receipt, source.verify_receipt):
            with pytest.raises(NotFound, match="source is unavailable"):
                with session_scope(case.factory) as session:
                    method(session, published)
