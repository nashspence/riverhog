from __future__ import annotations

from dataclasses import replace

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord
from riverhog_core.raw_volume import (
    parse_raw_volume_plan,
    raw_volume_plan_bytes,
)

from tests.unit.test_collection_upload_preparation import _case, _unit
from tests.unit.test_raw_upload import (
    ARCHIVE_UNIT_BYTES,
    MemoryRawCheckpointStore,
    MemoryResumableStore,
    _plan,
    _uploader,
)


@pytest.mark.parametrize(
    "change",
    (
        {"volume_id": "segment-" + "1" * 64},
        {"sequence": 1},
        {"artifact_id": "b" * 64},
        {"artifact_offset": 1},
        {"plaintext_bytes": 1},
        {"artifact_bytes": 1},
        {"artifact_sha256": "b" * 64},
    ),
)
def test_completed_raw_receipt_requires_the_independent_original_plan(change):
    content = b"exact completed raw original"
    plan = _plan(content)
    store = MemoryResumableStore()
    checkpoints = MemoryRawCheckpointStore()
    uploader = _uploader(store, checkpoints)
    checkpoint = uploader.open(
        collection_id=1,
        plan=plan,
        object_path="archives/opaque/volumes/" + plan.volume_id + ".bin.age",
        relative_path="volumes/" + plan.volume_id + ".bin.age",
        target_part_plaintext_bytes=ARCHIVE_UNIT_BYTES,
    )
    checkpoint = uploader.upload_next_unit(plan=plan, checkpoint=checkpoint, plaintext=content)
    assert checkpoint.completed is not None
    original = checkpoint.to_json()
    objects, uploads, next_id = dict(store.objects), dict(store.uploads), store.next_id
    with pytest.raises(ValueError, match="does not match its plan"):
        uploader.sealed_receipt(checkpoint, plan=replace(plan, **change))
    assert checkpoint.to_json() == original
    receipt = uploader.sealed_receipt(checkpoint, plan=plan)
    assert receipt.artifact_id == plan.artifact_id
    assert store.objects == objects and store.uploads == uploads and store.next_id == next_id


def test_cold_payload_receiving_rejects_a_different_registered_plan(tmp_path):
    with _case(tmp_path, "segment") as case:
        _unit(case)
        with session_scope(case.factory) as session:
            volume = session.get(
                CollectionArchiveObjectUploadRecord, (case.collection_id, case.object_id)
            )
            assert volume.checkpoint_json is not None and volume.sealed_receipt_json is not None
            original_checkpoint, original_plan = volume.checkpoint_json, volume.plan_json
            volume.sealed_receipt_json = None
            changed = replace(parse_raw_volume_plan(original_plan), artifact_sha256="b" * 64)
            volume.plan_json = raw_volume_plan_bytes(changed).decode()
        with pytest.raises(ValueError, match="does not match its plan"):
            case.service._reconcile_sealed_volume_receipts(case.collection_id)
        with session_scope(case.factory) as session:
            volume = session.get(
                CollectionArchiveObjectUploadRecord, (case.collection_id, case.object_id)
            )
            assert volume.checkpoint_json == original_checkpoint
            assert volume.sealed_receipt_json is None
            volume.plan_json = original_plan
        case.service._reconcile_sealed_volume_receipts(case.collection_id)
        with session_scope(case.factory) as session:
            volume = session.get(
                CollectionArchiveObjectUploadRecord, (case.collection_id, case.object_id)
            )
            assert volume.checkpoint_json == original_checkpoint
            assert volume.sealed_receipt_json is not None
