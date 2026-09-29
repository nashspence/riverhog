from __future__ import annotations

import pytest
from pydantic import ValidationError
from riverhog_protocol import (
    ArtifactId,
    CollectionArtifactProvenanceBindingBatchDocument,
    CollectionUploadArtifactBatchDocument,
    CollectionUploadArtifactCustodyReceiptDocument,
    CollectionUploadArtifactIn,
    CollectionUploadCustodyObjectDocument,
    CollectionUploadProvenanceJournalStatusDocument,
    CollectionUploadRegistrationConstraintsDocument,
    CollectionUploadUnitAssignmentDocument,
    CollectionUploadWorkBatchDocument,
    validate_collection_upload_artifact_custody_receipt,
    validate_collection_upload_batch_against_registration_constraints,
)
from riverhog_protocol.raw_ingress import ordered_raw_part_commitment

FIRST = "0" * 63 + "1"
SECOND = "0" * 63 + "2"
JOURNAL = "urn:uuid:00000000-0000-0000-0000-000000000001"
ENTRY = "urn:uuid:00000000-0000-0000-0000-000000000002"
ASSOCIATION = "urn:uuid:00000000-0000-0000-0000-000000000003"


def _artifact(artifact_id: str = FIRST, *, byte_count: str = "1") -> dict[str, object]:
    return {"artifact_id": artifact_id, "bytes": byte_count, "sha256": "a" * 64}


def _constraints(
    *, pack_member_bytes: int = 1024
) -> CollectionUploadRegistrationConstraintsDocument:
    return CollectionUploadRegistrationConstraintsDocument.model_validate(
        {"pack_member_bytes": str(pack_member_bytes), "raw_part_plaintext_bytes": "65536"}
    )


def test_registration_is_pathless_and_arrival_order_does_not_assign_ids() -> None:
    batch = CollectionUploadArtifactBatchDocument.model_validate(
        {"artifacts": [_artifact(SECOND), _artifact(FIRST)]}
    )
    assert [item.artifact_id for item in batch.artifacts] == [SECOND, FIRST]
    assert set(CollectionUploadArtifactIn.model_json_schema()["properties"]) == {
        "artifact_id",
        "bytes",
        "sha256",
        "raw_parts",
    }
    with pytest.raises(ValidationError):
        CollectionUploadArtifactIn.model_validate({**_artifact(), "path": "old-name"})
    with pytest.raises(ValidationError, match="unique"):
        CollectionUploadArtifactBatchDocument.model_validate(
            {"artifacts": [_artifact(), _artifact()]}
        )


def test_raw_digest_declaration_is_bound_to_member_and_session_policy() -> None:
    count, commitment = ordered_raw_part_commitment(["b" * 64, "c" * 64])
    registered = CollectionUploadArtifactBatchDocument.model_validate(
        {
            "artifacts": [
                {
                    **_artifact(byte_count="65537"),
                    "raw_parts": {
                        "part_plaintext_bytes": "65536",
                        "part_count": str(count),
                        "ordered_sha256": commitment,
                    },
                }
            ]
        }
    )
    assert (
        validate_collection_upload_batch_against_registration_constraints(
            registered, _constraints(pack_member_bytes=1)
        )
        is registered
    )
    with pytest.raises(ValueError, match="only valid for large member"):
        validate_collection_upload_batch_against_registration_constraints(
            registered, _constraints(pack_member_bytes=65538)
        )


def test_custody_receipt_binds_exact_member_and_recovering_objects() -> None:
    receipt = CollectionUploadArtifactCustodyReceiptDocument.seal(
        collection_id=42,
        artifact_id=ArtifactId(FIRST),
        bytes=123,
        sha256="a" * 64,
        archive_root_sha256="c" * 64,
        provenance_root_sha256="d" * 64,
        provenance_root_receipt_sha256="e" * 64,
        archive_objects=(
            CollectionUploadCustodyObjectDocument(
                volume_id="segment-" + "0" * 63 + "1",
                sealed_receipt_sha256="b" * 64,
            ),
        ),
    )
    assert receipt.artifact_id == FIRST
    assert (
        CollectionUploadArtifactCustodyReceiptDocument.model_validate_json(
            receipt.model_dump_json()
        )
        == receipt
    )
    artifact = CollectionUploadArtifactIn.model_validate(_artifact(byte_count="123"))
    assert validate_collection_upload_artifact_custody_receipt(42, artifact, receipt) is receipt
    for collection_id, changed in (
        (43, artifact),
        (42, artifact.model_copy(update={"artifact_id": ArtifactId(SECOND)})),
        (42, artifact.model_copy(update={"bytes": 124})),
    ):
        with pytest.raises(ValueError, match="upload member identity"):
            validate_collection_upload_artifact_custody_receipt(collection_id, changed, receipt)


def test_provenance_status_and_binding_select_exact_journal_anchor() -> None:
    anchor = {
        "journal_id": JOURNAL,
        "through": {"entry_id": ENTRY, "sequence": "0", "json_sha256": "a" * 64},
        "prefix_sha256": "b" * 64,
        "prefix_bytes": "123",
    }
    sealed = CollectionUploadProvenanceJournalStatusDocument.model_validate(
        {
            "journal_id": JOURNAL,
            "state": "sealed",
            "bytes": "123",
            "sha256": "b" * 64,
            "accepted_bytes": "123",
            "anchor": anchor,
        }
    )
    assert sealed.anchor is not None and sealed.anchor.through.entry_id == ENTRY
    with pytest.raises(ValidationError, match="exact anchor"):
        CollectionUploadProvenanceJournalStatusDocument.model_validate(
            {**sealed.model_dump(mode="json"), "anchor": None}
        )
    binding = {"artifact_id": FIRST, "journal": anchor, "delivery_association_id": ASSOCIATION}
    batch = CollectionArtifactProvenanceBindingBatchDocument.model_validate({"bindings": [binding]})
    assert batch.bindings[0].artifact_id == FIRST
    with pytest.raises(ValidationError, match="ID ordered"):
        CollectionArtifactProvenanceBindingBatchDocument.model_validate(
            {"bindings": [{**binding, "artifact_id": SECOND}, binding]}
        )


def test_server_planned_work_uses_artifact_ids_and_exact_unit_coverage() -> None:
    assignment = CollectionUploadUnitAssignmentDocument.model_validate(
        {
            "volume": {
                "volume_id": "pack-" + "0" * 64,
                "sequence": "0" * 64,
                "kind": "pack",
            },
            "plan_sha256": "b" * 64,
            "unit": {
                "unit": "0",
                "payload_bytes": "5",
                "plaintext_bytes": "5",
                "sources": [
                    {
                        "artifact_id": FIRST,
                        "offset": "0",
                        "bytes": "5",
                        "artifact_sha256": "a" * 64,
                    }
                ],
                "state": "pending",
            },
        }
    )
    assert assignment.unit.sources[0].artifact_id == FIRST
    batch = CollectionUploadWorkBatchDocument(
        collection_id="7",
        planning_complete=False,
        complete=False,
        committed_payload_bytes="0",
        work=[assignment],
    )
    assert batch.work == [assignment]
    changed = assignment.model_dump(mode="json")
    changed["unit"]["payload_bytes"] = "4"
    with pytest.raises(ValidationError, match="source bytes"):
        CollectionUploadUnitAssignmentDocument.model_validate(changed)
