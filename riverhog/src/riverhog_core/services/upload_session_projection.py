"""Source-owned upload response projection from already observed metadata."""

from __future__ import annotations

import json

from riverhog_canonical_json import format_scalar

from riverhog_core.catalog_models import CollectionUploadRecord
from riverhog_core.collection_plan import CollectionVolumePolicy
from riverhog_core.services.upload_custody_projection import upload_custody_payload


def upload_session_payload(
    upload: CollectionUploadRecord,
    *,
    tag_count: int,
    custodied_files: int,
    custodied_bytes: int,
    uploaded_bytes: int,
    uploaded_units: int,
    total_units: int,
    copy_intents: list[dict[str, object]],
    policy: CollectionVolumePolicy,
    state: str | None = None,
    resumed: bool | None = None,
) -> dict[str, object]:
    """Project existing public fields; no SQL, provider work or authority."""
    files_total = upload.artifact_count
    bytes_total = upload.artifact_bytes
    payload: dict[str, object] = {
        "collection_id": format_scalar("sequence63", upload.collection_id),
        "created_at": upload.opened_at,
        "ingest_source": upload.ingest_source,
        "description": upload.description,
        "description_revision": upload.description_revision,
        "description_identity": upload.description_identity,
        "description_publication": (
            "pending"
            if upload.description_identity is None
            else "not_required"
            if upload.description_revision == 0
            else "current"
        ),
        "tag_revision": upload.tag_revision,
        "tag_set_identity": upload.tag_set_identity,
        "tag_publication": (
            "current" if upload.tag_publication_receipt_json is not None else "pending"
        ),
        "tag_count": tag_count,
        "provenance_identity": None,
        "delivery_context_id": upload.delivery_context_id,
        "construction_identity_sha256": upload.creation_identity_sha256,
        "completion_journal_id": upload.completion_journal_id,
        "completion_receipt": None
        if upload.completion_receipt_json is None
        else json.loads(upload.completion_receipt_json),
        "archive_store": upload.archive_store,
        "use_cache": upload.use_cache,
        "copy_to": json.loads(upload.copy_to_json),
        "copy_intents": copy_intents,
        "encryption_format": upload.encryption_format,
        "passphrase_id": upload.passphrase_id,
        "state": state or upload.state,
        "custody_mode": upload.custody_mode,
        "registration_constraints": {
            "pack_member_bytes": format_scalar("nonnegative", policy.pack_member_bytes),
            "raw_part_plaintext_bytes": format_scalar(
                "nonnegative", policy.raw_part_plaintext_bytes
            ),
        },
        "files_total": int(files_total),
        "bytes_total": int(bytes_total),
        "upload_state_expires_at": None if state == "canceled" else upload.lease_expires_at,
        "custody": upload_custody_payload(
            files=int(files_total),
            byte_count=int(bytes_total),
            custodied_files=custodied_files,
            custodied_bytes=custodied_bytes,
        ),
        "orphaned_at": None if state == "canceled" else upload.orphaned_at,
        "latest_failure": upload.archive_failure,
        "archive_phase": upload.archive_phase,
        "archive_phase_updated_at": upload.archive_phase_updated_at,
        "archive_next_attempt_at": upload.archive_next_attempt_at,
        "archive_storage_prefix": upload.archive_storage_prefix,
        "archive_uploaded_bytes": uploaded_bytes,
        "archive_total_bytes": None,
        "archive_uploaded_units": uploaded_units,
        "archive_total_units": total_units,
        "collection": None,
    }
    if resumed is not None:
        payload["resumed"] = resumed
    return payload
