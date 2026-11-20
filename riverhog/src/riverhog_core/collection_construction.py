"""Canonical construction identity shared by upload execution and registration."""

from __future__ import annotations

import json

from riverhog_canonical_json import format_scalar
from riverhog_protocol.pack_ingress import canonical_json_bytes

from riverhog_core.catalog_models import CollectionUploadRecord
from riverhog_core.incremental_plan import (
    incremental_volume_planner_checkpoint_payload,
    parse_incremental_volume_planner_checkpoint,
)


def upload_construction_fields(upload: CollectionUploadRecord) -> dict[str, object]:
    return {
        "collection_id": format_scalar("sequence63", upload.collection_id),
        "creation_identity": upload.creation_identity_sha256,
        "idempotency_key": upload.idempotency_key,
        "delivery_context_id": upload.delivery_context_id,
        "archive_generation": upload.archive_generation,
        "archive_store": upload.archive_store,
        "archive_incarnation_id": upload.archive_incarnation_id,
        "archive_storage_prefix": upload.archive_storage_prefix,
        "encryption_format": upload.encryption_format,
        "passphrase_id": upload.passphrase_id,
        "principal_id": upload.initiated_by_principal_id,
        "initial_key_id": upload.initiated_by_key_id,
        "initial_tag_set_identity": upload.initial_tag_set_identity,
        "ingest_source": upload.ingest_source,
        "event_context": json.loads(upload.event_context_json or "null"),
        "custody_mode": upload.custody_mode,
        "use_cache": upload.use_cache,
        "copy_to": json.loads(upload.copy_to_json),
    }


def upload_construction(upload: CollectionUploadRecord) -> str:
    if not upload.planner_checkpoint_json:
        raise RuntimeError("collection upload planner checkpoint is missing")
    checkpoint = parse_incremental_volume_planner_checkpoint(upload.planner_checkpoint_json)
    return canonical_json_bytes(
        {
            **upload_construction_fields(upload),
            "policy": incremental_volume_planner_checkpoint_payload(checkpoint)["policy"],
        }
    ).decode("utf-8")
