"""Generic collection-delivery effect contract owned by the rclone target."""

from __future__ import annotations

from typing import cast

from pydantic import JsonValue
from riverhog_canonical_json import scalar_schema
from stove0_protocol import JSON_SCHEMA_ONLY_SEMANTIC_PROFILE, JsonSchemaValidationProfile
from stove0_target_protocol import (
    InputArtifactContract,
    OperationContract,
    OperationContractPayload,
)

RCLONE_DELIVER_OPERATION_ID = "stove0.rclone.deliver/v1"

_INTENT_SCHEMA = JsonSchemaValidationProfile.from_schema(
    "stove0.rclone.deliver-intent/v1",
    {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": False,
    },
)

RCLONE_RECEIPT_SCHEMA = JsonSchemaValidationProfile.from_schema(
    "stove0.rclone.delivery-receipt/v1",
    {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": [
            "format",
            "destination_identity",
            "delivery_id",
            "source_selection_sha256",
            "manifest_sha256",
            "artifact_count",
            "total_bytes",
            "verification",
        ],
        "properties": {
            "format": {"const": "stove0-rclone-delivery-receipt/v1"},
            "destination_identity": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
            "delivery_id": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
            "source_selection_sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
            "manifest_sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
            "artifact_count": {"type": "integer", "minimum": 1},
            "total_bytes": cast(dict[str, JsonValue], scalar_schema("nonnegative")),
            "verification": {"const": "rclone-download-check-and-manifest-readback/v1"},
        },
        "additionalProperties": False,
    },
)

RCLONE_DELIVER_OPERATION = OperationContract.seal(
    OperationContractPayload(
        id=RCLONE_DELIVER_OPERATION_ID,
        result_kind="external-effect",
        intent_schema=_INTENT_SCHEMA,
        intent_semantics=JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
        inputs=(InputArtifactContract(role="*", minimum=1),),
        effect_receipt_schema=RCLONE_RECEIPT_SCHEMA,
        source_collection_retirement_permitted=True,
    )
)

__all__ = ["RCLONE_DELIVER_OPERATION", "RCLONE_DELIVER_OPERATION_ID", "RCLONE_RECEIPT_SCHEMA"]
