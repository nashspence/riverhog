"""Riverhog's exact canonical collection-production profile pack."""

from __future__ import annotations

from functools import lru_cache
from typing import Any

from riverhog_provenance_contracts import ProvenanceContractBinding

COLLECTION_PRODUCTION_CONTRACT_ID = (
    "https://nashspence.github.io/riverhog/v1/provenance/profiles/collection-production"
)
COLLECTION_MEMBER_ROLE = COLLECTION_PRODUCTION_CONTRACT_ID + "/member"
COLLECTION_MEMBER_HISTORY_ROLE = COLLECTION_PRODUCTION_CONTRACT_ID + "/member-history"
PRODUCER_SCHEMA_ID = COLLECTION_PRODUCTION_CONTRACT_ID + "/producer.json"
RECORD_MANIFEST_SCHEMA_ID = COLLECTION_PRODUCTION_CONTRACT_ID + "/record-manifest.json"
RECORD_FRAGMENT_SCHEMA_ID = COLLECTION_PRODUCTION_CONTRACT_ID + "/record-fragment.json"

_TEXT = {"type": "string", "minLength": 1}
_DIGEST = {"type": "string", "pattern": "^[0-9a-f]{64}$"}
_COUNT = {"type": "string", "pattern": "^(0|[1-9][0-9]*)$"}
_POSITIVE = {"type": "string", "pattern": "^[1-9][0-9]*$"}


def _schema(schema_id: str, properties: dict[str, Any]) -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": schema_id,
        "type": "object",
        "additionalProperties": False,
        "properties": properties,
        "required": list(properties),
    }


@lru_cache(maxsize=1)
def collection_production_contract() -> ProvenanceContractBinding:
    """Pin producer attribution and exact required-record fragments as one pack."""

    producer = _schema(
        PRODUCER_SCHEMA_ID,
        {
            "construction_identity": _DIGEST,
            "producer_app": _TEXT,
            "adapter_id": _TEXT,
            "adapter_version": _TEXT,
            "source_event_id": _TEXT,
            "ingest_source": _TEXT,
            "source_context_sha256": _DIGEST,
            "source_context_bytes": _COUNT,
        },
    )
    manifest = _schema(
        RECORD_MANIFEST_SCHEMA_ID,
        {
            "record_kind": _TEXT,
            "record_sha256": _DIGEST,
            "total_bytes": _COUNT,
            "part_count": _POSITIVE,
        },
    )
    fragment = _schema(
        RECORD_FRAGMENT_SCHEMA_ID,
        {
            "record_kind": _TEXT,
            "record_sha256": _DIGEST,
            "total_bytes": _COUNT,
            "offset": _COUNT,
            "data_base64": {"type": "string", "minLength": 1},
        },
    )
    return ProvenanceContractBinding(
        contract_id=COLLECTION_PRODUCTION_CONTRACT_ID,
        schemas=(producer, manifest, fragment),
    )


def collection_production_profile(schema_id: str, data: dict[str, Any]) -> dict[str, object]:
    binding = collection_production_contract()
    if schema_id not in binding.schemas:
        raise ValueError("unknown collection production profile schema")
    return {
        "profile": {
            "contract_id": binding.contract_id,
            "contract_sha256": binding.contract_sha256,
            "schema_id": schema_id,
        },
        "data": data,
    }


CONTRACT_BINDING = collection_production_contract()


__all__ = [
    "CONTRACT_BINDING",
    "COLLECTION_MEMBER_HISTORY_ROLE",
    "COLLECTION_MEMBER_ROLE",
    "COLLECTION_PRODUCTION_CONTRACT_ID",
    "PRODUCER_SCHEMA_ID",
    "RECORD_FRAGMENT_SCHEMA_ID",
    "RECORD_MANIFEST_SCHEMA_ID",
    "collection_production_contract",
    "collection_production_profile",
]
