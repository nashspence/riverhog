"""Riverhog's exact canonical collection-production profile pack."""

from __future__ import annotations

from collections.abc import Mapping
from functools import lru_cache
from typing import Any

from riverhog_provenance_contracts import ProvenanceContractBinding

from riverhog_protocol.collection_completion import CollectionCompletionRequirementDocument

COLLECTION_PRODUCTION_CONTRACT_ID = (
    "https://nashspence.github.io/riverhog/v1/provenance/profiles/collection-production"
)
COLLECTION_MEMBER_ROLE = COLLECTION_PRODUCTION_CONTRACT_ID + "/member"
COLLECTION_MEMBER_HISTORY_ROLE = COLLECTION_PRODUCTION_CONTRACT_ID + "/member-history"
PRODUCER_SCHEMA_ID = COLLECTION_PRODUCTION_CONTRACT_ID + "/producer.json"
RECORD_MANIFEST_SCHEMA_ID = COLLECTION_PRODUCTION_CONTRACT_ID + "/record-manifest.json"
RECORD_FRAGMENT_SCHEMA_ID = COLLECTION_PRODUCTION_CONTRACT_ID + "/record-fragment.json"
COMPLETION_REQUIREMENT_SCHEMA_ID = COLLECTION_PRODUCTION_CONTRACT_ID + "/required-completion.json"
EXECUTION_COMPLETION_SCHEMA_ID = COLLECTION_PRODUCTION_CONTRACT_ID + "/execution-completion.json"
EXECUTION_OUTPUT_SCHEMA_ID = COLLECTION_PRODUCTION_CONTRACT_ID + "/execution-output.json"
COLLECTION_RECORD_FRAGMENT_BYTES_MAX = 128 * 1024

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
    requirement = _schema(
        COMPLETION_REQUIREMENT_SCHEMA_ID,
        {
            "execution_id": _DIGEST,
            "execution_envelope_sha256": _DIGEST,
            "controller_evidence_sha256": _DIGEST,
            "record_kinds": {"type": "array", "minItems": 1, "uniqueItems": True, "items": _TEXT},
        },
    )
    completion = _schema(
        EXECUTION_COMPLETION_SCHEMA_ID,
        {
            "requirement_sha256": _DIGEST,
            "execution_id": _DIGEST,
            "execution_sha256": _DIGEST,
            "output_bindings_sha256": _DIGEST,
            "input_history_bindings_sha256": _DIGEST,
            "disposition_set_sha256": _DIGEST,
        },
    )
    output = _schema(EXECUTION_OUTPUT_SCHEMA_ID, {"execution_id": _DIGEST, "output_id": _TEXT})
    return ProvenanceContractBinding(
        contract_id=COLLECTION_PRODUCTION_CONTRACT_ID,
        schemas=(producer, manifest, fragment, requirement, completion, output),
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
    "COLLECTION_RECORD_FRAGMENT_BYTES_MAX",
    "COLLECTION_MEMBER_HISTORY_ROLE",
    "COLLECTION_MEMBER_ROLE",
    "COLLECTION_PRODUCTION_CONTRACT_ID",
    "PRODUCER_SCHEMA_ID",
    "RECORD_FRAGMENT_SCHEMA_ID",
    "RECORD_MANIFEST_SCHEMA_ID",
    "COMPLETION_REQUIREMENT_SCHEMA_ID",
    "EXECUTION_COMPLETION_SCHEMA_ID",
    "EXECUTION_OUTPUT_SCHEMA_ID",
    "collection_production_contract",
    "collection_production_profile",
]


def validate_member_completion_requirement(
    graph: Mapping[str, Any],
    *,
    delivery_context_id: str,
    state_id: str,
    requirement: CollectionCompletionRequirementDocument | None,
) -> str | None:
    """Check the accepted obligation and output key at the immutable early primary."""

    context = {"scope": "local", "object_id": delivery_context_id, "object_type": "context"}
    state = {"scope": "local", "object_id": state_id, "object_type": "state"}
    selected: list[Mapping[str, Any]] = []
    outputs: list[Mapping[str, Any]] = []
    contract = collection_production_contract()
    for row in graph.get("extensions", ()):
        expected_schema = None
        target = None
        if (
            row["property"] == COLLECTION_PRODUCTION_CONTRACT_ID + "/required-completion"
            and row["subject"] == context
        ):
            expected_schema, target = COMPLETION_REQUIREMENT_SCHEMA_ID, selected
        elif (
            row["property"] == COLLECTION_PRODUCTION_CONTRACT_ID + "/execution-output"
            and row["subject"] == state
        ):
            expected_schema, target = EXECUTION_OUTPUT_SCHEMA_ID, outputs
        if target is None:
            continue
        if row["value"]["type"] != "json":
            raise ValueError("execution declaration is not a profile value")
        value = row["value"]["value"]
        if value["profile"] != {
            "contract_id": contract.contract_id,
            "contract_sha256": contract.contract_sha256,
            "schema_id": expected_schema,
        }:
            raise ValueError("execution declaration profile differs")
        target.append(value["data"])
    if requirement is None:
        if selected or outputs:
            raise ValueError("primary declares an unaccepted completion obligation")
        return None
    if selected != [requirement.model_dump(mode="json")]:
        raise ValueError("primary omits or substitutes the accepted completion requirement")
    if len(outputs) != 1 or outputs[0]["execution_id"] != requirement.execution_id:
        raise ValueError("primary lacks its exact accepted execution output key")
    return str(outputs[0]["output_id"])
