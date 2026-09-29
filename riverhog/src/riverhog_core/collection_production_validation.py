"""Validate exact required producer records retained inside canonical assertions."""

from __future__ import annotations

import base64
import binascii
import hashlib
from collections import defaultdict
from collections.abc import Mapping
from typing import Any

from riverhog_protocol.collection_production_provenance import (
    COLLECTION_PRODUCTION_CONTRACT_ID,
    COLLECTION_RECORD_FRAGMENT_BYTES_MAX,
    PRODUCER_SCHEMA_ID,
    RECORD_FRAGMENT_SCHEMA_ID,
    RECORD_MANIFEST_SCHEMA_ID,
    collection_production_contract,
)
from riverhog_provenance import ProvenanceValidationError

_PROPERTIES = {
    COLLECTION_PRODUCTION_CONTRACT_ID + "/producer": PRODUCER_SCHEMA_ID,
    COLLECTION_PRODUCTION_CONTRACT_ID + "/record-manifest": RECORD_MANIFEST_SCHEMA_ID,
    COLLECTION_PRODUCTION_CONTRACT_ID + "/record-fragment": RECORD_FRAGMENT_SCHEMA_ID,
}


def validate_collection_production_records(
    graph: Mapping[str, Any], *, delivery_context_id: str
) -> None:
    """Reject missing, incomplete, duplicated or altered required preimage fragments.

    Only effective extensions on the selected delivery context are relevant.
    Exact profile schema validation is done by canonical journal admission; this
    selected creation-contract validator checks the relationships between those
    individually valid assertions.
    """

    selected: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    subject = {
        "scope": "local",
        "object_id": delivery_context_id,
        "object_type": "context",
    }
    contract = collection_production_contract()
    for row in graph.get("extensions", ()):
        schema_id = _PROPERTIES.get(row["property"])
        if schema_id is None or row["subject"] != subject:
            continue
        value = row["value"]
        if value["type"] != "json":
            raise ProvenanceValidationError("producer record is not a canonical profile value")
        profile_value = value["value"]
        if profile_value["profile"] != {
            "contract_id": contract.contract_id,
            "contract_sha256": contract.contract_sha256,
            "schema_id": schema_id,
        }:
            raise ProvenanceValidationError("producer record profile does not match its property")
        selected[schema_id].append(profile_value["data"])

    producers = selected[PRODUCER_SCHEMA_ID]
    if not producers:
        raise ProvenanceValidationError("primary member journal lacks producer attribution")
    manifests: dict[tuple[str, str], Mapping[str, Any]] = {}
    for manifest in selected[RECORD_MANIFEST_SCHEMA_ID]:
        key = manifest["record_kind"], manifest["record_sha256"]
        if key in manifests:
            raise ProvenanceValidationError("producer record manifest is duplicated")
        manifests[key] = manifest
    fragments: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for fragment in selected[RECORD_FRAGMENT_SCHEMA_ID]:
        fragments[fragment["record_kind"], fragment["record_sha256"]].append(fragment)
    if set(fragments) != set(manifests):
        raise ProvenanceValidationError("producer fragments and manifests differ")

    source_records = {
        (producer["source_context_sha256"], producer["source_context_bytes"])
        for producer in producers
    }
    admitted_source_records = {
        (manifest["record_sha256"], manifest["total_bytes"])
        for manifest in manifests.values()
        if manifest["record_kind"] == "source-context"
    }
    if source_records != admitted_source_records:
        raise ProvenanceValidationError("producer source context has no exact record manifest")

    for key, manifest in manifests.items():
        parts = fragments[key]
        if len(parts) != int(manifest["part_count"]):
            raise ProvenanceValidationError("producer record part count differs")
        digest = hashlib.sha256()
        length = 0
        for part in sorted(parts, key=lambda item: int(item["offset"])):
            if int(part["offset"]) != length or part["total_bytes"] != manifest["total_bytes"]:
                raise ProvenanceValidationError("producer record fragments are not contiguous")
            try:
                data = base64.b64decode(part["data_base64"], validate=True)
            except (binascii.Error, ValueError) as exc:
                raise ProvenanceValidationError("producer fragment is not base64") from exc
            if (
                not data
                or len(data) > COLLECTION_RECORD_FRAGMENT_BYTES_MAX
                or base64.b64encode(data).decode("ascii") != part["data_base64"]
            ):
                raise ProvenanceValidationError("producer fragment exceeds its exact byte contract")
            digest.update(data)
            length += len(data)
        if (
            length != int(manifest["total_bytes"])
            or digest.hexdigest() != manifest["record_sha256"]
        ):
            raise ProvenanceValidationError("producer record preimage differs from its manifest")


__all__ = ["validate_collection_production_records"]
