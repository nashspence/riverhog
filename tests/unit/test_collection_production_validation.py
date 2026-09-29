from __future__ import annotations

import copy
import hashlib

import pytest
from riverhog_client.canonical_production import ProducerAttribution, build_member_journal
from riverhog_core.collection_production_validation import validate_collection_production_records
from riverhog_protocol import ArtifactMemberIdentityDocument
from riverhog_protocol.collection_production_provenance import (
    COLLECTION_PRODUCTION_CONTRACT_ID,
    collection_production_contract,
)
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    ProvenanceValidationError,
    validate_journal,
)
from riverhog_provenance_contracts import ContractCatalog

_CONTEXT_ID = "urn:uuid:ba123103-fd14-4813-b2d9-b83859e36f31"


def _graph() -> dict:
    content = b"observed payload"
    produced = build_member_journal(
        member=ArtifactMemberIdentityDocument.model_validate(
            {
                "artifact_id": "a3" * 32,
                "bytes": str(len(content)),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
        ),
        observation=BoundedSourceObserver().observe(BytesSource(content)),
        delivery_context_id=_CONTEXT_ID,
        attribution=ProducerAttribution(
            "source-adapter", "test", "v1", "event", "test", {"native": "x" * 140000}, "f2" * 32
        ),
        materialization_hint=None,
    )
    return validate_journal(
        produced.content, catalog=ContractCatalog((collection_production_contract(),))
    ).graph


def _records(graph: dict, suffix: str) -> list[dict]:
    return [
        row["value"]["value"]["data"]
        for row in graph["extensions"]
        if row["property"] == COLLECTION_PRODUCTION_CONTRACT_ID + suffix
    ]


def test_producer_preimage_is_verified_across_bounded_fragments() -> None:
    validate_collection_production_records(_graph(), delivery_context_id=_CONTEXT_ID)


@pytest.mark.parametrize(
    "mutation",
    ["missing-fragment", "part-count", "gap", "content", "producer-digest", "duplicate-manifest"],
)
def test_producer_preimage_corruption_is_rejected(mutation: str) -> None:
    graph = copy.deepcopy(_graph())
    fragments = _records(graph, "/record-fragment")
    manifest = _records(graph, "/record-manifest")[0]
    producer = _records(graph, "/producer")[0]
    if mutation == "missing-fragment":
        graph["extensions"] = [
            row
            for row in graph["extensions"]
            if not (
                row["property"] == COLLECTION_PRODUCTION_CONTRACT_ID + "/record-fragment"
                and row["value"]["value"]["data"] is fragments[0]
            )
        ]
    elif mutation == "part-count":
        manifest["part_count"] = "3"
    elif mutation == "gap":
        fragments[1]["offset"] = str(int(fragments[1]["offset"]) + 1)
    elif mutation == "content":
        fragments[0]["data_base64"] = "eA=="
    elif mutation == "producer-digest":
        producer["source_context_sha256"] = "0" * 64
    else:
        duplicate = next(
            row
            for row in graph["extensions"]
            if row["property"] == COLLECTION_PRODUCTION_CONTRACT_ID + "/record-manifest"
        )
        graph["extensions"].append(copy.deepcopy(duplicate))
    with pytest.raises(ProvenanceValidationError):
        validate_collection_production_records(graph, delivery_context_id=_CONTEXT_ID)


def test_other_context_does_not_satisfy_producer_obligation() -> None:
    with pytest.raises(ProvenanceValidationError, match="lacks producer attribution"):
        validate_collection_production_records(
            _graph(), delivery_context_id="urn:uuid:04139d90-4f1b-485e-a46a-4a1c96f03563"
        )
