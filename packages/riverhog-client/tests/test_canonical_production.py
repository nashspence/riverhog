from __future__ import annotations

import base64
import hashlib
from unittest.mock import Mock

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_client.canonical_production import (
    ProducerAttribution,
    bind_produced_member,
    build_member_journal,
)
from riverhog_protocol import ArtifactMemberIdentityDocument
from riverhog_protocol.collection_production_provenance import (
    COLLECTION_PRODUCTION_CONTRACT_ID,
    collection_production_contract,
)
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    create_journal,
    external_reference,
    validate_journal,
    validate_journal_set,
)
from riverhog_provenance_contracts import ContractCatalog, require_canonical_uuid_urn


def test_member_journal_preserves_exact_source_record_and_delivered_hint() -> None:
    content = b"same bytes, independent opaque member"
    member = ArtifactMemberIdentityDocument.model_validate(
        {
            "artifact_id": "a3" * 32,
            "bytes": str(len(content)),
            "sha256": hashlib.sha256(content).hexdigest(),
        }
    )
    source_context = {"native": "\u00e9" * 70000, "observed": None}
    delivery_context_id = "urn:uuid:ba123103-fd14-4813-b2d9-b83859e36f31"
    produced = build_member_journal(
        member=member,
        observation=BoundedSourceObserver().observe(BytesSource(content)),
        delivery_context_id=delivery_context_id,
        attribution=ProducerAttribution(
            producer_app="example-adapter",
            adapter_id="example",
            adapter_version="v1",
            source_event_id="event-1",
            ingest_source="example",
            source_context=source_context,
            construction_identity="f2" * 32,
        ),
        materialization_hint=("camera", "clip.mkv"),
    )
    require_canonical_uuid_urn(produced.journal_id, "journal")
    summary = validate_journal(
        produced.content, catalog=ContractCatalog((collection_production_contract(),))
    )
    assert produced.binding.journal.model_dump(mode="json") == summary.anchor
    assert produced.binding.artifact_id == member.artifact_id
    assert produced.binding.delivery_association_id in summary.graph_validation.objects

    source_bytes = canonical_json_bytes(source_context)
    fragments: dict[int, bytes] = {}
    for row in summary.graph["extensions"]:
        if row["property"] != COLLECTION_PRODUCTION_CONTRACT_ID + "/record-fragment":
            continue
        data = row["value"]["value"]["data"]
        assert data["record_sha256"] == hashlib.sha256(source_bytes).hexdigest()
        fragments[int(data["offset"])] = base64.b64decode(data["data_base64"], validate=True)
    assert len(fragments) == 2
    assert b"".join(value for _, value in sorted(fragments.items())) == source_bytes


def test_member_journal_rejects_unmeasured_payload() -> None:
    content = b"measured"
    member = ArtifactMemberIdentityDocument.model_validate(
        {"artifact_id": "bf" * 32, "bytes": str(len(content)), "sha256": "0" * 64}
    )
    with pytest.raises(ValueError, match="differs from registered member bytes"):
        build_member_journal(
            member=member,
            observation=BoundedSourceObserver().observe(BytesSource(content)),
            delivery_context_id="urn:uuid:ba123103-fd14-4813-b2d9-b83859e36f31",
            attribution=ProducerAttribution(
                "example", "example", "v1", "event-1", "example", {}, "f2" * 32
            ),
            materialization_hint=None,
        )


def test_derived_member_primary_journal_carries_exact_source_causality() -> None:
    source_observation = BoundedSourceObserver().observe(BytesSource(b"source"))
    source_raw = create_journal(
        source_observation.graph_fragment(),
        recorded_by_agent_id=source_observation.observer_agent_id,
    )
    source = external_reference(validate_journal(source_raw), source_observation.state_id)
    content = b"derived"
    produced = build_member_journal(
        member=ArtifactMemberIdentityDocument.model_validate(
            {
                "artifact_id": "c4" * 32,
                "bytes": str(len(content)),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
        ),
        observation=BoundedSourceObserver().observe(BytesSource(content)),
        delivery_context_id="urn:uuid:ba123103-fd14-4813-b2d9-b83859e36f31",
        attribution=ProducerAttribution(
            "example", "example", "v1", "event-1", "example", {}, "f2" * 32
        ),
        materialization_hint=None,
        causal_input_states=(source,),
    )
    catalog = ContractCatalog((collection_production_contract(),))
    summary = validate_journal(produced.content, catalog=catalog)
    assert any(row["kind"] == "transformation" for row in summary.graph["activities"])
    assert len([row for row in summary.graph["relations"] if row["type"] == "usage"]) == 1
    assert len([row for row in summary.graph["relations"] if row["type"] == "generation"]) == 1
    derivation = next(row for row in summary.graph["relations"] if row["type"] == "derivation")
    assert derivation["used_state"] == source
    assert derivation["generated_state"]["object_type"] == "state"
    validate_journal_set(
        (source_raw, produced.content), catalog=catalog, require_all_references=True
    )


def test_publication_requires_explicit_hint_or_omission_before_network() -> None:
    content = b"target output"
    member = ArtifactMemberIdentityDocument.model_validate(
        {
            "artifact_id": "bf" * 32,
            "bytes": str(len(content)),
            "sha256": hashlib.sha256(content).hexdigest(),
        }
    )
    observed = BoundedSourceObserver().observe(BytesSource(content))
    attribution = ProducerAttribution(
        "example", "example", "v1", "event-1", "example", {}, "f2" * 32
    )
    api = Mock()
    with pytest.raises(ValueError, match="exactly one"):
        bind_produced_member(
            api,
            collection_id=12,
            member=member,
            observation=observed,
            delivery_context_id="urn:uuid:ba123103-fd14-4813-b2d9-b83859e36f31",
            attribution=attribution,
            materialization_hint=None,
            allow_missing_materialization_hint=False,
        )
    api.assert_not_called()
    assert not api.mock_calls

    produced = bind_produced_member(
        api,
        collection_id=12,
        member=member,
        observation=observed,
        delivery_context_id="urn:uuid:ba123103-fd14-4813-b2d9-b83859e36f31",
        attribution=attribution,
        materialization_hint=("output", "clip.mkv"),
        allow_missing_materialization_hint=False,
    )
    assert [call[0] for call in api.mock_calls] == [
        "upload_collection_upload_session_provenance_journal",
        "bind_collection_upload_session_artifact_provenance",
        "set_collection_upload_session_materialization_decisions",
    ]
    assert api.mock_calls[1].args[1].bindings == [produced.binding]
    assert api.mock_calls[2].args[1].decisions[0].materialization_hint.components == [
        "output",
        "clip.mkv",
    ]
