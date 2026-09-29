from __future__ import annotations

import base64
import hashlib
import uuid
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
from riverhog_provenance import BoundedSourceObserver, BytesSource, validate_journal
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


@pytest.mark.skipif(__import__("sys").platform != "linux", reason="Linux native source")
def test_native_source_contract_survives_producer_journal(tmp_path) -> None:
    from a_riverhog_linux_provenance_contract_lib import CONTRACT_BINDING
    from a_riverhog_linux_provenance_observer import LinuxProvenanceObserver

    content = b"native source evidence"
    source = tmp_path / "source.bin"
    source.write_bytes(content)
    observer = LinuxProvenanceObserver()
    observation = observer.observe(observer.source(source, host_id=f"urn:uuid:{uuid.uuid4()}"))
    member = ArtifactMemberIdentityDocument.model_validate(
        {
            "artifact_id": "b7" * 32,
            "bytes": str(len(content)),
            "sha256": hashlib.sha256(content).hexdigest(),
        }
    )
    produced = build_member_journal(
        member=member,
        observation=observation,
        delivery_context_id=f"urn:uuid:{uuid.uuid4()}",
        attribution=ProducerAttribution(
            "example", "linux-file", "v1", "event-1", "test", {}, "a1" * 32
        ),
        materialization_hint=("source.bin",),
    )
    summary = validate_journal(
        produced.content,
        catalog=ContractCatalog((CONTRACT_BINDING, collection_production_contract())),
    )
    assert summary.graph["descriptions"][0]["profiles"][0]["profile"]["contract_sha256"] == (
        CONTRACT_BINDING.contract_sha256
    )
    assert produced.binding.artifact_id == member.artifact_id
