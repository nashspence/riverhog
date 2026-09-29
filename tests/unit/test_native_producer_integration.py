from __future__ import annotations

import hashlib
import sys
import uuid

import pytest
from a_riverhog_linux_provenance_contract_lib import CONTRACT_BINDING
from a_riverhog_linux_provenance_observer import LinuxProvenanceObserver
from riverhog_client.canonical_production import ProducerAttribution, build_member_journal
from riverhog_protocol import ArtifactMemberIdentityDocument
from riverhog_protocol.collection_production_provenance import collection_production_contract
from riverhog_provenance import validate_journal
from riverhog_provenance_contracts import ContractCatalog


@pytest.mark.skipif(sys.platform != "linux", reason="Linux native source")
def test_native_source_contract_survives_producer_journal(tmp_path) -> None:
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
