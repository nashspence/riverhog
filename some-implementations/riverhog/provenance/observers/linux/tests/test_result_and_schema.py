from __future__ import annotations

import copy
import hashlib
from pathlib import Path

import pytest
from a_riverhog_linux_provenance_contract_lib import CONTRACT_BINDING
from a_riverhog_linux_provenance_observer import LinuxProvenanceObserver
from riverhog_provenance import (
    create_journal,
    parse_journal,
    validate_entry_document,
    validate_graph,
    validate_graph_fragment,
    validate_journal,
)
from riverhog_provenance_contracts import ContractCatalog, canonical_document


def _result(tmp_path: Path, urn_factory):
    payload = tmp_path / "file"
    payload.write_bytes(b"abc")
    observer = LinuxProvenanceObserver()
    return observer.observe(observer.source(payload, host_id=urn_factory()))


def test_native_graph_is_pinned_and_journaled(tmp_path: Path, urn_factory) -> None:
    result = _result(tmp_path, urn_factory)
    catalog = ContractCatalog((CONTRACT_BINDING,))
    graph = result.graph_fragment()
    validate_graph_fragment(graph)
    validate_graph(graph, catalog=catalog)
    raw = create_journal(graph, recorded_by_agent_id=result.observer_agent_id, catalog=catalog)
    frame = parse_journal(raw)[0]
    validate_entry_document(frame.document)
    assert frame.document["body"]["assertions"] == graph
    assert validate_journal(raw, catalog=catalog).journal_id == frame.document["journal_id"]
    assert canonical_document(frame.document) == frame.json_bytes


def test_observer_does_not_claim_payload_format(tmp_path: Path, urn_factory) -> None:
    payload = tmp_path / "misleading.jpg"
    payload.write_bytes(b"not actually a JPEG")
    observer = LinuxProvenanceObserver()
    result = observer.observe(observer.source(payload, host_id=urn_factory()))
    assert set(result.observation["content"]) == {"size_bytes", "digests"}
    assert "format" not in result.observation
    assert "format" not in result.artifact


def test_pinned_native_profile_rejects_modified_contract_digest(
    tmp_path: Path, urn_factory
) -> None:
    result = _result(tmp_path, urn_factory)
    graph = copy.deepcopy(result.graph_fragment())
    profile = graph["descriptions"][0]["profiles"][0]
    assert profile["profile"]["contract_sha256"] == CONTRACT_BINDING.contract_sha256
    profile["profile"]["contract_sha256"] = hashlib.sha256(b"wrong").hexdigest()
    with pytest.raises(ValueError, match="profile|contract"):
        validate_graph(graph, catalog=ContractCatalog((CONTRACT_BINDING,)))
