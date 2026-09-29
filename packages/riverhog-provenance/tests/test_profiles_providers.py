from __future__ import annotations

import copy
from types import SimpleNamespace

import pytest
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    ProvenanceObserverBinding,
    ProvenanceValidationError,
    assertion,
    byte_string,
    list_provenance_observers,
    new_id,
    providers,
    resolve_provenance_observer,
    validate_graph,
)
from riverhog_provenance_contracts import PROFILE, core_contract, profile_reference


def profile(name, data):
    return {
        "profile": profile_reference(PROFILE + "/profiles/" + name + ".schema.json"),
        "data": data,
    }


def filesystem_graph(graph, who):
    context = assertion(
        "context",
        who,
        kind="filesystem_namespace",
        profiles=[profile("filesystem-context", {"filesystem_type": "ext4"})],
    )
    execution = assertion(
        "context",
        who,
        kind="execution_environment",
        profiles=[profile("execution-environment", {"operating_system": {"name": "Ubuntu"}})],
    )
    graph["contexts"] = [context, execution]
    graph["occurrences"][0].update(kind="filesystem_object", source_context_id=context["id"])
    graph["descriptions"][0]["source_context_id"] = context["id"]
    graph["activities"][0]["contexts"] = [{"context_id": execution["id"], "role": "execution"}]
    graph["descriptions"][0]["profiles"] = [
        profile(
            "filesystem-observation",
            {
                "object_kind": "regular_file",
                "timestamps": [
                    {
                        "kind": "metadata_changed",
                        "value_status": "exact",
                        "value": "2026-01-01T00:00:00.123456789Z",
                        "resolution_ns": "1",
                        "source": {
                            "interface": "urn:linux:statx",
                            "field": {"kind": "text", "text": "stx_ctime"},
                            "context_id": context["id"],
                        },
                        "raw": {
                            "value": {"type": "integer", "value": "1767225600123456789"},
                            "unit_uri": "urn:unit:nanosecond",
                            "epoch": "1970-01-01T00:00:00Z",
                        },
                    }
                ],
            },
        )
    ]
    return graph


def test_filesystem_profile_preserves_timestamps_without_polluting_core(graph, who, catalog):
    filesystem_graph(graph, who)
    result = validate_graph(graph, catalog=catalog)
    assert "filesystem_metadata" not in result.graph["states"][0]
    assert result.graph["contexts"][0]["kind"] != result.graph["contexts"][1]["kind"]


def test_filesystem_profile_is_not_attached_to_pathless_stream_occurrence(graph, who, catalog):
    filesystem_graph(graph, who)
    graph["occurrences"][0]["kind"] = "stream_emission"
    with pytest.raises(ProvenanceValidationError, match="filesystem occurrence"):
        validate_graph(graph, catalog=catalog)


def test_profile_timestamp_calendar_is_semantically_validated(graph, who, catalog):
    filesystem_graph(graph, who)
    graph["descriptions"][0]["profiles"][0]["data"]["timestamps"][0]["value"] = (
        "2026-02-30T00:00:00Z"
    )
    with pytest.raises(ProvenanceValidationError, match="calendar"):
        validate_graph(graph, catalog=catalog)


def test_context_identifier_authority_must_resolve(graph, who, catalog):
    filesystem_graph(graph, who)
    graph["descriptions"][0]["profiles"][0]["data"]["native_identifiers"] = [
        {
            "scheme": "urn:linux:inode",
            "value": {"kind": "text", "text": "1234"},
            "scope": "context",
            "authority_id": new_id(),
        }
    ]
    with pytest.raises(ProvenanceValidationError, match="unresolved"):
        validate_graph(graph, catalog=catalog)


def test_policy_configuration_cannot_change_without_updating_its_commitment(graph, catalog):
    graph["activities"][0]["configuration"]["data"]["second_content_hash"] = True
    with pytest.raises(ProvenanceValidationError, match="configuration digest"):
        validate_graph(graph, catalog=catalog)


def test_unknown_profiles_are_not_interpreted_using_lookalike_core_keys(graph, catalog):
    graph["descriptions"][0]["profiles"] = [
        {
            "profile": {
                "contract_id": "foreign",
                "contract_sha256": "0" * 64,
                "schema_id": "urn:test:foreign",
            },
            "data": {
                "agent_id": "not-a-core-agent",
                "encoding": "base64",
                "data": "not-base64",
                "byte_length": "incorrect",
            },
        }
    ]
    value = validate_graph(graph, catalog=catalog, require_profiles=False)
    assert not value.profiles_verified


def test_native_names_preserve_binary_evidence_and_ignore_display_for_identity(graph, catalog):
    observation = graph["descriptions"][0]
    observation["capabilities"]["native_metadata"] = True
    raw = {
        "profile_id": PROFILE,
        "category": "urn:test:native",
        "name": {
            "kind": "bytes",
            "bytes": byte_string(b"\xff"),
            "encoding": "octets",
            "display": "replacement",
        },
        "source": {"interface": "urn:test:api", "field": {"kind": "text", "text": "attribute"}},
        "status": "captured",
        "sensitivity": "unknown",
        "value": {"type": "bytes", "value": byte_string(b"abc")},
    }
    observation["metadata"] = [raw]
    observation["coverage"].append(
        {"profile_id": PROFILE, "category": "urn:test:native", "status": "complete"}
    )
    validate_graph(graph, catalog=catalog)
    duplicate = copy.deepcopy(raw)
    duplicate["name"]["display"] = "different rendering"
    observation["metadata"].append(duplicate)
    with pytest.raises(ProvenanceValidationError, match="duplicate source-qualified"):
        validate_graph(graph, catalog=catalog)


def test_noncanonical_base64_padding_bits_are_rejected(graph, catalog):
    observation = graph["descriptions"][0]
    observation["capabilities"]["native_metadata"] = True
    observation["metadata"] = [
        {
            "profile_id": PROFILE,
            "category": "urn:test:native",
            "name": {"kind": "text", "text": "x"},
            "source": {"interface": "urn:test:api", "field": {"kind": "text", "text": "x"}},
            "status": "captured",
            "sensitivity": "unknown",
            "value": {
                "type": "bytes",
                "value": {"encoding": "base64", "data": "YR==", "byte_length": "1"},
            },
        }
    ]
    observation["coverage"].append(
        {"profile_id": PROFILE, "category": "urn:test:native", "status": "complete"}
    )
    with pytest.raises(ProvenanceValidationError, match="padding bits"):
        validate_graph(graph, catalog=catalog)


class EntryPoint:
    def __init__(self, name, value, payload):
        self.name = name
        self.value = value
        self.payload = payload
        self.loads = 0
        self.dist = SimpleNamespace(name="test-distribution", version="0.1.0")

    def load(self):
        self.loads += 1
        if isinstance(self.payload, BaseException):
            raise self.payload
        return self.payload


def configured(monkeypatch):
    contract = core_contract()
    binding = ProvenanceObserverBinding(
        "urn:test:observer",
        "test-contract",
        contract.contract_id,
        contract.contract_sha256,
        BoundedSourceObserver,
    )
    observer = EntryPoint("test-observer", "test:observer", binding)
    other = EntryPoint("other", "other:observer", AssertionError("unselected provider executed"))
    pack = EntryPoint("test-contract", "test:contract", contract)
    monkeypatch.setattr(
        providers,
        "_entry_points",
        lambda group: (pack,) if group == "riverhog.provenance-contracts" else (observer, other),
    )
    return observer, other, pack


def test_listing_provider_metadata_does_not_execute_plugins(monkeypatch):
    entries = configured(monkeypatch)
    assert len(list_provenance_observers()) == 2
    assert all(ep.loads == 0 for ep in entries)


def test_explicit_selection_loads_only_selected_implementation_and_contract(monkeypatch):
    observer, other, pack = configured(monkeypatch)
    selected = resolve_provenance_observer("test-observer")
    result = selected.create().observe(BytesSource(b"no file required"))
    assert (observer.loads, other.loads, pack.loads) == (1, 0, 1)
    detail = result.graph_fragment()["activities"][0]["details"][0]
    assert detail["data"]["provider"] == "test-observer"
    assert detail["data"]["contract"]["contract_sha256"] == core_contract().contract_sha256


def test_provider_pack_digest_mismatch_fails_before_capture(monkeypatch):
    observer, other, pack = configured(monkeypatch)
    observer.payload = ProvenanceObserverBinding(
        "urn:test:observer",
        "test-contract",
        core_contract().contract_id,
        "0" * 64,
        BoundedSourceObserver,
    )
    with pytest.raises(ValueError, match="identities disagree"):
        resolve_provenance_observer("test-observer")


def test_duplicate_installed_provider_names_are_ambiguous(monkeypatch):
    observer, other, pack = configured(monkeypatch)
    monkeypatch.setattr(providers, "_entry_points", lambda group: (observer, observer))
    with pytest.raises(ValueError, match="exactly once"):
        resolve_provenance_observer("test-observer")


def test_result_observer_is_not_selected_by_array_order(graph, who, catalog):
    from riverhog_provenance import ObservationResult

    second_id = new_id()
    graph["agents"].append(
        assertion("agent", who, object_id=second_id, kind="person", name="Operator")
    )
    graph["activities"][0]["associations"].insert(
        0, {"agent_id": second_id, "role": "urn:test:operator"}
    )
    validate_graph(graph, catalog=catalog)
    result = ObservationResult.from_graph(graph, observation_id=graph["descriptions"][0]["id"])
    assert result.observer_agent_id == who
