from __future__ import annotations

import copy

import pytest
from riverhog_provenance import (
    ProvenanceValidationError,
    UnresolvedContractError,
    assertion,
    byte_string,
    content_description,
    evidence,
    new_id,
    reference,
    validate_graph,
)
from riverhog_provenance_contracts import PROFILE


def reported(graph, who, *, payload=b"other", state_id=None):
    if state_id is None:
        row = assertion(
            "state",
            who,
            occurrence_id=graph["occurrences"][0]["id"],
            extent={"kind": "unknown", "reason": "historical boundary unavailable"},
        )
        graph["states"].append(row)
        state_id = row["id"]
    description = assertion(
        "reported_description",
        who,
        state=reference(state_id, "state"),
        evidence_items=[evidence(who, "imported_record")],
        content=content_description(payload),
    )
    graph["descriptions"].append(description)
    return state_id, description


def test_first_known_observation_does_not_imply_an_origin(graph, catalog):
    result = validate_graph(graph, catalog=catalog)
    assert "relations" not in result.graph
    assert "locator_bindings" not in result.graph
    assert "contexts" not in result.graph


def test_historical_entity_requires_neither_content_path_nor_capture(who, catalog):
    agent = assertion("agent", who, object_id=who, kind="software", name="archive-reporter")
    artifact = assertion("artifact", who, continuity_policy_uri="urn:test:continuity")
    occurrence = assertion("occurrence", who, artifact_id=artifact["id"], kind="opaque")
    state = assertion(
        "state",
        who,
        occurrence_id=occurrence["id"],
        extent={"kind": "unknown", "reason": "not retained"},
    )
    description = assertion(
        "reported_description",
        who,
        state=reference(state["id"], "state"),
        evidence_items=[
            evidence(who, "attestation", note="A predecessor was reported; bytes unavailable.")
        ],
    )
    graph = {
        "agents": [agent],
        "artifacts": [artifact],
        "occurrences": [occurrence],
        "states": [state],
        "descriptions": [description],
    }
    validate_graph(graph, catalog=catalog)
    assert "activities" not in graph


def test_observation_and_state_and_assertion_are_different_identities(graph, catalog):
    row = graph["descriptions"][0]
    state = graph["states"][0]
    assert len({row["id"], row["assertion_id"], state["id"], state["assertion_id"]}) == 4
    bad = copy.deepcopy(graph)
    bad["descriptions"][0]["assertion_id"] = state["id"]
    with pytest.raises(ProvenanceValidationError):
        validate_graph(bad, catalog=catalog)


@pytest.mark.parametrize("field", ["locator", "filesystem_metadata", "path", "current_state_id"])
def test_removed_core_fields_are_not_compatibility_aliases(field, graph, catalog):
    graph["states"][0][field] = "not-admitted"
    with pytest.raises(ProvenanceValidationError):
        validate_graph(graph, catalog=catalog)


def test_two_states_can_have_identical_primary_bytes_without_collapsing(graph, who, catalog):
    original = graph["descriptions"][0]
    _, desc = reported(graph, who, payload=b"opaque primary bytes\x00\xff")
    compare = assertion(
        "content_comparison",
        who,
        left_description=reference(original["id"], "observation"),
        right_description=reference(desc["id"], "reported_description"),
        result="matching_fixity",
        algorithm="sha-256",
    )
    graph["relations"] = [compare]
    value = validate_graph(graph, catalog=catalog)
    assert len(value.graph["states"]) == 2
    assert all(r["type"] != "specialization" for r in value.graph["relations"])


def test_conflicting_reported_content_is_retained_as_a_finding(graph, who, catalog):
    reported(graph, who, payload=b"conflicting", state_id=graph["states"][0]["id"])
    result = validate_graph(graph, catalog=catalog)
    assert any("conflicting primary-content" in f for f in result.findings)
    assert len(result.graph["descriptions"]) == 2


def test_capture_does_not_generate_subject_state(graph, who, catalog):
    graph["relations"] = [
        assertion(
            "generation",
            who,
            activity_id=graph["activities"][0]["id"],
            state=reference(graph["states"][0]["id"], "state"),
        )
    ]
    with pytest.raises(ProvenanceValidationError, match="does not generate"):
        validate_graph(graph, catalog=catalog)


def test_unknown_process_derivation_does_not_require_a_fabricated_activity(graph, who, catalog):
    second, _ = reported(graph, who)
    graph["relations"] = [
        assertion(
            "derivation",
            who,
            used_state=reference(graph["states"][0]["id"], "state"),
            generated_state=reference(second, "state"),
            kind="transformation",
            evidence_items=[
                evidence(who, "attestation", note="Derivative relationship was reported.")
            ],
        )
    ]
    validate_graph(graph, catalog=catalog)
    assert len(graph["activities"]) == 1


def test_derivation_cycle_rejected_without_using_array_order(graph, who, catalog):
    second, _ = reported(graph, who)
    first = graph["states"][0]["id"]
    graph["relations"] = [
        assertion(
            "derivation",
            who,
            used_state=reference(a, "state"),
            generated_state=reference(b, "state"),
            kind="copy",
        )
        for a, b in [(first, second), (second, first)]
    ]
    with pytest.raises(ProvenanceValidationError, match="cycle"):
        validate_graph(graph, catalog=catalog)


def test_unresolved_local_reference_is_not_treated_as_a_historical_entity(graph, catalog):
    graph["states"][0]["occurrence_id"] = new_id()
    with pytest.raises(ProvenanceValidationError, match="unresolved local"):
        validate_graph(graph, catalog=catalog)


def test_type_confused_references_fail(graph, catalog):
    graph["descriptions"][0]["state"] = reference(graph["agents"][0]["id"], "agent")
    with pytest.raises(ProvenanceValidationError):
        validate_graph(graph, catalog=catalog)


@pytest.mark.parametrize(
    "stamp",
    [
        "2026-02-30T00:00:00Z",
        "2026-13-01T00:00:00Z",
        "2026-01-01T24:00:00Z",
        "2026-01-01T00:00:60Z",
        "2026-01-01T00:00:00+00:00",
    ],
)
def test_impossible_or_noncanonical_instants_fail(stamp, graph, catalog):
    graph["activities"][0]["started_at"] = stamp
    with pytest.raises(ProvenanceValidationError):
        validate_graph(graph, catalog=catalog)


def test_nanosecond_order_is_not_rounded_to_postgres_microseconds(graph, catalog):
    graph["activities"][0]["started_at"] = "2026-01-01T00:00:00.000000002Z"
    graph["activities"][0]["ended_at"] = "2026-01-01T00:00:00.000000001Z"
    with pytest.raises(ProvenanceValidationError, match="ends before"):
        validate_graph(graph, catalog=catalog)


def test_raw_native_bytes_are_not_dynamic_json_keys(graph, who, catalog):
    category = "urn:test:coverage:xattrs"
    graph["descriptions"][0]["capabilities"]["native_metadata"] = True
    graph["descriptions"][0]["metadata"] = [
        {
            "profile_id": PROFILE,
            "category": category,
            "name": {
                "kind": "bytes",
                "bytes": byte_string(b"\xffname"),
                "encoding": "opaque-octets",
            },
            "source": {
                "interface": "urn:test:xattr-api",
                "field": {"kind": "text", "text": "value"},
            },
            "status": "captured",
            "value": {"type": "bytes", "value": byte_string(b"\x00\xff")},
            "observed_byte_length": "2",
            "sensitivity": "unknown",
        }
    ]
    graph["descriptions"][0]["coverage"].append(
        {"profile_id": PROFILE, "category": category, "status": "complete"}
    )
    validate_graph(graph, catalog=catalog)
    graph["descriptions"][0]["metadata"][0]["value"]["value"]["byte_length"] = "3"
    with pytest.raises(ProvenanceValidationError, match="byte length"):
        validate_graph(graph, catalog=catalog)


def test_unknown_profile_is_explicitly_unverified_not_silently_valid(graph, catalog):
    graph["descriptions"][0]["profiles"] = [
        {
            "profile": {
                "contract_id": "unknown",
                "contract_sha256": "0" * 64,
                "schema_id": "urn:test:unknown-schema",
            },
            "data": {"opaque": "value"},
        }
    ]
    with pytest.raises(UnresolvedContractError):
        validate_graph(graph, catalog=catalog)
    partial = validate_graph(graph, catalog=catalog, require_profiles=False)
    assert len(partial.unresolved_profiles) == 1 and not partial.profiles_verified


def test_partial_coverage_cannot_claim_capture_success(graph, catalog):
    graph["descriptions"][0]["coverage"].append(
        {
            "profile_id": "urn:test:profile",
            "category": "urn:test:metadata",
            "status": "partial",
            "reason": "permission denied",
        }
    )
    with pytest.raises(ProvenanceValidationError, match="partial capture"):
        validate_graph(graph, catalog=catalog)
    graph["activities"][0]["outcome"] = "partial"
    validate_graph(graph, catalog=catalog)


def test_known_address_requires_a_qualified_binding(graph, catalog):
    graph["descriptions"][0]["address_status"] = "known"
    with pytest.raises(ProvenanceValidationError, match="requires an observed locator"):
        validate_graph(graph, catalog=catalog)


def test_locators_are_plural_contextual_and_do_not_change_state(graph, who, catalog):
    context = assertion("context", who, kind="object_service", label="flat key namespace")
    graph["contexts"] = [context]
    graph["locator_bindings"] = [
        assertion(
            "locator_binding",
            who,
            target=reference(graph["states"][0]["id"], "state"),
            context_id=context["id"],
            locator={"kind": "object_key", "key": {"kind": "text", "text": key}},
            temporal_scope={"kind": "unknown", "reason": "historical binding time not retained"},
        )
        for key in ["literal/slashes/are/not/a/path", "other-key"]
    ]
    result = validate_graph(graph, catalog=catalog)
    assert len(result.graph["states"]) == 1 and len(result.graph["locator_bindings"]) == 2


def test_filesystem_path_is_only_valid_in_filesystem_namespace(graph, who, catalog):
    context = assertion("context", who, kind="object_service")
    graph["contexts"] = [context]
    graph["locator_bindings"] = [
        assertion(
            "locator_binding",
            who,
            target=reference(graph["states"][0]["id"], "state"),
            context_id=context["id"],
            locator={
                "kind": "filesystem_path",
                "syntax": "posix",
                "form": "absolute",
                "name": {"kind": "text", "text": "/wrong-context"},
            },
            temporal_scope={"kind": "unknown", "reason": "reported"},
        )
    ]
    with pytest.raises(ProvenanceValidationError, match="filesystem naming"):
        validate_graph(graph, catalog=catalog)


def test_ending_one_binding_does_not_invalidate_artifact(graph, who, catalog):
    context = assertion("context", who, kind="repository")
    binding = assertion(
        "locator_binding",
        who,
        target=reference(graph["states"][0]["id"], "state"),
        context_id=context["id"],
        locator={"kind": "repository_id", "identifier": {"kind": "text", "text": "item-1"}},
        temporal_scope={"kind": "instant", "at": "2026-01-01T00:00:00Z"},
    )
    end = assertion("locator_binding_end", who, binding_id=binding["id"], at="2026-02-01T00:00:00Z")
    graph["contexts"] = [context]
    graph["locator_bindings"] = [binding, end]
    validate_graph(graph, catalog=catalog)
    assert "relations" not in graph


def test_custody_and_availability_are_occurrence_scoped(graph, who, catalog):
    occurrence = graph["occurrences"][0]["id"]
    graph["custody_assertions"] = [
        assertion(
            "custody_assertion",
            who,
            target=reference(occurrence, "occurrence"),
            custodian_agent_id=who,
            role="urn:test:archival-custodian",
            temporal_scope={"kind": "unknown", "reason": "start unknown"},
        )
    ]
    graph["availability_assertions"] = [
        assertion(
            "availability_assertion",
            who,
            occurrence=reference(occurrence, "occurrence"),
            status="unavailable",
            temporal_scope={"kind": "instant", "at": "2026-09-01T00:00:00Z"},
        )
    ]
    validate_graph(graph, catalog=catalog)
    assert "relations" not in graph
