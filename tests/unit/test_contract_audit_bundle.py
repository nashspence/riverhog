from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest
import rfc8785

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

import contract_audit_bundle as audit  # noqa: E402

ARTIFACT = REPO_ROOT / "qualification/contracts/riverhog-v1.json"


def test_semantic_facts_round_trip_json_types_outside_rfc8785_number_forms() -> None:
    logical = {
        "large": 2**63 - 1,
        "integral_float": 100.0,
        "nested": [{"value": "unchanged"}],
    }

    facts = audit.semantic_facts(logical, scope="contract")

    assert audit.reassemble_facts(json.loads(json.dumps(facts))) == logical
    assert rfc8785.dumps(facts)


def test_candidate_routes_allow_corroboration_but_fail_closed_on_ambiguity() -> None:
    facts = [{"id": "fact:one", "kind": "value", "pointer": "", "value": 1}]
    candidates = [
        {
            "id": "candidate:openapi",
            "kind": "http",
            "detector": "http-openapi",
            "disposition": "contractual",
            "fact_id": "fact:one",
        },
        {
            "id": "candidate:client",
            "kind": "python",
            "detector": "python-export",
            "disposition": "contractual",
            "fact_id": "fact:one",
        },
        {
            "id": "candidate:launcher",
            "kind": "console-script",
            "detector": "cli-tree",
            "disposition": "excluded",
            "exclusion_policy": "process-launcher-not-cli/v1",
        },
    ]

    audit.validate_candidate_routes(facts, candidates)
    with pytest.raises(audit.AuditBundleError, match="identities are not unique"):
        audit.validate_candidate_routes(facts, [candidates[0], candidates[0]])
    with pytest.raises(audit.AuditBundleError, match="unknown fact"):
        audit.validate_candidate_routes(
            facts,
            [{**candidates[0], "fact_id": "fact:absent"}],
        )
    with pytest.raises(audit.AuditBundleError, match="ambiguous disposition"):
        audit.validate_candidate_routes(
            facts,
            [{**candidates[2], "fact_id": "fact:one"}, candidates[0]],
        )


def test_semantic_identity_changes_with_facts_but_not_context_routing() -> None:
    policies = list(audit.CONTRACT_POLICIES)
    facts = audit.semantic_facts({"surface": {"value": 1}}, scope="contract")

    def identity(current: list[dict[str, object]]) -> str:
        return hashlib.sha256(
            rfc8785.dumps(
                {
                    "schema": audit.CONTRACT_IDENTITY_SCHEMA,
                    "policies": policies,
                    "facts": sorted(current, key=lambda item: str(item["id"])),
                }
            )
        ).hexdigest()

    original = identity(facts)
    assert original == identity(list(reversed(facts)))
    changed = json.loads(json.dumps(facts))
    value_fact = next(item for item in changed if item["kind"] == "value")
    value_fact["value"]["surface"]["value"] = 2
    assert identity(changed) != original


def test_checked_bundle_rejects_a_stale_unreferenced_context(tmp_path: Path) -> None:
    bundle = audit.load_bundle(ARTIFACT)
    root = tmp_path / ARTIFACT.name
    root.write_bytes(ARTIFACT.read_bytes())
    for relative, payload in bundle.files.items():
        destination = tmp_path / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)
    stale = tmp_path / "riverhog-v1/contexts/stale.json"
    stale.write_text("{}", encoding="utf-8")

    with pytest.raises(audit.AuditBundleError, match="stale or unreferenced"):
        audit.load_bundle(root)


def test_checked_bundle_rejects_context_metadata_and_path_escape() -> None:
    checked = audit.load_bundle(ARTIFACT)
    root = json.loads(json.dumps(checked.root))
    first = audit.context_descriptors(root)[0]
    first["owner"] = "wrong-owner"
    root["contexts"][0] = [first[column] for column in root["context_columns"]]

    with pytest.raises(audit.AuditBundleError, match="differs from its root descriptor"):
        audit.validate_bundle(audit.AuditBundle(root=root, files=checked.files))

    escaped_root = json.loads(json.dumps(checked.root))
    escaped_root["context_directory"] = "../outside"
    with pytest.raises(audit.AuditBundleError, match="safe relative directory"):
        audit.validate_bundle(audit.AuditBundle(root=escaped_root, files=checked.files))


def test_checked_bundle_rejects_incomplete_candidate_trace() -> None:
    checked = audit.load_bundle(ARTIFACT)
    files = dict(checked.files)
    descriptor = next(
        context
        for context in audit.context_descriptors(checked.root)
        if "contractual" in context["dispositions"]
    )
    trace_relative = f"{checked.root['context_directory']}/{descriptor['trace']['path']}"
    trace = json.loads(files[trace_relative])
    trace["candidate_sources"] = trace["candidate_sources"][1:]
    files[trace_relative] = audit.canonical_bytes(trace)

    root = json.loads(json.dumps(checked.root))
    rows = []
    for current in audit.context_descriptors(root):
        if current["id"] == descriptor["id"]:
            current["trace"]["bytes"] = len(files[trace_relative])
            current["trace"]["sha256"] = hashlib.sha256(files[trace_relative]).hexdigest()
        rows.append([current.get(column) for column in root["context_columns"]])
    root["contexts"] = rows

    with pytest.raises(
        audit.AuditBundleError, match="candidate and candidate-trace coverage differ"
    ):
        audit.validate_bundle(audit.AuditBundle(root=root, files=files))
