from __future__ import annotations

from riverhog_core.canonical_discovery_rows import core_edges, iter_index_assertions
from riverhog_provenance import BoundedSourceObserver, BytesSource, create_journal, validate_journal


def test_index_rows_retain_exact_entry_and_scalar_support() -> None:
    observed = BoundedSourceObserver().observe(BytesSource(b"opaque"))
    raw = create_journal(observed.graph_fragment(), recorded_by_agent_id=observed.observer_agent_id)
    summary = validate_journal(raw)
    rows = tuple(iter_index_assertions(summary))
    assert {row.kind for row in rows} >= {"artifact", "occurrence", "state", "observation"}
    observation = next(row for row in rows if row.referent_id == observed.observation_id)
    assert observation.entry_id == summary.tail.document["id"]
    assert observation.prefix_sha256 == summary.journal_sha256
    assert observation.assertion_state == "effective"
    assert any(
        posting.pointer == "/content/size_bytes" and posting.value == "6"
        for posting in observation.postings
    )
    assert any(
        edge.role == "state" and edge.target_id == observed.state_id for edge in observation.edges
    )


def test_opaque_profile_lookalikes_never_become_core_edges() -> None:
    subject = "urn:uuid:ec20adea-64fb-4c73-9e85-6a608cb17f14"
    row = {
        "type": "extension",
        "subject": {"scope": "local", "object_type": "context", "object_id": subject},
        "value": {
            "type": "json",
            "value": {
                "profile": {
                    "contract_id": "urn:test",
                    "contract_sha256": "0" * 64,
                    "schema_id": "urn:test",
                },
                "data": {
                    "state": {
                        "scope": "local",
                        "object_type": "state",
                        "object_id": "urn:uuid:b034f5d0-7d39-4b2e-9556-febdf5c74cdc",
                    }
                },
            },
        },
    }
    assert [(edge.role, edge.target_id) for edge in core_edges(row)] == [("subject", subject)]


def test_reference_valued_extension_has_generic_exact_value_edge() -> None:
    subject = "urn:uuid:ec20adea-64fb-4c73-9e85-6a608cb17f14"
    target = "urn:uuid:b034f5d0-7d39-4b2e-9556-febdf5c74cdc"
    row = {
        "type": "extension",
        "subject": {"scope": "local", "object_type": "state", "object_id": subject},
        "property": "urn:test:uninterpreted-predicate",
        "value": {
            "type": "reference",
            "value": {"scope": "local", "object_type": "state", "object_id": target},
        },
    }
    assert [(edge.role, edge.target_id) for edge in core_edges(row)] == [
        ("subject", subject),
        ("value", target),
    ]
