from __future__ import annotations

import hashlib

import pytest
from riverhog_core.canonical_discovery_rows import core_edges, iter_index_assertions
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    JournalFrame,
    JournalSummary,
    append_checkpoint,
    append_correction,
    assertion,
    assertion_reference,
    create_journal,
    reference,
    validate_journal,
)


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


@pytest.mark.growth_witness(
    "riverhog-discovery-source-header-reuse/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
)
def test_native_discovery_reuses_exact_entry_headers_without_per_assertion_hashing(
    monkeypatch, record_property
):
    digest = JournalFrame.sha256.fget
    document = JournalFrame.document.fget
    anchor = JournalSummary.anchor_at
    totals = {
        "assertions_returned": 0,
        "journal_octets": 0,
        "source_digest_calls": 0,
        "source_digest_octets": 0,
        "frame_decodes": 0,
        "source_header_calls": 0,
    }
    for count, width in ((16, 0), (64, 0), (256, 0), (16, 4096), (64, 4096)):
        observed = BoundedSourceObserver().observe(BytesSource(b"opaque"))
        graph = observed.graph_fragment()
        graph["extensions"] = [
            assertion(
                "extension",
                observed.observer_agent_id,
                subject=reference(observed.artifact_id, "artifact"),
                property="urn:test:source-binding-work",
                value={"type": "text", "value": "x" * width + str(index)},
            )
            for index in range(count)
        ]
        raw = create_journal(graph, recorded_by_agent_id=observed.observer_agent_id)
        with validate_journal(raw) as summary:
            counts = {key: 0 for key in totals}

            def observed_digest(frame, observations=counts):
                observations["source_digest_calls"] += 1
                observations["source_digest_octets"] += len(frame.json_bytes)
                return digest(frame)

            def observed_document(frame, observations=counts):
                observations["frame_decodes"] += 1
                return document(frame)

            def observed_anchor(current, sequence, observations=counts):
                observations["source_header_calls"] += 1
                return anchor(current, sequence)

            with monkeypatch.context() as patch:
                patch.setattr(JournalFrame, "sha256", property(observed_digest))
                patch.setattr(JournalFrame, "document", property(observed_document))
                patch.setattr(JournalSummary, "anchor_at", observed_anchor)
                rows = tuple(iter_index_assertions(summary))
            assert len(rows) == count + 6
            assert {row.entry_sha256 for row in rows} == {hashlib.sha256(raw[1:-1]).hexdigest()}
            assert counts["source_header_calls"] == counts["frame_decodes"] == 1
            assert counts["source_digest_calls"] == counts["source_digest_octets"] == 0
            counts["assertions_returned"] = len(rows)
            counts["journal_octets"] = len(raw)
            for key, value in counts.items():
                totals[key] += value
    record_property("growth_observations", totals)


def test_discovery_entry_headers_keep_original_support_after_correction_and_checkpoint():
    observed = BoundedSourceObserver().observe(BytesSource(b"opaque"))
    graph = observed.graph_fragment()
    old = assertion(
        "extension",
        observed.observer_agent_id,
        subject=reference(observed.artifact_id, "artifact"),
        property="urn:test:source-binding-work",
        value={"type": "text", "value": "original"},
    )
    replacement = assertion(
        "extension",
        observed.observer_agent_id,
        subject=reference(observed.artifact_id, "artifact"),
        property="urn:test:source-binding-work",
        value={"type": "text", "value": "corrected"},
    )
    graph["extensions"] = [old]
    raw = create_journal(graph, recorded_by_agent_id=observed.observer_agent_id)
    with validate_journal(raw) as initial:
        raw = append_correction(
            raw,
            [assertion_reference(initial, old["assertion_id"])],
            reason="updated documentary claim",
            recorded_by_agent_id=observed.observer_agent_id,
            assertions={"extensions": [replacement]},
        )
    raw = append_checkpoint(
        raw, recorded_by_agent_id=observed.observer_agent_id, purpose="urn:test:source-header"
    )
    with validate_journal(raw) as summary:
        expected = {
            frame.document["id"]: hashlib.sha256(frame.json_bytes).hexdigest()
            for frame in summary.frames
        }
        rows = tuple(iter_index_assertions(summary))
        assert len(summary.frames) == 3
        assert all(row.entry_sha256 == expected[row.entry_id] for row in rows)
        selected = {row.assertion_id: row for row in rows}
        assert selected[old["assertion_id"]].assertion_state == "retracted"
        assert selected[old["assertion_id"]].entry_sequence == 0
        assert selected[replacement["assertion_id"]].assertion_state == "effective"
        assert selected[replacement["assertion_id"]].entry_sequence == 1
