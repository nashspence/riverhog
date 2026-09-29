from __future__ import annotations

import copy
import hashlib
from dataclasses import replace
from io import BytesIO

import pytest
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    ConcurrentJournalChangeError,
    ObservationRequest,
    ProvenanceValidationError,
    append_assertion_batches,
    append_assertions,
    append_checkpoint,
    append_correction,
    append_observation,
    assertion,
    assertion_reference,
    create_journal,
    encode_entry,
    external_reference,
    new_id,
    ordered_segment_commitment,
    parse_journal,
    reassemble_journal,
    recover_complete_prefix,
    reference,
    segment_journal,
    validate_journal,
    validate_journal_chunks,
    validate_journal_set,
    validate_journal_set_chunks,
    verify_delivery,
)
from riverhog_provenance_contracts import PROFILE, canonical_document


def extension(who, target, value="test"):
    return assertion(
        "extension",
        who,
        subject=reference(target, "artifact"),
        property="urn:test:property",
        value={"type": "text", "value": value},
    )


def appended(journal, who, catalog):
    summary = validate_journal(journal, catalog=catalog)
    row = extension(who, summary.graph["artifacts"][0]["id"])
    return append_assertions(
        journal, {"extensions": [row]}, recorded_by_agent_id=who, catalog=catalog
    ), row


def test_journal_has_no_global_current_path_or_payload(journal, catalog):
    summary = validate_journal(journal, catalog=catalog)
    assert len(summary.states) == 1
    assert summary.delivery_associations == ()
    assert not hasattr(summary, "current_state_id")
    catalog.validate(PROFILE + "/materialized.schema.json", summary.materialize())


@pytest.mark.parametrize("chunk_size", [1, 2, 3, 7, 127, 4096])
def test_streaming_frames_survive_arbitrary_octet_boundaries(journal, catalog, chunk_size):
    chunks = (journal[i : i + chunk_size] for i in range(0, len(journal), chunk_size))
    assert (
        validate_journal_chunks(chunks, catalog=catalog).journal_sha256
        == hashlib.sha256(journal).hexdigest()
    )


def test_journal_set_accepts_bounded_chunk_iterators(journal, catalog):
    chunks = (journal[i : i + 7] for i in range(0, len(journal), 7))
    streamed = validate_journal_set_chunks((chunks,), catalog=catalog)
    direct = validate_journal_set((journal,), catalog=catalog)
    assert streamed.journals[0].anchor == direct.journals[0].anchor


def test_exact_bytes_and_predecessor_hashes_are_preserved(journal, who, catalog):
    result, _ = appended(journal, who, catalog)
    assert result.startswith(journal)
    frames = parse_journal(result)
    assert frames[1].document["previous_entry"] == frames[0].reference
    assert frames[0].sha256 == hashlib.sha256(journal[1:-1]).hexdigest()
    assert len(validate_journal(result, catalog=catalog).frames) == 2


def test_batched_assertions_preserve_exact_chain_and_every_preimage(journal, who, catalog):
    artifact_id = validate_journal(journal, catalog=catalog).graph["artifacts"][0]["id"]
    rows = [extension(who, artifact_id, value=f"fragment-{index}") for index in range(3)]
    result = append_assertion_batches(
        journal,
        ({"extensions": [row]} for row in rows),
        recorded_by_agent_id=who,
        catalog=catalog,
    )
    summary = validate_journal(result, catalog=catalog)
    assert len(summary.frames) == 4
    assert {item["value"]["value"] for item in summary.graph["extensions"]} == {
        f"fragment-{index}" for index in range(3)
    }
    assert [frame.document["previous_entry"] for frame in summary.frames[1:]] == [
        frame.reference for frame in summary.frames[:-1]
    ]


@pytest.mark.parametrize(
    "mutation",
    ["sequence", "digest", "journal", "entry-reuse", "recorder", "timestamp", "assertion-reuse"],
)
def test_entry_graph_and_chain_corruptions_are_rejected(journal, who, catalog, mutation):
    raw, row = appended(journal, who, catalog)
    frames = parse_journal(raw)
    doc = frames[-1].document
    if mutation == "sequence":
        doc["sequence"] = "3"
    elif mutation == "digest":
        doc["previous_entry"]["json_sha256"] = "0" * 64
    elif mutation == "journal":
        doc["journal_id"] = new_id()
    elif mutation == "entry-reuse":
        doc["id"] = frames[0].document["id"]
    elif mutation == "recorder":
        doc["recorded_by_agent_id"] = new_id()
    elif mutation == "timestamp":
        doc["recorded_at"] = "2026-02-30T00:00:00Z"
    else:
        doc["body"]["assertions"]["extensions"][0]["assertion_id"] = frames[0].document["body"][
            "assertions"
        ]["artifacts"][0]["assertion_id"]
    broken = journal + b"\x1e" + canonical_document(doc) + b"\n"
    with pytest.raises(ProvenanceValidationError):
        validate_journal(broken, catalog=catalog)


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"{}\n",
        b"\xef\xbb\xbf\x1e{}\n",
        b"\x1e{}",
        b"\x1e{}\n ",
        b"\x1e{\x1e}\n",
        b'\x1e{"x":1,"x":2}\n',
        b'\x1e{ "x":1}\n',
    ],
)
def test_invalid_framing_or_noncanonical_json_is_rejected(raw, catalog):
    with pytest.raises((ProvenanceValidationError, ValueError)):
        validate_journal(raw, catalog=catalog)


def test_torn_final_frame_requires_explicit_recovery(journal, who, catalog):
    raw, _ = appended(journal, who, catalog)
    torn = raw[:-19]
    with pytest.raises(ProvenanceValidationError):
        validate_journal(torn, catalog=catalog)
    recovered = recover_complete_prefix(torn, catalog=catalog)
    assert recovered.complete_prefix == journal
    assert recovered.incomplete_tail == torn[len(journal) :]


def test_recovery_never_skips_a_malformed_complete_entry(journal, catalog):
    with pytest.raises(ProvenanceValidationError):
        recover_complete_prefix(journal + b'\x1e{"bad":true}\n\x1e{', catalog=catalog)


def test_checkpoint_commits_framed_prefix_not_just_json(journal, who, catalog):
    raw = append_checkpoint(
        journal, recorded_by_agent_id=who, purpose="urn:test:handoff", catalog=catalog
    )
    body = parse_journal(raw)[-1].document["body"]
    assert body["prefix_sha256"] == hashlib.sha256(journal).hexdigest()
    assert body["prefix_bytes"] == str(len(journal))
    assert body["covered_through"] == parse_journal(journal)[-1].reference


def test_checkpoint_tampering_fails(journal, who, catalog):
    raw = append_checkpoint(
        journal, recorded_by_agent_id=who, purpose="urn:test:handoff", catalog=catalog
    )
    doc = parse_journal(raw)[-1].document
    doc["body"]["prefix_bytes"] = "1"
    with pytest.raises(ProvenanceValidationError, match="checkpoint prefix"):
        validate_journal(journal + encode_entry(doc, catalog=catalog), catalog=catalog)


def test_clean_truncation_requires_external_tail_anchor(journal, who, catalog):
    extended, _ = appended(journal, who, catalog)
    anchor = validate_journal(extended, catalog=catalog).anchor
    validate_journal(
        journal, catalog=catalog
    )  # self-consistent but not complete relative to anchor
    with pytest.raises(ProvenanceValidationError, match="expected anchored"):
        validate_journal(journal, catalog=catalog, expected_anchor=anchor)
    earlier = validate_journal(journal, catalog=catalog).anchor
    validate_journal(extended, catalog=catalog, expected_anchor=earlier)
    with pytest.raises(ProvenanceValidationError, match="exact tail"):
        validate_journal(
            extended, catalog=catalog, expected_anchor=earlier, require_exact_tail=True
        )


def test_compare_and_append_rejects_stale_expected_tail(journal, who, catalog):
    longer, _ = appended(journal, who, catalog)
    expected = validate_journal(journal, catalog=catalog).tail.reference
    with pytest.raises(ConcurrentJournalChangeError):
        append_assertions(
            longer,
            {
                "extensions": [
                    extension(
                        who, validate_journal(journal, catalog=catalog).graph["artifacts"][0]["id"]
                    )
                ]
            },
            recorded_by_agent_id=who,
            expected_tail=expected,
            catalog=catalog,
        )


def test_late_recording_does_not_reorder_history_or_require_monotonic_wall_clock(
    journal, who, catalog
):
    value = extension(who, validate_journal(journal, catalog=catalog).graph["artifacts"][0]["id"])
    raw = append_assertions(
        journal,
        {"extensions": [value]},
        recorded_by_agent_id=who,
        recorded_at="2001-01-01T00:00:00Z",
        catalog=catalog,
    )
    assert validate_journal(raw, catalog=catalog).tail.document["sequence"] == "1"


def test_correction_retires_assertion_not_artifact_state(journal, who, catalog):
    old = validate_journal(journal, catalog=catalog)
    agent = old.graph["agents"][0]
    corrected = copy.deepcopy(agent)
    corrected["assertion_id"] = new_id()
    corrected["name"] = "Corrected label"
    raw = append_correction(
        journal,
        [assertion_reference(old, agent["assertion_id"])],
        reason="spelling",
        recorded_by_agent_id=who,
        assertions={"agents": [corrected]},
        catalog=catalog,
    )
    new = validate_journal(raw, catalog=catalog)
    assert agent["assertion_id"] in new.retracted_assertion_ids
    assert [s["id"] for s in new.states] == [s["id"] for s in old.states]
    assert new.graph["agents"][0]["name"] == "Corrected label"
    assert raw.startswith(journal)


def test_correction_cannot_leave_dangling_local_references(journal, who, catalog):
    old = validate_journal(journal, catalog=catalog)
    with pytest.raises(ProvenanceValidationError, match="unresolved local"):
        append_correction(
            journal,
            [assertion_reference(old, old.graph["states"][0]["assertion_id"])],
            reason="must also retire dependents",
            recorded_by_agent_id=who,
            catalog=catalog,
        )


def test_retracted_assertion_id_cannot_be_reused(journal, who, catalog):
    first, row = appended(journal, who, catalog)
    summary = validate_journal(first, catalog=catalog)
    second = append_correction(
        first,
        [assertion_reference(summary, row["assertion_id"])],
        reason="unsupported",
        recorded_by_agent_id=who,
        catalog=catalog,
    )
    with pytest.raises(ProvenanceValidationError, match="cannot be reused"):
        append_assertions(second, {"extensions": [row]}, recorded_by_agent_id=who, catalog=catalog)
    with pytest.raises(ProvenanceValidationError, match="already retired"):
        append_correction(
            second,
            [assertion_reference(summary, row["assertion_id"])],
            reason="again",
            recorded_by_agent_id=who,
            catalog=catalog,
        )


def test_state_identity_cannot_be_redefined_in_a_correction(journal, who, catalog):
    old = validate_journal(journal, catalog=catalog)
    state = old.graph["states"][0]
    new = copy.deepcopy(state)
    new["assertion_id"] = new_id()
    new["extent"] = {"kind": "unknown", "reason": "changed assertion"}
    with pytest.raises(ProvenanceValidationError, match="immutable referent"):
        append_correction(
            journal,
            [assertion_reference(old, state["assertion_id"])],
            reason="not a valid correction",
            recorded_by_agent_id=who,
            assertions={"states": [new]},
            catalog=catalog,
        )


def test_reobservation_same_artifact_and_occurrence_has_new_state(journal, observation, catalog):
    result = BoundedSourceObserver(catalog=catalog).observe(
        BytesSource(b"new primary content"),
        ObservationRequest(artifact=observation.artifact, occurrence=observation.occurrence),
    )
    raw = append_observation(journal, result, catalog=catalog)
    summary = validate_journal(raw, catalog=catalog)
    assert len(summary.states) == 2
    assert len(summary.graph["occurrences"]) == 1
    assert len(summary.graph["artifacts"]) == 1
    assert "relations" not in summary.graph  # an edit is not inferred


def test_external_references_pin_assertion_and_exact_entry(journal, who, catalog):
    first = validate_journal(journal, catalog=catalog)
    other = BoundedSourceObserver(catalog=catalog).observe(BytesSource(b"derivative"))
    g = other.graph_fragment()
    g["relations"] = [
        assertion(
            "derivation",
            who,
            used_state=external_reference(first, first.states[0]["id"]),
            generated_state=reference(other.state_id, "state"),
            kind="transformation",
        )
    ]
    second = create_journal(g, recorded_by_agent_id=who, catalog=catalog)
    assert validate_journal(second, catalog=catalog).graph_validation.external_references
    assert not validate_journal_set([journal, second], catalog=catalog).unresolved_references
    with pytest.raises(ProvenanceValidationError, match="unresolved external"):
        validate_journal_set([second], catalog=catalog)
    assert validate_journal_set(
        [second], catalog=catalog, require_all_references=False
    ).unresolved_references


def test_foreign_comparison_is_rechecked_when_evidence_becomes_available(journal, who, catalog):
    first = validate_journal(journal, catalog=catalog)
    other = BoundedSourceObserver(catalog=catalog).observe(BytesSource(b"different"))
    g = other.graph_fragment()
    g["relations"] = [
        assertion(
            "content_comparison",
            who,
            left_description=external_reference(first, first.graph["descriptions"][0]["id"]),
            right_description=reference(other.observation_id, "observation"),
            result="matching_fixity",
            algorithm="sha-256",
        )
    ]
    second = create_journal(g, recorded_by_agent_id=who, catalog=catalog)
    assert validate_journal(second, catalog=catalog).findings
    with pytest.raises(ProvenanceValidationError, match="foreign content"):
        validate_journal_set([journal, second], catalog=catalog)


def test_independent_writers_must_not_reuse_journal_id(journal, who, catalog):
    left, _ = appended(journal, who, catalog)
    right, _ = appended(journal, who, catalog)
    with pytest.raises(ProvenanceValidationError, match="divergent writers"):
        validate_journal_set([left, right], catalog=catalog)
    assert len(validate_journal_set([journal, left], catalog=catalog).journals) == 1


def test_fork_anchors_a_prefix_and_keeps_an_independent_chain(journal, who, catalog):
    parent = validate_journal(journal, catalog=catalog)
    child = create_journal(
        {"agents": parent.graph["agents"]},
        recorded_by_agent_id=who,
        forked_from=parent.anchor,
        catalog=catalog,
    )
    assert validate_journal(child, catalog=catalog).journal_id != parent.journal_id
    validate_journal_set([journal, child], catalog=catalog)
    with pytest.raises(ProvenanceValidationError, match="fork prefix"):
        validate_journal_set([child], catalog=catalog)


def test_aggregate_detects_cross_journal_derivation_cycle(journal, who, catalog):
    a0 = validate_journal(journal, catalog=catalog)
    b = BoundedSourceObserver(catalog=catalog).observe(BytesSource(b"b"))
    bg = b.graph_fragment()
    bg["relations"] = [
        assertion(
            "derivation",
            who,
            used_state=external_reference(a0, a0.states[0]["id"]),
            generated_state=reference(b.state_id, "state"),
            kind="copy",
        )
    ]
    braw = create_journal(bg, recorded_by_agent_id=who, catalog=catalog)
    b0 = validate_journal(braw, catalog=catalog)
    araw = append_assertions(
        journal,
        {
            "relations": [
                assertion(
                    "derivation",
                    who,
                    used_state=external_reference(b0, b.state_id),
                    generated_state=reference(a0.states[0]["id"], "state"),
                    kind="copy",
                )
            ]
        },
        recorded_by_agent_id=who,
        catalog=catalog,
    )
    with pytest.raises(ProvenanceValidationError, match="cycle"):
        validate_journal_set([araw, braw], catalog=catalog)


def test_segmented_transport_has_no_logical_paths(journal, catalog):
    segments = segment_journal(journal, maximum_segment_bytes=97, catalog=catalog)
    assert len(segments) > 1
    assert reassemble_journal(segments, catalog=catalog) == journal
    assert all("path" not in s.descriptor() for s in segments)
    assert ordered_segment_commitment(
        s.descriptor() for s in segments
    ) != ordered_segment_commitment(s.descriptor() for s in reversed(segments))


@pytest.mark.parametrize("mutation", ["order", "gap", "corrupt", "missing", "identity"])
def test_segment_integrity_is_checked(journal, catalog, mutation):
    segments = list(segment_journal(journal, maximum_segment_bytes=100, catalog=catalog))
    if mutation == "order":
        segments.reverse()
    elif mutation == "gap":
        segments[1] = replace(segments[1], offset=101)
    elif mutation == "corrupt":
        segments[0] = replace(segments[0], content=b"X" + segments[0].content[1:])
    elif mutation == "missing":
        segments.pop()
    else:
        segments[0] = replace(segments[0], object_id=new_id())
    with pytest.raises(ProvenanceValidationError):
        reassemble_journal(segments, catalog=catalog)


def test_delivery_is_scoped_and_verifies_only_primary_bytes(journal, who, catalog):
    old = validate_journal(journal, catalog=catalog)
    context = assertion("context", who, kind="delivery", label="one transfer envelope")
    delivery = assertion(
        "delivery_association",
        who,
        delivery_context_id=context["id"],
        slot={"kind": "text", "text": "opaque slot identifier"},
        role="urn:test:payload",
        state=reference(old.states[0]["id"], "state"),
        verification_observation_id=old.graph["descriptions"][0]["id"],
    )
    raw = append_assertions(
        journal,
        {"contexts": [context], "delivery_associations": [delivery]},
        recorded_by_agent_id=who,
        catalog=catalog,
    )
    result = verify_delivery(
        validate_journal(raw, catalog=catalog),
        delivery["id"],
        BytesIO(b"opaque primary bytes\x00\xff"),
    )
    assert result["scope"] == "primary_bytes_only"
    with pytest.raises(ProvenanceValidationError):
        verify_delivery(
            validate_journal(raw, catalog=catalog),
            delivery["id"],
            BytesIO(b"opaque primary bytes\x00\xfe"),
        )


def test_assertion_identity_is_immutable_across_journals_even_for_labels(journal, who, catalog):
    graph = validate_journal(journal, catalog=catalog).graph
    graph["agents"][0]["name"] = "Different claim under the same assertion identity"
    other = create_journal(graph, recorded_by_agent_id=who, catalog=catalog)
    with pytest.raises(ProvenanceValidationError, match="assertion identity redefined"):
        validate_journal_set([journal, other], catalog=catalog)


def test_verbatim_assertion_replication_is_not_redefinition(journal, who, catalog):
    graph = validate_journal(journal, catalog=catalog).graph
    other = create_journal(graph, recorded_by_agent_id=who, catalog=catalog)
    validate_journal_set([journal, other], catalog=catalog)
