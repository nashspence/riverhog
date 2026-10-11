from __future__ import annotations

import copy
import hashlib
from collections import Counter

import pytest

from riverhog_provenance import (
    JournalFrame,
    ProvenanceValidationError,
    assertion,
    assertion_reference,
    encode_entry,
    new_id,
    parse_journal,
    reference,
    validate_graph,
    validate_journal,
    validate_journal_chunks,
)
from riverhog_provenance._journal_epochs import EpochFrames
from riverhog_provenance.journal import _entry, _validate_frames


def seed_graph(journal, who, catalog, n):
    initial = validate_journal(journal, catalog=catalog)
    original = initial.states[0]
    states = []
    for _ in range(n):
        state = copy.deepcopy(original)
        state.update(id=new_id(), assertion_id=new_id())
        states.append(state)
    docs = list(parse_journal(journal))
    return append_frame(docs, who, catalog, "assertion", {"assertions": {"states": states}}), states


def append_frame(frames, who, catalog, kind, body):
    document = _entry(
        journal_id=frames[0].document["journal_id"],
        recorder_id=who,
        kind=kind,
        body=body,
        sequence=len(frames),
        previous=frames[-1].reference,
        recorded_at="2026-01-01T00:00:00Z",
    )
    return [*frames, JournalFrame(encode_entry(document, catalog=catalog)[1:-1])]


def edge(who, source, target):
    return assertion("derivation", who, used_state=reference(source["id"], "state"),
                     generated_state=reference(target["id"], "state"), kind="copy")


def encoded(frames):
    return b"".join(frame.encoded for frame in frames)


def validate_counted(raw, catalog):
    with EpochFrames((raw,), catalog) as frames:
        summary = _validate_frames(frames, catalog, True, None, False)
        return summary, dict(frames.counts)


@pytest.mark.parametrize("n", [16, 32, 64, 128])
@pytest.mark.parametrize("reverse", [False, True])
def test_complete_transcript_certificate_visits_edges_once(journal, who, catalog, n, reverse):
    frames, states = seed_graph(journal, who, catalog, n + 1)
    positions = list(range(n))
    if reverse:
        positions.reverse()
    for i in positions:
        frames = append_frame(frames, who, catalog, "assertion", {
            "assertions": {"relations": [edge(who, states[i], states[i+1])]}})
    raw = encoded(frames)
    result, counts = validate_counted(raw, catalog)
    assert result.journal_sha256 == hashlib.sha256(raw).hexdigest()
    assert counts["epochs"] == 1
    assert counts["rank_edges"] == n
    assert counts["rank_vertices"] == n + 1
    assert counts["certificate_checks"] == n
    assert counts["staged_frames"] == len(frames)
    assert len(result.graph["relations"]) == n
    assert validate_graph(result.graph, catalog=catalog).objects == result.graph_validation.objects


def test_correction_can_reverse_an_edge_without_global_union_cycle(journal, who, catalog):
    frames, states = seed_graph(journal, who, catalog, 2)
    first = edge(who, *states)
    frames = append_frame(frames, who, catalog, "assertion", {"assertions": {"relations": [first]}})
    summary = validate_journal(encoded(frames), catalog=catalog)
    second = edge(who, *reversed(states))
    frames = append_frame(frames, who, catalog, "correction", {
        "retracts": [assertion_reference(summary, first["assertion_id"])],
        "reason": "corrected direction", "assertions": {"relations": [second]}})
    result, counts = validate_counted(encoded(frames), catalog)
    assert counts["epochs"] == 2
    assert result.graph["relations"] == [second]
    assert first["assertion_id"] in result.retracted_assertion_ids


def test_invalid_cycle_is_not_forgiven_by_later_correction(journal, who, catalog):
    frames, states = seed_graph(journal, who, catalog, 2)
    first, second = edge(who, *states), edge(who, *reversed(states))
    frames = append_frame(frames, who, catalog, "assertion", {"assertions": {"relations": [first]}})
    valid = validate_journal(encoded(frames), catalog=catalog)
    retract = assertion_reference(valid, first["assertion_id"])
    frames = append_frame(frames, who, catalog, "assertion", {"assertions": {"relations": [second]}})
    frames = append_frame(frames, who, catalog, "correction", {"retracts": [retract], "reason": "too late"})
    with pytest.raises(ProvenanceValidationError, match="cycle"):
        validate_journal(encoded(frames), catalog=catalog)


def test_same_graph_edge_two_assertions_then_retract_only_one(journal, who, catalog):
    frames, states = seed_graph(journal, who, catalog, 2)
    a, b = edge(who, *states), edge(who, *states)
    frames = append_frame(frames, who, catalog, "assertion", {"assertions": {"relations": [a, b]}})
    old = validate_journal(encoded(frames), catalog=catalog)
    frames = append_frame(frames, who, catalog, "correction", {
        "retracts": [assertion_reference(old, a["assertion_id"])], "reason": "one retired"})
    result = validate_journal(encoded(frames), catalog=catalog)
    assert result.graph["relations"] == [b]
    frames = append_frame(frames, who, catalog, "assertion", {
        "assertions": {"relations": [edge(who, *reversed(states))]}})
    with pytest.raises(ProvenanceValidationError, match="cycle"):
        validate_journal(encoded(frames), catalog=catalog)


def test_late_provider_revocation_never_returns_a_certificate_result(journal, catalog):
    def revoked():
        yield journal
        raise PermissionError("current provider revoked")
    with pytest.raises(PermissionError, match="revoked"):
        validate_journal_chunks(revoked(), catalog=catalog)


def test_future_rank_does_not_supply_future_local_reference(journal, who, catalog):
    frames, states = seed_graph(journal, who, catalog, 2)
    unknown = copy.deepcopy(states[0])
    unknown.update(id=new_id(), assertion_id=new_id())
    frames = append_frame(frames, who, catalog, "assertion", {
        "assertions": {"relations": [edge(who, unknown, states[1])]}})
    frames = append_frame(frames, who, catalog, "assertion", {"assertions": {"states": [unknown]}})
    with pytest.raises(ProvenanceValidationError, match="unresolved local"):
        validate_journal(encoded(frames), catalog=catalog)


def test_cyclic_epoch_reports_prior_identity_error_first(journal, who, catalog):
    frames, states = seed_graph(journal, who, catalog, 2)
    repeated = copy.deepcopy(states[0])
    frames = append_frame(frames, who, catalog, "assertion", {"assertions": {"states": [repeated]}})
    frames = append_frame(frames, who, catalog, "assertion", {"assertions": {"relations": [
        edge(who, *states), edge(who, *reversed(states))]}})
    with pytest.raises(ProvenanceValidationError, match="cannot be reused"):
        validate_journal(encoded(frames), catalog=catalog)


def test_semantic_failure_closes_speculative_scratch(journal, who, catalog, monkeypatch):
    from riverhog_provenance import _journal_epochs
    created = []
    original = _journal_epochs.TemporaryDirectory
    def capture(*a, **kw):
        item = original(*a, **kw)
        created.append(item.name)
        return item
    monkeypatch.setattr(_journal_epochs, "TemporaryDirectory", capture)
    raw = bytearray(journal)
    raw[20] = ord("!")
    with pytest.raises((ProvenanceValidationError, ValueError)):
        validate_journal(bytes(raw), catalog=catalog)
    from pathlib import Path
    assert created and all(not Path(p).exists() for p in created)


def activity_frame(frames, who, catalog):
    activity = copy.deepcopy(frames[0].document["body"]["assertions"]["activities"][0])
    activity.update(id=new_id(), assertion_id=new_id(), kind="transformation")
    activity.pop("started_at", None)
    activity.pop("ended_at", None)
    return append_frame(frames, who, catalog, "assertion", {
        "assertions": {"activities": [activity]}}), activity


def timed_event(kind, who, state, activity, second):
    return assertion(kind, who, state=reference(state["id"], "state"),
                     activity_id=activity["id"], at=f"2026-01-01T00:00:{second:02d}Z",
                     **({"role": "urn:test:input"} if kind == "usage" else {}))


@pytest.mark.parametrize("kind,at,message", [
    ("generation", 20, "before known generation"),
    ("invalidation", 5, "after known invalidation"),
])
def test_late_lifecycle_bound_checks_prior_usages(journal, who, catalog, kind, at, message):
    frames, states = seed_graph(journal, who, catalog, 1)
    frames, activity = activity_frame(frames, who, catalog)
    frames = append_frame(frames, who, catalog, "assertion", {"assertions": {"relations": [
        timed_event("usage", who, states[0], activity, 10)]}})
    frames = append_frame(frames, who, catalog, "assertion", {"assertions": {"relations": [
        timed_event(kind, who, states[0], activity, at)]}})
    with pytest.raises(ProvenanceValidationError, match=message):
        validate_journal(encoded(frames), catalog=catalog)


def test_retract_extreme_usage_rebuilds_temporal_bounds(journal, who, catalog):
    frames, states = seed_graph(journal, who, catalog, 1)
    frames, activity = activity_frame(frames, who, catalog)
    early = timed_event("usage", who, states[0], activity, 1)
    remaining = timed_event("usage", who, states[0], activity, 20)
    frames = append_frame(frames, who, catalog, "assertion", {
        "assertions": {"relations": [early, remaining]}})
    old = validate_journal(encoded(frames), catalog=catalog)
    generated = timed_event("generation", who, states[0], activity, 10)
    frames = append_frame(frames, who, catalog, "correction", {
        "retracts": [assertion_reference(old, early["assertion_id"])],
        "reason": "withdraw earliest usage", "assertions": {"relations": [generated]}})
    result = validate_journal(encoded(frames), catalog=catalog)
    assert len(result.graph["relations"]) == 2
    validate_graph(result.graph, catalog=catalog)


@pytest.mark.parametrize("n", [32, 64, 128])
def test_shared_state_usage_decodes_are_linear(journal, who, catalog, monkeypatch, n):
    from riverhog_provenance import journal_store
    frames, states = seed_graph(journal, who, catalog, 1)
    frames, activity = activity_frame(frames, who, catalog)
    for i in range(n):
        frames = append_frame(frames, who, catalog, "assertion", {"assertions": {"relations": [
            timed_event("usage", who, states[0], activity, 10 + i % 10)]}})
    original = journal_store.json.loads
    decoded = Counter()
    def counted(raw, *a, **kw):
        result = original(raw, *a, **kw)
        if isinstance(result, dict) and result.get("type") == "usage":
            decoded["usage"] += 1
        return result
    monkeypatch.setattr(journal_store.json, "loads", counted)
    result = validate_journal(encoded(frames), catalog=catalog)
    assert decoded["usage"] <= 6 * n
    assert len(result.graph["relations"]) == n


def test_touched_reset_uses_a_nonpartial_index():
    from riverhog_provenance.journal_store import JournalStore
    store = JournalStore()
    try:
        plan = list(store.db.execute("EXPLAIN QUERY PLAN UPDATE assertions SET touched=0 WHERE touched=1"))
        assert any("SEARCH assertions USING INDEX changes" in row[3] for row in plan)
        assert not any("SCAN assertions" in row[3] for row in plan)
    finally:
        store.close()
