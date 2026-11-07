from __future__ import annotations

from contextlib import ExitStack, contextmanager
from copy import deepcopy

import pytest
from riverhog_core import canonical_discovery_relevance as owner
from riverhog_protocol.provenance_transport import CollectionArtifactProvenanceBindingDocument
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    JournalSummary,
    ProvenanceValidationError,
    append_assertion_batches,
    append_checkpoint,
    append_correction,
    assertion,
    assertion_reference,
    create_journal,
    reference,
    validate_journal,
)
from riverhog_provenance.journal_store import InvalidJournalWorkspace

from tests.support.member_history import member_history_selection_fixture
from tests.unit.test_member_history_discovery import _members


@pytest.fixture
def corrected_source():
    observed = BoundedSourceObserver().observe(BytesSource(b"exact historical selection"))
    first = create_journal(
        observed.graph_fragment(), recorded_by_agent_id=observed.observer_agent_id
    )
    claim = assertion(
        "extension",
        observed.observer_agent_id,
        subject=reference(observed.state_id, "state"),
        property="urn:test:historical-claim",
        value={"type": "text", "value": "withdrawn at the later tail"},
    )
    historical = append_assertion_batches(
        first, ({"extensions": [claim]},), recorded_by_agent_id=observed.observer_agent_id
    )
    with validate_journal(historical) as selected:
        expected = deepcopy(selected.anchor)
        target = assertion_reference(selected, claim["assertion_id"])
    raw = append_correction(
        historical,
        (target,),
        reason="withdraw claim",
        recorded_by_agent_id=observed.observer_agent_id,
    )
    with validate_journal(raw) as complete:
        yield complete, expected, claim


class SourceReads:
    def __init__(self, actual):
        self.actual, self.validating, self.sequences, self.calls = actual, False, [], 0

    def execute(self, query, parameters=()):
        if query.startswith("SELECT value FROM frames"):
            assert self.validating, "historical selection reread payload before native validation"
            assert parameters and len(parameters) == 1
            self.sequences.append(int(parameters[0]))
        return self.actual.execute(query, parameters)


@contextmanager
def observe_source_reads(current, monkeypatch):
    observed = SourceReads(current._store.db)
    native = owner.validate_journal_chunks

    def validated(chunks, **options):
        observed.calls += 1
        observed.validating = True
        try:
            return native(chunks, **options)
        finally:
            observed.validating = False

    with monkeypatch.context() as patch:
        patch.setattr(current._store._storage, "connection", observed)
        patch.setattr(owner, "validate_journal_chunks", validated)
        yield observed


def test_historical_selection_reads_payload_once_for_its_exact_native_view(
    corrected_source, monkeypatch
):
    complete, expected, claim = corrected_source
    with observe_source_reads(complete, monkeypatch) as observed:
        with owner._anchored_snapshot(complete, expected, catalog=None) as selected:
            assert selected.anchor == expected
            assert claim["assertion_id"] not in selected.retracted_assertion_ids
            assert selected.graph_validation.objects[claim["id"]] == claim
        assert observed.calls == 1
        assert observed.sequences == list(range(int(expected["through"]["sequence"]) + 1))
    assert claim["assertion_id"] in complete.retracted_assertion_ids
    assert complete.anchor != expected


@pytest.mark.parametrize("fault", ["digest", "size", "entry", "sequence", "journal"])
def test_historical_header_mismatch_reads_no_source_payload(corrected_source, monkeypatch, fault):
    complete, expected, _ = corrected_source
    altered = deepcopy(expected)
    if fault == "digest":
        altered["prefix_sha256"] = "0" * 64
    elif fault == "size":
        altered["prefix_bytes"] = str(int(altered["prefix_bytes"]) + 1)
    elif fault == "entry":
        altered["through"]["entry_id"] = "urn:uuid:00000000-0000-4000-8000-000000000000"
    elif fault == "sequence":
        altered["through"]["sequence"] = "10000"
    else:
        altered["journal_id"] = "urn:uuid:00000000-0000-4000-8000-000000000000"
    with observe_source_reads(complete, monkeypatch) as observed:
        with pytest.raises(ProvenanceValidationError, match="prefix anchor"):
            owner._anchored_snapshot(complete, altered, catalog=None)
        assert observed.calls == 0
        assert observed.sequences == []


def test_current_tail_selection_keeps_an_independent_borrow_without_replay(
    corrected_source, monkeypatch
):
    complete, _, _ = corrected_source
    expected = complete.anchor
    with observe_source_reads(complete, monkeypatch) as observed:
        with owner._anchored_snapshot(complete, expected, catalog=None) as selected:
            assert selected.anchor == expected
        assert observed.calls == 0
        assert observed.sequences == []
    assert complete.anchor == expected


def test_private_source_corruption_remains_an_operational_failure(corrected_source, monkeypatch):
    complete, expected, _ = corrected_source

    def corrupted(current, sequence):
        raise InvalidJournalWorkspace("owned source header is corrupt")

    with monkeypatch.context() as patch:
        patch.setattr(JournalSummary, "anchor_at", corrupted)
        with pytest.raises(InvalidJournalWorkspace):
            owner._anchored_snapshot(complete, expected, catalog=None)


def test_external_input_state_uses_its_exact_entry_header_in_a_longer_source(monkeypatch):
    with ExitStack() as stack:
        context, catalog, members = _members()
        for _, _, summary, _ in members:
            stack.enter_context(summary)
        _, _, source, source_state = members[0]
        member, original_binding, primary, primary_state = members[1]
        source_raw = b"".join(frame.encoded for frame in source.frames)
        source_who = source.frames[0].document["recorded_by_agent_id"]
        later = append_checkpoint(
            source_raw,
            recorded_by_agent_id=source_who,
            purpose="urn:test:later-source-tail",
            catalog=catalog,
        )
        foreign = stack.enter_context(validate_journal(later, catalog=catalog))
        primary_who = primary.frames[0].document["recorded_by_agent_id"]
        relation = assertion(
            "derivation",
            primary_who,
            used_state=source_state,
            generated_state=reference(primary_state["object_id"], "state"),
            kind="copy",
        )
        raw = append_assertion_batches(
            b"".join(frame.encoded for frame in primary.frames),
            ({"relations": [relation]},),
            recorded_by_agent_id=primary_who,
            catalog=catalog,
        )
        current = stack.enter_context(validate_journal(raw, catalog=catalog))
        binding = CollectionArtifactProvenanceBindingDocument.model_validate(
            {**original_binding.model_dump(mode="json"), "journal": current.anchor}
        )
        corpus = {current.journal_id: current, foreign.journal_id: foreign}
        selected, closure, _ = member_history_selection_fixture(member, binding, corpus)
        stack.enter_context(closure)
        expected = foreign.anchor_at(int(source_state["entry"]["sequence"]))
        calls = []
        actual = JournalSummary.anchor_at

        def anchored(summary, sequence):
            result = actual(summary, sequence)
            calls.append((summary.journal_id, sequence, deepcopy(result)))
            return result

        with monkeypatch.context() as patch:
            patch.setattr(JournalSummary, "anchor_at", anchored)
            with owner.member_relevance(
                member=member,
                binding=binding,
                primary=current,
                corpus=corpus,
                delivery_context_id=context,
                history_binding=selected,
                closure=closure,
                catalog=catalog,
            ) as relevance:
                assert (
                    "input-history"
                    in relevance[
                        (
                            foreign.journal_id,
                            expected["prefix_sha256"],
                            source_state["assertion_id"],
                        )
                    ]
                )
                assert (
                    foreign.journal_id,
                    int(source_state["entry"]["sequence"]),
                    expected,
                ) in calls
                assert expected != foreign.anchor
