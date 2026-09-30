from __future__ import annotations

import hashlib

from riverhog_archive_contracts import HistoryJournalAnchor, MemberHistoryRoot
from riverhog_client.canonical_production import ProducerAttribution, build_member_journal
from riverhog_core.canonical_discovery_relevance import member_relevance, snapshots_for_relevance
from riverhog_core.canonical_discovery_rows import iter_index_assertions
from riverhog_protocol import ArtifactMemberIdentityDocument
from riverhog_protocol.collection_production_provenance import collection_production_contract
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    append_correction,
    assertion,
    assertion_reference,
    create_journal,
    external_reference,
    new_id,
    validate_journal,
)
from riverhog_provenance_contracts import ContractCatalog

from tests.support.member_history import member_history_selection_fixture

DESCRIBES = "https://nashspence.github.io/riverhog/v1/provenance/relations/describes"


def _members():
    catalog = ContractCatalog((collection_production_contract(),))
    context = new_id()
    result = []
    for artifact_id, payload in (("a1" * 32, b"description"), ("b2" * 32, b"described")):
        member = ArtifactMemberIdentityDocument(
            artifact_id=artifact_id,
            bytes=str(len(payload)),
            sha256=hashlib.sha256(payload).hexdigest(),
        )
        produced = build_member_journal(
            member=member,
            observation=BoundedSourceObserver().observe(BytesSource(payload)),
            delivery_context_id=context,
            attribution=ProducerAttribution(
                "example", "bytes", "v1", "event", "fixture", {}, "e1" * 32
            ),
            materialization_hint=None,
        )
        primary = validate_journal(produced.content, catalog=catalog)
        state = primary.graph_validation.objects[produced.binding.delivery_association_id]["state"]
        result.append(
            (member, produced.binding, primary, external_reference(primary, state["object_id"]))
        )
    return context, catalog, result


def _completion(left, right):
    who = new_id()
    relation = assertion(
        "extension",
        who,
        subject=left,
        property=DESCRIBES,
        value={"type": "reference", "value": right},
    )
    raw = create_journal(
        {
            "agents": [assertion("agent", who, object_id=who, kind="software", name="producer")],
            "extensions": [relation],
        },
        recorded_by_agent_id=who,
    )
    return who, relation, raw


def test_native_index_reads_late_bound_claim_without_assigning_it_to_a_sibling():
    context, catalog, members = _members()
    _, claim, raw = _completion(members[0][3], members[1][3])
    completion = validate_journal(raw)
    corpus = {primary.journal_id: primary for _, _, primary, _ in members}
    corpus[completion.journal_id] = completion
    root = MemberHistoryRoot(HistoryJournalAnchor.from_mapping(completion.anchor), "bound")
    for ordinal, (member, primary_binding, primary, _) in enumerate(members):
        selected, closure, _ = member_history_selection_fixture(
            member, primary_binding, corpus, roots=(root,)
        )
        with (
            closure,
            member_relevance(
                member=member,
                binding=primary_binding,
                primary=primary,
                corpus=corpus,
                delivery_context_id=context,
                catalog=catalog,
                history_binding=selected,
                closure=closure,
            ) as relevance,
        ):
            scopes = relevance[
                (completion.journal_id, completion.journal_sha256, claim["assertion_id"])
            ]
            assert ("member" in scopes) == (ordinal == 0)
            assert "recorded-history" in scopes
            assert completion.anchor in [
                snapshot.anchor for snapshot in snapshots_for_relevance(relevance, corpus=corpus)
            ]


def test_selected_completion_retraction_cannot_resurrect_a_late_member_claim():
    context, catalog, members = _members()
    who, claim, raw = _completion(members[0][3], members[1][3])
    old = validate_journal(raw)
    corrected = append_correction(
        raw,
        (assertion_reference(old, claim["assertion_id"]),),
        reason="producer withdrew its descriptive assertion",
        recorded_by_agent_id=who,
    )
    completion = validate_journal(corrected)
    corpus = {primary.journal_id: primary for _, _, primary, _ in members}
    corpus[completion.journal_id] = completion
    member, binding, primary, _ = members[0]
    selected, closure, _ = member_history_selection_fixture(
        member,
        binding,
        corpus,
        roots=(MemberHistoryRoot(HistoryJournalAnchor.from_mapping(completion.anchor), "bound"),),
    )
    with (
        closure,
        member_relevance(
            member=member,
            binding=binding,
            primary=primary,
            corpus=corpus,
            delivery_context_id=context,
            catalog=catalog,
            history_binding=selected,
            closure=closure,
        ) as relevance,
    ):
        assert relevance[
            (completion.journal_id, completion.journal_sha256, claim["assertion_id"])
        ] == frozenset({"recorded-history"})
        indexed = next(
            row
            for row in iter_index_assertions(completion)
            if row.assertion_id == claim["assertion_id"]
        )
        assert indexed.assertion_state == "retracted"
