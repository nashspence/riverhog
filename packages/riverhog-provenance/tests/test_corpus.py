from __future__ import annotations

import pytest
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    ProvenanceValidationError,
    append_assertions,
    assertion,
    create_journal,
    external_reference,
    reference,
    validate_canonical_corpus,
    validate_journal,
)


def test_corpus_preserves_exact_documentary_identity_and_fork_requirements(journal, who, catalog):
    primary = validate_journal(journal, catalog=catalog)
    other = create_journal(primary.graph, recorded_by_agent_id=who, catalog=catalog)
    validate_canonical_corpus((primary, validate_journal(other, catalog=catalog), primary))
    graph = primary.graph
    graph["agents"][0]["name"] = "conflicting assertion identity"
    conflict = create_journal(graph, recorded_by_agent_id=who, catalog=catalog)
    with pytest.raises(ProvenanceValidationError, match="identity"):
        validate_canonical_corpus((primary, validate_journal(conflict, catalog=catalog)))
    fork = create_journal(
        {"agents": primary.graph["agents"]},
        recorded_by_agent_id=who,
        forked_from=primary.anchor,
        catalog=catalog,
    )
    child = validate_journal(fork, catalog=catalog)
    validate_canonical_corpus((child, primary))
    with pytest.raises(ProvenanceValidationError, match="fork prefix"):
        validate_canonical_corpus((child,))


def test_corpus_validates_exact_foreign_fixity_comparison(journal, who, catalog):
    primary = validate_journal(journal, catalog=catalog)
    other = BoundedSourceObserver(catalog=catalog).observe(BytesSource(b"different"))
    graph = other.graph_fragment()
    graph["relations"] = [
        assertion(
            "content_comparison",
            who,
            left_description=external_reference(primary, primary.graph["descriptions"][0]["id"]),
            right_description=reference(other.observation_id, "observation"),
            result="matching_fixity",
            algorithm="sha-256",
        )
    ]
    child = validate_journal(
        create_journal(graph, recorded_by_agent_id=who, catalog=catalog), catalog=catalog
    )
    with pytest.raises(ProvenanceValidationError, match="unresolved"):
        validate_canonical_corpus((child,))
    with pytest.raises(ProvenanceValidationError, match="foreign content"):
        validate_canonical_corpus((primary, child))


def test_corpus_detects_cross_journal_causal_cycle(journal, who, catalog):
    primary = validate_journal(journal, catalog=catalog)
    other = BoundedSourceObserver(catalog=catalog).observe(BytesSource(b"other"))
    graph = other.graph_fragment()
    graph["relations"] = [
        assertion(
            "derivation",
            who,
            used_state=external_reference(primary, primary.states[0]["id"]),
            generated_state=reference(other.state_id, "state"),
            kind="copy",
        )
    ]
    child = validate_journal(
        create_journal(graph, recorded_by_agent_id=who, catalog=catalog), catalog=catalog
    )
    amended = append_assertions(
        journal,
        {
            "relations": [
                assertion(
                    "derivation",
                    who,
                    used_state=external_reference(child, other.state_id),
                    generated_state=reference(primary.states[0]["id"], "state"),
                    kind="copy",
                )
            ]
        },
        recorded_by_agent_id=who,
        catalog=catalog,
    )
    with pytest.raises(ProvenanceValidationError, match="cycle"):
        validate_canonical_corpus((validate_journal(amended, catalog=catalog), child))
