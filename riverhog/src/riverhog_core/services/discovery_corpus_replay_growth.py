"""Bounded application ports around the retained corpus replay conductor."""

from __future__ import annotations

from typing import Any

from riverhog_growth_contracts import (
    COUNT,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)

LIMITS = {
    "corpus_replay": {
        "corpus_dispatches": 1,
        "metadata_reads": 1,
        "freeze_calls": 1,
        "conductor_constructions": 1,
        "metadata_transactions": 1,
        "conductor_begins": 1,
    },
    "next_corpus_source": {
        "conductor_dispatches": 1,
        "metadata_reads": 1,
        "next_source_calls": 1,
    },
    "advance_corpus": {
        "conductor_dispatches": 1,
        "metadata_reads": 2,
        "next_source_calls": 1,
        "sealed_borrows": 1,
        "source_checks": 1,
        "native_progress_calls": 1,
        "native_results": 1,
        "summary_closes": 1,
        "conductor_prepares": 1,
        "metadata_transactions": 1,
        "conductor_applies": 1,
    },
}


def declare_corpus_replay_dispatch(owner: type[Any]) -> None:
    for mode, limits in LIMITS.items():
        operation = getattr(owner, mode)
        declare_growth(
            Declaration(
                id="riverhog-server.discovery-corpus-replay-dispatch-" + mode + ".growth/v1",
                authority="riverhog-server",
                subjects=(operation.__module__ + "." + operation.__qualname__,),
                scope="direct application conductor and retained source dispatch ports",
                starts_at="receive one frozen original selection beneath the current slot mutex",
                ends_at="return a requested source, pending work or one published corpus quantum",
                dimensions=(
                    Dimension("retained_sources", COUNT, "Independent retained source inputs."),
                ),
                bounds=tuple(
                    Bound(
                        metric,
                        COUNT,
                        "upper",
                        Expression("constant", value=limit, unit=COUNT),
                        "One selected original source or semantic phase without sibling traversal.",
                    )
                    for metric, limit in limits.items()
                ),
                conditions=(
                    "The application serializes work and closure under its held slot mutex.",
                    "Core, metadata, Native and reader bodies retain their separate bounds.",
                    "The caller prepares only the requested journal through the retained owner.",
                ),
                invariants=(
                    "Only the exact full original source can advance Native source coverage.",
                    "Selected prefixes remain separate from full source acceptance.",
                    "A borrowed summary stays reachable until its release is confirmed.",
                    "Native commits precede atomic Core cursor confirmation and can be retried.",
                    "Completion grants no member relevance, access or generation publication.",
                ),
                limitations=(
                    "Direct dispatch counts do not claim complete composed algorithm cost.",
                    "Source preparation, Native phase bodies and physical I/O are separate work.",
                ),
            )
        )(operation)
