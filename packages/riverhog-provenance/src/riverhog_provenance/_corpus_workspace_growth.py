"""Lifecycle costs around Native's separately declared corpus algorithms."""

from __future__ import annotations

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    SCALAR,
    Bound,
    Declaration,
    Dimension,
    Expression,
    Unit,
)


def _number(value: int, unit: Unit = COUNT) -> Expression:
    return Expression("constant", value=value, unit=unit)


def _dimension(name: str) -> Expression:
    return Expression("dimension", name=name)


def _scale(value: int, name: str) -> Expression:
    return Expression("product", terms=(_number(value, SCALAR), _dimension(name)))


def lifecycle_declaration(operation: str) -> Declaration:
    closing = operation == "close"
    opening = operation == "open_retained"
    dimensions = (
        Dimension("binding_octets", OCTETS, "Exact encoded immutable input binding width."),
        Dimension("checkpoint_octets", OCTETS, "Maximum complete before/after checkpoint width."),
        Dimension(
            "counter_octets", OCTETS, "Maximum complete decimal scalar width used by this call."
        ),
        Dimension(
            "retained_rows",
            COUNT,
            "Independent retained input and ordering rows, without enumeration.",
        ),
    )
    counters = Expression("sum", terms=(_scale(8, "counter_octets"), _number(128, OCTETS)))
    state_bytes = Expression("sum", terms=(_dimension("checkpoint_octets"), counters))
    parameters = Expression(
        "sum",
        terms=(
            _scale(4 if opening else 2, "binding_octets"),
            _scale(2, "checkpoint_octets"),
            counters,
        ),
    )
    limits = {
        "sql_statements": 0 if closing else 64 if opening else 7,
        "returned_rows": 0 if closing else 8 if opening else 3,
        "source_checks": 0 if closing else 2,
        "source_frame_reads": 0,
        "full_assertion_reads": 0,
        "closed_connections": 1 if closing or opening else 0,
        "opened_connections": 1 if opening else 0,
        "release_callbacks": 1 if closing else 0,
    }
    bounds = [
        Bound(metric, COUNT, "upper", _number(limit), meaning)
        for metric, limit, meaning in (
            (
                "sql_statements",
                limits["sql_statements"],
                "Fixed schema/lifecycle statements; transaction-body algorithms are separate.",
            ),
            (
                "returned_rows",
                limits["returned_rows"],
                "Only singleton lifecycle metadata is returned.",
            ),
            (
                "source_checks",
                limits["source_checks"],
                "Check the current owner before admission and commit.",
            ),
            (
                "source_frame_reads",
                0,
                "Lifecycle admission and checkpointing never replay source frames.",
            ),
            (
                "full_assertion_reads",
                0,
                "Lifecycle admission never reads documentary assertion payloads.",
            ),
            (
                "closed_connections",
                limits["closed_connections"],
                "Only the owned connection can close.",
            ),
            (
                "opened_connections",
                limits["opened_connections"],
                "At most one native connection is admitted.",
            ),
            (
                "release_callbacks",
                limits["release_callbacks"],
                "The transferred callback follows actual close.",
            ),
        )
    ]
    bounds.extend(
        (
            Bound(
                "returned_octets",
                OCTETS,
                "upper",
                _number(0, OCTETS) if closing else state_bytes,
                "One admitted checkpoint and its owning scalar metadata.",
            ),
            Bound(
                "parameter_octets",
                OCTETS,
                "upper",
                _number(0, OCTETS) if closing else parameters,
                "Complete repeated exact bindings, checkpoints and scalar compare-and-swap keys.",
            ),
            Bound(
                "checkpoint_decoded_octets",
                OCTETS,
                "upper",
                _number(0, OCTETS) if closing else _dimension("checkpoint_octets"),
                "Decode at most the admitted retained checkpoint, without traversing input data.",
            ),
            Bound(
                "checkpoint_encoded_octets",
                OCTETS,
                "upper",
                _number(0, OCTETS) if closing else _scale(2, "checkpoint_octets"),
                "Canonical round-trip admission and the one next checkpoint.",
            ),
        )
    )
    return Declaration(
        id="riverhog-provenance.retained-corpus-"
        + operation.strip("_").replace("_", "-")
        + ".growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance.corpus_workspace.RetainedCorpusValidator." + operation,),
        scope="native retained corpus lifecycle"
        if opening or closing
        else "entry and exit of one retained corpus owner transaction, excluding its body",
        starts_at="receive the owned workspace, independent exact inputs and explicit worker fence"
        if opening
        else "enter the lifecycle boundary with current ownership",
        ends_at="return or reject the retained owner"
        if opening
        else "close its sole native handle"
        if closing
        else "commit or roll back the body and its parent checkpoint together",
        dimensions=dimensions,
        bounds=tuple(bounds),
        conditions=(
            "The application authenticates the workspace and holds its exclusive kernel lease.",
            "The source port performs bounded currentness and exact indexed membership checks.",
            "The supplied cached contract catalog owns its independent policy identity.",
            "State widths include all admitted metadata and the complete output checkpoint.",
        ),
        invariants=(
            "Only a strictly newer worker replaces a retained owner.",
            "Input binding, schema cookie, worker and checkpoint remain one atomic fence.",
            "Creation is exclusive; reopening does not create or repair missing state.",
            "Exact source/selection memberships and independent counts precede semantic freezing.",
            "Lifecycle work grants no member relevance, current archive access or publication.",
        ),
        limitations=(
            "Body algorithms, manifest membership and release callback internals need composition.",
            "SQL visits and metadata bytes do not establish I/O, lock-wait or total build bounds.",
            "These roots do not establish composed retained work or complete build conformance.",
        ),
    )
