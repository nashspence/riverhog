"""Retryable custody while admitting one retained frozen journal result."""

from __future__ import annotations

from typing import TYPE_CHECKING

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)

if TYPE_CHECKING:
    from .journal_store import JournalStore


def _close_declaration() -> Declaration:
    return Declaration(
        id="riverhog-provenance.retained-result-opening-close.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance.journal_result_opening.JournalResultOpening.close",),
        scope="retryable release of actual partial read-only retained result storage",
        starts_at="receive the same early-retained opening before or after failed admission",
        ends_at="confirm actual storage closure or keep the identical unconfirmed handle",
        dimensions=(
            Dimension("frozen_frames", COUNT, "Independent accepted retained frame count."),
            Dimension("indexed_assertions", COUNT, "Independent retained assertion count."),
        ),
        bounds=(
            *(
                Bound(
                    metric,
                    COUNT,
                    "upper",
                    Expression("constant", value=limit, unit=COUNT),
                    "At most the one retained read-only Native connection closes.",
                )
                for metric, limit in {
                    "closed_databases": 1,
                    "release_callbacks": 0,
                    "sql_statements": 0,
                    "source_probes": 0,
                    "historical_payload_reads": 0,
                    "filesystem_mutations": 0,
                }.items()
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "Opening cleanup reads no original source payload.",
            ),
        ),
        conditions=(
            "The caller serializes admission and cleanup and retains the opening first.",
            "Only owner-created retained read-only storage enters this opening.",
        ),
        invariants=(
            "An uncertain close keeps the actual Native handle reachable for retry.",
            "No caller release callback transfers before successful result admission.",
            "Successful admission transfers custody to the returned independent result.",
            "Closing a transferred opening cannot close that result or another borrower.",
        ),
        limitations=(
            "SQLite connection internals and operating-system latency are excluded.",
            "Result admission, source fences and final result closure have separate bounds.",
        ),
    )


class JournalResultOpening:
    """Closeable storage custody during one frozen-result constructor.

    The early callback retains this object before any database acquisition.
    Failure keeps partial Native storage here until close confirms. Success
    transfers custody to the returned result and closes this empty opening.
    This handle exposes no result, query, replay or acceptance authority.
    """

    def __init__(self) -> None:
        self._store: JournalStore | None = None
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def _retain(self, store: JournalStore) -> None:
        self._store = store

    def _transfer(self) -> None:
        self._store = None
        self._closed = True

    @declare_growth(_close_declaration())
    def close(self) -> None:
        if self._closed:
            return
        if self._store is not None:
            self._store.close()
            self._store = None
        self._closed = True
