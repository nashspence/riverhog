"""Pure current-atom correlations, constructed once before semantic traversal."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    SCALAR,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)
from riverhog_provenance_contracts import PROFILE

from .constants import PRIMARY_CONTENT_CATEGORY


def _declaration() -> Declaration:
    rows = Expression("dimension", name="input_rows")
    return Declaration(
        id="riverhog-provenance.observation-coverage-index.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance._observation_coverage.ObservationCoverage.__init__",),
        scope="admitted-observation-coverage-correlation-index",
        starts_at="receive the current immutable owner-admitted observation atom",
        ends_at="retain coverage statuses and aggregate flags without semantic validation",
        dimensions=(
            Dimension("input_rows", COUNT, "Actual source coverage rows, including duplicates."),
            Dimension("key_octets", OCTETS, "UTF-8 profile/category bytes across source rows."),
        ),
        bounds=(
            Bound("indexed_rows", COUNT, "upper", rows, "One row per distinct coverage pair."),
            Bound(
                "retained_key_octets",
                OCTETS,
                "upper",
                Expression("dimension", name="key_octets"),
                "Existing immutable string keys.",
            ),
            Bound(
                "python_opcode_events",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        Expression("constant", value=8192, unit=COUNT),
                        Expression(
                            "product",
                            terms=(
                                Expression("constant", value=256, unit=SCALAR),
                                rows,
                            ),
                        ),
                    ),
                ),
                "One visit per coverage row, with no nested coverage/metadata traversal.",
            ),
        ),
        conditions=(
            "The owner supplies one immutable schema-admitted observation atom.",
            "The index is built once at atom admission, outside semantic checkpoint transactions.",
        ),
        invariants=(
            "Index construction reports no semantic acceptance or diagnostic.",
            "Native duplicate coverage checks precede all status correlations.",
            "Retained rejection reconstruction uses the same source-derived correlations.",
        ),
        limitations=(
            "UTF-8/string hashing and equality cost is proportional to admitted key bytes.",
            "Schema, decoding, canonicalization and current-atom reconstruction "
            "are separate costs.",
            "The index retains references to current-atom strings, not historical payloads.",
        ),
    )


class ObservationCoverage:
    @declare_growth(_declaration())
    def __init__(self, value: Mapping[str, Any]) -> None:
        self.statuses: dict[tuple[str, str], str] = {}
        self.primary_complete = False
        self.incomplete = False
        for item in value.get("coverage", []):
            profile, category, status = item["profile_id"], item["category"], item["status"]
            self.statuses.setdefault((profile, category), status)
            if profile == PROFILE and category == PRIMARY_CONTENT_CATEGORY and status == "complete":
                self.primary_complete = True
            if status in ("partial", "failed"):
                self.incomplete = True

    def status(self, item: Mapping[str, Any]) -> str | None:
        return self.statuses.get((item["profile_id"], item["category"]))
