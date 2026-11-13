"""Verify selected prefix facts against the current sealed Native result."""

from __future__ import annotations

from pathlib import Path

from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
    growth_binding,
)
from riverhog_protocol.errors import Conflict
from riverhog_provenance import JournalSummary
from riverhog_provenance_contracts import ContractCatalog
from sqlalchemy.orm import Session

from riverhog_core.services.upload_journal_result import open_upload_journal_result
from riverhog_core.services.upload_journal_result_receiving import UploadJournalResultResources


def _declaration() -> Declaration:
    dimensions: list[Dimension] = []
    bounds: list[Bound] = []
    for name, operation in (
        ("admission", open_upload_journal_result),
        ("anchor", JournalSummary.anchor_at),
    ):
        declaration = growth_binding(operation).declaration
        dimensions.extend(
            Dimension(name + "_" + item.name, item.unit, item.meaning)
            for item in declaration.dimensions
        )
        bounds.extend(
            Bound(
                name + "_" + bound.metric,
                bound.unit,
                bound.relation,
                Expression(
                    "delegate",
                    name=name,
                    metric=bound.metric,
                    terms=(Expression("constant", value=1, unit=COUNT),),
                    arguments=tuple(
                        (item.name, Expression("dimension", name=name + "_" + item.name))
                        for item in declaration.dimensions
                    ),
                ),
                "One actual current sealed admission and one exact accepted prefix selection.",
            )
            for bound in declaration.bounds
        )
    return Declaration(
        id="riverhog.selected-upload-journal-prefix.growth/v1",
        authority="riverhog",
        subjects=(
            "riverhog_core.services.upload_journal_prefix.verify_selected_upload_journal_prefix",
        ),
        scope="current-sealed-native-selected-prefix-proof",
        starts_at="receive one exact selected anchor in the finalizer's current catalog turn",
        ends_at="verify its accepted header and receive the original independently owned result",
        dimensions=tuple(dimensions),
        bounds=(
            *bounds,
            Bound(
                "original_openings",
                COUNT,
                "equal",
                Expression("constant", value=1, unit=COUNT),
                "One original opening is retained before any lease or Native acquisition.",
            ),
            Bound(
                "native_validation_advances",
                COUNT,
                "equal",
                Expression("constant", value=0, unit=COUNT),
                "Selected prefixes use their already accepted Native frame facts.",
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "Neither the caller nor either delegate reads or rehashes journal payload.",
            ),
        ),
        conditions=(
            "The current finalizer owns the catalog transaction, claim, open admission and epoch.",
            "The immutable catalog and sealed-source admission use the existing Native policy.",
            "The caller retains the original resource owner through every exceptional close.",
            "Complete current raw history and corpus validation still precedes closure success.",
        ),
        invariants=(
            "Every anchor coordinate must match an accepted prefix of the exact admitted source.",
            "No historical semantic graph, source replay, cache or new durable proof is created.",
            "Missing or changed retained work is never silently replaced with fresh validation.",
            "Original receiving precedes release of its resource grant, including on failure.",
        ),
        limitations=(
            "The declared delegate metrics cover successful admission and prefix selection.",
            "Original final-reader, construction and lease closure retain their existing owners; "
            "their resource work and failure receiving are separate from admission metrics.",
            "Member history traversal, primary semantics and final raw corpus validation are "
            "separate caller operations; provisional member progress is not closure acceptance.",
            "Lock waits, physical I/O and elapsed time remain observations.",
        ),
    )


@declare_growth(
    _declaration(),
    delegates=(
        ("admission", open_upload_journal_result),
        ("anchor", JournalSummary.anchor_at),
    ),
)
def verify_selected_upload_journal_prefix(
    session: Session,
    root: Path,
    collection_id: int,
    anchor: HistoryJournalAnchor,
    *,
    catalog: ContractCatalog,
    result_resources: UploadJournalResultResources,
) -> None:
    """Verify one exact selected prefix without replaying its accepted semantics.

    Native commits each prefix header only after that frame's semantic checks.
    This proof needs no historical effective graph. Current raw-byte integrity
    remains with the final complete-history and corpus barrier, before closure
    acceptance. An uncertain opening or close stays with this original owner.
    """
    original = result_resources.retain()
    summary = open_upload_journal_result(
        session,
        root,
        collection_id,
        anchor.journal_id,
        catalog=catalog,
        receiver=original,
    )
    try:
        try:
            selected = HistoryJournalAnchor.from_mapping(summary.anchor_at(anchor.through_sequence))
        except IndexError as error:
            raise Conflict("selected member history root has no accepted prefix") from error
        if selected != anchor:
            raise Conflict("selected member history root differs from its accepted prefix")
    finally:
        result_resources.receive(original)
