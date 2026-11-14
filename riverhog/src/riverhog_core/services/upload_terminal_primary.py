"""Select primary facts from the current terminal corpus attempt."""

from __future__ import annotations

from typing import Any

from riverhog_archive_contracts import HistoryJournalAnchor, MemberHistoryDocument
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
from riverhog_protocol.collection_production_provenance import COLLECTION_MEMBER_ROLE
from riverhog_protocol.errors import Conflict
from riverhog_provenance import CanonicalCorpusValidator
from riverhog_provenance.constants import PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
from riverhog_provenance_contracts import ContractCatalog
from sqlalchemy.orm import Session

from riverhog_core.services.upload_journal_result import prepare_upload_journal_result


def _declaration() -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def scale(multiplier: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=multiplier), term))

    def depth(name: str) -> Expression:
        return Expression(
            "log2_ceil", terms=(Expression("sum", terms=(count(2), dimension(name))),)
        )

    dimensions: list[Dimension] = []
    bounds: list[Bound] = []
    for name, operation in (
        ("endpoint", CanonicalCorpusValidator.require_source_endpoint),
        ("delivery", CanonicalCorpusValidator.selected_delivery_support),
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
                "One exact current source endpoint and one historical delivery selection.",
            )
            for bound in declaration.bounds
        )
    return Declaration(
        id="riverhog.upload-terminal-primary-state.growth/v1",
        authority="riverhog",
        subjects=(
            "riverhog_core.services.upload_terminal_primary.selected_upload_terminal_primary_state",
        ),
        scope="current-terminal-primary-historical-support",
        starts_at="receive one verified source descriptor in the current finalizer transaction",
        ends_at="return its exact historical State after current sealed source and job checks",
        dimensions=(
            *dimensions,
            Dimension("catalog_journals", COUNT, "All catalog journal rows."),
            Dimension("catalog_jobs", COUNT, "All catalog Native validation jobs."),
            Dimension(
                "catalog_row_octets",
                OCTETS,
                "Largest complete source or job metadata row, in UTF-8 and decimal scalars.",
            ),
        ),
        bounds=(
            *bounds,
            Bound(
                "catalog_statements",
                COUNT,
                "equal",
                Expression("constant", value=3, unit=COUNT),
                "Two current locked admission rows and one final exact source/job check.",
            ),
            Bound(
                "catalog_returned_rows",
                COUNT,
                "equal",
                Expression("constant", value=3, unit=COUNT),
                "Each successful admission and final check returns one metadata row.",
            ),
            Bound(
                "catalog_metadata_octets",
                OCTETS,
                "upper",
                scale(3, dimension("catalog_row_octets")),
                "Two complete metadata rows and the final workspace identity.",
            ),
            Bound(
                "catalog_parameter_octets",
                OCTETS,
                "upper",
                scale(6, dimension("catalog_row_octets")),
                "Exact source/job predicates and selected primary keys.",
            ),
            Bound(
                "catalog_changed_rows",
                COUNT,
                "equal",
                count(0),
                "Admission performs no catalog mutation or unrelated autoflush.",
            ),
            Bound(
                "catalog_payload_reads",
                COUNT,
                "equal",
                count(0),
                "No original or captured source payload column is read.",
            ),
            Bound(
                "catalog_open_cursors",
                COUNT,
                "equal",
                count(0),
                "Every catalog result closes before return.",
            ),
            Bound(
                "catalog_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        count(4096),
                        Expression(
                            "product",
                            terms=(
                                count(1024),
                                Expression(
                                    "sum",
                                    terms=(
                                        depth("catalog_journals"),
                                        depth("catalog_jobs"),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "SQLite primary-key and unique-source seeks avoid scans and temporary sorts.",
            ),
            Bound(
                "native_validation_advances",
                COUNT,
                "equal",
                Expression("constant", value=0, unit=COUNT),
                "Primary-reference fanout never replays accepted journal semantics.",
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "The primary selection consumes only indexed delivery metadata, never source "
                "bytes.",
            ),
            Bound(
                "native_result_openings",
                COUNT,
                "equal",
                Expression("constant", value=0, unit=COUNT),
                "No retained Native reader, construction or workspace lease is acquired.",
            ),
        ),
        conditions=(
            "The current finalizer owns the catalog transaction, claim and epoch.",
            "The same transaction populated the corpus from complete current sealed raw bytes.",
            "Source proof, artifact identity, requested extent and input State stay with history.",
        ),
        invariants=(
            "The current complete source endpoint must equal the raw-validated corpus endpoint.",
            "Delivery lookup uses the historical primary anchor, including correction intervals.",
            "The callback and its disposable corpus never outlive the current transaction.",
            "Missing or changed source/job/catalog metadata cannot fall back to unchecked reuse.",
        ),
        limitations=(
            "Current raw validation, source indexing and complete history/corpus work are separate."
            " The terminal attempt remains synchronous; failures restart with fresh raw reads.",
            "This bounds primary-selection work, not all finalization or total archive extent.",
            "Lock waits, physical I/O and elapsed time remain observations.",
        ),
    )


@declare_growth(
    _declaration(),
    delegates=(
        ("endpoint", CanonicalCorpusValidator.require_source_endpoint),
        ("delivery", CanonicalCorpusValidator.selected_delivery_support),
    ),
)
def selected_upload_terminal_primary_state(
    session: Session,
    collection_id: int,
    source: MemberHistoryDocument,
    *,
    catalog: ContractCatalog,
    corpus: CanonicalCorpusValidator,
) -> dict[str, Any]:
    """Resolve one exact historical delivery in the current terminal attempt.

    The corpus owns independently validated current raw bytes. Current metadata
    admission ties those bytes to the same sealed source, capture, job and catalog;
    it creates no Native reader or lease. The finalizer retains its claim/epoch
    and this transaction until complete history and corpus validation succeeds.
    """
    admission = prepare_upload_journal_result(
        session, collection_id, source.primary.journal.journal_id, catalog=catalog
    )
    corpus.require_source_endpoint(HistoryJournalAnchor.from_mapping(admission.anchor))
    support = corpus.selected_delivery_support(
        binding={"artifact_id": source.artifact_id, **source.primary.to_mapping()},
        artifact_id=source.artifact_id,
        byte_count=source.bytes,
        sha256=source.sha256,
        member_role=COLLECTION_MEMBER_ROLE,
        metadata_byte_budget=PROVENANCE_JOURNAL_ENTRY_BYTES_MAX,
    )
    if not admission.verify_current(session):
        raise Conflict("terminal primary source or validation job changed")
    return support.state_reference
