"""Indexed exact ranges over component-owned staged provenance journal chunks."""

from __future__ import annotations

from riverhog_archive_contracts import PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX
from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    OCTETS_PER_COUNT,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)
from riverhog_protocol import COLLECTION_UPLOAD_PROVENANCE_APPEND_BYTES_MAX
from sqlalchemy import select
from sqlalchemy.orm import Session

from riverhog_core.catalog_models import CollectionUploadProvenanceJournalChunkRecord


@declare_growth(
    Declaration(
        id="riverhog-server.sqlite-staged-journal-range.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.upload_journal_ranges.upload_journal_range_bytes",),
        scope="native-sqlite-successful-range-read",
        starts_at="locate one exact byte range in component-owned staged transport chunks",
        ends_at="return the complete requested range and close its query result",
        dimensions=(
            Dimension("range_octets", OCTETS, "Octets requested and returned by this call."),
            Dimension("overlap_chunks", COUNT, "Ordinals intersecting the requested byte range."),
            Dimension("staged_chunks", COUNT, "Retained transport chunks in the selected journal."),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                Expression("constant", value=2, unit=COUNT),
                "Native source-location and remaining-range SELECT statements.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                Expression("dimension", name="overlap_chunks"),
                "Native result rows visited; completed-prefix rows are outside the interval.",
            ),
            Bound(
                "loaded_chunk_octets",
                OCTETS,
                "upper",
                Expression(
                    "product",
                    terms=(
                        Expression(
                            "constant",
                            value=COLLECTION_UPLOAD_PROVENANCE_APPEND_BYTES_MAX,
                            unit=OCTETS_PER_COUNT,
                        ),
                        Expression("dimension", name="overlap_chunks"),
                    ),
                ),
                "Transport octets loaded from actual selected rows.",
            ),
            Bound(
                "range_buffer_octets",
                OCTETS,
                "upper",
                Expression("dimension", name="range_octets"),
                "Peak length of the range assembly buffer.",
            ),
            Bound(
                "returned_octets",
                OCTETS,
                "equal",
                Expression("dimension", name="range_octets"),
                "Exact complete returned bytes; neither gaps nor short tails succeed.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        Expression("constant", value=128, unit=COUNT),
                        Expression(
                            "product",
                            terms=(
                                Expression("constant", value=64),
                                Expression("dimension", name="overlap_chunks"),
                            ),
                        ),
                        Expression(
                            "product",
                            terms=(
                                Expression("constant", value=64, unit=COUNT),
                                Expression(
                                    "log2_ceil",
                                    terms=(
                                        Expression(
                                            "sum",
                                            terms=(
                                                Expression("constant", value=2, unit=COUNT),
                                                Expression("dimension", name="staged_chunks"),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Maintained native SQLite instruction envelope for indexed seeks and row "
                "iteration.",
            ),
        ),
        conditions=(
            "The private catalog uses its maintained (collection_id, journal_id, "
            "ordinal) primary key.",
            "The unique source byte-offset index locates the first intersecting chunk.",
            "Immutable contiguous source chunks are at most the declared append extent; "
            "completed history prefixes may retain shorter interior chunks.",
            "overlap_chunks counts actual retained chunks intersecting this range.",
            "The native SQLite execution profile is exercised by the maintained "
            "actual-body witness.",
        ),
        invariants=(
            "Selected ordinals and byte offsets remain exact and contiguous.",
            "The requested bytes remain exact and contiguous, and query ownership "
            "closes on every exit.",
        ),
        limitations=(
            "Other database engines require their own instruction/index-cost qualification.",
            "SQLite VM instructions exclude internal B-tree comparisons, page I/O and "
            "physical CPU costs.",
            "This range read does not assert semantic journal acceptance, complete "
            "publication, or permission.",
            "Physical memory, caller preparation, rejected attempts and retries are "
            "separately charged.",
        ),
    )
)
def upload_journal_range_bytes(
    session: Session,
    collection_id: int,
    journal_id: str,
    *,
    offset: int,
    size: int,
) -> bytes:
    if (
        type(offset) is not int
        or offset < 0
        or type(size) is not int
        or not 1 <= size <= PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX
    ):
        raise RuntimeError("provenance journal publication range is invalid")
    ordinal, row_offset, raw = upload_journal_chunk_at(
        session,
        collection_id,
        journal_id,
        offset=offset,
    )
    content = bytearray()
    start = offset - row_offset
    content.extend(raw[start : start + size])
    if len(content) < size:
        statement = (
            select(
                CollectionUploadProvenanceJournalChunkRecord.ordinal,
                CollectionUploadProvenanceJournalChunkRecord.byte_offset,
                CollectionUploadProvenanceJournalChunkRecord.content,
            )
            .where(
                CollectionUploadProvenanceJournalChunkRecord.collection_id == collection_id,
                CollectionUploadProvenanceJournalChunkRecord.journal_id == journal_id,
                CollectionUploadProvenanceJournalChunkRecord.byte_offset > row_offset,
                CollectionUploadProvenanceJournalChunkRecord.byte_offset < offset + size,
            )
            .order_by(CollectionUploadProvenanceJournalChunkRecord.byte_offset)
            .execution_options(yield_per=1)
        )
        expected = offset + len(content)
        ordinal += 1
        with session.execute(statement) as rows:
            for row in rows:
                raw = bytes(row.content)
                if (
                    row.ordinal != ordinal
                    or row.byte_offset != expected
                    or not 1 <= len(raw) <= COLLECTION_UPLOAD_PROVENANCE_APPEND_BYTES_MAX
                ):
                    raise RuntimeError("provenance journal chunks are not contiguous")
                take = min(len(raw), size - len(content))
                content.extend(raw[:take])
                expected += take
                ordinal += 1
                if len(content) == size:
                    break
    if len(content) != size:
        raise RuntimeError("provenance journal publication range is unavailable")
    return bytes(content)


def upload_journal_chunk_at(
    session: Session,
    collection_id: int,
    journal_id: str,
    *,
    offset: int,
) -> tuple[int, int, bytes]:
    """Locate one retained segment without assuming uniform historical chunks."""
    if type(offset) is not int or offset < 0:
        raise RuntimeError("provenance journal publication range is invalid")
    chunk = CollectionUploadProvenanceJournalChunkRecord
    statement = (
        select(chunk.ordinal, chunk.byte_offset, chunk.content)
        .where(
            chunk.collection_id == collection_id,
            chunk.journal_id == journal_id,
            chunk.byte_offset <= offset,
        )
        .order_by(chunk.byte_offset.desc())
        .limit(1)
        .execution_options(yield_per=1)
    )
    with session.execute(statement) as rows:
        for row in rows:
            raw = bytes(row.content)
            if (
                not 1 <= len(raw) <= COLLECTION_UPLOAD_PROVENANCE_APPEND_BYTES_MAX
                or not row.byte_offset <= offset < row.byte_offset + len(raw)
            ):
                raise RuntimeError("provenance journal chunks are not contiguous")
            return int(row.ordinal), int(row.byte_offset), raw
    raise RuntimeError("provenance journal publication range is unavailable")
