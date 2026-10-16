"""Private bounded correction-boundary scanning over retained upload frames."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)
from riverhog_provenance.constants import PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
from riverhog_provenance.epoch_boundaries import (
    JOURNAL_EPOCH_BOUNDARY_ALGORITHM,
    JournalEpochBoundary,
    classify_epoch_boundary,
)
from riverhog_provenance.framing import JournalFramePosition
from sqlalchemy import select, update
from state_schema import read_snapshot

from riverhog_core.catalog_db import SessionFactory
from riverhog_core.catalog_models import (
    CollectionUploadProvenanceEpochScanRecord as Scan,
)
from riverhog_core.catalog_models import (
    CollectionUploadProvenanceFrameRecord as Frame,
)
from riverhog_core.catalog_models import (
    CollectionUploadProvenanceJournalRecord as Journal,
)

from . import collection_uploads as source_owner
from .upload_journal_cleanup import advance_retired_upload_journal_preparation, enqueue_retirement

_FRAME_BUDGET = 64
_OCTET_BUDGET = 64 * 1024


@dataclass(frozen=True, slots=True)
class _EpochWork:
    source: source_owner._UploadJournalValidationWork
    version: int
    next_frame: int
    byte_offset: int
    epoch: int
    state: str


def _scan_fence(work: _EpochWork) -> tuple[Any, ...]:
    source = work.source
    return (
        Scan.collection_id == source.collection_id,
        Scan.journal_id == source.journal_id,
        Scan.attempt_id == source.attempt_id,
        Scan.directory_id == source.directory_id,
        Scan.algorithm == JOURNAL_EPOCH_BOUNDARY_ALGORITHM,
        Scan.version == work.version,
        Scan.next_frame == work.next_frame,
        Scan.byte_offset == work.byte_offset,
        Scan.epoch == work.epoch,
        Scan.state == work.state,
    )


def _begin_scan(
    factory: SessionFactory,
    source: source_owner._UploadJournalValidationWork,
) -> _EpochWork | None:
    with factory() as session, session.begin():
        if (
            session.scalar(
                select(Journal.collection_id)
                .where(*source_owner._upload_journal_validation_fence(source))
                .with_for_update(skip_locked=True)
            )
            is None
        ):
            return None
        row = session.get(Scan, (source.collection_id, source.journal_id), with_for_update=True)
        if row is None:
            row = Scan(collection_id=source.collection_id, journal_id=source.journal_id)
            session.add(row)
        if row.attempt_id != source.attempt_id:
            if row.attempt_id is not None:
                enqueue_retirement(session, row)
            row.attempt_id = source.attempt_id
            row.directory_id = source.directory_id
            row.algorithm = JOURNAL_EPOCH_BOUNDARY_ALGORITHM
            row.version = row.next_frame = row.byte_offset = row.epoch = 0
            row.state = "scanning"
            row.topology_algorithm = "riverhog-journal-topology-facts/v1"
            row.topology_next_frame = 0
            row.topology_state = "scanning"
            row.topology_cursor_json = '{"active":null,"facts":"0","last":null}'
            row.union_algorithm = "riverhog-journal-epoch-union/v1"
            row.union_next_frame = 0
            row.union_state = "building"
            row.union_cursor_json = (
                '{"active":"0","consumed":"0","epoch":"0","item":"0",'
                '"last_arc":null,"last_source":null,"phase":"frame",'
                '"seed_after":null,"seeded":"0"}'
            )
            row.rank_algorithm = "riverhog-journal-epoch-rank/v1"
            row.rank_state = "ranking"
            row.rank_cursor_json = (
                '{"active":null,"arcs":"0","edge_after":null,"epoch":"0",'
                '"kind":"derivation","last_arc":null,"last_graph":null,'
                '"last_vertex":null,"phase":"graph","ready":"0","seed_after":null,'
                '"seeded":"0","settled":"0","vertices":"0","visited":"0"}'
            )
            row.replay_binding_json = None
        if (
            row.directory_id != source.directory_id
            or row.algorithm != JOURNAL_EPOCH_BOUNDARY_ALGORITHM
        ):
            raise RuntimeError("journal epoch checkpoint binding differs from its attempt")
        work = _EpochWork(
            source, row.version, row.next_frame, row.byte_offset, row.epoch, row.state
        )
        if not (
            0 <= work.next_frame <= source.cursor.frame_count
            and 0 <= work.byte_offset <= source.cursor.byte_offset
            and 0 <= work.epoch <= work.next_frame
        ):
            raise RuntimeError("journal epoch checkpoint extent differs from its source")
        if work.next_frame:
            last = session.get(
                Frame,
                (source.collection_id, source.journal_id, source.directory_id, work.next_frame - 1),
            )
            if last is None or (
                last.epoch_attempt_id != source.attempt_id
                or last.epoch_ordinal != work.epoch
                or last.epoch_entry_kind is None
                or last.epoch_json_sha256 is None
                or last.byte_offset + last.json_bytes + 2 != work.byte_offset
            ):
                raise RuntimeError("journal epoch checkpoint lost its last published frame")
        elif work.byte_offset or work.epoch:
            raise RuntimeError("empty journal epoch checkpoint has nonempty progress")
        if work.state == "complete" and (
            work.next_frame != source.cursor.frame_count
            or work.byte_offset != source.source_bytes
            or source.phase != "complete"
        ):
            raise RuntimeError("completed journal epoch checkpoint differs from its source")
        return work


@declare_growth(
    Declaration(
        id="riverhog-server.sqlite-journal-epoch-frame-window.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.services.upload_journal_epochs._read_epoch_frames",),
        scope="native-sqlite-successful-fenced-private-epoch-frame-window",
        starts_at="check the exact capture and epoch cursor then seek its frame window",
        ends_at="return bounded position metadata after closing all snapshot resources",
        dimensions=(
            Dimension("retained_frames", COUNT, "Frames retained in this captured directory."),
            Dimension("requested_frames", COUNT, "Maximum positions admitted by this call."),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "equal",
                Expression("constant", value=2, unit=COUNT),
                "One joined exact-fence probe and one indexed position window.",
            ),
            Bound(
                "returned_positions",
                COUNT,
                "upper",
                Expression("dimension", name="requested_frames"),
                "Only the requested bounded frame metadata is returned.",
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "The window reads no source payload; exact page reads are separate delegates.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        Expression("constant", value=256, unit=COUNT),
                        Expression(
                            "product",
                            terms=(
                                Expression("constant", value=128),
                                Expression("dimension", name="requested_frames"),
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
                                                Expression("dimension", name="retained_frames"),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Maintained instruction envelope for the actual indexed native window.",
            ),
        ),
        conditions=(
            "The owning primary keys and exact source/attempt/cursor fences are present.",
            "The private requested frame budget is positive and at most 64.",
        ),
        invariants=(
            "No position or epoch fact establishes semantic acceptance.",
            "Offsets and ordinals remain contiguous under the selected captured source.",
        ),
        limitations=(
            "Source page lookup/hashing, entry decoding, durable publication, retries, "
            "and reclamation are separate costs.",
            "This boundary scan does not build graph certificates or resumable semantic state.",
            "SQLite VM instructions exclude internal B-tree comparisons, page I/O and CPU; "
            "other engines require their own qualification.",
        ),
    )
)
def _read_epoch_frames(
    factory: SessionFactory, work: _EpochWork, *, frame_budget: int
) -> tuple[JournalFramePosition, ...] | None:
    if type(frame_budget) is not int or not 1 <= frame_budget <= _FRAME_BUDGET:
        raise ValueError("invalid journal epoch frame budget")
    source = work.source
    with read_snapshot(factory) as session:
        if (
            session.scalar(
                select(Scan.collection_id)
                .join(
                    Journal,
                    (Journal.collection_id == Scan.collection_id)
                    & (Journal.journal_id == Scan.journal_id),
                )
                .where(*_scan_fence(work), *source_owner._upload_journal_validation_fence(source))
            )
            is None
        ):
            return None
        with session.execute(
            select(Frame.ordinal, Frame.byte_offset, Frame.json_bytes)
            .where(
                Frame.collection_id == source.collection_id,
                Frame.journal_id == source.journal_id,
                Frame.directory_id == source.directory_id,
                Frame.ordinal >= work.next_frame,
            )
            .order_by(Frame.ordinal)
            .limit(frame_budget)
        ) as rows:
            positions = tuple(JournalFramePosition(*row) for row in rows)
    expected = min(frame_budget, source.cursor.frame_count - work.next_frame)
    if len(positions) != expected:
        raise RuntimeError("journal epoch frame directory is incomplete")
    offset = work.byte_offset
    for ordinal, position in enumerate(positions, work.next_frame):
        if (
            position.ordinal != ordinal
            or position.byte_offset != offset
            or not 0 <= position.json_bytes <= PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
        ):
            raise RuntimeError("journal epoch frame directory is discontinuous")
        offset += position.json_bytes + 2
        if offset > source.cursor.byte_offset:
            raise RuntimeError("journal epoch frame exceeds its captured source")
    return positions


def _read_frame(
    factory: SessionFactory,
    source: source_owner._UploadJournalValidationWork,
    position: JournalFramePosition,
    *,
    page_cache: dict[int, bytes],
) -> bytes | None:
    extent = source_owner._JOURNAL_CAPTURE_SEGMENT_OCTETS
    start, end = position.byte_offset, position.byte_offset + position.json_bytes + 2
    pieces = []
    for ordinal in range(start // extent, (end - 1) // extent + 1):
        page = page_cache.get(ordinal)
        if page is None:
            page = source_owner._read_upload_journal_capture_segment(
                factory, source, ordinal=ordinal
            )
            if page is None:
                return None
            page_cache.clear()
            page_cache[ordinal] = page
        page_start = ordinal * extent
        pieces.append(page[max(0, start - page_start) : min(extent, end - page_start)])
    raw = b"".join(pieces)
    if len(raw) != end - start or not raw.startswith(b"\x1e") or not raw.endswith(b"\n"):
        raise RuntimeError("journal epoch frame differs from its retained position")
    return raw[1:-1]


@dataclass(frozen=True, slots=True)
class _EpochAdvance:
    frames: tuple[tuple[JournalFramePosition, JournalEpochBoundary], ...]
    next_frame: int
    byte_offset: int
    epoch: int
    state: str


def _compute_scan(factory: SessionFactory, work: _EpochWork) -> _EpochAdvance | None:
    positions = _read_epoch_frames(factory, work, frame_budget=_FRAME_BUDGET)
    if positions is None:
        return None
    frames: list[tuple[JournalFramePosition, JournalEpochBoundary]] = []
    next_frame, offset, epoch = work.next_frame, work.byte_offset, work.epoch
    state = "scanning"
    admitted = 0
    # Contiguous positions share one page for this detached batch only. Source
    # and attempt authority are checked again before any result can be published.
    page_cache: dict[int, bytes] = {}
    for position in positions:
        size = position.json_bytes + 2
        if frames and admitted + size > _OCTET_BUDGET:
            break
        raw = _read_frame(factory, work.source, position, page_cache=page_cache)
        if raw is None:
            return None
        try:
            boundary = classify_epoch_boundary(raw, ordinal=position.ordinal, previous_epoch=epoch)
        except (ValueError, TypeError):
            # A speculative finding cannot outrank an earlier native prefix error.
            state = "deferred"
            break
        frames.append((position, boundary))
        admitted += size
        next_frame += 1
        offset += size
        epoch = boundary.epoch
    if state != "deferred" and next_frame == work.source.cursor.frame_count:
        state = "complete" if work.source.phase == "complete" else "deferred"
        if state == "complete" and offset != work.source.source_bytes:
            raise RuntimeError("journal epoch terminal extent differs from its source")
    return _EpochAdvance(tuple(frames), next_frame, offset, epoch, state)


def _commit_scan(factory: SessionFactory, work: _EpochWork, advance: _EpochAdvance) -> bool:
    source = work.source
    with factory() as session, session.begin():
        if (
            session.scalar(
                select(Journal.collection_id)
                .where(*source_owner._upload_journal_validation_fence(source))
                .with_for_update(skip_locked=True)
            )
            is None
        ):
            return False
        changed = session.execute(
            update(Scan)
            .where(*_scan_fence(work))
            .values(
                version=work.version + 1,
                next_frame=advance.next_frame,
                byte_offset=advance.byte_offset,
                epoch=advance.epoch,
                state=advance.state,
            )
            .returning(Scan.collection_id)
        ).scalar_one_or_none()
        if changed is None:
            return False
        for position, boundary in advance.frames:
            changed = session.execute(
                update(Frame)
                .where(
                    Frame.collection_id == source.collection_id,
                    Frame.journal_id == source.journal_id,
                    Frame.directory_id == source.directory_id,
                    Frame.ordinal == position.ordinal,
                    Frame.byte_offset == position.byte_offset,
                    Frame.json_bytes == position.json_bytes,
                )
                .values(
                    epoch_attempt_id=source.attempt_id,
                    epoch_ordinal=boundary.epoch,
                    epoch_entry_kind=boundary.entry_kind,
                    epoch_json_sha256=boundary.json_sha256,
                )
                .returning(Frame.ordinal)
            ).scalar_one_or_none()
            if changed is None:
                raise RuntimeError("journal epoch publication lost its exact frame position")
    return True


def advance_upload_journal_epoch_scan(
    factory: SessionFactory,
    source: source_owner._UploadJournalValidationWork,
) -> bool:
    """Publish one bounded private boundary batch; return readiness for native replay."""
    work = _begin_scan(factory, source)
    if work is None:
        return False
    if not advance_retired_upload_journal_preparation(factory, source):
        return False
    if work.state in {"complete", "deferred"}:
        return True
    advance = _compute_scan(factory, work)
    if advance is None or not _commit_scan(factory, work, advance):
        return False
    return advance.state in {"complete", "deferred"}
