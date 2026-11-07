"""Checkpoint one exact frozen corpus input or one bounded Native phase.

The cursor follows original Core inputs. Native owns replay and global semantic
acceptance; completion here grants neither member relevance nor publication.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, replace
from typing import Any

from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_canonical_json import format_scalar, parse_scalar, require_canonical_json
from riverhog_provenance import JournalSummary, RetainedCorpusValidator

from riverhog_core.canonical_discovery_archive_work import ArchiveTaskKey, StaleArchiveTask
from riverhog_core.canonical_discovery_corpus_replay_growth import declare_replay_growth
from riverhog_core.canonical_discovery_history_inputs import _atomic, _one
from riverhog_core.canonical_discovery_selection import (
    CorpusSelectionWork,
    FrozenCorpusSelection,
    frozen_corpus_selection,
)

_PHASES = (
    "sources",
    "selections",
    "semantics",
    "dependencies",
    "derivation",
    "specialization",
    "complete",
)


@dataclass(frozen=True, slots=True)
class CorpusReplayProgress:
    version: int
    phase: str
    sources: int
    selections: int
    after: tuple[str, str] | None
    receipt_json: str | None


@dataclass(frozen=True, slots=True)
class PreparedCorpusReplay:
    key: ArchiveTaskKey
    expected: CorpusReplayProgress
    position: tuple[str, str] | None
    anchor_json: str | None
    complete: bool


class CorpusReplayWork:
    """One detached preparation under the application's serialized Native owner."""

    def __init__(
        self,
        selection: CorpusSelectionWork,
        manifest: FrozenCorpusSelection,
        corpus: RetainedCorpusValidator,
    ) -> None:
        self.selection, self.manifest, self.corpus = selection, manifest, corpus
        self._prepared: PreparedCorpusReplay | None = None


def initialize_corpus_replay(db: sqlite3.Connection) -> None:
    if db.in_transaction:
        raise ValueError("corpus replay initialization requires no active transaction")
    db.execute("""
        CREATE TABLE IF NOT EXISTS discovery_corpus_replay (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, worker_id TEXT NOT NULL,
            source_manifest_sha256 TEXT NOT NULL, selection_sha256 TEXT NOT NULL,
            version TEXT NOT NULL, phase TEXT NOT NULL,
            sources TEXT NOT NULL, selections TEXT NOT NULL,
            after_a TEXT, after_b TEXT, receipt_json TEXT,
            PRIMARY KEY(build_id,admission_sha256),
            CHECK((after_a IS NULL) = (after_b IS NULL))
        ) WITHOUT ROWID
    """).close()


def _fence(db: sqlite3.Connection, key: ArchiveTaskKey, owner: CorpusReplayWork) -> None:
    if frozen_corpus_selection(db, key, owner.selection) != owner.manifest:
        raise StaleArchiveTask("corpus replay has another exact original input freeze")


def _natural(value: Any) -> int:
    if type(value) is not str:
        raise StaleArchiveTask("corpus replay counter representation changed")
    try:
        return parse_scalar("nonnegative", value)
    except (TypeError, ValueError):
        raise StaleArchiveTask("corpus replay counter is not canonical") from None


def _input(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    phase: str,
    position: tuple[str, str],
) -> str | None:
    if phase == "sources":
        if position[1]:
            raise StaleArchiveTask("corpus source cursor has another original coordinate")
        sql = (
            "SELECT anchor_json FROM discovery_corpus_source_inputs "
            "WHERE build_id=? AND admission_sha256=? AND journal_id=?"
        )
        parameters: tuple[object, ...] = key.build_id, key.admission_sha256, position[0]
    else:
        sql = (
            "SELECT anchor_json FROM discovery_corpus_prefix_inputs "
            "WHERE build_id=? AND admission_sha256=? AND source_key=? AND prefix_sha256=?"
        )
        parameters = key.build_id, key.admission_sha256, *position
    row = _one(db, sql, parameters)
    return None if row is None else row[0]


def corpus_replay_progress(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: CorpusReplayWork
) -> CorpusReplayProgress:
    _fence(db, key, owner)
    row = _one(
        db,
        "SELECT worker_id,source_manifest_sha256,selection_sha256,version,phase,"
        "sources,selections,after_a,after_b,receipt_json FROM discovery_corpus_replay "
        "WHERE build_id=? AND admission_sha256=?",
        (key.build_id, key.admission_sha256),
    )
    expected = (
        key.worker_id,
        owner.manifest.sources.manifest_sha256,
        owner.manifest.selection_sha256,
    )
    if row is None or row[:3] != expected:
        raise StaleArchiveTask("corpus replay has another original manifest or worker")
    if row[4] not in _PHASES or (row[7] is None) != (row[8] is None):
        raise StaleArchiveTask("corpus replay phase or original cursor changed")
    progress = CorpusReplayProgress(
        _natural(row[3]),
        row[4],
        _natural(row[5]),
        _natural(row[6]),
        None if row[7] is None else (row[7], row[8]),
        row[9],
    )
    phase = _PHASES.index(progress.phase)
    source_count, selection_count = (
        owner.manifest.sources.source_count,
        owner.manifest.selection_count,
    )
    if (
        progress.sources > source_count
        or progress.selections > selection_count
        or (phase == 0 and progress.selections != 0)
        or (phase > 0 and progress.sources != source_count)
        or (phase > 1 and progress.selections != selection_count)
        or progress.version < progress.sources + progress.selections + phase
    ):
        raise StaleArchiveTask("corpus replay counters differ from independent frozen inputs")
    if phase < 2:
        count = progress.sources if phase == 0 else progress.selections
        if (progress.after is None) != (count == 0) or (progress.receipt_json is None) != (
            count == 0
        ):
            raise StaleArchiveTask("corpus replay lost its last original input receipt")
        if progress.after is not None and _input(db, key, progress.phase, progress.after) != (
            progress.receipt_json
        ):
            raise StaleArchiveTask("corpus replay receipt differs from its original input")
    elif progress.after is not None or progress.receipt_json is not None:
        raise StaleArchiveTask("corpus semantic phase retains an unrelated source cursor")
    return progress


def begin_corpus_replay(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: CorpusReplayWork
) -> CorpusReplayProgress:
    with _atomic(db):
        _fence(db, key, owner)
        db.execute(
            "INSERT OR IGNORE INTO discovery_corpus_replay VALUES("
            "?,?,?,?,?,'0','sources','0','0',NULL,NULL,NULL)",
            (
                key.build_id,
                key.admission_sha256,
                key.worker_id,
                owner.manifest.sources.manifest_sha256,
                owner.manifest.selection_sha256,
            ),
        ).close()
        return corpus_replay_progress(db, key, owner)


def _next(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    progress: CorpusReplayProgress,
) -> tuple[tuple[str, str], str] | None:
    if progress.phase not in ("sources", "selections"):
        return None
    parameters: tuple[object, ...] = key.build_id, key.admission_sha256
    if progress.phase == "sources":
        sql = (
            "SELECT journal_id,'',anchor_json FROM discovery_corpus_source_inputs "
            "WHERE build_id=? AND admission_sha256=? "
        )
        if progress.after is not None:
            sql += "AND journal_id>? "
            parameters += (progress.after[0],)
        row = _one(db, sql + "ORDER BY journal_id LIMIT 1", parameters)
    else:
        sql = (
            "SELECT source_key,prefix_sha256,anchor_json FROM discovery_corpus_prefix_inputs "
            "WHERE build_id=? AND admission_sha256=? "
        )
        if progress.after is None:
            row = _one(db, sql + "ORDER BY source_key,prefix_sha256 LIMIT 1", parameters)
        else:
            a, b = progress.after
            row = _one(
                db,
                sql + "AND source_key=? AND prefix_sha256>? ORDER BY prefix_sha256 LIMIT 1",
                (*parameters, a, b),
            )
            if row is None:
                row = _one(
                    db,
                    sql + "AND source_key>? ORDER BY source_key,prefix_sha256 LIMIT 1",
                    (*parameters, a),
                )
    return None if row is None else ((row[0], row[1]), row[2])


def next_corpus_replay_source(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: CorpusReplayWork
) -> HistoryJournalAnchor | None:
    """Request one complete qualified source, without opening or replaying it."""
    progress = corpus_replay_progress(db, key, owner)
    if progress.phase != "sources":
        return None
    item = _next(db, key, progress)
    return (
        None
        if item is None
        else HistoryJournalAnchor.from_mapping(require_canonical_json(item[1].encode()))
    )


def prepare_corpus_replay(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    owner: CorpusReplayWork,
    *,
    summary: JournalSummary | None = None,
    frame_budget: int = 1,
    row_budget: int = 64,
    byte_budget: int,
) -> PreparedCorpusReplay:
    """Run at most one Native quantum outside the Core transaction.

    The caller borrows only the requested exact complete source from its owning
    journal session. This function neither revalidates a journal nor retains its
    summary. Selected prefixes are registered separately from full sources.
    """
    owner._prepared = None
    if db.in_transaction:
        raise ValueError("corpus replay preparation requires no active transaction")
    for value in (frame_budget, row_budget, byte_budget):
        if type(value) is not int or value < 1:
            raise ValueError("corpus replay budgets must be positive integers")
    progress = corpus_replay_progress(db, key, owner)
    item = _next(db, key, progress)
    position, raw = (None, None) if item is None else item
    complete = False
    phase = progress.phase
    if phase in ("sources", "selections"):
        if raw is None:
            count = progress.sources if phase == "sources" else progress.selections
            expected = (
                owner.manifest.sources.source_count
                if phase == "sources"
                else owner.manifest.selection_count
            )
            if count != expected:
                raise StaleArchiveTask("corpus replay ended before independent input coverage")
            if summary is not None:
                raise StaleArchiveTask("corpus replay has no requested original source")
            complete = True
        else:
            anchor = HistoryJournalAnchor.from_mapping(require_canonical_json(raw.encode()))
            count = progress.sources if phase == "sources" else progress.selections
            limit = (
                owner.manifest.sources.source_count
                if phase == "sources"
                else owner.manifest.selection_count
            )
            if count >= limit or (phase == "sources" and position != (anchor.journal_id, "")):
                raise StaleArchiveTask("corpus replay exceeds its independent original inputs")
            if phase == "sources":
                if summary is None or summary.anchor != anchor.to_mapping():
                    raise StaleArchiveTask(
                        "corpus replay requires its exact complete qualified source"
                    )
                native = owner.corpus.advance_source(summary, frame_budget=frame_budget)
                if (
                    summary.anchor != anchor.to_mapping()
                    or native.journal_id != anchor.journal_id
                    or native.source_sha256 != anchor.prefix_sha256
                    or native.source_octets != anchor.prefix_bytes
                    or native.accepted_frames > anchor.through_sequence + 1
                    or (native.complete and native.accepted_frames != anchor.through_sequence + 1)
                ):
                    raise StaleArchiveTask("Native replay differs from the original full source")
                complete = native.complete
            else:
                if summary is not None:
                    raise StaleArchiveTask(
                        "selected prefix replay does not acquire a source summary"
                    )
                owner.corpus.select_source(anchor)
                complete = True
    else:
        if summary is not None:
            raise StaleArchiveTask("corpus semantic work does not acquire a source summary")
        if phase == "semantics":
            complete = owner.corpus.advance_semantics(
                row_budget=row_budget, byte_budget=byte_budget
            ).complete
        elif phase == "dependencies":
            complete = owner.corpus.advance_dependencies(
                row_budget=row_budget, byte_budget=byte_budget
            ).complete
        elif phase in ("derivation", "specialization", "complete"):
            complete = owner.corpus.advance_order(
                "specialization" if phase == "complete" else phase,
                row_budget=row_budget,
                byte_budget=byte_budget,
            ).complete
            if phase == "complete" and not complete:
                raise StaleArchiveTask("completed corpus lost its Native ordering terminal")
    page = PreparedCorpusReplay(key, progress, position, raw, complete)
    owner._prepared = page
    return page


def apply_corpus_replay(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    owner: CorpusReplayWork,
    page: PreparedCorpusReplay,
) -> CorpusReplayProgress:
    if owner._prepared is not page or page.key != key:
        raise StaleArchiveTask("corpus replay requires its exact detached preparation")
    with _atomic(db):
        before = corpus_replay_progress(db, key, owner)
        if before != page.expected:
            raise StaleArchiveTask("corpus replay preparation has another current cursor")
        expected = None if page.position is None else (page.position, page.anchor_json)
        if _next(db, key, before) != expected:
            raise StaleArchiveTask("corpus replay original next input changed")
        if before.phase == "complete":
            return before
        after = replace(before, version=before.version + 1)
        if page.complete:
            if page.position is not None:
                after = replace(
                    after,
                    sources=before.sources + int(before.phase == "sources"),
                    selections=before.selections + int(before.phase == "selections"),
                    after=page.position,
                    receipt_json=page.anchor_json,
                )
            else:
                after = replace(
                    after,
                    phase=_PHASES[_PHASES.index(before.phase) + 1],
                    after=None,
                    receipt_json=None,
                )
        a, b = (None, None) if after.after is None else after.after
        changed = db.execute(
            "UPDATE discovery_corpus_replay SET version=?,phase=?,sources=?,selections=?,"
            "after_a=?,after_b=?,receipt_json=? "
            "WHERE build_id=? AND admission_sha256=? AND worker_id=? AND version=?",
            (
                format_scalar("nonnegative", after.version),
                after.phase,
                format_scalar("nonnegative", after.sources),
                format_scalar("nonnegative", after.selections),
                a,
                b,
                after.receipt_json,
                key.build_id,
                key.admission_sha256,
                key.worker_id,
                format_scalar("nonnegative", before.version),
            ),
        ).rowcount
        if changed != 1:
            raise StaleArchiveTask("corpus replay worker changed before publication")
        return after


declare_replay_growth(
    {
        "begin": begin_corpus_replay,
        "progress": corpus_replay_progress,
        "next": next_corpus_replay_source,
        "prepare": prepare_corpus_replay,
        "apply": apply_corpus_replay,
    }
)
