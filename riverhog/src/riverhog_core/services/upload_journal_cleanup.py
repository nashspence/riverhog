"""Bounded retirement of private preparation rows from replaced attempts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)
from sqlalchemy import Table, delete, literal, select, tuple_, update
from sqlalchemy.orm import Session
from state_schema import read_snapshot

from riverhog_core.catalog_db import SessionFactory
from riverhog_core.catalog_models import (
    CollectionUploadProvenanceActiveEdgeRecord,
    CollectionUploadProvenanceEpochArcRecord,
    CollectionUploadProvenanceEpochGraphRecord,
    CollectionUploadProvenanceEpochRankGraphRecord,
    CollectionUploadProvenanceEpochRankVertexRecord,
    CollectionUploadProvenanceEpochVertexRecord,
    CollectionUploadProvenanceTopologyFactRecord,
)
from riverhog_core.catalog_models import (
    CollectionUploadProvenanceEpochScanRecord as Scan,
)
from riverhog_core.catalog_models import (
    CollectionUploadProvenanceJournalRecord as Journal,
)
from riverhog_core.catalog_models import (
    CollectionUploadProvenanceRetiredPreparationRecord as Retirement,
)

from . import collection_uploads as source_owner

RETIREMENT_ALGORITHM = "riverhog-journal-preparation-retirement/v1"
_ROW_BUDGET = 64
_TABLES = (
    CollectionUploadProvenanceEpochRankVertexRecord.__table__,
    CollectionUploadProvenanceEpochRankGraphRecord.__table__,
    CollectionUploadProvenanceEpochArcRecord.__table__,
    CollectionUploadProvenanceEpochVertexRecord.__table__,
    CollectionUploadProvenanceEpochGraphRecord.__table__,
    CollectionUploadProvenanceActiveEdgeRecord.__table__,
    CollectionUploadProvenanceTopologyFactRecord.__table__,
)


def enqueue_retirement(session: Session, scan: Scan) -> None:
    """Retain the old private attempt identity in the same reset transaction."""
    key = (scan.collection_id, scan.journal_id, scan.attempt_id)
    row = session.get(Retirement, key)
    if row is not None:
        if row.directory_id != scan.directory_id or row.algorithm != RETIREMENT_ALGORITHM:
            raise RuntimeError("retired preparation binding differs from its attempt")
        return
    session.add(
        Retirement(
            collection_id=scan.collection_id,
            journal_id=scan.journal_id,
            retired_attempt_id=scan.attempt_id,
            directory_id=scan.directory_id,
            algorithm=RETIREMENT_ALGORITHM,
            version=0,
            phase=0,
        )
    )


@dataclass(frozen=True, slots=True)
class _Work:
    source: source_owner._UploadJournalValidationWork
    retired_attempt_id: str
    directory_id: str
    version: int
    phase: int


def _current(
    session: Session, source: source_owner._UploadJournalValidationWork, *, lock: bool = False
) -> bool:
    query = (
        select(Journal.collection_id)
        .join(
            Scan,
            (Scan.collection_id == Journal.collection_id) & (Scan.journal_id == Journal.journal_id),
        )
        .where(
            *source_owner._upload_journal_validation_fence(source),
            Scan.attempt_id == source.attempt_id,
            Scan.directory_id == source.directory_id,
        )
    )
    if lock:
        query = query.with_for_update(of=Journal, skip_locked=True)
    return session.scalar(query) is not None


def _begin(
    factory: SessionFactory, source: source_owner._UploadJournalValidationWork
) -> tuple[bool, _Work | None]:
    with read_snapshot(factory) as session:
        if not _current(session, source):
            return False, None
        with session.execute(
            select(
                Retirement.retired_attempt_id,
                Retirement.directory_id,
                Retirement.version,
                Retirement.phase,
                Retirement.algorithm,
            )
            .where(
                Retirement.collection_id == source.collection_id,
                Retirement.journal_id == source.journal_id,
            )
            .order_by(Retirement.retired_attempt_id)
            .limit(1)
        ) as rows:
            row = rows.one_or_none()
    if row is None:
        return True, None
    attempt, directory, version, phase, algorithm = row
    if (
        not attempt
        or not directory
        or attempt == source.attempt_id
        or algorithm != RETIREMENT_ALGORITHM
        or not 0 <= phase < len(_TABLES)
    ):
        raise RuntimeError("retired preparation checkpoint differs from its owned attempt")
    return True, _Work(source, attempt, directory, version, phase)


def _fence(work: _Work) -> tuple[Any, ...]:
    return (
        Retirement.collection_id == work.source.collection_id,
        Retirement.journal_id == work.source.journal_id,
        Retirement.retired_attempt_id == work.retired_attempt_id,
        Retirement.directory_id == work.directory_id,
        Retirement.algorithm == RETIREMENT_ALGORITHM,
        Retirement.version == work.version,
        Retirement.phase == work.phase,
    )


@declare_growth(
    Declaration(
        id="riverhog-server.sqlite-retired-journal-preparation.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.services.upload_journal_cleanup._delete_batch",),
        scope="native-sqlite-one-private-retired-attempt-relation",
        starts_at="delete an admitted batch from one exactly selected retired preparation relation",
        ends_at="return the actual deletion count after closing the native result",
        dimensions=(
            Dimension("retained_rows", COUNT, "Rows retained in the selected relation."),
            Dimension("row_budget", COUNT, "Maximum rows admitted for this deletion."),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "equal",
                Expression("constant", value=1, unit=COUNT),
                "One exact-attempt indexed limited deletion with RETURNING.",
            ),
            Bound(
                "deleted_rows",
                COUNT,
                "upper",
                Expression("dimension", name="row_budget"),
                "Only the admitted rows from the selected retired attempt are removed.",
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "Source octets and semantic assertion payloads are not loaded.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        Expression("constant", value=1024, unit=COUNT),
                        Expression(
                            "product",
                            terms=(
                                Expression("constant", value=4096),
                                Expression("dimension", name="row_budget"),
                            ),
                        ),
                        Expression(
                            "product",
                            terms=(
                                Expression("constant", value=256, unit=COUNT),
                                Expression(
                                    "log2_ceil",
                                    terms=(
                                        Expression(
                                            "sum",
                                            terms=(
                                                Expression("constant", value=2, unit=COUNT),
                                                Expression("dimension", name="retained_rows"),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Instruction envelope for bounded indexed removal and index maintenance.",
            ),
        ),
        conditions=(
            "The seven owning preparation relations retain their exact-attempt primary keys.",
            "The source owner fences the selected retired attempt away from the current attempt.",
            "Native UUID identities and owning scalar domains apply; row_budget is 1 through 64.",
        ),
        invariants=(
            "Current and other retired attempts and retained source custody are unchanged.",
            "No graph, assertion payload or terminal cardinality scan is required.",
        ),
        limitations=(
            "Queue selection, fences, checkpoint publication and retries are separate costs.",
            "Every retired row and attempt is charged across calls.",
            "Filesystem reclamation, archive data and semantic workspace cleanup are separate.",
            "SQLite instructions exclude internal key comparisons, page I/O and physical CPU time.",
        ),
    )
)
def _delete_batch(session: Session, work: _Work, *, row_budget: int) -> int:
    if type(row_budget) is not int or not 1 <= row_budget <= _ROW_BUDGET:
        raise ValueError("retired preparation row budget must be between 1 and 64")
    if work.retired_attempt_id == work.source.attempt_id or not 0 <= work.phase < len(_TABLES):
        raise RuntimeError("current preparation cannot be retired")
    table = cast(Table, _TABLES[work.phase])
    keys = tuple(table.primary_key.columns)
    target = (
        table.c.collection_id == work.source.collection_id,
        table.c.journal_id == work.source.journal_id,
        table.c.attempt_id == work.retired_attempt_id,
    )
    window = select(*keys).where(*target).order_by(*keys).limit(row_budget)
    with session.execute(
        delete(table).where(*target, tuple_(*keys).in_(window)).returning(literal(1))
    ) as rows:
        return sum(1 for _ in rows)


def _commit(factory: SessionFactory, work: _Work) -> int | None:
    with factory() as session, session.begin():
        if not _current(session, work.source, lock=True):
            return None
        changed = session.execute(
            update(Retirement)
            .where(*_fence(work))
            .values(version=work.version + 1)
            .returning(Retirement.retired_attempt_id)
        ).scalar_one_or_none()
        if changed is None:
            return None
        count = _delete_batch(session, work, row_budget=_ROW_BUDGET)
        if count < _ROW_BUDGET:
            key = (
                Retirement.collection_id == work.source.collection_id,
                Retirement.journal_id == work.source.journal_id,
                Retirement.retired_attempt_id == work.retired_attempt_id,
            )
            if work.phase + 1 == len(_TABLES):
                session.execute(delete(Retirement).where(*key))
            else:
                session.execute(update(Retirement).where(*key).values(phase=work.phase + 1))
        return count


def advance_retired_upload_journal_preparation(
    factory: SessionFactory, source: source_owner._UploadJournalValidationWork
) -> bool:
    """Reclaim at most 64 private rows in one phase; retain exact source custody."""
    current, work = _begin(factory, source)
    if not current:
        return False
    if work is None:
        return True
    _commit(factory, work)
    return False
