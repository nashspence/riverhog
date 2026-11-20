"""Select bounded due workflow hints independently of execution and write authority."""

from __future__ import annotations

from threading import Lock

from sqlalchemy import Select, literal_column, select
from sqlalchemy.exc import OperationalError

from riverhog_core.catalog_db import SessionFactory, catalog_write_contention, session_scope
from riverhog_core.catalog_models import CollectionUploadRecord

FinalizationDueCursor = tuple[str, int]


def finalization_due_query(
    now: str,
    *,
    limit: int,
    at: FinalizationDueCursor | None = None,
    after_time: str | None = None,
) -> Select[tuple[str | None, int]]:
    """Seek either one due-time equality prefix or the following timestamp range."""
    if type(limit) is not int or limit < 1:
        raise ValueError("finalization selection limit must be positive")
    if at is not None and after_time is not None:
        raise ValueError("finalization selection requires one exact seek range")
    query = select(
        CollectionUploadRecord.archive_next_attempt_at,
        CollectionUploadRecord.collection_id,
    ).where(
        CollectionUploadRecord.state == literal_column("'finalizing'"),
        CollectionUploadRecord.archive_phase.in_(
            (literal_column("'finalization_queued'"), literal_column("'retry_wait'"))
        ),
        CollectionUploadRecord.archive_next_attempt_at <= now,
    )
    # Separate equality/id and later-time ranges avoid SQLite filtering through
    # an earlier id prefix when a row-value comparison selects only the time key.
    if at is not None:
        query = query.where(
            CollectionUploadRecord.archive_next_attempt_at == at[0],
            CollectionUploadRecord.collection_id > at[1],
        )
    elif after_time is not None:
        query = query.where(CollectionUploadRecord.archive_next_attempt_at > after_time)
    return query.order_by(
        CollectionUploadRecord.archive_next_attempt_at,
        CollectionUploadRecord.collection_id,
    ).limit(limit)


class ArchiveFinalizationDue:
    """A volatile seek hint; current admission belongs to each point-selected parent.

    Two indexed ranges fill a bounded page; one further read can wrap an exhausted
    cursor. Rotating past busy or closed candidates gives independent due parents
    another turn without retaining work, permission, a queue or completion.
    A lost hint restarts the scan.
    """

    def __init__(self, factory: SessionFactory) -> None:
        self._factory = factory
        self._after: FinalizationDueCursor | None = None
        self._lock = Lock()

    def select(self, now: str, *, limit: int) -> tuple[int, ...]:
        if type(limit) is not int or limit < 1:
            raise ValueError("finalization selection limit must be positive")
        if not self._lock.acquire(blocking=False):
            return ()
        try:
            with session_scope(self._factory) as session:
                if self._after is None:
                    rows = list(session.execute(finalization_due_query(now, limit=limit)))
                else:
                    rows = list(
                        session.execute(finalization_due_query(now, limit=limit, at=self._after))
                    )
                    if len(rows) < limit:
                        rows.extend(
                            session.execute(
                                finalization_due_query(
                                    now,
                                    limit=limit - len(rows),
                                    after_time=self._after[0],
                                )
                            )
                        )
                    if not rows:
                        rows = list(session.execute(finalization_due_query(now, limit=limit)))
            if rows:
                when, collection_id = rows[-1]
                assert when is not None
                self._after = when, collection_id
            else:
                self._after = None
            return tuple(row.collection_id for row in rows)
        except OperationalError as error:
            if catalog_write_contention(error):
                return ()
            raise
        finally:
            self._lock.release()
