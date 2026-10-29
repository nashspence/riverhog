"""Owned journal padding, derived progress minimum and observed physical backing."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Literal

from riverhog_client._prepared_primary_types import (
    PrimaryStateBusy,
    PrimaryStateCapacity,
    PrimaryStateError,
)

if TYPE_CHECKING:
    import sqlite3

    from riverhog_client._prepared_primary_sqlite import TransactionalPrimaryFiles

Phase = Literal["unprepared", "building", "prepared", "depleted"]
_PHASES: tuple[Phase, ...] = ("unprepared", "building", "prepared", "depleted")
_TAG = b"RHJPAD01"
_SIZE = 64
_BLOCK = 4096
_MAXIMUM = (1 << 63) - 1


@dataclass(frozen=True, slots=True)
class SqliteProgressJournal:
    phase: Phase = "unprepared"
    grant_revision: int = 0
    minimum_bytes: int = 0
    workspace_bytes: int = 0
    pad_rows: int = 0
    backed_bytes: int = 0
    minimum_pages: int = 0

    def encode(self) -> bytes:
        values = (
            self.minimum_bytes,
            self.workspace_bytes,
            self.pad_rows,
            self.backed_bytes,
            self.minimum_pages,
        )
        if (
            self.phase not in _PHASES
            or type(self.grant_revision) is not int
            or not 0 <= self.grant_revision <= (1 << 64) - 1
            or any(type(v) is not int or not 0 <= v <= _MAXIMUM for v in values)
        ):
            raise PrimaryStateError("prepared-primary progress journal record is invalid")
        if self.phase == "unprepared":
            if any(values):
                raise PrimaryStateError(
                    "prepared-primary unprepared progress journal has live obligations"
                )
        elif (
            self.minimum_bytes == 0
            or self.minimum_pages == 0
            or self.workspace_bytes < _BLOCK
            or self.workspace_bytes % _BLOCK
            or self.pad_rows > self.workspace_bytes // _BLOCK
            or (
                self.phase == "prepared"
                and (
                    self.backed_bytes < self.minimum_bytes
                    or self.pad_rows != self.workspace_bytes // _BLOCK
                )
            )
        ):
            raise PrimaryStateError("prepared-primary progress journal profile is invalid")
        return _TAG + b"".join(
            v.to_bytes(8, "big") for v in (_PHASES.index(self.phase), self.grant_revision, *values)
        )


@dataclass(frozen=True, slots=True)
class SqliteProgressJournalStep:
    reserve: SqliteProgressJournal
    done: bool


def read(files: TransactionalPrimaryFiles) -> SqliteProgressJournal:
    with files._mutex:
        grants = files.resource_grants()
        row = files._one("SELECT progress_journal FROM primary_format WHERE id=1")
        raw = None if row is None else row[0]
        if type(raw) is not bytes or len(raw) != _SIZE or raw[:8] != _TAG:
            raise PrimaryStateError("prepared-primary progress journal record is invalid")
        values = tuple(int.from_bytes(raw[i : i + 8], "big") for i in range(8, _SIZE, 8))
        if values[0] >= len(_PHASES):
            raise PrimaryStateError("prepared-primary progress journal phase is invalid")
        result = SqliteProgressJournal(_PHASES[values[0]], *values[1:])
        result.encode()
        if (
            result.grant_revision != grants.revision
            or result.minimum_bytes > grants.journal_bytes
            or result.backed_bytes > grants.journal_bytes
            or result.workspace_bytes > grants.database_bytes
        ):
            raise PrimaryStateError("prepared-primary progress journal grant changed")
        return result


def save(
    db: sqlite3.Connection, previous: SqliteProgressJournal, selected: SqliteProgressJournal
) -> None:
    with db.blobopen("primary_format", "progress_journal", 1) as blob:
        if len(blob) != _SIZE or blob.read(_SIZE) != previous.encode():
            raise PrimaryStateError("prepared-primary progress journal changed during execution")
        blob.seek(0)
        blob.write(selected.encode())


def revised(current: SqliteProgressJournal, revision: int) -> SqliteProgressJournal:
    return replace(
        current,
        grant_revision=revision,
        phase="depleted" if current.phase == "prepared" else current.phase,
    )


def _backed(files: TransactionalPrimaryFiles, floor: int) -> bool:
    _database, journal = files._guard.file_space()
    return journal.end_bytes >= floor and journal.allocated_bytes >= floor


def require(files: TransactionalPrimaryFiles) -> SqliteProgressJournal:
    """Point admission only; retained size or EOF alone cannot provide backing."""
    with files._mutex:
        reserve = read(files)
        if reserve.phase != "prepared" or not _backed(files, reserve.backed_bytes):
            raise PrimaryStateCapacity("prepared-primary progress journal backing is unavailable")
        return reserve


def step(
    files: TransactionalPrimaryFiles,
    *,
    prefix_lengths: tuple[int, ...],
    workspace_bytes: int,
) -> SqliteProgressJournalStep:
    """Allocate one known padding row, or warm the explicitly bounded workspace.

    Each allocation and its cursor commit together. Warming touches the known
    fixed extents in one transaction, with work proportional to the granted
    workspace, not the archive. Native bootstrap grants separately bound all
    structural work, reads, writes, callbacks and recovery.
    """
    from riverhog_client._prepared_primary_sqlite_journal import (
        _retention,
        _validate_control_prefixes,
    )
    from riverhog_client._prepared_primary_sqlite_ready import deplete

    _validate_control_prefixes(prefix_lengths)
    if (
        type(workspace_bytes) is not int
        or not _BLOCK <= workspace_bytes <= _MAXIMUM
        or workspace_bytes % _BLOCK
    ):
        raise ValueError("prepared-primary journal workspace must be a positive page multiple")
    with files._mutex:
        if files is not files._root:
            raise PrimaryStateError("prepared-primary progress journal requires the container root")
        if files._db().in_transaction:
            raise PrimaryStateBusy(
                "prepared-primary progress journal requires a settled transaction"
            )
        current = read(files)
        if workspace_bytes < current.workspace_bytes:
            raise ValueError("prepared-primary progress journal cannot lower its workspace")
        # The fixed readiness write itself must fit before its transaction starts.
        envelope = files._guard.control_envelope((*prefix_lengths, _SIZE))
        grants = files.resource_grants()
        minimum = max(current.minimum_bytes, envelope.journal_bytes)
        if minimum > grants.journal_bytes or workspace_bytes > grants.database_bytes:
            raise PrimaryStateCapacity("prepared-primary progress journal exceeds retained grants")
        selected = replace(
            current,
            phase="building",
            minimum_bytes=minimum,
            minimum_pages=max(current.minimum_pages, envelope.database_pages),
            workspace_bytes=workspace_bytes,
        )
        if (
            current.phase == "prepared"
            and replace(selected, phase="prepared") == current
            and _backed(files, current.backed_bytes)
        ):
            return SqliteProgressJournalStep(current, True)
        if current.pad_rows < workspace_bytes // _BLOCK:
            selected = replace(selected, pad_rows=current.pad_rows + 1)
            with files._transaction(preparing=True) as db:
                deplete(files)
                db.execute(
                    "INSERT INTO primary_journal_pad(id,body) VALUES (?,zeroblob(4096))",
                    (selected.pad_rows,),
                ).close()
                save(db, current, selected)
            return SqliteProgressJournalStep(selected, False)
        with _retention(files, -1):
            with files._transaction(preparing=True) as db:
                deplete(files)
                for identifier in range(1, selected.pad_rows + 1):
                    with db.blobopen("primary_journal_pad", "body", identifier) as blob:
                        if len(blob) != _BLOCK:
                            raise PrimaryStateError(
                                "prepared-primary journal padding extent changed"
                            )
                        original = blob.read(_BLOCK)
                        blob.seek(0)
                        blob.write(original)
                save(db, current, selected)
            _database, journal = files._guard.file_space()
            observed = journal.end_bytes
            if observed > grants.journal_bytes:
                from riverhog_client._prepared_primary_sqlite_journal import _limit

                # Retain only the grant through a valid fixed update.
                _limit(files._db(), grants.journal_bytes)
                files.sync()
                raise PrimaryStateCapacity("prepared-primary journal warming exceeds retention")
            if observed < minimum or not _backed(files, max(observed, current.backed_bytes)):
                raise PrimaryStateCapacity(
                    "prepared-primary progress journal needs larger backed workspace"
                )
        prepared = replace(
            selected, phase="prepared", backed_bytes=max(observed, current.backed_bytes)
        )
        with files._transaction(preparing=True) as db:
            deplete(files)
            save(db, selected, prepared)
        if not _backed(files, prepared.backed_bytes):
            with files._transaction(preparing=True) as db:
                deplete(files)
                save(db, prepared, replace(prepared, phase="depleted"))
            raise PrimaryStateCapacity("prepared-primary progress journal lost backing")
        return SqliteProgressJournalStep(prepared, True)
