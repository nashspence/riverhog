"""Bounded execution composition of the container's existing preparation cursors."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from riverhog_client._prepared_primary_sqlite_database_reserve import _validate_plan
from riverhog_client._prepared_primary_sqlite_journal import _validate_control_prefixes
from riverhog_client._prepared_primary_sqlite_progress_journal import _validate_workspace
from riverhog_client._prepared_primary_types import (
    PrimaryStateBusy,
    PrimaryStateCapacity,
    PrimaryStateError,
)

if TYPE_CHECKING:
    from riverhog_client._prepared_primary_sqlite import TransactionalPrimaryFiles


@dataclass(frozen=True, slots=True)
class SqliteReadinessPlan:
    prefix_lengths: tuple[int, ...]
    journal_workspace_bytes: int
    free_pages: int
    target_pages: int
    database_workspace_bytes: int

    def __post_init__(self) -> None:
        _validate_control_prefixes(self.prefix_lengths)
        _validate_workspace(self.journal_workspace_bytes)
        _validate_plan(self.free_pages, self.target_pages, self.database_workspace_bytes)


@dataclass(frozen=True, slots=True)
class SqlitePreparationStep:
    stage: Literal["journal", "database", "ready"]
    done: bool


def step(files: TransactionalPrimaryFiles, plan: SqliteReadinessPlan) -> SqlitePreparationStep:
    """Advance one existing dependency cursor, or commit/recheck Ready.

    Journal warming costs the explicitly granted workspace; a database quantum
    inserts/deletes one known reserve row. This function never drains either
    cursor, scans payload/members, changes original grants or consumes input.
    The caller schedules subsequent execution steps ahead of dependent payload.
    Status and close do not call this execution path.
    """
    if type(plan) is not SqliteReadinessPlan:
        raise TypeError("prepared-primary readiness requires an exact plan")
    with files._mutex:
        if files is not files._root:
            raise PrimaryStateError("prepared-primary preparation requires the container root")
        if files._db().in_transaction:
            raise PrimaryStateBusy("prepared-primary preparation requires a settled transaction")
        before_journal, before_database = files.progress_journal(), files.database_reserve()
        grants = files.resource_grants()
        if (
            plan.target_pages > grants.database_bytes // 4096
            or plan.free_pages > grants.database_bytes // 4096
            or plan.journal_workspace_bytes > grants.database_bytes
        ):
            raise PrimaryStateCapacity("prepared-primary preparation exceeds original resources")
        if (
            plan.journal_workspace_bytes < before_journal.workspace_bytes
            or plan.free_pages < before_database.free_pages
            or plan.target_pages < before_database.target_pages
            or plan.database_workspace_bytes < before_database.workspace_bytes
        ):
            raise ValueError("prepared-primary preparation cannot lower retained reserves")

        from riverhog_client._prepared_primary_sqlite_database_reserve import _SIZE as database_size
        from riverhog_client._prepared_primary_sqlite_journal import _reservation
        from riverhog_client._prepared_primary_sqlite_operations import _SIZE as operations_size
        from riverhog_client._prepared_primary_sqlite_progress_journal import _SIZE as journal_size
        from riverhog_client._prepared_primary_sqlite_ready import _SIZE as ready_size
        from riverhog_client._prepared_primary_sqlite_resources import _SIZE as grants_size

        # The journal cursor's own fixed record is added by its owner. Select
        # the largest independent progress profile, including an atomic grant
        # increase and the largest already allocated control, before preparation.
        profiles = [plan.prefix_lengths, (grants_size, database_size, ready_size, operations_size)]
        _floor, representative = _reservation(files)
        if representative:
            row = files._one(
                "SELECT length(body),typeof(body) FROM primary_controls WHERE rowid=?",
                (representative,),
            )
            if row is None or row[1] != "blob" or type(row[0]) is not int or row[0] < 9:
                raise PrimaryStateError("prepared-primary progress control extent is unavailable")
            profiles.append((row[0], 16))
        bounded = [
            (files._guard.control_envelope((*profile, journal_size)), profile)
            for profile in profiles
        ]
        envelope, selected = max(bounded, key=lambda value: value[0].journal_bytes)
        if max(before_journal.minimum_bytes, envelope.journal_bytes) > grants.journal_bytes:
            raise PrimaryStateCapacity("prepared-primary progress profile exceeds journal grant")

        from riverhog_client._prepared_primary_sqlite_progress_journal import _backed

        journal_was_backed = before_journal.phase == "prepared" and _backed(
            files, before_journal.backed_bytes
        )
        journal = files.reserve_journal_step(
            prefix_lengths=selected, workspace_bytes=plan.journal_workspace_bytes
        )
        if not journal.done or not journal_was_backed or journal.reserve != before_journal:
            return SqlitePreparationStep("journal", False)
        database = files.reserve_database_step(
            free_pages=plan.free_pages,
            target_pages=plan.target_pages,
            workspace_bytes=plan.database_workspace_bytes,
        )
        if not database.done or database.reserve != before_database:
            return SqlitePreparationStep("database", False)
        files.prepare_physical_ready()
        return SqlitePreparationStep("ready", True)
