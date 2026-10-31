"""Bounded source registration and fact extraction over one qualified journal.

The caller owns the durable workspace, source lease and current archive/build
fences. This driver owns each short SQLite work transaction and retains only
the current decoded entry and current assertion compiler between advances.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

from riverhog_growth_contracts import (
    COUNT,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)

from riverhog_core.canonical_discovery_declared_source_work import (
    DeclaredAssertionSource,
    prepare_declared_source_page,
)
from riverhog_core.canonical_discovery_fact_work import (
    FactTaskKey,
    StaleFactTask,
    fact_extraction_cursor,
    fact_task_progress,
    finish_assertion,
)
from riverhog_core.canonical_discovery_journal_work import (
    JournalProgress,
    JournalTaskKey,
    QualifiedJournalSource,
    StaleJournalTask,
    acknowledge_assertion,
    apply_journal_page,
    begin_journal,
    finish_journal,
    journal_progress,
    pending_journal_assertion,
    prepare_journal_page,
)
from riverhog_core.canonical_discovery_source_work import (
    AdmittedAssertionSource,
    apply_source_page,
    prepare_lexical_source_page,
)


@contextmanager
def _write(db: sqlite3.Connection) -> Iterator[None]:
    if db.in_transaction:
        raise ValueError("journal extraction owns its short work transactions")
    db.execute("BEGIN IMMEDIATE")
    try:
        yield
        db.commit()
    except BaseException:
        db.rollback()
        raise


@dataclass(frozen=True, slots=True)
class JournalExtractionAdvance:
    phase: str
    parent: JournalProgress
    child: FactTaskKey | None
    actions: int


class JournalFactExtraction:
    """Resume registered source/fact cursors without scanning completed work.

    Finish pending children before registering another source page. In
    particular, the next entry is not loaded while the previous entry still
    has pending children. Reopening reconstructs only the current pending
    assertion; the Native source remains borrowed from the caller.
    """

    def __init__(
        self,
        db: sqlite3.Connection,
        key: JournalTaskKey,
        source: QualifiedJournalSource,
    ) -> None:
        if db.in_transaction:
            raise ValueError("journal extraction requires no active owner transaction")
        self.db, self.key, self.source = db, key, source
        self._atom: AdmittedAssertionSource | None = None
        self._declared: DeclaredAssertionSource | None = None
        self._child: FactTaskKey | None = None
        with _write(db):
            begin_journal(db, key, source)

    def _forget_child(self) -> None:
        self._atom = self._declared = self._child = None

    @declare_growth(
        Declaration(
            id="riverhog-server.discovery-journal-extraction-step.growth/v1",
            authority="riverhog-server",
            subjects=(
                "riverhog_core.canonical_discovery_journal_extraction."
                "JournalFactExtraction.advance",
            ),
            scope="one source-qualified Core journal/fact driver advance",
            starts_at="receive a live qualified Native source and current private work database",
            ends_at="commit one bounded page or terminal and return its source progress",
            dimensions=(
                Dimension("action_budget", COUNT, "Requested positive physical-action count."),
            ),
            bounds=(
                Bound(
                    "sql_statements",
                    COUNT,
                    "upper",
                    Expression(
                        "sum",
                        terms=(
                            Expression("constant", value=64, unit=COUNT),
                            Expression(
                                "product",
                                terms=(
                                    Expression("constant", value=64),
                                    Expression("dimension", name="action_budget"),
                                ),
                            ),
                        ),
                    ),
                    "Fenced Core work reads and bounded stack, fragment and control writes.",
                ),
                Bound(
                    "returned_rows",
                    COUNT,
                    "upper",
                    Expression(
                        "sum",
                        terms=(
                            Expression("constant", value=32, unit=COUNT),
                            Expression(
                                "product",
                                terms=(
                                    Expression("constant", value=32),
                                    Expression("dimension", name="action_budget"),
                                ),
                            ),
                        ),
                    ),
                    "Every Core source/task/stream/stack lookup is point-addressed.",
                ),
                Bound(
                    "work_actions",
                    COUNT,
                    "upper",
                    Expression("dimension", name="action_budget"),
                    "One page or one separately budgeted child/parent terminal per call.",
                ),
                Bound(
                    "commits",
                    COUNT,
                    "upper",
                    Expression("constant", value=1, unit=COUNT),
                    "At most one short owned work transaction commits.",
                ),
                Bound(
                    "rollbacks",
                    COUNT,
                    "upper",
                    Expression("constant", value=1, unit=COUNT),
                    "At most one failed owned work transaction is rolled back.",
                ),
            ),
            conditions=(
                "The caller holds current archive/build authority and the exact workspace lease.",
                "Native owns the completed validation and frozen summary.",
                "The current private work schema has its exact keys and partial pending index.",
                "Retain the driver/cache; restart reconstructs only unfinished work.",
                "The driver has exclusive use of the current source-entry cache while it runs.",
            ),
            invariants=(
                "Pending child extraction precedes the next source registration page.",
                "A decoded source entry remains current through all its pending children.",
                "Assertion preparation and classification run outside the owner write transaction.",
                "Finishing an already extracted child does not reconstruct its source atom.",
            ),
            limitations=(
                "Counters cover Core work SQL; Native storage and fences are separate.",
                "Encoding, hashing, indexes, CPU, memory and backend I/O are separate costs.",
                "SQL excludes storage-engine work and implicit transaction statements.",
                "Composite source-union, authorization and publication conformance are excluded.",
            ),
        )
    )
    def advance(self, *, action_budget: int) -> JournalExtractionAdvance:
        """Perform one bounded source/fact page or one child/parent terminal."""
        if type(action_budget) is not int or action_budget < 1:
            raise ValueError("journal extraction action budget must be positive")
        if self.db.in_transaction:
            raise ValueError("prepare extraction outside the owner write transaction")
        self.source.check_open()
        parent = journal_progress(self.db, self.key)
        if parent.complete:
            self._forget_child()
            return JournalExtractionAdvance("complete", parent, None, 0)
        pending = pending_journal_assertion(self.db, self.key)
        if pending is None:
            self._forget_child()
            if parent.pending:
                raise StaleJournalTask("parent has unaccounted pending assertion children")
            if parent.cursor.phase == "done":
                with _write(self.db):
                    parent = finish_journal(self.db, self.key, expected_version=parent.version)
                return JournalExtractionAdvance("complete", parent, None, 1)
            page = prepare_journal_page(self.db, self.key, self.source, action_budget=action_budget)
            with _write(self.db):
                parent = apply_journal_page(self.db, self.key, self.source, page)
            return JournalExtractionAdvance("registered", parent, None, len(page.actions))
        child_progress = fact_task_progress(self.db, pending.key)
        if child_progress.complete:
            with _write(self.db):
                parent = acknowledge_assertion(self.db, self.key, pending.assertion_id)
            self._forget_child()
            return JournalExtractionAdvance("acknowledged", parent, pending.key, 1)
        progress, cursor = fact_extraction_cursor(self.db, pending.key)
        if cursor.category == "done":
            with _write(self.db):
                finish_assertion(
                    self.db,
                    pending.key,
                    source_terminal=pending.canonical_stream_ref,
                    expected_version=progress.version,
                )
            return JournalExtractionAdvance("sealed-assertion", parent, pending.key, 1)
        if self._child != pending.key:
            self._forget_child()
            origin = json.loads(pending.origin_entry_json)
            atom = AdmittedAssertionSource(
                self.key.source_key,
                origin["json_sha256"],
                self.source.assertion(pending.sequence, pending.group, pending.item),
            )
            if (
                atom.assertion_id != pending.assertion_id
                or atom.canonical_sha256 != pending.canonical_stream_ref
            ):
                raise StaleFactTask(
                    "pending assertion differs from its admitted source coordinates"
                )
            self._atom, self._child = atom, pending.key
        assert self._atom is not None
        if cursor.category in {"canonical", "scalars"}:
            fact_page = prepare_lexical_source_page(
                self.db, pending.key, self._atom, action_budget=action_budget
            )
        else:
            if self._declared is None:
                self._declared = DeclaredAssertionSource(self._atom)
            fact_page = prepare_declared_source_page(
                self.db, pending.key, self._declared, action_budget=action_budget
            )
        if not fact_page.actions:
            raise StaleFactTask("unfinished assertion has no source continuation")
        with _write(self.db):
            apply_source_page(self.db, pending.key, fact_page)
        return JournalExtractionAdvance("facts", parent, pending.key, len(fact_page.actions))
