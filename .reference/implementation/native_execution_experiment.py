"""A disposable, real-Native experiment isolating parent progress publication.

This is not a production executor or a recovery implementation. The maintained
EntryReplay, every semantic child, prefix acceptance and independent result
reader run unchanged. Only the parent ValidationLedger is replaced by live
state and its progress hash by an explicitly volatile observation. Child
checkpoint encoding, private SQLite transactions, and repeated prefix checks
remain. No retained execution, F1, platform durability or speedup is proved.

Instrumentation patches one serializer for a serial experiment; do not run
these measurements concurrently in one Python process. Product files are not
modified. Physical synchronization is not observed by sqlite3 tracing.
"""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Iterable, Iterator
from contextlib import closing, contextmanager
from copy import deepcopy
from dataclasses import asdict, dataclass
from time import perf_counter_ns
from typing import Any
from unittest.mock import patch

from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    JournalSummary,
    append_checkpoint,
    append_correction,
    assertion_reference,
    create_journal,
    new_id,
    validate_journal,
)
from riverhog_provenance._entry_replay import install as install_replay
from riverhog_provenance._journal_prefix import PrefixState
from riverhog_provenance._session_checkpoint import (
    StaleValidationCheckpoint,
    ValidationCheckpoint,
    ValidationLedger,
    ValidationSnapshot,
)
from riverhog_provenance.journal_session import (
    JournalValidationBudget,
    JournalValidationSession,
)
from riverhog_provenance_contracts import ContractCatalog

_PARENT = re.compile(r"\bjournal_validation\b", re.IGNORECASE)
_WRITE = re.compile(r"^\s*(?:INSERT|UPDATE|DELETE|CREATE)\b", re.IGNORECASE)


@dataclass
class Measurements:
    sql_statements: int = 0
    parent_checkpoint_sql: int = 0
    parent_checkpoint_writes: int = 0
    parent_checkpoint_commits: int = 0
    transaction_begins: int = 0
    transaction_commits: int = 0
    commit_attempts: int = 0
    rollback_attempts: int = 0
    parent_encoding_calls: int = 0
    parent_encoding_octets: int = 0
    native_advance_calls: int = 0
    final_semantic_assertions: int = 0
    elapsed_ns: int = 0
    physical_sync_calls: None = None


class _ObservedConnection(sqlite3.Connection):
    measurements: Measurements
    conductor: _VolatileLedger | None = None
    _parent_write: bool = False
    fail_next_commit: bool = False

    def observe(self, statement: str) -> None:
        value = statement.strip().upper()
        stats = self.measurements
        stats.sql_statements += 1
        if _PARENT.search(statement):
            stats.parent_checkpoint_sql += 1
            if _WRITE.match(statement):
                stats.parent_checkpoint_writes += 1
                self._parent_write = True
        if value.startswith("BEGIN"):
            stats.transaction_begins += 1
        elif value == "COMMIT":
            stats.transaction_commits += 1
            stats.parent_checkpoint_commits += int(self._parent_write)
            self._parent_write = False
        elif value == "ROLLBACK":
            self._parent_write = False

    def commit(self) -> None:
        self.measurements.commit_attempts += 1
        if self.fail_next_commit:
            self.fail_next_commit = False
            raise sqlite3.OperationalError("injected pre-commit failure")
        super().commit()
        # A live control swap follows the successful private transaction. It
        # cannot get ahead of a failed commit and is never a durable receipt.
        if self.conductor is not None:
            self.conductor.receive_commit()

    def rollback(self) -> None:
        self.measurements.rollback_attempts += 1
        super().rollback()
        if self.conductor is not None:
            self.conductor.discard_pending()


class _VolatileLedger(ValidationLedger):
    """Single-writer live parent; no SQL parent table, encoder or recovery API."""

    def __init__(self, original: ValidationLedger) -> None:
        super().__init__(
            original.db,
            attempt_id=original.attempt_id,
            binding=original.binding,
            worker_epoch=original.worker_epoch,
        )
        self._current: ValidationSnapshot | None = None
        self._pending: tuple[ValidationSnapshot, PrefixState] | None = None
        self.accepted: PrefixState | None = None

    def _initialize(self) -> ValidationSnapshot:
        if not self.db.in_transaction:
            raise RuntimeError("initialization requires its private transaction")
        prefix, accepted = self._prefix()
        if accepted.frame_count:
            raise RuntimeError("initial parent requires an empty accepted prefix")
        snapshot = ValidationSnapshot(ValidationCheckpoint(), prefix)
        self._pending = snapshot, accepted
        return snapshot

    def load(self) -> ValidationSnapshot:
        self._detached()
        if self._current is None:
            raise RuntimeError("live parent has not been initialized")
        return self._current

    def _fence(self, before: ValidationSnapshot) -> None:
        if not self.db.in_transaction:
            raise RuntimeError("live parent requires its private transaction")
        if self._current != before or self._prefix()[0] != before.prefix:
            raise StaleValidationCheckpoint("live parent or prefix changed")

    def _publish(
        self,
        before: ValidationSnapshot,
        after: ValidationCheckpoint,
        *,
        prefix: tuple[str, bytes, str] | None = None,
    ) -> ValidationSnapshot:
        if not self.db.in_transaction or self._current != before:
            raise StaleValidationCheckpoint("live parent lost its private transaction")
        if after.version != before.checkpoint.version + 1:
            raise ValueError("live parent transition must advance once")
        selected = before.prefix if prefix is None else prefix
        actual, accepted = self._prefix()
        if selected != actual or accepted.frame_count != after.frame_ordinal:
            raise StaleValidationCheckpoint("live parent lost its exact accepted prefix")
        if self._pending is not None:
            raise RuntimeError("a live transition still awaits its private commit")
        snapshot = ValidationSnapshot(after, selected)
        self._pending = snapshot, accepted
        return snapshot

    def receive_commit(self) -> None:
        if self._pending is not None:
            self._current, self.accepted = self._pending
            self._pending = None

    def discard_pending(self) -> None:
        self._pending = None

    def replace_worker(self, worker_epoch: int) -> ValidationLedger:
        raise NotImplementedError("this disposable experiment has no worker recovery")


@dataclass(frozen=True)
class VolatileProgress:
    """Current execution observation; no persisted or resumable-prefix claim."""

    attempt_id: str
    worker_epoch: int
    phase: str
    completed_frames: int
    completed_octets: int
    observed_source_octets: int
    complete: bool


class _VolatileSession(JournalValidationSession):
    def _initialize(self, *, retained: bool) -> None:
        if retained:
            raise NotImplementedError("this experiment supports disposable streams only")
        db = self._store.db
        if not isinstance(db, _ObservedConnection):
            raise TypeError("the experiment requires its commit-receiving connection")
        conductor = _VolatileLedger(self._ledger)
        self._ledger = conductor
        db.conductor = conductor
        try:
            db.execute("BEGIN IMMEDIATE").close()
            install_replay(db)
            self._prefix._initialize()
            conductor._initialize()
            db.commit()
        except BaseException:
            db.rollback()
            raise

    def _progress(self, snapshot: ValidationSnapshot) -> VolatileProgress:
        conductor = self._ledger
        assert isinstance(conductor, _VolatileLedger)
        accepted = conductor.accepted
        assert accepted is not None
        state = snapshot.checkpoint
        return VolatileProgress(
            self.attempt_id,
            conductor.worker_epoch,
            state.phase,
            state.frame_ordinal,
            accepted.byte_count,
            self._input.cursor.byte_offset,
            state.phase == "done",
        )

    @classmethod
    def open_retained(cls, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("this experiment does not admit retained recovery")


@contextmanager
def observed_session(
    chunks: Iterable[bytes],
    *,
    volatile: bool,
    measurements: Measurements,
    catalog: ContractCatalog,
) -> Iterator[JournalValidationSession]:
    """Instrument one serial run, counting actual SQL and parent encoder output."""

    def connect(*args: Any, **kwargs: Any) -> _ObservedConnection:
        connection = sqlite3.connect(*args, factory=_ObservedConnection, **kwargs)
        connection.measurements = measurements
        connection.set_trace_callback(connection.observe)
        return connection

    encode = ValidationCheckpoint.to_json

    def measured_encode(state: ValidationCheckpoint) -> str:
        result = encode(state)
        measurements.parent_encoding_calls += 1
        measurements.parent_encoding_octets += len(result.encode())
        return result

    kind = _VolatileSession if volatile else JournalValidationSession
    with patch.object(ValidationCheckpoint, "to_json", measured_encode):
        with kind(
            chunks,
            attempt_id="e" * 32,
            catalog=catalog,
            connection_factory=connect,
        ) as session:
            yield session


def result_evidence(summary: JournalSummary) -> dict[str, Any]:
    """Bounded experiment fixtures permit complete evidence comparison."""
    return {
        "materialized": summary.materialize(),
        "origins": dict(summary.assertion_entries),
        "frames": [frame.json_bytes.hex() for frame in summary.frames],
        "journal_id": summary.journal_id,
        "sha256": summary.journal_sha256,
        "octets": summary.journal_bytes,
    }


def capture_examples(catalog: ContractCatalog) -> dict[str, bytes]:
    """Capture once, then supply these identical bytes to each measured arm."""
    observation = BoundedSourceObserver(catalog=catalog).observe(
        BytesSource(b"opaque primary bytes\x00\xff")
    )
    raw = create_journal(
        observation.graph_fragment(),
        recorded_by_agent_id=observation.observer_agent_id,
        catalog=catalog,
    )
    with validate_journal(raw, catalog=catalog) as summary:
        original = summary.graph["artifacts"][0]
        replacement = deepcopy(original)
        replacement["assertion_id"] = new_id()
        target = assertion_reference(summary, original["assertion_id"])
    corrected = append_correction(
        raw,
        [target],
        reason="Replace the documentary assertion while retaining its artifact.",
        recorded_by_agent_id=observation.observer_agent_id,
        assertions={"artifacts": [replacement]},
        catalog=catalog,
    )
    checkpointed = append_checkpoint(
        corrected,
        recorded_by_agent_id=observation.observer_agent_id,
        purpose="urn:reference:native-execution-experiment",
        catalog=catalog,
    )
    return {"small": raw, "correction": corrected, "journal_checkpoint": checkpointed}


def run_journal(
    raw: bytes, *, volatile: bool, catalog: ContractCatalog, rows: int = 64
) -> dict[str, Any]:
    """Measure one already captured input; never regenerate it between arms."""
    measurements = Measurements()
    started = perf_counter_ns()
    try:
        with observed_session(
            (raw,), volatile=volatile, measurements=measurements, catalog=catalog
        ) as session:
            budget = JournalValidationBudget(rows=rows)
            while True:
                measurements.native_advance_calls += 1
                if session.advance(budget).complete:
                    break
            with closing(session._store.db.execute("SELECT count(*) FROM assertions")) as cursor:
                measurements.final_semantic_assertions = cursor.fetchone()[0]
            reader = session.result()
        # This is deliberately after session close: the independent result owns
        # its original storage until this last reader actually receives close.
        with reader:
            outcome = {"success": result_evidence(reader)}
    except Exception as error:
        outcome = {"error_type": type(error).__name__, "error_text": str(error)}
    measurements.elapsed_ns = perf_counter_ns() - started
    return {"outcome": outcome, "measurements": asdict(measurements)}
