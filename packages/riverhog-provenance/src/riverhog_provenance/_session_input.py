"""Bounded source cursors in an owning retained validation workspace."""

from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from riverhog_canonical_json import canonical_json_bytes

from ._entry_replay import EntryReplayCapacity
from ._session_checkpoint import StaleValidationCheckpoint, ValidationSnapshot
from .errors import IncompleteSourceError, ProvenanceValidationError
from .framing import JournalFramePosition, JournalFramingCursor, advance_journal_framing
from .journal import JournalFrame

if TYPE_CHECKING:
    from ._session_checkpoint import ValidationLedger
    from .journal_session import JournalValidationSource

ALGORITHM = "riverhog-retained-validation-input/v1"
_HEADER_OCTETS = 8192


@dataclass(frozen=True)
class _InputState:
    cursor: JournalFramingCursor = JournalFramingCursor()
    pending: JournalFramePosition | None = None
    complete: bool = False

    def encode(self) -> str:
        return canonical_json_bytes(
            {
                "algorithm": ALGORITHM,
                "byte_offset": str(self.cursor.byte_offset),
                "frame_start": None
                if self.cursor.frame_start is None
                else str(self.cursor.frame_start),
                "frame_count": str(self.cursor.frame_count),
                "pending": None
                if self.pending is None
                else [
                    str(self.pending.ordinal),
                    str(self.pending.byte_offset),
                    str(self.pending.json_bytes),
                ],
                "complete": self.complete,
            }
        ).decode()

    @classmethod
    def decode(cls, raw: str) -> _InputState:
        def natural(value: object) -> int:
            if type(value) is not str or not value.isascii() or not value.isdecimal():
                raise ValueError("source cursor scalar")
            parsed = int(value)
            if str(parsed) != value:
                raise ValueError("noncanonical source cursor scalar")
            return parsed

        try:
            if type(raw) is not str or len(raw.encode()) > _HEADER_OCTETS:
                raise ValueError("source cursor envelope")
            value = json.loads(raw)
            if type(value) is not dict or value.get("algorithm") != ALGORITHM:
                raise ValueError("source cursor algorithm")
            start = value["frame_start"]
            pending = value["pending"]
            if pending is not None and (type(pending) is not list or len(pending) != 3):
                raise ValueError("source cursor pending frame")
            state = cls(
                JournalFramingCursor(
                    natural(value["byte_offset"]),
                    None if start is None else natural(start),
                    natural(value["frame_count"]),
                ),
                None if pending is None else JournalFramePosition(*(natural(v) for v in pending)),
                value["complete"],
            )
            if type(state.complete) is not bool or state.encode() != raw:
                raise ValueError("noncanonical source cursor")
            return state
        except (ValueError, TypeError, KeyError, OverflowError) as exc:
            raise RuntimeError("corrupt retained validation source cursor") from exc


def install(ledger: ValidationLedger) -> None:
    if not ledger.db.in_transaction:
        raise RuntimeError("source cursor initialization requires its owner transaction")
    ledger.db.execute(
        "CREATE TABLE journal_validation_input("
        "singleton INTEGER PRIMARY KEY CHECK(singleton=1),checkpoint TEXT NOT NULL)"
    ).close()
    ledger.db.execute(
        "INSERT INTO journal_validation_input VALUES(1,?)", (_InputState().encode(),)
    ).close()


class RetainedInput:
    """Source bytes remain with the trusted owner; only a fixed cursor is stored."""

    def __init__(self, ledger: ValidationLedger, source: JournalValidationSource) -> None:
        self.ledger, self.source = ledger, source
        self.binding = source.binding
        self.state = self._load()
        self._check(ledger.load())

    @property
    def cursor(self) -> JournalFramingCursor:
        return self.state.cursor

    @property
    def pending(self) -> bool:
        return self.state.pending is not None

    @property
    def complete(self) -> bool:
        return self.state.complete

    def _load(self) -> _InputState:
        if self.ledger.db.in_transaction:
            raise RuntimeError("source cursor loading requires a closed owner transaction")
        with closing(
            self.ledger.db.execute(
                "SELECT substr(checkpoint,1,?) FROM journal_validation_input WHERE singleton=1",
                (_HEADER_OCTETS + 1,),
            )
        ) as cursor:
            row = cursor.fetchone()
        if row is None:
            raise RuntimeError("retained source cursor is missing")
        return _InputState.decode(row[0])

    def _check(self, before: ValidationSnapshot) -> None:
        from ._journal_prefix import PrefixState

        state = self.state
        accepted = PrefixState.from_json(before.prefix[2])
        cursor, pending = state.cursor, state.pending
        if self.source.binding != self.binding:
            raise StaleValidationCheckpoint("retained source binding changed")
        if not accepted.byte_count <= cursor.byte_offset <= int(self.binding.source_bytes):
            raise RuntimeError("source cursor differs from the accepted prefix or retained extent")
        if state.complete and (
            cursor.byte_offset != int(self.binding.source_bytes) or cursor.frame_start is not None
        ):
            raise RuntimeError("source cursor has an incomplete terminal extent")
        if pending is None:
            if cursor.frame_count != accepted.frame_count:
                raise RuntimeError("source cursor lost its pending frame")
            if cursor.frame_start not in (None, accepted.byte_count):
                raise RuntimeError("partial source frame differs from the accepted prefix")
            if cursor.frame_start is None and cursor.byte_offset != accepted.byte_count:
                raise RuntimeError("source cursor has an unaccounted range")
        elif (
            cursor.frame_start is not None
            or cursor.frame_count != pending.ordinal + 1
            or cursor.byte_offset != pending.byte_offset + pending.json_bytes + 2
            or not accepted.frame_count - 1 <= pending.ordinal <= accepted.frame_count
            or pending.ordinal == accepted.frame_count
            and pending.byte_offset != accepted.byte_count
            or pending.ordinal == accepted.frame_count - 1
            and cursor.byte_offset != accepted.byte_count
        ):
            raise RuntimeError("pending source frame differs from its accepted-prefix link")

    def _publish(self, before: ValidationSnapshot, after: _InputState) -> ValidationSnapshot:
        old = self.state.encode()
        encoded = after.encode()
        if len(encoded.encode()) > _HEADER_OCTETS:
            raise EntryReplayCapacity("source cursor cannot admit its owning scalar widths")
        db = self.ledger.db
        try:
            db.execute("BEGIN IMMEDIATE").close()
            self.ledger._fence(before)
            with closing(
                db.execute(
                    "UPDATE journal_validation_input SET checkpoint=? "
                    "WHERE singleton=1 AND checkpoint=?",
                    (encoded, old),
                )
            ) as rows:
                if rows.rowcount != 1:
                    raise StaleValidationCheckpoint("source cursor proposal was superseded")
            changed = self.ledger._publish(
                before, replace(before.checkpoint, version=before.checkpoint.version + 1)
            )
            db.commit()
        except BaseException:
            db.rollback()
            raise
        self.state = after
        return changed

    def _available(self, before: ValidationSnapshot, octets: int) -> int:
        control = 16384 + 16 * (
            len(self.ledger.binding)
            + len(self.state.encode().encode())
            + len(before.checkpoint.to_json().encode())
            + sum(len(v.encode()) if isinstance(v, str) else len(v) for v in before.prefix)
        )
        if octets <= control:
            raise EntryReplayCapacity("source cursor cannot admit its fixed owner metadata")
        return octets - control

    def synchronize(self, before: ValidationSnapshot, octets: int) -> ValidationSnapshot | None:
        self._check(before)
        pending = self.state.pending
        if pending is None or pending.ordinal == before.checkpoint.frame_ordinal:
            return None
        self._available(before, octets)
        return self._publish(before, replace(self.state, pending=None))

    def scan(
        self, before: ValidationSnapshot, octets: int, source_octets: int
    ) -> ValidationSnapshot:
        self._check(before)
        if self.pending or self.complete:
            raise RuntimeError("retained source already has a pending frame or terminal state")
        remaining = int(self.binding.source_bytes) - self.cursor.byte_offset
        size = min(remaining, source_octets, self._available(before, octets))
        raw = self.source.read_window(self.cursor.byte_offset, size) if size else b""
        if type(raw) is not bytes or not (1 <= len(raw) <= size if size else len(raw) == 0):
            raise IncompleteSourceError("retained validation source did not supply its window")
        step = advance_journal_framing(
            self.cursor, raw, byte_budget=octets, frame_budget=1, at_end=len(raw) == remaining
        )
        if step.failure is not None:
            raise ProvenanceValidationError(step.failure)
        pending = step.positions[0] if step.positions else None
        return self._publish(before, _InputState(step.cursor, pending, step.complete))

    def frame(self, octets: int) -> JournalFrame:
        pending = self.state.pending
        if pending is None:
            raise RuntimeError("retained source has no complete frame")
        if 2 * pending.json_bytes + 65536 > octets:
            raise EntryReplayCapacity("journal validation cannot admit its existing frame atom")
        raw = self.source.read_exact(pending.byte_offset + 1, pending.json_bytes)
        if type(raw) is not bytes or len(raw) != pending.json_bytes:
            raise IncompleteSourceError("retained validation source did not supply its exact frame")
        frame = JournalFrame(raw)
        try:
            _ = frame.document
        except (ValueError, TypeError) as exc:
            raise ProvenanceValidationError(str(exc)) from exc
        return frame

    def admitted(self) -> None:
        pass
