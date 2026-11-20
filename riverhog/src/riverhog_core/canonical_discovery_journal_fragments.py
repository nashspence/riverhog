"""Read bounded physical fact bytes under their exact original journal source."""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass

from riverhog_archive_contracts import HistoryJournalAnchor

from riverhog_core.canonical_discovery_fact_work import (
    FACT_FRAGMENT_BYTES,
    FactSegmentConflict,
    FactStreamKey,
    _identity,
)
from riverhog_core.canonical_discovery_journal_fragment_growth import (
    declare_growth_for_journal_fragment,
)
from riverhog_core.canonical_discovery_journal_lookup import (
    JournalAssertionHeader,
    documentary_journal_assertion,
)
from riverhog_core.canonical_discovery_journal_work import (
    JournalTaskKey,
    QualifiedJournalSource,
    StaleJournalTask,
    _one,
)


class JournalFactCapacity(ValueError):
    """The next physical fragment exceeds the supplied execution capacity."""


@dataclass(frozen=True, slots=True)
class JournalFactFragment:
    assertion: JournalAssertionHeader
    stream: FactStreamKey
    ordinal: int
    fragments: int
    octets: int
    exact_bytes: bytes

    @property
    def terminal(self) -> bool:
        return self.ordinal == self.fragments


def read_journal_fact_fragment(
    db: sqlite3.Connection,
    key: JournalTaskKey,
    source: QualifiedJournalSource,
    anchor: HistoryJournalAnchor,
    assertion_id: str,
    stream: FactStreamKey,
    *,
    ordinal: int,
    octet_budget: int,
) -> JournalFactFragment:
    """Detach one physical fragment or its explicit end, without decoding a value.

    The caller retains its exact stream and next ordinal across work boundaries.
    Missing or damaged work never becomes an empty value. Reading a terminal
    proves this stream's end, not that the caller has visited every prior byte.
    """
    if type(ordinal) is not int or ordinal < 0:
        raise ValueError("fact fragment ordinal must be nonnegative")
    if type(octet_budget) is not int or octet_budget < 1:
        raise ValueError("fact fragment execution capacity must be positive octets")
    if not isinstance(stream, FactStreamKey):
        raise TypeError("fact fragment requires an exact producer stream key")
    _identity(stream.kind)
    _identity(stream.record_key)
    assertion = documentary_journal_assertion(db, key, source, anchor, assertion_id)
    if assertion is None:
        raise StaleJournalTask("fact assertion is not documentary at the requested exact prefix")
    identity = (key.build_id, assertion.key.task_id, stream.kind, stream.record_key)
    row = _one(
        db,
        "SELECT s.fragments,s.octets,s.last_fragment_bytes,s.complete,s.terminal_version,"
        "t.terminal_version FROM discovery_fact_streams s JOIN discovery_fact_tasks t "
        "ON t.build_id=s.build_id AND t.task_id=s.task_id "
        "WHERE s.build_id=? AND s.task_id=? AND s.kind=? AND s.record_key=?",
        identity,
    )
    if row is None:
        raise FactSegmentConflict("fact stream has no exact completed producer record")
    fragments, octets, tail, complete, terminal, task_terminal = row
    if (
        any(type(value) is not int or value < 0 for value in row[:3])
        or complete != 1
        or type(terminal) is not int
        or type(task_terminal) is not int
        or not 0 <= terminal < task_terminal
        or fragments != (octets + FACT_FRAGMENT_BYTES - 1) // FACT_FRAGMENT_BYTES
        or tail != ((octets - 1) % FACT_FRAGMENT_BYTES + 1 if octets else 0)
    ):
        raise FactSegmentConflict("fact stream differs from its exact completed terminal")
    if ordinal > fragments:
        raise ValueError("fact fragment ordinal exceeds its exact stream terminal")
    if ordinal == fragments:
        excess = _one(
            db,
            "SELECT ordinal FROM discovery_fact_segments "
            "WHERE build_id=? AND task_id=? AND kind=? AND record_key=? AND ordinal>=? "
            "ORDER BY ordinal LIMIT 1",
            (*identity, ordinal),
        )
        if excess is not None:
            raise FactSegmentConflict("fact stream has fragments beyond its exact terminal")
        return JournalFactFragment(assertion, stream, ordinal, fragments, octets, b"")
    expected = tail if ordinal == fragments - 1 else FACT_FRAGMENT_BYTES
    if expected > octet_budget:
        raise JournalFactCapacity("next physical fact fragment exceeds execution capacity")
    part = _one(
        db,
        "SELECT s.ordinal,length(s.exact_bytes),s.sha256,s.staged_version,"
        "CASE WHEN length(s.exact_bytes)=? THEN s.exact_bytes ELSE NULL END,p.staged_version "
        "FROM discovery_fact_segments s LEFT JOIN discovery_fact_segments p "
        "ON p.build_id=s.build_id AND p.task_id=s.task_id AND p.kind=s.kind "
        "AND p.record_key=s.record_key AND p.ordinal=s.ordinal-1 "
        "WHERE s.build_id=? AND s.task_id=? AND s.kind=? AND s.record_key=? AND s.ordinal=?",
        (expected, *identity, ordinal),
    )
    if (
        part is None
        or part[0] != ordinal
        or part[1] != expected
        or type(part[4]) is not bytes
        or type(part[3]) is not int
        or not ordinal <= part[3] < terminal
        or ordinal > 0
        and (type(part[5]) is not int or not 0 <= part[5] < part[3])
        or hashlib.sha256(part[4]).hexdigest() != part[2]
    ):
        raise FactSegmentConflict("fact fragment differs from its exact stream terminal")
    return JournalFactFragment(assertion, stream, ordinal, fragments, octets, part[4])


declare_growth_for_journal_fragment(globals())
