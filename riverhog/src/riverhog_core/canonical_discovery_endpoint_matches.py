"""Compare one inverse candidate with its original object-reference bytes."""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from typing import Literal

from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_canonical_json import canonical_json_bytes, format_scalar
from riverhog_provenance_contracts import ExternalReference, LocalReference

from riverhog_core.canonical_discovery_endpoint_index import (
    EndpointIndexWork,
    _output,
    next_endpoint_candidate,
)
from riverhog_core.canonical_discovery_endpoint_match_growth import (
    declare_growth_for_endpoint_match,
)
from riverhog_core.canonical_discovery_endpoint_source import JournalEndpoint, read_journal_endpoint
from riverhog_core.canonical_discovery_journal_fragments import JournalFactCapacity
from riverhog_core.canonical_discovery_journal_lookup import (
    _source,
    documentary_journal_assertion,
)
from riverhog_core.canonical_discovery_journal_work import JournalTaskKey, StaleJournalTask


@dataclass(frozen=True, slots=True)
class EndpointMatchStep:
    after: tuple[str, str] | None
    endpoint: JournalEndpoint | None
    complete: bool


def _target_bytes(target: LocalReference | ExternalReference) -> bytes:
    if isinstance(target, LocalReference) and target.scope == "local":
        return target.object_id.encode("ascii")
    if isinstance(target, ExternalReference) and target.scope == "external":
        return canonical_json_bytes(target.model_dump(mode="json"))
    raise TypeError("endpoint matching requires a typed exact object reference")


def _equal_bytes(left: bytes, right: bytes) -> bool:
    return left == right


def advance_endpoint_match(
    db: sqlite3.Connection,
    key: JournalTaskKey,
    work: EndpointIndexWork,
    anchor: HistoryJournalAnchor,
    *,
    role: str,
    target: LocalReference | ExternalReference,
    assertion_state: Literal["documentary", "effective"],
    octet_budget: int,
    after: tuple[str, str] | None = None,
) -> EndpointMatchStep:
    """Inspect one candidate, returning explicit progress for an unmatched row.

    This is an object-reference accessor for fixed scope selectors, not a
    relevance traversal. The caller binds its saved position to the exact
    source, prefix, role, target and assertion-state request. It also owns any
    member/import/causal conditions and current authorization.
    """
    if assertion_state not in {"documentary", "effective"}:
        raise ValueError("endpoint assertion state must be documentary or effective")
    if type(octet_budget) is not int or octet_budget < 1:
        raise ValueError("endpoint matching capacity must be positive")
    expected = _target_bytes(target)
    if len(expected) > octet_budget:
        raise JournalFactCapacity("exact endpoint operand exceeds execution capacity")
    _source(db, key, work.source, anchor)
    index_role = "context" if role.startswith("context:") else role
    row = next_endpoint_candidate(
        db,
        key,
        work,
        role=index_role,
        target_scope=target.scope,
        target_type=target.object_type,
        target_sha256=hashlib.sha256(expected).hexdigest(),
        after=after,
    )
    if row is None:
        return EndpointMatchStep(after, None, True)
    position = (row[2], row[3])
    # Qualify interval metadata before allowing it to suppress an otherwise
    # matching candidate. The full source includes future births at this prefix.
    original = documentary_journal_assertion(db, key, work.source, work.anchor, row[2])
    if original is None or (row[4], row[5], row[12], row[13]) != (
        original.key.task_id,
        original.canonical_stream_ref,
        format_scalar("nonnegative", original.born_sequence),
        None
        if original.retired_sequence is None
        else format_scalar("nonnegative", original.retired_sequence),
    ):
        raise StaleJournalTask("endpoint candidate differs from its original assertion interval")
    if original.born_sequence > anchor.through_sequence or (
        assertion_state == "effective"
        and original.retired_sequence is not None
        and original.retired_sequence <= anchor.through_sequence
    ):
        return EndpointMatchStep(position, None, False)
    endpoint = read_journal_endpoint(
        db, key, work.source, anchor, row[2], row[3], octet_budget=octet_budget
    )
    if endpoint.assertion != original or _output(key, endpoint) != row:
        raise StaleJournalTask("endpoint candidate differs from its original fact record")
    actual = (
        endpoint.edge.exact_foreign_reference
        if endpoint.edge.target_scope == "external"
        else endpoint.edge.target_id.encode("ascii")
    )
    assert actual is not None
    selected = _equal_bytes(actual, expected) and (role == "context" or endpoint.edge.role == role)
    return EndpointMatchStep(position, endpoint if selected else None, False)


declare_growth_for_endpoint_match(globals())
