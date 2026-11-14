"""Read one original typed Native edge for source-normal endpoint indexing."""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass

from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_canonical_json import format_scalar, require_canonical_json
from riverhog_provenance import inspect_documentary_node
from riverhog_provenance_contracts import require_canonical_uuid_urn

from riverhog_core.canonical_discovery_endpoint_growth import declare_growth_for_endpoint
from riverhog_core.canonical_discovery_fact_work import FactSegmentConflict, FactStreamKey
from riverhog_core.canonical_discovery_journal_fragments import read_journal_fact_fragment
from riverhog_core.canonical_discovery_journal_lookup import JournalAssertionHeader
from riverhog_core.canonical_discovery_journal_work import JournalTaskKey, QualifiedJournalSource
from riverhog_core.canonical_discovery_rows import _ID_EDGES, _REFERENCE_EDGES, CoreEdge


@dataclass(frozen=True, slots=True)
class JournalEndpoint:
    assertion: JournalAssertionHeader
    record_key: str
    edge: CoreEdge
    target_sha256: str
    target_octets: int

    @property
    def index_role(self) -> str:
        return "context" if self.edge.role.startswith("context:") else self.edge.role

    @property
    def target_stream(self) -> FactStreamKey:
        return FactStreamKey(
            "edge-foreign" if self.edge.target_scope == "external" else "edge-target",
            self.record_key,
        )


def read_journal_endpoint(
    db: sqlite3.Connection,
    key: JournalTaskKey,
    source: QualifiedJournalSource,
    anchor: HistoryJournalAnchor,
    assertion_id: str,
    record_key: str,
    *,
    octet_budget: int,
) -> JournalEndpoint:
    """Detach one fixed-domain edge, with no assertion or profile reconstruction.

    Native UUIDs, role enums, object kinds and signed-64-bit sequence references
    bound each of these four control fields to one physical fragment. Variable
    lexical/profile values retain their separate segmented representation.
    The caller supplies per-fragment execution capacity and owns publication,
    semantic resolution and original-byte equality checks for candidate digests.
    """
    controls = {}
    assertion = None
    for kind in ("edge-header", "edge-role", "edge-target", "edge-foreign"):
        stream = FactStreamKey(kind, record_key)
        part = read_journal_fact_fragment(
            db,
            key,
            source,
            anchor,
            assertion_id,
            stream,
            ordinal=0,
            octet_budget=octet_budget,
        )
        if part.fragments > 1:
            raise FactSegmentConflict("edge control exceeds its current Native grammar")
        if not part.terminal:
            end = read_journal_fact_fragment(
                db,
                key,
                source,
                anchor,
                assertion_id,
                stream,
                ordinal=1,
                octet_budget=octet_budget,
            )
            if (end.assertion, end.fragments, end.octets) != (
                part.assertion,
                part.fragments,
                part.octets,
            ) or not end.terminal:
                raise FactSegmentConflict("edge control terminal changed during preparation")
        if assertion is not None and part.assertion != assertion:
            raise FactSegmentConflict("edge controls have different original assertion bindings")
        assertion = part.assertion
        controls[kind] = part.exact_bytes
    assert assertion is not None
    try:
        header = require_canonical_json(controls["edge-header"])
        if (
            not isinstance(header, dict)
            or set(header) != {"path_node", "ordinal", "target_type", "target_scope"}
            or type(header["path_node"]) is not int
            or header["path_node"] < 0
            or type(header["ordinal"]) is not int
            or header["ordinal"] < 0
            or format_scalar("nonnegative", header["ordinal"]) != record_key
            or header["target_scope"] not in {"local", "external"}
            or type(header["target_type"]) is not str
            or not header["target_type"]
        ):
            raise ValueError
        role = controls["edge-role"].decode("ascii")
        target = controls["edge-target"].decode("ascii")
        require_canonical_uuid_urn(target)
        roles = set(_ID_EDGES.get(assertion.kind, {})) | set(
            _REFERENCE_EDGES.get(assertion.kind, ())
        )
        if assertion.kind == "activity":
            roles.add("association")
            if role.startswith("context:"):
                roles.add(role)
        if assertion.kind == "extension":
            roles.add("value")
        if role not in roles:
            raise ValueError
        foreign = controls["edge-foreign"]
        if header["target_scope"] == "external":
            reference = inspect_documentary_node(require_canonical_json(foreign)).reference
            if (
                reference is None
                or reference["object_id"] != target
                or reference["object_type"] != header["target_type"]
            ):
                raise ValueError
            exact = foreign
        else:
            if foreign:
                raise ValueError
            exact = controls["edge-target"]
    except (ValueError, TypeError, KeyError, UnicodeError):
        raise FactSegmentConflict(
            "endpoint controls differ from their original Native grammar"
        ) from None
    edge = CoreEdge(
        role,
        target,
        header["target_type"],
        header["target_scope"],
        foreign or None,
    )
    return JournalEndpoint(
        assertion,
        record_key,
        edge,
        hashlib.sha256(exact).hexdigest(),
        len(exact),
    )


declare_growth_for_endpoint(globals())
