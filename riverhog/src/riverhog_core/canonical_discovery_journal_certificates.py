"""Closed, original-source-qualified rank evidence for Native discovery replay."""

from __future__ import annotations

import hashlib
import sqlite3

from riverhog_canonical_json import canonical_json_bytes, parse_scalar
from riverhog_provenance.epoch_boundaries import JOURNAL_EPOCH_BOUNDARY_ALGORITHM
from riverhog_provenance.epoch_rank import EPOCH_RANK_ALGORITHM
from riverhog_provenance.epoch_union import EPOCH_UNION_ALGORITHM, GRAPH_KINDS
from riverhog_provenance.errors import JournalValidationCapacity
from riverhog_provenance.graph_certificates import (
    InvalidJournalGraphCertificate,
    JournalGraphCertificate,
    JournalGraphCertificateBinding,
)
from riverhog_provenance.topology_checkpoint import validate_checkpoint_digest
from riverhog_provenance.topology_facts import TOPOLOGY_CAPTURE_ALGORITHM
from riverhog_provenance_contracts import require_canonical_uuid_urn

from riverhog_core import canonical_discovery_journal_epochs as epochs
from riverhog_core import canonical_discovery_journal_rank as rank
from riverhog_core.canonical_discovery_archive_work import ArchiveTaskKey, StaleArchiveTask
from riverhog_core.canonical_discovery_history_inputs import _one
from riverhog_core.canonical_discovery_journal_certificates_growth import (
    declare_growth_for_journal_certificates,
)
from riverhog_core.canonical_discovery_metadata import DiscoveryMetadata

_MANIFEST_COLUMNS = (
    "worker_id",
    "directory_id",
    "algorithm",
    "binding_json",
    "union_version",
    *rank._COLUMNS,
)


def _manifest(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: rank.DiscoveryJournalRankWork,
) -> tuple[object, ...]:
    p = rank.discovery_journal_rank_progress(db, key, work)
    if p.state not in {"complete", "deferred"}:
        raise InvalidJournalGraphCertificate("original rank preparation is not complete")
    row = _one(
        db,
        "SELECT " + ",".join(_MANIFEST_COLUMNS) + " FROM discovery_journal_rank "
        "WHERE build_id=? AND admission_sha256=? AND journal_id=?",
        rank._key(key, work),
    )
    if row is None or row[5:] != rank._controls(p):
        raise InvalidJournalGraphCertificate("original rank changed during certificate admission")
    return tuple(row)


class DiscoveryJournalGraphCertificates:
    """Borrow exact metadata custody; return only detached, bounded point evidence."""

    def __init__(self, metadata: DiscoveryMetadata, work: rank.DiscoveryJournalRankWork) -> None:
        self.metadata, self.work, self._key = metadata, work, metadata.key
        with metadata.read() as (db, key):
            self._manifest = _manifest(db, key, work)
        self._mode = self._manifest[6]
        source = work.union.topology.epochs.source.binding
        encoded = canonical_json_bytes(
            {
                "source": source.to_json(),
                "algorithms": [
                    JOURNAL_EPOCH_BOUNDARY_ALGORITHM,
                    TOPOLOGY_CAPTURE_ALGORITHM,
                    EPOCH_UNION_ALGORITHM,
                    EPOCH_RANK_ALGORITHM,
                ],
                "rank": list(self._manifest),
            }
        )
        self._binding = JournalGraphCertificateBinding(source, hashlib.sha256(encoded).hexdigest())

    @property
    def binding(self) -> JournalGraphCertificateBinding:
        return self._binding

    def _current(self, db: sqlite3.Connection, key: ArchiveTaskKey) -> None:
        if key != self._key or _manifest(db, key, self.work) != self._manifest:
            raise InvalidJournalGraphCertificate("original rank manifest is no longer current")

    def verify_current(self) -> bool:
        try:
            with self.metadata.read() as (db, key):
                self._current(db, key)
            return True
        except (StaleArchiveTask, InvalidJournalGraphCertificate):
            return False

    def edge(
        self,
        *,
        frame_ordinal: int,
        frame_sha256: str,
        kind: str,
        source: str,
        target: str,
        byte_budget: int,
    ) -> JournalGraphCertificate | None:
        try:
            return self._edge(
                frame_ordinal=frame_ordinal,
                frame_sha256=frame_sha256,
                kind=kind,
                source=source,
                target=target,
                byte_budget=byte_budget,
            )
        except (StaleArchiveTask, ValueError, TypeError) as error:
            raise InvalidJournalGraphCertificate("original rank evidence is invalid") from error

    def _edge(
        self,
        *,
        frame_ordinal: int,
        frame_sha256: str,
        kind: str,
        source: str,
        target: str,
        byte_budget: int,
    ) -> JournalGraphCertificate | None:
        if type(byte_budget) is not int or byte_budget < 4096:
            raise JournalValidationCapacity("rank point reads need their fixed metadata admission")
        if type(frame_ordinal) is not int or not 0 <= frame_ordinal < parse_scalar(
            "nonnegative",
            self.binding.source.frame_count,
        ):
            raise InvalidJournalGraphCertificate("graph frame is outside its original source")
        validate_checkpoint_digest(frame_sha256)
        if kind not in GRAPH_KINDS:
            raise InvalidJournalGraphCertificate("unknown Native graph kind")
        require_canonical_uuid_urn(source)
        require_canonical_uuid_urn(target)
        with self.metadata.read() as (db, key):
            self._current(db, key)
            if self._mode == "deferred":
                return None
            _, boundary = epochs.discovery_journal_epoch_frame(
                db,
                key,
                self.work.union.topology.epochs,
                frame_ordinal,
            )
            if boundary.json_sha256 != frame_sha256:
                raise InvalidJournalGraphCertificate("rank frame differs from original bytes")
            prefix = rank._graph_key(key, self.work, boundary.epoch, kind)
            graph = _one(
                db,
                "SELECT state,vertices,arcs,settled,visited FROM discovery_journal_rank_graphs "
                "WHERE build_id=? AND admission_sha256=? AND journal_id=? AND epoch=? AND kind=?",
                prefix,
            )
            if graph is None:
                raise InvalidJournalGraphCertificate("original rank graph result is missing")
            state = graph[0]
            vertices, arcs, settled, visited = (
                parse_scalar("nonnegative", value) for value in graph[1:]
            )
            if (
                state not in {"acyclic", "cyclic"}
                or rank._graph(db, key, self.work, boundary.epoch, kind) != (vertices, arcs)
                or not 0 <= settled <= vertices
                or not 0 <= visited <= arcs
                or _one(
                    db,
                    "SELECT 1 FROM discovery_journal_union_arcs "
                    "WHERE build_id=? AND admission_sha256=? AND journal_id=? AND epoch=? "
                    "AND kind=? AND source_id=? AND target_id=?",
                    (*prefix, source, target),
                )
                is None
            ):
                raise InvalidJournalGraphCertificate("rank lost its exact original union links")
            if state == "cyclic":
                return None
            left = rank._vertex(db, key, self.work, boundary.epoch, kind, source)
            right = rank._vertex(db, key, self.work, boundary.epoch, kind, target)
            if (
                settled != vertices
                or visited != arcs
                or left.degree
                or right.degree
                or left.ready
                or right.ready
                or left.rank is None
                or right.rank is None
            ):
                raise InvalidJournalGraphCertificate("original acyclic rank evidence is incomplete")
        proof = JournalGraphCertificate(
            self.binding.checkpoint_sha256(),
            frame_ordinal,
            frame_sha256,
            boundary.epoch,
            kind,
            source,
            target,
            left.rank,
            right.rank,
            vertices,
            arcs,
        )
        proof.verify(
            self.binding,
            frame_ordinal=frame_ordinal,
            frame_sha256=frame_sha256,
            kind=kind,
            source=source,
            target=target,
        )
        if proof.encoded_octets() > byte_budget:
            raise JournalValidationCapacity("rank point metadata exceeds its admitted work bytes")
        return proof


declare_growth_for_journal_certificates(globals())
