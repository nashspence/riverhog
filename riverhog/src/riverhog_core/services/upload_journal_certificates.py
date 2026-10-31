"Closed, source-fenced rank reads for the native finite replay engine."

from __future__ import annotations

import hashlib
from typing import Any

from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
    growth_binding,
)
from riverhog_provenance.errors import JournalValidationCapacity
from riverhog_provenance.graph_certificates import (
    InvalidJournalGraphCertificate,
    JournalGraphCertificate,
    JournalGraphCertificateBinding,
)
from riverhog_provenance_contracts import ContractCatalog, require_canonical_uuid_urn
from sqlalchemy import and_, select
from sqlalchemy.orm import aliased
from state_schema import read_snapshot

from riverhog_core.catalog_db import SessionFactory
from riverhog_core.catalog_models import CollectionUploadProvenanceEpochArcRecord as Arc
from riverhog_core.catalog_models import CollectionUploadProvenanceEpochGraphRecord as Graph
from riverhog_core.catalog_models import CollectionUploadProvenanceEpochRankGraphRecord as Result
from riverhog_core.catalog_models import CollectionUploadProvenanceEpochRankVertexRecord as Ranked
from riverhog_core.catalog_models import CollectionUploadProvenanceEpochScanRecord as Scan
from riverhog_core.catalog_models import CollectionUploadProvenanceFrameRecord as Frame
from riverhog_core.catalog_models import CollectionUploadProvenanceJournalRecord as Journal

from . import collection_uploads as source_owner
from . import upload_journal_rank as rank
from .upload_journal_replay import _binding

_MANIFEST_FIELDS = (
    "version",
    "algorithm",
    "state",
    "next_frame",
    "byte_offset",
    "epoch",
    "topology_algorithm",
    "topology_state",
    "topology_next_frame",
    "topology_cursor_json",
    "union_algorithm",
    "union_state",
    "union_next_frame",
    "union_cursor_json",
    "rank_algorithm",
    "rank_state",
    "rank_cursor_json",
)
_EDGE_METADATA_OCTETS = 4096


def _lifecycle_declaration(mode: str) -> Declaration:
    constructing = mode == "__init__"

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    def add(*terms: Expression) -> Expression:
        return Expression("sum", terms=terms)

    def octets(name: str) -> Expression:
        return dimension(name) if constructing else Expression("constant", value=0, unit=OCTETS)

    return Declaration(
        id="riverhog-server.graph-certificate-" + mode.strip("_").replace("_", "-") + ".growth/v1",
        authority="riverhog-server",
        subjects=(
            "riverhog_core.services.upload_journal_certificates.UploadJournalGraphCertificates."
            + mode,
        ),
        scope=("sqlite source-fenced certificate construction and manifest currentness"),
        starts_at=(
            "accept the exact detached source and supplied sealed catalog or frozen manifest"
        ),
        ends_at=("return the provider/currentness or reject missing, unready or changed metadata"),
        dimensions=(
            Dimension(
                "retained_rows", COUNT, "Rows across the catalog relations this operation probes."
            ),
            Dimension(
                "metadata_row_octets",
                OCTETS,
                (
                    "Largest native encoded control row in the admitted lookup "
                    "inputs; includes fixed hexadecimal ordinal storage, excludes"
                    " SQL/schema labels and source payload."
                ),
            ),
            Dimension(
                "checkpoint_octets",
                OCTETS,
                "Sum of the stored union, topology and rank checkpoint JSON widths.",
            ),
            Dimension(
                "source_binding_octets",
                OCTETS,
                (
                    "Canonical replay binding width from the exact source and "
                    "cached catalog identity."
                ),
            ),
            Dimension(
                "manifest_octets",
                OCTETS,
                (
                    "Canonical source/mode/preparation document width from the "
                    "exact input manifest fields."
                ),
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                count(12 if constructing else 1),
                (
                    "Application point queries include union/rank restoration, "
                    "optional last-record checks and final manifest selection."
                ),
            ),
            Bound(
                "driver_statements",
                COUNT,
                "upper",
                count(24 if constructing else 8),
                (
                    "All actual native statements, including pool health checks, "
                    "cold connection pragmas and transaction control."
                ),
            ),
            Bound(
                "connection_statements",
                COUNT,
                "upper",
                count(12 if constructing else 7),
                ("The bounded snapshot contexts may check or initialize their pooled connection."),
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                count(24 if constructing else 8),
                (
                    "Actual DBAPI fetched rows, including connection-control "
                    "results, not ORM identity-map hits."
                ),
            ),
            Bound(
                "returned_metadata_octets",
                OCTETS,
                "upper",
                scale(24 if constructing else 8, dimension("metadata_row_octets")),
                (
                    "Complete native control values and optional checkpoint "
                    "records retain their real widths."
                ),
            ),
            Bound(
                "bound_parameter_octets",
                OCTETS,
                "upper",
                scale(48 if constructing else 8, dimension("metadata_row_octets")),
                (
                    "At most twelve point statements repeat only their bounded "
                    "source/control row fields and keys."
                ),
            ),
            Bound(
                "changed_rows",
                COUNT,
                "equal",
                count(0),
                (
                    "Provider construction and currentness checks never "
                    "initialize, write or recreate retained preparation."
                ),
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                ("No original/captured source, documentary assertion or schema payload is read."),
            ),
            Bound(
                "json_decodes",
                COUNT,
                "upper",
                count(3 if constructing else 0),
                "Union, topology and rank checkpoint headers are decoded once each.",
            ),
            Bound(
                "json_encodes",
                COUNT,
                "upper",
                count(5 if constructing else 0),
                ("Canonical header checks, source binding and manifest encoding execute normally."),
            ),
            Bound(
                "json_decoded_octets",
                OCTETS,
                "upper",
                octets("checkpoint_octets"),
                "Only the three actual checkpoint texts enter their codecs.",
            ),
            Bound(
                "json_encoded_octets",
                OCTETS,
                "upper",
                add(
                    octets("checkpoint_octets"),
                    octets("source_binding_octets"),
                    octets("manifest_octets"),
                ),
                (
                    "Canonical header round trips and the exact binding/manifest "
                    "each retain their full byte widths."
                ),
            ),
            Bound(
                "hashed_octets",
                OCTETS,
                "upper",
                octets("manifest_octets"),
                "One actual complete manifest hash binds every owning field.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                add(
                    count(8192),
                    Expression(
                        "product",
                        terms=(
                            count(1024 if constructing else 256),
                            Expression(
                                "log2_ceil", terms=(add(count(2), dimension("retained_rows")),)
                            ),
                        ),
                    ),
                ),
                (
                    "Fixed schema/pool setup and the maintained exact point-query"
                    " envelope, including driver work."
                ),
            ),
        ),
        conditions=(
            (
                "The maintained SQLite catalog schema and "
                "source/attempt/metadata indexes are present."
            ),
            (
                "The supplied catalog exposes its cached sealed identity; "
                "admitted control rows include complete native storage "
                "widths."
            ),
            (
                "The healthy owning engine retains its pool checks; cold "
                "connection setup is observed without disabling maintenance."
            ),
            (
                "Actual source and preparation state determine rejection; no "
                "missing preparation is replaced with a new state."
            ),
        ),
        invariants=(
            (
                "The complete source/catalog/preparation manifest remains "
                "exact across every currentness probe."
            ),
            (
                "All snapshots close on success and rejection, with no "
                "catalog write or source replay."
            ),
            (
                "Certificate preparation does not itself establish semantic "
                "journal or archive acceptance."
            ),
        ),
        limitations=(
            (
                "Rank preparation, edge proof construction, native semantics,"
                " worker publication and retirement retain their owning "
                "composed costs."
            ),
            (
                "SQLite instructions exclude internal B-tree comparisons, "
                "page I/O and physical CPU/lock latency; other engines "
                "require separate qualification."
            ),
            (
                "Logical byte/SQL/codec/hash observations do not guarantee "
                "physical allocation, elapsed time or power-loss behavior."
            ),
        ),
    )


def _point_declaration() -> Declaration:
    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    return Declaration(
        id="riverhog-server.graph-certificate-probe.growth/v1",
        authority="riverhog-server",
        subjects=(
            "riverhog_core.services.upload_journal_certificates.UploadJournalGraphCertificates._probe",
        ),
        scope="sqlite exact source/manifest/frame/graph/arc/rank point lookup",
        starts_at=("accept the exact frozen provider and requested frame, kind and endpoint keys"),
        ends_at=("close the snapshot and return detached metadata or no current manifest"),
        dimensions=(
            Dimension("retained_rows", COUNT, "Rows across all retained catalog relations probed."),
            Dimension(
                "metadata_row_octets",
                OCTETS,
                (
                    "Largest native encoded control row in the lookup inputs, "
                    "including full manifest fields and hexadecimal ordinal "
                    "storage."
                ),
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                count(1),
                "One exact indexed join includes both endpoint rank aliases.",
            ),
            Bound(
                "driver_statements",
                COUNT,
                "upper",
                count(8),
                ("Includes actual pool checks, cold connection setup and transaction control."),
            ),
            Bound(
                "connection_statements",
                COUNT,
                "upper",
                count(7),
                "Connection maintenance remains enabled and observed.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                count(8),
                ("Includes native connection-control fetches and the single detached result."),
            ),
            Bound(
                "returned_metadata_octets",
                OCTETS,
                "upper",
                scale(8, dimension("metadata_row_octets")),
                "All fetched metadata retains its real native storage width.",
            ),
            Bound(
                "bound_parameter_octets",
                OCTETS,
                "upper",
                scale(64, dimension("metadata_row_octets")),
                ("The join repeats only bounded keys and exact frozen source/manifest controls."),
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        count(8192),
                        Expression(
                            "product",
                            terms=(
                                count(512),
                                Expression(
                                    "log2_ceil",
                                    terms=(
                                        Expression(
                                            "sum", terms=(count(2), dimension("retained_rows"))
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                ("Fixed schema setup and indexed point probes include native driver instructions."),
            ),
            Bound(
                "changed_rows",
                COUNT,
                "equal",
                count(0),
                "No catalog initialization, write or recreation.",
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                ("Original source, captured content and schema payload are never selected."),
            ),
        ),
        conditions=(
            (
                "The maintained SQLite schema and source, manifest, frame, "
                "graph, arc and rank indexes are present."
            ),
            (
                "Keys passed to the private lookup are canonical "
                "owner-admitted frame/kind/UUID values; the public edge "
                "operation owns earlier validation."
            ),
            (
                "The full input control-row width is supplied, including the "
                "frozen manifest and source fence."
            ),
        ),
        invariants=(
            (
                "The source, preparation, frame, union graph, arc and both "
                "endpoint ranks retain their exact identity links."
            ),
            (
                "Every snapshot closes before detached metadata escapes; no "
                "missing state is recreated."
            ),
        ),
        limitations=(
            (
                "SQLite VM counts exclude internal B-tree comparisons, "
                "physical I/O and CPU/lock latency; other engines need their "
                "own qualification."
            ),
            (
                "Proof construction, source semantics, publication and "
                "retirement retain their owning costs."
            ),
        ),
    )


def _edge_declaration() -> Declaration:
    child = growth_binding(UploadJournalGraphCertificates._probe).declaration

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    def delegate(metric: str) -> Expression:
        return Expression(
            "delegate",
            name="probe",
            metric=metric,
            terms=(dimension("probe_calls"),),
            arguments=tuple((item.name, dimension(item.name)) for item in child.dimensions),
        )

    return Declaration(
        id="riverhog-server.graph-certificate-edge.growth/v1",
        authority="riverhog-server",
        subjects=(
            "riverhog_core.services.upload_journal_certificates.UploadJournalGraphCertificates.edge",
        ),
        scope=(
            "closed source-fenced native edge proof with inclusive admission and codec/hash work"
        ),
        starts_at=("accept the exact provider and requested edge, frame digest and byte admission"),
        ends_at=(
            "return a verified detached proof, select cyclic/deferred fallback or reject the call"
        ),
        dimensions=(
            *child.dimensions,
            Dimension(
                "probe_calls",
                COUNT,
                "Zero for an early rejection, otherwise one actual point lookup.",
            ),
            Dimension(
                "request_octets",
                OCTETS,
                ("Complete encoded request scalar and string widths, including rejected values."),
            ),
            Dimension(
                "binding_octets", OCTETS, "Complete canonical input certificate binding width."
            ),
            Dimension(
                "proof_octets",
                OCTETS,
                (
                    "Canonical proof document width from the pre-call retained "
                    "rank/frame facts and request; zero when no proof can be "
                    "built."
                ),
            ),
        ),
        bounds=(
            *(
                Bound(
                    bound.metric,
                    bound.unit,
                    bound.relation,
                    delegate(bound.metric),
                    (
                        "All native query, row, parameter and connection work belongs"
                        " to the actual point lookup."
                    ),
                )
                for bound in child.bounds
            ),
            Bound(
                "json_decodes",
                COUNT,
                "equal",
                count(0),
                "The already authenticated binding is not restored from JSON.",
            ),
            Bound(
                "json_encodes",
                COUNT,
                "upper",
                count(5),
                (
                    "Two complete binding hashes each encode the source and outer"
                    " binding; the proof is encoded once."
                ),
            ),
            Bound(
                "json_decoded_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "No JSON document is decoded.",
            ),
            Bound(
                "json_encoded_octets",
                OCTETS,
                "upper",
                Expression(
                    "sum", terms=(scale(4, dimension("binding_octets")), dimension("proof_octets"))
                ),
                (
                    "Canonical source/binding documents and the complete native "
                    "proof retain their byte widths."
                ),
            ),
            Bound(
                "hashed_octets",
                OCTETS,
                "upper",
                scale(2, dimension("binding_octets")),
                ("Proof creation and proof verification each hash the complete binding."),
            ),
            Bound(
                "validated_identity_octets",
                OCTETS,
                "upper",
                scale(2, dimension("request_octets")),
                (
                    "Both admitted endpoint identities are validated during "
                    "admission and native proof verification."
                ),
            ),
            *(
                Bound(
                    "probe_" + bound.metric,
                    bound.unit,
                    bound.relation,
                    delegate(bound.metric),
                    "Retain every actual child metric and its exact input domain.",
                )
                for bound in child.bounds
            ),
        ),
        conditions=(
            (
                "The provider is constructed by its owning finite preparation"
                " with exact source/catalog/manifest pins."
            ),
            (
                "Pre-call admission and request values determine whether a "
                "point query is reached; there is at most one actual query."
            ),
            (
                "Proof width comes from the exact pre-call retained facts and"
                " requested scalars, not from a mocked result or a timing "
                "reference."
            ),
        ),
        invariants=(
            (
                "The existing fixed metadata admission rejects before opening"
                " a reader; no collection or member ceiling is introduced."
            ),
            (
                "Only complete acyclic evidence can skip native online graph "
                "work; cyclic and deferred evidence consistently selects "
                "fallback."
            ),
            (
                "Missing, replaced, incomplete or mismatched evidence fails "
                "explicitly with closed readers and zero catalog writes."
            ),
        ),
        limitations=(
            (
                "Logical SQL, byte, codec, hash and identity-input "
                "observations do not promise physical allocation, CPU, I/O or"
                " elapsed time."
            ),
            (
                "Proofs cover only the native graph check; journal semantics,"
                " archive acceptance, publication and retirement retain their"
                " own authorities and costs."
            ),
        ),
    )


class UploadJournalGraphCertificates:
    def __init__(
        self,
        factory: SessionFactory,
        source: source_owner._UploadJournalValidationWork,
        *,
        catalog: ContractCatalog,
    ) -> None:
        work = rank._begin(factory, source)
        if work is None or work.state not in {"complete", "deferred"}:
            raise InvalidJournalGraphCertificate("finite rank preparation is not complete")
        query = (
            select(*(getattr(Scan, name) for name in _MANIFEST_FIELDS))
            .join(
                Journal,
                (Scan.collection_id == Journal.collection_id)
                & (Scan.journal_id == Journal.journal_id),
            )
            .where(
                Scan.collection_id == source.collection_id,
                Scan.journal_id == source.journal_id,
                Scan.attempt_id == source.attempt_id,
                Scan.directory_id == source.directory_id,
                Scan.version == work.parent.version,
                *source_owner._upload_journal_validation_fence(source),
            )
        )
        with read_snapshot(factory) as session:
            with session.execute(query) as rows:
                row = rows.one_or_none()
        if row is None:
            raise InvalidJournalGraphCertificate("finite rank source changed during preparation")
        self.factory, self.source = factory, source
        self._manifest, self._mode = tuple(row), work.state
        source_binding = _binding(source, catalog)
        encoded = canonical_json_bytes(
            {
                "source": source_binding.to_json(),
                "mode": self._mode,
                "preparation": {
                    key: str(value) if type(value) is int else value
                    for key, value in zip(_MANIFEST_FIELDS, self._manifest, strict=True)
                },
            }
        )
        self._binding = JournalGraphCertificateBinding(
            source_binding, hashlib.sha256(encoded).hexdigest()
        )

    @property
    def binding(self) -> JournalGraphCertificateBinding:
        return self._binding

    def _fence(self) -> tuple[Any, ...]:
        source = self.source
        return (
            Scan.collection_id == source.collection_id,
            Scan.journal_id == source.journal_id,
            Scan.attempt_id == source.attempt_id,
            Scan.directory_id == source.directory_id,
            *(
                getattr(Scan, name) == value
                for name, value in zip(_MANIFEST_FIELDS, self._manifest, strict=True)
            ),
            *source_owner._upload_journal_validation_fence(source),
        )

    def verify_current(self) -> bool:
        query = (
            select(Scan.collection_id)
            .join(
                Journal,
                (Scan.collection_id == Journal.collection_id)
                & (Scan.journal_id == Journal.journal_id),
            )
            .where(*self._fence())
        )
        with read_snapshot(self.factory) as session:
            with session.execute(query) as rows:
                return rows.one_or_none() is not None

    def _probe(
        self, *, frame_ordinal: int, kind: str, source: str, target: str
    ) -> tuple[Any, ...] | None:
        left, right = aliased(Ranked), aliased(Ranked)

        def epoch_key(model: Any) -> tuple[Any, ...]:
            return (
                model.collection_id == self.source.collection_id,
                model.journal_id == self.source.journal_id,
                model.attempt_id == self.source.attempt_id,
                model.epoch == Frame.epoch_ordinal,
                model.kind == kind,
            )

        query = (
            select(
                Frame.epoch_ordinal,
                Frame.epoch_json_sha256,
                Result.state,
                Result.vertex_count,
                Result.arc_count,
                Result.settled_vertices,
                Result.visited_arcs,
                Graph.vertex_count,
                Graph.arc_count,
                Graph.state,
                Arc.source_id,
                left.rank,
                right.rank,
                left.remaining_degree,
                right.remaining_degree,
                left.ready,
                right.ready,
            )
            .select_from(Scan)
            .join(
                Journal,
                (Scan.collection_id == Journal.collection_id)
                & (Scan.journal_id == Journal.journal_id),
            )
            .outerjoin(
                Frame,
                and_(
                    Frame.collection_id == self.source.collection_id,
                    Frame.journal_id == self.source.journal_id,
                    Frame.directory_id == self.source.directory_id,
                    Frame.ordinal == frame_ordinal,
                    Frame.epoch_attempt_id == self.source.attempt_id,
                ),
            )
            .outerjoin(Result, and_(*epoch_key(Result)))
            .outerjoin(Graph, and_(*epoch_key(Graph)))
            .outerjoin(Arc, and_(*epoch_key(Arc), Arc.source_id == source, Arc.target_id == target))
            .outerjoin(left, and_(*epoch_key(left), left.vertex_id == source))
            .outerjoin(right, and_(*epoch_key(right), right.vertex_id == target))
            .where(*self._fence())
        )
        with read_snapshot(self.factory) as session:
            with session.execute(query) as rows:
                result = rows.one_or_none()
        return None if result is None else tuple(result)

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
        if type(byte_budget) is not int or byte_budget < _EDGE_METADATA_OCTETS:
            raise JournalValidationCapacity("rank point reads need their fixed metadata admission")
        if type(frame_ordinal) is not int or not 0 <= frame_ordinal < int(
            self.binding.source.frame_count
        ):
            raise InvalidJournalGraphCertificate("graph frame is outside its retained source")
        if kind not in {"derivation", "specialization"}:
            raise InvalidJournalGraphCertificate("unknown native graph kind")
        require_canonical_uuid_urn(source)
        require_canonical_uuid_urn(target)
        row = self._probe(frame_ordinal=frame_ordinal, kind=kind, source=source, target=target)
        if row is None:
            raise InvalidJournalGraphCertificate("source or rank manifest is no longer current")
        if self._mode == "deferred":
            return None
        (
            epoch,
            digest,
            state,
            vertices,
            arcs,
            settled,
            visited,
            union_vertices,
            union_arcs,
            union_state,
            member,
            left,
            right,
            left_degree,
            right_degree,
            left_ready,
            right_ready,
        ) = row
        if (
            epoch is None
            or digest != frame_sha256
            or member != source
            or state not in {"acyclic", "cyclic"}
            or union_state != "complete"
            or (vertices, arcs) != (union_vertices, union_arcs)
            or not 0 <= settled <= vertices
            or not 0 <= visited <= arcs
        ):
            raise InvalidJournalGraphCertificate("rank evidence lost its exact source/union links")
        if state == "cyclic":
            return None
        if (
            settled != vertices
            or visited != arcs
            or left is None
            or right is None
            or left_degree != 0
            or right_degree != 0
            or left_ready is not False
            or right_ready is not False
        ):
            raise InvalidJournalGraphCertificate("acyclic rank evidence is incomplete")
        proof = JournalGraphCertificate(
            self.binding.checkpoint_sha256(),
            frame_ordinal,
            frame_sha256,
            epoch,
            kind,
            source,
            target,
            left,
            right,
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
        except (ValueError, TypeError) as exc:
            raise InvalidJournalGraphCertificate("retained rank metadata is invalid") from exc


declare_growth(_lifecycle_declaration("__init__"))(UploadJournalGraphCertificates.__init__)
declare_growth(_lifecycle_declaration("verify_current"))(
    UploadJournalGraphCertificates.verify_current
)


declare_growth(_point_declaration())(UploadJournalGraphCertificates._probe)
declare_growth(_edge_declaration(), delegates=(("probe", UploadJournalGraphCertificates._probe),))(
    UploadJournalGraphCertificates.edge
)
