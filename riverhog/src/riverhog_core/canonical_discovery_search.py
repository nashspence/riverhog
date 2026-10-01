"""Permission-checked discovery over one complete canonical index generation."""

from __future__ import annotations

import hashlib
import json
from typing import Any, cast

from riverhog_canonical_json import canonical_json_bytes, format_scalar
from riverhog_protocol import ArtifactDiscoveryRequest, AssertionClause, ValuePredicate
from riverhog_protocol.collection_tags import collection_tag_sha256
from riverhog_protocol.errors import BadRequest, Conflict, ServiceUnavailable
from riverhog_provenance_contracts import core_contract
from sqlalchemy import and_, exists, func, not_, or_, select
from sqlalchemy.orm import Session, aliased
from sqlalchemy.sql.elements import ColumnElement

from riverhog_core.app_permissions import CATALOG_READ, PROVENANCE_READ, Principal
from riverhog_core.artifact_access import artifact_scope_filter
from riverhog_core.canonical_discovery_extraction import ascii_fold, assertion_clause_matches
from riverhog_core.canonical_discovery_index import EXTRACTION_CONTRACT_SHA256
from riverhog_core.catalog_models import (
    CatalogSyncStateRecord,
    CollectionArtifactRecord,
    CollectionRecord,
    CollectionTagMembershipRecord,
)
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexAssertionRecord as Assertion,
)
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexEntryRecord as Entry,
)
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexGenerationRecord as Generation,
)
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexMembershipRecord as Membership,
)
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexProfileRecord as Profile,
)
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexSnapshotRecord as Snapshot,
)
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexStateRecord as IndexState,
)
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexTextChunkRecord as TextChunk,
)
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexValueRecord as Value,
)
from riverhog_core.collection_access import collection_access_filter

_MAX_CANDIDATE_BATCH = 256


def _selected_collections(
    session: Session, request: ArtifactDiscoveryRequest, principal: Principal | None
) -> Any:
    allowed = collection_access_filter(CollectionRecord.id, principal, CATALOG_READ)
    filters: list[ColumnElement[bool]] = [allowed]
    if request.provenance_all:
        filters.append(collection_access_filter(CollectionRecord.id, principal, PROVENANCE_READ))
    if request.collections:
        filters.append(CollectionRecord.id.in_(tuple(map(int, request.collections))))
    for tag in request.tags_all:
        filters.append(_has_tag(collection_tag_sha256(tag)))
    if request.tags_any:
        filters.append(_has_any_tag(tuple(map(collection_tag_sha256, request.tags_any))))
    if request.tags_none:
        filters.append(not_(_has_any_tag(tuple(map(collection_tag_sha256, request.tags_none)))))
    if request.description_contains is not None:
        description = func.coalesce(CollectionRecord.description, "")
        if session.get_bind().dialect.name == "sqlite":
            filters.append(func.instr(description, request.description_contains) > 0)
        else:
            filters.append(func.strpos(description, request.description_contains) > 0)
    return (
        select(
            CollectionRecord.id.label("collection_id"),
            CollectionRecord.archive_root_sha256,
            CollectionRecord.provenance_identity,
            CollectionRecord.tag_revision,
            CollectionRecord.description_revision,
            IndexState.active_build_id,
            Generation.generation_id,
            Generation.archive_root_sha256.label("index_root_sha256"),
            Generation.provenance_identity.label("index_provenance_identity"),
            Generation.core_contract_sha256,
            Generation.extraction_contract_sha256,
        )
        .outerjoin(IndexState, IndexState.collection_id == CollectionRecord.id)
        .outerjoin(
            Generation,
            and_(
                Generation.build_id == IndexState.active_build_id,
                Generation.complete.is_(True),
            ),
        )
        .where(*filters)
    )


def _has_tag(tag_sha256: str) -> ColumnElement[bool]:
    return exists(
        select(1).where(
            CollectionTagMembershipRecord.collection_id == CollectionRecord.id,
            CollectionTagMembershipRecord.tag_sha256 == tag_sha256,
        )
    )


def _has_any_tag(tag_sha256s: tuple[str, ...]) -> ColumnElement[bool]:
    return exists(
        select(1).where(
            CollectionTagMembershipRecord.collection_id == CollectionRecord.id,
            CollectionTagMembershipRecord.tag_sha256.in_(tag_sha256s),
        )
    )


def _read_identity(
    session: Session,
    selected: Any,
    *,
    request: ArtifactDiscoveryRequest,
    principal: Principal | None,
    source_identity: str,
) -> str:
    header = {
        "format": "riverhog-discovery-read/v1",
        "query_identity": request.identity(),
        "source_identity": source_identity,
        "authorization_view": principal.authorization_view_identity if principal else None,
    }
    digest = hashlib.sha256(canonical_json_bytes(header))
    expected_core = core_contract().contract_sha256
    for row in session.execute(
        selected.order_by(selected.selected_columns.collection_id)
    ).yield_per(128):
        if request.provenance_all and (
            row.active_build_id is None
            or row.generation_id is None
            or row.index_root_sha256 != row.archive_root_sha256
            or row.index_provenance_identity != row.provenance_identity
            or row.core_contract_sha256 != expected_core
            or row.extraction_contract_sha256 != EXTRACTION_CONTRACT_SHA256
        ):
            raise ServiceUnavailable(
                "canonical discovery index is unavailable for a selected collection"
            )
        record = {
            "collection_id": format_scalar("sequence63", row.collection_id),
            "archive_root_sha256": row.archive_root_sha256,
            "index_generation": row.generation_id,
            "tag_revision": format_scalar("nonnegative", row.tag_revision),
            "description_revision": format_scalar("nonnegative", row.description_revision),
        }
        encoded = canonical_json_bytes(record)
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def _literal_pattern(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


def _value_filters(
    value: Any,
    predicate: ValuePredicate,
    *,
    profile: Any | None,
) -> list[ColumnElement[bool]]:
    filters: list[ColumnElement[bool]] = []
    if predicate.pointer is not None:
        filters.append(value.pointer == predicate.pointer)
    if profile is not None:
        data_pointer = profile.pointer.concat("/data")
        filters.append(
            or_(value.pointer == data_pointer, value.pointer.startswith(data_pointer + "/"))
        )
    if predicate.operator == "bytes-equals":
        filters.extend(
            (
                value.representation == "exact-bytes",
                value.exact_json == canonical_json_bytes(predicate.value),
            )
        )
    elif predicate.operator == "equals":
        filters.append(value.representation != "exact-bytes")
        if type(predicate.value) is str:
            field = value.folded_text if predicate.text_mode == "ascii-fold" else value.text_value
            wanted = (
                ascii_fold(predicate.value)
                if predicate.text_mode == "ascii-fold"
                else predicate.value
            )
            filters.append(field == wanted)
        else:
            filters.append(value.exact_json == canonical_json_bytes(predicate.value))
    else:
        assert isinstance(predicate.value, str)
        chunk = aliased(TextChunk)
        wanted = (
            ascii_fold(predicate.value) if predicate.text_mode == "ascii-fold" else predicate.value
        )
        field = chunk.folded_chunk if predicate.text_mode == "ascii-fold" else chunk.text_chunk
        filters.extend(
            (
                value.representation != "exact-bytes",
                exists(
                    select(1).where(
                        chunk.build_id == value.build_id,
                        chunk.row_key == value.row_key,
                        chunk.value_ordinal == value.ordinal,
                        field.like(_literal_pattern(wanted), escape="\\"),
                    )
                ),
            )
        )
    return filters


def _value_exists(assertion: Any, predicate: ValuePredicate) -> ColumnElement[bool]:
    value = aliased(Value)
    return exists(
        select(1)
        .where(
            value.build_id == assertion.build_id,
            value.row_key == assertion.row_key,
            *_value_filters(value, predicate, profile=None),
        )
        .correlate(assertion)
    )


def _clause_predicate(assertion: Any, clause: AssertionClause) -> ColumnElement[bool]:
    filters: list[ColumnElement[bool]] = []
    if clause.kind is not None:
        filters.append(assertion.kind == clause.kind)
    if clause.assertion_state != "any":
        filters.append(assertion.assertion_state == clause.assertion_state)
    if clause.profile is None:
        filters.extend(_value_exists(assertion, predicate) for predicate in clause.values)
    else:
        profile = aliased(Profile)
        statement = select(1).select_from(profile)
        profile_filters = [
            profile.build_id == assertion.build_id,
            profile.row_key == assertion.row_key,
            profile.contract_id == clause.profile.contract_id,
            profile.contract_sha256 == clause.profile.contract_sha256,
            profile.schema_id == clause.profile.schema_id,
        ]
        for predicate in clause.values:
            value = aliased(Value)
            statement = statement.join(
                value,
                and_(value.build_id == profile.build_id, value.row_key == profile.row_key),
            )
            profile_filters.extend(_value_filters(value, predicate, profile=profile))
        filters.append(exists(statement.where(*profile_filters).correlate(assertion)))
    return and_(*filters)


def _matching_members(clause: AssertionClause) -> Any:
    membership = aliased(Membership)
    assertion = aliased(Assertion)
    return (
        select(membership.build_id, membership.artifact_id)
        .select_from(membership)
        .join(
            assertion,
            and_(
                assertion.build_id == membership.build_id,
                assertion.row_key == membership.row_key,
            ),
        )
        .where(
            membership.scope.in_(clause.scopes),
            _clause_predicate(assertion, clause),
        )
        .distinct()
        .subquery()
    )


def _exact_support(
    session: Session,
    *,
    build_id: str,
    artifact_id: str,
    clause: AssertionClause,
) -> list[dict[str, object]] | None:
    statement = (
        select(Assertion, Membership.scope, Snapshot, Entry)
        .join(
            Membership,
            and_(
                Membership.build_id == Assertion.build_id,
                Membership.row_key == Assertion.row_key,
            ),
        )
        .join(
            Snapshot,
            and_(
                Snapshot.build_id == Assertion.build_id,
                Snapshot.journal_id == Assertion.journal_id,
                Snapshot.prefix_sha256 == Assertion.prefix_sha256,
            ),
        )
        .join(
            Entry,
            and_(
                Entry.build_id == Assertion.build_id,
                Entry.journal_id == Assertion.journal_id,
                Entry.prefix_sha256 == Assertion.prefix_sha256,
                Entry.sequence == Assertion.sequence,
            ),
        )
        .where(
            Membership.build_id == build_id,
            Membership.artifact_id == artifact_id,
            Membership.scope.in_(clause.scopes),
            _clause_predicate(Assertion, clause),
        )
        .order_by(
            Assertion.journal_id, Assertion.sequence, Assertion.assertion_id, Membership.scope
        )
        .execution_options(yield_per=32)
    )
    for row, scope, snapshot, entry in session.execute(statement):
        assertion = json.loads(row.canonical_json)
        matched = assertion_clause_matches(
            assertion,
            clause,
            assertion_state=cast(Any, row.assertion_state),
        )
        if matched is None:
            continue
        return [
            {
                "journal_anchor": {
                    "journal_id": snapshot.journal_id,
                    "through": {
                        "entry_id": snapshot.through_entry_id,
                        "sequence": format_scalar("sequence63", snapshot.through_sequence),
                        "json_sha256": snapshot.through_json_sha256,
                    },
                    "prefix_sha256": snapshot.prefix_sha256,
                    "prefix_bytes": format_scalar("nonnegative", snapshot.prefix_bytes),
                },
                "entry": {
                    "entry_id": entry.entry_id,
                    "sequence": format_scalar("sequence63", entry.sequence),
                    "json_sha256": entry.json_sha256,
                },
                "assertion_id": row.assertion_id,
                "referent_id": row.referent_id,
                "assertion_kind": row.kind,
                "assertion_state": row.assertion_state,
                "relationship": scope,
                "pointer": posting.pointer,
                "representation": posting.representation,
                "value_sha256": posting.source_value_sha256,
            }
            for posting in matched
        ]
    return None


def _discovery_candidate_statement(
    selected: Any, *, request: ArtifactDiscoveryRequest, principal: Principal | None
) -> Any:
    """The indexed candidate statement used before exact support validation."""
    collections = selected.subquery()
    artifacts = CollectionArtifactRecord
    statement = select(
        artifacts.collection_id,
        artifacts.artifact_id,
        artifacts.bytes,
        artifacts.sha256,
        collections.c.archive_root_sha256,
        collections.c.provenance_identity,
        collections.c.active_build_id,
        collections.c.generation_id,
        collections.c.tag_revision,
        collections.c.description_revision,
    ).join(collections, collections.c.collection_id == artifacts.collection_id)
    statement = statement.where(
        artifact_scope_filter(artifacts.collection_id, artifacts.artifact_id, principal)
    )
    if request.collections:
        # Expose the explicit bound on both sides of the selected-view join
        # so planning retains indexed collection/member lookups.
        statement = statement.where(
            artifacts.collection_id.in_(tuple(map(int, request.collections)))
        )
    if request.artifact_id is not None:
        statement = statement.where(artifacts.artifact_id == request.artifact_id)
    if request.payload_sha256 is not None:
        statement = statement.where(artifacts.sha256 == request.payload_sha256)
    for clause in request.provenance_all:
        matches = _matching_members(clause)
        statement = statement.join(
            matches,
            and_(
                matches.c.build_id == collections.c.active_build_id,
                matches.c.artifact_id == artifacts.artifact_id,
            ),
        )

    return statement


def discover_artifacts(
    session: Session,
    *,
    request: ArtifactDiscoveryRequest,
    principal: Principal | None,
    position: tuple[object, ...] | None,
) -> dict[str, object]:
    """Return one bounded stable page, with exact support for every provenance match."""

    source = session.get(CatalogSyncStateRecord, 1)
    if source is None:
        raise ServiceUnavailable("catalog source identity is unavailable")
    selected = _selected_collections(session, request, principal)
    read_identity = _read_identity(
        session,
        selected,
        request=request,
        principal=principal,
        source_identity=source.source_identity,
    )
    if position is not None:
        if (
            len(position) != 3
            or type(position[0]) is not str
            or type(position[1]) is not str
            or type(position[2]) is not str
        ):
            raise BadRequest("discovery page position is invalid")
        if position[0] != read_identity:
            raise Conflict("discovery read changed; restart from the first page")
        try:
            previous_id = int(position[1])
            if format_scalar("sequence63", previous_id) != position[1]:
                raise ValueError("noncanonical collection")
        except ValueError as exc:
            raise BadRequest("discovery page position is invalid") from exc
        previous_artifact = position[2]
    else:
        previous_id, previous_artifact = 0, ""

    statement = _discovery_candidate_statement(selected, request=request, principal=principal)
    artifacts = CollectionArtifactRecord

    hits: list[dict[str, object]] = []
    exhausted = False
    cursor_id, cursor_artifact = previous_id, previous_artifact
    while len(hits) <= request.page_size and not exhausted:
        batch = list(
            session.execute(
                statement.where(
                    or_(
                        artifacts.collection_id > cursor_id,
                        and_(
                            artifacts.collection_id == cursor_id,
                            artifacts.artifact_id > cursor_artifact,
                        ),
                    )
                )
                .order_by(artifacts.collection_id, artifacts.artifact_id)
                .limit(_MAX_CANDIDATE_BATCH)
            )
        )
        exhausted = len(batch) < _MAX_CANDIDATE_BATCH
        for row in batch:
            cursor_id, cursor_artifact = row.collection_id, row.artifact_id
            support: list[dict[str, object]] = []
            for clause in request.provenance_all:
                found = _exact_support(
                    session,
                    build_id=row.active_build_id,
                    artifact_id=row.artifact_id,
                    clause=clause,
                )
                if found is None:
                    break
                support.extend(found)
            else:
                hits.append(
                    {
                        "source_identity": source.source_identity,
                        "collection_id": format_scalar("sequence63", row.collection_id),
                        "archive_root_sha256": row.archive_root_sha256,
                        "provenance_identity": row.provenance_identity,
                        "artifact": {
                            "artifact_id": row.artifact_id,
                            "bytes": format_scalar("nonnegative", row.bytes),
                            "sha256": row.sha256,
                        },
                        "index_generation": row.generation_id,
                        "tag_revision": format_scalar("nonnegative", row.tag_revision),
                        "description_revision": format_scalar(
                            "nonnegative", row.description_revision
                        ),
                        "matches": support,
                    }
                )
                if len(hits) > request.page_size:
                    break
    more = len(hits) > request.page_size
    page = hits[: request.page_size]
    next_position: tuple[object, ...] | None = None
    if more:
        last = page[-1]
        last_artifact = cast(dict[str, object], last["artifact"])
        next_position = (read_identity, last["collection_id"], last_artifact["artifact_id"])
    return {
        "format": "riverhog-artifact-discovery-page/v1",
        "query_identity": request.identity(),
        "read_identity": read_identity,
        "artifacts": page,
        "complete": not more,
        "_next_position": next_position,
    }


__all__ = ["discover_artifacts"]
