from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from http_api_contracts import BrowseScalar, closed_literal_values
from riverhog_canonical_json import format_scalar
from riverhog_protocol import ArtifactDiscoveryRequest, SearchSort, SortOrder
from riverhog_protocol.errors import BadRequest
from riverhog_protocol.paths import (
    PathNormalizationError,
    normalize_collection_id,
    text_search_key,
)
from sqlalchemy import asc, desc, exists, or_, select
from sqlalchemy.sql.elements import ColumnElement
from state_schema import read_snapshot

from riverhog_core.app_permissions import CATALOG_READ, Principal
from riverhog_core.artifact_access import artifact_scope_filter
from riverhog_core.browse import bounded_page, keyset_statement, validate_page_size
from riverhog_core.canonical_discovery_search import discover_artifacts
from riverhog_core.catalog_db import SessionFactory, make_session_factory
from riverhog_core.catalog_models import (
    CollectionArtifactRecord,
    CollectionRecord,
    CollectionTagMembershipRecord,
    CollectionTagRecord,
)
from riverhog_core.collection_access import collection_access_filter, require_collection_access
from riverhog_core.runtime_config import RuntimeConfig

_SORT_FIELDS = closed_literal_values(SearchSort)
_SORT_ORDERS = closed_literal_values(SortOrder)


def _like_pattern(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


class SqlAlchemySearchService:
    def __init__(
        self,
        config: RuntimeConfig,
        *,
        session_factory: SessionFactory | None = None,
    ) -> None:
        self._session_factory = session_factory or make_session_factory(config.database_url)

    def discover(
        self,
        *,
        request: ArtifactDiscoveryRequest,
        position: tuple[object, ...] | None,
        principal: Principal | None = None,
    ) -> dict[str, object]:
        with read_snapshot(self._session_factory) as session:
            return discover_artifacts(
                session, request=request, principal=principal, position=position
            )

    def search(
        self,
        *,
        q: str | None,
        page_size: int,
        position: tuple[str | int | bool | bytes | None, ...] | None,
        sort: str,
        order: str,
        collection: int | None = None,
        principal: Principal | None = None,
    ) -> dict[str, object]:
        validate_page_size(page_size)
        if sort not in _SORT_FIELDS:
            raise BadRequest(f"sort must be one of {', '.join(sorted(_SORT_FIELDS))}")
        if order not in _SORT_ORDERS:
            raise BadRequest("order must be asc or desc")

        normalized_collection, query, _, stmt, key_columns = _search_statement(
            q=q,
            collection=collection,
            sort=sort,
            order=order,
            principal=principal,
        )
        with read_snapshot(self._session_factory) as session:
            if normalized_collection is not None:
                require_collection_access(
                    session,
                    principal,
                    CATALOG_READ,
                    normalized_collection,
                )
            rows, next_position = bounded_page(
                list(
                    session.execute(
                        keyset_statement(
                            stmt,
                            columns=key_columns,
                            position=position,
                            order=order,
                            page_size=page_size,
                        )
                    )
                ),
                page_size=page_size,
                position_of=lambda row: _search_position(row, sort=sort),
            )

        return {
            "query": query,
            "collection": (
                None
                if normalized_collection is None
                else format_scalar("sequence63", normalized_collection)
            ),
            "page_size": page_size,
            "_next_position": next_position,
            "sort": sort,
            "order": order,
            "artifacts": [
                {
                    "artifact_ref": f"{row.collection_id}/{row.artifact_id}",
                    "collection_id": format_scalar("sequence63", row.collection_id),
                    "artifact_id": row.artifact_id,
                    "bytes": format_scalar("nonnegative", row.bytes),
                    "sha256": row.sha256,
                }
                for row in rows
            ],
        }

    def iter_artifacts(
        self,
        *,
        q: str | None,
        sort: str,
        order: str,
        collection: int | None = None,
        principal: Principal | None = None,
    ) -> Iterator[dict[str, object]]:
        if sort not in _SORT_FIELDS:
            raise BadRequest(f"sort must be one of {', '.join(sorted(_SORT_FIELDS))}")
        if order not in _SORT_ORDERS:
            raise BadRequest("order must be asc or desc")
        normalized_collection, _query, _filters, statement, key_columns = _search_statement(
            q=q,
            collection=collection,
            sort=sort,
            order=order,
            principal=principal,
        )
        direction = desc if order == "desc" else asc
        statement = statement.order_by(*(direction(column) for column in key_columns))
        statement = statement.execution_options(yield_per=100)
        with read_snapshot(self._session_factory) as session:
            if normalized_collection is not None:
                require_collection_access(session, principal, CATALOG_READ, normalized_collection)
            for row in session.execute(statement):
                yield {
                    "artifact_ref": f"{row.collection_id}/{row.artifact_id}",
                    "collection_id": format_scalar("sequence63", row.collection_id),
                    "artifact_id": row.artifact_id,
                    "bytes": format_scalar("nonnegative", row.bytes),
                    "sha256": row.sha256,
                }


def _search_statement(
    *,
    q: str | None,
    collection: int | None,
    sort: str,
    order: str,
    principal: Principal | None,
) -> tuple[int | None, str | None, list[ColumnElement[bool]], Any, tuple[Any, ...]]:
    normalized_collection, query, filters = _search_filters(
        q=q,
        collection=collection,
        principal=principal,
    )
    statement = select(
        CollectionArtifactRecord.collection_id,
        CollectionArtifactRecord.artifact_id,
        CollectionArtifactRecord.bytes,
        CollectionArtifactRecord.sha256,
    ).where(*filters)
    return normalized_collection, query, filters, statement, _key_columns(sort)


def _key_columns(sort: str) -> tuple[Any, ...]:
    if sort in {"artifact_ref", "collection_id"}:
        return CollectionArtifactRecord.collection_id, CollectionArtifactRecord.artifact_id
    if sort == "artifact_id":
        return CollectionArtifactRecord.artifact_id, CollectionArtifactRecord.collection_id
    if sort == "bytes":
        return (
            CollectionArtifactRecord.bytes,
            CollectionArtifactRecord.collection_id,
            CollectionArtifactRecord.artifact_id,
        )
    raise BadRequest(f"sort must be one of {', '.join(sorted(_SORT_FIELDS))}")


def _search_position(row: Any, *, sort: str) -> tuple[BrowseScalar, ...]:
    if sort in {"artifact_ref", "collection_id"}:
        return row.collection_id, row.artifact_id
    if sort == "artifact_id":
        return row.artifact_id, row.collection_id
    return row.bytes, row.collection_id, row.artifact_id


def _search_filters(
    *,
    q: str | None,
    collection: int | None,
    principal: Principal | None,
) -> tuple[int | None, str | None, list[ColumnElement[bool]]]:
    normalized_collection: int | None = None
    if collection:
        try:
            normalized_collection = normalize_collection_id(collection)
        except PathNormalizationError as exc:
            raise BadRequest(str(exc)) from exc
    filters: list[ColumnElement[bool]] = [
        collection_access_filter(CollectionArtifactRecord.collection_id, principal, CATALOG_READ)
    ]
    filters.append(
        artifact_scope_filter(
            CollectionArtifactRecord.collection_id,
            CollectionArtifactRecord.artifact_id,
            principal,
        )
    )
    query = q.strip() if q is not None else None
    if query:
        pattern = _like_pattern(text_search_key(query))
        filters.append(
            or_(
                CollectionArtifactRecord.artifact_id.like(pattern, escape="\\"),
                CollectionArtifactRecord.sha256.like(pattern, escape="\\"),
                exists(
                    select(1).where(
                        CollectionRecord.id == CollectionArtifactRecord.collection_id,
                        or_(
                            CollectionRecord.search_text.like(pattern, escape="\\"),
                            CollectionRecord.description_search.like(pattern, escape="\\"),
                        ),
                    )
                ),
                exists(
                    select(1)
                    .select_from(CollectionTagMembershipRecord)
                    .join(
                        CollectionTagRecord,
                        CollectionTagMembershipRecord.tag_sha256 == CollectionTagRecord.tag_sha256,
                    )
                    .where(
                        CollectionTagMembershipRecord.collection_id
                        == CollectionArtifactRecord.collection_id,
                        CollectionTagRecord.search_text.like(pattern, escape="\\"),
                    )
                ),
            )
        )
    if normalized_collection is not None:
        filters.append(CollectionArtifactRecord.collection_id == normalized_collection)
    return normalized_collection, query, filters
