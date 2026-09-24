from __future__ import annotations

from typing import Annotated, cast

from fastapi import Query
from http_api_contracts import mutable_browse_operation
from riverhog_protocol import CollectionIdParameter, SearchSort, SortOrder

from riverhog_api.auth import CatalogReader
from riverhog_api.browse import (
    BrowsePageTokenQuery,
    BrowseQueryParameter,
    canonical_selectors,
    page_payload,
    page_position,
)
from riverhog_api.deps import ContainerDep
from riverhog_api.routing import RiverhogRouter
from riverhog_api.schemas.search import SearchFileOut, SearchOut

router = RiverhogRouter(tags=["search"])


@router.get(
    "/search",
    response_model=SearchOut,
    openapi_extra=mutable_browse_operation(),
)
def search(
    container: ContainerDep,
    principal: CatalogReader,
    q: BrowseQueryParameter = None,
    page_size: int = Query(25, ge=1, le=100),
    page_token: BrowsePageTokenQuery = None,
    sort: Annotated[SearchSort, Query()] = "file_ref",
    order: Annotated[SortOrder, Query()] = "asc",
    collection: Annotated[CollectionIdParameter | None, Query()] = None,
) -> SearchOut:
    selectors = canonical_selectors(q=q, sort=sort, order=order, collection=collection)
    position = page_position(
        container,
        principal=principal,
        operation="search",
        page_token=page_token,
        selectors=selectors,
    )
    payload = page_payload(
        container.search.search(
            q=q,
            page_size=page_size,
            position=position,
            sort=sort,
            order=order,
            collection=collection,
            principal=principal,
        ),
        container=container,
        principal=principal,
        operation="search",
        selectors=selectors,
    )
    files = cast(list[dict[str, object]], payload["files"])
    return SearchOut.model_validate(
        {
            **payload,
            "files": [SearchFileOut.model_validate(record) for record in files],
        }
    )
