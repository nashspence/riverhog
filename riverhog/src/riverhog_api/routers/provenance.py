"""Root-selected canonical history, addressed by collection and artifact identity."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Annotated, Any

from fastapi import Header, Query, Request, Response
from http_api_contracts import operation_interface, parse_quoted_sha256_identity
from riverhog_archive_contracts import provenance_structure_object_path
from riverhog_protocol import ArtifactId, CollectionIdParameter
from riverhog_protocol.errors import BadRequest, PreconditionFailed, PreconditionRequired
from riverhog_provenance_contracts import ProvenanceJournalId
from starlette.responses import StreamingResponse

from riverhog_api.auth import ProvenanceExporter, ProvenanceReader
from riverhog_api.deps import ContainerDep
from riverhog_api.routing import RiverhogRouter
from riverhog_api.schemas.provenance import (
    CollectionArtifactProvenanceDetailOut,
    ListCollectionArtifactProvenanceOut,
    ListCollectionProvenanceJournalsOut,
)

router = RiverhogRouter(tags=["provenance"])

_STRUCTURE_RESPONSE: dict[int | str, dict[str, Any]] = {
    200: {
        "description": "Exact bounded canonical archive structure, pinned to its enclosing root.",
        "content": {"application/json": {"schema": {"type": "string", "format": "binary"}}},
    }
}


@router.get(
    "/collections/{collection_id}/provenance/structure/{object_id}",
    response_class=Response,
    responses=_STRUCTURE_RESPONSE,
    openapi_extra=operation_interface("client-only-primitive"),
)
def get_collection_provenance_structure(
    collection_id: CollectionIdParameter,
    object_id: str,
    principal: ProvenanceExporter,
    container: ContainerDep,
    if_match: Annotated[str | None, Header(alias="If-Match")] = None,
) -> Response:
    if if_match is None:
        raise PreconditionRequired("history structure requires its archive root If-Match")
    try:
        provenance_structure_object_path(object_id)
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    root = parse_quoted_sha256_identity(if_match)
    content = container.provenance.get_structure_object(
        collection_id, object_id, expected_root=root, principal=principal
    )
    return Response(content, media_type="application/json", headers={"ETag": f'"{root}"'})


@router.get(
    "/collections/{collection_id}/provenance/artifacts/{artifact_id}/history-binding-proof",
    response_class=Response,
    responses=_STRUCTURE_RESPONSE,
    openapi_extra=operation_interface("client-only-primitive"),
)
def get_collection_artifact_history_binding_proof(
    collection_id: CollectionIdParameter,
    artifact_id: ArtifactId,
    principal: ProvenanceExporter,
    container: ContainerDep,
    if_match: Annotated[str | None, Header(alias="If-Match")] = None,
) -> Response:
    if if_match is None:
        raise PreconditionRequired("history binding proof requires its archive root If-Match")
    root = parse_quoted_sha256_identity(if_match)
    content = container.provenance.get_history_binding_proof(
        collection_id, artifact_id, expected_root=root, principal=principal
    )
    return Response(content, media_type="application/json", headers={"ETag": f'"{root}"'})


_PROVENANCE_JOURNAL_RESPONSE: dict[int | str, dict[str, Any]] = {
    200: {
        "description": "Exact immutable canonical provenance journal.",
        "headers": {
            "Content-Length": {"required": True, "schema": {"type": "integer", "minimum": 0}},
            "ETag": {"required": True, "schema": {"type": "string"}},
            "Accept-Ranges": {"required": True, "schema": {"type": "string", "const": "bytes"}},
        },
        "content": {"application/json-seq": {"schema": {"type": "string", "format": "binary"}}},
    },
    206: {
        "description": "Exact immutable journal byte range.",
        "headers": {
            "Content-Length": {"required": True, "schema": {"type": "integer"}},
            "Content-Range": {"required": True, "schema": {"type": "string"}},
            "ETag": {"required": True, "schema": {"type": "string"}},
            "Accept-Ranges": {"required": True, "schema": {"type": "string", "const": "bytes"}},
        },
        "content": {"application/json-seq": {"schema": {"type": "string", "format": "binary"}}},
    },
}


@router.get(
    "/collections/{collection_id}/provenance/artifacts",
    response_model=ListCollectionArtifactProvenanceOut,
    openapi_extra=operation_interface("standard-tool/protocol"),
)
def list_collection_artifact_provenance(
    collection_id: CollectionIdParameter,
    principal: ProvenanceReader,
    container: ContainerDep,
    page_size: Annotated[int, Query(ge=1, le=200)] = 50,
    after_artifact_id: ArtifactId | None = None,
    if_match: Annotated[str | None, Header(alias="If-Match")] = None,
) -> dict[str, Any]:
    if after_artifact_id is not None and if_match is None:
        raise PreconditionRequired("provenance continuation requires the archive root If-Match")
    expected_root = parse_quoted_sha256_identity(if_match) if if_match is not None else None
    page = container.provenance.list_artifacts(
        collection_id,
        page_size=page_size,
        after_artifact_id=after_artifact_id,
        principal=principal,
    )
    if expected_root is not None and page["archive_root_sha256"] != expected_root:
        raise PreconditionFailed("collection archive root changed")
    return page


@router.get(
    "/collections/{collection_id}/provenance/artifacts/{artifact_id}",
    response_model=CollectionArtifactProvenanceDetailOut,
    openapi_extra=operation_interface("standard-tool/protocol"),
)
def get_collection_artifact_provenance(
    collection_id: CollectionIdParameter,
    artifact_id: ArtifactId,
    principal: ProvenanceReader,
    container: ContainerDep,
) -> dict[str, Any]:
    return container.provenance.get_artifact(collection_id, artifact_id, principal=principal)


@router.get(
    "/collections/{collection_id}/provenance/journals",
    response_model=ListCollectionProvenanceJournalsOut,
    openapi_extra=operation_interface("standard-tool/protocol"),
)
def list_collection_provenance_journals(
    collection_id: CollectionIdParameter,
    principal: ProvenanceExporter,
    container: ContainerDep,
    page_size: Annotated[int, Query(ge=1, le=200)] = 50,
    after_journal_id: ProvenanceJournalId | None = None,
    if_match: Annotated[str | None, Header(alias="If-Match")] = None,
) -> dict[str, Any]:
    if after_journal_id is not None and if_match is None:
        raise PreconditionRequired("provenance continuation requires the archive root If-Match")
    expected_root = parse_quoted_sha256_identity(if_match) if if_match is not None else None
    page = container.provenance.list_journals(
        collection_id,
        page_size=page_size,
        after_journal_id=after_journal_id,
        principal=principal,
    )
    if expected_root is not None and page["archive_root_sha256"] != expected_root:
        raise PreconditionFailed("collection archive root changed")
    return page


@router.head(
    "/collections/{collection_id}/provenance/journals/{journal_id}",
    include_in_schema=False,
    operation_id="head_collection_provenance_journal",
    openapi_extra=operation_interface("standard-tool/protocol"),
)
@router.get(
    "/collections/{collection_id}/provenance/journals/{journal_id}",
    response_class=Response,
    responses=_PROVENANCE_JOURNAL_RESPONSE,
    openapi_extra=operation_interface("client-only-primitive"),
)
def stream_collection_provenance_journal(
    collection_id: CollectionIdParameter,
    journal_id: ProvenanceJournalId,
    principal: ProvenanceExporter,
    container: ContainerDep,
    http_request: Request,
    range_header: Annotated[str | None, Header(alias="Range")] = None,
    if_match: Annotated[str | None, Header(alias="If-Match")] = None,
) -> Response:
    byte_count, sha256 = container.provenance.journal_metadata(
        collection_id, journal_id, principal=principal
    )
    if range_header is not None:
        if if_match is None:
            raise PreconditionRequired("provenance journal continuation requires If-Match")
        if parse_quoted_sha256_identity(if_match) != sha256:
            raise PreconditionFailed("provenance journal identity changed")
    start, end = _parse_range(range_header, byte_count)
    status_code = 206 if range_header is not None else 200
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Length": str(end - start),
        "ETag": f'"{sha256}"',
        "Content-Disposition": f'attachment; filename="{journal_id}.json-seq"',
    }
    if status_code == 206:
        headers["Content-Range"] = f"bytes {start}-{end - 1}/{byte_count}"
    if http_request.method == "HEAD":
        return Response(status_code=status_code, headers=headers)

    def content() -> Iterator[bytes]:
        yield from container.provenance.iter_journal_range(
            collection_id,
            journal_id,
            offset=start,
            size=end - start,
            principal=principal,
        )

    return StreamingResponse(
        content(), status_code=status_code, media_type="application/json-seq", headers=headers
    )


def _parse_range(value: str | None, total_bytes: int) -> tuple[int, int]:
    if value is None:
        return 0, total_bytes
    unit, separator, raw = value.partition("=")
    if unit.casefold() != "bytes" or not separator or "," in raw:
        raise BadRequest("only one bytes range is supported")
    start_raw, dash, end_raw = raw.partition("-")
    if not dash:
        raise BadRequest("invalid bytes range")
    try:
        if not start_raw:
            suffix = int(end_raw)
            if suffix <= 0:
                raise ValueError
            start = max(0, total_bytes - suffix)
            end = total_bytes
        else:
            start = int(start_raw)
            end = total_bytes if not end_raw else int(end_raw) + 1
    except ValueError as exc:
        raise BadRequest("invalid bytes range") from exc
    if start < 0 or start >= total_bytes or end <= start or end > total_bytes:
        raise BadRequest("bytes range is outside the journal")
    return start, end
