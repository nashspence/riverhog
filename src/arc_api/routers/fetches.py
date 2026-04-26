from __future__ import annotations

from fastapi import APIRouter, Request, Response

from arc_api.deps import ContainerDep
from arc_api.mappers import map_fetch
from arc_api.schemas.fetches import (
    CompleteFetchResponse,
    FetchManifestResponse,
    FetchSummaryOut,
    FetchUploadSessionResponse,
)
from arc_core.domain.errors import BadRequest

router = APIRouter(tags=["fetches"])
_TUS_RESUMABLE = "1.0.0"
_TUS_EXTENSIONS = "checksum,expiration,termination"
_TUS_CHECKSUM_ALGORITHMS = "sha256"


def _fetch_upload_headers(payload: dict[str, object], *, request: Request) -> dict[str, str]:
    headers = {
        "Tus-Resumable": _TUS_RESUMABLE,
        "Cache-Control": "no-store",
        "Upload-Offset": str(payload["offset"]),
        "Upload-Length": str(payload["length"]),
        "Location": str(request.url),
    }
    if payload.get("expires_at") is not None:
        headers["Upload-Expires"] = str(payload["expires_at"])
    return headers


@router.get("/fetches/{fetch_id}", response_model=FetchSummaryOut)
def get_fetch(fetch_id: str, container: ContainerDep) -> FetchSummaryOut:
    summary = container.fetches.get(fetch_id)
    return FetchSummaryOut.model_validate(map_fetch(summary))


@router.get("/fetches/{fetch_id}/manifest", response_model=FetchManifestResponse)
def get_manifest(fetch_id: str, container: ContainerDep) -> FetchManifestResponse:
    payload = container.fetches.manifest(fetch_id)
    return FetchManifestResponse.model_validate(payload)


@router.post(
    "/fetches/{fetch_id}/entries/{entry_id}/upload", response_model=FetchUploadSessionResponse
)
def create_or_resume_fetch_entry_upload(
    fetch_id: str,
    entry_id: str,
    request: Request,
    response: Response,
    container: ContainerDep,
) -> FetchUploadSessionResponse:
    payload = container.fetches.create_or_resume_upload(fetch_id=fetch_id, entry_id=entry_id)
    payload["upload_url"] = str(request.url)
    response.headers.update(_fetch_upload_headers(payload, request=request))
    return FetchUploadSessionResponse.model_validate(payload)


@router.patch("/fetches/{fetch_id}/entries/{entry_id}/upload", status_code=204)
async def append_fetch_entry_upload_chunk(
    fetch_id: str,
    entry_id: str,
    request: Request,
    container: ContainerDep,
) -> Response:
    raw_offset = request.headers.get("Upload-Offset")
    raw_checksum = request.headers.get("Upload-Checksum")
    tus_resumable = request.headers.get("Tus-Resumable")
    if raw_offset is None:
        raise BadRequest("missing Upload-Offset header")
    if raw_checksum is None:
        raise BadRequest("missing Upload-Checksum header")
    if tus_resumable != _TUS_RESUMABLE:
        raise BadRequest(f"Tus-Resumable header must be {_TUS_RESUMABLE}")
    try:
        offset = int(raw_offset)
    except ValueError as exc:
        raise BadRequest("Upload-Offset header must be an integer") from exc

    payload = container.fetches.append_upload_chunk(
        fetch_id,
        entry_id,
        offset=offset,
        checksum=raw_checksum,
        content=await request.body(),
    )
    return Response(status_code=204, headers=_fetch_upload_headers(payload, request=request))


@router.head("/fetches/{fetch_id}/entries/{entry_id}/upload", status_code=204)
def head_fetch_entry_upload(
    fetch_id: str,
    entry_id: str,
    request: Request,
    container: ContainerDep,
) -> Response:
    payload = container.fetches.get_entry_upload(fetch_id, entry_id)
    return Response(status_code=204, headers=_fetch_upload_headers(payload, request=request))


@router.delete("/fetches/{fetch_id}/entries/{entry_id}/upload", status_code=204)
def delete_fetch_entry_upload(
    fetch_id: str,
    entry_id: str,
    container: ContainerDep,
) -> Response:
    container.fetches.cancel_entry_upload(fetch_id, entry_id)
    return Response(
        status_code=204,
        headers={
            "Tus-Resumable": _TUS_RESUMABLE,
            "Cache-Control": "no-store",
        },
    )


@router.options("/fetches/{fetch_id}/entries/{entry_id}/upload", status_code=204)
def options_fetch_entry_upload() -> Response:
    return Response(
        status_code=204,
        headers={
            "Tus-Resumable": _TUS_RESUMABLE,
            "Tus-Version": _TUS_RESUMABLE,
            "Tus-Extension": _TUS_EXTENSIONS,
            "Tus-Checksum-Algorithm": _TUS_CHECKSUM_ALGORITHMS,
        },
    )


@router.post("/fetches/{fetch_id}/complete", response_model=CompleteFetchResponse)
def complete_fetch(fetch_id: str, container: ContainerDep) -> CompleteFetchResponse:
    payload = container.fetches.complete(fetch_id)
    return CompleteFetchResponse.model_validate(payload)
