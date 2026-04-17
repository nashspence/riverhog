from __future__ import annotations

from fastapi import APIRouter

from ..progress import cache_session_stream_name, download_stream_name, job_stream_name, progress_stream, upload_stream_name

router = APIRouter(prefix="/v1/progress", tags=["progress"])


@router.get("/uploads/{upload_id}/stream")
def upload_progress_stream(upload_id: str):
    return progress_stream(upload_stream_name(upload_id))


@router.get("/jobs/{job_id}/stream")
def job_progress_stream(job_id: str):
    return progress_stream(job_stream_name(job_id))


@router.get("/cache-sessions/{session_id}/stream")
def cache_session_progress_stream(session_id: str):
    return progress_stream(cache_session_stream_name(session_id))


@router.get("/downloads/{session_id}/stream")
def download_progress_stream(session_id: str):
    return progress_stream(download_stream_name(session_id))
