from __future__ import annotations

from fastapi import APIRouter

from ..progress import download_stream_name, progress_stream

router = APIRouter(prefix="/v1/progress", tags=["progress"])


@router.get("/downloads/{session_id}/stream")
def download_progress_stream(session_id: str):
    return progress_stream(download_stream_name(session_id))
