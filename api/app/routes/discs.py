from __future__ import annotations

import secrets
import shutil
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from ..config import DOWNLOAD_CHUNK_SIZE, TUSD_BASE_URL
from ..db import SessionLocal
from ..models import ArchivePiece, CacheSession, Disc, DiscEntry, DownloadSession, UploadSlot
from ..planner import force_close_pending, import_closed_discs
from ..progress import download_stream_name, publish_progress
from ..schemas import (
    CacheSessionCompleteResponse,
    CacheSessionCreateResponse,
    CacheUploadSlotRequest,
    DownloadSessionCreateResponse,
    IsoRegisterRequest,
    OfflineError,
    TreeNode,
    TreeResponse,
    UploadSlotCreateResponse,
)
from ..storage import active_cache_root, cache_staging_root, canonical_tree_hash, disc_tree_nodes, normalize_relpath, rebuild_job_export, registered_iso_storage_path

router = APIRouter(prefix="/v1/discs", tags=["discs"])


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


Db = Annotated[Session, Depends(get_db)]


@router.post("/flush")
def flush_pending(force: bool = False, db: Session = Depends(get_db)):
    closed = force_close_pending(db) if force else []
    disc_ids = import_closed_discs(db, closed) if closed else []
    touched_jobs: set[str] = set()
    for disc_id in disc_ids:
        disc = db.execute(select(Disc).where(Disc.id == disc_id).options(selectinload(Disc.archive_pieces).selectinload(ArchivePiece.job_file))).scalar_one()
        touched_jobs.update(piece.job_file.job_id for piece in disc.archive_pieces)
    for job_id in sorted(touched_jobs):
        rebuild_job_export(db, job_id)
    return {"status": "ok", "closed_discs": disc_ids}


@router.get("/{disc_id}/tree", response_model=TreeResponse)
def disc_tree(disc_id: str, db: Db) -> TreeResponse:
    disc = db.execute(select(Disc).where(Disc.id == disc_id).options(selectinload(Disc.entries))).scalar_one_or_none()
    if disc is None:
        raise HTTPException(status_code=404, detail="disc not found")
    nodes = [TreeNode(**node) for node in disc_tree_nodes(disc)]
    return TreeResponse(root_id=disc_id, root_kind="disc", nodes=nodes)


@router.get("/{disc_id}/content/{disc_relative_path:path}", responses={409: {"model": OfflineError}})
def get_disc_file(disc_id: str, disc_relative_path: str, db: Db):
    disc_rel = normalize_relpath(disc_relative_path)
    disc = db.execute(select(Disc).where(Disc.id == disc_id)).scalar_one_or_none()
    if disc is None:
        raise HTTPException(status_code=404, detail="disc not found")
    entry = db.execute(select(DiscEntry).where(DiscEntry.disc_id == disc_id, DiscEntry.relative_path == disc_rel)).scalar_one_or_none()
    if entry is None:
        raise HTTPException(status_code=404, detail="file not found")
    if not disc.cached_root_abs_path:
        return JSONResponse(status_code=409, content={"error": "disc_offline", "message": f"This file is on partition {disc_id}, which is offline right now.", "disc_ids": [disc_id]})
    path = Path(disc.cached_root_abs_path) / disc_rel
    if not path.exists():
        return JSONResponse(status_code=409, content={"error": "disc_offline", "message": f"This file is on partition {disc_id}, which is offline right now.", "disc_ids": [disc_id]})
    return FileResponse(path=path, filename=Path(disc_rel).name, media_type="application/octet-stream")


@router.post("/{disc_id}/cache/sessions", response_model=CacheSessionCreateResponse)
def create_cache_session(disc_id: str, db: Db) -> CacheSessionCreateResponse:
    disc = db.execute(select(Disc).where(Disc.id == disc_id).options(selectinload(Disc.entries))).scalar_one_or_none()
    if disc is None:
        raise HTTPException(status_code=404, detail="disc not found")
    session = CacheSession(disc_id=disc_id, expected_total_bytes=disc.total_root_bytes)
    db.add(session)
    db.commit()
    return CacheSessionCreateResponse(session_id=session.id, disc_id=disc_id, expected_total_bytes=disc.total_root_bytes, expected_files=len(disc.entries), progress_stream_url=f"/v1/progress/cache-sessions/{session.id}/stream")


@router.get("/{disc_id}/cache/sessions/{session_id}/expected")
def cache_session_expected(disc_id: str, session_id: str, db: Db):
    session = db.execute(select(CacheSession).where(CacheSession.id == session_id, CacheSession.disc_id == disc_id)).scalar_one_or_none()
    if session is None:
        raise HTTPException(status_code=404, detail="cache session not found")
    entries = db.execute(select(DiscEntry).where(DiscEntry.disc_id == disc_id).order_by(DiscEntry.relative_path)).scalars().all()
    return {"disc_id": disc_id, "session_id": session_id, "entries": [{"relative_path": e.relative_path, "size_bytes": e.size_bytes, "sha256": e.sha256, "kind": e.kind} for e in entries]}


@router.post("/{disc_id}/cache/sessions/{session_id}/uploads", response_model=UploadSlotCreateResponse)
def create_cache_upload_slot(disc_id: str, session_id: str, body: CacheUploadSlotRequest, db: Db) -> UploadSlotCreateResponse:
    session_obj = db.execute(select(CacheSession).where(CacheSession.id == session_id, CacheSession.disc_id == disc_id)).scalar_one_or_none()
    if session_obj is None:
        raise HTTPException(status_code=404, detail="cache session not found")
    if session_obj.status not in {"open", "uploading"}:
        raise HTTPException(status_code=409, detail="cache session is closed")
    rel = normalize_relpath(body.relative_path)
    entry = db.execute(select(DiscEntry).where(DiscEntry.disc_id == disc_id, DiscEntry.relative_path == rel)).scalar_one_or_none()
    if entry is None:
        raise HTTPException(status_code=409, detail="path is not part of the known partition root")
    existing = db.execute(select(UploadSlot).where(UploadSlot.cache_session_id == session_id, UploadSlot.relative_path == rel)).scalar_one_or_none()
    if existing and existing.status == "completed":
        raise HTTPException(status_code=409, detail="path already uploaded for this cache session")

    upload_id = secrets.token_hex(16)
    upload_token = secrets.token_urlsafe(32)
    slot = UploadSlot(upload_id=upload_id, upload_token=upload_token, kind="cache_file", relative_path=rel, size_bytes=entry.size_bytes, expected_sha256=entry.sha256, cache_session_id=session_id)
    db.add(slot)
    session_obj.status = "uploading"
    db.commit()
    return UploadSlotCreateResponse(upload_id=upload_id, upload_token=upload_token, tus_create_url=TUSD_BASE_URL, tus_metadata={"upload_id": upload_id, "upload_token": upload_token, "relative_path": rel}, upload_stream_url=f"/v1/progress/uploads/{upload_id}/stream", aggregate_stream_url=f"/v1/progress/cache-sessions/{session_id}/stream")


@router.post("/{disc_id}/cache/sessions/{session_id}/complete", response_model=CacheSessionCompleteResponse)
def complete_cache_session(disc_id: str, session_id: str, db: Db) -> CacheSessionCompleteResponse:
    disc = db.execute(select(Disc).where(Disc.id == disc_id).options(selectinload(Disc.entries), selectinload(Disc.archive_pieces).selectinload(ArchivePiece.job_file))).scalar_one_or_none()
    if disc is None:
        raise HTTPException(status_code=404, detail="disc not found")
    cache_session = db.execute(select(CacheSession).where(CacheSession.id == session_id, CacheSession.disc_id == disc_id)).scalar_one_or_none()
    if cache_session is None:
        raise HTTPException(status_code=404, detail="cache session not found")

    staging = cache_staging_root(session_id)
    if not staging.exists():
        raise HTTPException(status_code=409, detail="cache session has no uploaded root")

    actual_hash, total_bytes, rows = canonical_tree_hash(staging)
    expected = {(e.relative_path, e.size_bytes, e.sha256) for e in disc.entries}
    actual = {(str(r["relative_path"]), int(r["size_bytes"]), str(r["sha256"])) for r in rows}
    if actual_hash != disc.contents_hash or actual != expected or total_bytes != disc.total_root_bytes:
        cache_session.status = "failed"
        db.commit()
        raise HTTPException(status_code=409, detail="uploaded root does not match the known partition contents")

    active = active_cache_root(disc_id)
    if active.exists():
        shutil.rmtree(active)
    active.parent.mkdir(parents=True, exist_ok=True)
    staging.replace(active)
    disc.cached_root_abs_path = str(active)
    disc.status = "cached"
    cache_session.status = "completed"
    cache_session.uploaded_bytes = disc.total_root_bytes
    db.commit()

    touched_jobs = sorted({piece.job_file.job_id for piece in disc.archive_pieces})
    for job_id in touched_jobs:
        rebuild_job_export(db, job_id)
    return CacheSessionCompleteResponse(disc_id=disc_id, session_id=session_id, status="cached", contents_hash=actual_hash)


@router.delete("/{disc_id}/cache")
def evict_disc_cache(disc_id: str, db: Db):
    disc = db.execute(select(Disc).where(Disc.id == disc_id).options(selectinload(Disc.archive_pieces).selectinload(ArchivePiece.job_file))).scalar_one_or_none()
    if disc is None:
        raise HTTPException(status_code=404, detail="disc not found")
    active = active_cache_root(disc_id)
    if active.exists():
        shutil.rmtree(active)
    disc.cached_root_abs_path = None
    disc.status = "offline"
    db.commit()
    touched_jobs = sorted({piece.job_file.job_id for piece in disc.archive_pieces})
    for job_id in touched_jobs:
        rebuild_job_export(db, job_id)
    return {"status": "ok", "disc_id": disc_id}


@router.post("/{disc_id}/iso/register")
def register_iso(disc_id: str, body: IsoRegisterRequest, db: Db):
    disc = db.get(Disc, disc_id)
    if disc is None:
        raise HTTPException(status_code=404, detail="disc not found")
    src = Path(body.server_path)
    if not src.exists() or not src.is_file():
        raise HTTPException(status_code=404, detail="server_path not found")
    dest = registered_iso_storage_path(disc_id)
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)
    disc.iso_abs_path = str(dest)
    disc.iso_size_bytes = dest.stat().st_size
    db.commit()
    return {"status": "ok", "disc_id": disc_id, "iso_path": str(dest), "size_bytes": disc.iso_size_bytes}


@router.post("/{disc_id}/download-sessions", response_model=DownloadSessionCreateResponse)
async def create_download_session(disc_id: str, db: Db) -> DownloadSessionCreateResponse:
    disc = db.get(Disc, disc_id)
    if disc is None:
        raise HTTPException(status_code=404, detail="disc not found")
    if not disc.iso_abs_path or not Path(disc.iso_abs_path).exists():
        raise HTTPException(status_code=409, detail="iso not registered or not online")
    total_bytes = Path(disc.iso_abs_path).stat().st_size
    session = DownloadSession(disc_id=disc_id, total_bytes=total_bytes)
    db.add(session)
    db.commit()
    await publish_progress(download_stream_name(session.id), {"status": "ready", "bytes_sent": 0, "total_bytes": total_bytes})
    return DownloadSessionCreateResponse(session_id=session.id, disc_id=disc_id, total_bytes=total_bytes, progress_stream_url=f"/v1/progress/downloads/{session.id}/stream", content_url=f"/v1/discs/downloads/{session.id}/content")


@router.get("/downloads/{session_id}/content")
async def stream_iso(session_id: str, db: Db):
    session = db.get(DownloadSession, session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="download session not found")
    disc = db.get(Disc, session.disc_id)
    if disc is None or not disc.iso_abs_path or not Path(disc.iso_abs_path).exists():
        raise HTTPException(status_code=409, detail="iso is offline")
    iso_path = Path(disc.iso_abs_path)
    total_bytes = iso_path.stat().st_size

    async def iterator():
        sent = 0
        worker_db = SessionLocal()
        try:
            dl = worker_db.get(DownloadSession, session_id)
            if dl is not None:
                dl.status = "streaming"
                worker_db.commit()
            await publish_progress(download_stream_name(session_id), {"status": "streaming", "bytes_sent": 0, "total_bytes": total_bytes})
            with iso_path.open("rb") as handle:
                while True:
                    chunk = handle.read(DOWNLOAD_CHUNK_SIZE)
                    if not chunk:
                        break
                    sent += len(chunk)
                    dl = worker_db.get(DownloadSession, session_id)
                    if dl is not None:
                        dl.bytes_sent = sent
                        dl.status = "streaming"
                        worker_db.commit()
                    await publish_progress(download_stream_name(session_id), {"status": "streaming", "bytes_sent": sent, "total_bytes": total_bytes})
                    yield chunk
            dl = worker_db.get(DownloadSession, session_id)
            if dl is not None:
                dl.bytes_sent = sent
                dl.status = "completed"
                worker_db.commit()
            await publish_progress(download_stream_name(session_id), {"status": "completed", "bytes_sent": sent, "total_bytes": total_bytes})
        except Exception:
            dl = worker_db.get(DownloadSession, session_id)
            if dl is not None:
                dl.bytes_sent = sent
                dl.status = "failed"
                worker_db.commit()
            await publish_progress(download_stream_name(session_id), {"status": "failed", "bytes_sent": sent, "total_bytes": total_bytes})
            raise
        finally:
            worker_db.close()

    headers = {"Content-Length": str(total_bytes), "Content-Disposition": f'attachment; filename="{iso_path.name}"'}
    return StreamingResponse(iterator(), media_type="application/octet-stream", headers=headers)
