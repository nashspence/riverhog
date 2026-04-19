from __future__ import annotations

import shutil
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from ..db import SessionLocal
from ..models import ArchivePiece, Collection, CollectionFile
from ..notifications import isoformat_z
from ..planner import import_closed_containers, ingest_collection
from ..schemas import (
    CollectionListResponse,
    CollectionSealRequest,
    CollectionSummary,
    SealCollectionResponse,
    TreeNode,
    TreeResponse,
)
from ..storage import (
    buffered_collection_root,
    collection_id_from_upload_path,
    export_collection_root,
    inactive_collection_hash_manifest_path,
    inactive_collection_hash_proof_path,
    recompute_collection_file_runtime,
    rebuild_collection_export,
    refresh_collection_hash_artifacts,
    release_collection_buffer_files,
    sync_collection_from_buffer,
    upload_collection_root,
)
from ..transfers import sync_tree

router = APIRouter(prefix="/v1/collections", tags=["collections"])


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


Db = Annotated[Session, Depends(get_db)]


@router.get("", response_model=CollectionListResponse)
def list_collections(db: Db) -> CollectionListResponse:
    collections = (
        db.execute(
            select(Collection)
            .options(selectinload(Collection.files), selectinload(Collection.directories))
            .order_by(Collection.created_at.desc(), Collection.id.asc())
        )
        .scalars()
        .all()
    )

    summaries = [
        CollectionSummary(
            collection_id=collection.id,
            status=collection.status,
            upload_relative_path=collection.upload_relpath,
            upload_path=str(upload_collection_root(collection.upload_relpath)),
            buffer_path=str(buffered_collection_root(collection.id)) if buffered_collection_root(collection.id).exists() else None,
            description=collection.description,
            keep_buffer_after_archive=collection.keep_buffer_after_archive,
            file_count=len(collection.files),
            directory_count=len(collection.directories),
            created_at=isoformat_z(collection.created_at) or "",
            sealed_at=isoformat_z(collection.sealed_at),
            export_path=str(export_collection_root(collection.id)),
            hash_manifest_path=str(inactive_collection_hash_manifest_path(collection.id)),
            hash_proof_path=str(inactive_collection_hash_proof_path(collection.id)),
        )
        for collection in collections
    ]
    return CollectionListResponse(collections=summaries)


@router.post("/seal", response_model=SealCollectionResponse)
def seal_collection(body: CollectionSealRequest, db: Db) -> SealCollectionResponse:
    upload_relpath = body.upload_path
    source_root = upload_collection_root(upload_relpath)
    if not source_root.exists() or not source_root.is_dir():
        raise HTTPException(status_code=404, detail="upload directory not found")

    collection_id = collection_id_from_upload_path(upload_relpath)
    if db.get(Collection, collection_id) is not None:
        raise HTTPException(status_code=409, detail="collection already exists for this upload path")

    buffered_root = buffered_collection_root(collection_id)
    buffered_root.parent.mkdir(parents=True, exist_ok=True)

    try:
        sync_tree(source_root, buffered_root)
        collection = Collection(
            id=collection_id,
            status="buffered",
            upload_relpath=upload_relpath,
            description=body.description,
            keep_buffer_after_archive=body.keep_buffer_after_archive,
        )
        db.add(collection)
        db.flush()
        sync_collection_from_buffer(db, collection_id)
        refresh_collection_hash_artifacts(db, collection_id)
        result = ingest_collection(db, collection_id)
        closed_ids = import_closed_containers(db, result["closed"])
        rebuild_collection_export(db, collection_id)
        shutil.rmtree(source_root, ignore_errors=True)
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception:
        db.rollback()
        raise

    return SealCollectionResponse(
        collection_id=collection_id,
        status="sealed",
        closed_containers=closed_ids,
        buffer_bytes=result["buffer_bytes"],
    )


@router.get("/{collection_id}/tree", response_model=TreeResponse)
def collection_tree(collection_id: str, db: Db) -> TreeResponse:
    collection = (
        db.execute(
            select(Collection)
            .where(Collection.id == collection_id)
            .options(
                selectinload(Collection.directories),
                selectinload(Collection.files)
                .selectinload(CollectionFile.archive_pieces)
                .selectinload(ArchivePiece.container),
            )
        )
        .scalar_one_or_none()
    )
    if collection is None:
        raise HTTPException(status_code=404, detail="collection not found")

    nodes: list[TreeNode] = []
    explicit_dirs = {d.relative_path for d in collection.directories}
    derived_dirs = set()
    for collection_file in collection.files:
        parts = collection_file.relative_path.split("/")
        for index in range(1, len(parts)):
            derived_dirs.add("/".join(parts[:index]))
    for rel in sorted(explicit_dirs | derived_dirs):
        nodes.append(TreeNode(path=rel, kind="directory", active=True, source="virtual", status="listed"))
    for collection_file in sorted(collection.files, key=lambda item: item.relative_path):
        active_path, source, container_ids = recompute_collection_file_runtime(collection_file)
        nodes.append(
            TreeNode(
                path=collection_file.relative_path,
                kind="file",
                size_bytes=collection_file.size_bytes,
                active=active_path is not None,
                source=source,
                container_ids=container_ids or sorted({piece.container_id for piece in collection_file.archive_pieces}),
                status=collection_file.status,
                extra=None,
            )
        )
    return TreeResponse(root_id=collection_id, root_kind="collection", nodes=nodes)


@router.post("/{collection_id}/buffer/release")
def release_collection_buffer(collection_id: str, db: Db):
    if db.get(Collection, collection_id) is None:
        raise HTTPException(status_code=404, detail="collection not found")
    release_collection_buffer_files(db, collection_id)
    return {"status": "ok", "collection_id": collection_id}
