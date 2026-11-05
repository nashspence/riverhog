"""Initial metadata's original archive basis and current finalization claim."""

from __future__ import annotations

import hashlib
from typing import Any

from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy import select
from sqlalchemy.orm import Session

from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.collection_construction import upload_construction
from riverhog_core.services.archive_authority_checkpoint import (
    ArchiveRecoverySource,
    ArchiveRootSource,
)
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission


def initial_metadata_parent(session: Session, collection_id: int) -> Upload:
    upload = session.scalar(
        select(Upload)
        .where(Upload.collection_id == collection_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if upload is None:
        raise NotFound("initial metadata parent is unavailable")
    return upload


def require_initial_metadata_claim(session: Session, upload: Upload) -> None:
    if (
        upload.state != "finalizing"
        or upload.archive_phase != "finalizing"
        or upload.final_authority_json is None
    ):
        raise Conflict("initial metadata finalizer claim is not current")
    ArchiveWriteAdmission.require_no_unresolved_calls(session, upload.collection_id)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def current_initial_metadata_basis(session: Session, upload: Upload) -> dict[str, Any]:
    authority = ArchiveRecoverySource.load(session, upload.collection_id)
    recovery = authority.received(session)
    root = authority.root_source.received(session)
    if root is None or recovery is None:
        raise Conflict("initial metadata archive authority is incomplete")
    return {
        "construction_sha256": _sha(upload_construction(upload)),
        "archive_incarnation_id": upload.archive_incarnation_id,
        "archive_storage_prefix": upload.archive_storage_prefix,
        "passphrase_id": upload.passphrase_id,
        "archive_root_sha256": root.plaintext_sha256,
        "archive_root_receipt_sha256": _sha(authority.root_receipt_json),
    }


def require_original_metadata_basis(
    session: Session, upload: Upload, original: dict[str, Any]
) -> None:
    if (
        original["construction_sha256"] != _sha(upload_construction(upload))
        or original["archive_incarnation_id"] != upload.archive_incarnation_id
        or original["archive_storage_prefix"] != upload.archive_storage_prefix
        or original["passphrase_id"] != upload.passphrase_id
    ):
        raise Conflict("initial metadata original construction changed")
    root = ArchiveRootSource.load(session, upload.collection_id).received(session)
    if (
        root is None
        or root.plaintext_sha256 != original["archive_root_sha256"]
        or upload.archive_root_receipt_json is None
        or _sha(upload.archive_root_receipt_json) != original["archive_root_receipt_sha256"]
    ):
        raise Conflict("initial metadata original root receiving proof changed")
