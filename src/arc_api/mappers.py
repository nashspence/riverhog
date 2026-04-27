from __future__ import annotations

from arc_core.domain.models import (
    CollectionCoverageImage,
    CollectionSummary,
    CopySummary,
    FetchSummary,
    GlacierArchiveStatus,
    PinSummary,
)


def map_glacier(summary: GlacierArchiveStatus) -> dict[str, object]:
    return {
        "state": summary.state.value,
        "object_path": summary.object_path,
        "stored_bytes": summary.stored_bytes,
        "backend": summary.backend,
        "storage_class": summary.storage_class,
        "last_uploaded_at": summary.last_uploaded_at,
        "last_verified_at": summary.last_verified_at,
        "failure": summary.failure,
    }


def map_collection(summary: CollectionSummary) -> dict[str, object]:
    return {
        "id": str(summary.id),
        "files": summary.files,
        "bytes": summary.bytes,
        "hot_bytes": summary.hot_bytes,
        "archived_bytes": summary.archived_bytes,
        "pending_bytes": summary.pending_bytes,
        "protection_state": summary.protection_state.value,
        "protected_bytes": summary.protected_bytes,
        "image_coverage": [map_collection_coverage_image(image) for image in summary.image_coverage],
    }


def map_copy(summary: CopySummary) -> dict[str, object]:
    return {
        "id": str(summary.id),
        "volume_id": summary.volume_id,
        "location": summary.location,
        "created_at": summary.created_at,
        "state": summary.state.value,
    }


def map_collection_coverage_image(summary: CollectionCoverageImage) -> dict[str, object]:
    return {
        "id": str(summary.id),
        "filename": summary.filename,
        "protection_state": summary.protection_state.value,
        "physical_copies_required": summary.physical_copies_required,
        "physical_copies_registered": summary.physical_copies_registered,
        "physical_copies_missing": summary.physical_copies_missing,
        "copies": [map_copy(copy) for copy in summary.copies],
        "glacier": map_glacier(summary.glacier),
    }


def map_fetch(summary: FetchSummary) -> dict[str, object]:
    return {
        "id": str(summary.id),
        "target": str(summary.target),
        "state": summary.state.value,
        "files": summary.files,
        "bytes": summary.bytes,
        "entries_total": summary.entries_total,
        "entries_pending": summary.entries_pending,
        "entries_partial": summary.entries_partial,
        "entries_byte_complete": summary.entries_byte_complete,
        "entries_uploaded": summary.entries_uploaded,
        "uploaded_bytes": summary.uploaded_bytes,
        "missing_bytes": summary.missing_bytes,
        "upload_state_expires_at": summary.upload_state_expires_at,
        "copies": [
            {"id": str(c.id), "volume_id": c.volume_id, "location": c.location}
            for c in summary.copies
        ],
    }


def map_pin(summary: PinSummary) -> dict[str, object]:
    return {
        "target": str(summary.target),
        "fetch": {
            "id": str(summary.fetch.id),
            "state": summary.fetch.state.value,
            "copies": [
                {"id": str(copy.id), "volume_id": copy.volume_id, "location": copy.location}
                for copy in summary.fetch.copies
            ],
        },
    }
