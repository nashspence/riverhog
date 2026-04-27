from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


@dataclass(frozen=True)
class ArchiveUploadReceipt:
    object_path: str
    stored_bytes: int
    backend: str
    storage_class: str
    uploaded_at: str
    verified_at: str | None = None


class ArchiveStore(Protocol):
    def upload_finalized_image(
        self,
        *,
        image_id: str,
        filename: str,
        image_root: Path,
    ) -> ArchiveUploadReceipt: ...
