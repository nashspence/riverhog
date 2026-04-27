from __future__ import annotations

import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from arc_core.iso.streaming import build_iso_cmd_from_root
from arc_core.ports.archive_store import ArchiveUploadReceipt
from arc_core.runtime_config import RuntimeConfig
from arc_core.stores.s3_support import create_glacier_s3_client


def _utc_now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class S3ArchiveStore:
    def __init__(self, config: RuntimeConfig) -> None:
        self._config = config
        self._bucket = config.glacier_bucket
        self._client = create_glacier_s3_client(config)

    def _object_key(self, *, image_id: str, filename: str) -> str:
        suffix = Path(filename).suffix or ".iso"
        return f"{self._config.glacier_prefix}/{image_id}/{image_id}{suffix}"

    def upload_finalized_image(
        self,
        *,
        image_id: str,
        filename: str,
        image_root: Path,
    ) -> ArchiveUploadReceipt:
        object_key = self._object_key(image_id=image_id, filename=filename)
        uploaded_at = _utc_now()

        with tempfile.TemporaryDirectory(prefix="arc-glacier-upload-") as tmpdir:
            iso_path = Path(tmpdir) / f"{image_id}.iso"
            with iso_path.open("wb") as handle:
                proc = subprocess.run(
                    build_iso_cmd_from_root(image_root=image_root, volume_id=image_id),
                    stdout=handle,
                    stderr=subprocess.PIPE,
                    check=False,
                )
            if proc.returncode != 0:
                detail = (proc.stderr or b"").decode("utf-8", errors="replace").strip()
                raise RuntimeError(detail or f"xorriso exited {proc.returncode}")

            # The canonical Garage harness does not emulate Glacier storage-class
            # semantics, so the runtime records the intended class in Riverhog's
            # catalog and object metadata instead of depending on backend support.
            self._client.upload_file(
                str(iso_path),
                self._bucket,
                object_key,
                ExtraArgs={
                    "Metadata": {
                        "arc-backend": self._config.glacier_backend,
                        "arc-storage-class": self._config.glacier_storage_class,
                    }
                },
            )
            head = self._client.head_object(Bucket=self._bucket, Key=object_key)

        verified_at = _utc_now()
        return ArchiveUploadReceipt(
            object_path=object_key,
            stored_bytes=int(head.get("ContentLength", 0)),
            backend=self._config.glacier_backend,
            storage_class=self._config.glacier_storage_class,
            uploaded_at=uploaded_at,
            verified_at=verified_at,
        )
