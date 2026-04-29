from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from arc_core.planner.manifest import MANIFEST_FILENAME
from arc_core.recovery_payloads import decrypt_recovery_payload


@dataclass(frozen=True, slots=True)
class FinalizedImageCoveragePart:
    collection_id: str
    path: str
    part_index: int
    part_count: int


def read_finalized_image_coverage_parts(image_root: str | Path) -> list[FinalizedImageCoveragePart]:
    manifest_path = Path(image_root) / MANIFEST_FILENAME
    manifest = yaml.safe_load(decrypt_recovery_payload(manifest_path.read_bytes()))
    rows: list[FinalizedImageCoveragePart] = []
    for collection in manifest.get("collections", []):
        collection_id = str(collection["id"])
        for file_entry in collection.get("files", []):
            path = str(file_entry["path"]).lstrip("/")
            parts_block = file_entry.get("parts")
            if parts_block is None:
                rows.append(
                    FinalizedImageCoveragePart(
                        collection_id=collection_id,
                        path=path,
                        part_index=0,
                        part_count=1,
                    )
                )
                continue
            part_count = int(parts_block["count"])
            for present in parts_block.get("present", []):
                rows.append(
                    FinalizedImageCoveragePart(
                        collection_id=collection_id,
                        path=path,
                        part_index=int(present["index"]) - 1,
                        part_count=part_count,
                    )
                )
    return rows
