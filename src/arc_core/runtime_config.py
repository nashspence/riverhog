from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from arc_core.domain.errors import BadRequest


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    staging_root: Path
    sqlite_path: Path

    def resolve_staging_path(self, logical_path: str) -> Path:
        candidate = logical_path.strip().replace("\\", "/")
        logical = PurePosixPath(candidate)
        if not logical.is_absolute():
            raise BadRequest("staging path must be absolute")

        parts = [part for part in logical.parts if part != "/"]
        try:
            staging_index = parts.index("staging")
        except ValueError as exc:
            raise BadRequest("staging path must include a staging root segment") from exc
        if len(parts) <= staging_index + 1:
            raise BadRequest("staging path must include a collection path beneath the staging root")

        resolved = self.staging_root.joinpath(*parts[staging_index + 1 :]).resolve()
        try:
            resolved.relative_to(self.staging_root.resolve())
        except ValueError as exc:
            raise BadRequest("staging path must stay within the configured staging root") from exc
        return resolved


def load_runtime_config() -> RuntimeConfig:
    staging_root_raw = os.getenv("ARC_STAGING_ROOT", "/staging")
    sqlite_path_raw = os.getenv("ARC_DB_PATH", ".arc/state.sqlite3")

    staging_root = Path(staging_root_raw).expanduser().resolve(strict=False)
    sqlite_path = Path(sqlite_path_raw).expanduser().resolve()
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)

    return RuntimeConfig(
        staging_root=staging_root,
        sqlite_path=sqlite_path,
    )
