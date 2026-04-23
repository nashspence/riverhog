from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path, PurePosixPath

from arc_core.domain.errors import BadRequest

_DURATION_RE = re.compile(r"^(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$")


def _parse_duration(value: str) -> timedelta:
    m = _DURATION_RE.match(value.strip())
    if not m or not any(m.groups()):
        raise ValueError(f"invalid duration {value!r}: expected format like '24h', '30m', '90s'")
    hours = int(m.group(1) or 0)
    minutes = int(m.group(2) or 0)
    seconds = int(m.group(3) or 0)
    return timedelta(hours=hours, minutes=minutes, seconds=seconds)


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    staging_root: Path
    sqlite_path: Path
    incomplete_upload_ttl: timedelta = field(default_factory=lambda: timedelta(hours=24))

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
    ttl_raw = os.getenv("INCOMPLETE_UPLOAD_TTL", "24h")

    staging_root = Path(staging_root_raw).expanduser().resolve(strict=False)
    sqlite_path = Path(sqlite_path_raw).expanduser().resolve()
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    incomplete_upload_ttl = _parse_duration(ttl_raw)

    return RuntimeConfig(
        staging_root=staging_root,
        sqlite_path=sqlite_path,
        incomplete_upload_ttl=incomplete_upload_ttl,
    )
