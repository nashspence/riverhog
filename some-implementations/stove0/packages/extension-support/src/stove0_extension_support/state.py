"""Atomic files for component-owned restart state, without a shared job schema."""

from __future__ import annotations

import json
import os
import secrets
from pathlib import Path

from pydantic import BaseModel


def write_state_model(root: Path, path: Path, model: BaseModel) -> None:
    if path.parent != root or path.is_symlink():
        raise ValueError("component state path is outside its owner or is a symlink")
    temporary = root / f".{path.name}.{secrets.token_hex(16)}.part"
    # Restart state preserves scalar types of embedded, separately owned
    # authorities. Their own canonical encoders still determine identities.
    encoded = json.dumps(
        model.model_dump(mode="json", by_alias=True),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    descriptor = os.open(
        temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)
