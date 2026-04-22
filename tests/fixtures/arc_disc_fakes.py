from __future__ import annotations

import base64
import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any


def _fixture() -> dict[str, Any]:
    path = os.environ["ARC_DISC_FIXTURE_PATH"]
    return json.loads(Path(path).read_text(encoding="utf-8"))


class FixtureOpticalReader:
    def read_iter(self, disc_path: str, *, device: str) -> Iterator[bytes]:
        fixture = _fixture()
        reader = fixture["reader"]
        if disc_path in reader["fail_disc_paths"]:
            raise RuntimeError(f"fixture optical read failed for {disc_path} on {device}")
        try:
            encoded = reader["payload_by_disc_path"][disc_path]
        except KeyError as exc:
            raise RuntimeError(f"missing recovery fixture for {disc_path}") from exc
        yield base64.b64decode(encoded)
