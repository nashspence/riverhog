#!/usr/bin/env python3
"""Inspect a captured fixture without importing or executing Riverhog code."""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import lzma
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixture", type=Path)
    args = parser.parse_args()
    value = json.loads(args.fixture.read_bytes())
    if "xz_base64" in value:
        raw = lzma.decompress(base64.b64decode(value["xz_base64"], validate=True))
        if hashlib.sha256(raw).hexdigest() != value["decoded_sha256"]:
            parser.error("captured fixture digest mismatch")
        value = json.loads(raw)
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
