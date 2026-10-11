#!/usr/bin/env python3
"""Load the losslessly factored research inventory; this is not a product registry."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


def load_inventory(directory: Path | None = None) -> dict[str, Any]:
    root = (directory or Path(__file__).resolve().parent).resolve()
    metadata = json.loads((root / "metadata.json").read_text(encoding="utf-8"))
    representation = metadata.pop("publication_representation")
    if representation["format"] != "riverhog-growth-reference-parts/v1":
        raise ValueError("unsupported research representation")
    sources = json.loads((root / "sources.json").read_text(encoding="utf-8"))
    base = metadata["audited_source"]
    families = []
    for name in representation["parts"]:
        path = (root / name).resolve()
        if path.parent != root or path.suffix != ".json":
            raise ValueError("invalid inventory part path")
        for family in json.loads(path.read_text(encoding="utf-8")):
            if any(key in family for key in representation["family_defaults"]):
                raise ValueError("part overrides shared conformance qualification")
            family.update(representation["family_defaults"])
            for anchor in family["source_anchors"]:
                anchor["source_sha256"] = sources[anchor["path"]]
                anchor["url"] = (
                    f"https://github.com/{base['repository']}/blob/{base['sha']}/"
                    f"{anchor['path']}#L{anchor['line']}"
                )
            families.append(family)
    projects = json.loads((root / "distributions.json").read_text(encoding="utf-8"))
    for project in projects:
        project["directory"] = str(Path(project["pyproject"]).parent)
        project["public_closure_binding_status"] = representation[
            "distribution_default_binding_status"
        ]
    metadata["families"] = families
    metadata["distribution_coverage"] = projects
    encoded = json.dumps(metadata, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    if hashlib.sha256(encoded.encode()).hexdigest() != representation[
        "reviewed_inventory_canonical_sha256"
    ]:
        raise ValueError("published inventory differs from the reviewed proposal")
    return metadata


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="write the complete reviewed inventory JSON")
    args = parser.parse_args()
    inventory = load_inventory()
    if args.output:
        with args.output.open("x", encoding="utf-8") as stream:
            json.dump(inventory, stream, indent=2, ensure_ascii=False)
            stream.write("\n")
    else:
        print(json.dumps({
            "families": len(inventory["families"]),
            "projects": len(inventory["distribution_coverage"]),
            "proposal_restored_exactly": True,
            "runtime_growth_qualified": False,
        }, sort_keys=True))


if __name__ == "__main__":
    main()
