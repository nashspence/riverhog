# SPDX-License-Identifier: Apache-2.0
"""Check reference frames against audited core schemas; optionally the real runtime.

Schema validation is not semantic/runtime or product qualification. The optional
--full-core mode must run in the repository's locked environment. No substitutes
are installed for missing project dependencies.
"""
from __future__ import annotations

import argparse
import importlib.metadata
import json
import sys
import tomllib
from pathlib import Path

from jsonschema import Draft202012Validator
from referencing import Registry
from referencing.jsonschema import DRAFT202012

from annotation_reference import (
    CORE, PROFILE, SCHEMA, ArchiveFixture, Copy, compose, parse_frame, profile_reference,
)

ROOT = Path(__file__).resolve().parents[1]
SCHEMAS = ROOT / "packages/riverhog-provenance-contracts/src/riverhog_provenance_contracts/schemas"


def example():
    fixture = ArchiveFixture.from_repository(ROOT)
    source = Copy(fixture.root)
    source.mutate("initial", "initial", initial_tags=("unreviewed",))
    source.mutate("add", "tag_add", "reviewed")
    source.mutate("remove", "tag_remove", "unreviewed")
    source.mutate("replace", "description_set", "Family — été")
    source.mutate("revise", "description_set", "Corrected collection description")
    source.mutate("clear", "description_clear")
    source.mutate("readd", "tag_add", "unreviewed")
    package = compose(source, fixture, fixture.member_id, checkpoint=source.head)
    return source, fixture, package


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--full-core", action="store_true")
    args = parser.parse_args()
    lock = tomllib.loads((ROOT / "uv.lock").read_text())
    expected = {p["name"]: p["version"] for p in lock["package"]}
    checked = ("jsonschema", "referencing", "rpds-py", "attrs", "jsonschema-specifications")
    for name in checked:
        actual = importlib.metadata.version(name)
        if actual != expected[name]:
            raise RuntimeError(f"{name}: need locked {expected[name]}, found {actual}")
    documents = [json.loads(p.read_bytes()) for p in sorted(SCHEMAS.glob("*.json"))]
    pack = json.loads(Path(__file__).with_name("profile.schema.json").read_bytes())["schemas"]
    registry = Registry().with_resources((s["$id"], DRAFT202012.create_resource(s))
                                         for s in documents + pack)
    indexed = {s["$id"]: s for s in documents + pack}
    validator = Draft202012Validator(indexed[CORE + "/journal-entry.schema.json"], registry=registry)
    source, fixture, package = example()
    frames = source.recover().frames + [fixture.original_journal, package["objects"][package["bridge"]]]
    profile_count = 0
    for raw in frames:
        frame = parse_frame(raw)
        validator.validate(frame)
        groups = frame["body"]["assertions"]
        for row in groups.get("contexts", []) + groups.get("activities", []):
            for item in row.get("profiles", []) + row.get("details", []):
                ref = item["profile"]
                if ref["contract_id"] == PROFILE:
                    assert ref == profile_reference(ref["schema_id"])
                    Draft202012Validator(indexed[ref["schema_id"]], registry=registry).validate(item["data"])
                    profile_count += 1
    result = {"reference_only": True, "core_schema_frames": len(frames),
              "profile_values": profile_count, "locked_schema_dependencies": list(checked),
              "full_core_runtime": "not_requested", "product_qualification": "not_performed"}
    if args.full_core:
        for package_dir in (ROOT / "packages").glob("*/src"):
            sys.path.insert(0, str(package_dir))
        # Import failure is an explicit blocked check, never a simulated pass.
        from riverhog_provenance import validate_journal_set
        from riverhog_provenance_contracts import ContractCatalog, ProvenanceContractBinding
        binding = ProvenanceContractBinding(contract_id=PROFILE, schemas=pack)
        assert binding.contract_sha256 == profile_reference(SCHEMA)["contract_sha256"]
        validated = validate_journal_set(
            [source.recover().journal, fixture.original_journal, package["objects"][package["bridge"]]],
            catalog=ContractCatalog([binding]), require_profiles=True,
        )
        assert not validated.unresolved_references
        result["full_core_runtime"] = "passed"
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
