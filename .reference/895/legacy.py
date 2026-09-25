"""Read selected current checked records; no runtime imports or rediscovery.

The default selection is a REVIEW FIXTURE, not a proposed production inventory.
Production integration must disposition all owned field families separately.
"""
from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
from typing import Any

from records import BoundaryError, at, build, canonical_json_bytes, digest, loads, parts, unpack


def select_default(element: dict[str, Any]) -> bool:
    pair = (element["authority"], element["interface"])
    return (
        pair == ("release", "compatibility-guarantees")
        or pair == ("riverhog-application-access", "python")
        or element["pointers"] == ["/external_contract/python/gogurt_core.GOGURT_ROUTE_PATTERN"]
        or element["pointers"] == ["/external_contract/http_openapi/riverhog/components/schemas/CollectionTag"]
        or element["pointers"] == ["/external_contract/extents/rules/schema-bound~1v1"]
    )


def from_legacy(root: dict[str, Any], revision: str, selected_ids: set[str] | None = None) -> dict[str, Any]:
    if root.get("format") != "riverhog-contract-machine-closure/v1":
        raise BoundaryError("this adapter expects the inspected current machine format")
    integer_paths = root.get("projection_unsafe_integer_paths", [])
    if len(integer_paths) != len(set(integer_paths)):
        raise BoundaryError("duplicate integer paths in original projection")
    projection = unpack({"value": root["projection"], "integer_paths": sorted(integer_paths)})
    elements = root["elements"]
    if len({e["id"] for e in elements}) != len(elements):
        raise BoundaryError("duplicate legacy element identity")
    chosen = [e for e in elements if e["id"] in selected_ids] if selected_ids is not None else [e for e in elements if select_default(e)]
    if selected_ids is not None and {e["id"] for e in chosen} != selected_ids:
        raise BoundaryError("requested element not found")
    sources = {s["id"]: s for s in root["sources"]}
    if len(sources) != len(root["sources"]):
        raise BoundaryError("duplicate legacy source identity")
    records = []
    for e in chosen:
        if len(e["pointers"]) != 1:
            raise BoundaryError("multi-pointer elements are outside this contained adapter")
        pointer = e["pointers"][0]
        try:
            provenance = [copy.deepcopy(sources[s]) for s in e["source_authority_ids"]]
        except KeyError as exc:
            raise BoundaryError("selected element has missing source identity") from exc
        records.append({"id": e["id"], "authority": e["authority"], "interface": e["interface"], "name": parts(pointer)[-1], "pointer": pointer, "value": copy.deepcopy(at(projection, pointer)), "sources": provenance})
    pointers = [r["pointer"] for r in records]
    decisions = [copy.deepcopy(d) for d in projection["external_contract"]["extents"]["decisions"] if any(d["source_pointer"] == p or d["source_pointer"].startswith(p + "/") for p in pointers)]
    decision_ids = {d["id"] for d in decisions}
    witnesses = {}
    for w in root["trace"].get("segmented_extent_witnesses", []):
        if w["id"] in witnesses:
            raise BoundaryError("duplicate legacy witness")
        witnesses[w["id"]] = w
    associations: dict[str, set[str]] = {}
    for link in root["trace"].get("extent_sources", []):
        if link["id"] in decision_ids:
            for identity in link.get("segmented_extent_witnesses", []):
                if identity not in witnesses:
                    raise BoundaryError("legacy witness link is unresolved")
                associations.setdefault(identity, set()).add(link["id"])
    return {"series": root["series"], "source_revision": revision, "records": records, "extent_analysis": decisions, "witnesses": [{"id": k, "analysis_ids": sorted(v), "record": copy.deepcopy(witnesses[k])} for k, v in sorted(associations.items())], "discovery_accounting": {"selection_only": True, "selected_elements": len(records), "legacy_element_count": len(elements)}}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--machine", type=Path)
    p.add_argument("--fixture", type=Path, default=Path(__file__).with_name("fixture.json"))
    p.add_argument("--source-revision")
    p.add_argument("--element-id", action="append", help="Exact selected legacy identity; repeat as needed")
    p.add_argument("--out", type=Path, required=True)
    args = p.parse_args()
    if args.machine:
        if not args.source_revision:
            p.error("--source-revision is required with --machine")
        source = from_legacy(loads(args.machine.read_bytes()), args.source_revision, set(args.element_id) if args.element_id else None)
    else:
        source = loads(args.fixture.read_bytes())["source"]
    closure, audit = build(source)
    from render import emit
    emit(closure, audit, args.out)
    print(json.dumps({"closure_sha256": digest(closure), "audit_sha256": digest(audit), "output": str(args.out), "scope": "non-authoritative selected reference slice"}, sort_keys=True))


if __name__ == "__main__":
    main()
