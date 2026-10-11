#!/usr/bin/env python3
"""Verify the proposal's source references; does not test runtime complexity."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tomllib

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True, type=Path)
    args = parser.parse_args()
    here = Path(__file__).resolve().parent
    from load_inventory import load_inventory
    data = load_inventory(here)
    sha = subprocess.check_output(["git", "-C", str(args.repo), "rev-parse", "HEAD"], text=True).strip()
    if sha != data["audited_source"]["sha"]:
        raise SystemExit("Source revision differs; reconcile rather than pretending this is current evidence.")
    ids = [x["id"] for x in data["families"]]
    if len(ids) != len(set(ids)):
        raise SystemExit("Duplicate proposal IDs.")
    projects = {}
    for file in args.repo.rglob("pyproject.toml"):
        if ".git" in file.parts:
            continue
        value = tomllib.loads(file.read_text())
        if "project" in value:
            projects[value["project"]["name"]] = str(file.relative_to(args.repo))
    represented = {x["name"] for x in data["distribution_coverage"]}
    if represented != set(projects):
        raise SystemExit(f"Project coverage differs: {represented ^ set(projects)}")
    count = 0
    for family in data["families"]:
        if not set(family["proposed_owners"]) <= represented:
            raise SystemExit(f"Unknown owner in {family['id']}")
        for anchor in family["source_anchors"]:
            file = args.repo / anchor["path"]
            payload = file.read_bytes()
            if hashlib.sha256(payload).hexdigest() != anchor["source_sha256"]:
                raise SystemExit(f"Source changed: {anchor['path']}")
            token = anchor["symbol_or_anchor"]
            if token and token not in payload.decode():
                raise SystemExit(f"Anchor absent: {token}")
            count += 1
    print(json.dumps({"source_sha":sha, "families":len(ids),
                      "projects":len(projects), "anchors_checked":count,
                      "runtime_complexity_tested":False}, sort_keys=True))

if __name__ == "__main__":
    main()
