#!/usr/bin/env python3
"""Produce and verify trusted workflow evidence; coordinate source-check readiness."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from contract_atlas.github_publication import GitHubPublication
from contract_atlas.model import ContractAtlasError, canonical_bytes
from contract_atlas.workflow_evidence import (
    WORKFLOWS,
    collect_workflow_artifact,
    execution_record,
    source_workflow_checks,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repository", default=os.environ.get("GITHUB_REPOSITORY", "nashspence/riverhog")
    )
    sub = parser.add_subparsers(dest="command", required=True)
    checks = sub.add_parser(
        "checks", help="Select exact-source trusted push runs and their latest attempts."
    )
    checks.add_argument("--source-sha", required=True)
    checks.add_argument("--branch", required=True, choices=("main", "release/v1"))
    checks.add_argument(
        "--workflow", action="append", required=True, choices=("ci.yml", "codeql.yml")
    )
    checks.add_argument("--allow-pending", action="store_true")
    checks.add_argument("--github-output", type=Path)
    for name in ("record", "download"):
        child = sub.add_parser(name)
        child.add_argument("--kind", required=True, choices=tuple(WORKFLOWS))
        child.add_argument("--source-sha", required=True)
        child.add_argument("--version", required=True)
        child.add_argument("--output", type=Path, required=True)
        if name == "download":
            child.add_argument("--run-id", type=int, required=True)
    args = parser.parse_args()
    try:
        remote = GitHubPublication(args.repository)
        if args.command == "record":
            record = execution_record(args.kind, args.source_sha, args.version)
            args.output.write_bytes(canonical_bytes(record))
        elif args.command == "download":
            collect_workflow_artifact(
                remote, args.run_id, args.kind, args.source_sha, args.version, args.output
            )
        else:
            ready = source_workflow_checks(
                remote, args.source_sha, args.branch, tuple(args.workflow)
            )
            if args.github_output is not None:
                with args.github_output.open("a", encoding="utf-8") as output:
                    output.write("ready=" + str(ready).lower() + "\n")
            if not ready and not args.allow_pending:
                raise ContractAtlasError(
                    "selected exact-source workflow attempts are not successful"
                )
    except (ContractAtlasError, OSError, KeyError, ValueError) as exc:
        parser.exit(2, f"workflow evidence failed: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
