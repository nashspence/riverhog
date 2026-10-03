#!/usr/bin/env python3
"""Generate, validate and inspect contracts derived from this source revision."""

from __future__ import annotations

import argparse
import hashlib
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import cast

import contract_freeze
from contract_atlas.documentation import AuthoredDocumentation, validate_authoring_tree
from contract_atlas.generation import (
    DEFAULT_OUTPUT,
    ROOT,
    build_candidate,
    prepared_source,
    source_revision,
    verify_candidate,
)
from contract_atlas.model import ContractAtlasError, canonical_bytes
from contract_atlas.records import load_bundle
from contract_atlas.review import compare_revisions
from contract_atlas.review import summary as comparison_summary


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("generate", "check"):
        command = commands.add_parser(name)
        command.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
        command.add_argument("--source-sha")
        command.add_argument("--release")
        command.add_argument("--documentation-commit")
        command.add_argument("--replace", action="store_true")
        command.add_argument("--source-repository", type=Path)
        command.add_argument("--release-version")
        command.add_argument("--source-archive", type=Path)
    for name in ("summary", "list", "show"):
        command = commands.add_parser(name)
        command.add_argument("--candidate", type=Path, default=DEFAULT_OUTPUT)
        if name == "list":
            command.add_argument("--authority")
            command.add_argument("--interface")
            command.add_argument("--policy")
        if name == "show":
            command.add_argument("element_id")
    diff = commands.add_parser("diff")
    diff.add_argument("--base", required=True)
    diff.add_argument("--head", default="HEAD")
    diff.add_argument("--base-mode", choices=("merge-base", "tip"), default="merge-base")
    diff.add_argument("--output", type=Path, required=True)
    diff.add_argument("--summary-limit", type=int, default=40)
    authoring = commands.add_parser("validate-authoring")
    authoring.add_argument("--documentation-commit", required=True)
    authoring.add_argument("--source-sha", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "validate-authoring":
            source_revision(requested=args.source_sha)
            validate_authoring_tree(ROOT, args.documentation_commit, args.source_sha)
            print(
                canonical_bytes(
                    {
                        "status": "validated",
                        "source_sha": args.source_sha,
                        "documentation_commit": args.documentation_commit,
                    }
                ).decode()
            )
            return 0
        if args.command == "diff":
            report = compare_revisions(
                args.base, args.head, args.output, mode=args.base_mode, limit=args.summary_limit
            )
            print(comparison_summary(report, args.summary_limit), end="")
            return 0
        if args.command in {"summary", "list", "show"}:
            verify_candidate(args.candidate)
            bundle = load_bundle(args.candidate / "riverhog-v1.json")
            payload = (
                contract_freeze._summary(bundle)
                if args.command == "summary"
                else contract_freeze._listed_elements(bundle, args)
                if args.command == "list"
                else contract_freeze._shown_element(bundle, args.element_id)
            )
        else:
            if bool(args.release) != bool(args.documentation_commit):
                raise ContractAtlasError("release and documentation commit are required together")
            prepared = (args.source_repository, args.release_version, args.source_archive)
            if any(prepared) and (not all(prepared) or args.source_sha is None):
                raise ContractAtlasError(
                    "prepared source requires repository, version, archive and SHA"
                )
            preparation = (
                prepared_source(
                    args.source_repository,
                    args.source_sha,
                    args.release_version,
                    args.source_archive,
                )
                if all(prepared)
                else None
            )
            revision = (
                args.source_sha
                if preparation is not None
                else source_revision(requested=args.source_sha)
            )
            document = (
                AuthoredDocumentation.resolve(
                    args.source_repository or ROOT,
                    args.release,
                    args.documentation_commit,
                    source_sha=revision,
                )
                if args.release
                else None
            )
            candidate = build_candidate(
                revision=revision, documentation=document, preparation=preparation
            )
            candidate.write(args.output, replace=args.replace)
            if args.command == "check":
                # A fresh process uses this checkout's own interpreter and locked environment.
                with tempfile.TemporaryDirectory(prefix="riverhog-contract-check-") as temporary:
                    other = Path(temporary) / "candidate"
                    command = [
                        sys.executable,
                        str(Path(__file__).resolve()),
                        "generate",
                        "--output",
                        str(other),
                    ]
                    if revision is not None:
                        command.extend(("--source-sha", revision))
                    if preparation is not None:
                        command.extend(
                            (
                                "--source-repository",
                                str(args.source_repository),
                                "--release-version",
                                args.release_version,
                                "--source-archive",
                                str(args.source_archive),
                            )
                        )
                    if args.release:
                        command.extend(
                            (
                                "--release",
                                args.release,
                                "--documentation-commit",
                                args.documentation_commit,
                            )
                        )
                    subprocess.run(command, check=True, stdout=subprocess.DEVNULL)
                    repeated = verify_candidate(other, expected_source=revision)
                    if repeated != candidate.manifest:
                        raise ContractAtlasError(
                            "independent contract generations are not deterministic"
                        )
                verify_candidate(args.output, expected_source=revision)
            payload = {
                "status": "checked" if args.command == "check" else "generated",
                "source_sha": revision,
                "output": str(args.output),
                "closure_sha256": candidate.manifest["closure_sha256"],
                "audit_sha256": candidate.manifest["audit_sha256"],
                "build_manifest_sha256": hashlib.sha256(
                    canonical_bytes(candidate.manifest)
                ).hexdigest(),
                "documentation": candidate.manifest["documentation"],
                "contract_elements": len(cast(list[object], candidate.bundle.closure["elements"])),
                "files": len(candidate.files),
            }
        sys.stdout.buffer.write(canonical_bytes(payload) + b"\n")
        return 0
    except (
        ContractAtlasError,
        contract_freeze.ContractFreezeError,
        subprocess.CalledProcessError,
    ) as exc:
        print(f"contract candidate failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
