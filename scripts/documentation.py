#!/usr/bin/env python3
"""Plan, preview and check through one release Markdown compiler and audit."""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path
from typing import Any, cast

import yaml
from contract_atlas.documentation import AuthoredDocumentation
from contract_atlas.documentation_audit import (
    build_audit,
    build_snapshot,
    check_record,
    terminal_summary,
)
from contract_atlas.documentation_baseline import resolve as resolve_baseline
from contract_atlas.documentation_markdown import SOURCE, compile_corpus, front_matter
from contract_atlas.documentation_native import expected_outputs, native_slots
from contract_atlas.documentation_requirements import build_requirements
from contract_atlas.generation import (
    ROOT,
    build_candidate,
    documentation_compiler,
    verify_candidate,
)
from contract_atlas.model import ContractAtlasError, canonical_bytes


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workflow", choices=("plan", "preview", "check"))
    parser.add_argument("--release", default="v1.0.0")
    parser.add_argument("--documentation-commit")
    parser.add_argument("--output", type=Path, default=ROOT / "build/documentation")
    parser.add_argument(
        "--candidate", type=Path, help="Exact prepared candidate to check without rebuilding."
    )
    parser.add_argument(
        "--baseline", type=Path, help="Coordinator-selected verified prior prepared candidate."
    )
    parser.add_argument("--initial-review", action="store_true")
    parser.add_argument("--coordinate-baseline", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument(
        "--starter", type=Path, help="Explicit non-overwriting incomplete front matter."
    )
    parser.add_argument(
        "--element-id", help="Focus the readable plan and optional starter on one exact element."
    )
    return parser


def run(args: argparse.Namespace) -> int:
    baseline, custody = resolve_baseline(args.baseline, initial=args.initial_review)
    if args.candidate:
        if args.workflow != "check" or args.documentation_commit or args.starter:
            raise ContractAtlasError("prepared checking does not recompile or author prose")
        verify_candidate(args.candidate)
        record = json.loads((args.candidate / "documentation-audit.json").read_bytes())
        if (
            record["baseline_custody"] != custody
            or (record["comparison"] == "initial-review") != args.initial_review
        ):
            raise ContractAtlasError("prepared candidate uses another selected baseline")
        print(terminal_summary(record), end="")
        return check_record(record, prepared=True)
    candidate = build_candidate()
    closure = cast(dict[str, Any], candidate.bundle.closure)
    source_sha = cast(str | None, candidate.manifest["source_sha"])
    requirements = build_requirements(closure)
    authored = (
        AuthoredDocumentation.resolve(ROOT, args.release, args.documentation_commit)
        if args.documentation_commit
        else None
    )
    files = {} if authored is None else authored.files
    from contract_atlas.html_rendering import _element_file, render_contract, validate_render

    compiled = compile_corpus(
        files, requirements, routes={e["id"]: _element_file(e["id"]) for e in closure["elements"]}
    )
    snapshot = build_snapshot(
        closure,
        compiled,
        requirements,
        identity={
            "source_sha": candidate.manifest["source_sha"],
            "documentation_commit": None if authored is None else authored.commit,
            "tag": args.release,
            "compiler": documentation_compiler(),
        },
        expected=expected_outputs(native_slots(compiled, requirements, args.release)),
    )
    record = build_audit(
        snapshot,
        stage="preview",
        baseline=baseline,
        baseline_custody=custody,
        initial=args.initial_review,
    )
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "documentation-audit.json").write_bytes(canonical_bytes(record))
    (args.output / "documentation-requirements.json").write_bytes(canonical_bytes(requirements))
    missing = {canonical_bytes(target) for target in compiled["coverage"]["missing"]}
    plan_lines = [
        "# Missing release documentation",
        "",
        "Exact targets come from the selected source; empty summaries do not cover them.",
        "",
    ]
    grouped: dict[tuple[str, str], list[Any]] = {}
    for row in requirements["subjects"].values():
        if canonical_bytes(row["target"]) in missing and (
            args.element_id is None or row["target"]["element_id"] == args.element_id
        ):
            grouped.setdefault((row["authority"], row["interface"]), []).append(row)
    for (owner, interface), rows in sorted(grouped.items()):
        plan_lines.extend([f"## {owner}: {interface}", ""])
        for row in rows:
            plan_lines.extend(
                [
                    "### " + row["title"],
                    "",
                    "```json",
                    canonical_bytes(row["target"]).decode(),
                    "```",
                    "",
                ]
            )
    (args.output / "documentation-plan.md").write_text("\n".join(plan_lines))
    if args.starter:
        missing = {canonical_bytes(target) for target in compiled["coverage"]["missing"]}
        rows = [
            row
            for row in requirements["subjects"].values()
            if row["rule"] == "authored"
            and canonical_bytes(row["target"]) in missing
            and (args.element_id is None or row["target"]["element_id"] == args.element_id)
        ]
        if not rows:
            raise ContractAtlasError("starter selection has no missing native subjects")
        metadata = {
            "format": SOURCE,
            "id": "incomplete-reference",
            "kind": "reference",
            "title": "Incomplete reference",
            "subjects": [{"target": row["target"], "summary": ""} for row in rows],
        }

        class Strings(yaml.SafeDumper):
            def ignore_aliases(self, data: Any) -> bool:
                return True

        starter = "---\n" + yaml.dump(metadata, Dumper=Strings, sort_keys=False) + "---\n"
        front_matter(starter.encode())
        args.starter.parent.mkdir(parents=True, exist_ok=True)
        with args.starter.open("x") as stream:
            stream.write(starter)
    if args.workflow == "preview":
        document = None
        if authored:
            import contract_freeze

            document, _ = authored.bind(
                closure,
                candidate.bundle.audit,
                contract_freeze._cli_parsers(),
                source_sha,
                preview=True,
            )
        rendered = render_contract(
            closure,
            candidate.bundle.audit,
            document,
            source_revision=source_sha,
            documentation_audit=record,
            documentation_assets={
                name: raw for name, raw in files.items() if name.endswith(".png")
            },
        )
        rendered.update(
            {
                "riverhog-v1/documentation-assets/" + name: raw
                for name, raw in files.items()
                if name.endswith(".png")
            }
        )
        validate_render(rendered)
        for name, raw in rendered.items():
            path = args.output / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
        for name, value in (
            ("riverhog-v1.json", closure),
            ("riverhog-v1-audit.json", candidate.bundle.audit),
        ):
            (args.output / name).write_bytes(canonical_bytes(value))
        if document:
            assert authored is not None
            (args.output / "documentation-record.json").write_bytes(canonical_bytes(document))
            (args.output / "documentation-source.json").write_bytes(authored.payload)
    print(terminal_summary(record), end="")
    print(f"Complete readable missing-subject plan: {args.output / 'documentation-plan.md'}")
    return check_record(record) if args.workflow == "check" else 0


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        if args.coordinate_baseline:
            if args.baseline or args.initial_review:
                raise ContractAtlasError(
                    "coordinator selects the baseline from authenticated publication"
                )
            from contract_atlas.documentation_baseline import capture_published
            from contract_atlas.github_publication import GitHubPublication

            remote = GitHubPublication("nashspence/riverhog")
            products = remote.snapshot()["products"]
            with tempfile.TemporaryDirectory(
                prefix="riverhog-documentation-baseline-"
            ) as temporary:
                args.baseline = (
                    capture_published(remote, products[0], Path(temporary)) if products else None
                )
                args.initial_review = not products
                return run(args)
        return run(args)
    except (ContractAtlasError, OSError, ValueError) as exc:
        print(f"Documentation FAIL: {exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
