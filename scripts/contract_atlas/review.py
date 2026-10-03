"""Value-aware comparisons between independently generated exact source revisions."""

from __future__ import annotations

import html
import json
import os
import subprocess
import tempfile
from collections import Counter
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .generation import BUILD_FILENAME, BUILD_FORMAT, ROOT
from .model import ContractAtlasError, canonical_bytes, canonical_sha256, pointer_value
from .records import AUDIT_FILENAME, AUDIT_FORMAT, CLOSURE_FORMAT


def _elements(closure: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    if closure.get("format") != CLOSURE_FORMAT or not isinstance(closure.get("elements"), list):
        raise ContractAtlasError("unsupported contract comparison format")
    result: dict[str, dict[str, Any]] = {}
    for element in closure["elements"]:
        if (
            not isinstance(element, dict)
            or not isinstance(element.get("id"), str)
            or not element["id"]
            or element["id"] in result
            or not isinstance(element.get("pointers"), list)
            or not element["pointers"]
        ):
            raise ContractAtlasError("contract comparison has malformed or duplicate elements")
        try:
            for pointer in element["pointers"]:
                pointer_value(closure, pointer)
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            raise ContractAtlasError("contract comparison has an unresolved owned pointer") from exc
        result[element["id"]] = element
    return result


def _changes(
    before: Mapping[str, Any], after: Mapping[str, Any], prefix: str = ""
) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    for key in sorted(set(before) | set(after)):
        pointer = prefix + "/" + key.replace("~", "~0").replace("/", "~1")
        if key not in before:
            changes.append({"pointer": pointer, "change": "added", "after": after[key]})
        elif key not in after:
            changes.append({"pointer": pointer, "change": "removed", "before": before[key]})
        elif isinstance(before[key], dict) and isinstance(after[key], dict):
            changes.extend(_changes(before[key], after[key], pointer))
        elif canonical_bytes(before[key]) != canonical_bytes(after[key]):
            changes.append(
                {
                    "pointer": pointer,
                    "change": "changed",
                    "before": before[key],
                    "after": after[key],
                }
            )
    return changes


def compare_records(
    base: Mapping[str, Any],
    head: Mapping[str, Any],
    base_audit: Mapping[str, Any],
    head_audit: Mapping[str, Any],
    *,
    base_sha: str,
    head_sha: str,
    base_mode: str = "tip",
) -> dict[str, Any]:
    """Report metadata, owned values, global values and separately bound audit changes."""

    import contract_freeze

    old, new = _elements(base), _elements(head)
    if base.get("series") != head.get("series") or any(
        record.get("format") != AUDIT_FORMAT for record in (base_audit, head_audit)
    ):
        raise ContractAtlasError("unsupported comparison series or audit format")
    for closure, audit in ((base, base_audit), (head, head_audit)):
        if audit.get("closure_sha256") != canonical_sha256(closure):
            raise ContractAtlasError("comparison Audit Record is not bound to its Closure")
    elements: list[dict[str, Any]] = []
    for identity in sorted(set(old) | set(new)):
        a, b = old.get(identity), new.get(identity)
        before = (
            {"element": a, "values": {p: pointer_value(base, p) for p in a["pointers"]}}
            if a
            else None
        )
        after = (
            {"element": b, "values": {p: pointer_value(head, p) for p in b["pointers"]}}
            if b
            else None
        )
        if canonical_bytes(before) == canonical_bytes(after):
            continue
        subject = b or a
        assert subject is not None
        elements.append(
            {
                "id": identity,
                "authority": subject["authority"],
                "interface": subject["interface"],
                "change": "added" if a is None else "removed" if b is None else "changed",
                "before": before,
                "after": after,
            }
        )
    global_changes = _changes(
        {k: v for k, v in base.items() if k != "elements"},
        {k: v for k, v in head.items() if k != "elements"},
    )
    pointers = {p for e in (*old.values(), *new.values()) for p in e["pointers"]}
    for change in global_changes:
        change["unattributed"] = not any(
            change["pointer"] == p or change["pointer"].startswith(p + "/") for p in pointers
        )
    extents = contract_freeze._extent_diff(
        {
            "external_contract": {
                "extents": {"decisions": base_audit["extent_analysis"]["decisions"]}
            }
        },
        {
            "external_contract": {
                "extents": {"decisions": head_audit["extent_analysis"]["decisions"]}
            }
        },
    )
    return {
        "format": "riverhog-contract-comparison/v1",
        "base_sha": base_sha,
        "head_sha": head_sha,
        "base_mode": base_mode,
        "base_closure_sha256": canonical_sha256(base),
        "head_closure_sha256": canonical_sha256(head),
        "base_audit_sha256": canonical_sha256(base_audit),
        "head_audit_sha256": canonical_sha256(head_audit),
        "contract_changed": bool(elements or global_changes),
        "audit_changed": canonical_bytes(base_audit) != canonical_bytes(head_audit),
        "counts": {
            kind: sum(e["change"] == kind for e in elements)
            for kind in ("added", "changed", "removed")
        },
        "by_authority": dict(sorted(Counter(e["authority"] for e in elements).items())),
        "elements": elements,
        "global_changes": global_changes,
        "audit_changes": _changes(base_audit, head_audit),
        "extent_changes_by_authority": extents,
    }


def summary(report: Mapping[str, Any], limit: int = 40) -> str:
    if limit < 1:
        raise ContractAtlasError("comparison summary limit must be positive")

    def label(value: object) -> str:
        return html.escape(str(value)[:160]).replace("`", "&#96;").replace("|", "&#124;")

    lines = [
        "# Contract comparison",
        "",
        f"Base ({report['base_mode']}): `{report['base_sha']}`",
        f"Head: `{report['head_sha']}`",
        "",
        f"Contract changes: {report['counts']}; audit changed: {report['audit_changed']}.",
        f"Global changes: {len(report['global_changes'])}; unattributed: "
        f"{sum(c['unattributed'] for c in report['global_changes'])}.",
        "",
    ]
    for authority, count in list(report["by_authority"].items())[:20]:
        lines.append(f"- {label(authority)}: {count} changed elements")
    lines.append("")
    for change in report["elements"][:limit]:
        lines.append(f"- {change['change']}: {label(change['id'])}")
    lines += [
        "",
        f"Showing {min(limit, len(report['elements']))} of {len(report['elements'])} "
        "element changes.",
        "Complete element, value, global, audit and extent changes are in diff.json.",
        "This comparison does not establish compatibility or behavioral qualification.",
    ]
    return "\n".join(lines) + "\n"


def _git(*args: str) -> str:
    return subprocess.check_output(["git", "-C", str(ROOT), *args], text=True).strip()


def compare_revisions(
    base: str, head: str, output: Path, *, mode: str = "merge-base", limit: int = 40
) -> dict[str, Any]:
    """Run each revision's generator and locked environment in separate detached worktrees."""

    base_tip = _git("rev-parse", "--verify", "--end-of-options", base + "^{commit}")
    head_sha = _git("rev-parse", "--verify", "--end-of-options", head + "^{commit}")
    base_sha = _git("merge-base", base_tip, head_sha) if mode == "merge-base" else base_tip
    records: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]] = []
    if output.exists() and any(output.iterdir()):
        raise ContractAtlasError("comparison output must be empty")
    with tempfile.TemporaryDirectory(prefix="riverhog-contract-review-") as temporary:
        for label, revision in (("base", base_sha), ("head", head_sha)):
            checkout = Path(temporary) / label
            candidate = Path(temporary) / f"{label}-candidate"
            _git("worktree", "add", "--detach", str(checkout), revision)
            try:
                if not (checkout / "scripts/contract_candidate.py").is_file():
                    raise ContractAtlasError(f"unsupported source generator at {label} {revision}")
                environment = {
                    **os.environ,
                    "MISE_TRUSTED_CONFIG_PATHS": str(checkout),
                    "UV_PROJECT_ENVIRONMENT": str(checkout / ".venv"),
                }
                subprocess.run(
                    ["mise", "install", "--locked"], cwd=checkout, env=environment, check=True
                )
                subprocess.run(
                    [
                        "mise",
                        "x",
                        "--",
                        "uv",
                        "run",
                        "--locked",
                        "--all-packages",
                        "--group",
                        "dev",
                        "python",
                        "scripts/contract_candidate.py",
                        "generate",
                        "--source-sha",
                        revision,
                        "--output",
                        str(candidate),
                    ],
                    cwd=checkout,
                    env=environment,
                    check=True,
                    stdout=subprocess.DEVNULL,
                )
                build = json.loads((candidate / BUILD_FILENAME).read_bytes())
                if build.get("format") != BUILD_FORMAT or build.get("source_sha") != revision:
                    raise ContractAtlasError(
                        "revision generator produced mismatched source provenance"
                    )
                pair = []
                for name in ("riverhog-v1.json", AUDIT_FILENAME):
                    payload = (candidate / name).read_bytes()
                    import hashlib

                    if hashlib.sha256(payload).hexdigest() != build["files"][name]:
                        raise ContractAtlasError(
                            "revision generator produced mismatched record bytes"
                        )
                    value = json.loads(payload)
                    if payload != canonical_bytes(value):
                        raise ContractAtlasError("revision generator produced noncanonical records")
                    pair.append(value)
                records.append((pair[0], pair[1], build))
            finally:
                _git("worktree", "remove", "--force", str(checkout))
    report = compare_records(
        records[0][0],
        records[1][0],
        records[0][1],
        records[1][1],
        base_sha=base_sha,
        head_sha=head_sha,
        base_mode=mode,
    )
    report["requested_base_sha"] = base_tip
    report["generators"] = {"base": records[0][2], "head": records[1][2]}
    output.mkdir(parents=True, exist_ok=True)
    (output / "diff.json").write_bytes(canonical_bytes(report))
    (output / "summary.md").write_text(summary(report, limit), encoding="utf-8")
    return report
