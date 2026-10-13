"""Read-only reference integrity, committed-source reconciliation and patch checks.

Does not import Riverhog, collect pytest, execute project code, contact a network,
or establish integration readiness. Exit 1: invalid reference; exit 2: source drift.
"""

import argparse
import ast
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile

REFERENCE = Path(__file__).resolve().parent


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def git(root: Path, *arguments: str) -> bytes:
    return subprocess.check_output(["git", "-C", str(root), *arguments], stderr=subprocess.PIPE)


def check_patch(root: Path, revision: str) -> None:
    patch = REFERENCE / "onboarding.patch"
    paths = re.findall(r"^\+\+\+ b/(.+)$", patch.read_text(), flags=re.MULTILINE)
    if paths != ["AGENTS.md", "tests/unit/test_entrypoints.py"]:
        raise ValueError("guide patch must only touch the approved two files")
    before = git(root, "show", f"{revision}:AGENTS.md").decode()
    with tempfile.TemporaryDirectory(prefix="968-guide-check-") as temporary:
        stage = Path(temporary)
        git(stage, "init", "-q")
        for name in paths:
            target = stage / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(git(root, "show", f"{revision}:{name}"))
        git(stage, "apply", "--check", str(patch))
        git(stage, "apply", str(patch))
        after = (stage / "AGENTS.md").read_text()
        heading = "## Account for validation evidence\n"
        old_start, new_start = before.index(heading), after.index(heading)
        old_end = before.index("\n## ", old_start + len(heading))
        new_end = after.index("\n## ", new_start + len(heading))
        if before[:old_start] != after[:new_start] or before[old_end:] != after[new_end:]:
            raise ValueError("guide patch modifies unrelated guidance")
        if after[new_start:new_end].strip() != (REFERENCE / "AGENTS.section.md").read_text().strip():
            raise ValueError("guide section is not the approved reference wording")
        if len(after.split()) > 2100:
            raise ValueError("guide exceeds its retained length guard")
        tests = (stage / "tests/unit/test_entrypoints.py").read_text()
        if digest(after.encode()) not in tests:
            raise ValueError("guide integrity guard was not updated")
        ast.parse(tests)


def verify(root: Path, revision: str | None) -> int:
    manifest = json.loads((REFERENCE / "manifest.json").read_bytes())
    actual = {p.relative_to(REFERENCE).as_posix() for p in REFERENCE.rglob("*")
              if p.is_file() and "__pycache__" not in p.parts and p.name != "manifest.json"}
    expected = manifest["files"]
    if actual != set(expected):
        raise ValueError("reference file membership differs")
    for name, sha in expected.items():
        path = REFERENCE / name
        if path.is_symlink() or digest(path.read_bytes()) != sha:
            raise ValueError(f"reference integrity mismatch: {name}")
        if path.suffix == ".py":
            ast.parse(path.read_text(), filename=name)
        if path.suffix == ".md":
            for target in re.findall(r"\]\(([^)]+)\)", path.read_text()):
                if "://" in target or target.startswith("#"):
                    continue
                directory = root if name == "AGENTS.section.md" else path.parent
                if not (directory / target.split("#")[0]).exists():
                    raise ValueError(f"unresolved local link: {name}: {target}")
    source = json.loads((REFERENCE / "source-seams.json").read_bytes())
    selected = revision or source["audited_source"]
    resolved = git(root, "rev-parse", "--verify", selected + "^{commit}").decode().strip()
    changed = []
    for row in source["files"]:
        try:
            data = git(root, "show", f"{resolved}:{row['path']}")
            mode = git(root, "ls-tree", resolved, "--", row["path"]).split()[0].decode()
            if digest(data) != row["sha256"] or mode != row["mode"]:
                changed.append(row["path"])
        except subprocess.CalledProcessError:
            changed.append(row["path"])
    if changed:
        print(json.dumps({"source": resolved, "reconciliation_required": changed}, indent=2))
        return 2
    check_patch(root, resolved)
    print(json.dumps({"reference_files_verified": len(actual), "source_files_verified": len(source["files"]),
                      "source": resolved, "onboarding_patch": "applies; section, scope, digest and syntax checked",
                      "product_qualification": "not performed"}, indent=2))
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=REFERENCE.parent)
    parser.add_argument("--ref")
    args = parser.parse_args()
    try:
        raise SystemExit(verify(args.repo.resolve(), args.ref))
    except (OSError, ValueError, KeyError, TypeError, subprocess.CalledProcessError) as exc:
        parser.exit(1, f"reference verification failed: {exc}\n")
