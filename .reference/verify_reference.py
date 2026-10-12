"""Verify reference bytes and compare committed source seams; never run project code."""

import argparse
import hashlib
import json
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent


def git(repo: Path, *arguments: str) -> bytes:
    return subprocess.check_output(["git", "-C", str(repo), *arguments], stderr=subprocess.PIPE)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=HERE.parent)
    parser.add_argument("--ref", default="HEAD")
    args = parser.parse_args()
    try:
        manifest = json.loads((HERE / "manifest.json").read_text())
        actual = {p.name for p in HERE.iterdir() if p.is_file() and p.name != "manifest.json"}
        if actual != set(manifest["files"]):
            raise ValueError("reference file membership differs from manifest")
        for name, digest in manifest["files"].items():
            if Path(name).name != name or hashlib.sha256((HERE / name).read_bytes()).hexdigest() != digest:
                raise ValueError(f"reference integrity mismatch: {name}")
        commit = git(args.repo, "rev-parse", "--verify", "--end-of-options", args.ref + "^{commit}").decode().strip()
        seams = json.loads((HERE / "source-seams.json").read_text())
        changed = []
        for item in seams["files"]:
            try:
                content = git(args.repo, "show", commit + ":" + item["path"])
            except subprocess.CalledProcessError:
                changed.append({"path": item["path"], "change": "unavailable"})
                continue
            if hashlib.sha256(content).hexdigest() != item["sha256"]:
                changed.append({"path": item["path"], "change": "different"})
        print(json.dumps({
            "reference_integrity": "matched",
            "audited_base": seams["audited_base"],
            "compared_commit": commit,
            "source_seams": len(seams["files"]),
            "changed": changed,
            "interpretation": "Reference reconciliation only; not product qualification.",
        }, indent=2))
        return 2 if changed else 0
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as exc:
        parser.exit(1, f"reference verification failed: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
