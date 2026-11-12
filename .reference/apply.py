"""Apply the reference's hash-pinned source/witness proposal in a scratch checkout."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

REFERENCE = Path(__file__).resolve().parent
ROOT = REFERENCE.parent


def replace_once(value: str, before: str, after: str) -> str:
    if value.count(before) != 1:
        raise RuntimeError("source anchor is not unique; reconcile instead of guessing")
    return value.replace(before, after, 1)


def edit(name: str, value: str) -> str:
    if name == "journal_retirement.py":
        value = replace_once(
            value,
            "def make(case, *, metadata_budget=1024 * 1024):",
            "def make(case, *, metadata_budget=1024 * 1024, terminal_workers=2):",
        )
        return replace_once(
            value, "owned.make(terminal_workers=2)", "owned.make(terminal_workers=terminal_workers)"
        )
    if name == "journal_retirement_registry.py":
        value = replace_once(
            value,
            "from riverhog_core.services.workspace_retirement_registry import "
            "WorkspaceRetirementRegistry",
            "from riverhog_core.services.workspace_retirement_registry import (\n"
            "    WorkspaceRetirementRegistry,\n    _Closed,\n    _Context,\n)",
        )
        return (
            value
            + "\n    def _run(self, context: _Context[ValidationJob, bool]) -> bool | _Closed:\n"
            + (
                '        """Native specialization: shared execution, '
                'exact Native callback graph."""\n'
                "        return super()._run(context)\n"
            )
        )
    if name != "upload_journal_validation_retirement.py":
        raise RuntimeError("unknown source edit")
    value = replace_once(
        value,
        "from .journal_retirement_registry import",
        "from .journal_retirement_growth import declare_async_retirement_growth\n"
        "from .journal_retirement_registry import",
    )
    value = replace_once(value, "from riverhog_core.growth_bounds import bound_envelope\n", "")
    start = value.index("def _retirement_loop_children()")
    end = value.index('_PHASES = ("directory", "retiring", "orphan")', start)
    value = value[:start] + value[end:]
    value = replace_once(
        value,
        '"close every catalog/kernel resource and return the progress "\n'
        '            "decision or propagate the real storage rejection"',
        '"return the unit decision or propagate rejection while the caller-retained "\n'
        '            "original turn still owns uncertain resources"',
    )
    value = replace_once(
        value,
        '"The outer scheduling loop retains its own selection, candidate, "\n'
        '                "failure-formatting and repeated-unit composition."',
        '"The original worker owns error formatting and receiving. The scheduler "\n'
        '                "has a distinct coordination scope '
        'and does not compose worker work inline."',
    )
    value = replace_once(
        value,
        '"""Schedule/receive bounded quanta without waiting for Native filesystem work."""',
        '"""Return coordination events, not completed file/catalog progress.\n\n'
        "        For positive limit L: at most L joined outcomes + L reservation\n"
        "        withdrawals + L close resubmissions + L new submissions = 4L.\n"
        "        Native effects and their positive receiving belong to original workers.\n"
        '        """',
    )
    return replace_once(
        value,
        "declare_growth(_retirement_loop_declaration(), delegates=_retirement_loop_children())(\n"
        "    UploadJournalValidationRetirement.advance\n)",
        "declare_async_retirement_growth(UploadJournalValidationRetirement)",
    )


def sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--verify", action="store_true", help="verify applied files without writing"
    )
    arguments = parser.parse_args()
    manifest = json.loads((REFERENCE / "PATCH.json").read_text())
    prepared = {}
    for name, item in manifest["files"].items():
        path = ROOT / name
        current = path.read_bytes() if path.exists() else None
        digest = None if current is None else sha(current)
        if digest == item["after_sha256"]:
            continue
        if arguments.verify or digest != item["before_sha256"]:
            raise RuntimeError(f"unexpected source: {name}; reconcile the exact checkout")
        if "replacement" in item:
            content = (REFERENCE / item["replacement"]).read_bytes()
        else:
            assert current is not None
            content = edit(path.name, current.decode()).encode()
        if sha(content) != item["after_sha256"]:
            raise RuntimeError(f"proposal hash mismatch: {name}")
        prepared[path] = content
    for path, content in prepared.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    print(
        json.dumps(
            {
                "base": manifest["base"],
                "verified_files": len(manifest["files"]),
                "written_files": len(prepared),
                "proposal_sha256": sha((REFERENCE / "PATCH.json").read_bytes()),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
