from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from tests.unit.test_operation_qualification import REPO_ROOT, load_script


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=root, check=True, capture_output=True, text=True
    ).stdout.strip()


@pytest.fixture
def source_checkout(tmp_path, monkeypatch):
    root = tmp_path / "checkout"
    root.mkdir()
    for relative in ("scripts/qualification_source.py", "tests/operation_observer.py"):
        destination = root / relative
        destination.parent.mkdir(exist_ok=True)
        shutil.copyfile(REPO_ROOT / relative, destination)
    (root / "tests/__init__.py").touch()
    (root / ".gitignore").write_text("__pycache__/\n.pytest_cache/\nbuild/\n")
    (root / "authority.py").write_text("VALUE = 1\n")
    (root / "test_source.py").write_text(
        "import os\n"
        "from pathlib import Path\n"
        "import authority\n"
        "def test_source():\n"
        "    assert authority.VALUE > 0\n"
        "    if os.getenv('SOURCE_TEST_ACTION') == 'restore':\n"
        "        assert authority.VALUE == 222\n"
        "        Path('authority.py').write_text('VALUE = 1\\n')\n"
        "    if os.getenv('SOURCE_TEST_ACTION') == 'edit':\n"
        "        Path('authority.py').write_text('VALUE = 222\\n')\n"
    )
    _git(root, "init", "-q")
    _git(root, "add", ".")
    _git(
        root,
        "-c",
        "user.name=Qualification fixture",
        "-c",
        "user.email=qualification@example.invalid",
        "commit",
        "-qm",
        "Committed fixture",
    )
    sha = _git(root, "rev-parse", "HEAD")
    # Ignored build outputs and Python/pytest caches must remain usable.
    (root / "build").mkdir()
    (root / "build/output.json").write_text("{}")
    module = load_script()
    monkeypatch.setattr(module.qualification_source, "REPO_ROOT", root)
    # Isolate source identity from the real operation/extent behavioral tests.
    monkeypatch.setattr(module, "operation_matrix", lambda: ())
    monkeypatch.setattr(module, "application_surfaces", lambda: ())
    monkeypatch.setattr(module, "_contract_freeze_identity", lambda: {})
    monkeypatch.setattr(module, "_cold_cli_timings", lambda: {})
    return root, sha, module


def _observe(root: Path, sha: str, *, action: str = "") -> Path:
    timings = root.parent / "timings.json"
    selected = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "tests.operation_observer", "."],
        cwd=root,
        env={
            **os.environ,
            "SOURCE_TEST_ACTION": action,
            "RIVERHOG_OPERATION_SOURCE_SHA": sha,
            "RIVERHOG_OPERATION_TIMINGS": str(timings),
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert selected.returncode == 0, selected.stdout + selected.stderr
    return timings


@pytest.mark.parametrize(
    "change", ["clean", "tracked", "staged", "untracked", "during", "restored"]
)
def test_observer_binds_the_executed_checkout_even_if_cleaned_before_report(
    source_checkout, change
):
    root, sha, module = source_checkout
    if change in {"tracked", "staged", "restored"}:
        (root / "authority.py").write_text("VALUE = 222\n")
    if change == "staged":
        _git(root, "add", "authority.py")
    added = root / "test_uncommitted.py"
    if change == "untracked":
        added.write_text("def test_new_assertion():\n    assert True\n")
    timings = _observe(root, sha, action={"during": "edit", "restored": "restore"}.get(change, ""))
    observed = json.loads(timings.read_text())
    assert observed["source_checkout"] == {
        "start": {"head": sha, "clean": change in {"clean", "during"}},
        "finish": {"head": sha, "clean": change in {"clean", "restored"}},
    }

    # End-only checks would incorrectly accept these passing development runs.
    _git(root, "restore", "--staged", "--worktree", ".")
    added.unlink(missing_ok=True)
    assert module._source_sha(sha) == sha
    if change == "clean":
        assert module.evidence(source_sha=sha, timings=timings)["source_sha"] == sha
    else:
        with pytest.raises(module.QualificationError, match="both test start and finish"):
            module.evidence(source_sha=sha, timings=timings)


@pytest.mark.parametrize("change", ["tracked", "untracked", "commit", "during_report"])
def test_producer_rechecks_source_after_observation_and_report_construction(
    source_checkout, monkeypatch, change
):
    root, sha, module = source_checkout
    timings = _observe(root, sha)

    def edit():
        (root / "authority.py").write_text("VALUE = 222\n")
        return ()

    if change == "during_report":
        monkeypatch.setattr(module, "operation_matrix", edit)
    elif change == "untracked":
        (root / "new_assertion.py").write_text("assert True\n")
    elif change == "commit":
        _git(
            root,
            "-c",
            "user.name=Qualification fixture",
            "-c",
            "user.email=qualification@example.invalid",
            "commit",
            "--allow-empty",
            "-qm",
            "Different source commit",
        )
        # Relabeling old observations with the new HEAD must also fail.
        with pytest.raises(module.QualificationError, match="identity or test result"):
            module.evidence(source_sha=_git(root, "rev-parse", "HEAD"), timings=timings)
    else:
        edit()
    with pytest.raises(module.QualificationError, match="clean checkout at the exact source SHA"):
        module.evidence(source_sha=sha, timings=timings)


@pytest.mark.parametrize("invalid", [None, {}, {"head": "0" * 40, "clean": True}, {"clean": 1}])
def test_producer_rejects_unverified_or_mismatched_observation_source(source_checkout, invalid):
    root, sha, module = source_checkout
    timings = _observe(root, sha)
    observed = json.loads(timings.read_text())
    observed["source_checkout"]["start"] = invalid
    timings.write_text(json.dumps(observed))
    with pytest.raises(module.QualificationError, match="both test start and finish"):
        module.evidence(source_sha=sha, timings=timings)
