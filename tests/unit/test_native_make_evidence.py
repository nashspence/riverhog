from __future__ import annotations

import json
import shlex
import subprocess
import sys
from pathlib import Path

import pytest
from riverhog_canonical_json import require_canonical_json

from scripts import ci_make, ci_qualification
from scripts.validation_numeric import qualification_numeric_policy
from scripts.validation_witnesses import collect_test_witnesses

ROOT = Path(__file__).resolve().parents[2]
NATIVE = "packages/retained-resource/tests/test_native_growth.py"
LEASE = "tests/unit/test_journal_validation_workspace_lease.py"
NODES = (
    NATIVE + "::test_creation_measures_actual_native_entries_and_full_control_operands",
    NATIVE + "::test_native_receiving_has_one_lifetime_dispatch_and_complete_control_work",
    LEASE + "::test_receiving_native_work_is_independent_of_reference_and_namespace_population",
)


@pytest.mark.skipif(sys.platform not in {"linux", "darwin"}, reason="POSIX Make and lease owner")
@pytest.mark.parametrize("defect", [None, "recorder", "observations", "source", "span", "numeric"])
def test_native_make_receives_actual_owner_evidence_and_refuses_invalid_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, defect: str | None
) -> None:
    work = ci_qualification.validation_work("client-platform-qualification")
    root = tmp_path / "source"
    root.mkdir()
    for relative in (NATIVE, LEASE):
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes((ROOT / relative).read_bytes())
    (root / ".gitignore").write_text("__pycache__/\n.pytest_cache/\n")
    (root / "pytest.ini").write_text(
        "[pytest]\nmarkers =\n    growth_witness(identity, gates): owned observations\n"
    )
    timing = tmp_path / "timings"
    commands = tmp_path / "commands.json"
    launcher = root / "native.py"
    launcher.write_text(
        "import json,subprocess,sys\n"
        "from pathlib import Path\n"
        "from scripts import ci_qualification as qualification\n"
        "from riverhog_canonical_json import canonical_json_bytes\n"
        f"root=Path({str(root)!r});timing=Path({str(timing)!r})\n"
        f"defect={defect!r};commands=[]\n"
        f"qualification.NATIVE_TESTS={NODES!r}\n"
        "def run(command):\n"
        "    commands.append(command)\n"
        "    if command[1:3]==['-m','pytest']:\n"
        "        if defect=='recorder':\n"
        "            command=command.copy();index=command.index('-p');del command[index:index+2]\n"
        "        subprocess.run(command,cwd=root,check=True)\n"
        "        for path in (timing/'pytest').glob('*/*/*/*.json'):\n"
        "            row=json.loads(path.read_bytes())\n"
        "            if defect=='observations':row['growth_observations'].pop("
        f"{NODES[0]!r})\n"
        "            if defect=='source':row['source_end']['source_sha']='0'*40\n"
        "            if defect=='span':row['execution_span']='unowned'\n"
        "            if defect=='numeric':row['numeric_policy']['max_work']='1'\n"
        "            path.write_bytes(canonical_json_bytes(row))\n"
        "qualification.run=run\n"
        "qualification.native_platform()\n"
        f"Path({str(commands)!r}).write_text(json.dumps(commands))\n"
    )
    makefile = root / "Makefile"
    makefile.write_text(
        "SHELL := bash\nVALIDATION_TARGETS := client-platform-qualification\n"
        "ifneq ($(RIVERHOG_VALIDATION_MAKE_DIRECTORY),)\n"
        "SHELL := $(RIVERHOG_VALIDATION_MAKE_SHELL)\n.SHELLFLAGS := -c\n"
        "export RIVERHOG_VALIDATION_RECIPE_TARGET = $@\nendif\n"
        "# Maintained target recipes.\n"
        "client-platform-qualification:\n\t" + shlex.join((sys.executable, str(launcher))) + "\n"
    )
    for command in (
        ["git", "init", "-q"],
        ["git", "add", "."],
        [
            "git",
            "-c",
            "user.name=Example",
            "-c",
            "user.email=example@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-qm",
            "Define Native Make witness fixture",
        ],
    ):
        subprocess.run(command, cwd=root, check=True, capture_output=True)
    source = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    for variable in (
        *ci_make.SELECTION_VARIABLES,
        "MAKEFLAGS",
        "MFLAGS",
        "MAKEOVERRIDES",
        "RIVERHOG_VALIDATION_MAKE_DIRECTORY",
        "RIVERHOG_VALIDATION_MAKE_REENTRY",
        "RIVERHOG_VALIDATION_PARENT_SPAN",
        "RIVERHOG_VALIDATION_ACTIVE_SPAN",
    ):
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.chdir(root)
    monkeypatch.setenv("PYTHONPATH", str(ROOT))
    monkeypatch.setenv("RIVERHOG_CI_TIMING_DIR", str(timing))
    monkeypatch.setenv("RIVERHOG_CI_RUN_ID", "actual-native-make")
    monkeypatch.setenv("RIVERHOG_CI_LANE", work.name)
    monkeypatch.setattr(ci_make, "ROOT", root)
    monkeypatch.setattr(ci_make, "make_work_definitions", lambda *_args: [work.definition()])
    status = ci_make.observe_make(makefile, (work.name,))
    observed = json.loads(commands.read_text())
    assert len(observed) == 2
    assert observed[0][1:7] == ["-m", "pytest", "-q", "--maxfail=1", "-p", "scripts.ci_timing"]
    assert observed[1][1] == "scripts/qualify_installation.py"
    assert observed[1][4:7] == ["--listener-lifecycle", "--listener-lifecycle-repetitions", "12"]
    manifest = require_canonical_json(
        next((timing / "make").glob("actual-native-make/*/manifest.json")).read_bytes()
    )
    assert manifest["source_sha"] == source and manifest["source_clean"] is True
    assert manifest["source_end"] == {"source_sha": source, "source_clean": True}
    assert len(manifest["recipes"]) == 1 and manifest["recipes"][0]["exit_status"] == 0
    if defect is not None:
        assert status != 0 and manifest["state"] == "evidence-failed"
        return
    assert status == 0 and manifest["state"] == "completed"
    assert manifest["growth_results"] == {
        work.name: {identity: "pass" for identity in work.growth_witnesses}
    }
    paths = tuple((timing / "pytest").glob("*/*/*/*.json"))
    assert len(paths) == 1
    record = require_canonical_json(paths[0].read_bytes())
    assert record["execution_span"] == manifest["span"]
    assert [record["growth_observations"][node]["operations"] for node in NODES] == [684, 1368, 228]
    assert collect_test_witnesses(
        paths,
        root=root,
        expected=(*work.growth_witnesses, "riverhog-server.live-lease-frame-receiving"),
        source_sha=source,
        run_id="actual-native-make",
        numeric_policy=qualification_numeric_policy(),
    ) == {
        identity: "pass"
        for identity in (*work.growth_witnesses, "riverhog-server.live-lease-frame-receiving")
    }
