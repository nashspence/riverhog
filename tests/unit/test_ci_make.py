from __future__ import annotations

import copy
import json
import os
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import require_canonical_json

from scripts import ci_make
from scripts.ci_qualification import require_validation_work

ROOT = Path(__file__).resolve().parents[2]


def write_makefile(root: Path, *, failure: bool = False) -> Path:
    makefile = root / "Makefile"
    makefile.write_text(
        "SHELL := bash\n"
        "VALIDATION_TARGETS := one two\n"
        "ifneq ($(RIVERHOG_VALIDATION_MAKE_DIRECTORY),)\n"
        "SHELL := $(RIVERHOG_VALIDATION_MAKE_SHELL)\n"
        ".SHELLFLAGS := -c\n"
        "export RIVERHOG_VALIDATION_RECIPE_TARGET = $@\n"
        "endif\n"
        "# Maintained target recipes.\n"
        ".PHONY: one two shared\n"
        "one: shared\n"
        "\t@printf 'one\\n' >> executed\n"
        + ("\t@exit 7\n" if failure else "\t@sleep 0.1\n")
        + "two: shared\n"
        "\t@printf 'two\\n' >> executed\n"
        "\t@sleep 0.1\n"
        "shared:\n"
        "\t@printf 'shared\\n' >> executed\n"
    )
    return makefile


@pytest.fixture(scope="module")
def actual_make_receipt(tmp_path_factory: pytest.TempPathFactory):
    root = tmp_path_factory.mktemp("make-observation")
    makefile = write_makefile(root)
    timing = root / "timings"
    ticks = []
    monotonic = time.monotonic

    def clock():
        value = monotonic()
        ticks.append(value)
        return value

    with pytest.MonkeyPatch.context() as patch:
        patch.chdir(root)
        patch.setenv("RIVERHOG_CI_TIMING_DIR", str(timing))
        patch.setenv("RIVERHOG_CI_RUN_ID", "actual-make")
        patch.setenv("MAKEFLAGS", "-j2")
        patch.setattr(ci_make, "time", SimpleNamespace(monotonic=clock))
        assert ci_make.observe_make(makefile, ("one", "two")) == 0
    paths = tuple(timing.glob("make/actual-make/*/manifest.json"))
    assert len(paths) == 1
    record = require_canonical_json(paths[0].read_bytes())
    return root, makefile, record, ticks


def validate(makefile, record, *, require_qualification=False):
    return ci_make.validate_make_receipt(
        record,
        makefile=makefile,
        goals=("one", "two"),
        parameters={},
        source={"source_sha": record["source_sha"], "source_clean": record["source_clean"]},
        run_id="actual-make",
        require_qualification=require_qualification,
    )


def test_parallel_make_observation_records_real_work_once_and_actual_wall_time(actual_make_receipt):
    root, makefile, record, ticks = actual_make_receipt
    assert sorted((root / "executed").read_text().splitlines()) == ["one", "shared", "two"]
    assert record["expected_recipes"] == {"shared": 1, "one": 2, "two": 2}
    assert len(record["recipes"]) == 5
    assert len(ticks) == 4
    assert record["elapsed_seconds"] == ticks[3] - ticks[0]
    assert record["phase_costs"]["command-including-owner-cleanup"] == ticks[2] - ticks[1]
    assert validate(makefile, record)["qualification_exit_status"] == 0
    assert validate(makefile, record)["qualification_eligible"] is False
    with pytest.raises(ValueError, match="iteration"):
        validate(makefile, record, require_qualification=True)


@pytest.mark.parametrize(
    "defect",
    [
        "missing",
        "duplicate",
        "stale-run",
        "stale-span",
        "unexpected",
        "incomplete",
        "malformed-time",
        "extra-work",
        "wrong-source",
        "phase-cost",
        "parameter",
    ],
)
def test_required_recipe_and_measurement_defects_are_rejected(actual_make_receipt, defect):
    _, makefile, original, _ = actual_make_receipt
    record = copy.deepcopy(original)
    recipes = record["recipes"]
    if defect == "missing":
        recipes.pop()
    elif defect == "duplicate":
        recipes.append(copy.deepcopy(recipes[0]))
    elif defect == "stale-run":
        recipes[0]["run_id"] = "another-attempt"
    elif defect == "stale-span":
        recipes[0]["span"] = "another-execution"
    elif defect == "unexpected":
        recipes[0]["target"] = "unexecuted"
    elif defect == "incomplete":
        recipes[0]["state"] = "running"
    elif defect == "malformed-time":
        recipes[0]["elapsed_seconds"] = True
    elif defect == "extra-work":
        record["expected_recipes"]["unexecuted"] = 1
    elif defect == "wrong-source":
        record["source_end"]["source_sha"] = "a" * 40
    elif defect == "phase-cost":
        record["phase_costs"]["preparation"] = 1e9
    else:
        record["parameters"] = {"UNIT_SHARD": "unit"}
    with pytest.raises(ValueError):
        validate(makefile, record)


def test_recipe_failure_remains_failure_even_with_complete_observation(actual_make_receipt):
    _, makefile, original, _ = actual_make_receipt
    record = copy.deepcopy(original)
    record["recipes"][0]["exit_status"] = 9
    assert validate(makefile, record)["qualification_exit_status"] == 1


def test_make_failure_preserves_original_result_and_unstarted_work(tmp_path, monkeypatch):
    makefile = write_makefile(tmp_path, failure=True)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("RIVERHOG_CI_TIMING_DIR", str(tmp_path / "timings"))
    monkeypatch.setenv("RIVERHOG_CI_RUN_ID", "failed-make")
    monkeypatch.delenv("MAKEFLAGS", raising=False)
    monkeypatch.delenv("MFLAGS", raising=False)
    assert ci_make.observe_make(makefile, ("one", "two")) == 2
    path = next((tmp_path / "timings").glob("make/failed-make/*/manifest.json"))
    record = json.loads(path.read_bytes())
    assert record["state"] == "completed" and record["exit_status"] == 2
    assert 7 in [row["exit_status"] for row in record["recipes"]]
    assert "two" not in (tmp_path / "executed").read_text()
    assert "two" in record["expected_recipes"]


def test_shared_definitions_cover_every_declared_direct_make_validation_target():
    targets = ci_make.maintained_make_targets(ROOT / "Makefile")
    parameterized = {"unit-shard", "compose-shard", "image-qualification", "build-%"}
    work = require_validation_work([target for target in targets if target not in parameterized])
    assert {item.name for item in work} == set(targets) - parameterized
    for name, variable, value, identity in (
        ("unit-shard", "UNIT_SHARD", "unit", "unit-unit"),
        ("image-qualification", "IMAGE_GROUP", "core", "image-core"),
        ("compose-shard", "COMPOSE_LANE", "storage", "compose-storage"),
    ):
        definitions = ci_make.make_work_definitions((name,), {variable: value})
        assert definitions[0]["name"] == identity


@pytest.mark.parametrize("identity", ["", ".", "..", "../foreign", "x/y", "x" * 129])
def test_invalid_path_identity_never_redirects_evidence(identity):
    with pytest.raises(ValueError):
        ci_make.validation_path_identity(identity)


def test_retention_removes_only_closed_runs_and_preserves_active_evidence(
    actual_make_receipt, tmp_path, monkeypatch
):
    from riverhog_canonical_json import canonical_json_bytes

    _, _, original, _ = actual_make_receipt
    for index, name in enumerate(("old-a", "old-b", "active")):
        directory = tmp_path / "make" / name / original["span"]
        directory.mkdir(parents=True)
        record = copy.deepcopy(original)
        record["run_id"] = name
        path = directory / "manifest.json"
        path.write_bytes(canonical_json_bytes(record))
        os.utime(path, (index, index))
    (tmp_path / "make/active/.active-owner").touch()
    monkeypatch.setenv("RIVERHOG_VALIDATION_RETAIN_RUNS", "2")
    ci_make.retain_completed_runs(tmp_path, "new")
    assert not (tmp_path / "make/old-a").exists()
    assert (tmp_path / "make/old-b").exists()
    assert (tmp_path / "make/active/.active-owner").exists()


def test_exhausted_storage_does_not_discard_active_or_incomplete_attempts(tmp_path, monkeypatch):
    directory = tmp_path / "make/active"
    directory.mkdir(parents=True)
    active = directory / ".active-owner"
    active.write_bytes(b"retained active evidence")
    monkeypatch.setenv("RIVERHOG_VALIDATION_MAX_BYTES", "1")
    with pytest.raises(ValueError, match="storage budget"):
        ci_make.retain_completed_runs(tmp_path, "new")
    assert active.read_bytes() == b"retained active evidence"


def test_recipe_binds_child_evidence_to_the_owner_and_clears_shell_reentry(tmp_path, monkeypatch):
    directory = tmp_path / "owned-recipes"
    result = tmp_path / "child-context.json"
    monkeypatch.setenv("RIVERHOG_VALIDATION_MAKE_DIRECTORY", str(directory))
    monkeypatch.setenv("RIVERHOG_VALIDATION_MAKE_SPAN", "actual-owner")
    monkeypatch.setenv("RIVERHOG_CI_RUN_ID", "actual-recipe")
    monkeypatch.setenv("RIVERHOG_VALIDATION_MAKE_REENTRY", "old-goal")
    import shlex
    import sys

    script = (
        "import json,os,pathlib; "
        f"pathlib.Path({str(result)!r}).write_text(json.dumps({{"
        "name:os.environ.get(name) for name in "
        "('RIVERHOG_VALIDATION_PARENT_SPAN','RIVERHOG_VALIDATION_ACTIVE_SPAN',"
        "'RIVERHOG_VALIDATION_MAKE_DIRECTORY','RIVERHOG_VALIDATION_MAKE_REENTRY')"
        "}))"
    )
    assert ci_make.observe_recipe("one", ("-c", shlex.join((sys.executable, "-c", script)))) == 0
    assert json.loads(result.read_text()) == {
        "RIVERHOG_VALIDATION_PARENT_SPAN": "actual-owner",
        "RIVERHOG_VALIDATION_ACTIVE_SPAN": "actual-owner",
        "RIVERHOG_VALIDATION_MAKE_DIRECTORY": None,
        "RIVERHOG_VALIDATION_MAKE_REENTRY": None,
    }
    receipt = require_canonical_json(next((directory / "recipes").glob("*.json")).read_bytes())
    assert receipt["run_id"] == "actual-recipe" and receipt["span"] == "actual-owner"
    assert receipt["state"] == "completed" and receipt["exit_status"] == 0


@pytest.mark.skipif(os.name != "posix", reason="POSIX Make and recipe cancellation")
@pytest.mark.parametrize("requested_signal", [2, 15])
def test_actual_make_cancellation_joins_owned_recipe_cleanup(tmp_path, requested_signal):
    import shlex
    import subprocess
    import sys

    leaf = tmp_path / "leaf.py"
    leaf.write_text(
        "import signal,time\nfrom pathlib import Path\n"
        f"root=Path({str(tmp_path)!r})\n"
        "def stop(*_):\n    time.sleep(0.1)\n"
        "    (root/'cleaned').write_text('owned cleanup')\n    raise SystemExit(0)\n"
        "signal.signal(signal.SIGINT,stop)\nsignal.signal(signal.SIGTERM,stop)\n"
        "(root/'ready').write_text('ready')\n"
        "deadline=time.monotonic()+20\n"
        "while time.monotonic()<deadline: time.sleep(0.01)\n"
    )
    makefile = tmp_path / "Makefile"
    makefile.write_text(
        "SHELL := bash\nVALIDATION_TARGETS := owner\n"
        "ifneq ($(RIVERHOG_VALIDATION_MAKE_DIRECTORY),)\n"
        "SHELL := $(RIVERHOG_VALIDATION_MAKE_SHELL)\n.SHELLFLAGS := -c\n"
        "export RIVERHOG_VALIDATION_RECIPE_TARGET = $@\nendif\n"
        "# Maintained target recipes.\nowner:\n\t" + shlex.join((sys.executable, str(leaf))) + "\n"
    )
    timing = tmp_path / "timings"
    env = {
        **os.environ,
        "RIVERHOG_CI_TIMING_DIR": str(timing),
        "RIVERHOG_CI_RUN_ID": "cancelled-make",
    }
    for name in ("MAKEFLAGS", "MFLAGS", "MAKEOVERRIDES"):
        env.pop(name, None)
    with subprocess.Popen(
        [
            sys.executable,
            str(ROOT / "scripts/ci_make.py"),
            "observe",
            "--makefile",
            str(makefile),
            "--goal",
            "owner",
        ],
        cwd=tmp_path,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    ) as parent:
        deadline = time.monotonic() + 10
        while not (tmp_path / "ready").exists():
            assert parent.poll() is None and time.monotonic() < deadline
            time.sleep(0.01)
        os.kill(parent.pid, requested_signal)
        assert parent.wait(timeout=10) == 128 + requested_signal
    assert (tmp_path / "cleaned").read_text() == "owned cleanup"
    manifest = require_canonical_json(
        next(timing.glob("make/cancelled-make/*/manifest.json")).read_bytes()
    )
    assert manifest["state"] == "interrupted"
    assert manifest["qualification_eligible"] is False
    assert manifest["recipes"][0]["state"] == "interrupted"
    assert manifest["elapsed_seconds"] >= 0.1


def test_retention_prunes_closed_phase_runs_with_their_owner_but_keeps_active_streams(
    tmp_path, monkeypatch
):
    from riverhog_canonical_json import canonical_json_bytes

    for run, state in (("closed", "completed"), ("active", "running")):
        group = tmp_path / "make" / run / "span"
        group.mkdir(parents=True)
        (group / "manifest.json").write_bytes(
            canonical_json_bytes(
                {
                    "format": "riverhog-make-validation/v1",
                    "run_id": run,
                    "state": state,
                }
            )
        )
        lane = tmp_path / "compose-storage" / run
        lane.mkdir(parents=True)
        (lane / "phases.jsonl").write_bytes(
            canonical_json_bytes(
                {
                    "format": "riverhog-ci-phase-timing/v1",
                    "run_id": run,
                }
            )
            + b"\n"
        )
        (lane / "planning-cost.json").write_text("owned diagnostics")
    monkeypatch.setenv("RIVERHOG_VALIDATION_RETAIN_RUNS", "1")
    ci_make.retain_completed_runs(tmp_path, "new")
    assert not (tmp_path / "compose-storage/closed").exists()
    assert (tmp_path / "compose-storage/active/phases.jsonl").exists()
    assert (tmp_path / "make/active/span/manifest.json").exists()
