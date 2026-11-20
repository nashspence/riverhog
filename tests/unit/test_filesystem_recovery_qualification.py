from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/test_filesystem_recovery_qualification.sh"


@pytest.mark.parametrize(
    ("failure", "phase"),
    [
        ("build-test", "images-test"),
        ("build-client", "image-client"),
        ("start", "service-start"),
        ("upload", "collection-upload"),
        ("rebuild", "index-rebuild"),
    ],
)
def test_filesystem_failure_retains_phase_status_and_run_owned_cleanup(tmp_path, failure, phase):
    tools = tmp_path / "tools"
    tools.mkdir()
    docker = tools / "docker"
    docker.write_text(
        f"#!{sys.executable}\n"
        "import json,os,sys\n"
        "from pathlib import Path\n"
        "args=sys.argv[1:];operation='cleanup'\n"
        "if args[0]=='compose':\n"
        " if 'build' in args:operation='build-'+args[-1]\n"
        " elif 'up' in args:operation='start'\n"
        " elif 'exec' in args:\n"
        "  bootstrap=any(a.startswith("
        "'RIVERHOG_QUALIFICATION_BOOTSTRAP_TOKEN=') for a in args)\n"
        "  operation='key' if bootstrap else 'rebuild'\n"
        "elif args[0]=='build':operation='build-'+args[args.index('--target')+1]\n"
        "elif args[:2]==['image','rm']:operation='cleanup-images'\n"
        "elif args[0]=='run' and 'collection' in args:operation='upload'\n"
        "with Path(os.environ['QUALIFICATION_TEST_CALLS']).open('a') as out:\n"
        " out.write(json.dumps({'operation':operation})+'\\n')\n"
        "if operation==os.environ['QUALIFICATION_TEST_FAILURE']:raise SystemExit(7)\n"
        "if operation=='key':print('fixture-only-key')\n"
        "elif operation=='upload':\n"
        " print(json.dumps({'collection_id':'1','archive_root_sha256':'0'*64}))\n"
    )
    docker.chmod(0o755)
    mise = tools / "mise"
    mise.write_text(
        f"#!{sys.executable}\n"
        "import os,sys\n"
        "args=sys.argv[1:];pos=args.index('python')\n"
        "os.execv(sys.executable,[sys.executable,*args[pos+1:]])\n"
    )
    mise.chmod(0o755)
    dd = tools / "dd"
    dd.write_text(
        f"#!{sys.executable}\n"
        "from pathlib import Path\n"
        "import sys\n"
        "Path(next(a[3:] for a in sys.argv[1:] if a.startswith('of='))).write_bytes(b'fixture')\n"
    )
    dd.chmod(0o755)
    compose_env = tmp_path / "compose.env"
    compose_env.touch()
    calls = tmp_path / "calls.jsonl"
    evidence = tmp_path / "evidence"
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    result = subprocess.run(
        ["bash", str(SCRIPT)],
        cwd=ROOT,
        env={
            **os.environ,
            "PATH": str(tools) + os.pathsep + os.environ["PATH"],
            "MISE_BIN": str(mise),
            "COMPOSE_ENV_FILE": str(compose_env),
            "COMPOSE_PROJECT_NAME": "qualification-phase-fixture",
            "RIVERHOG_QUALIFICATION_IMAGES_READY": "0",
            "SOURCE_REVISION": "fixture-revision",
            "RIVERHOG_CI_LANE": "filesystem-recovery-qualification",
            "RIVERHOG_CI_RUN_ID": "phase-fixture",
            "RIVERHOG_CI_TIMING_DIR": str(evidence),
            "QUALIFICATION_TEST_CALLS": str(calls),
            "QUALIFICATION_TEST_FAILURE": failure,
            "TMPDIR": str(scratch),
        },
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 7, result.stderr
    phases = [
        json.loads(line)
        for line in (evidence / "filesystem-recovery-qualification/phase-fixture/phases.jsonl")
        .read_text()
        .splitlines()
    ]
    assert phases[-2]["phase"] == phase
    assert phases[-1]["phase"] == "teardown"
    assert [row["exit_status"] for row in phases[-2:]] == [7, 7]
    assert all(row["exit_status"] == 0 for row in phases[:-2])
    assert all(row["elapsed_seconds"] >= 0 for row in phases)
    assert {row["run_id"] for row in phases} == {"phase-fixture"}
    assert {row["lane"] for row in phases} == {"filesystem-recovery-qualification"}
    operations = [json.loads(line)["operation"] for line in calls.read_text().splitlines()]
    assert failure in operations and "cleanup-images" in operations
    assert operations[-1] == "cleanup"
    assert not list(scratch.iterdir())


@pytest.mark.parametrize("variable", ["receipt", "description"])
def test_qualification_phase_reporting_preserves_captured_client_json(tmp_path, variable):
    source = SCRIPT.read_text()
    start = source.index(variable + '="$({')
    end = source.index("\n}", start)
    end = source.index("\n", end + 1)
    capture = source[start:end]
    prelude = r"""
ci_phase() { printf 'Qualification phase: %s\n' "$1"; }
docker() { printf '{"fixture":1}\n'; }
proof_root="$1"
network_name=fixture
qualification_token=fixture
client_image=fixture
recovery_image=fixture
archive_relative=fixture
tags=()
"""
    observed = subprocess.run(
        [
            "bash",
            "-eu",
            "-c",
            prelude + capture + "\nprintf '%s' \"$" + variable + '"',
            "fixture",
            str(tmp_path),
        ],
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert observed.returncode == 0, observed.stderr
    assert json.loads(observed.stdout) == {"fixture": 1}
