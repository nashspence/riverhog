from __future__ import annotations

import io
import json
import subprocess
import sys
from pathlib import Path

from scripts.compose_exception_diagnostics import (
    MAX_ANNOTATION_BYTES,
    MAX_FRAMES,
    MAX_INPUT_BYTES,
    PREFIX,
    PublicSources,
    observe,
)

ROOT = Path(__file__).resolve().parents[2]


def _sources(tmp_path):
    names = (
        "riverhog/src/riverhog_api/app.py",
        "some-implementations/ftp/src/a_riverhog_ftp_spool/app.py",
    )
    for name in names:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# public source\n" * 200)
    return PublicSources(tmp_path, names)


def test_traceback_uses_only_public_source_identity_and_fixed_error_types(tmp_path):
    sources = _sources(tmp_path)
    payload = (
        'private-container | File "/private/token/site-packages/riverhog_api/app.py", '
        "line 17, in private_function\n"
        'private-container | File "C:\\private\\secret\\a_riverhog_ftp_spool\\app.py", '
        "line 23, in secret_handler\n"
        "private-container | sqlalchemy.exc.IntegrityError: private SQL and credentials\n"
        "private-container | RuntimeError: https://private.invalid/private-token\n"
    )
    result = observe(io.BytesIO(payload.encode()), sources, "ftp")
    assert result == {
        "format": "riverhog-compose-exception-locations/v1",
        "owner": "ftp",
        "frames": [
            {"path": "riverhog/src/riverhog_api/app.py", "line": 17},
            {"path": "some-implementations/ftp/src/a_riverhog_ftp_spool/app.py", "line": 23},
        ],
        "error_types": ["IntegrityError", "RuntimeError"],
        "sample_truncated": False,
    }
    encoded = json.dumps(result)
    assert "private" not in encoded and "secret" not in encoded


def test_foreign_paths_unknown_types_and_invalid_source_lines_are_omitted(tmp_path):
    sources = _sources(tmp_path)
    payload = (
        'File "/private/foreign/app.py", line 12, in leak\n'
        'File "/private/riverhog_api/app.py", line 999, in leak\n'
        'File "/private/riverhog_api/app.py", line 0, in leak\n'
        "PrivateCredentialError: private payload\n"
    )
    assert observe(io.BytesIO(payload.encode()), sources, "riverhog") is None


def test_source_index_rejects_traversal_and_external_symlinks(tmp_path):
    outside = tmp_path / "outside.py"
    outside.write_text("# private data\n" * 40)
    root = tmp_path / "repo"
    path = root / "scripts/leak.py"
    path.parent.mkdir(parents=True)
    path.symlink_to(outside)
    sources = PublicSources(root, ("../outside.py", str(outside), "scripts/leak.py"))
    payload = b'File "/private/scripts/leak.py", line 1, in leak\n'
    assert observe(io.BytesIO(payload), sources, "riverhog") is None


def test_large_log_input_is_bounded_and_marks_partial_sampling(tmp_path):
    payload = (
        b'File "/app/riverhog_api/app.py", line 1, in handler\n'
        + b"private-value" * MAX_INPUT_BYTES
    )
    stream = io.BytesIO(payload)
    result = observe(stream, _sources(tmp_path), "riverhog")
    assert result is not None and result["sample_truncated"] is True
    assert result["frames"] == [{"path": "riverhog/src/riverhog_api/app.py", "line": 1}]
    assert stream.tell() <= MAX_INPUT_BYTES
    assert len(json.dumps(result).encode()) <= MAX_ANNOTATION_BYTES
    assert "private-value" not in json.dumps(result)


def test_oversized_line_cannot_inject_a_frame_from_its_middle(tmp_path):
    payload = (
        b"private-value" * 400
        + b'File "/app/riverhog_api/app.py", line 99, in injected\n'
        + b'File "/app/riverhog_api/app.py", line 7, in actual\n'
    )
    result = observe(io.BytesIO(payload), _sources(tmp_path), "riverhog")
    assert result is not None and result["sample_truncated"] is True
    assert result["frames"] == [{"path": "riverhog/src/riverhog_api/app.py", "line": 7}]


def test_exception_group_locations_keep_a_bounded_recent_distinct_sample(tmp_path):
    payload = "".join(
        f'worker | File "/app/riverhog_api/app.py", line {number}, in dispatch\n'
        for number in range(1, 41)
    )
    payload += 'worker | File "/app/riverhog_api/app.py", line 40, in dispatch\n'
    result = observe(io.BytesIO(payload.encode()), _sources(tmp_path), "processing")
    assert result is not None
    assert result["frames"] == [
        {"path": "riverhog/src/riverhog_api/app.py", "line": number}
        for number in range(41 - MAX_FRAMES, 41)
    ]
    assert len(json.dumps(result).encode()) <= MAX_ANNOTATION_BYTES


def test_cli_emits_only_the_bounded_annotation_and_does_not_copy_the_input():
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/compose_exception_diagnostics.py"),
            "--owner",
            "riverhog",
        ],
        input=(
            'service | File "/private/riverhog_api/app.py", line 1, in secret_handler\n'
            "service | RuntimeError: private credentials and request body\n"
        ),
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout == "" and result.stderr.startswith(PREFIX)
    annotation = json.loads(result.stderr.removeprefix(PREFIX))
    assert annotation["frames"] == [{"path": "riverhog/src/riverhog_api/app.py", "line": 1}]
    assert annotation["error_types"] == ["RuntimeError"]
    assert "private" not in result.stderr and "secret" not in result.stderr


def test_cleanup_retains_the_original_exit_even_if_log_classification_fails(tmp_path):
    owner = (ROOT / "scripts/test_compose_smoke.sh").read_text()
    helper = "qualification_logs() {\n" + owner.split("qualification_logs() {\n", 1)[1]
    helper = helper.split("\ncleanup() {\n", 1)[0]
    tool = tmp_path / "collector"
    tool.write_text("#!/usr/bin/env bash\ncat >/dev/null\nexit 19\n")
    tool.chmod(0o755)
    script = (
        "set -Eeuo pipefail\n"
        + helper
        + '\nROOT_DIR="$1"\nMISE_BIN="$2"\n'
        + "fixture_logs() { printf 'operator-log-preserved\\n'; return 17; }\n"
        + "cleanup() { local status=$?; qualification_logs riverhog fixture_logs; "
        + 'return "$status"; }\n'
        + "trap cleanup EXIT\nexit 23\n"
    )
    result = subprocess.run(
        ["bash", "-c", script, "fixture", str(ROOT), str(tool)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 23
    assert result.stdout == "" and "operator-log-preserved" in result.stderr
