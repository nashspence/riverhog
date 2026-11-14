from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
OWNER = ROOT / "scripts/test_compose_smoke.sh"
PREFIX = "::error title=Compose qualification::"


def run_owner_header(tmp_path, body, *, closed_stderr=False, extra_args=()):
    header = OWNER.read_text().split("\nsource ", 1)[0]
    script = tmp_path / "private-payload-not-for-diagnostics.sh"
    exit_receipt = tmp_path / "exit-status"
    script.write_text(
        header
        + '\nexit_receipt="$1"\ntrap \'printf "%s" "$?" > "$exit_receipt"\' EXIT\n'
        + body
        + "\n"
    )
    result = subprocess.run(
        ["bash", str(script), str(exit_receipt), *extra_args],
        capture_output=True,
        text=True,
        check=False,
    )
    annotations = [
        json.loads(line.removeprefix(PREFIX))
        for line in result.stderr.splitlines()
        if line.startswith(PREFIX)
    ]
    assert "private-payload" not in result.stderr
    assert exit_receipt.read_text() == str(result.returncode)
    if not closed_stderr:
        assert all(
            set(row) == {"format", "exit_status", "source_lines"}
            and row["format"] == "riverhog-compose-failure-diagnostic/v1"
            and (row["exit_status"] == result.returncode)
            and (1 <= len(row["source_lines"]) <= 3)
            and all(
                type(line) is int and 0 < line <= len(script.read_text().splitlines())
                for line in row["source_lines"]
            )
            for row in annotations
        )
    return (result, annotations, script)


@pytest.mark.parametrize(
    "body,status",
    [
        ("bash -c 'exit 23' private-payload", 23),
        ("inner() {\n  bash -c 'exit 17' private-payload\n}\nouter() {\n  inner\n}\nouter", 17),
        ("value=\"$(bash -c 'exit 19' private-payload)\"", 19),
        ("printf '%s' private-payload | bash -c 'exit 21'", 21),
        ("test private-payload = different", 1),
    ],
)
def test_compose_unhandled_failures_preserve_exit_and_emit_only_owned_source_lines(
    tmp_path, body, status
):
    result, annotations, script = run_owner_header(tmp_path, body + "\nprintf unreachable")
    assert result.returncode == status
    assert result.stdout == ""
    assert len(annotations) == 1
    lines = script.read_text().splitlines()
    assert any(
        any(token in lines[line - 1] for token in ("exit ", "test ", "inner", "outer", "value="))
        for line in annotations[0]["source_lines"]
    )


@pytest.mark.parametrize(
    "body",
    ["if false; then printf unreachable; fi", "false || true", "! false", "set +e\nfalse\ntrue"],
)
def test_compose_handled_failures_do_not_emit_error_annotations(tmp_path, body):
    result, annotations, _script = run_owner_header(tmp_path, body)
    assert result.returncode == 0 and result.stderr == ""
    assert not annotations


def test_successful_compose_work_keeps_output_without_diagnostic_work(tmp_path):
    result, annotations, _script = run_owner_header(tmp_path, "printf expected-output")
    assert result.returncode == 0 and result.stdout == "expected-output"
    assert result.stderr == "" and (not annotations)


def test_compose_reporting_failure_cannot_replace_original_exit(tmp_path):
    result, annotations, _script = run_owner_header(
        tmp_path, "exec 2>&-\nbash -c 'exit 23'", closed_stderr=True
    )
    assert result.returncode == 23 and (not annotations)


def test_compose_foreign_helper_failure_reports_only_its_owned_call_site(tmp_path):
    foreign = tmp_path / "private-payload-helper.sh"
    foreign.write_text("bash -c 'exit 27'\n")
    result, annotations, script = run_owner_header(
        tmp_path, 'source "$2"', extra_args=(str(foreign),)
    )
    assert result.returncode == 27 and len(annotations) == 1
    lines = script.read_text().splitlines()
    assert all(lines[line - 1] == 'source "$2"' for line in annotations[0]["source_lines"])


def test_compose_diagnostic_bounds_deep_shell_call_stacks(tmp_path):
    body = ["f0() { bash -c 'exit 29'; }"]
    body.extend(f"f{index}() {{ f{index - 1}; }}" for index in range(1, 30))
    body.append("f29")
    result, annotations, _script = run_owner_header(tmp_path, "\n".join(body))
    assert result.returncode == 29 and len(annotations) == 1
    assert len(annotations[0]["source_lines"]) == 3


def test_successful_command_substitution_does_not_report_its_nonterminal_failure(tmp_path):
    result, annotations, _script = run_owner_header(
        tmp_path, 'value="$(false; printf expected-output)"\nprintf "%s" "$value"'
    )
    assert result.returncode == 0 and result.stdout == "expected-output"
    assert result.stderr == "" and (not annotations)
