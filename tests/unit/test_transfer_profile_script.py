from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from types import ModuleType
from uuid import uuid4

import pytest
from riverhog_canonical_json import canonical_json_bytes

from scripts import performance_measurement as performance

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "transfer_profile.py"


def comparison_context() -> dict[str, object]:
    return {
        "format": performance.CONTEXT_FORMAT,
        "comparison_id": str(uuid4()),
        "workload_sha256": "a" * 64,
        "environment_sha256": "b" * 64,
        "path_sha256": "c" * 64,
        "byte_domain": "logical-payload",
        "completion_boundary": "verified-command-completion",
        "cache_state": "cold",
        "concurrency": 1,
    }


def load_script() -> ModuleType:
    spec = importlib.util.spec_from_file_location("transfer_profile", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_transfer_log_parser_imports_with_only_the_standard_library() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-S",
            "-c",
            "import sys; "
            + f"sys.path.insert(0, {str(REPO)!r}); "
            + "from scripts.transfer_profile import SCENARIO_OPERATIONS, summarize_transfer_log; "
            + "print(summarize_transfer_log('transfer operation=raw_write_segment "
            + "plaintext_bytes=1 stored_bytes=2', "
            + "expected_operations=SCENARIO_OPERATIONS['stove0-derived-publication']).records)",
        ],
        check=False,
        cwd=REPO,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "1"


def test_transfer_log_summary_selects_scenario_and_omits_identity() -> None:
    module = load_script()
    text = (
        "ignored line\n"
        "transfer operation=pack_write_segment identity_sha256=secret-digest "
        "plaintext_bytes=1048576 stored_bytes=1048600 queue_seconds=0.1 "
        "source_seconds=0.2 integrity_seconds=0.3 crypto_seconds=0.4 "
        "processing_seconds=0.5 remote_seconds=0.6 checkpoint_seconds=0.7 "
        "downstream_seconds=0.8 elapsed_seconds=3.6 bottleneck=downstream\n"
        "transfer operation=pack_retrieval_range identity_sha256=other "
        "plaintext_bytes=1 stored_bytes=2 remote_seconds=10 bottleneck=remote\n"
    )

    summary = module.summarize_transfer_log(
        text,
        expected_operations=module.SCENARIO_OPERATIONS["riverhog-ingress"],
    )

    assert summary.records == 1
    assert summary.operations == {"pack_write_segment": 1}
    assert summary.bottlenecks == {"downstream": 1}
    assert summary.plaintext_bytes == 1048576
    assert summary.stored_bytes == 1048600
    assert summary.phase_seconds == {
        "checkpoint": 0.7,
        "crypto": 0.4,
        "downstream": 0.8,
        "integrity": 0.3,
        "processing": 0.5,
        "queue": 0.1,
        "remote": 0.6,
        "source": 0.2,
    }
    assert "secret-digest" not in repr(summary)


def test_transfer_profile_reports_jcs_without_echoing_command(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = load_script()
    log = tmp_path / "transfer.log"
    log.write_text(
        "transfer operation=raw_write_segment identity_sha256=private "
        "plaintext_bytes=2097152 stored_bytes=2097200 queue_seconds=0 "
        "source_seconds=0.1 crypto_seconds=0.2 remote_seconds=0.3 "
        "checkpoint_seconds=0.1 downstream_seconds=0 elapsed_seconds=0.7 "
        "bottleneck=remote\n",
        encoding="utf-8",
    )
    commands: list[list[str]] = []
    context = comparison_context()
    context_path = tmp_path / "context.json"
    context_path.write_text(json.dumps(context), encoding="utf-8")
    reference = performance.sample(
        scenario="riverhog-ingress",
        workload="large-file",
        context=context,
        completed_bytes=200 * module.MIB,
        elapsed_seconds=1.6,
        completion_verified=True,
        run_id=str(uuid4()),
    )
    reference_path = tmp_path / "reference.json"
    reference_path.write_text(
        json.dumps({"format": "riverhog-transfer-profile/v3", "sample": reference}),
        encoding="utf-8",
    )

    def run(
        command: list[str],
        *,
        check: bool,
        stdout: int,
        stderr: int,
        env: dict[str, str],
    ) -> subprocess.CompletedProcess[str]:
        assert not check
        assert stdout == subprocess.DEVNULL
        assert stderr == subprocess.DEVNULL
        commands.append(command)
        Path(env["RIVERHOG_PERFORMANCE_RECEIPT"]).write_text(
            json.dumps(
                {
                    "format": "riverhog-performance-completion/v1",
                    "run_id": env["RIVERHOG_PERFORMANCE_RUN_ID"],
                    "completed_bytes": 200 * module.MIB,
                    "completed_items": 1,
                    "verified": True,
                }
            ),
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(module.subprocess, "run", run)
    ticks = iter((10.0, 12.0))
    monkeypatch.setattr(module.time, "perf_counter", lambda: next(ticks))

    assert (
        module.main(
            [
                "--scenario",
                "riverhog-ingress",
                "--workload",
                "large-file",
                "--payload-bytes",
                str(200 * module.MIB),
                "--context",
                str(context_path),
                "--reference",
                str(reference_path),
                "--target-ratio",
                "0.9",
                "--transfer-log",
                str(log),
                "--",
                "riverhog",
                "upload",
                "/private/input",
            ]
        )
        == 0
    )

    output = capsys.readouterr().out
    result = json.loads(output)
    assert output.encode() == canonical_json_bytes(result) + b"\n"
    assert commands == [["riverhog", "upload", "/private/input"]]
    assert result["mib_per_second"] == 100.0
    assert result["target"]["reference_ratio"] == 0.9
    assert result["comparison"]["status"] == "missed"
    assert result["comparison"]["report_only"] is True
    assert result["comparison"]["reference_ratio"] == 0.8
    assert result["comparison"]["candidate_bytes_per_second"] == 100 * module.MIB
    assert result["comparison"]["reference_bytes_per_second"] == 125 * module.MIB
    assert result["rate_basis"] == "receipt-verified"
    assert result["items_per_second"] == 0.5
    assert result["seconds_per_item"] == 2.0
    assert result["transfer_log"]["operations"] == {"raw_write_segment": 1}
    assert "/private/input" not in json.dumps(result)


def test_omitted_reference_or_context_is_reported_without_failing_profile(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = load_script()
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda command, **_kwargs: subprocess.CompletedProcess(command, 0),
    )
    ticks = iter((1.0, 2.0))
    monkeypatch.setattr(module.time, "perf_counter", lambda: next(ticks))
    assert (
        module.main(
            [
                "--scenario",
                "archive-replication",
                "--workload",
                "resume",
                "--payload-bytes",
                "1",
                "--target-ratio",
                "0.9",
                "--",
                "true",
            ]
        )
        == 0
    )
    result = json.loads(capsys.readouterr().out)
    assert result["comparison"]["status"] == "not-compared"
    assert result["comparison"]["reason"] == "no-measured-reference"
    assert result["target"]["reference_ratio"] == 0.9


def test_recovery_tool_profile_does_not_require_network_baseline(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = load_script()
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda command, **_kwargs: subprocess.CompletedProcess(command, 0),
    )
    ticks = iter((1.0, 2.0))
    monkeypatch.setattr(module.time, "perf_counter", lambda: next(ticks))

    assert (
        module.main(
            [
                "--scenario",
                "a-riverhog-recovery-tool",
                "--workload",
                "many-small-files",
                "--payload-bytes",
                str(module.MIB),
                "--items",
                "20",
                "--",
                "a-riverhog-recovery-tool",
                "archive",
                "output",
            ]
        )
        == 0
    )
    result = json.loads(capsys.readouterr().out)
    assert result["comparison"]["status"] == "not-compared"
    assert result["comparison"]["reason"] == "scenario-has-no-reference"
    assert result["rate_basis"] == "declared-workload-unverified"
    assert result["items"] == 20
    assert result["items_per_second"] == 20.0
    assert result["seconds_per_item"] == 0.05
    assert os.access(SCRIPT, os.X_OK)


def test_command_failure_reports_observation_and_exits_nonzero(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = load_script()
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda command, **_kwargs: subprocess.CompletedProcess(command, 17),
    )
    ticks = iter((1.0, 2.0))
    monkeypatch.setattr(module.time, "perf_counter", lambda: next(ticks))
    assert (
        module.main(
            [
                "--scenario",
                "riverhog-ingress",
                "--workload",
                "large-file",
                "--payload-bytes",
                "1024",
                "--target-ratio",
                "0.9",
                "--",
                "false",
            ]
        )
        == 2
    )
    result = json.loads(capsys.readouterr().out)
    assert result["observed"]["command_exit_code"] == 17
    assert result["sample"] is None
    assert result["comparison"] == {
        "status": "not-compared",
        "reason": "command-failed",
        "report_only": True,
    }


def test_command_launch_failure_reports_and_exits_nonzero(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = load_script()

    def unavailable(*_args: object, **_kwargs: object) -> None:
        raise FileNotFoundError("private-command-name")

    monkeypatch.setattr(module.subprocess, "run", unavailable)
    ticks = iter((1.0, 2.0))
    monkeypatch.setattr(module.time, "perf_counter", lambda: next(ticks))
    assert (
        module.main(
            [
                "--scenario",
                "riverhog-ingress",
                "--workload",
                "large-file",
                "--payload-bytes",
                "1024",
                "--",
                "private-command-name",
            ]
        )
        == 2
    )
    result = json.loads(capsys.readouterr().out)
    assert result["observed"]["launch_state"] == "failed"
    assert result["comparison"]["reason"] == "command-launch-failed"
    assert "private-command-name" not in json.dumps(result)


def test_absent_transfer_log_operations_do_not_gate_completed_work(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = load_script()
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda command, **_kwargs: subprocess.CompletedProcess(command, 0),
    )
    ticks = iter((1.0, 2.0))
    monkeypatch.setattr(module.time, "perf_counter", lambda: next(ticks))
    log = tmp_path / "transfer.log"
    log.write_text("no matching operation", encoding="utf-8")
    assert (
        module.main(
            [
                "--scenario",
                "riverhog-ingress",
                "--workload",
                "large-file",
                "--payload-bytes",
                "1024",
                "--transfer-log",
                str(log),
                "--",
                "true",
            ]
        )
        == 0
    )
    result = json.loads(capsys.readouterr().out)
    assert result["transfer_log"] is None
    assert result["transfer_log_state"] == "unavailable-or-incomplete"
    assert result["comparison"]["status"] == "not-compared"


@pytest.mark.parametrize("reference", ["missing", "malformed", "invalid-sample"])
def test_explicit_invalid_reference_fails_before_running_command(
    reference: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = load_script()
    path = tmp_path / "reference.json"
    if reference == "malformed":
        path.write_text("{", encoding="utf-8")
    elif reference == "invalid-sample":
        path.write_text(
            json.dumps({"format": "riverhog-transfer-profile/v3", "sample": {}}),
            encoding="utf-8",
        )
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *_args, **_kwargs: pytest.fail("invalid reference started the workload"),
    )

    assert (
        module.main(
            [
                "--scenario",
                "riverhog-ingress",
                "--workload",
                "large-file",
                "--payload-bytes",
                "1024",
                "--reference",
                str(path),
                "--",
                "true",
            ]
        )
        == 2
    )
    assert capsys.readouterr().out == ""


def test_invalid_completion_receipt_fails_profiling(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = load_script()

    def run(_command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        environment = kwargs["env"]
        assert isinstance(environment, dict)
        Path(environment["RIVERHOG_PERFORMANCE_RECEIPT"]).write_text(
            json.dumps({"format": "riverhog-performance-completion/v1", "verified": True}),
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(["true"], 0)

    monkeypatch.setattr(module.subprocess, "run", run)
    assert (
        module.main(
            [
                "--scenario",
                "riverhog-ingress",
                "--workload",
                "large-file",
                "--payload-bytes",
                "1024",
                "--",
                "true",
            ]
        )
        == 2
    )
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "invalid measurement input or execution" in captured.err


def test_impossible_measurement_fails_profiling(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = load_script()
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda command, **_kwargs: subprocess.CompletedProcess(command, 0),
    )
    monkeypatch.setattr(module.time, "perf_counter", lambda: 1.0)
    assert (
        module.main(
            [
                "--scenario",
                "riverhog-ingress",
                "--workload",
                "large-file",
                "--payload-bytes",
                "1024",
                "--",
                "true",
            ]
        )
        == 2
    )
    result = json.loads(capsys.readouterr().out)
    assert result["comparison"]["reason"] == "measurement-unavailable"
