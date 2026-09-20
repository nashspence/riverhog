from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from scripts import transfer_profile as profile
from scripts import performance_objectives as performance


def test_transfer_log_summary_selects_scenario_and_omits_identity() -> None:
    text = ("transfer operation=pack_write_segment identity_sha256=secret-digest "
            "plaintext_bytes=1048576 stored_bytes=1048600 queue_seconds=0.1 "
            "source_seconds=0.2 integrity_seconds=0.3 crypto_seconds=0.4 "
            "processing_seconds=0.5 remote_seconds=0.6 checkpoint_seconds=0.7 "
                "downstream_seconds=0.8\n"
            "transfer operation=pack_retrieval_range plaintext_bytes=1 "
                "stored_bytes=2 remote_seconds=10\n")
    summary = profile.summarize_transfer_log(text,
        expected_operations=profile.SCENARIO_OPERATIONS["riverhog-ingress"])
    assert summary.records == 1
    assert summary.operations == {"pack_write_segment": 1}
    assert summary.plaintext_bytes == 1048576
    assert summary.stored_bytes == 1048600
    assert summary.phase_seconds["downstream"] == 0.8
    assert "secret-digest" not in repr(summary)


def simulate(monkeypatch, *, receipt=True, code=0, stale=False):
    monkeypatch.setattr(performance, "source_identity", lambda: {"sha": "1" * 40, "clean": True})
    ticks = iter([10.0, 12.0])
    monkeypatch.setattr(profile.time, "perf_counter", lambda: next(ticks))
    calls = []
    def run(command, *, check, stdout, stderr, env):
        assert not check
        assert stdout == stderr == subprocess.DEVNULL
        calls.append(command)
        if receipt:
            Path(env["RIVERHOG_PERFORMANCE_RECEIPT"]).write_text(json.dumps({
                "schema": "riverhog-performance-completion/v1",
                "run_id": "stale" if stale else env["RIVERHOG_PERFORMANCE_RUN_ID"],
                "completed_bytes": 200 * profile.MIB, "completed_items": 1, "verified": True,
            }))
        return subprocess.CompletedProcess(command, code)
    monkeypatch.setattr(profile.subprocess, "run", run)
    return calls


def arguments():
    return ["--scenario", "riverhog-ingress", "--workload", "large-file", "--payload-bytes",
        str(200 * profile.MIB),
            "--", "riverhog", "upload", "/private/input"]


def test_transfer_profile_runs_without_echoing_command(monkeypatch, capsys) -> None:
    calls = simulate(monkeypatch)
    assert profile.main(arguments()) == 0
    result = json.loads(capsys.readouterr().out)
    assert calls == [["riverhog", "upload", "/private/input"]]
    assert result["mib_per_second"] == 100
    assert result["sample"]["completion_verified"] is True
    assert result["evaluation"]["status"] == "not_evaluated"
    assert result["items_per_second"] == 0.5
    assert "/private/input" not in json.dumps(result)


def test_unverified_command_success_is_not_verified_goodput(monkeypatch, capsys) -> None:
    simulate(monkeypatch, receipt=False)
    assert profile.main(arguments()) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["sample"]["completion_verified"] is False
    assert result["rate_basis"] == "declared-workload-not-verified-goodput"


def test_nominal_baselines_and_anonymous_target_overrides_are_rejected() -> None:
    for flag in ("--baseline-mib-per-second", "--target-utilization"):
        with pytest.raises(SystemExit):
            profile.main([flag, "125", *arguments()])


def test_stale_receipt_is_not_reused(monkeypatch, capsys) -> None:
    simulate(monkeypatch, stale=True)
    assert profile.main(arguments()) == 2
    assert "/private/input" not in capsys.readouterr().err


def test_child_failure_does_not_emit_a_goodput_verdict(monkeypatch, capsys) -> None:
    simulate(monkeypatch, code=7)
    assert profile.main(arguments()) == 7
    assert capsys.readouterr().out == ""


def test_reference_recovery_profile_does_not_require_network_baseline(monkeypatch, capsys) -> None:
    simulate(monkeypatch)
    args = arguments()
    args[1] = "reference-recovery"
    assert profile.main(args) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["sample"]["objective"] is None
    assert result["evaluation"]["reason"] == "no_target"
    assert os.access(profile.__file__, os.X_OK)


def test_invalid_phase_duration_is_not_published() -> None:
    with pytest.raises(performance.PerformanceError):
        profile.summarize_transfer_log(
            'transfer operation=raw_write_segment plaintext_bytes=1 stored_bytes=1 '
            'crypto_seconds=nan',
            expected_operations=profile.SCENARIO_OPERATIONS["riverhog-ingress"],
        )
