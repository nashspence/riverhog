from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize("application", ["riverhog", "stove0"])
def test_event_cursor_continues_across_process_restart(application, tmp_path, record_property):
    def run(phase):
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "tests.harness.event_cursor_restart",
                application,
                phase,
                str(tmp_path),
            ],
            cwd=Path(__file__).resolve().parents[2],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        return json.loads(result.stdout)

    # Each invocation exits before the next starts; only the SQLite database and
    # fixture credentials cross the process boundary. Both clients use real ASGI routes.
    before = run("prepare")
    (tmp_path / "before.json").write_text(json.dumps(before))
    after = run("resume")

    assert after["operation_id"] == before["operation_id"]
    history = before["history"]["events"]
    assert [event["subject"] for event in history] == before["subjects"]
    assert len(history) == 3
    assert len({event["id"] for event in history}) == 3
    assert before["history"]["has_more"] is False
    first = before["first"]
    assert first["events"] == history[:1]
    assert first["has_more"] is True

    cursor = first["next_cursor"]
    for index, page in enumerate(after["unread"], start=1):
        assert page["events"] == history[index : index + 1]
        assert page["has_more"] is (index == 1)
        assert page["next_cursor"] != cursor
        cursor = page["next_cursor"]
    assert cursor == before["history"]["next_cursor"]
    assert after["empty"] == {"events": [], "next_cursor": cursor, "has_more": False}

    appended = after["appended"]
    assert len(appended["events"]) == 1
    event = appended["events"][0]
    assert event["subject"] == after["appended_subject"]
    assert event["id"] not in {previous["id"] for previous in history}
    assert appended["next_cursor"] != cursor
    assert appended["has_more"] is False
    assert after["terminal"] == {
        "events": [],
        "next_cursor": appended["next_cursor"],
        "has_more": False,
    }
    # The observer exports this attribution only for a passed, non-xfailed test.
    record_property(
        "event_cursor_restart",
        {
            "application": application,
            "operation_id": before["operation_id"],
        },
    )
