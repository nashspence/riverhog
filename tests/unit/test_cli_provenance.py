from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import a_riverhog_cli.main
from a_riverhog_cli.main import app
from typer.testing import CliRunner

RUNNER = CliRunner()
ARTIFACT_ID = "a" * 64
JOURNAL_ID = "urn:uuid:00000000-0000-4000-8000-000000000042"
JOURNAL = b'\x1e{"exact":"journal"}\n'


def test_provenance_artifacts_journals_and_export_share_one_cli_surface(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls: list[tuple[str, object]] = []
    shown = {
        "collection_id": 41,
        "archive_root_sha256": "b" * 64,
        "artifact": {"artifact_id": ARTIFACT_ID, "bytes": 7, "sha256": "c" * 64},
        "binding": {
            "journal": {"journal_id": JOURNAL_ID, "prefix_sha256": "d" * 64},
            "delivery_association_id": "urn:uuid:00000000-0000-4000-8000-000000000043",
        },
    }
    artifacts = {
        "artifacts": [{"collection_id": 41, "artifact_id": ARTIFACT_ID}],
        "next_artifact_id": None,
    }
    journals = {
        "journals": [{"journal_id": JOURNAL_ID, "bytes": len(JOURNAL)}],
        "next_journal_id": None,
    }

    class FakeClient:
        def list_collection_artifact_provenance(
            self, collection_id: int, **kwargs: Any
        ) -> dict[str, Any]:
            calls.append(("list", (collection_id, kwargs)))
            return artifacts

        def get_collection_artifact_provenance(
            self, collection_id: int, artifact_id: str
        ) -> dict[str, Any]:
            calls.append(("show", (collection_id, artifact_id)))
            return shown

        def list_collection_provenance_journals(
            self, collection_id: int, **kwargs: Any
        ) -> dict[str, Any]:
            calls.append(("journals", (collection_id, kwargs)))
            return journals

        def download_collection_provenance_journal(
            self,
            collection_id: int,
            journal_id: str,
            *,
            output: Path,
        ) -> tuple[int, str]:
            calls.append(("export", (collection_id, journal_id)))
            output.write_bytes(JOURNAL)
            return len(JOURNAL), hashlib.sha256(JOURNAL).hexdigest()

    monkeypatch.setattr(a_riverhog_cli.main, "client", FakeClient)
    output = tmp_path / "journal.json-seq"

    listed = RUNNER.invoke(app, ["collection", "provenance", "list", "41", "--json"])
    shown_result = RUNNER.invoke(
        app, ["collection", "provenance", "show", "41", ARTIFACT_ID, "--json"]
    )
    journal_page = RUNNER.invoke(app, ["collection", "provenance", "journals", "41", "--json"])
    exported = RUNNER.invoke(
        app,
        ["collection", "provenance", "export", "41", JOURNAL_ID, "--output", str(output), "--json"],
    )

    assert (
        listed.exit_code
        == shown_result.exit_code
        == journal_page.exit_code
        == exported.exit_code
        == 0
    )
    assert json.loads(listed.stdout) == artifacts
    assert json.loads(shown_result.stdout) == shown
    assert json.loads(journal_page.stdout) == journals
    assert output.read_bytes() == JOURNAL
    assert json.loads(exported.stdout) == {
        "collection_id": 41,
        "journal_id": JOURNAL_ID,
        "output": str(output.resolve()),
        "bytes": len(JOURNAL),
        "sha256": hashlib.sha256(JOURNAL).hexdigest(),
    }
    assert calls == [
        ("list", (41, {"page_size": 50, "after_artifact_id": None, "archive_root_sha256": None})),
        ("show", (41, ARTIFACT_ID)),
        (
            "journals",
            (41, {"page_size": 50, "after_journal_id": None, "archive_root_sha256": None}),
        ),
        ("export", (41, JOURNAL_ID)),
    ]

    human = RUNNER.invoke(app, ["collection", "provenance", "show", "41", ARTIFACT_ID])
    journal_human = RUNNER.invoke(app, ["collection", "provenance", "journals", "41"])
    assert human.exit_code == journal_human.exit_code == 0
    assert ARTIFACT_ID in human.stdout
    assert JOURNAL_ID in human.stdout
    assert JOURNAL_ID in journal_human.stdout


def test_provenance_list_selectors_emit_exact_artifact_ids(monkeypatch) -> None:
    class FakeClient:
        def list_collection_artifact_provenance(
            self, collection_id: int, **_kwargs: Any
        ) -> dict[str, Any]:
            assert collection_id == 41
            return {
                "artifacts": [
                    {"collection_id": 41, "artifact_id": "a" * 64},
                    {"collection_id": 41, "artifact_id": "b" * 64},
                ],
                "next_artifact_id": None,
            }

    monkeypatch.setattr(a_riverhog_cli.main, "client", FakeClient)
    result = RUNNER.invoke(app, ["collection", "provenance", "list", "41", "--selectors"])

    assert result.exit_code == 0
    assert result.stdout == f"41::{'a' * 64}\n41::{'b' * 64}\n"
