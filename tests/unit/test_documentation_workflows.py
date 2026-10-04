"""An author plans, commits Markdown and inspects an honestly incomplete preview."""

from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import documentation as workflow  # noqa: E402
from contract_atlas.documentation_markdown import front_matter  # noqa: E402
from contract_atlas.documentation_requirements import target_key  # noqa: E402


def test_author_can_plan_write_exact_markdown_and_review_an_incomplete_candidate(
    tmp_path, monkeypatch, capsys, generated_contract_closure
):
    repository = tmp_path / "authoring"
    repository.mkdir()

    def git(*args):
        return (
            subprocess.check_output(
                [
                    "git",
                    "-C",
                    str(repository),
                    "-c",
                    "user.name=Documentation author test",
                    "-c",
                    "user.email=documentation@example.invalid",
                    "-c",
                    "commit.gpgsign=false",
                    *args,
                ],
                stderr=subprocess.DEVNULL,
            )
            .decode()
            .strip()
        )

    git("init", "-b", "main")
    (repository / "README.md").write_text("Synthetic code fixture.\n")
    git("add", "README.md")
    git("commit", "-m", "Synthetic code history")
    git("checkout", "--orphan", "release-documentation")
    (repository / "README.md").write_text("Synthetic independent authoring fixture.\n")

    original = generated_contract_closure["candidate"]
    candidate = replace(original, manifest={**original.manifest, "source_sha": None})
    element = next(
        row
        for row in candidate.bundle.closure["elements"]
        if row["interface"] == "cli" and row["title"] == "a-riverhog-cli"
    )
    target = {"element_id": element["id"], "pointer": ""}
    monkeypatch.setattr(workflow, "ROOT", repository)
    monkeypatch.setattr(workflow, "build_candidate", lambda: candidate)
    output = tmp_path / "preview"
    starter = repository / "v1.0.0/reference/client.md"
    common = ["--initial-review", "--output", str(output), "--element-id", element["id"]]
    assert workflow.main(["plan", *common, "--starter", str(starter)]) == 0
    text = (output / "documentation-plan.md").read_text()
    assert element["authority"] in text and element["id"] in text
    metadata, _ = front_matter(starter.read_bytes())
    assert all(entry["summary"] == "" for entry in metadata["subjects"])
    before = starter.read_bytes()
    assert workflow.main(["plan", *common, "--starter", str(starter)]) == 2
    assert starter.read_bytes() == before

    git("add", ".")
    git("commit", "-m", "Explicit incomplete Markdown starter")
    empty_commit = git("rev-parse", "HEAD")
    assert workflow.main(["preview", *common, "--documentation-commit", empty_commit]) == 0
    old = json.loads((output / "documentation-audit.json").read_bytes())
    assert old["state"] == "FAIL" and old["checks"]["native"] == "not-performed"
    assert target_key(target) in {
        key for key, row in old["current"]["subjects"].items() if row["prose"]["entry"] is None
    }

    selected = next(entry for entry in metadata["subjects"] if entry["target"] == target)
    selected["summary"] = "Selected command explanation for this synthetic fixture."
    selected["body"] = "#"
    starter.write_text(
        "---\n" + yaml.safe_dump(metadata, sort_keys=False) + "---\n"
        "# Client\n\nExact independently authored body.\n"
    )
    git("add", ".")
    git("commit", "-m", "Author one selected command")
    authored_commit = git("rev-parse", "HEAD")
    assert authored_commit != empty_commit
    assert workflow.main(["preview", *common, "--documentation-commit", authored_commit]) == 0
    current = json.loads((output / "documentation-audit.json").read_bytes())
    assert current["current"]["identity"]["documentation_commit"] == authored_commit
    assert current["current"]["identity"]["source_sha"] is None
    assert current["current"]["subjects"][target_key(target)]["prose"]["entry"] is not None
    assert current["checks"]["native"] == "not-performed"
    assert current["destinations"] and all(
        row["status"] == "unverified" for row in current["destinations"]
    )
    assert current["state"] == "FAIL"  # Other required subjects are still incomplete.
    document = json.loads((output / "documentation-record.json").read_bytes())
    assert document["build_scope"] == "workspace"
    route = next(
        path
        for path, row in document["compiled"]["documents"].items()
        if row["id"] == metadata["id"]
    )
    assert route == "reference/client.md"
    page = next(output.glob("riverhog-v1/d-*.html")).read_text()
    assert "Exact independently authored body." in page
    assert "Documentation Audit" in (output / "riverhog-v1/documentation-audit.html").read_text()
    assert workflow.main(["check", *common, "--documentation-commit", authored_commit]) == 2
    assert "native extraction: not-performed" in capsys.readouterr().out.lower()
