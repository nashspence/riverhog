from __future__ import annotations

import hashlib
import re
from pathlib import Path

import pytest

from tests.workspace import workspace_pyprojects

REPO = Path(__file__).resolve().parents[2]
ENTRYPOINTS = {REPO / "README.md", REPO / "AGENTS.md"}
DURABLE_CONTEXT = {REPO / "docs/architecture.md"}
HAND_MAINTAINED_MARKDOWN = {
    REPO / "AGENTS.md",
    REPO / "LICENSE.md",
    REPO / "README.md",
    REPO / "SECURITY.md",
    REPO / "THIRD_PARTY_NOTICES.md",
    REPO / "docs/architecture.md",
}
REPOSITORY_MAP_TARGETS = {
    REPO / "riverhog",
    REPO / "some-implementations",
    REPO / "packages",
}
MARKDOWN_LINK_RE = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
IGNORED_TREES = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "build",
    "dist",
    "node_modules",
}


def _markdown_files() -> set[Path]:
    return {
        path.resolve()
        for path in REPO.rglob("*.md")
        if not IGNORED_TREES.intersection(path.relative_to(REPO).parts)
    }


def _local_links(path: Path) -> set[Path]:
    links: set[Path] = set()
    for target in MARKDOWN_LINK_RE.findall(path.read_text(encoding="utf-8")):
        if "://" in target or target.startswith("#"):
            continue
        relative = target.split("#", 1)[0].split("?", 1)[0]
        if not relative:
            continue
        links.add((path.parent / relative).resolve())
    return links


def test_all_markdown_is_reachable_and_links_resolve() -> None:
    markdown = _markdown_files()
    reachable: set[Path] = set()
    pending = list(ENTRYPOINTS)

    while pending:
        path = pending.pop()
        if path in reachable:
            continue
        assert path.is_file(), f"missing documentation target: {path.relative_to(REPO)}"
        reachable.add(path)
        for target in _local_links(path):
            assert target.exists(), (
                f"{path.relative_to(REPO)} links to missing {target.relative_to(REPO)}"
            )
            if target.suffix == ".md" and target not in reachable:
                pending.append(target)

    assert markdown == reachable


def test_hand_maintained_markdown_surface_is_explicit() -> None:
    assert {path for path in _markdown_files()} == HAND_MAINTAINED_MARKDOWN


def test_agents_guidance_stays_compact_and_routes_enforceable_policy_to_tests() -> None:
    agents = (REPO / "AGENTS.md").read_text(encoding="utf-8")
    assert len(agents.split()) <= 900
    assert "scoped policy tests" in agents
    assert "Enforceable repository policy belongs in clearly named tests." in " ".join(
        agents.split()
    )


def test_main_context_documents_are_exact_and_directly_routed() -> None:
    assert set((REPO / "docs").glob("*.md")) == DURABLE_CONTEXT

    for entrypoint in ENTRYPOINTS:
        links = [
            (entrypoint.parent / target.split("#", 1)[0]).resolve()
            for target in MARKDOWN_LINK_RE.findall(entrypoint.read_text(encoding="utf-8"))
            if "://" not in target and not target.startswith("#")
        ]
        direct_context = [link for link in links if link.parent == REPO / "docs"]
        assert len(direct_context) == len(DURABLE_CONTEXT)
        assert set(direct_context) == DURABLE_CONTEXT


@pytest.mark.parametrize(
    ("relative", "expected_sha256"),
    [
        # Maintainer-approved briefs: #957 README and #956 architecture (with navigation links).
        ("README.md", "c6c14c6940b91e43f07907ce229c4c49aa23c3ce182fbe062bf9cc694a373627"),
        (
            "docs/architecture.md",
            "c3be5f420d5306332067f2b1ae9ab516c4080a25db8d137205825e4eaab49dcf",
        ),
    ],
)
def test_maintained_context_document_requires_explicit_revision(
    relative: str, expected_sha256: str
) -> None:
    actual = hashlib.sha256((REPO / relative).read_bytes()).hexdigest()
    assert actual == expected_sha256, (
        f"Intentional edits to {relative} require an explicit update to its approved "
        f"SHA-256 in this test; actual SHA-256: {actual}"
    )


def test_readme_routes_make_target_documentation_to_make_help() -> None:
    readme = " ".join((REPO / "README.md").read_text(encoding="utf-8").split())

    assert "Use `make help` for development and validation commands." in readme
    assert re.findall(r"`make\s+([^`]+)`", readme) == ["help"]


def test_readme_routes_product_context_licensing_and_security_reporting() -> None:
    readme_path = REPO / "README.md"
    assert _local_links(readme_path) == {
        REPO / "LICENSE.md",
        REPO / "SECURITY.md",
        REPO / "docs/architecture.md",
    }
    assert "https://nashspence.github.io/riverhog/" in readme_path.read_text(encoding="utf-8")


def test_agents_requires_precommit_gates_and_exhaustive_integration_qualification() -> None:
    validation = (REPO / "AGENTS.md").read_text(encoding="utf-8").partition("## Validation\n")[2]

    assert re.findall(r"```bash\n(.*?)\n```", validation, flags=re.DOTALL) == [
        "make lint\nmake unit\nmake dist-smoke\nmake build"
    ]
    assert "`make linux-qualification`" in validation
    assert "substantial integration/handoff validation and release-related work" in validation
    assert "exhaustive local qualification rail" in validation


def test_agents_requires_post_push_github_validation() -> None:
    agents = " ".join((REPO / "AGENTS.md").read_text(encoding="utf-8").split())

    assert "watch the pushed commit's GitHub Actions checks through completion" in agents
    assert "Required GitHub checks are part of complete validation" in agents
    assert "`release.toml` owns the release-governance policy" in agents
    assert "Direct commits to `main`" in agents
    assert (
        "`release/v1` branch may synchronize to an explicitly approved green main commit" in agents
    )
    assert "Provider qualification stays disabled" in agents
    assert "never moves a v1 tag" in agents
    assert "Changes to root `README.md` or `docs/architecture.md` are exceptional" in agents


def test_agents_requires_locked_disposable_container_tool_stages() -> None:
    agents = " ".join((REPO / "AGENTS.md").read_text(encoding="utf-8").split())

    assert "mise install --locked" in agents
    assert "digest-pinned disposable build stage" in agents
    assert "copy only its required runtime artifacts forward" in agents


def test_repository_map_exactly_covers_the_workspace_layout() -> None:
    architecture_path = REPO / "docs/architecture.md"
    architecture = architecture_path.read_text(encoding="utf-8")
    section = architecture.partition("## How this maps to the repository\n")[2].partition("\n## ")[
        0
    ]
    targets = [
        (architecture_path.parent / target.split("#", 1)[0]).resolve()
        for target in MARKDOWN_LINK_RE.findall(section)
        if "://" not in target and not target.startswith("#")
    ]
    projects = {path.parent.resolve() for path in workspace_pyprojects(REPO)}

    assert len(targets) == len(REPOSITORY_MAP_TARGETS)
    assert set(targets) == REPOSITORY_MAP_TARGETS
    assert all(
        any(project == target or target in project.parents for project in projects)
        for target in targets
    )
    assert all(
        any(project == target or target in project.parents for target in targets)
        for project in projects
    )
