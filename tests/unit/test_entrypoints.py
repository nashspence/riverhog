from __future__ import annotations

import hashlib
import re
from pathlib import Path

import pytest

from tests.workspace import workspace_pyprojects

REPO = Path(__file__).resolve().parents[2]
ENTRYPOINTS = {REPO / "README.md", REPO / "AGENTS.md"}
HAND_MAINTAINED_MARKDOWN = {
    REPO / "AGENTS.md",
    REPO / "LICENSE.md",
    REPO / "README.md",
    REPO / "SECURITY.md",
    REPO / "THIRD_PARTY_NOTICES.md",
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


def _guide_section(heading: str) -> str:
    text = (REPO / "AGENTS.md").read_text(encoding="utf-8")
    _, separator, section = text.partition(f"## {heading}\n")
    assert separator, f"missing developer-guide section: {heading}"
    return " ".join(section.partition("\n## ")[0].split())


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
    assert _markdown_files() == HAND_MAINTAINED_MARKDOWN
    assert not (REPO / "docs").exists()


def test_agents_guidance_stays_compact_and_routes_enforceable_policy_to_tests() -> None:
    agents = (REPO / "AGENTS.md").read_text(encoding="utf-8")
    assert len(agents.split()) <= 2100
    assert "scoped policy tests" in agents
    assert "Enforceable repository policy belongs in clearly named tests." in " ".join(
        agents.split()
    )


def test_onboarding_routes_directly_between_the_two_root_documents() -> None:
    for entrypoint in ENTRYPOINTS:
        targets = MARKDOWN_LINK_RE.findall(entrypoint.read_text(encoding="utf-8"))
        other = "AGENTS.md" if entrypoint.name == "README.md" else "README.md"
        assert targets.count(other) == 1
        assert REPO / other in _local_links(entrypoint)


@pytest.mark.parametrize(
    ("relative", "expected_sha256"),
    [
        ("README.md", "8453410441ad4ad1bf7c9bd86f53105546fce8d91fe211cebefba8e783d5f1a2"),
        ("AGENTS.md", "8f79807cd6c515dd7e6a6414d443fd5f509ed6c0ee935b3d9cd1493c1f6772b3"),
    ],
)
def test_onboarding_document_requires_explicit_revision(
    relative: str, expected_sha256: str
) -> None:
    actual = hashlib.sha256((REPO / relative).read_bytes()).hexdigest()
    assert actual == expected_sha256, (
        f"Intentional edits to {relative} require an explicit update to its approved "
        f"SHA-256 in this test; actual SHA-256: {actual}"
    )


def test_developer_guide_routes_make_target_documentation_to_make_help() -> None:
    validation = _guide_section("Validate the change you actually deliver")
    assert "Use `make help` for current command guidance." in validation


def test_readme_routes_product_context_licensing_and_security_reporting() -> None:
    readme_path = REPO / "README.md"
    assert _local_links(readme_path) == {
        REPO / "LICENSE.md",
        REPO / "SECURITY.md",
        REPO / "AGENTS.md",
        REPO / "riverhog",
        REPO / "packages",
        REPO / "some-implementations",
        REPO / "some-implementations/riverhog/recovery",
    }
    assert "https://nashspence.github.io/riverhog/" in readme_path.read_text(encoding="utf-8")


def test_agents_requires_main_gates_and_pr_checkpoint_qualification() -> None:
    agents = (REPO / "AGENTS.md").read_text(encoding="utf-8")
    validation = _guide_section("Validate the change you actually deliver")

    assert re.findall(r"```bash\n(.*?)\n```", agents, flags=re.DOTALL) == [
        "make lint\nmake unit\nmake dist-smoke\nmake build"
    ]
    assert "`make linux-qualification`" in validation
    assert "Before pushing directly to `main`, run:" in validation
    assert (
        "run `make lint` and focused tests of the changed owners before checkpoint pushes"
        in validation
    )
    assert "Use the pull request’s required CI suites for complete qualification." in validation
    assert "Work may continue while CI runs" in validation
    assert "cancel superseded runs as needed" in validation
    assert "Before merging or handing work back, require complete passing checks" in validation
    assert "exact final source and its tested merge checkout" in validation
    assert "When exhaustive local qualification is needed" in validation


def test_agents_requires_post_push_github_validation() -> None:
    validation = _guide_section("Validate the change you actually deliver")
    assert (
        "watch that exact commit’s required GitHub Actions checks through completion" in validation
    )
    assert "Fix failures before handing work back." in validation
    assert "A skipped or blocked check is not a pass" in validation
    assert "Do not remove required coverage or weaken a check" in validation


def test_agents_keeps_release_authorization_separate_from_validation() -> None:
    authorization = _guide_section("Keep authorization separate from validation")
    links = _local_links(REPO / "AGENTS.md")
    assert {REPO / "release.toml", REPO / ".github/workflows"} <= links
    assert "Main remains the pre-v1 development authority" in authorization
    assert "required checks for delivered changes" in authorization
    assert "Maintainers may commit directly or use pull requests" in authorization
    assert "validate integration branches before merging" in authorization
    assert "explicit approval of a fully green commit" in authorization
    assert "This does not select a *release candidate*" in authorization
    assert "Provider qualification remains disabled until separately authorized" in authorization
    assert (
        "Publication requires protected approval and a complete governance check" in authorization
    )
    assert "Published v1 tags are never moved or deleted." in authorization


def test_onboarding_documents_require_explicit_maintainer_direction_for_broader_edits() -> None:
    guidance = _guide_section("Start from the source that owns the answer")
    assert (
        "Change `README.md` or `AGENTS.md` only to correct material drift or when the maintainer "
        "explicitly directs broader work."
    ) in guidance


def test_agents_routes_locked_container_tool_policy_to_its_owning_tests() -> None:
    links = _local_links(REPO / "AGENTS.md")
    assert {
        REPO / "tests/unit/test_dependency_policy.py",
        REPO / "tests/unit/test_image_build_graph.py",
    } <= links


def test_agents_preserves_supported_database_upgrades_and_durable_state() -> None:
    state = _guide_section("Change durable state deliberately")
    assert "upgrading databases between supported releases" in state
    assert "existing data survives" in state
    assert "interrupted upgrades" in state
    assert "does not silently upgrade or erase existing data" in state
    assert REPO / "packages/state-schema" in _local_links(REPO / "AGENTS.md")


def test_agents_requires_growth_validation_without_machine_timing_guarantees() -> None:
    growth = _guide_section("Preserve declared growth bounds")
    assert "*linear growth*" in growth
    assert "Violating a declared growth bound is a validation failure." in growth
    assert "Machine-dependent timing targets remain performance observations" in growth
    assert "not product guarantees" in growth


def test_agents_separates_required_validation_evidence_from_advisory_performance() -> None:
    measurement = _guide_section("Measure performance without creating gates")
    evidence = _guide_section("Account for validation evidence")
    handoff = _guide_section("Validate the change you actually deliver")
    assert "a nonbinding goal" in measurement
    assert "Missing a target does not fail validation." in measurement
    assert "required evidence fail validation" in evidence
    assert "A valid timing miss does not." in evidence
    assert "not validation failures or approval conditions" in handoff
    assert REPO / "scripts/ci_qualification.py" in _local_links(REPO / "AGENTS.md")


def test_repository_map_exactly_covers_the_workspace_layout() -> None:
    readme_path = REPO / "README.md"
    readme = readme_path.read_text(encoding="utf-8")
    section = readme.partition("## How this maps to the repository\n")[2].partition("\n## ")[0]
    targets = [
        (readme_path.parent / target.split("#", 1)[0]).resolve()
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
