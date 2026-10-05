"""Shared portable qualification targets and coarse Actions execution plan."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast

import yaml
from riverhog_canonical_json import canonical_json_bytes

from scripts.ci_unit import SHARDS

ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_TARGETS = (
    "lint",
    "compile",
    "c2sp-vectors",
    "postgres-concurrency",
    "filesystem-recovery-qualification",
    "dist-smoke",
)
DOCKER_TARGETS = {"postgres-concurrency", "filesystem-recovery-qualification"}
COMPOSE_LANES = ("storage", "processing", "review-delivery", "witnesses")
IMAGE_GROUPS = ("core", "observers", "targets", "companions")
NATIVE_TESTS = (
    "tests/platform/test_native_provenance.py",
    "some-implementations/gogurt/application/tests",
    "tests/platform/test_end_user_artifacts.py",
)


def run(command: Sequence[str], *, env: dict[str, str] | None = None) -> None:
    subprocess.run(command, cwd=ROOT, env=env, check=True)


def bake_graph() -> dict[str, Any]:
    return cast(
        dict[str, Any],
        json.loads(
            subprocess.check_output(
                ["docker", "buildx", "bake", "--file", "docker-bake.hcl", "--print"], cwd=ROOT
            )
        ),
    )


def image_groups(graph: dict[str, Any]) -> dict[str, list[str]]:
    groups: dict[str, list[str]] = {name: [] for name in IMAGE_GROUPS}
    targets = graph["group"]["default"]["targets"]
    if not targets or len(targets) != len(set(targets)):
        raise ValueError("default Bake targets must be nonempty and unique")
    for name in targets:
        dockerfile = graph["target"][name]["dockerfile"]
        if "/observers/" in dockerfile:
            group = "observers"
        elif "/targets/" in dockerfile or name == "review0":
            group = "targets"
        elif "/storage/" in dockerfile or "/riverhog/applications/" in dockerfile:
            group = "companions"
        elif name in {"riverhog", "stove0", "test"} or "/ingress/" in dockerfile:
            group = "core"
        else:
            raise ValueError(f"Bake target has no qualification group: {name}")
        groups[group].append(name)
    if not all(groups.values()):
        raise ValueError("every coarse image group must own a target")
    return groups


def build_settings(targets: Sequence[str], *, compose: bool = False) -> list[str]:
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    created = subprocess.check_output(
        ["git", "show", "-s", "--format=%cI", "HEAD"], cwd=ROOT, text=True
    ).strip()
    settings = [
        f"*.args.SOURCE_REVISION={revision}",
        f"*.args.BUILD_CREATED={created}",
        "*.args.SOURCE_DATE_EPOCH=0",
        "*.args.RELEASE_VERSION=development",
    ]
    if compose and "stove0" in targets:
        settings.append("stove0.target=bundled-components")
    return settings


def cache_settings(targets: Sequence[str], *, compose: bool = False) -> list[str]:
    settings: list[str] = []
    for target in targets:
        scope = "stove0-compose" if compose and target == "stove0" else target
        settings.extend(
            (
                f"{target}.cache-from=type=gha,scope={scope}",
                f"{target}.cache-to=type=gha,scope={scope},mode=max,ignore-error=true",
            )
        )
    return settings


def compose_targets(lane: str, graph: dict[str, Any]) -> list[str]:
    if lane not in (*COMPOSE_LANES, "all"):
        raise ValueError(f"unknown Compose qualification: {lane}")
    files: list[tuple[str, set[str] | None]] = [
        ("riverhog/compose.yaml", {"test", "app", "filesystem-cache-adapter"})
    ]
    if lane in {"all", "processing", "review-delivery"}:
        files.append(("some-implementations/stove0/application/compose.yaml", None))
    if lane in {"all", "processing"}:
        files.append(("some-implementations/riverhog/ingress/ftp/compose.yaml", None))
    if lane in {"all", "witnesses"}:
        files.extend(
            (f"some-implementations/riverhog/applications/{name}/compose.yaml", None)
            for name in ("a-riverhog-minisign-witness", "a-riverhog-opentimestamps-witness")
        )
    by_dockerfile = {value["dockerfile"]: name for name, value in graph["target"].items()}
    selected: set[str] = set()
    for filename, services in files:
        document = yaml.safe_load((ROOT / filename).read_text())
        for service, definition in document["services"].items():
            if services is not None and service not in services:
                continue
            # GPU runtime qualification remains outside this CPU-only lifecycle.
            if definition.get("gpus"):
                continue
            build = definition.get("build")
            if build is None:
                continue
            dockerfile = build["dockerfile"]
            if dockerfile not in by_dockerfile:
                raise ValueError(
                    f"Compose build has no canonical Bake target: {filename}:{service}"
                )
            selected.add(by_dockerfile[dockerfile])
    return sorted(selected)


def verify_images(targets: Sequence[str], graph: dict[str, Any], *, compose: bool = False) -> None:
    settings = build_settings(targets, compose=compose)
    revision = settings[0].split("=", 1)[1]
    created = settings[1].split("=", 1)[1]
    for target in targets:
        tags = graph["target"][target]["tags"]
        if len(tags) != 1:
            raise ValueError(f"qualification requires one exact local image for {target}")
        inspected = json.loads(
            subprocess.check_output(["docker", "image", "inspect", tags[0]], cwd=ROOT)
        )[0]
        labels = inspected["Config"].get("Labels") or {}
        required = {
            "org.opencontainers.image.revision": revision,
            "org.opencontainers.image.created": created,
            "org.opencontainers.image.version": "development",
        }
        if compose and target == "stove0":
            required["io.github.nashspence.riverhog.composition"] = "bundled-components"
        if any(labels.get(key) != value for key, value in required.items()):
            raise ValueError(f"qualification image does not match source/config inputs: {target}")
        if (
            not compose
            and target == "stove0"
            and labels.get("io.github.nashspence.riverhog.composition")
        ):
            raise ValueError("standalone Stove0 qualification cannot use the composed image")
        print(f"Verified qualification image {target}: {inspected['Id']}")


def prepare_images(targets: Sequence[str], graph: dict[str, Any], *, compose: bool = False) -> None:
    if os.environ.get("RIVERHOG_CI_IMAGES_PREBUILT") != "1":
        command = ["docker", "buildx", "bake", "--file", "docker-bake.hcl", "--load"]
        for setting in build_settings(targets, compose=compose):
            command.extend(("--set", setting))
        run([*command, *targets])
    verify_images(targets, graph, compose=compose)


def qualify_images(targets: Sequence[str], graph: dict[str, Any]) -> None:
    prepare_images(targets, graph)
    for target in targets:
        if target == "a-riverhog-event-relay":
            run(["make", "a-riverhog-event-relay-smoke"])
        if target in {"a-riverhog-minisign-witness", "a-riverhog-opentimestamps-witness"}:
            run([sys.executable, "scripts/test_witness_image.py", target])
        if target.startswith("a-riverhog-") and target != "a-riverhog-ftp-spool":
            run([sys.executable, "scripts/test_runtime_compose.py", target])
        if target != "test":
            run([sys.executable, "scripts/check_runtime_image.py", target])


def native_platform() -> None:
    run([sys.executable, "-m", "pytest", "-q", *NATIVE_TESTS])
    command = [
        sys.executable,
        "scripts/qualify_installation.py",
        "--version",
        "1.0.0",
        "--listener-lifecycle",
        "--listener-lifecycle-repetitions",
        "12",
    ]
    evidence = os.environ.get("GOGURT_FAILURE_EVIDENCE_DIR")
    if evidence:
        command.extend(("--gogurt-evidence-dir", evidence))
    fixture = os.environ.get("RIVERHOG_QUALIFICATION_MOUNT_FIXTURE")
    if fixture:
        command.extend(("--listener-mount-fixture", fixture))
    lock = os.environ.get("RIVERHOG_QUALIFICATION_MOUNT_LOCK")
    if lock:
        command.extend(("--listener-mount-lock", lock))
    run(command)


def plan(graph: dict[str, Any]) -> dict[str, object]:
    groups = image_groups(graph)
    return {
        "repository": {
            "include": [
                {"target": target, "docker": target in DOCKER_TARGETS}
                for target in REPOSITORY_TARGETS
            ]
        },
        "units": {"shard": list(SHARDS)},
        "images": {
            "include": [
                {
                    "group": group,
                    "targets": ",".join(targets),
                    "cache_settings": "\n".join(cache_settings(targets)),
                }
                for group, targets in groups.items()
            ]
        },
        "compose": {
            "include": [
                {
                    "lane": lane,
                    "targets": ",".join(compose_targets(lane, graph)),
                    "cache_settings": "\n".join(
                        cache_settings(compose_targets(lane, graph), compose=True)
                    ),
                    "stage_settings": (
                        "stove0.target=bundled-components"
                        if "stove0" in compose_targets(lane, graph)
                        else ""
                    ),
                }
                for lane in COMPOSE_LANES
            ]
        },
    }


def linux_qualification() -> None:
    if not sys.platform.startswith("linux"):
        raise ValueError("portable Linux qualification must run on Linux")
    run(["make", "client-platform-qualification"])
    for target in REPOSITORY_TARGETS:
        run(["make", target])
    run(["make", "unit-shards-check"])
    for shard in SHARDS:
        run(["make", "unit-shard", f"UNIT_SHARD={shard}"])
    for group in IMAGE_GROUPS:
        run(["make", "image-qualification", f"IMAGE_GROUP={group}"])
    for lane in COMPOSE_LANES:
        run(["make", "compose-shard", f"COMPOSE_LANE={lane}"])


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("plan", "images", "compose-prepare", "native", "linux"))
    parser.add_argument("--group", choices=IMAGE_GROUPS)
    parser.add_argument("--lane", choices=(*COMPOSE_LANES, "all"))
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "native":
            native_platform()
        elif args.command == "linux":
            linux_qualification()
        else:
            graph = bake_graph()
            if args.command == "images":
                if args.group is None:
                    parser.error("images requires --group")
                qualify_images(image_groups(graph)[args.group], graph)
            elif args.command == "compose-prepare":
                if args.lane is None:
                    parser.error("compose-prepare requires --lane")
                prepare_images(compose_targets(args.lane, graph), graph, compose=True)
            else:
                observed = plan(graph)
                if args.github_output is not None:
                    with args.github_output.open("a", encoding="utf-8") as output:
                        for key, value in observed.items():
                            output.write(f"{key}={canonical_json_bytes(value).decode()}\n")
                else:
                    print(canonical_json_bytes(observed).decode())
    except (ValueError, subprocess.CalledProcessError) as exc:
        print(f"qualification failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
