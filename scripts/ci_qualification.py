"""Shared portable qualification targets and coarse Actions execution plan."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from collections.abc import Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, cast
from uuid import uuid4

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
COMPOSE_LANES = (
    "storage",
    "ingress-custody",
    "processing-admission",
    "processing-e2e",
    "processing-overlap",
    "review-delivery",
    "witnesses",
)
PROCESSING_LANES = frozenset(
    {"processing-admission", "processing-e2e", "processing-overlap", "processing-scale"}
)
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
    if lane not in (*COMPOSE_LANES, "processing-scale", "all"):
        raise ValueError(f"unknown Compose qualification: {lane}")
    files: list[tuple[str, set[str] | None]] = [
        ("riverhog/compose.yaml", {"test", "app", "filesystem-cache-adapter"})
    ]
    if lane in {"all", "review-delivery"}:
        files.append(("some-implementations/stove0/application/compose.yaml", None))
    elif lane in PROCESSING_LANES:
        files.append(
            (
                "some-implementations/stove0/application/compose.yaml",
                {
                    "state",
                    "api",
                    "controller",
                    "worker",
                    "a-stove0-ffprobe-observer",
                    "a-stove0-magic-observer",
                    "a-stove0-filename-prefix-sidecar-observer",
                    "a-stove0-riverhog-provenance-observer",
                    "a-stove0-exiftool-observer",
                    "a-stove0-opus-target",
                },
            )
        )
    if lane in PROCESSING_LANES or lane in {"all", "ingress-custody"}:
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


def independent_proofs(
    commands: Sequence[Sequence[str]],
    *,
    jobs: int,
    docker_jobs: int | None = None,
    images_prepared: bool = False,
) -> None:
    """Bound independent work without occupying worker slots on resource waits.

    Compose images must be prepared before this pool. Each scenario owns its
    project, ephemeral host port, private scratch and persistent fixture state.
    Every started proof is joined through its own cleanup when any proof fails.
    """
    docker_jobs = jobs if docker_jobs is None else docker_jobs
    if min(jobs, docker_jobs) < 1:
        raise ValueError("local qualification jobs and Docker jobs must be positive")
    if not images_prepared and any(command[1] == "compose-shard" for command in commands):
        raise ValueError("Compose proofs require the prepared image barrier")

    def docker_heavy(command: Sequence[str]) -> bool:
        return command[1] in DOCKER_TARGETS | {"compose-shard"}

    def proof(command: Sequence[str]) -> None:
        lane = command[1]
        if lane == "unit-shard":
            lane = "unit-" + command[2].removeprefix("UNIT_SHARD=")
        if lane != "compose-shard":
            run(command, env={**os.environ, "RIVERHOG_CI_LANE": lane})
            return
        lane = command[2].removeprefix("COMPOSE_LANE=")
        project = f"riverhog-proof-{lane}-{uuid4().hex[:12]}"
        with TemporaryDirectory(prefix=f"riverhog-qualification-{lane}-") as temporary:
            run(
                command,
                env={
                    **os.environ,
                    "RIVERHOG_CI_LANE": "compose-" + lane,
                    "RIVERHOG_CI_IMAGES_PREBUILT": "1",
                    "COMPOSE_PROJECT_NAME": project,
                    "TEST_COMPOSE_PROJECT_NAME": project,
                    "RIVERHOG_API_PORT": "0",
                    "TMPDIR": temporary,
                },
            )

    pending = list(commands)
    with ThreadPoolExecutor(max_workers=jobs) as executor:
        running: dict[Future[None], Sequence[str]] = {}
        try:
            while pending or running:
                while pending and len(running) < jobs:
                    active_docker = sum(docker_heavy(command) for command in running.values())
                    eligible = next(
                        (
                            index
                            for index, command in enumerate(pending)
                            if not docker_heavy(command) or active_docker < docker_jobs
                        ),
                        None,
                    )
                    if eligible is None:
                        break
                    command = pending.pop(eligible)
                    running[executor.submit(proof, command)] = command
                completed, _ = wait(running, return_when=FIRST_COMPLETED)
                for future in completed:
                    del running[future]
                    future.result()
        except BaseException:
            for future in running:
                future.cancel()
            raise


def compose_qualification(*, jobs: int = 2, docker_jobs: int = 2) -> None:
    if min(jobs, docker_jobs) < 1:
        raise ValueError("local qualification jobs and Docker jobs must be positive")
    graph = bake_graph()
    prepare_images(compose_targets("all", graph), graph, compose=True)
    independent_proofs(
        [["make", "compose-shard", f"COMPOSE_LANE={lane}"] for lane in COMPOSE_LANES],
        jobs=jobs,
        docker_jobs=docker_jobs,
        images_prepared=True,
    )


def linux_qualification(*, jobs: int = 2, docker_jobs: int = 2) -> None:
    if not sys.platform.startswith("linux"):
        raise ValueError("portable Linux qualification must run on Linux")
    if min(jobs, docker_jobs) < 1:
        raise ValueError("local qualification jobs and Docker jobs must be positive")
    run(["make", "client-platform-qualification"])
    for target in REPOSITORY_TARGETS:
        run(["make", target])
    run(["make", "unit-shards-check"])
    for group in IMAGE_GROUPS:
        run(["make", "image-qualification", f"IMAGE_GROUP={group}"])
    # Standalone image qualification owns mutable development tags. Build and
    # verify the composed variant before any independent lifecycle is started.
    graph = bake_graph()
    prepare_images(compose_targets("all", graph), graph, compose=True)
    independent_proofs(
        [
            *(["make", "unit-shard", f"UNIT_SHARD={shard}"] for shard in SHARDS),
            *(["make", "compose-shard", f"COMPOSE_LANE={lane}"] for lane in COMPOSE_LANES),
        ],
        jobs=jobs,
        docker_jobs=docker_jobs,
        images_prepared=True,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=("plan", "images", "compose-prepare", "compose-all", "native", "linux")
    )
    parser.add_argument("--group", choices=IMAGE_GROUPS)
    parser.add_argument("--lane", choices=(*COMPOSE_LANES, "processing-scale", "all"))
    parser.add_argument("--jobs", type=int, default=2)
    parser.add_argument("--docker-jobs", type=int, default=2)
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "native":
            native_platform()
        elif args.command == "linux":
            linux_qualification(jobs=args.jobs, docker_jobs=args.docker_jobs)
        elif args.command == "compose-all":
            compose_qualification(jobs=args.jobs, docker_jobs=args.docker_jobs)
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
