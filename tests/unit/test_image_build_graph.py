from __future__ import annotations

import re
import shlex
import tomllib
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
BAKE_FILE = REPO_ROOT / "docker-bake.hcl"
CI_FILE = REPO_ROOT / ".github/workflows/ci.yml"
SBOM_GENERATOR = (
    "docker.io/docker/buildkit-syft-scanner:stable-1@"
    "sha256:79e7b013cbec16bbb436f312819a49a4a57752b2270c1a9332ae1a10fcc82a68"
)
MISE_IMAGE = (
    "jdxcode/mise:2026.7.13@sha256:7fe9145156e33f95f7712632dbadb418592ed0f60fb46e5bcc8c113d372ad8a3"
)
MISE_CONTAINER_TOOLS = {
    "riverhog": {"uv"},
    "a-riverhog-ftp-spool": {"uv"},
    "a-riverhog-aws-store": {"uv"},
    "a-riverhog-b2-store": {"uv"},
    "a-riverhog-filesystem-store": {"uv"},
    "stove0": {"uv"},
    "a-stove0-exiftool-observer": {"http:exiftool", "uv"},
    "a-stove0-ffprobe-sampling-observer": {"uv"},
    "a-stove0-nvenc-av1-opus-target": {"uv"},
    "a-stove0-opus-target": {"uv"},
    "a-review0-materializer": {"uv"},
    "a-review0-rclone-target": {"rclone", "uv"},
    "a-riverhog-event-relay": {"uv"},
    "a-riverhog-minisign-witness": {"minisign", "uv"},
    "a-riverhog-opentimestamps-witness": {"uv"},
    "test": {"age", "http:exiftool", "minisign", "uv"},
}
NON_ROOT_RUNTIME_IMAGES = set(MISE_CONTAINER_TOOLS) - {"test"}

IMAGE_CONTRACTS = {
    "riverhog": {
        "dockerfile": "riverhog/Dockerfile",
        "tag": "riverhog-app:dev",
        "title": "Riverhog",
        "license": "CAL-1.0",
        "compose": (
            ("riverhog/compose.yaml", "state"),
            ("riverhog/compose.yaml", "app"),
        ),
    },
    "a-riverhog-ftp-spool": {
        "dockerfile": "some-implementations/riverhog/ingress/ftp/Dockerfile",
        "tag": "a-riverhog-ftp-spool:dev",
        "title": "Riverhog FTP spool",
        "license": "Apache-2.0",
        "compose": (("some-implementations/riverhog/ingress/ftp/compose.yaml", "ftp-spool"),),
    },
    "a-riverhog-aws-store": {
        "dockerfile": "some-implementations/riverhog/storage/aws/Dockerfile",
        "tag": "a-riverhog-aws-store:dev",
        "title": "Riverhog AWS store",
        "license": "CAL-1.0",
        "compose": (),
    },
    "a-riverhog-b2-store": {
        "dockerfile": "some-implementations/riverhog/storage/backblaze/Dockerfile",
        "tag": "a-riverhog-b2-store:dev",
        "title": "Riverhog B2 store",
        "license": "CAL-1.0",
        "compose": (),
    },
    "a-riverhog-filesystem-store": {
        "dockerfile": "some-implementations/riverhog/storage/filesystem/Dockerfile",
        "tag": "a-riverhog-filesystem-store:dev",
        "title": "Riverhog filesystem store",
        "license": "CAL-1.0",
        "compose": (("riverhog/compose.yaml", "filesystem-cache-adapter"),),
    },
    "stove0": {
        "dockerfile": "some-implementations/stove0/application/server/Dockerfile",
        "tag": "stove0:dev",
        "compose_target": "bundled-components",
        "title": "stove0",
        "license": "CAL-1.0",
        "compose": (
            ("some-implementations/stove0/application/compose.yaml", "state"),
            ("some-implementations/stove0/application/compose.yaml", "api"),
            ("some-implementations/stove0/application/compose.yaml", "controller"),
            ("some-implementations/stove0/application/compose.yaml", "worker"),
        ),
    },
    "a-stove0-exiftool-observer": {
        "dockerfile": "some-implementations/stove0/observers/exiftool/Dockerfile",
        "tag": "a-stove0-exiftool-observer:dev",
        "title": "stove0 ExifTool observer",
        "license": "CAL-1.0",
        "compose": (
            ("some-implementations/stove0/application/compose.yaml", "a-stove0-exiftool-observer"),
        ),
    },
    "a-stove0-ffprobe-sampling-observer": {
        "dockerfile": "some-implementations/stove0/observers/ffprobe-sampling/Dockerfile",
        "tag": "a-stove0-ffprobe-sampling-observer:dev",
        "title": "stove0 FFprobe sampling observer",
        "license": "CAL-1.0",
        "compose": (
            (
                "some-implementations/stove0/application/compose.yaml",
                "a-stove0-ffprobe-sampling-observer",
            ),
        ),
    },
    "a-stove0-nvenc-av1-opus-target": {
        "dockerfile": "some-implementations/stove0/targets/nvenc-av1-opus/Dockerfile",
        "tag": "a-stove0-nvenc-av1-opus-target:dev",
        "title": "stove0 NVENC AV1 + Opus target",
        "license": "CAL-1.0",
        "compose": (
            (
                "some-implementations/stove0/application/compose.yaml",
                "a-stove0-nvenc-av1-opus-target",
            ),
            (
                "some-implementations/stove0/application/compose.yaml",
                "a-review0-nvenc-av1-opus-sampler",
            ),
        ),
    },
    "a-stove0-opus-target": {
        "dockerfile": "some-implementations/stove0/targets/opus/Dockerfile",
        "tag": "a-stove0-opus-target:dev",
        "title": "stove0 Opus target",
        "license": "CAL-1.0",
        "compose": (
            ("some-implementations/stove0/application/compose.yaml", "a-stove0-opus-target"),
            ("some-implementations/stove0/application/compose.yaml", "a-review0-opus-sampler"),
        ),
    },
    "a-review0-materializer": {
        "dockerfile": "some-implementations/stove0/review0/materialize-target/Dockerfile",
        "tag": "a-review0-materializer:dev",
        "title": "Review0 materializer",
        "license": "CAL-1.0",
        "compose": (
            ("some-implementations/stove0/application/compose.yaml", "a-review0-materializer"),
        ),
    },
    "a-review0-rclone-target": {
        "dockerfile": "some-implementations/stove0/review0/rclone-effect-target/Dockerfile",
        "tag": "a-review0-rclone-target:dev",
        "title": "Review0 rclone target",
        "license": "CAL-1.0",
        "compose": (
            ("some-implementations/stove0/application/compose.yaml", "a-review0-rclone-target"),
        ),
    },
    "a-riverhog-event-relay": {
        "dockerfile": (
            "some-implementations/riverhog/applications/a-riverhog-event-relay/Dockerfile"
        ),
        "tag": "a-riverhog-event-relay:dev",
        "title": "Riverhog event relay",
        "license": "Apache-2.0",
        "compose": (),
    },
    "a-riverhog-minisign-witness": {
        "dockerfile": (
            "some-implementations/riverhog/applications/a-riverhog-minisign-witness/Dockerfile"
        ),
        "tag": "a-riverhog-minisign-witness:dev",
        "title": "Riverhog Minisign collection witness",
        "license": "Apache-2.0",
        "compose": (),
    },
    "a-riverhog-opentimestamps-witness": {
        "dockerfile": (
            "some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/Dockerfile"
        ),
        "tag": "a-riverhog-opentimestamps-witness:dev",
        "title": "Riverhog OpenTimestamps collection witness",
        "license": "Apache-2.0",
        "compose": (),
    },
    "test": {
        "dockerfile": "tests/Dockerfile",
        "tag": "riverhog-test:dev",
        "title": "Riverhog Test Suite",
        "license": "CAL-1.0 AND Apache-2.0",
        "compose": (("riverhog/compose.yaml", "test"),),
    },
}

PINNED_EXTERNAL_IMAGES = {
    "python:3.12-slim@sha256:090ba77e2958f6af52a5341f788b50b032dd4ca28377d2893dcf1ecbdfdfe203",
    MISE_IMAGE,
    "nvidia/cuda:13.0.0-devel-ubuntu24.04@sha256:1e8ac7a54c184a1af8ef2167f28fa98281892a835c981ebcddb1fad04bdd452d",
    "nvidia/cuda:13.0.0-runtime-ubuntu24.04@sha256:95318efecfd68ab3d109da5277863257b06137c84f34a87f38de970d5cd035d3",
}

PINNED_EXTERNAL_COMPOSE_IMAGES = {
    "alpine:3.22@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce",
    "dxflrs/garage:b72b090a097c8ee2711c8fb065d250ed68dcd0bf@sha256:f22f09abe741e54ab244e95638310e040b81eda41e2c6ab9b7373cda4b9e955c",
    "postgres:16-alpine@sha256:57c72fd2a128e416c7fcc499958864df5301e940bca0a56f58fddf30ffc07777",
}


def test_runtime_image_isolation_is_derived_for_every_runtime_image() -> None:
    workflow = yaml.safe_load(CI_FILE.read_text(encoding="utf-8"))
    steps = {step["name"]: step for step in workflow["jobs"]["images"]["steps"]}
    assert steps["Verify runtime image isolation"] == {
        "name": "Verify runtime image isolation",
        "if": "matrix.target != 'test'",
        "env": {"IMAGE_TARGET": "${{ matrix.target }}"},
        "run": 'python3 scripts/check_runtime_image.py "$IMAGE_TARGET"',
    }

    checker = (REPO_ROOT / "scripts/check_runtime_image.py").read_text(encoding="utf-8")
    assert "docker-bake.hcl" in checker
    assert "_workspace_dependency_closure" in checker
    assert "FORBIDDEN_RUNTIME_COMMANDS" in checker
    assert "locked_runtime_payloads" in checker
    assert "STANDALONE_ATTRIBUTION_PROGRAM" in checker


def _bake_graph() -> dict[str, object]:
    text = BAKE_FILE.read_text(encoding="utf-8")
    group_match = re.search(r'group "default" \{(?P<body>.*?)\n\}', text, re.DOTALL)
    assert group_match is not None
    group_targets_match = re.search(
        r"targets\s*=\s*\[(?P<targets>.*?)\]",
        group_match.group("body"),
        re.DOTALL,
    )
    assert group_targets_match is not None
    group_targets = re.findall(r'"([^"]+)"', group_targets_match.group("targets"))

    common_match = re.search(r'target "image-common" \{(?P<body>.*?)\n\}', text, re.DOTALL)
    assert common_match is not None
    common_body = common_match.group("body")
    platforms_match = re.search(r"platforms\s*=\s*\[(.*?)\]", common_body, re.DOTALL)
    epoch_match = re.search(r'SOURCE_DATE_EPOCH\s*=\s*"([^"]+)"', common_body)
    attest_match = re.search(r"attest\s*=\s*\[(.*?)\]", common_body, re.DOTALL)
    assert platforms_match is not None
    assert epoch_match is not None
    assert attest_match is not None

    targets: dict[str, dict[str, object]] = {}
    for match in re.finditer(
        r'target "(?P<name>[^"]+)" \{(?P<body>.*?)\n\}',
        text,
        re.DOTALL,
    ):
        if match.group("name") == "image-common":
            continue
        body = match.group("body")
        inherits_match = re.search(r"inherits\s*=\s*\[(.*?)\]", body)
        context_match = re.search(r'context\s*=\s*"([^"]+)"', body)
        dockerfile_match = re.search(r'dockerfile\s*=\s*"([^"]+)"', body)
        tags_match = re.search(r"tags\s*=\s*\[(.*?)\]", body)
        revision_match = re.search(r'SOURCE_REVISION\s*=\s*"([^"]+)"', body)
        assert context_match is not None
        assert dockerfile_match is not None
        assert tags_match is not None
        assert revision_match is not None
        assert inherits_match is not None
        targets[match.group("name")] = {
            "inherits": re.findall(r'"([^"]+)"', inherits_match.group(1)),
            "context": context_match.group(1),
            "dockerfile": dockerfile_match.group(1),
            "tags": re.findall(r'"([^"]+)"', tags_match.group(1)),
            "args": {"SOURCE_REVISION": revision_match.group(1)},
        }

    return {
        "group": {"default": {"targets": group_targets}},
        "common": {
            "platforms": re.findall(r'"([^"]+)"', platforms_match.group(1)),
            "args": {"SOURCE_DATE_EPOCH": epoch_match.group(1)},
            "attest": re.findall(r'"([^"]+)"', attest_match.group(1)),
        },
        "target": targets,
    }


def test_bake_graph_is_the_canonical_image_build_contract() -> None:
    graph = _bake_graph()
    release_images = tomllib.loads((REPO_ROOT / "release.toml").read_text(encoding="utf-8"))[
        "images"
    ]["runtime"]

    assert graph["group"] == {"default": {"targets": list(IMAGE_CONTRACTS)}}
    assert graph["common"] == {
        "platforms": ["linux/amd64"],
        "args": {"SOURCE_DATE_EPOCH": "0"},
        "attest": [f"type=sbom,generator={SBOM_GENERATOR}"],
    }
    assert set(graph["target"]) == set(IMAGE_CONTRACTS)

    for name, contract in IMAGE_CONTRACTS.items():
        target = graph["target"][name]
        assert target == {
            "inherits": ["image-common"],
            "context": ".",
            "dockerfile": contract["dockerfile"],
            "tags": [contract["tag"]],
            "args": {"SOURCE_REVISION": "unknown"},
        }
        dockerfile = (REPO_ROOT / contract["dockerfile"]).read_text(encoding="utf-8")
        assert "ARG SOURCE_DATE_EPOCH=0" in dockerfile
        assert "SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH}" in dockerfile
        assert "ARG BUILD_CREATED=1970-01-01T00:00:00Z" in dockerfile
        assert "ARG RELEASE_VERSION=development" in dockerfile
        assert "ARG SOURCE_REVISION=unknown" in dockerfile
        assert f'org.opencontainers.image.title="{contract["title"]}"' in dockerfile
        assert f'org.opencontainers.image.licenses="{contract["license"]}"' in dockerfile
        assert (
            'org.opencontainers.image.source="https://github.com/nashspence/riverhog"' in dockerfile
        )
        assert 'org.opencontainers.image.revision="${SOURCE_REVISION}"' in dockerfile
        assert 'org.opencontainers.image.version="${RELEASE_VERSION}"' in dockerfile
        assert 'org.opencontainers.image.created="${BUILD_CREATED}"' in dockerfile
        assert (
            'org.opencontainers.image.documentation="https://nashspence.github.io/riverhog/v1/"'
            in dockerfile
        )
        if name in release_images:
            release_image = release_images[name]
            assert (
                f'org.opencontainers.image.description="{release_image["description"]}"'
                in dockerfile
            )
            assert (
                f'io.github.nashspence.riverhog.release-role="{release_image["role"]}"'
                in dockerfile
            )


def test_commit_metadata_cannot_invalidate_stable_image_build_steps() -> None:
    for name, contract in IMAGE_CONTRACTS.items():
        dockerfile = (REPO_ROOT / contract["dockerfile"]).read_text(encoding="utf-8")
        stages = re.split(r"(?=^FROM )", dockerfile, flags=re.MULTILINE)
        for stage in stages:
            if "ARG BUILD_CREATED=" not in stage:
                continue
            lines = stage.splitlines()
            last_material_step = max(
                index
                for index, line in enumerate(lines)
                if line.startswith(("RUN ", "COPY ", "ADD "))
            )
            metadata_start = next(
                index for index, line in enumerate(lines) if line.startswith("ARG BUILD_CREATED=")
            )
            assert metadata_start > last_material_step, name
            assert any(
                'org.opencontainers.image.revision="${SOURCE_REVISION}"' in line
                for line in lines[metadata_start:]
            ), name
            assert any(
                'org.opencontainers.image.created="${BUILD_CREATED}"' in line
                for line in lines[metadata_start:]
            ), name

    for path in (
        REPO_ROOT / "Makefile",
        REPO_ROOT / ".github/workflows/ci.yml",
        REPO_ROOT / ".github/workflows/provider-qualification.yml",
        REPO_ROOT / "scripts/release.py",
    ):
        text = path.read_text(encoding="utf-8")
        assert re.search(r"args\.SOURCE_DATE_EPOCH=0", text), path
        assert not re.search(r"args\.SOURCE_DATE_EPOCH=(?!0(?:[\"'\\\n]|$))", text), path

    ci = (REPO_ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    provider = (REPO_ROOT / ".github/workflows/provider-qualification.yml").read_text(
        encoding="utf-8"
    )
    for workflow in (ci, provider):
        assert ",mode=max,ignore-error=true" in workflow


def test_every_external_image_input_is_versioned_and_digest_pinned() -> None:
    observed: set[str] = set()
    for contract in IMAGE_CONTRACTS.values():
        dockerfile = (REPO_ROOT / contract["dockerfile"]).read_text(encoding="utf-8")
        observed.update(
            reference
            for reference in re.findall(r"(?m)^FROM (\S+)", dockerfile)
            if "@sha256:" in reference
        )
        observed.update(
            reference
            for reference in re.findall(r"(?m)^COPY --from=(\S+)", dockerfile)
            if "/" in reference
        )

    assert observed == PINNED_EXTERNAL_IMAGES


def test_every_external_compose_image_is_versioned_and_digest_pinned() -> None:
    observed: set[str] = set()
    for compose_path in REPO_ROOT.rglob("compose.yaml"):
        compose = yaml.safe_load(compose_path.read_text(encoding="utf-8"))
        for service in compose.get("services", {}).values():
            if "build" not in service:
                observed.add(str(service["image"]))

    assert observed == PINNED_EXTERNAL_COMPOSE_IMAGES


def test_mise_container_tools_use_disposable_locked_build_stages() -> None:
    mise_config = tomllib.loads((REPO_ROOT / "mise.toml").read_text(encoding="utf-8"))
    container_tools = set().union(*MISE_CONTAINER_TOOLS.values())
    assert container_tools | {"python"} == set(mise_config["tools"])

    observed_images = set()
    for name, contract in IMAGE_CONTRACTS.items():
        dockerfile = (REPO_ROOT / contract["dockerfile"]).read_text(encoding="utf-8")
        if f"FROM {MISE_IMAGE} AS mise" not in dockerfile:
            continue
        observed_images.add(name)

        assert f"FROM {MISE_IMAGE} AS mise" in dockerfile
        assert "COPY --from=mise /usr/local/bin/mise /usr/local/bin/mise" in dockerfile
        assert "COPY mise.toml mise.lock ./" in dockerfile
        install_match = re.search(r"mise install --locked (?P<tools>[^\\\n]+)", dockerfile)
        assert install_match is not None
        assert set(shlex.split(install_match.group("tools"))) == MISE_CONTAINER_TOOLS[name]
        assert "COPY --from=locked-tools " in dockerfile

        final_stage = dockerfile.rsplit("\nFROM ", maxsplit=1)[1]
        assert "/usr/local/bin/mise" not in final_stage

    assert observed_images == set(IMAGE_CONTRACTS)


def test_mise_artifacts_match_each_image_role() -> None:
    for name, contract in IMAGE_CONTRACTS.items():
        dockerfile = (REPO_ROOT / contract["dockerfile"]).read_text(encoding="utf-8")
        assert '"$(mise which uv)" /opt/riverhog-tools/bin/uv' in dockerfile
        if name == "test":
            assert "COPY --from=locked-tools /opt/riverhog-tools/bin/ /usr/local/bin/" in dockerfile
        else:
            assert (
                "COPY --from=locked-tools /opt/riverhog-tools/bin/uv /usr/local/bin/uv"
                in dockerfile
            )

    test = (REPO_ROOT / IMAGE_CONTRACTS["test"]["dockerfile"]).read_text(encoding="utf-8")
    for binary in ("age", "age-keygen", "age-plugin-batchpass", "minisign", "uv"):
        assert f'"$(mise which {binary})" /opt/riverhog-tools/bin/{binary}' in test
    assert 'test "$(exiftool -ver)" = "13.59"' in test
    observer = (REPO_ROOT / IMAGE_CONTRACTS["a-stove0-exiftool-observer"]["dockerfile"]).read_text(
        encoding="utf-8"
    )
    assert 'test "$(exiftool -ver)" = "13.59"' in observer


def test_production_images_use_the_common_unprivileged_runtime_identity() -> None:
    for name in NON_ROOT_RUNTIME_IMAGES:
        contract = IMAGE_CONTRACTS[name]
        dockerfile = (REPO_ROOT / contract["dockerfile"]).read_text(encoding="utf-8")
        final_stage = dockerfile.rsplit("\nFROM ", maxsplit=1)[1]
        if "\nUSER 65532:65532\n" not in final_stage:
            assert final_stage.startswith("runtime-base AS ")
            runtime_base = dockerfile.split(" AS runtime-base", 1)[1].split("\nFROM ", 1)[0]
            assert "\nUSER 65532:65532\n" in runtime_base

    a_riverhog_event_relay = (
        REPO_ROOT / IMAGE_CONTRACTS["a-riverhog-event-relay"]["dockerfile"]
    ).read_text(encoding="utf-8")
    assert "install -d -o 65532 -g 65532 -m 0700 /state" in a_riverhog_event_relay

    riverhog = (REPO_ROOT / IMAGE_CONTRACTS["riverhog"]["dockerfile"]).read_text(encoding="utf-8")
    assert "HOME=/tmp" in riverhog

    compose = yaml.safe_load((REPO_ROOT / "riverhog/compose.yaml").read_text(encoding="utf-8"))
    for service_name in ("state", "app"):
        service = compose["services"][service_name]
        assert service["read_only"] is True
        assert service["tmpfs"] == ["/tmp:rw,noexec,nosuid,nodev,mode=700,uid=65532,gid=65532"]


def test_container_python_ownership_matches_the_supported_runtime_minor() -> None:
    mise_config = tomllib.loads((REPO_ROOT / "mise.toml").read_text(encoding="utf-8"))
    supported_minor = ".".join(mise_config["tools"]["python"].split(".")[:2])
    observed_python_bases: set[str] = set()

    for contract in IMAGE_CONTRACTS.values():
        dockerfile = (REPO_ROOT / contract["dockerfile"]).read_text(encoding="utf-8")
        observed_python_bases.update(re.findall(r"(?m)^FROM python:(\d+\.\d+)-slim@", dockerfile))

    assert observed_python_bases == {supported_minor}

    av1_dockerfile = (
        REPO_ROOT / IMAGE_CONTRACTS["a-stove0-nvenc-av1-opus-target"]["dockerfile"]
    ).read_text(encoding="utf-8")
    assert "assert sys.version_info[:2] == (3, 12)" in av1_dockerfile


def test_av1_source_builds_verify_the_exact_requested_commits() -> None:
    dockerfile = (
        REPO_ROOT / IMAGE_CONTRACTS["a-stove0-nvenc-av1-opus-target"]["dockerfile"]
    ).read_text(encoding="utf-8")

    assert 'test "$(git rev-parse HEAD)" = "${NV_CODEC_HEADERS_REF}"' in dockerfile
    assert 'test "$(git rev-parse HEAD)" = "${FFMPEG_REF}"' in dockerfile


def test_compose_build_services_match_the_canonical_bake_graph() -> None:
    graph = _bake_graph()
    release = tomllib.loads((REPO_ROOT / "release.toml").read_text(encoding="utf-8"))
    runtime_images = release["images"]["runtime"]

    for name, contract in IMAGE_CONTRACTS.items():
        target = graph["target"][name]
        target_context = (REPO_ROOT / target["context"]).resolve()
        target_dockerfile = (target_context / target["dockerfile"]).resolve()
        for compose_name, service_name in contract["compose"]:
            compose_path = REPO_ROOT / compose_name
            compose = yaml.safe_load(compose_path.read_text(encoding="utf-8"))
            service = compose["services"][service_name]
            build = service["build"]
            compose_context = (compose_path.parent / build["context"]).resolve()
            compose_dockerfile = (compose_context / build["dockerfile"]).resolve()

            assert compose_context == target_context
            assert compose_dockerfile == target_dockerfile
            local_tag = contract.get("compose_tag", contract["tag"])
            expected_image = (
                f"${{{name.upper().replace('-', '_')}_IMAGE_REF:-{local_tag}}}"
                if name in runtime_images
                else local_tag
            )
            assert service["image"] == expected_image
            assert build["args"] == {"SOURCE_REVISION": "${SOURCE_REVISION:-unknown}"}
            assert build.get("target") == contract.get("compose_target")


def test_stove0_supplied_validators_are_compose_composition_only() -> None:
    dockerfile = (REPO_ROOT / IMAGE_CONTRACTS["stove0"]["dockerfile"]).read_text(encoding="utf-8")
    generic_build, composition_and_runtime = dockerfile.split(
        "FROM build AS bundled-components-build", 1
    )
    composition_build, runtime_stages = composition_and_runtime.split("FROM python:3.12-slim@", 1)
    composition_runtime, generic_runtime = runtime_stages.split("FROM runtime-base AS runtime", 1)

    assert "some-implementations/stove0/observers/" not in generic_build
    assert "some-implementations/stove0/targets/" not in generic_build
    assert "--package stove0-server --no-dev --no-editable" in generic_build
    assert "a-stove0-media-metadata-contract-lib" not in generic_build
    assert "a-stove0-media-sampling-contract-lib" not in generic_build

    assert (
        "COPY some-implementations/stove0/observers/contracts/media-metadata "
        "some-implementations/stove0/observers/contracts/media-metadata"
    ) in composition_build
    assert (
        "COPY some-implementations/stove0/observers/contracts/media-sampling "
        "some-implementations/stove0/observers/contracts/media-sampling"
    ) in composition_build
    assert "--package a-stove0-media-metadata-contract-lib" in composition_build
    assert "--package a-stove0-media-sampling-contract-lib" in composition_build

    assert "FROM runtime-base AS bundled-components" in composition_runtime
    assert 'io.github.nashspence.riverhog.composition="bundled-components"' in composition_runtime
    assert 'io.github.nashspence.riverhog.release-role="product"' not in composition_runtime
    assert "COPY --from=bundled-components-build /opt/venv /opt/venv" in (composition_runtime)
    assert 'io.github.nashspence.riverhog.release-role="application"' in generic_runtime
    assert "COPY --from=build /opt/venv /opt/venv" in generic_runtime
    assert "bundled-components-build" not in generic_runtime


def test_github_image_matrix_uses_bounded_per_image_bake_caches() -> None:
    workflow = yaml.safe_load(CI_FILE.read_text(encoding="utf-8"))
    assert workflow["permissions"] == {"contents": "read"}

    job = workflow["jobs"]["images"]
    assert job["strategy"]["matrix"] == {"target": list(IMAGE_CONTRACTS)}
    assert job["env"] == {"DOCKER_BUILD_RECORD_UPLOAD": "false"}
    steps = {step["name"]: step for step in job["steps"]}
    assert steps["Configure Docker Buildx"] == {
        "name": "Configure Docker Buildx",
        "uses": "docker/setup-buildx-action@bb05f3f5519dd87d3ba754cc423b652a5edd6d2c",
        "with": {"version": "v0.36.0", "driver": "docker"},
    }
    assert steps["Install Riverhog event relay smoke toolchain"] == {
        "name": "Install Riverhog event relay smoke toolchain",
        "if": "matrix.target == 'a-riverhog-event-relay'",
        "uses": "jdx/mise-action@9e7f7633ff6f6d6048a9418a68d48f288f50eb14",
        "with": {"install_args": "python"},
    }
    assert steps["Resolve image metadata"] == {
        "name": "Resolve image metadata",
        "id": "image-metadata",
        "run": 'echo "created=$(git show -s --format=%cI HEAD)" >> "$GITHUB_OUTPUT"\n',
    }
    assert steps["Build image"] == {
        "name": "Build image",
        "uses": "docker/bake-action@d3418bd7d0e9324001bca92fa8ba175ea7e6dc9b",
        "with": {
            "source": ".",
            "files": "docker-bake.hcl",
            "targets": "${{ matrix.target }}",
            "load": True,
            "set": (
                "*.args.SOURCE_REVISION=${{ inputs.ref || github.sha }}\n"
                "*.args.BUILD_CREATED=${{ steps.image-metadata.outputs.created }}\n"
                "*.args.SOURCE_DATE_EPOCH=0\n"
                "*.args.RELEASE_VERSION=development\n"
                "*.cache-from=type=gha,scope=${{ matrix.target }}\n"
                "*.cache-to=type=gha,scope=${{ matrix.target }},mode=max,ignore-error=true\n"
            ),
        },
    }
    assert steps["Smoke Riverhog event relay image"] == {
        "name": "Smoke Riverhog event relay image",
        "if": "matrix.target == 'a-riverhog-event-relay'",
        "run": "make a-riverhog-event-relay-smoke",
    }
