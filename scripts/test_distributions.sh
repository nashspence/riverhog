#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST_DIR="${DIST_DIR:-${ROOT_DIR}/dist}"
MISE_BIN="${MISE_BIN:-mise}"
SCRATCH="$(mktemp -d "${TMPDIR:-/tmp}/riverhog-dist-smoke.XXXXXX")"
trap 'rm -rf "${SCRATCH}"' EXIT

run_uv() {
  "${MISE_BIN}" x -- uv --no-config "$@"
}

workspace_wheel_closure() {
  local root_wheel="$1"
  "${MISE_BIN}" x -- uv run --locked --all-packages --group dev \
    python -I - "${DIST_DIR}" "${root_wheel}" <<'PY'
import re
import sys
import zipfile
from email.parser import BytesParser
from pathlib import Path

from packaging.markers import default_environment
from packaging.requirements import Requirement


def canonical_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).lower()


packages = {}
for wheel in sorted(Path(sys.argv[1]).glob("*.whl")):
    with zipfile.ZipFile(wheel) as archive:
        metadata_path = next(
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        )
        metadata = BytesParser().parsebytes(archive.read(metadata_path))
    packages[canonical_name(metadata["Name"])] = (wheel, metadata)

pending = [Path(sys.argv[2])]
selected = {}
while pending:
    wheel = pending.pop()
    with zipfile.ZipFile(wheel) as archive:
        metadata_path = next(
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        )
        metadata = BytesParser().parsebytes(archive.read(metadata_path))
    name = canonical_name(metadata["Name"])
    if name in selected:
        continue
    selected[name] = wheel
    for raw_requirement in metadata.get_all("Requires-Dist", []):
        requirement = Requirement(raw_requirement)
        if requirement.marker is not None and not requirement.marker.evaluate(
            environment=default_environment()
        ):
            continue
        dependency = canonical_name(requirement.name)
        if dependency in packages:
            pending.append(packages[dependency][0])

for wheel in sorted(selected.values()):
    print(wheel)
PY
}

single_wheel() {
  local pattern="$1"
  local wheels=("${DIST_DIR}"/${pattern})
  if [[ "${#wheels[@]}" -ne 1 || ! -f "${wheels[0]}" ]]; then
    printf 'expected one wheel matching %s; found %s\n' "${pattern}" "${#wheels[@]}" >&2
    return 1
  fi
  printf '%s' "${wheels[0]}"
}

smoke_workspace_distribution() {
  local name="$1"
  local pattern="$2"
  local import_check="$3"
  shift 3
  local wheel
  wheel="$(single_wheel "${pattern}")"
  run_uv venv --python 3.12 "${SCRATCH}/${name}"
  mapfile -t wheels < <(
    workspace_wheel_closure "${wheel}"
  )
  run_uv pip install \
    --strict \
    --python "${SCRATCH}/${name}/bin/python" \
    --find-links "${DIST_DIR}" \
    "${wheels[@]}"
  (
    cd "${SCRATCH}"
    env -u PYTHONPATH "${SCRATCH}/${name}/bin/python" -I -c "${import_check}"
    for executable in "$@"; do
      env -u PYTHONPATH "${SCRATCH}/${name}/bin/${executable}" --help >/dev/null
    done
  )
}

assert_installed_cli_version() {
  local environment="$1"
  local executable="$2"
  local distribution="$3"
  local reported_version
  local installed_version
  reported_version="$(env -u PYTHONPATH "${SCRATCH}/${environment}/bin/${executable}" --version)"
  installed_version="$(
    "${SCRATCH}/${environment}/bin/python" -I -c \
      "import importlib.metadata as m; print(m.version('${distribution}'))"
  )"
  "${SCRATCH}/${environment}/bin/python" -I -c \
    'import re, sys; assert re.search(rf"(?<![0-9A-Za-z.]){re.escape(sys.argv[2])}(?![0-9A-Za-z.])", sys.argv[1])' \
    "${reported_version}" "${installed_version}"
}

assert_cli_usage_failure() {
  local environment="$1"
  shift
  set +e
  env -u PYTHONPATH "${SCRATCH}/${environment}/bin/$1" "${@:2}" \
    >"${SCRATCH}/${environment}-usage.stdout" \
    2>"${SCRATCH}/${environment}-usage.stderr"
  local status=$?
  set -e
  [[ "${status}" -eq 2 ]]
  [[ ! -s "${SCRATCH}/${environment}-usage.stdout" ]]
  [[ -s "${SCRATCH}/${environment}-usage.stderr" ]]
}

recovery_wheel="$(single_wheel 'a_riverhog_recovery_tool-*.whl')"
server_wheel="$(single_wheel 'riverhog_server-*.whl')"

smoke_workspace_distribution \
  riverhog-client \
  'riverhog_client-*.whl' \
  'import importlib.metadata as m, sys; import riverhog_client; assert not any(name.startswith("riverhog_client.processing") for name in sys.modules); import riverhog_client.processing as t; assert "inventory" not in t.ClaimedCollectionReader.__dict__; assert "inventory" not in t.ClaimedCollectionRuntime.__dict__; assert "inventory" not in t.CollectionTransformRuntime.__dict__; assert m.version("riverhog-client")'
env -u PYTHONPATH "${SCRATCH}/riverhog-client/bin/python" -I \
  "${ROOT_DIR}/tests/fixtures/external_riverhog_client_application.py"

smoke_workspace_distribution \
  a-riverhog-cli \
  'a_riverhog_cli-*.whl' \
  'import importlib.metadata as m; import a_riverhog_cli.main; import a_riverhog_cli.cli_support; names = {d.metadata["Name"].lower() for d in m.distributions()}; native = {"a-riverhog-linux-provenance-observer", "a-riverhog-macos-provenance-observer", "a-riverhog-windows-provenance-observer"}; contracts = {"a-riverhog-linux-provenance-contract-lib", "a-riverhog-macos-provenance-contract-lib", "a-riverhog-windows-provenance-contract-lib"}; assert names.isdisjoint(native | contracts); assert "riverhog-provenance-contracts" in names; assert m.version("a-riverhog-cli")' \
  a-riverhog-cli
assert_installed_cli_version a-riverhog-cli a-riverhog-cli a-riverhog-cli
assert_cli_usage_failure a-riverhog-cli a-riverhog-cli collection show

linux_observer_wheel="$(single_wheel 'a_riverhog_linux_provenance_observer-*.whl')"
mapfile -t linux_observer_wheels < <(
  workspace_wheel_closure "${linux_observer_wheel}"
)
run_uv pip install \
  --strict \
  --python "${SCRATCH}/a-riverhog-cli/bin/python" \
  --find-links "${DIST_DIR}" \
  "${linux_observer_wheels[@]}"
"${SCRATCH}/a-riverhog-cli/bin/a-riverhog-cli" local provenance-observer show a-riverhog-linux-provenance-observer --json \
  | "${SCRATCH}/a-riverhog-cli/bin/python" -I -c \
    'import json, sys; value = json.load(sys.stdin); assert value["observer_id"] == "a-riverhog-linux-provenance-observer/v1"; assert value["contract_id"] == "riverhog-provenance-linux-observation/v1"; assert len(value["contract_sha256"]) == 64'

run_uv venv --python 3.12 "${SCRATCH}/recovery"
mapfile -t recovery_wheels < <(
  workspace_wheel_closure "${recovery_wheel}"
)
run_uv pip install \
  --strict \
  --python "${SCRATCH}/recovery/bin/python" \
  --find-links "${DIST_DIR}" \
  "${recovery_wheels[@]}"
(
  cd "${SCRATCH}"
  env -u PYTHONPATH "${SCRATCH}/recovery/bin/a-riverhog-recovery-tool" --help >/dev/null
  "${SCRATCH}/recovery/bin/python" -I -c \
    'import importlib.metadata as m; import a_riverhog_recovery_tool; m.version("a-riverhog-recovery-tool")'
)

run_uv venv --python 3.12 "${SCRATCH}/server"
mapfile -t server_wheels < <(
  workspace_wheel_closure "${server_wheel}"
)
run_uv pip install \
  --strict \
  --python "${SCRATCH}/server/bin/python" \
  --find-links "${DIST_DIR}" \
  "${server_wheels[@]}"
(
  cd "${SCRATCH}"
  env -u PYTHONPATH "${SCRATCH}/server/bin/python" -I - "${SCRATCH}" <<'PY'
import importlib.metadata as metadata
import os
import sys
from pathlib import Path

import yaml
from riverhog_api.app import create_app

root = Path(sys.argv[1])
def secret(name, value):
    path = root / name
    path.write_text(value + "\n")
    return str(path)

document = {
    "database_url_file": secret("database-url", "postgresql+psycopg://app:pass@localhost/app"),
    "bootstrap_token_file": secret("bootstrap-token", "distribution-smoke-bootstrap-token"),
    "browse_token_signing_key_file": secret("browse-key", "distribution-smoke-browse-token-signing-key-v1"),
    "archive_passphrase_files": {
        "distribution-smoke-key-v1": secret("archive-passphrase", "distribution-smoke-archive-passphrase")
    },
    "archive_active_passphrase_id": "distribution-smoke-key-v1",
    "archive_write_store": "archive",
    "archive_stores": {
        "archive": {
            "base_url": "https://archive.example.invalid",
            "token_file": secret("adapter-token", "distribution-smoke-adapter-token"),
        }
    },
}
path = root / "server-config.yaml"
os.environ["RIVERHOG_CONFIG"] = str(path)

def reject(name, changed):
    path.write_text(yaml.safe_dump(changed))
    try:
        create_app()
    except ValueError:
        return
    raise AssertionError(f"installed Riverhog accepted {name}")

reject("missing archive passphrase set", {**document, "archive_passphrase_files": {}})
reject("missing active archive passphrase ID", {**document, "archive_active_passphrase_id": ""})
empty_key = secret("empty-browse-key", "")
reject("missing browse signing key", {**document, "browse_token_signing_key_file": empty_key})
path.write_text(yaml.safe_dump(document))
app = create_app()
assert app.version == metadata.version("riverhog-server")
assert any(ep.name == "riverhog-api" for ep in metadata.entry_points(group="console_scripts"))
PY
)

smoke_workspace_distribution \
  a-riverhog-ftp-spool \
  'a_riverhog_ftp_spool-*.whl' \
  'import importlib.metadata as m; import a_riverhog_ftp_spool.app; m.version("a-riverhog-ftp-spool")' \
  a-riverhog-ftp-spool
smoke_workspace_distribution \
  a-riverhog-filesystem-store \
  'a_riverhog_filesystem_store-*.whl' \
  'import importlib.metadata as m; import a_riverhog_filesystem_store.materialize; m.version("a-riverhog-filesystem-store")' \
  a-riverhog-filesystem-store-materialize

# Produce the source with the full development tree, then prove that the built
# adapter-owned exporter and independent recovery application need neither the
# Riverhog server nor its database at recovery time.
proof_root="${SCRATCH}/filesystem-recovery-proof"
proof_path="$(dirname "$("${MISE_BIN}" which age)"):${PATH}"
PATH="${proof_path}" "${MISE_BIN}" x -- uv run --locked --all-packages --group dev \
  python -I "${ROOT_DIR}/tests/harness/filesystem_recovery_materialization.py" \
  prepare "${proof_root}"
filesystem_materializer="${SCRATCH}/a-riverhog-filesystem-store/bin/a-riverhog-filesystem-store-materialize"
recovery_command="${SCRATCH}/recovery/bin/a-riverhog-recovery-tool"
"${SCRATCH}/a-riverhog-filesystem-store/bin/python" -I -c \
  'import importlib.util; assert importlib.util.find_spec("riverhog_core") is None; assert importlib.util.find_spec("sqlalchemy") is None'
"${SCRATCH}/recovery/bin/python" -I -c \
  'import importlib.util; assert importlib.util.find_spec("riverhog_core") is None; assert importlib.util.find_spec("sqlalchemy") is None'
"${filesystem_materializer}" \
  "${proof_root}/adapter-root" "${proof_root}/full" \
  --prefix archives/recovery-proof/
PATH="${proof_path}" "${recovery_command}" \
  "${proof_root}/full/archives/recovery-proof" "${proof_root}/recovered" \
  --passphrases-file "${proof_root}/passphrases.json"
"${filesystem_materializer}" \
  "${proof_root}/adapter-root" "${proof_root}/description" \
  --path archives/recovery-proof/recovery.json \
  --path archives/recovery-proof/manifest.json.age \
  --path archives/recovery-proof/description.json.age
PATH="${proof_path}" "${recovery_command}" \
  "${proof_root}/description/archives/recovery-proof" \
  --passphrases-file "${proof_root}/passphrases.json" \
  --description-only >"${proof_root}/description.json"
"${filesystem_materializer}" \
  "${proof_root}/adapter-root" "${proof_root}/tags" \
  --path archives/recovery-proof/recovery.json \
  --path archives/recovery-proof/manifest.json.age \
  --path archives/recovery-proof/tags/head.json.age \
  --prefix archives/recovery-proof/tags/nodes/
PATH="${proof_path}" "${recovery_command}" \
  "${proof_root}/tags/archives/recovery-proof" \
  --passphrases-file "${proof_root}/passphrases.json" \
  --tags-only >"${proof_root}/tags.json-seq"
for proof in full description tags; do
  "${MISE_BIN}" x -- uv run --locked --all-packages --group dev \
    python -I "${ROOT_DIR}/tests/harness/filesystem_recovery_materialization.py" \
    "verify-${proof}" "${proof_root}"
done
smoke_workspace_distribution \
  a-stove0-cli \
  'a_stove0_cli-*.whl' \
  'import importlib.metadata as m; import a_stove0_cli.main; m.version("a-stove0-cli")' \
  stove0
assert_installed_cli_version a-stove0-cli stove0 a-stove0-cli
smoke_workspace_distribution \
  stove0-server \
  'stove0_server-*.whl' \
  'import importlib.metadata as m; import stove0_api.app; import stove0_core; m.version("stove0-server")' \
  stove0-server
smoke_workspace_distribution \
  a-stove0-ffprobe-sampling-observer \
  'a_stove0_ffprobe_sampling_observer-*.whl' \
  'import importlib.metadata as m; import a_stove0_ffprobe_sampling_observer.app; m.version("a-stove0-ffprobe-sampling-observer")' \
  a-stove0-ffprobe-sampling-observer
smoke_workspace_distribution \
  a-stove0-exiftool-observer \
  'a_stove0_exiftool_observer-*.whl' \
  'import importlib.metadata as m; import a_stove0_exiftool_observer.app; m.version("a-stove0-exiftool-observer")' \
  a-stove0-exiftool-observer
smoke_workspace_distribution \
  a-stove0-nvenc-av1-opus-target \
  'a_stove0_nvenc_av1_opus_target-*.whl' \
  'import importlib.metadata as m; import a_stove0_nvenc_av1_opus_target.app; m.version("a-stove0-nvenc-av1-opus-target")' \
  a-stove0-nvenc-av1-opus-target
smoke_workspace_distribution \
  a-review0-nvenc-av1-opus-sampler \
  'a_review0_nvenc_av1_opus_sampler-*.whl' \
  'import importlib.metadata as m; import a_review0_nvenc_av1_opus_sampler.app; m.version("a-review0-nvenc-av1-opus-sampler")' \
  a-review0-nvenc-av1-opus-sampler
smoke_workspace_distribution \
  a-stove0-opus-target \
  'a_stove0_opus_target-*.whl' \
  'import importlib.metadata as m; import a_stove0_opus_target.app; m.version("a-stove0-opus-target")' \
  a-stove0-opus-target
smoke_workspace_distribution \
  a-review0-opus-sampler \
  'a_review0_opus_sampler-*.whl' \
  'import importlib.metadata as m; import a_review0_opus_sampler.app; m.version("a-review0-opus-sampler")' \
  a-review0-opus-sampler
smoke_workspace_distribution \
  review0-target-lib \
  'review0_target_lib-*.whl' \
  'import importlib.metadata as m; import review0_target_lib; m.version("review0-target-lib")'
smoke_workspace_distribution \
  review0-planner \
  'review0_planner-*.whl' \
  'import importlib.metadata as m; import review0_planner; m.version("review0-planner")' \
  review0-planner
env -u PYTHONPATH "${SCRATCH}/review0-planner/bin/review0-planner" \
  | "${SCRATCH}/review0-planner/bin/python" -I -c \
    'import json, sys; assert json.load(sys.stdin)["format"] == "review0-contract-report/v1"'
smoke_workspace_distribution \
  a-review0-materializer \
  'a_review0_materializer-*.whl' \
  'import importlib.metadata as m; import a_review0_materializer.app; m.version("a-review0-materializer")' \
  a-review0-materializer
smoke_workspace_distribution \
  a-review0-rclone-target \
  'a_review0_rclone_target-*.whl' \
  'import importlib.metadata as m; import a_review0_rclone_target.app; m.version("a-review0-rclone-target")' \
  a-review0-rclone-target
smoke_workspace_distribution \
  stove0-observer-support \
  'stove0_observer_support-*.whl' \
  'import importlib.metadata as m; import stove0_observer_support; m.version("stove0-observer-support")' \
  stove0-observer-conformance
smoke_workspace_distribution \
  stove0-target-support \
  'stove0_target_support-*.whl' \
  'import importlib.metadata as m; import stove0_target_support; m.version("stove0-target-support")' \
  stove0-target-conformance
smoke_workspace_distribution \
  review0-sampler-lib \
  'review0_sampler_lib-*.whl' \
  'import importlib.metadata as m; import review0_sampler_lib; m.version("review0-sampler-lib")' \
  review0-sampler-conformance \
  review0-sampler-schemas
smoke_workspace_distribution \
  gogurt-listener-runtime \
  'gogurt_listener_runtime-*.whl' \
  'import importlib.metadata as m; import gogurt_listener_runtime.listener; assert m.version("gogurt-listener-runtime")'
smoke_workspace_distribution \
  gogurt \
  'gogurt-*.whl' \
  'import importlib.metadata as m; import gogurt.cli; import gogurt_listener_runtime.listener; m.version("gogurt"); names = {d.metadata["Name"].lower() for d in m.distributions()}; assert "gogurt-listener-runtime" in names; providers = {f"gogurt-{platform}-{capability}" for platform in ("linux", "macos", "windows") for capability in ("listener-host", "mounted-volume")}; assert names.isdisjoint(providers)' \
  gogurt
assert_installed_cli_version gogurt gogurt gogurt
set +e
gogurt_error="$(env -u PYTHONPATH "${SCRATCH}/gogurt/bin/gogurt" list --json 2>/dev/null)"
gogurt_status=$?
set -e
[[ "${gogurt_status}" -eq 1 ]]
printf '%s' "${gogurt_error}" \
  | "${SCRATCH}/gogurt/bin/python" -I -c \
    'import json, sys; value = json.load(sys.stdin); assert value["error"]["code"] == "config_error"'
linux_mounted_volume_wheel="$(single_wheel 'a_gogurt_linux_volume-*.whl')"
mapfile -t linux_mounted_volume_wheels < <(
  workspace_wheel_closure "${linux_mounted_volume_wheel}"
)
run_uv pip install \
  --strict \
  --python "${SCRATCH}/gogurt/bin/python" \
  --find-links "${DIST_DIR}" \
  "${linux_mounted_volume_wheels[@]}"
"${SCRATCH}/gogurt/bin/python" -c \
  'import importlib.metadata as m; mounted = {e.name for e in m.entry_points(group="gogurt.mounted-volume-providers")}; listeners = {e.name for e in m.entry_points(group="gogurt.listener-host-providers")}; assert "a-gogurt-linux-volume" in mounted; assert "a-gogurt-linux-listener" not in listeners'
"${SCRATCH}/gogurt/bin/gogurt" provider mounted-volume show a-gogurt-linux-volume --json >/dev/null
linux_listener_host_wheel="$(single_wheel 'a_gogurt_linux_listener-*.whl')"
mapfile -t linux_listener_host_wheels < <(
  workspace_wheel_closure "${linux_listener_host_wheel}"
)
run_uv pip install \
  --strict \
  --python "${SCRATCH}/gogurt/bin/python" \
  --find-links "${DIST_DIR}" \
  "${linux_listener_host_wheels[@]}"
"${SCRATCH}/gogurt/bin/gogurt" provider listener-host show a-gogurt-linux-listener --json >/dev/null
smoke_workspace_distribution \
  a-riverhog-event-relay \
  'a_riverhog_event_relay-*.whl' \
  'import importlib.metadata as m; import a_riverhog_event_relay.cli; m.version("a-riverhog-event-relay")' \
  a-riverhog-event-relay
assert_installed_cli_version a-riverhog-event-relay a-riverhog-event-relay a-riverhog-event-relay

printf 'All application distribution smoke tests passed.\n'
