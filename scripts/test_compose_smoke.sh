#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_compose_env.sh"
source "${ROOT_DIR}/scripts/_ci_timing.sh"

qualification_lane="${1:-all}"
case "${qualification_lane}" in
  all|storage|ingress-custody|processing-admission|processing-e2e|processing-overlap|review-delivery|witnesses|processing-scale) ;;
  *) printf 'Unknown Compose qualification lane: %s\n' "${qualification_lane}" >&2; exit 2 ;;
esac
if [[ "${qualification_lane}" == "all" ]]; then
  cd "${ROOT_DIR}"
  exec "${MISE_BIN:-mise}" x -- uv run --locked --all-packages --group dev \
    python -m scripts.ci_qualification compose-all \
    --jobs "${LOCAL_QUALIFICATION_JOBS:-2}" \
    --docker-jobs "${LOCAL_QUALIFICATION_DOCKER_JOBS:-2}"
fi
owns_qualification() {
  [[ "${qualification_lane}" == "$1" ]]
}

setup_test_compose_project
configure_compose_tty
export COMPOSE_PROFILES=development
export SOURCE_REVISION="${SOURCE_REVISION:-$(git -C "${ROOT_DIR}" rev-parse HEAD)}"
export RIVERHOG_API_PORT="${RIVERHOG_API_PORT:-0}"

smoke_root="$(mktemp -d "${TMPDIR:-/tmp}/riverhog-compose-smoke.XXXXXX")"
source "${ROOT_DIR}/scripts/_processing_fixture.sh"
configure_processing_fixture "${qualification_lane}"
stove0_project="${COMPOSE_PROJECT_NAME}-stove0"
stove0_projects=()
adapter_started=0
adapter_project="${COMPOSE_PROJECT_NAME}-ftp-spool"
minisign_project="${COMPOSE_PROJECT_NAME}-minisign-witness"
ots_project="${COMPOSE_PROJECT_NAME}-opentimestamps-witness"
stove0_compose_file="${ROOT_DIR}/some-implementations/stove0/application/compose.yaml"
stove0_content_budget_file="${smoke_root}/content-read-budget.compose.yaml"
adapter_compose_file="${ROOT_DIR}/some-implementations/riverhog/ingress/ftp/compose.yaml"
adapter_isolation_file="${smoke_root}/ftp-isolation.compose.yaml"
# Qualification FTP clients use the owning Compose network for control and data.
# Publishing the deployment's fixed passive range would couple independent proofs.
cat > "${adapter_isolation_file}" <<'EOF'
services:
  ftp-spool:
    ports: !reset []
  ftp-listener:
    ports: !reset []
EOF
minisign_compose_file="${ROOT_DIR}/some-implementations/riverhog/applications/a-riverhog-minisign-witness/compose.yaml"
ots_compose_file="${ROOT_DIR}/some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/compose.yaml"
export STOVE0_CONFIG_HOST_PATH="${smoke_root}/stove0.yaml"

stove0_compose() {
  docker compose --project-name "${stove0_project}" \
    --file "${stove0_compose_file}" --file "${stove0_content_budget_file}" "$@"
}

adapter_compose() {
  docker compose --project-name "${adapter_project}" \
    --file "${adapter_compose_file}" --file "${adapter_isolation_file}" "$@"
}

minisign_compose() {
  docker compose --project-name "${minisign_project}" --file "${minisign_compose_file}" "$@"
}

ots_compose() {
  docker compose --project-name "${ots_project}" --file "${ots_compose_file}" "$@"
}

cleanup() {
  local status=$?
  ci_phase teardown "${status}" || true
  if [[ "${status}" -ne 0 ]]; then
    if owns_qualification witnesses; then
      minisign_compose ps >&2 || true
      minisign_compose logs --no-color --tail 200 >&2 || true
      ots_compose ps >&2 || true
      ots_compose logs --no-color --tail 200 >&2 || true
    fi
    if [[ "${adapter_started}" == "1" ]]; then
      adapter_compose ps >&2 || true
      adapter_compose logs --no-color --tail 200 >&2 || true
    fi
    if (( ${#stove0_projects[@]} )); then
      stove0_compose ps >&2 || true
      stove0_compose logs --no-color --tail 200 >&2 || true
    fi
    compose ps >&2 || true
    compose logs --no-color --tail 200 >&2 || true
  fi
  if owns_qualification witnesses; then
    minisign_compose down --volumes --remove-orphans || true
    ots_compose down --volumes --remove-orphans || true
  fi
  if [[ "${adapter_started}" == "1" ]]; then
    adapter_compose down --volumes --remove-orphans || true
  fi
  if (( ${#stove0_projects[@]} )); then
    for stove0_project in "${stove0_projects[@]}"; do
      stove0_compose down --volumes --remove-orphans || true
    done
  fi
  compose down --volumes --remove-orphans
  if [[ -d "${smoke_root}" ]]; then
    docker run --rm \
      --volume "${smoke_root}:/cleanup" \
      alpine:3.22@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce \
      chown -R "$(id -u):$(id -g)" /cleanup || true
  fi
  rm -rf -- "${smoke_root}"
  ci_phase_finish "${status}" || true
  return "${status}"
}
trap cleanup EXIT

ci_phase qualification-images
(
  cd "${ROOT_DIR}"
  "${MISE_BIN:-mise}" x -- uv run --locked --all-packages --group dev \
    python -m scripts.ci_qualification compose-prepare --lane "${qualification_lane}"
)
export RIVERHOG_QUALIFICATION_IMAGES_READY=1
ci_phase storage-bootstrap
"${ROOT_DIR}/scripts/bootstrap_garage.sh"
compose up --detach --wait archive-adapter filesystem-cache-adapter elastic-cache-adapter
if owns_qualification storage; then
ci_phase storage-adapter-qualification
compose run --rm "${COMPOSE_RUN_TTY_ARGS[@]}" \
  --entrypoint riverhog-storage-adapter-conformance \
  test \
  --base-url http://filesystem-cache-adapter:8080 \
  --token-file /run/secrets/riverhog-storage-adapter.token \
  --object-prefix conformance/compose-filesystem \
  --allow-insecure-http
compose run --rm "${COMPOSE_RUN_TTY_ARGS[@]}" \
  --entrypoint python \
  test -m tests.harness.storage_adapter_goodput_probe \
  --base-url http://filesystem-cache-adapter:8080 \
  --token-file /run/secrets/riverhog-storage-adapter.token
continuation_root="${smoke_root}/storage-adapter-continuation"
install -d -m 0700 "${continuation_root}"
compose run --rm "${COMPOSE_RUN_TTY_ARGS[@]}" \
  --volume "${continuation_root}:/continuation" \
  --entrypoint python \
  test -m tests.harness.storage_adapter_restart_probe prepare /continuation/state.json
compose restart archive-adapter
compose up --detach --wait archive-adapter
compose run --rm "${COMPOSE_RUN_TTY_ARGS[@]}" \
  --volume "${continuation_root}:/continuation" \
  --entrypoint python \
  test -m tests.harness.storage_adapter_restart_probe resume /continuation/state.json
filesystem_continuation_root="${smoke_root}/filesystem-adapter-continuation"
install -d -m 0700 "${filesystem_continuation_root}"
compose run --rm "${COMPOSE_RUN_TTY_ARGS[@]}" \
  --env RIVERHOG_STORAGE_ADAPTER_RESTART_PROBE_CACHE_STORE=local \
  --volume "${filesystem_continuation_root}:/continuation" \
  --entrypoint python \
  test -m tests.harness.storage_adapter_restart_probe prepare /continuation/state.json
compose restart filesystem-cache-adapter
compose up --detach --wait filesystem-cache-adapter
compose run --rm "${COMPOSE_RUN_TTY_ARGS[@]}" \
  --env RIVERHOG_STORAGE_ADAPTER_RESTART_PROBE_CACHE_STORE=local \
  --volume "${filesystem_continuation_root}:/continuation" \
  --entrypoint python \
  test -m tests.harness.storage_adapter_restart_probe resume /continuation/state.json
compose run --rm \
  --env RIVERHOG_GARAGE_ARCHIVE_INGRESS_TEST=1 \
  --entrypoint python \
  test -m pytest -q tests/integration/test_garage_encrypted_archive_store.py
# Keep ambiguous external effects and catalog replay behind deterministic
# barriers in the installed test image. Ordinary completed-stage restarts below
# complement these exact interleavings; they do not substitute for them.
compose run --rm "${COMPOSE_RUN_TTY_ARGS[@]}" \
  --entrypoint python test -m pytest -q \
  packages/riverhog-protocol/tests/test_collection_tag_protocol.py::test_late_tag_page_seeks_by_fixed_identity_without_rescanning_prior_tags \
  tests/unit/test_collection_descriptions.py::test_delayed_primary_description_writer_cannot_overwrite_newer_authority \
  tests/unit/test_collection_descriptions.py::test_delayed_description_replica_cannot_overwrite_newer_authority \
  tests/unit/test_collection_tags.py::test_maximum_length_tag_is_a_nonfinal_browse_page \
  tests/unit/test_collection_tags.py::test_provider_nodes_for_retained_exact_revisions_remain_recoverable \
  tests/unit/test_collection_tags.py::test_committed_tag_head_reconciles_after_its_response_is_lost \
  tests/unit/test_collection_tags.py::test_delayed_old_head_writer_cannot_overwrite_newer_acknowledged_authority \
  tests/unit/test_collection_tags.py::test_delayed_gc_cannot_delete_a_node_republished_by_a_newer_authority \
  some-implementations/stove0/application/tests/test_classification_admission.py::test_stale_upsert_cannot_resurrect_a_departed_catalog_revision \
  some-implementations/stove0/application/tests/test_classification_admission.py::test_equal_catalog_revision_with_different_authority_fails_closed \
  some-implementations/stove0/application/tests/test_classification_admission.py::test_failed_lowest_candidate_is_delayed_and_does_not_starve_the_next
fi
ci_phase riverhog-build
ensure_compose_image app
ci_phase riverhog-lifecycle
compose up --detach --wait app
compose exec -T app sh -c \
  'test "$(id -u)" = 65532 && test "$(id -g)" = 65532 && test -w /tmp && test ! -w /usr/share/doc/riverhog'
compose exec -T app python - < "${ROOT_DIR}/tests/harness/provenance_workspace_probe.py"

bootstrap_token="$(cat "${ROOT_DIR}/tests/harness/riverhog-bootstrap-token")"
create_code="import json, os, urllib.request
health = json.load(urllib.request.urlopen('http://127.0.0.1:8000/health/ready'))
assert health['status'] == 'ok'
openapi = json.load(urllib.request.urlopen('http://127.0.0.1:8000/openapi.json'))
assert '/v1/apps' in openapi['paths']
request = urllib.request.Request(
    'http://127.0.0.1:8000/v1/apps',
    headers={'Authorization': 'Bearer ' + os.environ['RIVERHOG_SMOKE_TOKEN']},
)
apps = json.load(urllib.request.urlopen(request))
assert apps['apps'] == []
request = urllib.request.Request(
    'http://127.0.0.1:8000/v1/apps/smoke/keys',
    method='POST',
    data=json.dumps({'access': [{'permission': '*', 'resource': '*'}]}).encode(),
    headers={
        'Authorization': 'Bearer ' + os.environ['RIVERHOG_SMOKE_TOKEN'],
        'Content-Type': 'application/json',
    },
)
created = json.load(urllib.request.urlopen(request))
assert created['app'] == 'smoke'
request = urllib.request.Request(
    'http://127.0.0.1:8000/v1/apps/smoke/keys/' + created['id'] + '/download-quota',
    method='PUT',
    data=json.dumps({'monthly_bytes': int(os.environ['RIVERHOG_SMOKE_DOWNLOAD_QUOTA_BYTES'])}).encode(),
    headers={
        'Authorization': 'Bearer ' + os.environ['RIVERHOG_SMOKE_TOKEN'],
        'Content-Type': 'application/json',
    },
)
quota = json.load(urllib.request.urlopen(request))
assert quota['monthly_bytes'] == int(os.environ['RIVERHOG_SMOKE_DOWNLOAD_QUOTA_BYTES'])
print(created['token'])"
smoke_token="$(
  compose exec -T \
    --env "RIVERHOG_SMOKE_TOKEN=${bootstrap_token}" \
    --env "RIVERHOG_SMOKE_DOWNLOAD_QUOTA_BYTES=${smoke_download_quota_bytes}" \
    app python -c "${create_code}"
)"
test -n "${smoke_token}"

compose restart app
compose up --detach --wait app

restart_code="import json, os, urllib.request
health = json.load(urllib.request.urlopen('http://127.0.0.1:8000/health/ready'))
assert health['status'] == 'ok'
request = urllib.request.Request(
    'http://127.0.0.1:8000/v1/apps',
    headers={'Authorization': 'Bearer ' + os.environ['RIVERHOG_SMOKE_TOKEN']},
)
apps = json.load(urllib.request.urlopen(request))
assert [app['name'] for app in apps['apps']] == ['smoke']"
compose exec -T --env "RIVERHOG_SMOKE_TOKEN=${bootstrap_token}" app python -c "${restart_code}"

secret_root="${smoke_root}/secrets"
intake_root="${smoke_root}/intake"
install -d -m 0700 "${secret_root}" "${intake_root}"
umask 077
printf '%s\n' 'postgresql+psycopg://riverhog:riverhog@postgres:5432/stove0' > "${secret_root}/stove0-database-url"
printf '%s\n' 'stove0-compose-smoke-token' > "${secret_root}/stove0-api-token"
printf '%s\n' 'stove0-compose-target-callback-signing-key' > "${secret_root}/stove0-target-callback-signing-key"
printf '%s\n' 'stove0-compose-browse-token-signing-key-v1' > "${secret_root}/stove0-browse-token-signing-key"
printf '%s\n' "${smoke_token}" > "${secret_root}/stove0-api-riverhog-token"
printf '%s\n' "${smoke_token}" > "${secret_root}/stove0-controller-riverhog-token"
printf '%s\n' "${smoke_token}" > "${secret_root}/stove0-worker-riverhog-token"
printf '%s\n' 'stove0-compose-ffprobe-observer-token' > "${secret_root}/a-stove0-ffprobe-observer-token"
printf '%s\n' 'stove0-compose-magic-observer-token' > "${secret_root}/a-stove0-magic-observer-token"
printf '%s\n' 'stove0-compose-filename-observer-token' > "${secret_root}/a-stove0-filename-prefix-sidecar-observer-token"
printf '%s\n' 'stove0-compose-canonical-hint-observer-token' > "${secret_root}/a-stove0-riverhog-provenance-observer-token"
printf '%s\n' 'stove0-compose-exiftool-observer-token' > "${secret_root}/a-stove0-exiftool-observer-token"
printf '%s\n' 'stove0-compose-nvenc-target-token' > "${secret_root}/a-stove0-nvenc-av1-opus-target-token"
printf '%s\n' 'stove0-compose-nvenc-sampler-token' > "${secret_root}/a-review0-nvenc-av1-opus-sampler-token"
printf '%s\n' 'stove0-compose-opus-target-token' > "${secret_root}/a-stove0-opus-target-token"
printf '%s\n' 'stove0-compose-opus-review-sampler-token' > "${secret_root}/a-review0-opus-sampler-token"
printf '%s\n' 'stove0-compose-review0-token' > "${secret_root}/review0-token"
printf '%s\n' 'stove0-compose-rclone-target-token' > "${secret_root}/a-stove0-rclone-target-token"
printf '%s\n' "${smoke_token}" > "${secret_root}/adapter-riverhog-token"
printf '%s\n' 'a-riverhog-ftp-spool-compose-smoke-token' > "${secret_root}/ftp-spool-api-token"
printf '%s\n' 'a-riverhog-ftp-spool-compose-smoke-password' > "${secret_root}/ftp-spool-password"
chmod 0640 "${secret_root}"/*


adapter_config="${smoke_root}/ftp-spool.yaml"
cat > "${adapter_config}" <<EOF
host_id: urn:uuid:00000000-0000-4000-8000-000000000522
riverhog_base_url: http://app:8000
riverhog_token_file: /run/secrets/riverhog_token
api_token_file: /run/secrets/api_token
allow_insecure_http: true
provenance_observer: a-riverhog-linux-provenance-observer
poll_seconds: 0.25
pending_claim_capacity: 16
claim_attempt_budget: 8
discovery_entry_budget: 4096
completion_failure_capacity: 16
completion_failure_attempt_budget: 8
sources:
  - id: ftp-smoke
    root: /intake/ftp
    ingest_source: ftp:compose-smoke
    close_mode: explicit-flush
    max_files: ${smoke_claim_file_count}
    max_bytes: ${smoke_max_bytes}
    description: FTP exact-event compose qualification
    tags: []
EOF
chmod 0640 "${adapter_config}"
adapter_config_base="${smoke_root}/ftp-spool.base.yaml"
cp "${adapter_config}" "${adapter_config_base}"

export RIVERHOG_CONTROL_NETWORK="${COMPOSE_PROJECT_NAME}_default"
export STOVE0_SECRET_FILE_GID="$(id -g)"
export STOVE0_API_PORT=0
export STOVE0_OBSERVER_TMPFS_SIZE=256m
export STOVE0_TARGET_TMPFS_SIZE=256m
export REVIEW0_TMPFS_SIZE=256m
export STOVE0_DATABASE_URL_FILE="${secret_root}/stove0-database-url"
export STOVE0_API_TOKEN_FILE="${secret_root}/stove0-api-token"
export STOVE0_TARGET_CALLBACK_SIGNING_KEY_FILE="${secret_root}/stove0-target-callback-signing-key"
export STOVE0_BROWSE_TOKEN_SIGNING_KEY_FILE="${secret_root}/stove0-browse-token-signing-key"
export STOVE0_API_RIVERHOG_TOKEN_FILE="${secret_root}/stove0-api-riverhog-token"
export STOVE0_CONTROLLER_RIVERHOG_TOKEN_FILE="${secret_root}/stove0-controller-riverhog-token"
export STOVE0_WORKER_RIVERHOG_TOKEN_FILE="${secret_root}/stove0-worker-riverhog-token"
export A_STOVE0_FFPROBE_OBSERVER_TOKEN_FILE="${secret_root}/a-stove0-ffprobe-observer-token"
export A_STOVE0_RIVERHOG_PROVENANCE_OBSERVER_TOKEN_FILE="${secret_root}/a-stove0-riverhog-provenance-observer-token"
export A_STOVE0_MAGIC_OBSERVER_TOKEN_FILE="${secret_root}/a-stove0-magic-observer-token"
export A_STOVE0_FILENAME_PREFIX_SIDECAR_OBSERVER_TOKEN_FILE="${secret_root}/a-stove0-filename-prefix-sidecar-observer-token"
export A_STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE="${secret_root}/a-stove0-exiftool-observer-token"
export A_STOVE0_NVENC_AV1_OPUS_TARGET_TOKEN_FILE="${secret_root}/a-stove0-nvenc-av1-opus-target-token"
export A_REVIEW0_NVENC_AV1_OPUS_SAMPLER_TOKEN_FILE="${secret_root}/a-review0-nvenc-av1-opus-sampler-token"
export A_STOVE0_OPUS_TARGET_TOKEN_FILE="${secret_root}/a-stove0-opus-target-token"
export A_REVIEW0_OPUS_SAMPLER_TOKEN_FILE="${secret_root}/a-review0-opus-sampler-token"
export REVIEW0_TOKEN_FILE="${secret_root}/review0-token"
export A_STOVE0_RCLONE_TARGET_TOKEN_FILE="${secret_root}/a-stove0-rclone-target-token"
export A_STOVE0_FFPROBE_OBSERVER_IMAGE_ID="sha256:$(printf '1%.0s' {1..64})"
export A_STOVE0_RIVERHOG_PROVENANCE_OBSERVER_IMAGE_ID="sha256:$(printf '8%.0s' {1..64})"
export A_STOVE0_MAGIC_OBSERVER_IMAGE_ID="sha256:$(printf '9%.0s' {1..64})"
export A_STOVE0_FILENAME_PREFIX_SIDECAR_OBSERVER_IMAGE_ID="sha256:$(printf 'a%.0s' {1..64})"
export A_STOVE0_EXIFTOOL_OBSERVER_IMAGE_ID="sha256:$(printf '6%.0s' {1..64})"
export A_STOVE0_NVENC_AV1_OPUS_TARGET_IMAGE_ID="sha256:$(printf '2%.0s' {1..64})"
export A_STOVE0_OPUS_TARGET_IMAGE_ID="sha256:$(printf '3%.0s' {1..64})"
export REVIEW0_IMAGE_ID="sha256:$(printf '4%.0s' {1..64})"
export A_STOVE0_RCLONE_TARGET_IMAGE_ID="sha256:$(printf '7%.0s' {1..64})"
# Compose interpolates the complete model before it selects services.  The
# review0 is created only after this bootstrap value is replaced with
# the running sampler's exact descriptor identity below.
export A_REVIEW0_OPUS_SAMPLER_DESCRIPTOR_SHA256="$(printf '5%.0s' {1..64})"
materializer_config="${smoke_root}/review-materializer.yaml"
rclone_target_config="${smoke_root}/rclone-target.yaml"
export REVIEW0_CONFIG_HOST_PATH="${materializer_config}"
export A_STOVE0_RCLONE_TARGET_CONFIG_HOST_PATH="${rclone_target_config}"
write_review_configs() {
  cat > "${materializer_config}" <<EOF
token_file: /run/secrets/review0_token
samplers:
  - id: opus
    base_url: http://a-review0-opus-sampler:8080
    token_file: /run/secrets/a_review0_opus_sampler_token
    allow_insecure_http: true
    descriptor_sha256: ${A_REVIEW0_OPUS_SAMPLER_DESCRIPTOR_SHA256}
    image_id: ${A_STOVE0_OPUS_TARGET_IMAGE_ID}
EOF
  cat > "${rclone_target_config}" <<EOF
token_file: /run/secrets/a_stove0_rclone_target_token
destination_identity: bdb097e00217151eda04cc51bff5262f21aea08af4da16df5467d727570875a1
destination_naming_rules:
  windows_names: false
  case_sensitive: true
  unicode_equivalence: exact
  component_bytes: 255
  relative_path_bytes: 4096
rclone_remote: /var/lib/stove0-rclone-delivery
EOF
  chmod 0644 "${materializer_config}" "${rclone_target_config}"
}
write_review_configs
export A_RIVERHOG_FTP_SPOOL_PUBLIC_HOST=
export A_RIVERHOG_FTP_SPOOL_SOURCE_ID=ftp-smoke
export A_RIVERHOG_FTP_SPOOL_SECRET_FILE_GID="$(id -g)"
export A_RIVERHOG_FTP_SPOOL_INTAKE_GID="$(id -g)"
export A_RIVERHOG_FTP_SPOOL_INTAKE_HOST_DIR="${intake_root}"
export A_RIVERHOG_FTP_SPOOL_CONFIG_HOST_PATH="${adapter_config}"
export A_RIVERHOG_FTP_SPOOL_RIVERHOG_TOKEN_FILE="${secret_root}/adapter-riverhog-token"
export A_RIVERHOG_FTP_SPOOL_API_TOKEN_FILE="${secret_root}/ftp-spool-api-token"
export A_RIVERHOG_FTP_SPOOL_PASSWORD_FILE="${secret_root}/ftp-spool-password"

client_environment=(
  --env RIVERHOG_BASE_URL=http://app:8000
  --env RIVERHOG_ALLOW_INSECURE_HTTP=true
  --env "RIVERHOG_TOKEN=${smoke_token}"
)
# Bootstrap identities allow Compose to interpolate unselected services; inject
# exact OCI identities only for services actually used by the selected proof.
processing_services=(
  a-stove0-ffprobe-observer a-stove0-magic-observer
  a-stove0-filename-prefix-sidecar-observer a-stove0-riverhog-provenance-observer
  a-stove0-exiftool-observer a-stove0-opus-target
)
write_processing_content_budget() {
  {
    printf '%s\n' 'services:'
    for service in "${processing_services[@]}"; do
      printf '  %s:\n    environment:\n      RIVERHOG_HTTP_TIMEOUT_SECONDS: "%s"\n' \
        "${service}" "${smoke_content_timeout}"
    done
  } > "${stove0_content_budget_file}"
}
write_processing_content_budget
if owns_qualification processing-admission || owns_qualification processing-e2e ||
  owns_qualification processing-overlap || owns_qualification processing-scale ||
  owns_qualification review-delivery; then
  inspected_images=("${processing_services[@]}")
  if owns_qualification review-delivery; then
    inspected_images+=(review0 a-stove0-rclone-target)
  fi
  for image in "${inspected_images[@]}"; do
    prefix="${image//-/_}"
    image_id="$(docker image inspect --format '{{.Id}}' "${image}:dev")"
    [[ "${image_id}" =~ ^sha256:[0-9a-f]{64}$ ]]
    test "$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' "${image}:dev")" = "${SOURCE_REVISION}"
    export "${prefix^^}_IMAGE_ID=${image_id}"
  done
fi

start_stove0_scope() {
  local scope="$1" database="stove0_${1//-/_}"
  configure_processing_fixture "${scope}"
  write_processing_content_budget
  stove0_project="${COMPOSE_PROJECT_NAME}-${scope}"
  stove0_projects+=("${stove0_project}")
  export STOVE0_CONFIG_HOST_PATH="${smoke_root}/${scope}.yaml"
  export STOVE0_DATABASE_URL_FILE="${secret_root}/${scope}-database-url"
  printf '%s\n' "postgresql+psycopg://riverhog:riverhog@postgres:5432/${database}" > "${STOVE0_DATABASE_URL_FILE}"
  chmod 0640 "${STOVE0_DATABASE_URL_FILE}"
  compose exec -T postgres createdb --username riverhog --owner riverhog "${database}"
  compose exec -T postgres psql --username riverhog --dbname "${database}" \
    --command 'CREATE EXTENSION pg_trgm WITH SCHEMA public;'
  sed '/^recipes:/,$d' "${ROOT_DIR}/qualification/fixtures/stove0/config.yaml" > "${STOVE0_CONFIG_HOST_PATH}"
  printf '%s\n' "observation_execution_timeout_seconds: ${smoke_observation_timeout}" >> "${STOVE0_CONFIG_HOST_PATH}"
  printf '%s\n' 'recipes:' >> "${STOVE0_CONFIG_HOST_PATH}"
  sed 's/^/  /' "${ROOT_DIR}/qualification/fixtures/stove0/recipes.yaml" >> "${STOVE0_CONFIG_HOST_PATH}"
  printf '%s\n' 'admissions:' >> "${STOVE0_CONFIG_HOST_PATH}"
  if [[ "${scope}" == "processing-admission" || "${scope}" == "processing-e2e" ]]; then
    jq '{format, policies: [.policies[] | select(.id == "conformance-media")]}' \
      "${ROOT_DIR}/qualification/fixtures/stove0/admissions.json" | \
      sed 's/^/  /' >> "${STOVE0_CONFIG_HOST_PATH}"
  else
    jq '{format, policies: []}' "${ROOT_DIR}/qualification/fixtures/stove0/admissions.json" | \
      sed 's/^/  /' >> "${STOVE0_CONFIG_HOST_PATH}"
  fi
  chmod 0640 "${STOVE0_CONFIG_HOST_PATH}"
  ci_phase "${scope}-bootstrap"
  stove0_compose up --detach --wait state api "${processing_services[@]}"
  if [[ "${scope}" == "processing-admission" || "${scope}" == "processing-e2e" ]]; then
    stove0_compose up --detach --wait controller
  elif [[ "${scope}" == "review-delivery" ]]; then
    stove0_compose up --detach --wait controller worker a-review0-opus-sampler
  fi
}

collect_processing_costs() {
  stove0_compose logs --no-color api controller worker | \
    "${MISE_BIN:-mise}" x -- uv run --locked --all-packages --group dev \
    python "${ROOT_DIR}/scripts/collect_stove0_planning_cost.py" \
      --lane "${qualification_lane}" --source-sha "${SOURCE_REVISION}" \
      --output "${ci_timing_file%/*}/planning-cost.json"
}

finish_stove0_scope() {
  stove0_compose down --volumes --remove-orphans
}

start_ftp() {
  adapter_started=1
  adapter_compose up --detach --no-build --wait intake-init ftp-spool ftp-listener
}

configure_media_ftp() {
  local next="${smoke_root}/ftp-spool.next.yaml"
  sed \
    -e 's/description: FTP exact-event compose qualification/description: Classified FTP compose qualification/' \
    -e 's/^    tags: \[\]/    tags: [stove0\/conformance]/' \
    -e "s|^    max_files: .*|    max_files: ${smoke_claim_file_count}|" \
    -e "s|^    max_bytes: .*|    max_bytes: ${smoke_max_bytes}|" \
    "${adapter_config_base}" > "${next}"
  chmod 0640 "${next}"
  mv "${next}" "${adapter_config}"
  start_ftp
  adapter_compose up --detach --force-recreate --wait ftp-spool ftp-listener
}

processing_qualification="$(cat "${ROOT_DIR}/scripts/qualify_stove0_processing.py")"
invoke_processing() {
  stove0_compose exec -T \
    --env "RIVERHOG_SMOKE_INVOCATION_OUTPUT=$4" \
    --env "RIVERHOG_INPUT_RECEIPT=$1" \
    --env "STOVE0_SMOKE_FILE_COUNT=$2" \
    --env "STOVE0_SMOKE_SIDECAR_COUNT=$3" \
    --env "STOVE0_SMOKE_RECIPE_ID=${processing_recipe_id}" \
    --env "STOVE0_SMOKE_COMPLETION_TIMEOUT=${smoke_completion_timeout}" \
    --env "EXPECTED_WORK_ID=${expected_work_id:-}" \
    api python -c "${processing_qualification}" invoke
}

wait_processing() {
  stove0_compose exec -T \
    --env "STOVE0_SMOKE_FILE_COUNT=${smoke_file_count}" \
    --env "STOVE0_SMOKE_COMPLETION_TIMEOUT=${smoke_completion_timeout}" \
    api python -c "${processing_qualification}" wait
}

processing_snapshot() {
  stove0_compose exec -T --env RIVERHOG_SMOKE_SNAPSHOT_OUTPUT=1 \
    --env "STOVE0_WORK_IDS=${processing_work_ids}" \
    --env "STOVE0_SMOKE_RECIPE_ID=${processing_recipe_id}" \
    api python -c "${processing_qualification}" snapshot
}

verify_target_records() {
  stove0_compose exec -T \
    --env "STOVE0_SETTLED_SNAPSHOT=${settled_snapshot}" \
    a-stove0-opus-target python -c "${processing_qualification}" target-records
}

run_ingress_custody() {
ci_phase ingress-custody
start_ftp
partition_run_code="from ftplib import FTP, all_errors
from io import BytesIO
import json
from pathlib import Path
import time
from a_riverhog_ftp_spool_client import RiverhogFtpSpoolClient
same_path = Path('/intake/ftp/same-path.bin')
expected = (b'first exact FTP event', b'second distinct exact FTP event')

def connect():
    ftp = FTP(timeout=10)
    ftp.connect('ftp-listener', 2121)
    ftp.login('ftp-intake', 'a-riverhog-ftp-spool-compose-smoke-password')
    return ftp

def upload(content):
    deadline = time.monotonic() + 30
    last_error = None
    while time.monotonic() < deadline:
        try:
            with connect() as ftp:
                ftp.storbinary('STOR ' + same_path.name, BytesIO(content))
            return
        except all_errors as error:
            last_error = error
            time.sleep(0.25)
    raise RuntimeError('FTP listener did not accept same-path input') from last_error

# Both terminal transfers enter listener custody before the adapter consumes
# either one. Reuse of the visible pathname must not overwrite the first event.
for content in expected:
    upload(content)
    assert not same_path.exists()
completion_log = Path('/intake/ftp/.a-riverhog-ftp-spool/completed-transfers.log')
assert completion_log.read_bytes().count(b'\n') == 3
with RiverhogFtpSpoolClient(
    base_url='http://127.0.0.1:8080',
    token='a-riverhog-ftp-spool-compose-smoke-token',
    allow_insecure_http=True,
) as client:
    result = client.flush_ftp_spool_source('ftp-smoke')
    assert result['failed'] == [], result
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        receipt_paths = sorted(
            Path('/intake/ftp/.a-riverhog-ftp-spool/receipts').glob('*.json')
        )
        status = client.get_ftp_spool_status()
        if len(receipt_paths) == 2 and status.sources[0].claims == 0:
            break
        time.sleep(0.25)
    else:
        raise AssertionError({'receipts': receipt_paths, 'status': status})
assert len(receipt_paths) == 2, receipt_paths
receipts = sorted(
    (json.loads(path.read_text(encoding='utf-8')) for path in receipt_paths),
    key=lambda row: int(row['collection_id']),
)
assert receipts[0]['collection_id'] != receipts[1]['collection_id']
print(json.dumps([
    {
        key: receipt[key]
        for key in ('collection_id', 'archive_root_sha256', 'artifact_set_identity')
    }
    for receipt in receipts
], sort_keys=True))"
partition_receipts_json="$(adapter_compose exec -T \
  --env RIVERHOG_SMOKE_PARTITION_OUTPUT=1 \
  ftp-spool python -c "${partition_run_code}")"
test "$(printf '%s' "${partition_receipts_json}" | jq 'length')" -eq 2

# Restart both owners before inspecting cleanup. The listener retires any exact
# acquisition marker and the adapter reclaims replay guards only at the locked
# completion-log tip.
adapter_compose restart ftp-spool ftp-listener
adapter_compose up --detach --wait ftp-spool ftp-listener
partition_cleanup_code="import sqlite3
from pathlib import Path
import time
control = Path('/intake/ftp/.a-riverhog-ftp-spool')
deadline = time.monotonic() + 30
while time.monotonic() < deadline:
    with sqlite3.connect(control / 'state.sqlite3') as connection:
        claims = connection.execute('SELECT COUNT(*) FROM claims').fetchone()[0]
        events = connection.execute('SELECT COUNT(*) FROM completion_events').fetchone()[0]
    retained = {
        name: list((control / name).glob('*'))
        for name in ('claims', 'handoffs', 'handoff-intents')
    }
    if claims == 0 and events == 0 and all(not paths for paths in retained.values()):
        break
    time.sleep(0.25)
else:
    raise AssertionError({'claims': claims, 'events': events, 'retained': retained})
assert len(list((control / 'receipts').glob('*.json'))) == 2"
adapter_compose exec -T ftp-spool python -c "${partition_cleanup_code}"

partition_verify_code="import hashlib
import json
import os
from riverhog_client import ApiClient
from tests.support.qualification.artifact_readouts import root_bound_artifacts
receipts = json.loads(os.environ['PARTITION_RECEIPTS'])
expected = (b'first exact FTP event', b'second distinct exact FTP event')
with ApiClient() as client:
    for receipt, content in zip(receipts, expected, strict=True):
        collection_id = int(receipt['collection_id'])
        readouts = list(root_bound_artifacts(client, collection_id))
        assert len(readouts) == 1, readouts
        artifact, hint = readouts[0]
        assert hint == ('same-path.bin',), hint
        assert int(artifact.bytes) == len(content)
        assert artifact.sha256 == hashlib.sha256(content).hexdigest()
        plan = client.plan_retrieval(
            [(collection_id, str(artifact.artifact_id))],
            restore_policy='never',
        )
        job = client.create_retrieval_job(plan['id'], plan_etag=plan['etag'])
        assert job['state'] == 'ready', job
        with client.stream_retrieval_artifact(
            job['id'],
            collection_id=collection_id,
            artifact_id=str(artifact.artifact_id),
            expected_bytes=int(artifact.bytes),
            expected_sha256=artifact.sha256,
        ) as chunks:
            assert b''.join(chunks) == content
        assert client.acknowledge_retrieval_job(job['id'])['state'] == 'completed'"
compose run --rm "${COMPOSE_RUN_TTY_ARGS[@]}" "${client_environment[@]}" \
  --env "PARTITION_RECEIPTS=${partition_receipts_json}" \
  --entrypoint python test -c "${partition_verify_code}"

}

upload_media_fixture() {
configure_media_ftp
adapter_run_code="from ftplib import FTP, all_errors
from io import BytesIO
import json
import os
from pathlib import Path
import time
import wave
from a_riverhog_ftp_spool_client import RiverhogFtpSpoolClient
payload = BytesIO()
with wave.open(payload, 'wb') as audio:
    audio.setnchannels(1)
    audio.setsampwidth(2)
    audio.setframerate(8000)
    audio.writeframes(b'\\x00\\x00' * int(os.environ['STOVE0_SMOKE_AUDIO_FRAMES']))
expected = payload.getvalue()
sources = [
    Path(f'/intake/ftp/smoke-{index:04}.wav')
    for index in range(int(os.environ['STOVE0_SMOKE_FILE_COUNT']))
]
sidecar = Path('/intake/ftp/smoke-0000.xmp')
sidecar_payload = b'''<x:xmpmeta xmlns:x="adobe:ns:meta/">
 <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
  <rdf:Description xmlns:xmp="http://ns.adobe.com/xap/1.0/"
                   xmp:CreateDate="2026-08-23T00:00:00Z"/>
 </rdf:RDF>
</x:xmpmeta>
'''
uploads = [(source, expected) for source in sources] + [(sidecar, sidecar_payload)]
receipt_root = Path('/intake/ftp/.a-riverhog-ftp-spool/receipts')
existing_receipts = {path.name for path in receipt_root.glob('*.json')}
completion_log = Path('/intake/ftp/.a-riverhog-ftp-spool/completed-transfers.log')
initial_log_lines = completion_log.read_bytes().count(b'\n') if completion_log.exists() else 1

def connect():
    ftp = FTP(timeout=10)
    ftp.connect('ftp-listener', 2121)
    ftp.login('ftp-intake', 'a-riverhog-ftp-spool-compose-smoke-password')
    return ftp

def upload(source, content, *, rest=None):
    deadline = time.monotonic() + 30
    last_error = None
    while time.monotonic() < deadline:
        try:
            with connect() as ftp:
                ftp.storbinary('STOR ' + source.name, BytesIO(content), rest=rest)
            return
        except all_errors as error:
            last_error = error
            time.sleep(0.25)
    raise RuntimeError('FTP listener did not accept completed input') from last_error

if os.environ['STOVE0_SMOKE_INTERRUPT_TRANSFER'] == '1':
    first_source, first_content = uploads[0]
    split = max(1, len(first_content) // 3)
    deadline = time.monotonic() + 30
    last_error = None
    while time.monotonic() < deadline:
        try:
            ftp = connect()
            data = ftp.transfercmd('STOR ' + first_source.name)
            data.sendall(first_content[:split])
            receipt_deadline = time.monotonic() + 10
            try:
                while time.monotonic() < receipt_deadline:
                    status = ftp.sendcmd('STAT')
                    if any(
                        line.strip() == f'Total bytes received: {split}'
                        for line in status.splitlines()
                    ):
                        break
                    time.sleep(0.1)
                else:
                    raise AssertionError('FTP listener did not receive the exact interrupted prefix')
                # Close the transfer through the protocol's abort path. Closing
                # the control socket first can let a FIN on the data socket turn
                # this prefix into a successful complete upload under load.
                assert ftp.abort().startswith('426 ')
                assert ftp.getresp().startswith('226 ')
            finally:
                ftp.close()
                data.close()
            partial_deadline = time.monotonic() + 10
            while time.monotonic() < partial_deadline:
                try:
                    if first_source.stat().st_size == split:
                        break
                except FileNotFoundError:
                    pass
                time.sleep(0.1)
            else:
                raise AssertionError('FTP listener did not retain the exact interrupted prefix')
            break
        except all_errors as error:
            last_error = error
            time.sleep(0.25)
    else:
        raise RuntimeError('FTP listener did not accept interrupted input') from last_error
    upload(first_source, first_content[split:], rest=split)
else:
    upload(*uploads[0])
for source, content in uploads[1:]:
    upload(source, content)
assert all(not source.exists() for source, _content in uploads)
completion_log = Path('/intake/ftp/.a-riverhog-ftp-spool/completed-transfers.log')
deadline = time.monotonic() + 10
while time.monotonic() < deadline:
    if completion_log.read_bytes().count(b'\n') == len(uploads) + initial_log_lines:
        break
    time.sleep(0.1)
else:
    raise AssertionError('FTP listener did not durably hand off every completed upload')
with RiverhogFtpSpoolClient(
    base_url='http://127.0.0.1:8080',
    token='a-riverhog-ftp-spool-compose-smoke-token',
    allow_insecure_http=True,
    timeout_seconds=300 + 30 * max(0, int(os.environ['STOVE0_SMOKE_FILE_COUNT']) - 16),
) as client:
    health = client.ftp_spool_health_ready()
    assert health.service == 'a-riverhog-ftp-spool'
    assert health.status == 'ok'
    result = client.flush_ftp_spool_source('ftp-smoke')
    assert result['completed'] == 1, result
    assert result['failed'] == [], result
    status = client.get_ftp_spool_status()
    assert status.sources[0].claims == 0, status
assert all(not source.exists() for source, _content in uploads)
receipts = sorted(receipt_root.glob('*.json'))
new_receipts = [path for path in receipts if path.name not in existing_receipts]
assert len(receipts) == len(existing_receipts) + 1, receipts
assert len(new_receipts) == 1, new_receipts
receipt = json.loads(new_receipts[0].read_text(encoding='utf-8'))
print(json.dumps({
    key: receipt[key]
    for key in ('collection_id', 'archive_root_sha256', 'artifact_set_identity')
}, sort_keys=True))"
ci_phase "${active_processing_lane}-publication"
scale_started_ns="$(date +%s%N)"
input_receipt_json="$(adapter_compose exec -T \
  --env RIVERHOG_SMOKE_RECEIPT_OUTPUT=1 \
  --env "STOVE0_SMOKE_FILE_COUNT=${smoke_file_count}" \
  --env "STOVE0_SMOKE_AUDIO_FRAMES=${smoke_audio_frames}" \
  --env "STOVE0_SMOKE_INTERRUPT_TRANSFER=${interrupt_transfer}" \
  ftp-spool python -c "${adapter_run_code}")"
test -n "${input_receipt_json}"
input_collection_id="$(printf '%s' "${input_receipt_json}" | jq -r '.collection_id')"

classification_code="import os
from riverhog_client import ApiClient
with ApiClient() as client:
    collection_id = int(os.environ['INPUT_COLLECTION_ID'])
    collection = client.get_collection(collection_id)
    assert collection['description'] == os.environ['EXPECTED_DESCRIPTION'], collection
    page = client.list_collection_tags(
        collection_id,
        revision=collection['tag_revision'],
        tag_set_identity=collection['tag_set_identity'],
        page_size=100,
    )
    assert page['tags'] == ['stove0/conformance'], page
    assert page['revision'] == collection['tag_revision']
    assert page['tag_set_identity'] == collection['tag_set_identity']"
compose run --rm "${COMPOSE_RUN_TTY_ARGS[@]}" "${client_environment[@]}" \
  --env "INPUT_COLLECTION_ID=${input_collection_id}" \
  --env "EXPECTED_DESCRIPTION=Classified FTP compose qualification" \
  --entrypoint python test -c "${classification_code}"

}

run_processing_admission() {
active_processing_lane=processing-admission
configure_processing_fixture "${active_processing_lane}"
interrupt_transfer=1
processing_recipe_id=stove0.conformance-media/v1
start_stove0_scope "${active_processing_lane}"
# Qualify the independently selectable stream-facts and sampling contracts with
# actual CPU-produced video/audio; malformed probing must fail, never classify.
stove0_compose exec -T a-stove0-opus-target ffmpeg -nostdin -hide_banner -loglevel error \
  -f lavfi -i 'color=c=black:s=320x180:r=24' \
  -f lavfi -i 'sine=frequency=440:sample_rate=48000' -t 1 \
  -c:v libx264 -profile:v high -preset veryfast -pix_fmt yuv420p \
  -x264-params 'colorprim=bt709:transfer=bt709:colormatrix=bt709' \
  -colorspace bt709 -color_trc bt709 -color_primaries bt709 \
  -c:a aac -b:a 96000 -ac 2 -ar 48000 \
  -movflags frag_keyframe+empty_moov -f mp4 - | \
  stove0_compose exec -T a-stove0-ffprobe-observer python -c \
    "$(cat "${ROOT_DIR}/tests/harness/ffprobe_observer_tool_parity.py")"

admission_baseline_code="import json, time, urllib.request
deadline = time.monotonic() + 60
while time.monotonic() < deadline:
    request = urllib.request.Request(
        'http://127.0.0.1:8080/v1/admission-policies',
        headers={'Authorization': 'Bearer stove0-compose-smoke-token'},
    )
    payload = json.load(urllib.request.urlopen(request, timeout=5))
    policies = payload['policies']
    if len(policies) == 1 and policies[0]['phase'] == 'following':
        assert policies[0]['policy']['id'] == 'conformance-media'
        break
    time.sleep(0.25)
else:
    raise TimeoutError(payload)"
stove0_compose exec -T api python -c "${admission_baseline_code}"
# Establish the catalog cursor before publication, then reconcile offline.
stove0_compose stop controller
upload_media_fixture
# Poll durable admission milestones through bounded scheduler contacts.
# Observation execution and data access retain their separate physical budgets.
admission_state_code="import json, os, urllib.request
collection_id = os.environ['INPUT_COLLECTION_ID']
request = urllib.request.Request(
    'http://127.0.0.1:8080/v1/admissions?page_size=100&sort=admission_id&order=asc',
    headers={'Authorization': 'Bearer stove0-compose-smoke-token'},
)
payload = json.load(urllib.request.urlopen(request, timeout=5))
matches = [
    row for row in payload['admissions']
    if row['intent']['collection']['collection_id'] == collection_id
]
assert len(matches) == 1, matches
assert matches[0]['state'] == os.environ['EXPECTED_ADMISSION_STATE'], matches[0]"

# The controller is offline and no worker is started in this scope.
for admission_state in intent previewed work_bound; do
  ci_phase "${active_processing_lane}-admission-${admission_state}"
  stove0_compose exec -T \
    --env "RIVERHOG_SMOKE_SCHEDULER_STEP=${admission_state}" \
    --env "INPUT_COLLECTION_ID=${input_collection_id}" \
    --env "STOVE0_SMOKE_ADMISSION_TIMEOUT=${smoke_admission_timeout}" \
    api python -c "${processing_qualification}" admission
  ci_phase "${active_processing_lane}-admission-${admission_state}-restart"
  stove0_compose restart api
  stove0_compose up --detach --wait api
  stove0_compose exec -T \
    --env "INPUT_COLLECTION_ID=${input_collection_id}" \
    --env "EXPECTED_ADMISSION_STATE=${admission_state}" \
    api python -c "${admission_state_code}"
done

admission_wait_code="import json, os, time, urllib.request
collection_id = os.environ['INPUT_COLLECTION_ID']
deadline = time.monotonic() + 90
last = None
while time.monotonic() < deadline:
    request = urllib.request.Request(
        'http://127.0.0.1:8080/v1/admissions?page_size=100&sort=admission_id&order=asc',
        headers={'Authorization': 'Bearer stove0-compose-smoke-token'},
    )
    payload = json.load(urllib.request.urlopen(request, timeout=5))
    matches = [
        row for row in payload['admissions']
        if row['intent']['collection']['collection_id'] == collection_id
    ]
    if matches:
        last = matches[0]
        if last['state'] == 'work_bound':
            assert last['intent']['policy_id'] == 'conformance-media'
            print(last['work_id'])
            break
    time.sleep(0.25)
else:
    raise TimeoutError(last)"
stove0_work_id="$(stove0_compose exec -T \
  --env RIVERHOG_SMOKE_ADMISSION_OUTPUT=ftp \
  --env "INPUT_COLLECTION_ID=${input_collection_id}" \
  api python -c "${admission_wait_code}")"
test -n "${stove0_work_id}"
ci_phase "${active_processing_lane}-manual-convergence"
expected_work_id="${stove0_work_id}"
test "$(invoke_processing "${input_receipt_json}" 16 1 ftp)" = "${stove0_work_id}"
expected_work_id=
stove0_compose exec -T a-stove0-opus-target \
  python -c "${processing_qualification}" target-records
collect_processing_costs
finish_stove0_scope
}

run_processing_execution() {
active_processing_lane="$1"
configure_processing_fixture "${active_processing_lane}"
interrupt_transfer=0
processing_recipe_id=stove0.conformance-media/v1
processing_target_count=2
if [[ "${active_processing_lane}" == "processing-scale" ]]; then
  processing_recipe_id=stove0.audio-archive/v1
  processing_target_count=1
fi
# Each scenario owns its Root fixture and Stove0 state. The E2E policy binds
# this fixture automatically; its worker remains stopped until recovery is proved.
start_stove0_scope "${active_processing_lane}"
upload_media_fixture
ci_phase "${active_processing_lane}-planning"
if [[ "${active_processing_lane}" == "processing-e2e" ]]; then
  # Recover automatically bound FTP work before any worker may execute it.
  stove0_compose exec -T \
    --env RIVERHOG_SMOKE_SCHEDULER_STEP=work_bound \
    --env "INPUT_COLLECTION_ID=${input_collection_id}" \
    --env "STOVE0_SMOKE_ADMISSION_TIMEOUT=${smoke_admission_timeout}" \
    api python -c "${processing_qualification}" admission
  stove0_compose stop controller
  stove0_work_id="$(stove0_compose exec -T \
    --env "INPUT_COLLECTION_ID=${input_collection_id}" \
    api python -c "${processing_qualification}" admission-work)"
  stove0_compose exec -T a-stove0-opus-target \
    python -c "${processing_qualification}" target-records
  stove0_compose restart api
  stove0_compose up --detach --wait api
  test "$(stove0_compose exec -T \
    --env "INPUT_COLLECTION_ID=${input_collection_id}" \
    api python -c "${processing_qualification}" admission-work)" = "${stove0_work_id}"
else
  stove0_work_id="$(invoke_processing "${input_receipt_json}" "${smoke_file_count}" 1 ftp)"
fi
processing_work_ids="${stove0_work_id}"
cache_code="import os
from riverhog_client import ApiClient
def collect(method, key, **kwargs):
    page_token = None
    rows = []
    while True:
        payload = method(page_size=100, page_token=page_token, **kwargs)
        rows.extend(payload[key])
        page_token = payload.get('next_page_token')
        if page_token is None:
            return rows
with ApiClient() as client:
    input_id = int(os.environ['INPUT_COLLECTION_ID'])
    collection = client.get_collection(input_id)
    assert collection['id'] == str(input_id), collection
    cached = collect(client.list_retrieval_cache_objects, 'objects', collection_id=input_id)
    assert cached, cached
    assert all(row['state'] == 'ready' for row in cached), cached
    assert all('new_archive' in row['lease_categories'] for row in cached), cached
    assert {row['cache_store'] for row in cached} == {'local'}, cached"
compose run --rm "${COMPOSE_RUN_TTY_ARGS[@]}" "${client_environment[@]}" \
  --env "INPUT_COLLECTION_ID=${input_collection_id}" \
  --entrypoint python test -c "${cache_code}"

if [[ "${active_processing_lane}" == "processing-overlap" ]]; then
client_input_root="${smoke_root}/cli-input"
install -d -m 0700 "${client_input_root}"
python3 -c "import sys, wave
with wave.open(sys.argv[1], 'wb') as audio:
    audio.setnchannels(1)
    audio.setsampwidth(2)
    audio.setframerate(8000)
    audio.writeframes(b'\\x00\\x00' * 400)" \
  "${client_input_root}/cli-input.wav"
client_receipt_json="$(
  compose run --rm "${COMPOSE_RUN_TTY_ARGS[@]}" "${client_environment[@]}" \
    --env RIVERHOG_SMOKE_CLIENT_RECEIPT_OUTPUT=1 \
    --volume "${client_input_root}:/cli-input:ro" \
    --entrypoint a-riverhog-cli test collection upload start /cli-input \
    --description 'Classified CLI compose qualification' \
    --tag stove0/conformance \
    --json
)"
client_collection_id="$(printf '%s' "${client_receipt_json}" | jq -r '.collection_id')"
compose run --rm "${COMPOSE_RUN_TTY_ARGS[@]}" "${client_environment[@]}" \
  --env "INPUT_COLLECTION_ID=${client_collection_id}" \
  --env "EXPECTED_DESCRIPTION=Classified CLI compose qualification" \
  --entrypoint python test -c "${classification_code}"
client_work_id="$(invoke_processing "${client_receipt_json}" 1 0 client)"
test -n "${client_work_id}"
test "${client_work_id}" != "${stove0_work_id}"
processing_work_ids="${stove0_work_id},${client_work_id}"
fi
stove0_compose up --detach --wait controller worker
ci_phase "${active_processing_lane}-execution"
# The supplied Opus target intentionally admits one target job at a time.
# Two independently classified producers and the overlapping-route proof create
# four valid jobs. Scale the fixture deadline with its declared workload;
# target cardinality and archive extents remain unchanged.
wait_processing

scale_elapsed_ns=$(( $(date +%s%N) - scale_started_ns ))

output_code="import json, os, urllib.request
request = urllib.request.Request(
    'http://127.0.0.1:8080/v1/work/' + os.environ['STOVE0_WORK_ID'],
    headers={'Authorization': 'Bearer stove0-compose-smoke-token'},
)
work = json.load(urllib.request.urlopen(request, timeout=30))
assert work['phase'] == 'complete', work
targets = work['preview_acceptance']['target_plans']
assert len(targets) == int(os.environ['STOVE0_SMOKE_TARGET_COUNT']), work
for row in targets:
    request = urllib.request.Request(
        'http://127.0.0.1:8080/v1/work/' + row['work_id'],
        headers={'Authorization': 'Bearer stove0-compose-smoke-token'},
    )
    other = json.load(urllib.request.urlopen(request, timeout=30))
    assert other['phase'] == 'complete' and other['output'] is not None, other
    assert other['target_status']['state'] == 'succeeded'
    assert other['target_settlement'] is not None
target = next(row for row in targets if row['branch_id'] == 'archive-audio')
child_request = urllib.request.Request(
    'http://127.0.0.1:8080/v1/work/' + target['work_id'],
    headers={'Authorization': 'Bearer stove0-compose-smoke-token'},
)
child = json.load(urllib.request.urlopen(child_request, timeout=30))
assert child['phase'] == 'complete' and child['output'] is not None, child
print(child['output']['collection_id'])"
output_collection_id="$(stove0_compose exec -T \
  --env RIVERHOG_SMOKE_SETTLEMENT_OUTPUT=1 \
  --env "STOVE0_WORK_ID=${stove0_work_id}" \
  --env "STOVE0_SMOKE_TARGET_COUNT=${processing_target_count}" \
  api python -c "${output_code}")"
test -n "${output_collection_id}"

lineage_code="import json, os
from riverhog_client import ApiClient
from tests.support.qualification.artifact_readouts import root_bound_artifacts
def readouts(client, collection_id):
    result = {}
    for artifact, hint in root_bound_artifacts(client, collection_id):
        assert hint is not None, artifact
        name = '/'.join(hint)
        assert name not in result, name
        result[name] = artifact.model_dump(mode='json')
    return result
with ApiClient() as client:
    inputs = [client.get_collection(int(os.environ['INPUT_COLLECTION_ID']))]
    outputs = [client.get_collection(int(os.environ['OUTPUT_COLLECTION_ID']))]
    input_artifacts = readouts(client, inputs[0]['id'])
    output_artifacts = readouts(client, outputs[0]['id'])
    audio_count = int(os.environ['STOVE0_SMOKE_FILE_COUNT'])
    assert len(input_artifacts) == audio_count + 1
    expected_names = {
        name
        for ordinal in range(audio_count)
        for name in (f'smoke-{ordinal:04d}.opus', f'smoke-{ordinal:04d}.opus.xmp')
    } | {'smoke-0000.xmp'}
    assert set(output_artifacts) == expected_names
    source_xmp = input_artifacts['smoke-0000.xmp']
    retained_xmp = output_artifacts['smoke-0000.xmp']
    assert (retained_xmp['bytes'], retained_xmp['sha256']) == (source_xmp['bytes'], source_xmp['sha256'])
    elapsed_seconds = int(os.environ['STOVE0_SMOKE_ELAPSED_NS']) / 1_000_000_000
    input_bytes = sum(int(row['bytes']) for row in input_artifacts.values())
    derivation = client.get_collection_derivation(outputs[0]['id'])
    assert derivation['derivation']['format'] == 'riverhog-collection-derivation/v1'
    authority = derivation['derivation']['input_set_sha256']
    ordinal = 0
    roots = []
    while True:
        page = client.list_processing_claim_inputs(
            derivation['derivation']['claim']['id'],
            identity_sha256=authority,
            start_ordinal=ordinal,
        )
        assert page.identity.sha256 == authority
        roots.extend(page.inputs)
        if page.next_ordinal is None:
            break
        ordinal = page.next_ordinal
    assert [row.collection_id for row in roots] == [int(inputs[0]['id'])]
    print(json.dumps({
        'format': 'stove0-final-image-scale/v1',
        'qualification_lane': os.environ['STOVE0_SMOKE_PROCESSING_LANE'],
        'recipe_id': os.environ['STOVE0_SMOKE_RECIPE_ID'],
        'elapsed_seconds': elapsed_seconds,
        'input_bytes': input_bytes,
        'input_artifacts': len(input_artifacts),
        'items_per_second': len(input_artifacts) / elapsed_seconds,
        'measurement': 'ftp-ingress-through-opus-derived-publication',
        'mib_per_second': input_bytes / 1048576 / elapsed_seconds,
        'output_bytes': sum(int(row['bytes']) for row in output_artifacts.values()),
        'output_artifacts': len(output_artifacts),
    }, sort_keys=True))"
compose run --rm "${COMPOSE_RUN_TTY_ARGS[@]}" "${client_environment[@]}" \
  --env "STOVE0_SMOKE_FILE_COUNT=${smoke_file_count}" \
  --env "STOVE0_SMOKE_ELAPSED_NS=${scale_elapsed_ns}" \
  --env "STOVE0_SMOKE_PROCESSING_LANE=${active_processing_lane}" \
  --env "STOVE0_SMOKE_RECIPE_ID=${processing_recipe_id}" \
  --env "INPUT_COLLECTION_ID=${input_collection_id}" \
  --env "OUTPUT_COLLECTION_ID=${output_collection_id}" \
  --entrypoint python test -c "${lineage_code}"

state_metrics_code="import json
from collections import Counter
from sqlalchemy import text
from stove0_core import SqlAlchemyStateStore, database_url_from_config
store = SqlAlchemyStateStore(database_url_from_config(), initialize=False)
rows = list(store.iter_work(sort='work_id', order='asc'))
assert rows and all(row.phase == 'complete' for row in rows), rows
table_names = (
    'stove0_artifact_selections',
    'stove0_evaluation_records',
    'stove0_event_cursors',
    'stove0_lifecycle_events',
    'stove0_work_records',
)
with store.engine.connect() as connection:
    table_rows = {
        name: int(connection.scalar(text(f'SELECT count(*) FROM {name}')) or 0)
        for name in table_names
    }
    table_bytes = {
        name: int(
            connection.scalar(
                text('SELECT pg_total_relation_size(to_regclass(:table_name))'),
                {'table_name': name},
            )
            or 0
        )
        for name in table_names
    }
print(json.dumps({
    'format': 'stove0-operational-scale/v1',
    'database_bytes': sum(table_bytes.values()),
    'document_bytes': sum(
        len(row.model_dump_json().encode())
        for row in rows
    ),
    'phase_counts': dict(sorted(Counter(row.phase for row in rows).items())),
    'table_bytes': table_bytes,
    'table_rows': table_rows,
    'work_records': len(rows),
}, sort_keys=True))
store.engine.dispose()"
stove0_compose exec -T api python -c "${state_metrics_code}"

target_metrics_code="import json, os
from pathlib import Path
state = Path('/var/lib/a-stove0-opus-target')
workspace = Path('/run/a-stove0-opus-target')
peak_path = Path('/sys/fs/cgroup/memory.peak')
peak = peak_path.read_text().strip() if peak_path.is_file() else None
cpu_path = Path('/sys/fs/cgroup/cpu.stat')
cpu = dict(line.split() for line in cpu_path.read_text().splitlines()) if cpu_path.is_file() else {}
filesystem = os.statvfs(workspace)
workspace_bytes = sum(path.stat().st_size for path in workspace.rglob('*') if path.is_file())
assert workspace_bytes == 0
print(json.dumps({
    'format': 'stove0-target-scale/v1',
    'accepted_records': len(tuple(state.glob('*.accepted.json'))),
    'cpu_usage_seconds': int(cpu['usage_usec']) / 1_000_000 if 'usage_usec' in cpu else None,
    'memory_peak_bytes': int(peak) if peak and peak.isdecimal() else None,
    'source_revision': os.environ['A_STOVE0_OPUS_TARGET_SOURCE_REVISION'],
    'status_records': len(tuple(state.glob('*.status.json'))),
    'workspace_capacity_bytes': filesystem.f_blocks * filesystem.f_frsize,
    'workspace_current_bytes': workspace_bytes,
}, sort_keys=True))"
stove0_compose exec -T a-stove0-opus-target python -c "${target_metrics_code}"

if [[ "${STOVE0_SMOKE_TRANSFER_METRICS:-1}" == "1" ]]; then
  compose logs --no-color app > "${smoke_root}/riverhog-transfer.log"
  PYTHONPATH="${ROOT_DIR}" python3 -c "from dataclasses import asdict
import json
from pathlib import Path
import sys
from scripts.transfer_profile import SCENARIO_OPERATIONS, summarize_transfer_log
summary = summarize_transfer_log(
    Path(sys.argv[1]).read_text(encoding='utf-8'),
    expected_operations=SCENARIO_OPERATIONS['stove0-derived-publication'],
)
print(json.dumps({'format': 'stove0-transfer-phases/v1', **asdict(summary)}, sort_keys=True))" \
    "${smoke_root}/riverhog-transfer.log"
fi

ci_phase "${active_processing_lane}-restart-replay"
settled_snapshot="$(processing_snapshot)"
verify_target_records
stove0_compose restart api controller worker "${processing_services[@]}"
stove0_compose up --detach --wait api controller worker "${processing_services[@]}"
wait_processing
test "$(processing_snapshot)" = "${settled_snapshot}"
verify_target_records
collect_processing_costs
finish_stove0_scope
}

if owns_qualification storage; then
ci_phase storage-cache-placement
local_root="${smoke_root}/local-cache-input"
install -d -m 0700 "${local_root}"
printf '%s\n' 'small local-cache qualification' > "${local_root}/local.txt"
compose run --rm "${COMPOSE_RUN_TTY_ARGS[@]}" "${client_environment[@]}" \
  --volume "${local_root}:/local-cache-input:ro" \
  --entrypoint a-riverhog-cli test collection upload start /local-cache-input --json > /dev/null
overflow_root="${smoke_root}/overflow"
install -d -m 0700 "${overflow_root}"
truncate -s 2MiB "${overflow_root}/larger-than-local-budget.bin"
overflow_result="$(
  compose run --rm "${COMPOSE_RUN_TTY_ARGS[@]}" "${client_environment[@]}" \
    --volume "${overflow_root}:/overflow:ro" \
    --entrypoint a-riverhog-cli test collection upload start /overflow \
    --json
)"
overflow_collection_id="$(printf '%s' "${overflow_result}" | jq -r '.collection_id')"
overflow_cache_code="import os, time
from riverhog_client import ApiClient
def collect(method, key, **kwargs):
    page_token = None
    rows = []
    while True:
        payload = method(page_size=100, page_token=page_token, **kwargs)
        rows.extend(payload[key])
        page_token = payload.get('next_page_token')
        if page_token is None:
            return rows
with ApiClient() as client:
    deadline = time.monotonic() + 60
    while True:
        rows = collect(client.list_retrieval_cache_objects, 'objects')
        overflow = [
            row for row in rows
            if row['collection_id'] == os.environ['OVERFLOW_COLLECTION_ID']
        ]
        stores = {row['cache_store'] for row in rows if row['state'] == 'ready'}
        if overflow and all(row['state'] == 'ready' for row in overflow) and stores == {'local', 'elastic'}:
            break
        if time.monotonic() >= deadline:
            raise AssertionError((stores, overflow))
        time.sleep(0.25)
    assert 'elastic' in {row['cache_store'] for row in overflow}, overflow
    status = client.retrieval_cache_status()
    assert [store['cache_store'] for store in status['stores']] == ['local', 'elastic'], status
    assert status['stores'][0]['admission_budget_bytes'] == 1048576, status"
compose run --rm "${COMPOSE_RUN_TTY_ARGS[@]}" "${client_environment[@]}" \
  --env "OVERFLOW_COLLECTION_ID=${overflow_collection_id}" \
  --entrypoint python test -c "${overflow_cache_code}"

fi
if owns_qualification ingress-custody; then
  run_ingress_custody
fi
if owns_qualification processing-admission; then
  run_processing_admission
fi
if owns_qualification processing-e2e; then
  run_processing_execution processing-e2e
fi
if owns_qualification processing-overlap; then
  run_processing_execution processing-overlap
fi
if owns_qualification processing-scale; then
  run_processing_execution processing-scale
fi

if owns_qualification review-delivery; then
start_stove0_scope review-delivery
sampler_descriptor_code="import json, urllib.request
request = urllib.request.Request(
    'http://127.0.0.1:8080/v1/sampler',
    headers={'Authorization': 'Bearer stove0-compose-opus-review-sampler-token'},
)
print(json.load(urllib.request.urlopen(request))['descriptor_sha256'])"
export A_REVIEW0_OPUS_SAMPLER_DESCRIPTOR_SHA256="$(
  stove0_compose exec -T a-review0-opus-sampler python -c "${sampler_descriptor_code}"
)"
write_review_configs
stove0_compose up --detach --wait review0 a-stove0-rclone-target
stove0_compose exec -T review0 python -c "import json, urllib.request; request = urllib.request.Request('http://127.0.0.1:8080/v1/target', headers={'Authorization': 'Bearer stove0-compose-review0-token'}); assert json.load(urllib.request.urlopen(request))['protocol'] == 'stove0-transform-target/v1'"
stove0_compose exec -T a-stove0-rclone-target python -c "import json, urllib.request; request = urllib.request.Request('http://127.0.0.1:8080/v1/target', headers={'Authorization': 'Bearer stove0-compose-rclone-target-token'}); assert json.load(urllib.request.urlopen(request))['protocol'] == 'stove0-effect-target/v1'"
stove0_compose exec -T a-stove0-rclone-target python -c "from pathlib import Path; import subprocess; source = Path('/tmp/rclone-probe'); source.write_bytes(b'riverhog-rclone-effect-probe'); destination = Path('/var/lib/stove0-rclone-delivery/qualification/probe'); subprocess.run(['rclone', 'copyto', str(source), str(destination)], check=True); assert destination.read_bytes() == source.read_bytes(); source.unlink(); destination.unlink()"
# Review0's ordinary finalized collection is inspected before introducing the
# independent delivery admission policy. This makes the collection boundary
# observable instead of relying on a race with fast local rclone delivery.
ci_phase review-delivery
review_input_root="${smoke_root}/review-input"
install -d -m 0700 "${review_input_root}"
python3 -c "import sys, wave
with wave.open(sys.argv[1], 'wb') as audio:
    audio.setnchannels(1)
    audio.setsampwidth(2)
    audio.setframerate(8000)
    audio.writeframes(b'\\x00\\x00' * 16000)" \
  "${review_input_root}/review-input.wav"
review_input_receipt_json="$(
  compose run --rm "${COMPOSE_RUN_TTY_ARGS[@]}" "${client_environment[@]}" \
    --volume "${review_input_root}:/review-input:ro" \
    --entrypoint a-riverhog-cli test collection upload start /review-input \
    --json
)"
review_qualification="$(cat "${ROOT_DIR}/scripts/qualify_review0_delivery.py")"
review_result_json="$(stove0_compose exec -T \
  --env "REVIEW_INPUT_RECEIPT=${review_input_receipt_json}" \
  api python -c "${review_qualification}" review)"
review_output_collection_id="$(printf '%s' "${review_result_json}" | jq -r '.collection_id')"
review_source_collection_id="$(printf '%s' "${review_result_json}" | jq -r '.source_collection_id')"
test -n "${review_output_collection_id}"

full_admission_config="${smoke_root}/stove0.full-admissions.yaml"
sed '/^admissions:/,$d' "${STOVE0_CONFIG_HOST_PATH}" > "${full_admission_config}"
printf '%s\n' 'admissions:' >> "${full_admission_config}"
jq '{format, policies: [.policies[] | select(.id == "review0-output-delivery")]}' \
  "${ROOT_DIR}/qualification/fixtures/stove0/admissions.json" | \
  sed 's/^/  /' >> "${full_admission_config}"
cat "${full_admission_config}" > "${STOVE0_CONFIG_HOST_PATH}"
stove0_compose restart api controller worker
stove0_compose up --detach --wait api controller worker

delivery_result_json="$(stove0_compose exec -T \
  --env "REVIEW_OUTPUT_COLLECTION_ID=${review_output_collection_id}" \
  --env "REVIEW_SOURCE_COLLECTION_ID=${review_source_collection_id}" \
  api python -c "${review_qualification}" delivery)"
delivery_id="$(printf '%s' "${delivery_result_json}" | jq -r '.delivery_id')"
manifest_sha256="$(printf '%s' "${delivery_result_json}" | jq -r '.manifest_sha256')"
stove0_compose exec -T \
  --env "RCLONE_DELIVERY_ID=${delivery_id}" \
  --env "RCLONE_MANIFEST_SHA256=${manifest_sha256}" \
  a-stove0-rclone-target python -c "import hashlib, json, os
from pathlib import Path
root = Path('/var/lib/stove0-rclone-delivery') / os.environ['RCLONE_DELIVERY_ID']
marker = (root / 'manifest.json').read_bytes()
assert hashlib.sha256(marker).hexdigest() == os.environ['RCLONE_MANIFEST_SHA256']
manifest = json.loads(marker)
assert manifest['delivery_id'] == os.environ['RCLONE_DELIVERY_ID']
assert manifest['artifacts']
for artifact in manifest['artifacts']:
    delivered = (root / 'objects' / artifact['delivered_path']).read_bytes()
    assert len(delivered) == int(artifact['bytes'])
    assert hashlib.sha256(delivered).hexdigest() == artifact['sha256']"
stove0_compose restart api controller worker a-stove0-rclone-target
stove0_compose up --detach --wait api controller worker a-stove0-rclone-target
stove0_compose exec -T \
  --env "REVIEW_OUTPUT_COLLECTION_ID=${review_output_collection_id}" \
  --env "REVIEW_SOURCE_COLLECTION_ID=${review_source_collection_id}" \
  api python -c "${review_qualification}" delivery > /dev/null
finish_stove0_scope
fi

if owns_qualification witnesses; then
# Qualify both independent witnesses against a real finalized catalog item.
# The deterministic OTS calendar seam supplies a bounded pending attestation;
# this smoke does not depend on a public calendar or claim Bitcoin confirmation.
ci_phase witnesses
witness_input_root="${smoke_root}/witness-input"
install -d -m 0700 "${witness_input_root}"
printf '%s\n' 'independent witness Compose qualification' > "${witness_input_root}/statement.txt"
witness_receipt_json="$(
  compose run --rm "${COMPOSE_RUN_TTY_ARGS[@]}" "${client_environment[@]}" \
    --volume "${witness_input_root}:/witness-input:ro" \
    --entrypoint a-riverhog-cli test collection upload start /witness-input \
    --json
)"
witness_collection_id="$(printf '%s' "${witness_receipt_json}" | jq -r '.collection_id')"
test -n "${witness_collection_id}"

export RIVERHOG_ALLOW_INSECURE_HTTP=true
export A_RIVERHOG_MINISIGN_WITNESS_RIVERHOG_BASE_URL=http://app:8000
export A_RIVERHOG_MINISIGN_WITNESS_RIVERHOG_TOKEN_FILE="${secret_root}/stove0-api-riverhog-token"
export A_RIVERHOG_MINISIGN_WITNESS_SECRET_FILE_GID="$(id -g)"
export A_RIVERHOG_MINISIGN_WITNESS_POLL_SECONDS=3600
export A_RIVERHOG_OPENTIMESTAMPS_WITNESS_RIVERHOG_BASE_URL=http://app:8000
export A_RIVERHOG_OPENTIMESTAMPS_WITNESS_RIVERHOG_TOKEN_FILE="${secret_root}/stove0-api-riverhog-token"
export A_RIVERHOG_OPENTIMESTAMPS_WITNESS_SECRET_FILE_GID="$(id -g)"
export A_RIVERHOG_OPENTIMESTAMPS_WITNESS_CALENDAR_URLS=https://calendar.example.invalid
export A_RIVERHOG_OPENTIMESTAMPS_WITNESS_POLL_SECONDS=3600
witness_key_root="${smoke_root}/witness-key"
install -d -m 0777 "${witness_key_root}"
export A_RIVERHOG_MINISIGN_WITNESS_SECRET_KEY_FILE="${witness_key_root}/secret.key"
export A_RIVERHOG_MINISIGN_WITNESS_PUBLIC_KEY_FILE="${witness_key_root}/public.key"
docker run --rm --user 65532:65532 \
  --volume "${witness_key_root}:/keys" --entrypoint minisign \
  a-riverhog-minisign-witness:dev -G -W -s /keys/secret.key -p /keys/public.key
docker run --rm --user 65532:65532 --group-add "$(id -g)" \
  --volume "${witness_key_root}:/keys:ro" \
  --volume "${secret_root}/stove0-api-riverhog-token:/token:ro" \
  --env "WITNESS_TOKEN_GID=$(id -g)" --entrypoint python \
  a-riverhog-minisign-witness:dev -c "import os, stat
from pathlib import Path
key = Path('/keys/secret.key').stat()
token = Path('/token').stat()
assert stat.S_IMODE(key.st_mode) == 0o600 and key.st_uid == 65532
assert stat.S_IMODE(token.st_mode) == 0o640 and token.st_gid == int(os.environ['WITNESS_TOKEN_GID'])
assert Path('/keys/secret.key').read_bytes() and Path('/token').read_bytes()"
minisign_compose up --detach --wait state
ots_compose up --detach --wait state
witness_probe="${ROOT_DIR}/tests/harness/witness_compose_probe.py:/qualification.py:ro"
minisign_result="$(minisign_compose run --rm --no-deps -T \
  --volume "${witness_probe}" --entrypoint python run \
  /qualification.py minisign prepare "${witness_collection_id}")"
ots_result="$(ots_compose run --rm --no-deps -T \
  --volume "${witness_probe}" --entrypoint python run \
  /qualification.py opentimestamps prepare "${witness_collection_id}")"
minisign_compose up --detach --wait run
ots_compose up --detach --wait run
minisign_compose restart run
ots_compose restart run
minisign_compose up --detach --wait run
ots_compose up --detach --wait run
test "$(minisign_compose run --rm --no-deps -T \
  --volume "${witness_probe}" --entrypoint python run \
  /qualification.py minisign verify "${witness_collection_id}" \
  "$(printf '%s' "${minisign_result}" | jq -r '.digest')" \
  "$(printf '%s' "${minisign_result}" | jq -r '.evidence_sha256')")" = retained
test "$(ots_compose run --rm --no-deps -T \
  --volume "${witness_probe}" --entrypoint python run \
  /qualification.py opentimestamps verify "${witness_collection_id}" \
  "$(printf '%s' "${ots_result}" | jq -r '.digest')" \
  "$(printf '%s' "${ots_result}" | jq -r '.evidence_sha256')")" = retained
fi
