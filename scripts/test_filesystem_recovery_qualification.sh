#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_compose_env.sh"

setup_test_compose_project
configure_compose_tty
export COMPOSE_PROFILES=development
export SOURCE_REVISION="${SOURCE_REVISION:-$(git -C "${ROOT_DIR}" rev-parse HEAD)}"
export RIVERHOG_API_PORT=0
proof_root="$(mktemp -d "${TMPDIR:-/tmp}/riverhog-filesystem-recovery.XXXXXX")"
export RIVERHOG_CONFIG_HOST_PATH="${proof_root}/riverhog.yaml"
export RIVERHOG_BOOTSTRAP_TOKEN_HOST_PATH="${proof_root}/bootstrap-token"
export RIVERHOG_BROWSE_KEY_HOST_PATH="${proof_root}/browse-key"
export RIVERHOG_ARCHIVE_PASSPHRASE_HOST_PATH="${proof_root}/archive-passphrase"
printf '%s\n' 'riverhog-filesystem-proof-bootstrap-token' > "${RIVERHOG_BOOTSTRAP_TOKEN_HOST_PATH}"
printf '%s\n' 'filesystem-recovery-qualification-browse-key' > "${RIVERHOG_BROWSE_KEY_HOST_PATH}"
printf '%s\n' 'filesystem-recovery-qualification-passphrase' > "${RIVERHOG_ARCHIVE_PASSPHRASE_HOST_PATH}"
cat > "${RIVERHOG_CONFIG_HOST_PATH}" <<'EOF'
database_url_file: /run/secrets/riverhog-database-url
bootstrap_token_file: /run/secrets/riverhog-bootstrap-token
browse_token_signing_key_file: /run/secrets/riverhog-browse-key
archive_passphrase_files:
  filesystem-proof: /run/secrets/riverhog-archive-passphrase
archive_active_passphrase_id: filesystem-proof
archive_scrypt_work_factor: 10
archive_write_store: archive
archive_stores:
  archive:
    base_url: http://filesystem-cache-adapter:8080
    token_file: /run/secrets/riverhog-storage-adapter.token
    allow_insecure_http: true
retrieval_cache_stores:
  local:
    base_url: http://filesystem-cache-adapter:8080
    token_file: /run/secrets/riverhog-storage-adapter.token
    allow_insecure_http: true
retrieval_cache_new_archive_enabled: false
EOF
chmod 0644 "${proof_root}"/*
volume_name="${COMPOSE_PROJECT_NAME}_filesystem-cache-data"
client_image="riverhog-filesystem-recovery-client:${SOURCE_REVISION}-${COMPOSE_PROJECT_NAME}"
recovery_image="riverhog-filesystem-recovery-tool:${SOURCE_REVISION}-${COMPOSE_PROJECT_NAME}"
filesystem_image="a-riverhog-filesystem-store:dev"
network_name="${COMPOSE_PROJECT_NAME}_default"
receipt=""

cleanup() {
  local status=$?
  if [[ "${status}" -ne 0 ]]; then
    if [[ -n "${receipt}" ]]; then
      printf '\nclient stdout:\n%s\n' "${receipt}" >&2
    fi
    for diagnostic in client.stderr description.stderr; do
      if [[ -s "${proof_root}/${diagnostic}" ]]; then
        printf '\n%s:\n' "${diagnostic}" >&2
        sed -n '1,200p' "${proof_root}/${diagnostic}" >&2
      fi
    done
    compose ps >&2 || true
    compose logs --no-color --tail 200 >&2 || true
  fi
  compose down --volumes --remove-orphans || true
  docker image rm "${client_image}" "${recovery_image}" >/dev/null 2>&1 || true
  docker run --rm \
    --volume "${proof_root}:/cleanup" \
    alpine:3.22@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce \
    chown -R "$(id -u):$(id -g)" /cleanup >/dev/null 2>&1 || true
  rm -rf -- "${proof_root}"
  return "${status}"
}
trap cleanup EXIT

ensure_compose_image test
ensure_compose_image filesystem-cache-adapter
ensure_compose_image app
docker build \
  --file "${ROOT_DIR}/tests/harness/filesystem_recovery.Dockerfile" \
  --target client \
  --build-arg "SOURCE_REVISION=${SOURCE_REVISION}" \
  --tag "${client_image}" \
  "${ROOT_DIR}"
docker build \
  --file "${ROOT_DIR}/tests/harness/filesystem_recovery.Dockerfile" \
  --target recovery \
  --build-arg "SOURCE_REVISION=${SOURCE_REVISION}" \
  --tag "${recovery_image}" \
  "${ROOT_DIR}"

install -d -m 0755 "${proof_root}/oracle"
printf '%s\n' 'small packed member alpha' >"${proof_root}/oracle/alpha.txt"
printf '%s\n' 'small packed member beta' >"${proof_root}/oracle/beta.txt"
dd if=/dev/zero of="${proof_root}/oracle/direct.bin" bs=1M count=17 status=none
chmod 0644 "${proof_root}/oracle/alpha.txt" "${proof_root}/oracle/beta.txt" \
  "${proof_root}/oracle/direct.bin"
printf '%s' '{"filesystem-proof":"filesystem-recovery-qualification-passphrase"}' \
  >"${proof_root}/passphrases.json"
chmod 0600 "${proof_root}/passphrases.json"

compose up --detach --wait filesystem-cache-adapter postgres app
bootstrap_token="$(cat "${RIVERHOG_BOOTSTRAP_TOKEN_HOST_PATH}")"
qualification_key_code="import json, os, urllib.request
request = urllib.request.Request(
    'http://127.0.0.1:8000/v1/apps/filesystem-recovery-qualification/keys',
    method='POST',
    data=json.dumps({'access': [{'permission': '*', 'resource': '*'}]}).encode(),
    headers={
        'Authorization': 'Bearer ' + os.environ['RIVERHOG_QUALIFICATION_BOOTSTRAP_TOKEN'],
        'Content-Type': 'application/json',
    },
)
print(json.load(urllib.request.urlopen(request))['token'])"
qualification_token="$(compose exec -T \
  --env "RIVERHOG_QUALIFICATION_BOOTSTRAP_TOKEN=${bootstrap_token}" \
  app python -c "${qualification_key_code}")"
test -n "${qualification_token}"
tags=()
for number in $(seq 1 300); do
  tags+=(--tag "qualification/recovery/tag-${number}")
done
receipt="$({
  docker run --rm \
    --network "${network_name}" \
    --env RIVERHOG_BASE_URL=http://app:8000 \
    --env "RIVERHOG_TOKEN=${qualification_token}" \
    --env RIVERHOG_ALLOW_INSECURE_HTTP=true \
    --env RIVERHOG_PROVENANCE_STATE_HOME=/state \
    --tmpfs /state:rw,noexec,nosuid,nodev,mode=700,uid=65532,gid=65532 \
    --volume "${proof_root}/oracle:/input:ro" \
    "${client_image}" collection upload start /input \
    --description 'Built-service filesystem recovery qualification' \
    --provenance-observer a-riverhog-linux-provenance-observer \
    "${tags[@]}" \
    --json
} 2>"${proof_root}/client.stderr")"
printf '%s\n' "${receipt}" >"${proof_root}/receipt.json"
python3 -c 'import json,sys; value=json.load(open(sys.argv[1])); collection_id=value["collection_id"]; assert isinstance(collection_id,str) and collection_id == str(int(collection_id)) and int(collection_id) > 0; assert len(value["archive_root_sha256"]) == 64' "${proof_root}/receipt.json"

compose stop app postgres filesystem-cache-adapter
docker run --rm \
  --user 65532:65532 \
  --volume "${volume_name}:/var/lib/riverhog-filesystem" \
  --entrypoint python \
  "${filesystem_image}" -c \
  'from pathlib import Path
from a_riverhog_filesystem_store import FilesystemStorageAdapter, FilesystemStorageAdapterConfig
from riverhog_storage_adapter_protocol import WriteStartRequest
with FilesystemStorageAdapter(FilesystemStorageAdapterConfig(root=Path("/var/lib/riverhog-filesystem"), minimum_free_bytes=0)) as adapter:
    adapter.begin_write(WriteStartRequest(object_path="archives/incomplete/payload.age", expected_bytes=65536, content_type="application/octet-stream", required_identity_assertions={"qualification":"incomplete"}, placement_policy="archive_default"))'

chmod 0777 "${proof_root}"
install -d -m 0777 \
  "${proof_root}/recovery-output" \
  "${proof_root}/materializer-full" \
  "${proof_root}/materializer-description" \
  "${proof_root}/materializer-tags"
docker run --rm \
  --network none \
  --volume "${proof_root}:/proof" \
  alpine:3.22@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce \
  chown 65532:65532 /proof/passphrases.json

docker run --rm \
  --network none \
  --user 65532:65532 \
  --volume "${volume_name}:/source:ro" \
  --volume "${proof_root}/materializer-full:/work" \
  --entrypoint /bin/sh \
  "${filesystem_image}" -ceu \
  'test ! -e /oracle; test ! -e /prior-full; test ! -e /passphrases.json; exec a-riverhog-filesystem-store-materialize "$@"' \
  sh /source /work/tree --all --json >"${proof_root}/materialize-full.json"
archive_relative="$(docker run --rm \
  --network none \
  --volume "${proof_root}/materializer-full/tree:/materialized:ro" \
  --entrypoint python \
  "${recovery_image}" -c \
  'from pathlib import Path
items=list(Path("/materialized").glob("archives/*/recovery.json"))
assert len(items) == 1, items
print(items[0].parent.relative_to("/materialized").as_posix())')"
test -n "${archive_relative}"
test ! -e "${proof_root}/materializer-full/tree/archives/incomplete"

docker run --rm \
  --network none \
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,nodev,mode=700,uid=65532,gid=65532 \
  --volume "${proof_root}/materializer-full/tree:/materialized:ro" \
  --volume "${proof_root}/recovery-output:/output-parent" \
  --volume "${proof_root}/passphrases.json:/passphrases.json:ro" \
  "${recovery_image}" "/materialized/${archive_relative}" /output-parent/recovered \
  --passphrases-file /passphrases.json
docker run --rm \
  --network none \
  --volume "${proof_root}/oracle:/oracle:ro" \
  --volume "${proof_root}/recovery-output/recovered:/recovered:ro" \
  alpine:3.22@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce \
  sh -ceu 'cmp /oracle/alpha.txt /recovered/alpha.txt; cmp /oracle/beta.txt /recovered/beta.txt; cmp /oracle/direct.bin /recovered/direct.bin; test -d /recovered/.riverhog/provenance'

instrument=(
  docker run --rm --network none --user 0
  --volume "${volume_name}:/source"
  --volume "${ROOT_DIR}/tests/harness/filesystem_recovery_payload_access.py:/instrument.py:ro"
  --entrypoint python "${filesystem_image}" /instrument.py /source
)
description_paths=(
  "${archive_relative}/recovery.json"
  "${archive_relative}/manifest.json.age"
  "${archive_relative}/description.json.age"
)
"${instrument[@]}" "${description_paths[@]}"
description_args=()
for path in "${description_paths[@]}"; do
  description_args+=(--path "${path}")
done
docker run --rm --network none --user 65532:65532 \
  --volume "${volume_name}:/source:ro" \
  --volume "${proof_root}/materializer-description:/work" \
  --entrypoint /bin/sh "${filesystem_image}" -ceu \
  'test ! -e /oracle; test ! -e /prior-full; test ! -e /passphrases.json; exec a-riverhog-filesystem-store-materialize "$@"' \
  sh /source /work/tree "${description_args[@]}"
description="$({
  docker run --rm --network none --read-only \
    --tmpfs /tmp:rw,noexec,nosuid,nodev,mode=700,uid=65532,gid=65532 \
    --volume "${proof_root}/materializer-description/tree:/materialized:ro" \
    --volume "${proof_root}/passphrases.json:/passphrases.json:ro" \
    "${recovery_image}" "/materialized/${archive_relative}" \
    --passphrases-file /passphrases.json --description-only
} 2>"${proof_root}/description.stderr")"
python3 -c 'import json,sys; assert json.loads(sys.argv[1])["description"] == "Built-service filesystem recovery qualification"' "${description}"

tag_paths=("${archive_relative}/recovery.json" "${archive_relative}/manifest.json.age" "${archive_relative}/tags/head.json.age")
tag_prefix="${archive_relative}/tags/nodes/"
"${instrument[@]}" "${tag_paths[@]}" --prefix "${tag_prefix}"
docker run --rm --network none --user 65532:65532 \
  --volume "${volume_name}:/source:ro" \
  --volume "${proof_root}/materializer-tags:/work" \
  --entrypoint /bin/sh "${filesystem_image}" -ceu \
  'test ! -e /oracle; test ! -e /prior-full; test ! -e /passphrases.json; exec a-riverhog-filesystem-store-materialize "$@"' \
  sh /source /work/tree \
  --path "${tag_paths[0]}" --path "${tag_paths[1]}" --path "${tag_paths[2]}" \
  --prefix "${tag_prefix}"
docker run --rm --network none --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,nodev,mode=700,uid=65532,gid=65532 \
  --volume "${proof_root}/materializer-tags/tree:/materialized:ro" \
  --volume "${proof_root}/passphrases.json:/passphrases.json:ro" \
  "${recovery_image}" "/materialized/${archive_relative}" \
  --passphrases-file /passphrases.json --tags-only >"${proof_root}/tags.json-seq"
python3 -c 'import json,sys; rows=[json.loads(line) for line in open(sys.argv[1])]; assert sum(row.get("record")=="tag" for row in rows)==300; assert rows[-1]=={"record":"complete","tag_count":300}' "${proof_root}/tags.json-seq"
"${instrument[@]}" --restore

for directory in invalid-corrupt invalid-missing invalid-authority invalid-output; do
  install -d -m 0777 "${proof_root}/${directory}"
done
docker run --rm --network none --volume "${proof_root}:/proof" \
  alpine:3.22@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce \
  sh -ceu 'cp -a /proof/materializer-full/tree/. /proof/invalid-corrupt/; cp -a /proof/materializer-full/tree/. /proof/invalid-missing/; cp -a /proof/materializer-full/tree/. /proof/invalid-authority/'
docker run --rm --network none --volume "${proof_root}/invalid-corrupt:/tree" \
  --entrypoint python "${recovery_image}" -c \
  'from pathlib import Path
path=next(Path("/tree").glob("archives/*/manifest.json.age"))
data=bytearray(path.read_bytes()); data[0]^=255; path.write_bytes(data)'
docker run --rm --network none --volume "${proof_root}/invalid-missing:/tree" \
  alpine:3.22@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce \
  sh -ceu 'path=$(find /tree -path "*/volumes/*" -type f -print -quit); test -n "$path"; rm "$path"'
docker run --rm --network none --volume "${proof_root}/invalid-authority:/tree" \
  --entrypoint python "${recovery_image}" -c \
  'from pathlib import Path
import json
path=next(Path("/tree").glob("archives/*/recovery.json"))
value=json.loads(path.read_text()); value["root"]["stored_sha256"]="0"*64
path.write_text(json.dumps(value,sort_keys=True,separators=(",",":")))'
for invalid in invalid-corrupt invalid-missing invalid-authority; do
  if docker run --rm --network none --read-only \
    --tmpfs /tmp:rw,noexec,nosuid,nodev,mode=700,uid=65532,gid=65532 \
    --volume "${proof_root}/${invalid}:/materialized:ro" \
    --volume "${proof_root}/invalid-output:/out" \
    --volume "${proof_root}/passphrases.json:/passphrases.json:ro" \
    "${recovery_image}" "/materialized/${archive_relative}" \
    "/out/${invalid}" --passphrases-file /passphrases.json >/dev/null 2>&1; then
    printf 'recovery unexpectedly accepted %s canonical tree\n' "${invalid}" >&2
    exit 1
  fi
  test ! -e "${proof_root}/invalid-output/${invalid}"
done

printf 'Qualified built filesystem recovery at %s; client=%s recovery=%s filesystem=%s\n' \
  "${SOURCE_REVISION}" \
  "$(docker image inspect --format '{{.Id}}' "${client_image}")" \
  "$(docker image inspect --format '{{.Id}}' "${recovery_image}")" \
  "$(docker image inspect --format '{{.Id}}' "${filesystem_image}")"
