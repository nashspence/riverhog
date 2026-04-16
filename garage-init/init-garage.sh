#!/usr/bin/env bash
set -euo pipefail

strip_ansi() {
  sed -r 's/\x1B\[[0-9;]*[mK]//g'
}

garage_cmd() {
  /usr/local/bin/garage "$@"
}

garage_capture() {
  garage_cmd "$@" 2>&1 | strip_ansi
}

wait_for_garage() {
  local attempts=0
  until garage_capture status >/tmp/garage-status.txt; do
    attempts=$((attempts + 1))
    if [[ "${attempts}" -ge 60 ]]; then
      echo "Garage never became reachable" >&2
      cat /tmp/garage-status.txt >&2 || true
      return 1
    fi
    sleep 1
  done
}

assign_single_node_layout() {
  local status_output layout_output node_id version

  layout_output="$(garage_capture layout show)"
  if ! grep -q "No nodes currently have a role" <<<"${layout_output}"; then
    return 0
  fi

  status_output="$(garage_capture status)"
  node_id="$(awk 'NR > 2 && $1 ~ /^[0-9a-f]+$/ {print $1; exit}' <<<"${status_output}")"
  if [[ -z "${node_id}" ]]; then
    echo "Unable to determine Garage node ID" >&2
    echo "${status_output}" >&2
    return 1
  fi

  garage_cmd layout assign \
    -z "${GARAGE_LAYOUT_ZONE:-dc1}" \
    -c "${GARAGE_LAYOUT_CAPACITY:-1TB}" \
    "${node_id}"

  layout_output="$(garage_capture layout show)"
  version="$(awk '/garage layout apply --version/ {print $5; exit}' <<<"${layout_output}")"
  if [[ -z "${version}" ]]; then
    echo "Unable to determine Garage layout version" >&2
    echo "${layout_output}" >&2
    return 1
  fi

  garage_cmd layout apply --version "${version}"
}

ensure_key() {
  if garage_cmd key info "${S3_ACCESS_KEY}" >/dev/null 2>&1; then
    return 0
  fi

  garage_cmd key import \
    --yes \
    -n "${GARAGE_KEY_NAME:-archive}" \
    "${S3_ACCESS_KEY}" \
    "${S3_SECRET_KEY}"
}

ensure_bucket() {
  if garage_cmd bucket info "${S3_BUCKET}" >/dev/null 2>&1; then
    return 0
  fi

  garage_cmd bucket create "${S3_BUCKET}" >/tmp/garage-bucket.txt
}

ensure_permissions() {
  local bucket_info bucket_id

  bucket_info="$(garage_capture bucket info "${S3_BUCKET}")"
  bucket_id="$(awk '/^Bucket:/ {print $2; exit}' <<<"${bucket_info}")"
  if [[ -z "${bucket_id}" ]]; then
    echo "Unable to determine Garage bucket ID" >&2
    echo "${bucket_info}" >&2
    return 1
  fi

  if grep -q "${S3_ACCESS_KEY}" <<<"${bucket_info}"; then
    return 0
  fi

  garage_cmd bucket allow \
    --read \
    --write \
    --owner \
    "${bucket_id}" \
    --key "${S3_ACCESS_KEY}"
}

wait_for_garage
assign_single_node_layout
ensure_key
ensure_bucket
ensure_permissions

echo "Garage initialized for bucket ${S3_BUCKET}"
