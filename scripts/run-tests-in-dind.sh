#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/docker-compose.test.yml"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-riverhog_tests}"
export COMPOSE_MENU="${COMPOSE_MENU:-false}"
HEARTBEAT_SECONDS="${HEARTBEAT_SECONDS:-30}"

timestamp() {
  date -u +"%H:%M:%S"
}

log() {
  printf '[run-tests %s] %s\n' "$(timestamp)" "$*"
}

format_duration() {
  local total_seconds="$1"
  local minutes=$((total_seconds / 60))
  local seconds=$((total_seconds % 60))
  printf '%dm%02ds' "${minutes}" "${seconds}"
}

run_with_heartbeat() {
  local label="$1"
  local expectation="$2"
  shift 2

  log "${label}"
  log "${expectation}"

  local started_at=$SECONDS
  local next_heartbeat="${HEARTBEAT_SECONDS}"
  "$@" &
  local pid=$!

  while kill -0 "${pid}" >/dev/null 2>&1; do
    sleep 1
    local elapsed=$((SECONDS - started_at))
    if (( elapsed >= next_heartbeat )) && kill -0 "${pid}" >/dev/null 2>&1; then
      log "${label} still running after $(format_duration "${elapsed}")."
      next_heartbeat=$((next_heartbeat + HEARTBEAT_SECONDS))
    fi
  done

  if wait "${pid}"; then
    log "${label} finished in $(format_duration "$((SECONDS - started_at))")."
  else
    local status=$?
    log "${label} failed after $(format_duration "$((SECONDS - started_at))")."
    return "${status}"
  fi
}

cleanup() {
  docker compose -f "${COMPOSE_FILE}" down --volumes --remove-orphans >/dev/null 2>&1 || true
}

trap cleanup EXIT
cleanup

if (($# == 0)); then
  set -- pytest
elif [[ "$1" != "pytest" ]]; then
  set -- pytest "$@"
fi

if [[ $# -eq 1 && "$1" == "pytest" ]]; then
  TEST_EXPECTATION="Full suite usually takes about 3-4 minutes after the image is ready. Quiet stretches are normal during Playwright and Docker integration tests."
else
  TEST_EXPECTATION="Targeted runs are usually faster, but integration-heavy tests can still sit quiet for tens of seconds at a time."
fi

if docker image inspect riverhog-test:dev >/dev/null 2>&1; then
  BUILD_EXPECTATION="Using the local Docker cache. Rebuilds are often quick, but can still take a couple of minutes if browser layers need refreshing."
else
  BUILD_EXPECTATION="No local test image cache found. Expect roughly 2-4 minutes for the first build because the Playwright browser layers are large."
fi

log "Preparing Docker test environment."
log "Pytest command: $*"

run_with_heartbeat \
  "Step 1/2: building the Docker test image." \
  "${BUILD_EXPECTATION}" \
  docker compose -f "${COMPOSE_FILE}" build test

run_with_heartbeat \
  "Step 2/2: running pytest inside Docker." \
  "${TEST_EXPECTATION}" \
  docker compose -f "${COMPOSE_FILE}" run --rm test "$@"
