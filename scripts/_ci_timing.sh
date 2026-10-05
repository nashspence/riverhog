#!/usr/bin/env bash

# Each shell qualification owns its phase stream; no lifecycle state is shared.
ci_timing_started=$SECONDS
ci_timing_phase="bootstrap"
ci_timing_lane="${RIVERHOG_CI_LANE:-compose-smoke}"
export RIVERHOG_CI_RUN_ID="${RIVERHOG_CI_RUN_ID:-${BASHPID}-${EPOCHREALTIME}}"
ci_timing_file="${RIVERHOG_CI_TIMING_DIR:-${ROOT_DIR}/build/ci-timing}/${ci_timing_lane}/${RIVERHOG_CI_RUN_ID}/phases.jsonl"

ci_phase_finish() {
  local status="${1:-0}"
  "${MISE_BIN:-mise}" x -- uv run --locked --all-packages --group dev \
    python "${ROOT_DIR}/scripts/ci_timing.py" phase \
    --lane "${ci_timing_lane}" --name "${ci_timing_phase}" \
    --seconds "$((SECONDS - ci_timing_started))" --status "${status}" \
    --output "${ci_timing_file}"
}

ci_phase() {
  ci_phase_finish "${2:-0}"
  ci_timing_phase="$1"
  ci_timing_started=$SECONDS
  printf 'Qualification phase: %s\n' "${ci_timing_phase}"
}
