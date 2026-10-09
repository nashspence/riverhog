#!/usr/bin/env bash

# One physical fixture configuration for standalone and aggregate scenarios.
configure_processing_fixture() {
  local lane="$1"
  case "${lane}" in
    processing-scale) smoke_file_count="${STOVE0_SMOKE_FILE_COUNT:-128}" ;;
    processing-e2e) smoke_file_count=4 ;;
    processing-overlap) smoke_file_count=1 ;;
    *) smoke_file_count=16 ;;
  esac
  smoke_audio_frames="${STOVE0_SMOKE_AUDIO_FRAMES:-2000}"
  if ! [[ "${smoke_file_count}" =~ ^[0-9]+$ ]] ||
    (( smoke_file_count < 1 || smoke_file_count > 1000 )); then
    printf '%s\n' 'STOVE0_SMOKE_FILE_COUNT must be between 1 and 1000.' >&2
    return 2
  fi
  if ! [[ "${smoke_audio_frames}" =~ ^[0-9]+$ ]] ||
    (( smoke_audio_frames < 1 || smoke_audio_frames > 10000000 )); then
    printf '%s\n' 'STOVE0_SMOKE_AUDIO_FRAMES must be between 1 and 10000000.' >&2
    return 2
  fi
  smoke_claim_file_count=$((smoke_file_count + 1))
  # Four target outputs per overlapping-route input; separate bounded control
  # contacts from observation, content access, and complete settlement budgets.
  smoke_completion_timeout=$((600 + 480 * smoke_file_count))
  smoke_observation_timeout=$((300 + 60 * smoke_file_count))
  smoke_admission_timeout=$((smoke_observation_timeout + 600))
  smoke_content_timeout=$((300 + 30 * (smoke_file_count > 16 ? smoke_file_count - 16 : 0)))
  smoke_max_bytes=$((smoke_file_count * (smoke_audio_frames * 2 + 4096) + 16384))
  smoke_download_quota_bytes=$((8 * (smoke_max_bytes + smoke_claim_file_count * 65552)))
  if (( smoke_download_quota_bytes < 16777216 )); then
    smoke_download_quota_bytes=16777216
  fi
}

processing_fixture_json() {
  # Keys are sorted and every value is an exact integer, yielding JCS JSON.
  printf '{"smoke_admission_timeout":%d,"smoke_audio_frames":%d,"smoke_claim_file_count":%d,"smoke_completion_timeout":%d,"smoke_content_timeout":%d,"smoke_download_quota_bytes":%d,"smoke_file_count":%d,"smoke_max_bytes":%d,"smoke_observation_timeout":%d}\n' \
    "${smoke_admission_timeout}" "${smoke_audio_frames}" "${smoke_claim_file_count}" \
    "${smoke_completion_timeout}" "${smoke_content_timeout}" "${smoke_download_quota_bytes}" \
    "${smoke_file_count}" "${smoke_max_bytes}" "${smoke_observation_timeout}"
}
