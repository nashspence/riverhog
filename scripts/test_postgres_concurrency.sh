#!/usr/bin/env bash
set -Eeuo pipefail

qualification_phase="initialization"
qualification_failure_reported=0
qualification_failure() {
  local status="$1"
  if [[ "$-" != *e* || "$qualification_failure_reported" == "1" ||
        ! "$status" =~ ^[1-9][0-9]*$ ]] || (( status > 255 )); then
    return 0
  fi
  case "$qualification_phase" in
    initialization|image|database|tests|roundtrip|cleanup) ;;
    *) return 0 ;;
  esac
  qualification_failure_reported=1
  printf '::error title=PostgreSQL qualification::{"format":"riverhog-postgres-failure-diagnostic/v1","phase":"%s","exit_status":%s}\n' \
    "$qualification_phase" "$status" >&2 || true
  return 0
}
trap 'qualification_failure "$?"' ERR

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_compose_env.sh"

setup_test_compose_project
configure_compose_tty
qualification_phase="image"
ensure_compose_image test

cleanup() {
  qualification_phase="cleanup"
  compose down --volumes --remove-orphans
}
trap cleanup EXIT

qualification_phase="database"
compose up --detach --wait postgres

database_url="postgresql+psycopg://$(compose_env_value POSTGRES_USER riverhog):$(compose_env_value POSTGRES_PASSWORD riverhog)@postgres:5432/$(compose_env_value POSTGRES_DB riverhog)"
default_tests=(
  tests/integration/test_catalog_schema_postgres.py
  tests/integration/test_immutable_archive_preparation_concurrency.py
    tests/integration/test_immutable_archive_admission_concurrency.py
  tests/integration/test_collection_deletion_concurrency.py
  tests/integration/test_collection_upload_custody_concurrency.py
  tests/integration/test_collection_finalization_concurrency.py
  tests/integration/test_archive_volume_metadata_checkpoint_concurrency.py \
  tests/integration/test_archive_volume_metadata_receiving_concurrency.py
  tests/integration/test_archive_volume_metadata_writes_concurrency.py
  tests/integration/test_archive_volume_metadata_finalizer_concurrency.py
  tests/integration/test_archive_authority_checkpoint_concurrency.py
  tests/integration/test_archive_authority_finalizer_concurrency.py
  tests/integration/test_archive_provenance_structure_checkpoint_concurrency.py
  tests/integration/test_archive_provenance_structure_writer_concurrency.py
  tests/integration/test_archive_provenance_journal_checkpoint_concurrency.py
  tests/integration/test_archive_provenance_journal_writer_concurrency.py
  tests/integration/test_archive_provenance_volume_checkpoint_concurrency.py
  tests/integration/test_archive_provenance_volume_progress_concurrency.py
  tests/integration/test_archive_provenance_volume_finalizer_concurrency.py
  tests/integration/test_archive_provenance_authority_checkpoint_concurrency.py
  tests/integration/test_archive_provenance_authority_finalizer_concurrency.py
  tests/integration/test_collection_upload_tag_frontier.py
  tests/integration/test_archive_initial_description_concurrency.py
  tests/integration/test_archive_initial_tag_concurrency.py
  tests/integration/test_archive_upload_drain_concurrency.py
  tests/integration/test_immutable_archive_terminal_drain.py \
  tests/integration/test_archive_finalizer_terminal_drain.py
  tests/integration/test_archive_finalizer_registry.py
  tests/integration/test_archive_finalizer_work.py
  tests/integration/test_immutable_archive_writes_concurrency.py
  tests/integration/test_archive_immutable_source_receiving_concurrency.py
  tests/integration/test_operation_lifecycle_postgres.py
  tests/integration/test_canonical_discovery_rebuild_postgres.py
  tests/integration/test_canonical_discovery_corpus_jobs_postgres.py
  tests/integration/test_download_allowance_concurrency.py
  tests/integration/test_lifecycle_event_concurrency.py
  tests/integration/test_provider_qualification_checkpoint_postgres.py
  tests/integration/test_public_selector_plans_postgres.py
  tests/integration/test_retrieval_cache_admission_concurrency.py
  tests/integration/test_retrieval_selection_concurrency.py
  tests/integration/test_archive_copy_retirement_cache_concurrency.py
  tests/integration/test_stove0_postgres_concurrency.py
)
read -r -a postgres_tests <<< "${POSTGRES_TESTS:-${default_tests[*]}}"
qualification_phase="tests"
compose run --rm --no-deps \
  --env "RIVERHOG_TEST_POSTGRES_URL=${database_url}" \
  "${COMPOSE_RUN_TTY_ARGS[@]}" \
  test \
  -q \
  "${postgres_tests[@]}"

if [[ -z "${POSTGRES_TESTS:-}" || "${RIVERHOG_PROVIDER_CHECKPOINT_HOST_TEST:-}" == "1" ]]; then
  qualification_phase="roundtrip"
  export RIVERHOG_TEST_POSTGRES_URL="${database_url}"
  RIVERHOG_PROVIDER_CHECKPOINT_HOST_TEST=1 \
    mise x -- uv run --locked --group dev python -m pytest -p no:cacheprovider -q \
      tests/integration/test_provider_qualification_checkpoint_roundtrip.py
fi
