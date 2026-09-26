#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_compose_env.sh"

setup_test_compose_project
configure_compose_tty
ensure_compose_image test

cleanup() {
  compose down --volumes --remove-orphans
}
trap cleanup EXIT

compose up --detach --wait postgres

database_url="postgresql+psycopg://$(compose_env_value POSTGRES_USER riverhog):$(compose_env_value POSTGRES_PASSWORD riverhog)@postgres:5432/$(compose_env_value POSTGRES_DB riverhog)"
default_tests=(
  tests/integration/test_catalog_schema_postgres.py
  tests/integration/test_collection_deletion_concurrency.py
  tests/integration/test_collection_upload_custody_concurrency.py
  tests/integration/test_download_allowance_concurrency.py
  tests/integration/test_lifecycle_event_concurrency.py
  tests/integration/test_provider_qualification_checkpoint_postgres.py
  tests/integration/test_public_selector_plans_postgres.py
  tests/integration/test_retrieval_cache_admission_concurrency.py
  tests/integration/test_stove0_postgres_concurrency.py
)
read -r -a postgres_tests <<< "${POSTGRES_TESTS:-${default_tests[*]}}"
compose run --rm --no-deps \
  --env "RIVERHOG_TEST_POSTGRES_URL=${database_url}" \
  "${COMPOSE_RUN_TTY_ARGS[@]}" \
  test \
  -q \
  "${postgres_tests[@]}"

if [[ -z "${POSTGRES_TESTS:-}" || "${RIVERHOG_PROVIDER_CHECKPOINT_HOST_TEST:-}" == "1" ]]; then
  export RIVERHOG_TEST_POSTGRES_URL="${database_url}"
  RIVERHOG_PROVIDER_CHECKPOINT_HOST_TEST=1 \
    mise x -- uv run --locked --group dev python -m pytest -q \
      tests/integration/test_provider_qualification_checkpoint_roundtrip.py
fi
